#!/bin/bash
set -euo pipefail

INDIR=$1
OUTDIR=$2
NJOBS=$3

check_outputs() {
    # check the number of lines
    fallowed="${INDIR}/allowed.zst"
    if rclone lsf "$fallowed"  &>/dev/null; then
      x=$(rclone cat "$fallowed" | zstdcat | jq -c 'select(.allowed==true)'|wc)
    else
      x=$(rclone cat "${INDIR}/metadata.zst" | zstdcat | wc -l)
    fi

    C=`paste <(zstdcat ${OUTDIR}/text.zst|wc -l) <(zstdcat ${OUTDIR}/xml.zst|wc -l) <(zstdcat ${OUTDIR}/md.zst|wc -l) <(zstdcat ${OUTDIR}/metadata.zst|wc -l)`
    read a b c d  <<< "$C"
    if [[ $x == "$a" && $a == "$b" && $b == "$c" && $c == "$d" ]]; then
        echo $a $b $c $d >"${OUTDIR}/.done"
    else
      echo "ERROR: Number of lines mismatch: $x $a $b $c $d"  1>&2
      exit 1
    fi
}

# Cleanup trap to remove the temporary directory on exit
cleanup() {
    echo "Cleaning up temporary directory: $TMP_DIR"
    rm -rf "$TMP_DIR"
}
trap cleanup EXIT

# Create a temporary directory for intermediate files
TMP_DIR=$(mktemp -d)
# Ensure the main output directory exists
mkdir -p "$OUTDIR" "$TMP_DIR"

# intermediate files
FL1="$TMP_DIR/l1"
FL2="$TMP_DIR/l2"
FHTMLMETA="$TMP_DIR/htmlmeta"

mkfifo "$TMP_DIR/pipe_l1"
mkfifo "$TMP_DIR/pipe_l2"
mkfifo "$TMP_DIR/pipe_md"

NJOBS=$(($NJOBS - 2))  # 1 cpu for muxdemux, 1 for compression processes in the pipeline
GLJOBS=$(( NJOBS / 2 )); (( GLJOBS < 1 )) && GLJOBS=1   # (3N+3+2N+4)/6 = (5N+7)/6 = 5/6 N + 7/6 < N <=> N > 7
OLJOBS=$(( NJOBS / 3 )); (( OLJOBS < 1 )) && OLJOBS=1
MDJOBS=$(($NJOBS - $OLJOBS - $GLJOBS)); (( MDJOBS < 1 )) && MDJOBS=1

LID_BLOCKSIZE=30M  # the block size selected for OpenLID in stage2; should be ok for xml2md.py too as loading time is smaller and inputs are larger (xml vs. text)
XML2MD_BLOCKSIZE=10M  # the block size selected for OpenLID in stage2; should be ok for xml2md.py too as loading time is smaller and inputs are larger (xml vs. text)

run_lid_parallel() {
    # --keep-order guarantees that GNU Parallel will collect stdout from tasks and print it to its stdout in the order
    # aligned with the order of input lines
    # --block issues one task per block of input lines of roughly this size, but without breaking lines;
    # making it too small will increase extra costs on script initialization (e.e. weights loading for langid),
    # making it too large will require buffering too much outputs in parallel due to --keep-order requirement.
    printf "started %s in %s processes\n" "${2}" "${1}" 1>&2
    time_start=$(date +%s.%N)
    parallel --halt now,fail=1 --block $LID_BLOCKSIZE -j "${1}" --pipe --keep-order  \
            "python -m hplt_textpipes.stage3.fastertext_lid.proto_langid --identity ${2}"
    time_end=$(date +%s.%N)
    printf "%.3fs: finished %s in %s processes\n" "$(echo "$time_end - $time_start" | bc)" "${2}" "${1}" 1>&2
}

run_xml2md_parallel() {
    printf "started xml2md in max %s processes\n" "${1}" 1>&2
    time_start=$(date +%s.%N)
    parallel --halt now,fail=1 --block $XML2MD_BLOCKSIZE -j "${1}" --pipe --keep-order  \
            "python -m hplt_textpipes.stage3.xml2md --md-only --verbosity=0"
    time_end=$(date +%s.%N)
    printf "%.3fs: finished xml2md in max %s processes\n" "$(echo "$time_end - $time_start" | bc)" "${1}" 1>&2
}

compress() {
    zstd >"${1}"
}

# Start background processes to read from named pipes
run_xml2md_parallel $MDJOBS <"$TMP_DIR/pipe_md" | zstd >"$OUTDIR/md.zst"  &
PID_MD=$!

run_lid_parallel $GLJOBS glotlid-v3 <"$TMP_DIR/pipe_l2" >"$FL2"  &
PID_L2=$!

run_lid_parallel $OLJOBS openlid-v3 <"$TMP_DIR/pipe_l1" >"$FL1"  &
PID_L1=$!

echo $(date +"%T") starting jsonl_muxdemux 1
python -m hplt_textpipes.utils.jsonl_muxdemux \
    <(zstdcat "$INDIR/text.zst") \
    -- \
    >(compress "$OUTDIR/xml.zst") xml=x \
    >(compress "$OUTDIR/text.zst") text=t \
    "$FHTMLMETA" htmllang,metalang,tagfilter  \
    "$TMP_DIR/pipe_l2" t \
    "$TMP_DIR/pipe_l1" t \
    "$TMP_DIR/pipe_md" x

# Wait for background lid processes to finish

echo $(date +"%T") waiting for LIDs to finish
wait $PID_L2 $PID_L1

echo $(date +"%T") starting jsonl_muxdemux 2
python -m hplt_textpipes.utils.jsonl_muxdemux \
    <(zstdcat "$INDIR/metadata.zst" | python -m hplt_textpipes.stage3.add_id -) \
    <(zstdcat "$INDIR/lang.zst" | jq -c '{"openlid-v2":.}') \
    "$FHTMLMETA" \
    "$FL2" \
    "$FL1" \
    -- \
    >(compress "$OUTDIR/metadata.zst") '*'

# Wait for background xml2md processes to finish

echo $(date +"%T") waiting for xml2md to finish
wait $PID_MD
echo $(date +"%T") "all done"

check_outputs