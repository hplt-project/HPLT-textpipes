#!/bin/bash
set -euo pipefail

INPUT_DIR=$1
OUTPUT_DIR=$2
NJOBS=$3

check_outputs() {
    # check the number of lines
    fallowed="${INPUT_DIR}/allowed.zst"
    if rclone lsf "$fallowed"  &>/dev/null; then
      x=$(rclone cat "$fallowed" | zstdcat | jq -c 'select(.allowed==true)'|wc)
    else
      x=$(rclone cat "${INPUT_DIR}/metadata.zst" | zstdcat | wc -l)
    fi

    C=`paste <(zstdcat ${OUTPUT_DIR}/text.zst|wc -l) <(zstdcat ${OUTPUT_DIR}/xml.zst|wc -l) <(zstdcat ${OUTPUT_DIR}/md.zst|wc -l) <(zstdcat ${OUTPUT_DIR}/metadata.zst|wc -l)`
    read a b c d  <<< "$C"
    if [[ $x == "$a" && $a == "$b" && $b == "$c" && $c == "$d" ]]; then
        echo $a $b $c $d >"${OUTPUT_DIR}/.done"
    else
      echo "ERROR: Number of lines mismatch: $x $a $b $c $d"  1>&2
      exit 1
    fi
}

# Cleanup trap to remove the temporary directory on exit
cleanup() {
    echo "Cleaning up temporary directory: $TMP_DIR" 1>&2
    rm -rf "$TMP_DIR"
}
trap cleanup EXIT

# Create a temporary directory for intermediate files
TMP_DIR=$(mktemp -d)
# Ensure the main output directory exists
mkdir -p "$OUTPUT_DIR" "$TMP_DIR"

# intermediate files
FL1="$TMP_DIR/l1"
FL2="$TMP_DIR/l2"
FHTMLMETA="$TMP_DIR/htmlmeta"

mkfifo "$TMP_DIR/pipe_l1"
mkfifo "$TMP_DIR/pipe_l2"
mkfifo "$TMP_DIR/pipe_md"

NJOBS=$(( NJOBS - 2 ))  # leave 2 for non-cpu-intensive and auxiliary processes
# step1: 30% xml2md, 70% glotlid
MDJOBS=$(( NJOBS * 3 / 10 )); (( MDJOBS < 1 )) && MDJOBS=1
GLJOBS=$(( NJOBS - MDJOBS )); (( GLJOBS < 1 )) && GLJOBS=1
# step2: 3 for compression, rest openlid
OLJOBS=$(( NJOBS - 3 )); (( OLJOBS < 1 )) && OLJOBS=1

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
            "python -m hplt_textpipes.stage3.fastertext_lid.proto_langid --identity ${2} --text_field ${3}"
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

compress_file() {
    t2sz "${1}" -s 512K -l 3 -o "${2}".512K
}

# Run xml2md and glotlid in background, and mexdemux in foreground: text.zst -> l2, xml, text, md, htmllang
run_lid_parallel $GLJOBS glotlid-v3 text <"$TMP_DIR/pipe_l2" >"$FL2"  &
PID_L2=$!

run_xml2md_parallel $MDJOBS <"$TMP_DIR/pipe_md" >"$TMP_DIR/md"  &
PID_MD=$!

echo $(date +"%T") step1: starting jsonl_muxdemux  1>&2
python -m hplt_textpipes.utils.jsonl_muxdemux \
    <(rclone cat "$INPUT_DIR/text.zst" | zstdcat) \
    -- \
    "$TMP_DIR/pipe_l2" text=t \
    $TMP_DIR/xml xml=x \
    $TMP_DIR/text text=t \
    "$FHTMLMETA" htmllang,metalang,tagfilter  \
    "$TMP_DIR/pipe_md" x

echo $(date +"%T") waiting for background xml2md  1>&2
wait $PID_MD

# xml, text, md are ready, start compression to xml.zst, text.zst, md.zst in the background
pids=()
echo $(date +"%T") starting 3x t2sz in background  1>&2
for f in xml md text; do
    compress_file "$TMP_DIR/${f}" "$OUTPUT_DIR/${f}.zst" &
    pids+=("$!")
done

# NB! this results is a delay, but we risk OOM if running many openlids before all glotlids finished
echo $(date +"%T") waiting for background glotlid processes  1>&2
wait $PID_L2

echo $(date +"%T") step2: running openlid  1>&2
run_lid_parallel $OLJOBS openlid-v3 text <"$TMP_DIR/text" >"$FL1"

# step3 collects all metadata
echo $(date +"%T") step3: starting jsonl_muxdemux  1>&2
python -m hplt_textpipes.utils.jsonl_muxdemux \
    <(rclone cat "$INPUT_DIR/metadata.zst" | zstdcat | python -m hplt_textpipes.stage3.add_id -) \
    <(rclone cat  "$INPUT_DIR/lang.zst" | zstdcat | jq -c '{"openlid-v2":.}') \
    "$FHTMLMETA" \
    "$FL2" \
    "$FL1" \
    -- \
    $TMP_DIR/metadata '*'

echo "$(date +"%T") finishing compression"  1>&2
compress_file $TMP_DIR/metadata $OUTPUT_DIR/metadata.zst

# Wait for all compression PIDs
for pid in "${pids[@]}"; do
    wait "$pid"
done

check_outputs
echo $(date +"%T") "all done" 1>&2
