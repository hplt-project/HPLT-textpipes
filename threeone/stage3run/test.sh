#!/bin/bash
set -euo pipefail

INPUT_DIR=$1
OUTPUT_DIR=$2
NJOBS=$3

# Create a temporary directory for intermediate files
TMP_DIR=$(mktemp -d)
# Ensure the main output directory exists
mkdir -p "$OUTPUT_DIR" "$TMP_DIR"

# Cleanup trap to remove the temporary directory on exit
cleanup() {
    echo "Cleaning up temporary directory: $TMP_DIR"
    rm -rf "$TMP_DIR"
}
trap cleanup EXIT


# intermediate files
FL1="$TMP_DIR/l1"
FL2="$TMP_DIR/l2"
FHTMLMETA="$TMP_DIR/htmlmeta"

mkfifo "$TMP_DIR/pipe_l1"
mkfifo "$TMP_DIR/pipe_l2"
mkfifo "$TMP_DIR/pipe_md"


NJOBS=$(($NJOBS - 2))  # 1 cpu for muxdemux, 1 for 3xzstd in the pipeline
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
#    cat
#    jq -c '{lang:["cmn_Hans","cmn_Hant","jpn_Jpan"],prob:[0.9885,0.0115,0]}'

    parallel --halt now,fail=1 --block $LID_BLOCKSIZE -j "${1}" --pipe --keep-order  \
            "python -m hplt_textpipes.stage3.fastertext_lid.proto_langid --identity ${2}"
    time_end=$(date +%s.%N)
    printf "%.3fs: finished %s in %s processes\n" "$(echo "$time_end - $time_start" | bc)" "${2}" "${1}" 1>&2
}

run_xml2md_parallel() {
    printf "started xml2md in max %s processes\n" "${1}" 1>&2
    time_start=$(date +%s.%N)
#    cat
#    jq -c '{md: "# pásek Dakine Rivets black\n\nDostupnost: Skladem"}'
    parallel --halt now,fail=1 --block $XML2MD_BLOCKSIZE -j "${1}" --pipe --keep-order  \
            "python -m hplt_textpipes.stage3.xml2md --md-only --verbosity=0"
    time_end=$(date +%s.%N)
    printf "%.3fs: finished xml2md in max %s processes\n" "$(echo "$time_end - $time_start" | bc)" "${1}" 1>&2

}

compress() {
    zstd >"${1}"
}

#zstdcat "$INPUT_DIR/text.zst" | run_lid_parallel $GLJOBS glotlid-v3 >/dev/null
#zstdcat "$INPUT_DIR/text.zst" | run_lid_parallel $GLJOBS glotlid-v3 >"$FL1"

# Start background processes to read from named pipes
run_xml2md_parallel $MDJOBS <"$TMP_DIR/pipe_md" | zstd >"$OUTPUT_DIR/md.zst"  &
PID_MD=$!

run_lid_parallel $GLJOBS glotlid-v3 <"$TMP_DIR/pipe_l2" >"$FL2"  &
PID_L2=$!

run_lid_parallel $OLJOBS openlid-v3 <"$TMP_DIR/pipe_l1"   >"$FL1"  &
PID_L1=$!

#echo $(date +"%T") starting jsonl_muxdemux 1
#python -m hplt_textpipes.utils.jsonl_muxdemux \
#    <(zstdcat "$INPUT_DIR/text.zst") \
#    -- \
#    >(compress "$OUTPUT_DIR/xml.zst") xml=x \
#    >(compress "$OUTPUT_DIR/text.zst") text=t \
#    "$FHTMLMETA" htmllang,metalang,tagfilter  \
#    "$TMP_DIR/pipe_md" x \
#    "$TMP_DIR/pipe_l2" t \
#    "$TMP_DIR/pipe_l1" t

#echo $(date +"%T") waiting for background processes
#wait $PID_MD $PID_L2 $PID_L1
#echo $(date +"%T") background processes finished
#exit


echo $(date +"%T") starting jsonl_muxdemux 1
python -m hplt_textpipes.utils.jsonl_muxdemux \
    <(zstdcat "$INPUT_DIR/text.zst") \
    -- \
    >(compress "$OUTPUT_DIR/xml.zst") xml=x \
    >(compress "$OUTPUT_DIR/text.zst") text=t \
    "$FHTMLMETA" htmllang,metalang,tagfilter  \
    "$TMP_DIR/pipe_l2" t \
    "$TMP_DIR/pipe_l1" t \
    "$TMP_DIR/pipe_md" x

# Wait for background lid processes to finish

echo $(date +"%T") waiting for LIDs to finish
wait $PID_L2 $PID_L1

echo $(date +"%T") starting jsonl_muxdemux 2
python -m hplt_textpipes.utils.jsonl_muxdemux \
    <(zstdcat "$INPUT_DIR/metadata.zst" | python -m hplt_textpipes.stage3.add_id -) \
    <(zstdcat "$INPUT_DIR/lang.zst" | jq -c '{"openlid-v2":.}') \
    "$FHTMLMETA" \
    "$FL2" \
    "$FL1" \
    -- \
    >(compress "$OUTPUT_DIR/metadata.zst") '*'

# Wait for background xml2md processes to finish

echo $(date +"%T") waiting for xml2md to finish
wait $PID_MD
echo $(date +"%T") "all done"
