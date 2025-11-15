#!/bin/bash
set -euo pipefail

INPUT_DIR=$1
OUTPUT_DIR=$2
#NJOBS=`nproc --all`
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

#GLJOBS=4
#OLJOBS=3
#MDJOBS=1

#NJOBS=$(($NJOBS - 2))  # leave some threads for rclone, zstdcat, zstd steps in the pipeline
GLJOBS=$(( (NJOBS + 1) / 2 )) # (3N+3+2N+4)/6 = (5N+7)/6 = 5/6 N + 7/6 < N <=> N > 7
OLJOBS=$(( (NJOBS + 2) / 3 ))
MDJOBS=$(($NJOBS - $OLJOBS - $GLJOBS)); (( MDJOBS < 1 )) && MDJOBS=1

BLOCKSIZE=100M  # the block size selected for OpenLID in stage2; should be ok for xml2md.py too as loading time is smaller and inputs are larger (xml vs. text)

run_lid_parallel() {
    # --keep-order guarantees that GNU Parallel will collect stdout from tasks and print it to its stdout in the order
    # aligned with the order of input lines
    # --block issues one task per block of input lines of roughly this size, but without breaking lines;
    # making it too small will increase extra costs on script initialization (e.e. weights loading for langid),
    # making it too large will require buffering too much outputs in parallel due to --keep-order requirement.
    time_start=$(date +%s.%N)
    time -p parallel --halt now,fail=1 --block $BLOCKSIZE -j "${1}" --pipe --keep-order  \
            "python -m hplt_textpipes.stage3.fastertext_lid.proto_langid --identity ${2}"
    time_end=$(date +%s.%N)
    printf "%.3fs: finished %s in %s processes\n" "$(echo "$time_end - $time_start" | bc)" "${2}" "${1}" 1>&2
}

run_xml2md_parallel() {
    time_start=$(date +%s.%N)
    time -p parallel --halt now,fail=1 --block $BLOCKSIZE -j "${1}" --pipe --keep-order  \
            "python -m hplt_textpipes.stage3.xml2md --md-only --verbosity=0"
    time_end=$(date +%s.%N)
    printf "%.3fs: finished xml2md in %s processes\n" "$(echo "$time_end - $time_start" | bc)" "${1}" 1>&2

}

#time zstdcat "$INPUT_DIR/text.zst" | run_lid_parallel $GLJOBS glotlid-v3 >/dev/null
#time zstdcat "$INPUT_DIR/text.zst" | run_lid_parallel $GLJOBS glotlid-v3 >"$TMP_DIR/l2"

# Start background processes to read from named pipes
run_xml2md_parallel $MDJOBS <"$TMP_DIR/pipe_md" | zstd >"$OUTPUT_DIR/md.zst"  &
PID_MD=$!

run_lid_parallel $GLJOBS glotlid-v3 <"$TMP_DIR/pipe_l2" >"$FL2"  &
PID_L2=$!

run_lid_parallel $OLJOBS openlid-v3 <"$TMP_DIR/pipe_l1" >"$FL1"  &
PID_L1=$!

python -m hplt_textpipes.utils.jsonl_muxdemux \
    "$INPUT_DIR/text.zst" \
    -- \
    "$OUTPUT_DIR/xml.zst" xml=x \
    "$OUTPUT_DIR/text.zst" text=t \
    "$TMP_DIR/pipe_md" x \
    "$FHTMLMETA" htmllang,metalang,tagfilter  \
    "$TMP_DIR/pipe_l2" t \
    "$TMP_DIR/pipe_l1" t

# Wait for background lid processes to finish
wait $PID_L2 $PID_L1

python -m hplt_textpipes.utils.jsonl_muxdemux \
    <(zstdcat "$INPUT_DIR/metadata.zst" | python -u -m hplt_textpipes.stage3.add_id -) \
    <(zstdcat "$INPUT_DIR/lang.zst" | jq -c '{"openlid-v2":.}') \
    "$FHTMLMETA" \
    "$FL2" \
    "$FL1" \
    -- \
    "$OUTPUT_DIR/metadata.zst" '*'

# Wait for background xml2md processes to finish
wait $PID_MD