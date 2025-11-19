#!/bin/bash

LOCALSCRIPT=$1
BASEOUTDIR=$2

process() {
    local x=$1
    local NJOBS=$2
    local INDIR=${x%/*}
    local OUTDIR=$(stage3getoutdir.sh "${x}" "${BASEOUTDIR}")
    echo "$(date) Processing ${INDIR} in ${NJOBS} jobs, writing to ${OUTDIR}" >&2 # forward everything to stderr to interleave correctly with error messages from stage3local.sh
    bash "$LOCALSCRIPT" "${INDIR}" "${OUTDIR}" "${NJOBS}"
    c="$?"
    echo "$(date) stage3local_batch.sh exit code: $c for $x" >&2 # Added $x here for better clarity in parallel logs
    return $c
}

# Export variables and functions to make them available to GNU parallel
export LOCALSCRIPT
export BASEOUTDIR
export -f process

echo "$(date) stage3local_batch.sh: running at `hostname`"
echo "$(date) stage3local_batch.sh: processing ${@:3} in parallel, writing to $BASEOUTDIR"
echo -n "Total size in GB: "
printf "%s\n" "${@:3}" | xargs -n1 rclone lsjson | jq -c '.[]|.Size' | awk '{sum+=$1} END {print sum/2**30}'


source stage3preparenode.sh  # run node preparations that should be done once before processing the batch

# Use GNU parallel to run the process function on each input file.
# --no-notice suppresses the citation message.
# --line-buffer helps prevent output from different jobs from being interleaved badly.
# By default, parallel runs all jobs and its exit code will be non-zero if any job failed.
parallel --line-buffer --no-notice -j4 'process {} 32' ::: "${@:3}"
final_rc=$?

if [ "$final_rc" -eq 0 ]; then
  echo "$(date) stage3local_batch.sh: all files processed successfully"
else
  echo "$(date) stage3local_batch.sh: one or more files failed to process. Exit code from parallel: $final_rc"
fi

exit $final_rc
