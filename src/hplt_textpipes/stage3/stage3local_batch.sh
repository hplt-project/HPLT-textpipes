#!/bin/bash

LOCALSCRIPT=$1
BASEOUTDIR=$2

getoutdir() {
    local path_in=$1
    # Strip remote prefix if it exists (e.g., "s3:path/file" -> "path/file")
    local path_no_remote="${path_in#*:}"
    # Get the parent directory of the remaining path
    local parent_dir
    parent_dir=$(dirname "$path_no_remote")
    # Construct the final output directory path
    # If parent_dir is '.', it means the input was just a filename, so output to BASEOUTDIR
    if [[ "$parent_dir" == "." ]]; then
        echo "$BASEOUTDIR"
    else
        echo "${BASEOUTDIR}/${parent_dir}"
    fi
}

process() {
    local x=$1
    local OUTDIR=$2
    echo "$(date) Processing $x, writing to ${OUTDIR}" >&2 # forward everything to stderr to interleave correctly with error messages from stage3local.sh
    bash "$LOCALSCRIPT" "$x" "${OUTDIR}"
    c="$?"
    echo "$(date) stage3local_batch.sh exit code: $c for $x" >&2 # Added $x here for better clarity in parallel logs
    return $c
}

# Export variables and functions to make them available to GNU parallel
export LOCALSCRIPT
export BASEOUTDIR
export -f getoutdir
export -f process

echo "$(date) stage3local_batch.sh: running at `hostname`"
echo "$(date) stage3local_batch.sh: processing ${@:3} in parallel, writing to $BASEOUTDIR"
echo -n "Total size in GB: "
printf "%s\n" "${@:3}" | xargs -n1 rclone lsjson | jq -c '.[]|.Size' | awk '{sum+=$1} END {print sum/2**30}'

# Use GNU parallel to run the process function on each input file.
# --no-notice suppresses the citation message.
# --line-buffer helps prevent output from different jobs from being interleaved badly.
# By default, parallel runs all jobs and its exit code will be non-zero if any job failed.
parallel --no-notice --line-buffer 'process "{}" "$(getoutdir "{}")"' ::: "${@:3}"
final_rc=$?

if [ "$final_rc" -eq 0 ]; then
  echo "$(date) stage3local_batch.sh: all files processed successfully"
else
  echo "$(date) stage3local_batch.sh: one or more files failed to process. Exit code from parallel: $final_rc"
fi

exit $final_rc
