#!/usr/bin/bash
flist=$1

set -euo pipefail
flistname=$(basename $1)
bucketname=${flistname,,}  # bucket names should be in lowercase
rclone mkdir lumio:${bucketname}
rclone copy -P --s3-max-upload-parts 1000 --transfers 32 --s3-upload-concurrency 16 --s3-chunk-size 128M --checkers 1 --log-level DEBUG --log-file ${flistname}.log --multi-thread-streams 1 --http-url https://data.commoncrawl.org/ :http: lumio:${bucketname} --files-from $flist   --no-traverse --http-no-head
