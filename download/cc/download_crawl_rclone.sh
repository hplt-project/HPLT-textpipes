#!/usr/bin/bash
flist=$1

set -euo pipefail
flistname=$(basename $1)
bucketname=${flistname,,}  # bucket names should be in lowercase
rclone mkdir lumio:${bucketname}

# NB: Multipart uploads can use --transfers * --s3-upload-concurrency * --s3-chunk-size = 32 GB; experimentally it words with 32 GB and fails with 16 GB after some time
# HTTP and LUMIO does not support checksums, also modification time comparison does not work, so we can rely on size only to resume failed downloads
rclone copy --stats-one-line --s3-max-upload-parts 1000 --transfers 16 --size-only --s3-upload-concurrency 16 --s3-chunk-size 128M --checkers 1 --log-level DEBUG --log-file ${flistname}.log --multi-thread-streams 1 --http-url https://data.commoncrawl.org/ :http: lumio:${bucketname} --files-from $flist   --no-traverse --http-no-head
