#!/bin/bash
P=$1
for x in `seq 5`; do 
rclone -P --transfers 8 --stats-one-line copy lumio:$P $P --include "text.zst" --include "metadata.zst" --include "robotstxt.warc.gz" --include "lang.zst"
done
