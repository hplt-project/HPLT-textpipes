#!/bin/bash

print_usage() {
  echo "usage: $0 N_NODES BATCH_SIZE < LIST_OF_CRAWL_PATHS"
}

N_NODES=$1
BATCH_SIZE=$2

if [ -z $N_NODES ] || [ -z $BATCH_SIZE ]; then
  print_usage
  exit 1
fi

# for each crawl, find the files, shuffle them and split into batches
START_IDX=1
while read CRAWL_PATH; do
  echo $CRAWL_PATH
  CRAWL_NAME=$(basename "$CRAWL_PATH")
  s3cmd ls -r "$CRAWL_PATH" \
  | sed 's|^.*s3://|s3://|' | grep '.*\.warc\.gz$' > $CRAWL_NAME.crawl.txt
  N_FILES=$(wc -l $CRAWL_NAME.crawl.txt | cut -d' ' -f 1)
  shuf $CRAWL_NAME.crawl.txt \
  | split -a 4 --numeric-suffixes=$START_IDX -l $BATCH_SIZE - batch.
  # the meaning of the line below: START_IDX += ceiling(N_FILES / BATCH_SIZE)
  START_IDX=$(( $START_IDX + ($N_FILES+$BATCH_SIZE-1) / $BATCH_SIZE ))
done

rm *.crawl.txt

# put the batches into separate directories
for BATCH_FILE in batch.*; do
  BATCH_ID=$((10#${BATCH_FILE##batch.}))
  mkdir $BATCH_ID
  mv $BATCH_FILE $BATCH_ID/paths
  echo $BATCH_ID >> tasks
done

# shuffle the list of batch directories and split it over nodes
# NOTE: if using more than 10 nodes, change the -a parameter value
shuf tasks | split -a 1 -d -n r/$N_NODES - tasks.
rm tasks
