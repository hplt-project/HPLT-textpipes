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
while read CRAWL_PATH; do
  echo $CRAWL_PATH
  CRAWL_NAME=$(basename "$CRAWL_PATH")
  s3cmd ls -r "$CRAWL_PATH" \
  | sed 's|^.*s3://|s3://|' | grep '.*\.warc\.gz$' > $CRAWL_NAME.crawl.txt
  N_FILES=$(wc -l $CRAWL_NAME.crawl.txt | cut -d' ' -f 1)
  shuf $CRAWL_NAME.crawl.txt \
  | split -a 4 --numeric-suffixes=1 -l $BATCH_SIZE - batch.

# put the batches into separate directories
  mkdir -p $CRAWL_NAME
  for BATCH_FILE in batch.*; do
    BATCH_ID=$((10#${BATCH_FILE##batch.}))
    mkdir $CRAWL_NAME/$BATCH_ID
    mv $BATCH_FILE $CRAWL_NAME/$BATCH_ID/paths
    echo "$CRAWL_NAME/$BATCH_ID" >> tasks
  done

done

rm *.crawl.txt

# shuffle the list of batch directories and split it over nodes
# NOTE: if using more than 100 nodes, change the -a parameter value
shuf tasks | split -a 2 -d -n r/$N_NODES - tasks.
rm tasks

# remove the leading zeros from the task file names
for FILENAME in tasks.*; do
  FILE_IDX=${FILENAME##tasks.}
  if [ "$FILE_IDX" != "$((10#$FILE_IDX))" ]; then
    mv tasks.$FILE_IDX tasks.$((10#$FILE_IDX))
  fi
done
