#!/bin/bash

# Exit if any of the subcommands returns errors.
set -eo pipefail

BATCH_ID=$1
cd $BATCH_ID

if [ -z "$S3_OUTPUT_PREFIX" ]; then
  echo "Error: S3_OUTPUT_PREFIX must be set"
  exit 1
fi

SCRIPTS_DIR=$(dirname $(realpath ${BASH_SOURCE[0]}))

# create named pipes for the output files
OUTPUT_DIR=output
mkdir -p $OUTPUT_DIR
mkfifo $OUTPUT_DIR/html.zst
mkfifo $OUTPUT_DIR/pdf.warc.gz

# Create named pipes for the input files.
# We also keep a list of input file paths so that they can be passed
# to warc2text in the same order as they are streamed.
while read -r SOURCEFILE; do
  INPUTFILE=${SOURCEFILE#s3://}
  mkdir -p $(dirname "$INPUTFILE")
  mkfifo "$INPUTFILE"
  echo "$INPUTFILE"
done < paths > inputfiles.txt

# Run a single input streaming process per batch in the background.
# s3cmd get's are executed sequentially.
while read -r SOURCEFILE; do
    s3cmd get "$SOURCEFILE" - > "${SOURCEFILE#s3://}"
done < paths &

xargs -d '\n' \
warc2text --encoding-errors replace -f html,metadata --jsonl --compress zstd \
          --pdfpass $OUTPUT_DIR/pdf --robotspass $OUTPUT_DIR/robotstxt \
          --compress-level 9 --skip-text-extraction --classifier skip \
          --url-filters $SCRIPTS_DIR/url-filter-list.optimised \
          -o $OUTPUT_DIR \
< inputfiles.txt 2> warc2text.log &

# stream the PDF and HTML output
s3cmd --progress put - $S3_OUTPUT_PREFIX/$BATCH_ID/pdf.warc.gz \
      < $OUTPUT_DIR/pdf.warc.gz &> upload.pdf.log &
s3cmd --progress put - $S3_OUTPUT_PREFIX/$BATCH_ID/html.zst \
      < $OUTPUT_DIR/html.zst &> upload.html.log

# put the smaller output files to S3 without streaming
s3cmd --progress put $OUTPUT_DIR/metadata.zst \
      $S3_OUTPUT_PREFIX/$BATCH_ID/metadata.zst &> upload.metadata.log
s3cmd --progress put $OUTPUT_DIR/robotstxt.warc.gz \
      $S3_OUTPUT_PREFIX/$BATCH_ID/robotstxt.warc.gz &> upload.robotstxt.log

# delete the input and output files
while read -r INPUTFILE; do
  rm -rf "${INPUTFILE%%/*}";
done < inputfiles.txt
rm -rf $OUTPUT_DIR
