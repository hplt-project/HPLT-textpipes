#!/bin/bash

BATCH_ID=$1
cd $BATCH_ID

if [ -z "$S3_OUTPUT_PREFIX" ]; then
  echo "Error: S3_OUTPUT_PREFIX must be set"
  exit 1
fi

# create named pipes for the output files
OUTPUT_DIR=output
mkdir -p $OUTPUT_DIR
mkfifo $OUTPUT_DIR/html.zst
mkfifo $OUTPUT_DIR/pdf.warc.gz

# create named pipes for the input files
cat paths \
  | while read SOURCEFILE; do
      INPUTFILE=${SOURCEFILE#s3://}
      mkdir -p $(dirname $INPUTFILE)
      mkfifo $INPUTFILE
      echo $INPUTFILE
    done > inputfiles.txt

# It is important that warc2text processes the input files in the same order
# in which they are streamed (not necessarily alphabetic!). Hence we prepare
# a list of input files like this instead of using input/*.warc.gz.
readarray -t INPUTFILES < inputfiles.txt

# Run a single input streaming process per batch in the background.
# s3cmd get's are executed sequentially.
cat paths \
  | while read SOURCEFILE; do
    s3cmd get $SOURCEFILE - > ${SOURCEFILE#s3://}
done &

# FIXME where is the url-filter-list?
warc2text --encoding-errors replace -f html,metadata --jsonl --compress zstd \
          --pdfpass $OUTPUT_DIR/pdf --robotspass $OUTPUT_DIR/robotstxt \
          --compress-level 9 --skip-text-extraction --classifier skip \
          --url-filters ../url-filter-list.optimised \
          -o $OUTPUT_DIR ${INPUTFILES[@]} 2> warc2text.log &

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
for INPUTFILE in ${INPUTFILES[@]}; do
  rm -rf ${INPUTFILE%%/*};
done
rm -rf $OUTPUT_DIR
