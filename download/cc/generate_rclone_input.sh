CC_PATHS=$1
OUT_DIR=$(basename "$CC_PATHS")
OUT_DIR="${OUT_DIR%.*}"
mkdir -p  $OUT_DIR
for x in `cat $CC_PATHS`; do zcat warcpaths/$x | sed -r 's!((.*)/[^/]+)$!https://data.commoncrawl.org/\1\t  dir='${OUT_DIR}/'\2!'; done |shuf|tr '\t' '\n'
