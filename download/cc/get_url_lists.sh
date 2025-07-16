mkdir -p warcpaths
sed -r 's!^(.*)$!wget http://data.commoncrawl.org/crawl-data/\1/warc.paths.gz -O warcpaths/\1.gz!' < cc_list_latest.txt >cc_list_latest_download.sh
bash cc_list_latest_download.sh
for x in warcpaths/*gz; do
echo "Converting $x to a filelist for rclone..."
zcat $x | sed -r 's!((.*)/[^/]+)$!\1!' >${x%.*}.lst
done
