#!/bin/bash

for lang in */
do
	echo ${lang}
	lang2=${lang%*/} 
	for shard in ${lang}*jsonl.zst
	do
	     echo "https://data.hplt-project.org/two/cleaned/${shard}" >> ${lang2}_map.txt
	  done
	echo "<li><a href=\"https://data.hplt-project.org/two/cleaned/${lang2}_map.txt\">${lang2}</a></li>" >> lang_list.html
done


