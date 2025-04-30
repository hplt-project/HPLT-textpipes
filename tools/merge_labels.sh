#! /bin/bash

# This script downloads HPLT 2.0 datasets for a specific language and the corresponding register label files.
# Then it merges the files so that the register labels end up added to the main JSONL files.
# IMPORTANT: this script works with the "deduplicated" variant of the HPLT dataset.
# In contrast, the "cleaned variant" contains the same documents as "deduplicated" minus those filtered out by the HPLT cleaning heuristics.
# If you have your own cleaning pipeline, just apply it to the merged files.
# If you would rather rely on the HPLT cleaning heuristics, use the "id" field to remove documents not present in the "cleaned" variant.
# See https://hplt-project.org for more details.

LANG=${1}  # Specify the HPLT language you want, e.g., "nob_Latn"

echo "Creating the language directory..."
mkdir -p ${LANG}
cd ${LANG}

echo "Downloading the main HPLT datasets..."
wget -c -i https://data.hplt-project.org/two/deduplicated/${LANG}_map.txt --show-progress

for el in *.jsonl.zst
  do
        wget -c https://data.hplt-project.org/two/deduplicated/${LANG}/${el}.md5
        echo "Verifying file integrity for ${el}..."
        md5sum -c ${el}.md5
  done


echo "Downloading the HPLT register labels..."
for el in *.jsonl.zst
  do
        wget -c -P labels/ https://data.hplt-project.org/two/register_labels/${LANG}/${el} --show-progress
  done

echo "Merging the register labels into the main files..."

for el in *.jsonl.zst
  do
    echo "Merging ${el}, please wait..."
    paste <(zstd -dc ${el}) <(zstd -dc labels/${el}) | jq -cR 'split("\t") | map(fromjson) | add' | zstd > labels_${el}
  done
  
echo "Merging done! See the jsonl.zstd files with the name starting with 'labels_'"
