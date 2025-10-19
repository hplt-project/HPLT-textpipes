#! /bin/bash

# Filter an arbitrary HPLT dataset by removing documents coming from domains you don't want to have

LANG=${1}

mkdir -p ${LANG}

# This is the LUMI path:
for el in /appl/local/openeurollm/training/catalogue/hplt/3.0/sorted/${LANG}/*.jsonl.zst
do
        echo ${el}
        if [ $LANG = 'lij_Latn' ]; then
                zstdcat ${el} | jq -c 'select(."u" | contains("eodishasamachar.com") or contains("prameyanews.com") or contains("biswabijayeenewsodisha.com") or contains("hindustantimes.com") | not)' | zstd -10 > ${LANG}/$(basename "$el")
        elif [ $LANG = 'szl_Latn' ]; then
                zstdcat ${el} | jq -c 'select(."u" | contains("serbske-nowiny.de") or contains("rozhlad.de") or contains("nowycasnik.de") | not)' | zstd -10 > ${LANG}/$(basename "$el")
        fi
done
