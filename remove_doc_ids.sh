#!/bin/bash

# Use like this:
# find LANG/ -type f -name scored*.zst | parallel -j30 ./remove_doc_ids.sh {}
# where LANG is a directory with monotexted files

# sanity: exit on all errors and disallow unset environment variables
set -o errexit
set -o nounset

FILE2PROCESS=${1}
NEWNAME=${FILE2PROCESS/scored./}

if [ -f $NEWNAME ]; then
   echo "File ${NEWNAME} exists."
else
   echo ${FILE2PROCESS}
   zstdcat ${FILE2PROCESS} | python3 remove_doc_ids.py | zstd -10 -T2 > ${NEWNAME}
fi

