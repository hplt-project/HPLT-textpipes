#!/bin/bash
export HPLT_CACHE=/tmp/.cache/hplt
mkdir -p  $HPLT_CACHE
cp -r ~/.cache/hplt/* $HPLT_CACHE/  # we will have 1 load of lid weights run per 100 MB of input data, better avoid loading from disk and use /tmp mapped to RAM
