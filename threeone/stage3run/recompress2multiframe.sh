#!/bin/bash
FIN=$1  # should end with .zst
TMP=$2
FS=512K  # frame size
COMPRESSION=3

TMP=$TMP/${FIN%/.zst}
mkdir -p $(dirname $TMP)
rm -rf $TMP

rm -rf ${FIN%/.zst}.${FS}.zst
time -p zstd -o $TMP -d $FIN && time -p t2sz $TMP -s ${FS} -l $COMPRESSION -o ${FIN%/.zst}.${FS}.zst && rm $TMP

