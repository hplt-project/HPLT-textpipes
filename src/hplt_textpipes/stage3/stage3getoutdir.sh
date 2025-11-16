#!/bin/bash

FIN=$1
OUTDIR=$2
echo $FIN | sed -r "s@^.*/([^/]+)/html/([0-9]+)/text.zst@${OUTDIR%/}/\1/pool/\2@"
