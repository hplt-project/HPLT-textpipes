#!/usr/bin/bash
NJOBS=$1
FPATHS=$2
BATCH_GB=$3
OUTDIR=$4
LOCALSCRIPT=$5

LOGDIR=logs_$(date +%Y-%m-%d-%H-%M-%S)
mkdir $LOGDIR

getoutdir() {
    local FIN=$1
    local OUTDIR=$2
    echo $FIN | sed -r "s@^.*/([^/]+)/html/([0-9]+)/text.zst@${OUTDIR%/}/\1/pool/\2@"
}

export -f getoutdir

filter_done() {
  while read -r FIN; do
    fdone=$(getoutdir "${FIN}" "${OUTDIR}")/.done
    if [ ! -f "$fdone" ]; then
      echo "$line"
    else
      echo "$line already processed, skipped" 1>&2
    fi
  done
}

cat $FPATHS | filter_done | python -m hplt_textpipes.stage2.batch_htmls_prtpy $BATCH_GB | parallel --colsep ' ' --eta --joblog $LOGDIR/joblog -j $NJOBS \
  "{ srun --quiet --nodes=1 --cpus-per-task=128 stage3local_batch.sh $LOCALSCRIPT $OUTDIR {}; } &>$LOGDIR/{#}.out"
