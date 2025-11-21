#!/usr/bin/bash
NJOBS=$1
FPATHS=$2
BATCH_GB=$3
OUTDIR=$4
LOCALSCRIPT=$5

LOGDIR=logs_$(date +%Y-%m-%d-%H-%M-%S)
mkdir $LOGDIR

filter_done() {
  while read -r line; do
    FIN=`echo "$line" | sed -r 's/^[0-9]+ +//'`
    fdone=$(stage3getoutdir.sh "${FIN}" "${OUTDIR}")/.done
    if [ ! -f "$fdone" ]; then
      echo "$line"
    else
      echo "$line already processed, skipped" 1>&2
    fi
  done
}

cat $FPATHS | filter_done | python -m hplt_textpipes.stage2.batch_htmls_prtpy $BATCH_GB | parallel --colsep ' ' --eta --joblog $LOGDIR/joblog -j $NJOBS \
  "{ srun --quiet --nodes=1 --cpus-per-task=128 stage3local_batch.sh $LOCALSCRIPT $OUTDIR {}; } &>$LOGDIR/{#}.out"
