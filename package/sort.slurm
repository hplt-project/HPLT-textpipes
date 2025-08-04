#!/bin/bash

#
# for i in eng_Latn/*.jsonl.zst; do echo sbatch sort.slurm $(realpath --strip $i); done > eng.trickle
# trickle --start --limit 180 eng.trickle
# while true; do trickle --limit 180 eng.trickle; sleep 60; done
#

#SBATCH --job-name=prepare
#SBATCH --partition=small
#SBATCH --account=project_465001890
#SBATCH --time=2:00:00
#SBATCH --mail-type=FAIL
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem-per-cpu=4096M

set -x;

if [ -z "${1}" ]; then
  echo "prepare.slurm: missing command line argument (input file); exit." >&2;
  exit 1;
fi
if [ ! -f ${1} ]; then
  echo "prepare.slurm: invalid input file ${1}; exit." >&2;
  exit 1;
fi
INPUT=$(realpath --strip ${1});
OUTPUT=${INPUT%.zst}.s.zst;

CORES=${SLURM_CPUS_ON_NODE:-8};

FLASH=/flash/project_465001890;
if [ ! -d ${FLASH}/${USER} ]; then
  mkdir ${FLASH}/${USER};
fi
if [ ! -d ${FLASH}/${USER} ]; then
  echo "prepare.slurm: invalid flash directory ${FLASH}; exit." >&2;
  exit 1;
fi

PYTHON=/projappl/project_465001890/oe/python;
if [ ! -f ${PYTHON}/bin/activate ]; then
  echo "prepare.slurm: invalid Python environment ${PYTHON}; exit." >&2;
  exit 1;
fi
module reset;
module load cray-python;
source ${PYTHON}/bin/activate;

PACKAGE=/projappl/project_465001890/oe/textpipes/package;
if [ ! -f ${PACKAGE}/prepare.py ]; then
  echo "prepare.slurm: invalid package directory ${PACKAGE}; exit." >&2;
  exit 1;
fi


echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] prepare.slurm: root in $(pwd);"
echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] prepare.slurm: processing ${INPUT};"
echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] prepare.slurm: writing to ${OUTPUT};"
echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] prepare.slurm: with ${CORES} cores."

export LC_ALL=C;
locale;

python ${PACKAGE}/prepare.py ${INPUT} \
| sort --temporary-directory=${FLASH}/${USER} \
    --key=1,1 --numeric-sort --reverse \
    --parallel=${CORES} --buffer-size=60G \
| zstd --verbose --no-progress -z -10 -T${CORES} -f - -o ${OUTPUT}

echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] prepare.slurm: all processing complete."
exit 0;
