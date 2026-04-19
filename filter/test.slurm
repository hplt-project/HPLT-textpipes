#!/bin/bash

#
# for i in jaume/*_*/; do echo sbatch ../oe/textpipes/filter/test.slurm $i; done > test.slurm
# ../oe/textpipes/trickle --start --limit 160 test.slurm
# while true; do ../oe/textpipes/trickle --limit 160 test.slurm; sleep 60; done
#

#SBATCH --job-name=test
#SBATCH --output=log/%x-%j.out
#SBATCH --partition=small
#SBATCH --account=project_465002530
#SBATCH --time=12:00:00
#SBATCH --mail-type=FAIL
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=128
#SBATCH --mem-per-cpu=2G

set -x

BASE=/scratch/project_465002259;

if [ -z "${1}" ]; then
  echo "test.slurm: missing argument (input directory); exit." >&2;
  exit 1;
fi
if [ ! -d ${1} ]; then
  echo "test.slurm: invalid input directory ${1}; exit." >&2;
  exit 1;
fi
input=$(realpath --strip ${1});

CORES=${SLURM_CPUS_ON_NODE:-4};

FLASH=/flash/project_465002259;
if [ ! -d ${FLASH}/${USER} ]; then
  mkdir ${FLASH}/${USER};
fi
if [ ! -d ${FLASH}/${USER} ]; then
  echo "test.slurm: invalid flash directory ${FLASH}; exit." >&2;
  exit 1;
fi

PYTHON=${BASE}/oe/local;
if [ ! -f ${PYTHON}/bin/activate ]; then
  echo "duplicates.slurm: invalid Python environment ${PYTHON}; exit." >&2;
  exit 1;
fi
module reset;
module load LUMI parallel cray-python;
source ${PYTHON}/bin/activate;

CODE=${BASE}/oe/textpipes;
export CODE;
if [ ! -f ${CODE}/tools/zstdconcat.py ]; then
  echo "test.slurm: missing source code in ${BASE}/oe; exit." >&2;
  exit 1;
fi

echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] test.slurm: root in $(pwd);"
echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] test.slurm: processing ${input};"
echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] test.slurm: with ${CORES} cores."

function run() {
  local file=${1};
  local command="${CODE}/tools/zstdconcat.py --cores 2 --mode bytes ${file}";
  eval "${command} > /dev/null";
  if [ ${?} -eq 0 ]; then
    echo "test.slurm: successfully read ${file}." >&2;
  else
    echo "test.slurm: invalid input file ${file}." >&2;
  fi
} # run()
export -f run;

PARALLEL="parallel --plain --will-cite";
PARALLEL="${PARALLEL} --group --jobs $[${CORES} / 2]";

#
# finally, iterate through all the files in this directory
#
find ${input} -mindepth 1 -maxdepth 1 -name \*.jsonl.zst \
| eval ${PARALLEL} run {} ${target};
     
echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] test.slurm: all processing complete."
exit 0;
