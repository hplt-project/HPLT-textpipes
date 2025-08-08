#!/bin/bash

#SBATCH --job-name=merge
#SBATCH --partition=small
#SBATCH --account=project_465001890
#SBATCH --time=72:00:00
#SBATCH --mail-type=FAIL
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=32
#SBATCH --mem-per-cpu=2G

set -x;

if [ -z "${1}" -o ! -d "${1}" ]; then
  echo "merge.slurm: missing or invalid directory argument; exit." >&2;
  exit 1;
fi
INPUT=$(realpath --strip ${1});
TARGET=${INPUT%/*/*}/sorted/${INPUT##*/};
if [ ! -d "${TARGET}" ]; then mkdir -p ${TARGET}; fi
if [ ! -d "${TARGET}" ]; then
  echo "merge.slurm: invalid target directory ${TARGET}; exit." >&2;
  exit 1;
fi

CORES=${SLURM_CPUS_ON_NODE:-8};

LC_ALL=C;
locale;

PYTHON=/projappl/project_465001890/oe/python;
if [ ! -f ${PYTHON}/bin/activate ]; then
  echo "prepare.slurm: invalid Python environment ${PYTHON}; exit." >&2;
  exit 1;
fi
module reset;
module load cray-python;
source ${PYTHON}/bin/activate;

echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] merge.slurm: running in $(pwd);"
echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] merge.slurm: processing: ${INPUT};"
echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] merge.slurm: into: ${TARGET};"
echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] merge.slurm: with ${CORES} cores."
echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] merge.slurm: process environment:"
env;
echo;

PACKAGE=/projappl/project_465001890/oe/textpipes/package;
if [ -z "${2}" ]; then
  MIN=0;
  MAX=11;    
else
  MIN=${2};
  MAX=$[${2} + 1];    
fi

python ${PACKAGE}/merge.py \
  --cores ${CORES} --size $[128 * 1024 ** 3] \
  --min ${MIN} --max ${MAX} \
  --target ${TARGET} ${INPUT};

echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] merge.slurm: all processing complete."
exit 0;
