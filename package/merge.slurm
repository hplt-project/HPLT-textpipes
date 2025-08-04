#!/bin/bash

#SBATCH --job-name=merge
#SBATCH --partition=small
#SBATCH --account=project_462000131
#SBATCH --time=72:00:00
#SBATCH --mail-type=FAIL
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=32
#SBATCH --mem-per-cpu=2G

set -x;

if [ -z "${1}" -o ! -d "{$1}" ]; then
  echo "merge.slurm: missing or invalid directory argument; exit." >&2;
  exit 1;
fi
INPUT=$(realpath --strip ${1});
TARGET=${INPUT%/*/*}/sorted/${INPUT##*/};

CORES=${SLURM_CPUS_ON_NODE:-8};

PYTHON=/projappl/project_465001890/oe/python;
if [ ! -f ${PYTHON}/bin/activate ]; then
  echo "prepare.slurm: invalid Python environment ${PYTHON}; exit." >&2;
  exit 1;
fi

LC_ALL=C;
locale;

echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] merge.slurm: running in $(pwd);"
echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] merge.slurm: processing: ${INPUT};"
echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] merge.slurm: into: ${TARGET};"
echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] merge.slurm: with ${CORES} cores."
echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] merge.slurm: process environment:"
env;
echo;

module reset;
module load cray-python;
source ${PYTHON}/python/bin/activate;

PACKAGE=/scratch/project_462000827/oe/textpipes/package;
cd ${PACKAGE};
python ${PACKAGE}/merge.py --cores ${CORES} --size $[128 * 1024 ** 3] ${@};

echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] merge.slurm: all processing complete."
exit 0;
