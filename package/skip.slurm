#!/bin/bash

#SBATCH --job-name=skip
#SBATCH --partition=small
#SBATCH --account=project_465001890
#SBATCH --time=72:00:00
#SBATCH --mail-type=FAIL
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=128
#SBATCH --mem-per-cpu=2G

set -x;

if [ -z "${1}" -o ! -d "${1}" ]; then
  echo "skip.slurm: missing or invalid directory argument; exit." >&2;
  exit 1;
fi
INPUT=$(realpath --strip ${1});
if [ -z "${2}" ]; then
  echo "skip.slurm: missing second argument (bin index); exit." >&2;
  exit 1;
fi
BIN=${2};

CORES=${SLURM_CPUS_ON_NODE:-8};

LC_ALL=C;
locale;

PYTHON=/appl/local/openeurollm/training/catalogue/python;
if [ ! -f ${PYTHON}/bin/activate ]; then
  echo "prepare.slurm: invalid Python environment ${PYTHON}; exit." >&2;
  exit 1;
fi
module reset;
module load cray-python;
source ${PYTHON}/bin/activate;

echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] skip.slurm: running in $(pwd);"
echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] skip.slurm: processing: ${INPUT};"
echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] skip.slurm: into: ${TARGET};"
echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] skip.slurm: with ${CORES} cores."
echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] skip.slurm: process environment:"
env;
echo;

PACKAGE=/projappl/project_465001890/oe/textpipes/package;

/usr/bin/time -v python -c "
import sys;
sys.path.append(\"${PACKAGE}\");
import merge;
merge.skip(\"${INPUT}\", ${BIN}, cores = ${CORES});
";

echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] merge.slurm: all processing complete."
exit 0;
