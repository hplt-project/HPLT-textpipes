#!/bin/bash

#SBATCH --job-name=bin
#SBATCH --output=log/%x-%j.out
#SBATCH --partition=small
#SBATCH --account=project_465001890
#SBATCH --time=72:00:00
#SBATCH --mail-type=FAIL
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=128
#SBATCH --mem-per-cpu=2G

#
# for i in 9 8 7 6 5; do sbatch bin.slurm cleaned/eng_Latn $i; done
#

set -x;

if [ -z "${1}" -o ! -d "${1}" ]; then
  echo "bin.slurm: missing or invalid directory argument; exit." >&2;
  exit 1;
fi
input=$(realpath --strip ${1});
if [ -z "${2}" ]; then
  echo "bin.slurm: missing second argument (bin index); exit." >&2;
  exit 1;
fi
bin=${2};

CORES=${SLURM_CPUS_ON_NODE:-8};

LC_ALL=C;
locale;

PYTHON=/appl/local/openeurollm/training/catalogue/python;
if [ ! -f ${PYTHON}/bin/activate ]; then
  echo "bin.slurm: invalid Python environment ${PYTHON}; exit." >&2;
  exit 1;
fi
module reset;
module load cray-python;
source ${PYTHON}/bin/activate;

echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] bin.slurm: running in $(pwd);"
echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] bin.slurm: processing: ${input};"
echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] bin.slurm: with ${CORES} cores."
echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] bin.slurm: process environment:"
env;
echo;

PACKAGE=${HOME}/src/textpipes/package;

/usr/bin/time -v python -c "
import sys;
sys.path.append(\"${PACKAGE}\");
import merge;
merge.skip(\"${input}\", ${bin}, cores = ${CORES});
";

echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] bin.slurm: all processing complete."
exit 0;
