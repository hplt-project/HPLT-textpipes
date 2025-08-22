#!/bin/bash

#SBATCH --job-name=split
#SBATCH --output=log/%x-%j.out
#SBATCH --partition=small
#SBATCH --account=project_462000131
#SBATCH --time=12:00:00
#SBATCH --mail-type=FAIL
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=64
#SBATCH --mem-per-cpu=2G

#
# for i in lumped/{ara_Arab,fas_Arab,hbs_Cyrl,hbs_Latn,}; do sbatch split.slurm $i; done
#

set -x;

if [ -z "${1}" -o ! -d "${1}" ]; then
  echo "split.slurm: missing or invalid directory argument; exit." >&2;
  exit 1;
fi
input=$(realpath --strip ${1});

CORES=${SLURM_CPUS_ON_NODE:-8};

LC_ALL=C;
locale;

PYTHON=/appl/local/openeurollm/training/catalogue/python;
if [ ! -f ${PYTHON}/bin/activate ]; then
  echo "split.slurm: invalid Python environment ${PYTHON}; exit." >&2;
  exit 1;
fi
module reset;
module load cray-python;
source ${PYTHON}/bin/activate;

echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] split.slurm: running in $(pwd);"
echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] split.slurm: processing: ${input};"
echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] split.slurm: with ${CORES} cores."
echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] split.slurm: process environment:"
env;
echo;

PACKAGE=${HOME}/src/textpipes/package;

/usr/bin/time -v python -c "
import sys;
sys.path.append(\"${PACKAGE}\");
import merge;
merge.split(\"${input}\", cores = ${CORES});
";

echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] split.slurm: all processing complete."
exit 0;
