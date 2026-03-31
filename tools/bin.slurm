#!/bin/bash

#
# for i in pool/wide000*; do echo sbatch ../oe/textpipes/tools/bin.slurm $i; done
# for i in pool/CC-*; do sbatch /scratch/project_465002259/oe/textpipes/tools/bin.slurm $i; done
#

#SBATCH --job-name=bin
#SBATCH --output=log/%x-%j.out
#SBATCH --partition=small
#SBATCH --account=project_465002259
#SBATCH --time=72:00:00
#SBATCH --mail-type=FAIL
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem-per-cpu=8G

set -x

BASE=/scratch/project_465002259;

if [ -z "${1}" ]; then
  echo "bin.slurm: missing argument (input directory); exit." >&2;
  exit 1;
fi
if [ ! -d ${1} ]; then
  echo "bin.slurm: invalid input directory ${1}; exit." >&2;
  exit 1;
fi
input=$(realpath --strip ${1});

if [ -z "${2}" ]; then
  echo "bin.slurm: missing argument (target directory); exit." >&2;
  exit 1;
fi
if [ ! -d ${2} ]; then
  echo "bin.slurm: invalid target directory ${2}; exit." >&2;
  exit 1;
fi
target=$(realpath --strip ${2});

CORES=${SLURM_CPUS_ON_NODE:-4};

PYTHON=${BASE}/oe/local;
if [ ! -f ${PYTHON}/bin/activate ]; then
  echo "bin.slurm: invalid Python environment ${PYTHON}; exit." >&2;
  exit 1;
fi
module reset;
module load LUMI parallel cray-python;
source ${PYTHON}/bin/activate;

CODE=${BASE}/oe/textpipes;
export CODE;
if [ ! -f ${CODE}/tools/zstdconcat.py ]; then
  echo "bin.slurm: missing source code in ${BASE}/oe; exit." >&2;
  exit 1;
fi

echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] bin.slurm: root in $(pwd);"
echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] bin.slurm: processing ${input};"
echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] bin.slurm: with ${CORES} cores."

#
# finally, iterate through all the batches in this crawl
#
batches=$(find ${input} -type d -mindepth 1 -maxdepth 1);
${CODE}/tools/zstdconcat.py --trace --cores ${CORES} --bin ${target} ${batches}

echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] bin.slurm: all processing complete."
exit 0;
