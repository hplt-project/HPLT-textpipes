#!/bin/bash

#
# for i in pool/wide000*; do echo sbatch ../oe/textpipes/tools/morsel.slurm $i; done
# for i in pool/CC-*; do sbatch /scratch/project_465002259/oe/textpipes/tools/morsel.slurm $i; done
#

#SBATCH --job-name=morsel
#SBATCH --output=log/%x-%j.out
#SBATCH --partition=small
#SBATCH --account=project_465002259
#SBATCH --time=12:00:00
#SBATCH --mail-type=FAIL
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=128
#SBATCH --mem-per-cpu=2G

set -x

BASE=/scratch/project_465002259;

if [ -z "${1}" ]; then
  echo "morsel.slurm: missing argument (input directory); exit." >&2;
  exit 1;
fi
if [ ! -d ${1} ]; then
  echo "morsel.slurm: invalid input directory ${1}; exit." >&2;
  exit 1;
fi
input=$(realpath --strip ${1});

if [ -z "${2}" ]; then
  echo "morsel.slurm: missing argument (target directory); exit." >&2;
  exit 1;
fi
if [ ! -d ${2} ]; then
  echo "morsel.slurm: invalid target directory ${2}; exit." >&2;
  exit 1;
fi
target=$(realpath --strip ${2});

CORES=${SLURM_CPUS_ON_NODE:-4};

PYTHON=${BASE}/oe/local;
if [ ! -f ${PYTHON}/bin/activate ]; then
  echo "morsel.slurm: invalid Python environment ${PYTHON}; exit." >&2;
  exit 1;
fi
module reset;
module load LUMI parallel cray-python;
source ${PYTHON}/bin/activate;

CODE=${BASE}/oe/textpipes;
export CODE;
if [ ! -f ${CODE}/tools/zstdconcat.py ]; then
  echo "morsel.slurm: missing source code in ${BASE}/oe; exit." >&2;
  exit 1;
fi

echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] morsel.slurm: root in $(pwd);"
echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] morsel.slurm: processing ${input};"
echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] morsel.slurm: with ${CORES} cores."

function run() {
  local batch=${1};
  if [ ! -d ${batch} ]; then
    echo "invalid input batch directory: ${batch}; skip." >&2;
    return 1;
  fi
  local target=${2};
  if [ ! -d ${target} ]; then
    echo "invalid output target directory: ${target}; skip." >&2;
    return 1;
  fi
  local morsel="${target}/$(basename ${batch})"
  if [ -d ${morsel} ]; then
    echo "morsel target directory exists: ${morsel}; skip." >&2;
    return 1;
  fi
  mkdir ${morsel};
  local command="${CODE}/tools/zstdconcat.py --trace --cores 4 --bin ${morsel} ${batch}";
  eval ${command};
} # run()
export -f run;

PARALLEL="parallel --plain --will-cite";
PARALLEL="${PARALLEL} --group --jobs $[${CORES} / 4]";

#
# finally, iterate through all the batches in this crawl
#
find ${input} -mindepth 1 -maxdepth 1 -type d \
| eval ${PARALLEL} run {} ${target};
     
echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] morsel.slurm: all processing complete."
exit 0;
