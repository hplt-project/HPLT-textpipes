#!/bin/bash

#SBATCH --job-name=shard
#SBATCH --output=log/%x-%j.out
#SBATCH --partition=small
#SBATCH --account=project_465002259
#SBATCH --time=48:00:00
#SBATCH --mail-type=FAIL
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=64
#SBATCH --mem-per-cpu=2G

set -x

BASE=/scratch/project_465002259;

if [ -z "${1}" ]; then
  echo "shard.slurm: missing argument (input directory); exit." >&2;
  exit 1;
fi
if [ ! -d ${1} ]; then
  echo "shard.slurm: invalid input directory ${1}; exit." >&2;
  exit 1;
fi
input=$(realpath --strip ${1});

if [ -z "${2}" ]; then
  echo "shard.slurm: missing argument (target directory); exit." >&2;
  exit 1;
fi
if [ ! -d ${2} ]; then
  echo "shard.slurm: invalid target directory ${2}; exit." >&2;
  exit 1;
fi
target=$(realpath --strip ${2});

CORES=${SLURM_CPUS_ON_NODE:-4};

PYTHON=${BASE}/oe/local;
if [ ! -f ${PYTHON}/bin/activate ]; then
  echo "shard.slurm: invalid Python environment ${PYTHON}; exit." >&2;
  exit 1;
fi
module reset;
module load LUMI parallel cray-python;
source ${PYTHON}/bin/activate;

CODE=${BASE}/oe/textpipes;
if [ ! -f ${CODE}/tools/shard.py ]; then
  echo "shard.slurm: missing source code in ${BASE}/oe; exit." >&2;
  exit 1;
fi

echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] shard.slurm: root in $(pwd);"
echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] shard.slurm: processing ${input};"
echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] shard.slurm: into ${target};"
echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] shard.slurm: with ${CORES} cores."

export input target CODE;

function run() {
  local language=${1};
  local output=${target}/${language};
  if [ -d ${output} ]; then
    echo "shard.slurm: target directory exists: ${output}; skip." >&2;
    return 1;
  fi
  mkdir ${output};
  local command="${CODE}/tools/shard.py --cores 4 --target ${output}";
  eval ${command} ${input}/*/${language}/*.zst;
} # run()
export -f run;

PARALLEL="parallel --plain --will-cite";
PARALLEL="${PARALLEL} --group --jobs $[${CORES} / 4]";

  echo $input
  echo $target  

#
# finally, iterate through all the languages in this collection of morsels
#
find ${input} -mindepth 2 -maxdepth 2 -type d -name \*_\* \
| sed 's,^.*/,,g' | sort -u \
| eval ${PARALLEL} run {};

echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] shard.slurm: all processing complete."
exit 0;
