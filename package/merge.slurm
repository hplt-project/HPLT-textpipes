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
input=$(realpath --strip ${1});
if [ -z "${target}" ]; then target=${input%/*/*}/sorted/${input##*/}; fi
if [ ! -d "${target}" ]; then
  echo "merge.slurm: invalid target directory ${target}; exit." >&2;
  exit 1;
fi

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

echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] merge.slurm: running in $(pwd);"
echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] merge.slurm: processing: ${input};"
echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] merge.slurm: into: ${target};"
echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] merge.slurm: with ${CORES} cores."
echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] merge.slurm: process environment:"
env;
echo;

PACKAGE=${HOME}/src/textpipes/package;
if [ -z "${2}" ]; then
  min=0;
  max=11;    
else
  min=${2};
  max=$[${2} + 1];
  #
  # see whether we have smaller per-bin files available
  #
  if [ -n "$(echo ${input}/*.jsonl.${min}.zst)" ]; then
    suffix="jsonl.${min}.zst";
  else      
    suffix="jsonl.s.zst";
  fi
fi

python ${PACKAGE}/merge.py \
  --cores ${CORES} --size $[128 * 1024 ** 3] \
  --suffix ${suffix} \
  --min ${min} --max ${max} \
  --target ${target} ${input};

echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] merge.slurm: all processing complete."
exit 0;
