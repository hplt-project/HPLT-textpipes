#!/bin/bash

#
#
#

#SBATCH --job-name=pack
#SBATCH --output=log/%x-%j.out
#SBATCH --partition=small
#SBATCH --account=project_465001890
#SBATCH --time=72:00:00
#SBATCH --mail-type=FAIL
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=32
#SBATCH --mem-per-cpu=2G

set -x;

if [ -z "${1}" ]; then
  echo "pack.slurm: missing command line argument (directory); exit." >&2;
  exit 1;
fi
if [ ! -d ${1} ]; then
  echo "pack.slurm: invalid input directory ${1}; exit." >&2;
  exit 1;
fi
input=$(realpath --strip ${1});


if [ -z "${1}" ]; then
  echo "pack.slurm: missing command line argument (directory); exit." >&2;
  exit 1;
fi
if [ ! -d ${1} ]; then
  echo "pack.slurm: invalid input directory ${1}; exit." >&2;
  exit 1;
fi
input=$(realpath --strip ${1});

if [ -z "${2}" ]; then
  echo "pack.slurm: missing command line argument (bin); exit." >&2;
  exit 1;
fi
bin=${2};
output=$(dirname ${input})/000${bin}.jsonl.zst

CORES=${SLURM_CPUS_ON_NODE:-8};

FLASH=/flash/project_465001890;
if [ ! -d ${FLASH}/${USER} ]; then
  mkdir ${FLASH}/${USER};
fi
if [ ! -d ${FLASH}/${USER} ]; then
  echo "pack.slurm: invalid flash directory ${FLASH}; exit." >&2;
  exit 1;
fi

module reset;

echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] pack.slurm: root in $(pwd);"
echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] pack.slurm: processing ${input};"
echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] pack.slurm: for bin #${bin};"
echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] pack.slurm: writing to ${output};"
echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] pack.slurm: with ${CORES} cores."

export LC_ALL=C;
locale;

sort --temporary-directory=${FLASH}/${USER} \
  --parallel=$[${CORES} / 2] --buffer-size=60G \
  --key=1,1 --numeric-sort --reverse \
  ${input}/*.jsonl.${bin}.zst \
| zstd --verbose --no-progress -z -10 -T$[${CORES} / 2] -f - -o ${output}

echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] pack.slurm: all processing complete."
exit 0;
