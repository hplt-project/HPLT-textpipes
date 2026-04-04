#!/bin/bash

#SBATCH --job-name=identify
#SBATCH --output=log/%x-%j.out
#SBATCH --partition=small
#SBATCH --account=project_465002530
#SBATCH --time=12:00:00
#SBATCH --mail-type=FAIL
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=64
#SBATCH --mem-per-cpu=4G

set -x

BASE=/scratch/project_465002259;

if [ -z "${1}" ]; then
  echo "identify.slurm: missing argument (input directory); exit." >&2;
  exit 1;
fi
if [ ! -d ${1} ]; then
  echo "identify.slurm: invalid input directory ${1}; exit." >&2;
  exit 1;
fi
input=$(realpath --strip ${1});

CORES=${SLURM_CPUS_ON_NODE:-4};

FLASH=/flash/project_465002259;
if [ ! -d ${FLASH}/${USER} ]; then
  mkdir ${FLASH}/${USER};
fi
if [ ! -d ${FLASH}/${USER} ]; then
  echo "identify.slurm: invalid flash directory ${FLASH}; exit." >&2;
  exit 1;
fi

module reset;
module load LUMI parallel;

echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] identify.slurm: root in $(pwd);"
echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] identify.slurm: processing ${input};"
echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] identify.slurm: with ${CORES} cores."

function run() {
  local file=${1};
  local target="$(dirname ${file})/.$(basename ${file} .jsonl.zst).ids.zst";
  local sort="sort --temporary-directory=${FLASH}/${USER} --parallel=2 --buffer-size=6G"
  local command="zstdcat --threads=2 ${file} | jq .id | ${sort} | zstd -c -o ${target}";
  eval ${command};
} # run()
export -f run;

PARALLEL="parallel --plain --will-cite";
PARALLEL="${PARALLEL} --group --jobs $[${CORES} / 4]";

#
# finally, iterate through all the files in this directory
#
find ${input} -mindepth 1 -maxdepth 1 -name \*.jsonl.zst \
| eval ${PARALLEL} run {} ${target};
     
echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] identify.slurm: all processing complete."
exit 0;
