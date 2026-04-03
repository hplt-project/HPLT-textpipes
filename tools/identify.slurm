#!/bin/bash

#SBATCH --job-name=identify
#SBATCH --output=log/%x-%j.out
#SBATCH --partition=small
#SBATCH --account=project_465002259
#SBATCH --time=12:00:00
#SBATCH --mail-type=FAIL
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=64
#SBATCH --mem-per-cpu=2G

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

if [ -z "${2}" ]; then
  echo "identify.slurm: missing argument (target directory); exit." >&2;
  exit 1;
fi
if [ ! -d ${2} ]; then
  echo "identify.slurm: invalid target directory ${2}; exit." >&2;
  exit 1;
fi
target=$(realpath --strip ${2});

CORES=${SLURM_CPUS_ON_NODE:-4};

PYTHON=${BASE}/oe/local;
if [ ! -f ${PYTHON}/bin/activate ]; then
  echo "identify.slurm: invalid Python environment ${PYTHON}; exit." >&2;
  exit 1;
fi
module reset;
module load LUMI parallel cray-python;
source ${PYTHON}/bin/activate;

echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] identify.slurm: root in $(pwd);"
echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] identify.slurm: processing ${input};"
echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] identify.slurm: with ${CORES} cores."

function run() {
  local file=${1};
  local target="$(dirname ${file})/.$(basename ${file} .jsonl.zst).ids";
  local command="zstcat ${file} | jq .id | sort -u";
  echo ${command} ${target};
} # run()
export -f run;

PARALLEL="parallel --plain --will-cite";
PARALLEL="${PARALLEL} --group --jobs ${CORES}";

#
# finally, iterate through all the files in this directory
#
find ${input} -mindepth 1 -maxdepth 1 -type f -name \*.jsonl.zst \
| eval ${PARALLEL} run {} ${target};
     
echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] identify.slurm: all processing complete."
exit 0;
