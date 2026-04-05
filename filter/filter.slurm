#!/bin/bash

#
# assumes a fixed directory layout, where .annotations. are
# organized in parallel directories to the .input.
#

#SBATCH --job-name=filter
#SBATCH --output=log/%x-%j.out
#SBATCH --partition=small
#SBATCH --account=project_465002530
#SBATCH --time=72:00:00
#SBATCH --mail-type=FAIL
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem-per-cpu=4G

set -x

BASE=/scratch/project_465002259;

if [ -z "${1}" ]; then
  echo "filter.slurm: missing argument (input directory); exit." >&2;
  exit 1;
fi
if [ ! -d ${1} ]; then
  echo "filter.slurm: invalid input directory ${1}; exit." >&2;
  exit 1;
fi
input=$(realpath --strip ${1});

name=$(basename ${input});
blocked=$(realpath --strip ${input}/../../blocked/${name});
if [ -d ${blocked} ]; then
  echo "filter.slurm: directory exists: ${blocked}; exit." >&2;
  exit 1;
fi
mkdir -p ${blocked};
noisy=$(realpath --strip ${input}/../../noisy/${name});
if [ -d ${noisy} ]; then
  echo "filter.slurm: directory exists: ${noisy}; exit." >&2;
  exit 1;
fi
mkdir -p ${noisy};
clean=$(realpath --strip ${input}/../../clean/${name});
if [ -d ${clean} ]; then
  echo "filter.slurm: directory exists: ${clean}; exit." >&2;
  exit 1;
fi
mkdir -p ${clean};

duplicates=${input}/.duplicate.ids;
if [ -f ${duplicates} ]; then
  duplicates="--skip ${duplicates}";
else
  unset duplicates;
fi
annotations=$(realpath --strip ${input}/../../annotations);
if find ${annotations}/*/${name} -name \*.jsonl.zst -quit; then
  annotations=${annotations}/*/${name};
else
  unset annotations;
fi
					 
CORES=${SLURM_CPUS_ON_NODE:-8};

PYTHON=${BASE}/oe/local;
if [ ! -f ${PYTHON}/bin/activate ]; then
  echo "filter.slurm: invalid Python environment ${PYTHON}; exit." >&2;
  exit 1;
fi
module reset;
module load LUMI parallel cray-python;
source ${PYTHON}/bin/activate;

CODE=${BASE}/oe/textpipes;
export CODE;
if [ ! -f ${CODE}/filter/filter.py ]; then
  echo "filter.slurm: missing source code in ${BASE}/oe; exit." >&2;
  exit 1;
fi

echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] filter.slurm: root in $(pwd);"
echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] filter.slurm: processing ${input};"
echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] filter.slurm: with ${CORES} cores."

#
# finally, process our input directory, reading all files sequentially
#
eval ${CODE}/filter/filter.py --trace --cores ${CORES} ${duplicates} \
  --blocked ${blocked} --noisy ${noisy} --clean ${clean} \
  ${input} ${annotations};

echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] filter.slurm: all processing complete."
exit 0;
