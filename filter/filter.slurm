#!/bin/bash

#
# assumes a fixed directory layout, where “annotations” are organized in
# parallel directories to the .input.  to remove duplicate “id” values and
# record filtering statistics, another parallel directory tree “filter” is
# assumed, with per-language input files “duplicate.ids” and an output file
# “filter.json”.  an optional “--batch” parameter (following the .input.
# parameter), with a numeric argument, can be used to select a subset of
# input files and write outputs into distinct, per-batch subdirectories, e.g.
#
# for i in $(seq 0 9); do
#   sbatch ../oe/textpipes/filter/filter.slurm jaume/eng_Latn --batch $i --separate;
# done
#
# remaining parameters following “--batch” will be passed onto “filter.py”
# verbatim, for example to activate crawl source separation or tracing.
#


#SBATCH --job-name=filter
#SBATCH --output=log/%x-%j.out
#SBATCH --partition=small
#SBATCH --account=project_465002530
#SBATCH --time=72:00:00
#SBATCH --mail-type=FAIL
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem-per-cpu=32G

BASE=/scratch/project_465002891;

if [ -z "${1}" ]; then
  echo "filter.slurm: missing argument (input directory); exit." >&2;
  exit 1;
fi
if [ ! -d ${1} ]; then
  echo "filter.slurm: invalid input directory ${1}; exit." >&2;
  exit 1;
fi
input=$(realpath --strip ${1});
shift;

if [ "${1}" = "--batch" ]; then
  pattern="--pattern *${2}.jsonl.zst";
  suffix=".${2}";
  shift 2;
fi

name=$(basename ${input});
blocked=$(realpath --strip ${input}/../../blocked/${name})${suffix};
if [ -d ${blocked} ]; then
  echo "filter.slurm: directory exists: ${blocked}; exit." >&2;
  exit 1;
fi
mkdir -p ${blocked};
noisy=$(realpath --strip ${input}/../../noisy/${name})${suffix};
if [ -d ${noisy} ]; then
  echo "filter.slurm: directory exists: ${noisy}; exit." >&2;
  exit 1;
fi
mkdir -p ${noisy};
clean=$(realpath --strip ${input}/../../clean/${name})${suffix};
if [ -d ${clean} ]; then
  echo "filter.slurm: directory exists: ${clean}; exit." >&2;
  exit 1;
fi
mkdir -p ${clean};

filter=$(realpath --strip ${input}/../../filter/${name});
if [ ! -d ${filter} ]; then mkdir -p ${filter}; fi
duplicates=${filter}/duplicate.ids;
if [ -f ${duplicates} ]; then
  duplicates="--skip ${duplicates}";
else
  unset duplicates;
fi

counts=${filter}/filter${suffix}.json;
if [ -f ${counts} ]; then
  echo "filter.slurm: filter statistics exist: ${clean}; exit." >&2;
  exit 1;
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
if [ -n "${annotations}" ]; then
  echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] filter.slurm: with annotations ${annotations};"
fi
echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] filter.slurm: with ${CORES} cores."

#
# finally, process our input directory, reading all files sequentially
#
eval ${CODE}/filter/filter.py --trace --cores ${CORES} ${duplicates} ${pattern} \
  --blocked ${blocked} --noisy ${noisy} --clean ${clean} --counts ${counts} "${@}" \
  ${input} ${annotations};

echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] filter.slurm: all processing complete."
exit 0;
