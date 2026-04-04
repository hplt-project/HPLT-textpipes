#!/bin/bash

#
# for i in jaume/*_*/; do echo sbatch ../oe/textpipes/filter/duplicates.slurm $i; done \
# | egrep -v "rus_Cyrl" > duplicates.slurm
# ../oe/textpipes/trickle --start --limit 160 duplicates.slurm
# while true; do ../oe/textpipes/trickle --limit 160 duplicates.slurm; sleep 60; done
#

#SBATCH --job-name=duplicates
#SBATCH --output=log/%x-%j.out
#SBATCH --partition=small
#SBATCH --account=project_465002530
#SBATCH --time=12:00:00
#SBATCH --mail-type=FAIL
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem-per-cpu=4G

set -x

BASE=/scratch/project_465002259;

if [ -z "${1}" ]; then
  echo "duplicates.slurm: missing argument (input directory); exit." >&2;
  exit 1;
fi
if [ ! -d ${1} ]; then
  echo "duplicates.slurm: invalid input directory ${1}; exit." >&2;
  exit 1;
fi
input=$(realpath --strip ${1});
output=${input}/.duplicate.ids;

CORES=${SLURM_CPUS_ON_NODE:-8};

PYTHON=${BASE}/oe/local;
if [ ! -f ${PYTHON}/bin/activate ]; then
  echo "duplicates.slurm: invalid Python environment ${PYTHON}; exit." >&2;
  exit 1;
fi
module reset;
module load LUMI parallel cray-python;
source ${PYTHON}/bin/activate;

CODE=${BASE}/oe/textpipes;
export CODE;
if [ ! -f ${CODE}/filter/duplicates.py ]; then
  echo "duplicates.slurm: missing source code in ${BASE}/oe; exit." >&2;
  exit 1;
fi

echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] duplicates.slurm: root in $(pwd);"
echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] duplicates.slurm: processing ${input};"
echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] duplicates.slurm: with ${CORES} cores."

#
# finally, process our input directory, reading all files sequentially
#
${CODE}/filter/duplicates.py ${input} > ${output};

echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] duplicates.slurm: all processing complete."
exit 0;
