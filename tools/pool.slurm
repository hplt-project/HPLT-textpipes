#!/bin/bash

#
#
#

#SBATCH --job-name=pool
#SBATCH --output=log/%x-%j.out
#SBATCH --partition=small
#SBATCH --account=project_465002259
#SBATCH --time=12:00:00
#SBATCH --mail-type=FAIL
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=128
#SBATCH --mem-per-cpu=2048M

BASE=/scratch/project_465002259;

if [ -z "${1}" ]; then
  echo "pool.slurm: missing argument (input directory); exit." >&2;
  exit 1;
fi
if [ ! -d ${1} ]; then
  echo "pool.slurm: invalid input directory ${1}; exit." >&2;
  exit 1;
fi
input=$(realpath --strip ${1});
     
CORES=${SLURM_CPUS_ON_NODE:-8};

PYTHON=${BASE}/oe/local;
if [ ! -f ${PYTHON}/bin/activate ]; then
  echo "pool.slurm: invalid Python environment ${PYTHON}; exit." >&2;
  exit 1;
fi
module reset;
module load LUMI parallel cray-python;
source ${PYTHON}/bin/activate;

CODE=${BASE}/oe/textpipes;
export CODE;
if [ ! -f ${CODE}/tools/zstdconcat.py ]; then
  echo "pool.slurm: missing source code in ${BASE}/oe; exit." >&2;
  exit 1;
fi

if [ ! -f ${HOME}/.cache/hplt/openlid-v3.bin ]; then
  echo "pool.slurm: missing model file for OpenLID v3; exit." >&2;
  exit 1;
fi
if [ ! -f ${HOME}/.cache/hplt/glotlid-v3.bin ]; then
  echo "pool.slurm: missing model file for GlotLID v3; exit." >&2;
  exit 1;
fi

echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] pool.slurm: root in $(pwd);"
echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] pool.slurm: processing ${input};"
echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] pool.slurm: with ${CORES} cores."

function run() {
  local path=${1};
  local command="${CODE}/tools/zstdconcat.py --compress ${path}/markup.zst";
  if [ -f ${path}/xml.zst -a -f ${path}/md.zst ]; then
    command="${command} ${path}/xml.zst ${path}/md.zst";
    eval ${command};      
  else
    echo "invalid sub-directory; missing data files: ${path}." >&2;
    return 1;
  fi
} # run()
export -f run;

PARALLEL="parallel --plain --will-cite";
PARALLEL="${PARALLEL} --group --jobs ${CORES}";

#
# finally, iterate through all the batches in this crawl
#
find ${input} -name text.zst | sed "s,/text.zst$,,g" \
| eval ${PARALLEL} run {};

echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] pool.slurm: all processing complete."
exit 0;
