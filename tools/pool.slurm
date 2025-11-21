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
#SBATCH --cpus-per-task=4
#SBATCH --mem-per-cpu=4096M

set -x;

BASE=/scratch/project_465002259/threeone;
cd ${BASE};

if [ -z "${1}" ]; then
  echo "pool.slurm: missing command line argument (input file); exit." >&2;
  exit 1;
fi
if [ ! -d ${1} -o ! -f ${1}/text.zst ]; then
  echo "pool.slurm: invalid input directory ${1}; exit." >&2;
  exit 1;
fi
input=$(realpath --strip ${1});
output=$(realpath --strip ${input}/../pool/${input##*/});

if [ ! -d ${output} ]; then mkdir -p ${output}; fi
     
CORES=${SLURM_CPUS_ON_NODE:-8};


PYTHON=${BASE}/local;
if [ ! -f ${PYTHON}/bin/activate ]; then
  echo "pool.slurm: invalid Python environment ${PYTHON}; exit." >&2;
  exit 1;
fi
module reset;
module load cray-python;
source ${PYTHON}/bin/activate;

CODE=${BASE}/oe/textpipes;
if [ ! -f ${CODE}/tools/zstdconcat.py ]; then
  echo "pool.slurm: missing source code in ${BASE}; exit." >&2;
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
echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] pool.slurm: writing to ${output};"
echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] pool.slurm: with ${CORES} cores."

export LC_ALL=C;
locale;

function run() {
  local path=${1};
  local -a command=(  
  if [ -f ${path}/text.zst -a -f ${path}/lang.zst -a -f ${path}/metadata.zst ]; then
    
  else
    echo "invalid sub-directory; missing data files: ${path}." >&2;
    return 1;
  fi
  (
    cd ${path};
    target=$(awk '{print $2}' ${file});
    if [ ! -f ${target} ]; then
      echo "missing target file: ${path}/${target#./}." >&2;
      return 1;
    fi
    if md5sum --check --status ${file}; then
      echo "check-sum validated: ${path}/${target#./}." >&2;
    else
      echo "check-sum failure; removing: ${path}/${target#./}." >&2;
      /bin/rm ${target};
    fi
  )
} # validate()
export -f validate;

PARALLEL="parallel --plain --will-cite";
PARALLEL="${PARALLEL} --group --jobs ${CORES}";

#
# finally, iterate through all the batches in this crawl
#
egrep "^output " ${MD5} | sed "s/output //" \
| eval ${PARALLEL} run {};

echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] pool.slurm: all processing complete."
exit 0;
