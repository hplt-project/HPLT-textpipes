#!/bin/bash

#SBATCH --job-name=prepare
#SBATCH --partition=small
#SBATCH --account=project_462000131
#SBATCH --time=72:00:00
#SBATCH --mail-type=FAIL
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=32
#SBATCH --mem-per-cpu=4096M

set -x;

CORES=${SLURM_CPUS_ON_NODE:-8};
OELLM=/pfs/lustrep2/scratch/project_462000953/training/catalogue;

if [ -z "${1}" ]; then
  echo "prepare.slurm: missing command line argument (input file); exit." >&2;
  exit 1;
fi
if [ ! -f ${1} ]; then
  echo "prepare.slurm: invalid input file ${1}; exit." >&2;
  exit 1;
fi
FILE=$(realpath --strip ${1});
OUTPUT=${FILE%.zst}.s.zst;

echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] prepare.slurm: root in $(pwd);"
echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] prepare.slurm: processing ${FILE};"
echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] prepare.slurm: writing to ${OUTPUT};"
echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] prepare.slurm: with ${CORES} cores."


module reset;
module load cray-python;
source ${OELLM}/python/bin/activate;

PACKAGE=/scratch/project_462000827/oe/textpipes/package;
FLASH=/flash/project_462000827;

if [ ! -d ${FLASH}/${USER} ]; then
  mkdir ${FLASH}/${USER};
fi

export LC_ALL=C;
locale;

python ${PACKAGE}/prepare.py ${FILE} \
| sort --temporary-directory=${FLASH}/${USER} \
    --key=1,1 --numeric-sort --reverse \
    --parallel=${CORES} --buffer-size=120G \
| zstd --verbose --no-progress -z -10 -T${CORES} -f - -o ${OUTPUT}

echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] prepare.slurm: all processing complete."
exit 0;
