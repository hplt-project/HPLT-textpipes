#!/bin/bash

#SBATCH --job-name=merge
#SBATCH --partition=small
#SBATCH --account=project_462000131
#SBATCH --time=72:00:00
#SBATCH --mail-type=FAIL
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=32
#SBATCH --mem-per-cpu=2G

set -x;

CORES=${SLURM_CPUS_ON_NODE:-8};
OELLM=/pfs/lustrep2/scratch/project_462000953/training/catalogue;

if [ -z "${@}" ]; then
  echo "merge.slurm: missing command line argument(s) (input(s)); exit." >&2;
  exit 1;
fi

echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] merge.slurm: running in $(pwd);"
echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] merge.slurm: processing: ${@};"
echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] merge.slurm: with ${CORES} cores."
echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] merge.slurm: process environment:"
env;
echo;

module reset;
module load cray-python;
source ${OELLM}/python/bin/activate;

PACKAGE=/scratch/project_462000827/oe/textpipes/package;
cd ${PACKAGE};
python ${PACKAGE}/merge.py --n $[32 * 1024 * 1024] ${@};

echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] merge.slurm: all processing complete."
exit 0;
