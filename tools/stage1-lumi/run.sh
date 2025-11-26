#!/bin/bash
#SBATCH --partition=standard
#SBATCH --nodes=6
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=128
#SBATCH --time=48:00:00

S3_OUTPUT_PREFIX=$1
if [ -z "$S3_OUTPUT_PREFIX" ]; then
  echo "usage: run.sh S3_OUTPUT_PREFIX (NODE_OFFSET)"
  exit 1
fi

NODE_OFFSET=${2:-0}
echo "Running task lists $NODE_OFFSET - $(($NODE_OFFSET + $SLURM_JOB_NUM_NODES - 1))"

module purge
module load lumio
# the venv contains s3cmd version 2.4.0 which is required here
# (the `lumio` module currently has 2.3.0)
source /projappl/project_462000828/s3cmd-venv/bin/activate
module load LUMI/24.03
module load nlpl-warc2text/1.4.0
module load parallel/20240522

s3cmd --version       # this should print "s3cmd version 2.4.0"

srun bash -c \
    "parallel --joblog joblog.\$((\$SLURM_NODEID+$NODE_OFFSET)) \
              -j 48 -a tasks.\$((\$SLURM_NODEID+$NODE_OFFSET)) \
              process_batch.sh {} $S3_OUTPUT_PREFIX"
