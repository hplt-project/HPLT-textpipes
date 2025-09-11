#!/bin/bash
#SBATCH --partition=standard
#SBATCH --nodes=12
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=128
#SBATCH --mem-per-cpu=4G
#SBATCH --time=48:00:00

S3_OUTPUT_PREFIX=$1
if [ -z "$S3_OUTPUT_PREFIX" ]; then
  echo "usage: run.sh S3_OUTPUT_PREFIX"
  exit 1
fi

module purge
module load lumio
module load LUMI/24.03
module load nlpl-warc2text/1.4.0
module load parallel/20240522

srun bash -c "parallel --joblog joblog.\$SLURM_NODEID -j \$((SLURM_CPUS_PER_TASK / 2)) -a tasks.\$SLURM_NODEID process_batch.sh {} $S3_OUTPUT_PREFIX"

