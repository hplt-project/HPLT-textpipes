#!/bin/bash
#SBATCH --account=project_462000827
#SBATCH --partition=small
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=16
#SBATCH --mem-per-cpu=1750
#SBATCH --time=01:30:00


module use /projappl/project_462000828/EasyBuild/modules/LUMI/24.03/partition/C/
module load LUMI/24.03
module load nlpl-warc2text/1.3.0
module load parallel

srun bash -c "parallel --joblog joblog.\$SLURM_NODEID -j \$SLURM_CPUS_PER_TASK -a tasks.\$SLURM_NODEID process_batch.sh {}"

