#!/bin/bash
#SBATCH --job-name=HPLT-pool-count
#SBATCH --account=project_465002259
#SBATCH --partition=standard
#SBATCH --time=23:00:00
#SBATCH --nodes=1
#SBATCH --output=logs/%x-%j.log
#SBATCH --array=1-75

module --quiet purge
module load LUMI

POOL_DIR=/scratch/project_465001890/four/pool
#POOL_DIR=/scratch/project_465002259/jhelcl/test-pool
STATS_DIR=/scratch/project_465002259/jhelcl/pool-stats

source /scratch/project_465002259/jhelcl/HPLT-textpipes/env/bin/activate

offset=$SLURM_ARRAY_TASK_ID
crawl=`ls $POOL_DIR | sort | sed -n "${offset}p"`

N=128

for shard_dir in $POOL_DIR/$crawl/*; do

    (
        #crawl=`basename $crawl_dir`
        shard=`basename $shard_dir`
        stats="$STATS_DIR/$crawl.$shard.json"
        echo "PROCESSING $crawl - $shard";

        python four_pool_count_lines_and_chars.py $shard_dir > $stats
        sleep $(( (RANDOM % 3) + 1))
    ) &

    # Allow to execute up to $N jobs in parallel
    if [[ $(jobs -r -p | wc -l) -ge $N ]]; then
        # Now there are $N jobs already running, so wait here for any job
        # to be finished so there is a place to start next one.
        wait -n
    fi

done

wait

