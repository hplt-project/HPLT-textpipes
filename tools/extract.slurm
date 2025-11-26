#!/bin/bash
#SBATCH --job-name=HPLT
#SBATCH --account=project_465002259
#SBATCH --partition=small
#SBATCH --time=23:00:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=32
#SBATCH --mem=32G

# Extract field values from HPLT datasets in the catalogue

module --quiet purge
module load LUMI

LANG=${1}  # Language in the catalogue (e.g., ukr_Cyrl)
FIELD=${2} # What field to extract (ts, crawl_id, etc)
N=24 # Number of files processed in parallel

mkdir -p ${LANG}

echo ${LANG}
echo ${FIELD}
echo ${N}


for el in /appl/local/openeurollm/training/catalogue/hplt/3.0/sorted/${LANG}/*.jsonl.zst
do
    (
    echo ${el}
    zstdcat ${el} | jq --arg FIELD $FIELD -r '"\(.[$FIELD])"' | zstd > ${LANG}/${FIELD}_$(basename ${el}).txt.zst
    sleep $(( (RANDOM % 3) + 1))
    ) &
    # Allow to execute up to $N jobs in parallel
    if [[ $(jobs -r -p | wc -l) -ge $N ]]; then
        # Now there are $N jobs already running, so wait here for any job
        # to be finished so there is a place to start next one.
        wait -n
    fi
done

# No more jobs to be started but wait for pending jobs
# (all need to be finished)
wait

cat ${LANG}/${FIELD}_*.zst > ${LANG}/${FIELD}.zst

echo "Sorting..."

zstdcat ${LANG}/${FIELD}.zst | sort | uniq -c | sort -nr > ${LANG}/${FIELD}_sorted.txt

rm ${LANG}/${FIELD}_*.zst
echo "All done"

