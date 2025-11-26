#!/bin/bash
#SBATCH --job-name=HPLT
#SBATCH --account=project_465002259
#SBATCH --partition=standard
#SBATCH --time=23:00:00
#SBATCH --nodes=1

# Extract field values from HPLT datasets in the catalogue (for really large languages)

module --quiet purge
module load LUMI

# This is to speed up sorting:
export LC_ALL=C

LANG=${1}  # Path to language in the catalogue
FIELD=${2} # What field to extract (ts, crawl_id, etc)
N=128 # Number of files processed in parallel

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

zstdcat ${LANG}/${FIELD}.zst | sort -T /flash/project_465002259/ --parallel=32 | uniq -c | sort -T /flash/project_465002259/ --parallel=32 -nr > ${LANG}/${FIELD}_sorted.txt

rm ${LANG}/${FIELD}_*.zst
echo "All done"

