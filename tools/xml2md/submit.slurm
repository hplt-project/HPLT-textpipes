#!/bin/bash

#SBATCH --job-name=xml2md
#SBATCH --account=project_465001890
#SBATCH --partition=small
#SBATCH --time=1:00:00
#SBATCH --nodes=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=16G

set -euo pipefail

module load LUMI
source /projappl/project_465001890/jhelcl/HPLT-textpipes/tools/xml2md/env/bin/activate

OUTPUT_DIR=out
mkdir -p $OUTPUT_DIR


infile=/scratch/project_465001890/zaragoza/monotextor-processing/collections_merged/ces_Latn/batch_1.jsonl.zst
outfile=$OUTPUT_DIR/${infile##*/}

zstdcat $infile | python3 xml2md.py --xml-field=xml --verbosity=5 | zstd --compress -10 > $outfile
