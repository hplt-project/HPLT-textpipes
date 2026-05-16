#!/bin/bash

#
# for i in $(seq 0 9); do
#   sbatch ../oe/textpipes/filter/filter.slurm monotext/cmn_Hans --batch $i --separate;
# done
#
# for i in $(seq 0 9); do
#   sbatch ../oe/textpipes/filter/filter.slurm monotext/eng_Latn --batch $i --separate;
# done
#
# for i in $(seq 0 9); do
#   sbatch ../oe/textpipes/filter/filter.slurm monotext/rus_Cyrl --batch $i --separate;
# done
#
# for i in $(cat etc/openeurollm.txt); do sbatch ../oe/textpipes/filter/filter.slurm monotext/$i; done
#
# remaining parameters following “--batch” will be passed onto “filter.py”
# verbatim, for example to activate crawl source separation or tracing.
#


#SBATCH --job-name=shard
#SBATCH --output=log/%x-%j.out
#SBATCH --partition=small
#SBATCH --account=project_465002530
#SBATCH --time=72:00:00
#SBATCH --mail-type=FAIL
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem-per-cpu=16G

base=/scratch/project_465002891;

if [ -z "${1}" ]; then
  echo "shard.slurm: missing argument (output directory); exit." >&2;
  exit 1;
fi
if [ ! -d ${1} ]; then
  echo "shard.slurm: invalid output directory ${1}; exit." >&2;
  exit 1;
fi
output=$(realpath --strip ${1});
shift;

if [ -z "${1}" ]; then
  echo "shard.slurm: missing argument (shard prefix); exit." >&2;
  exit 1;
fi
prefix="${1}";
shift;

if [ "${1}" ]; then
  infix="${1}";
  pattern="${prefix}_*.${infix}.jsonl.zst";
else
  pattern="${prefix}_*.jsonl.zst";
fi

cores=${SLURM_CPUS_ON_NODE:-8};

python=${base}/oe/local;
if [ ! -f ${python}/bin/activate ]; then
  echo "shard.slurm: invalid Python environment ${python}; exit." >&2;
  exit 1;
fi
module reset;
module load LUMI parallel cray-python;
source ${python}/bin/activate;

code=${base}/oe/textpipes;
export code;
if [ ! -f ${code}/filter/shard.py ]; then
  echo "shard.slurm: missing source code in ${base}/oe; exit." >&2;
  exit 1;
fi

echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] shard.slurm: root in $(pwd);"
echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] shard.slurm: processing ${output};"
echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] shard.slurm: with ${cores} cores."

#
# finally, process the input directories, reading all files sequentially
#
eval ${code}/filter/shard.py --trace --cores ${cores} \
  --prefix ${prefix} --infix ${infix} \
  --output ${output} ${output}.?/${pattern};

echo "[$(date +"%Y-%m-%d (%H:%M:%S)")] shard.slurm: all processing complete."
exit 0;
