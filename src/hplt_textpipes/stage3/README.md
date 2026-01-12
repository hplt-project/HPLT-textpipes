# Stage3: reproduction instructions

## Prepare LUMI environment
Activate LUMI modules and python environment:
```commandline
source preplumicpu.sh
source venv/bin/activate
```

Export SLURM variables (replace the project with your project):
```commandline
export SLURM_ACCOUNT=project_465002259
export SLURM_MEM_PER_CPU=1750M  # same as --mem-per-cpu=1750M, recommended for the small partition in the LUMI docs to avoid extra billing for larger memory nodes
export SLURM_PARTITION=small
export SLURM_TIMELIMIT=0-72:00:00
```

## Prepare a list of input files
Create a list of input files: a path to one text.zst file in each line.

One way to create such file is using rclone. E.g. when processing data for HPLT v4 the endpoint four: was created with 
```rclone config``` pointing to the directory with the input data, then text.zst files from CC crawls were listed with:
```commandline
rclone ls --include="text.zst" four: | grep -E '[0-9 ]+CC-MAIN' | sed -r 's!( *[0-9]+\s+)!\1 four:!' >paths
```

## Run stage3
Run stage3, e.g. to run on 200 nodes with batches of 1200 GB of inputs (counting text.zst only) per node:
```commandline
stage3nodeparallel_batched.sh 200 paths 1200 ~/hplt_old/four/cc_stage3out stage3local_v2.sh
```
