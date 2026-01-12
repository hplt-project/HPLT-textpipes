# Stage2 (a.k.a. html2text): reproduction instructions 

## Prepare LUMI environment:
Activate necessary modules and python environment:
```commandline
source preplumicpu.sh
source venv/bin/activate
```

Export SLURM variables (replace the project with your project):
```commandline
export SLURM_ACCOUNT=project_465002259
export SLURM_MEM_PER_NODE=0  # same as --mem=0, requests all memory since standard nodes are allocated fully
export SLURM_PARTITION=standard
export SLURM_TIMELIMIT=0-48:00:00
```

```commandline
export SLURM_ACCOUNT=project_465002259
export SLURM_MEM_PER_CPU=1750M  # same as --mem-per-cpu=1750M, recommended for the small partition in the LUMI docs to avoid extra billing for larger memory nodes
export SLURM_PARTITION=small
export SLURM_TIMELIMIT=0-72:00:00
```

## Prepare a list of input files
Create a list of input files: a path to one html.zst file in each line.

E.g. to process files from LUMIO from the bucket _htmlsample_ create an rclone endpoint with ```rclone config``` 
pointing to LUMIO, then run:
```commandline
rclone ls --include="html.zst" lumio:htmlsample | sed -r 's!( *[0-9]+\s+)!\1 lumio:htmlsample/!' >lumio.paths
```

NB! If you want to process local files, please create an rclone endpoint with the type 'alias' for the parent folder of
all of these files and provide a list of files in the format endpoint:path. The code supports only paths in this format.
It strips _endpoint:_ and reconstructs path under the specified OUTPUT directory.

## Run stage2
Run stage2. E.g. to run on 100 parallel nodes max, 50 GB of input HTMLs per SLURM job:
```commandline
stage2nodeparallel_batched.sh 100 lumio.paths 50 ~/hplt/three/html_test
```
