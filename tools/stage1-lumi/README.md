# Running warc2text on LUMI with streaming from/to LUMI-O

## 0. Setup

First, set up LUMI-O access:
```
module load lumio
export LUMIO_S3_ACCESS=<your access token>
export LUMIO_S3_SECRET=<your secret>
lumio-conf --project-number 462000828 --noninteractive
```

Then, add the path to the scripts directory to `$PATH` so that `process_batch.sh`
can be called from anywhere. E.g. in the repo root directory:
```
export PATH="<path to this repo>/tools/stage1-lumi:$PATH"
```

Project-specific setup - setting the account and the EasyBuild directory
(in which warc2text is presumably installed):
```
export SBATCH_ACCOUNT=project_<project id>
module use <path to project-specific EasyBuild directory>/modules/LUMI/24.03/partition/C/
```

Finally, set up a directory for temporary data on `/scratch` and **change
into it** (all further scripts assume that it's the current
directory). For example:
```
mkdir -p /scratch/project_462000828/$USER/warc2text-lumi
cd /scratch/project_462000828/$USER/warc2text-lumi
```

## 1. Get the WARC paths and split them into batches

First, create a file `crawls.txt` containing a list of crawls to process,
as paths starting with `s3://`. For example:
```
s3://cc-main-2025-05.lst
s3://cc-main-2025-08.lst
s3://cc-main-2025-13.lst
```

Then, run the `prepare_tasks.sh` script. The first argument is the number
of nodes and the second the batch size.
```
prepare_tasks.sh 24 1000 < crawls.txt
```

This will create one directory per batch (containing paths of WARC files)
and one list of batches per node.

## 2. Run the processing

Before submitting the batch job, edit the slurm script and adjust the
number of nodes and allocated time. 

Then, submit the job:
```
sbatch <path to this repo>/tools/stage1-lumi/run.sh s3://<your output prefix>
```

### Dividing the processing in multiple batch jobs

The LUMI-O bandwidth limits the number of streaming processes that can
be run simultaneously. In October 2025 we determined that with **48
processes per node** we could run up to **6 nodes** simultaneosly
for a total of 288 simultaneous processes. Larger numbers caused too
frequent network timeouts, which resulted in data loss or failed jobs.

If the size of the data requires more than 6 nodes to process, you can
split the processing into multiple batch jobs. Use the second parameter
of the `run.sh` script to pass a "node offset" - this will use task files
from this index onwards. For example for `--nodes=6` and 12 nodes in total:

```
# run task lists for nodes 0-5
sbatch \
   <path to this repo>/tools/stage1-lumi/run.sh \
   s3://<your output prefix>

# run task lists for nodes 6-11
sbatch -d afterok:<first job id> \
    <path to this repo>/tools/stage1-lumi/run.sh \
    s3://<your output prefix> 6
```

The option `-d afterok:<first job id>` means that the second job will run
after the first one has successfully finished. You can read the job ID
from the output of the first `sbatch` command.

