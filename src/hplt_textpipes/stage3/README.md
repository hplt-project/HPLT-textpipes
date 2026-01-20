# (InternetArchive crawls only) Stage3: preparation of allowed.zst
To remove documents that are not compatible with robots.txt start with generating allowed.zst files which are used for filtering. This is required for InternetArchive crawls only, CommonCrawl crawls are checked against robots.txt while crawling.

## Prepare for robots.txt
Clone the [Monotextor repo](https://github.com/hplt-project/monotextor-slurm/tree/v2.0?tab=readme-ov-file#install)
v2.0 and follow the install instructions. It is not required to set up HyperQueue.
Configure the Monotextor pipeline and set the `$COLLECTIONS` environment variable to point to each of the crawls in Stage 3.

## Run robots.txt annotation
First create the robots.txt allowance lists per crawl running `09.robotstxt` script as described in the Monotextor pipeline.
Ignore the rest of the Monotextor pipeline steps, they are not required.
Note that this step will need the `robotstxt.warc.gz` files from Stage 1 as input.

After robotstxt processing and having one `disallowed-urls.fst` per crawl, run the robots annotation
[script](https://github.com/hplt-project/monotextor-slurm/blob/21298b12910aa494df0e732a63734940b5762738/09.2.robots-annotate).
Note that the annotation script is located in a different Monotextor pipeline version of the repo.
The script can just be copied and used in v2.0 of the previously cloned Monotextor repo.
To run it use `sbatch`.

In each input directory this annotation process generates an additional file `allowed.zst`, which is line-parallel to other input files and contains a binary mask describning compatability of each document with robots.txt. If present, this file will be used when running stage3 to remove incompatible documents.

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
