# HPLT TextPipes

This is a schematic step-by-step description of data processing (text extraction and cleaning) pipeline used to create [HPLT v2 datasets](https://hplt-project.org/datasets/v2.0).

Each step is accompanied by a link to the corresponding code base.

See more details in the [Deliverable Report 7.2](https://hplt-project.org/HPLT_D7_2___HPLT_pipelines_and_tools.pdf).

## Data ingestion
- [Internet Archive downloader](https://github.com/hplt-project/ia-download)
- [Helper scripts for CommonCrawl downloading](https://github.com/hplt-project/cc-download)
- [LUMI-specific scripts for CommonCrawl downloading directly to LUMIO](download/cc)

The output of this stage consists of WARC files.

## Text extraction

### Installation on LUMI
Load the required LUMI modules:
```commandline
source preplumicpu.sh
```
Install with pip in a virtual environment. Use --system-site-packages to reuse 
packages installed in cray-python when possible, which may be better optimized for LUMI. 
Install only extra dependencies from two/requirements_LUMIextra.txt 
```commandline
python -m venv --system-site-packages venv
source venv/bin/activate
pip install -r requirements_LUMIextra.txt
pip install .  
```

Download the language identification model weights:
```commandline
stage2download.sh
```

### Install on other systems (not tested!)
You might want to install on your local machine or a cluster other than LUMI.
Install using pip all the requirements, including those coming from cray-python module on LUMI: 
```commandline
python -m venv venv
source venv/bin/activate
pip install -r  requirements_LUMIall.txt
pip install .
```


### Stage1 (a.k.a. warc2html)

This stage extracts htmls, pdfs and various metadata from WARC files.

TBD: instructions for stage1 running on LUMI

### Stage2 (a.k.a. html2text)

Stage2 does text extraction with boilerplate removal (Trafilatura) and language identification (fasterText with the openLID model).
It is executed on 100 LUMI compute nodes, in 250 parallel processes on each.

Prepare LUMI environment:
```commandline
source src/warc2text_runner/stage2/stage2preplumic.sh
source venv/bin/activate
```

Prepare a list of HTML files to process from LUMIO:
```commandline
rclone ls --include="html.zst" lumio:htmlsample | sed -r 's!( *[0-9]+\s+)!\1 lumio:htmlsample/!' >lumio.paths
```

NB! If you want to process local files, please create an rclone endpoint with the type 'alias' for the parent folder of
all of these files and provide a list of files in the format endpoint:path. The code supports only paths in this format.
It strips endpoint: and reconstructs path under the specified OUTPUT directory.

Specify the account and the partition SLURM should use:
```commandline
export SLURM_ACCOUNT=project_465001890
export SLURM_MEM_PER_NODE=0  # same as --mem=0, requests all memory since standard nodes are allocated fully
export SLURM_PARTITION=standard
export SLURM_TIMELIMIT=0-48:00:00
```

```commandline
export SLURM_ACCOUNT=project_465001890
export SLURM_MEM_PER_CPU=1750M  # same as --mem-per-cpu=1750M, recommended for the small partition in the LUMI docs to avoid extra billing for larger memory nodes
export SLURM_PARTITION=small
export SLURM_TIMELIMIT=0-72:00:00
```

Run processing in 100 parallel nodes max, 50 GB of input HTMLs per SLURM job:
```commandline
stage2nodeparallel_batched.sh 100 lumio.paths 50 ~/hplt/three/html_test
```

### Older versions
The code for text extraction in this repository is based on 
- [Stage 1: Extracting HTML and metadata from WARC files](https://github.com/hplt-project/warc2text-runner/tree/main/two#stage1-aka-warc2html) (`warc2thml`)
- [Stage 2: Extracting raw text](https://github.com/hplt-project/warc2text-runner/tree/main/two#stage2-aka-html2text) (`html2text`)
    - [Trafilatura](https://github.com/hplt-project/warc2text-runner/blob/main/src/warc2text_runner/two/trafilatura/traf.py) (running text extraction and boilerplate removal)
    - [Document language identification with OpenLid](https://github.com/hplt-project/warc2text-runner/blob/main/src/warc2text_runner/two/fastertext_lid/proto_langid.py)

The output of this stage is plain text and metadata (separately) in JSONL format. 

## Deduplication, cleaning and filtering

- [Monotextor](https://github.com/hplt-project/monotextor-slurm/tree/v2.0)

The output of this stage is plain text merged with metadata in JSONL format.
It comes in the `deduplicated` and `cleaned` varieties.

