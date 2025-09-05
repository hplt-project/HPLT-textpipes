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

This stage extracts text and various metadata from htmls and performs language identification.

TBD: instructions for stage2 running on LUMI

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

