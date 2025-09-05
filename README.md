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
- [Stage 1: Extracting HTML and metadata from WARC files](https://github.com/hplt-project/warc2text-runner/tree/main/two#stage1-aka-warc2html) (`warc2thml`)

The output of this stage consists mostly of HTMLs.

- [Stage 2: Extracting raw text](https://github.com/hplt-project/warc2text-runner/tree/main/two#stage2-aka-html2text) (`html2text`)
    - [Trafilatura](https://github.com/hplt-project/warc2text-runner/blob/main/src/warc2text_runner/two/trafilatura/traf.py) (running text extraction and boilerplate removal)
    - [Document language identification with OpenLid](https://github.com/hplt-project/warc2text-runner/blob/main/src/warc2text_runner/two/fastertext_lid/proto_langid.py)

The output of this stage is plain text and metadata (separately) in JSONL format. 

## Deduplication, cleaning and filtering

- [Monotextor](https://github.com/hplt-project/monotextor-slurm/tree/v2.0)

The output of this stage is plain text merged with metadata in JSONL format.
It comes in the `deduplicated` and `cleaned` varieties.

