# HPLT TextPipes

This repository contains code and description of how [HPLT v4 datasets](https://hplt-project.org/datasets/v4.0) were created.
TDB: fix the link above, add relevant links 

## Install on LUMI
Load the required LUMI modules:
```commandline
source preplumicpu.sh
```
Install with pip in a virtual environment. Use --system-site-packages to reuse 
packages installed in cray-python when possible, which may be better optimized for LUMI. 
Install only extra dependencies from requirements_LUMIextra.txt 
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

<details>
<summary><b>Install on other systems</b></summary>

NB! The following instructions are not thoroughly tested and may require modifications.

You might want to install on your local machine or a cluster other than LUMI.
Install using pip all the requirements, including those coming from cray-python module on LUMI: 
```commandline
python -m venv venv
source venv/bin/activate
pip install -r requirements_LUMIall.txt
pip install .
stage2download.sh
```
</details>

## Input web crawls
HPLT datasets are built from web crawls. Technically, each web crawl is a set of WARC files logging communication between
a web crawler and websites. Among others, HTTP responses containing resources with text in natural language (web pages, 
PDF files) are logged. Text is extracted from such resources by the text extraction pipeline described below.

Two sources of web crawls are employed: Common Crawl and Internet Archive. Code developed for efficient downloading is
available in the corresponding repositories:
- [Internet Archive downloader](https://github.com/hplt-project/ia-download)
- [Helper scripts for CommonCrawl downloading](https://github.com/hplt-project/cc-download)
- [LUMI-specific scripts for CommonCrawl downloading directly to LUMIO](download/cc)


## Text extraction
Text extraction from web crawls for **HPLT v4** consists of the stages described below. 

<details>
<summary><b>Older version</b></summary>

Code and description of text extraction done for the older HPLT versions: 
- [HPLT v2](https://github.com/hplt-project/warc2text-runner/tree/main/two)
- [HPLT v3](https://github.com/hplt-project/warc2text-runner/tree/main/three)
</details>


### Stage1 (a.k.a. warc2html)

This stage extracts htmls, pdfs and various metadata from WARC files.
Both the input and output files are streamed from/to LUMI-O.

The outputs consist of:
- html.zst: jsonl with the following fields
  - html: the extracted HTML
- metadata.zst: jsonl with the following fields
  - f: path to the original WARC file
  - o: byte offset of the record in the original WARC file
  - s: record size
  - rs: byte size of the record payload (uncompressed)
  - u: URI specified in the WARC record
  - c: content type specified in the WARC record
  - ts: timestamp specified in the WARC record
  - de: detected encoding of the payload before it is converted to utf-8
- pdf.warc.gz: the subset of records from the original WARC file corresponding to HTTP responses containing PDF files
- robotstxt.warc.gz: the subset of records from the original WARC file with URIs ending by robots.txt

[Instructions for running](./tools/stage1-lumi/README.md)

### Stage2 (a.k.a. html2text)

This stage does text extraction with boilerplate removal (Trafilatura) and language identification (fasterText with the openLID model).
This stage is much more CPU-intensive than stage1, it was executed on 100 LUMI compute nodes, in 250 parallel processes on each.

The outputs consist of:
- text.zst: jsonl with the following fields
  - t: text extracted from the HTML page by Trafilatura, null in case of any errors 
  - traferr: error in case extraction with Trafilatura finished with an error 
  - x: xml representation of the page returned by Trafilatura, null in case of any errors
  - htmllang: a list with all values of html lang attributes on the page
  - metalang: a list with all values of meta lang attributes on the page
- lang.zst: jsonl with the following fields
  - lang: a list of the most probable language or null if classification cannot be performed (e.g. no text or text is too short)
  - prob: a list of the predicted probabilities of the most probable languages

[Instructions for running](src/hplt_textpipes/stage2/README.md)

### Stage3

This stage was introduced starting from HPLT v4. It includes augmenting data with auxiliary information 
(markdown obtained from xml representation of web pages, predictions of complementary LID models), 
removing pages that are non-compliant with robots.txt (relevant for IA crawls only), repacking data into 
multiframe .zst files to enable partial downloads for efficient random access to specific documents.  

The outputs consist of:
- text.zst: jsonl with the following fields
  - text: text extracted from the HTML page by Trafilatura, null in case of any errors
- xml.zst: jsonl with the following fields
  - xml: xml representation of the page returned by Trafilatura, null in case of any errors
- md.zst: jsonl with the following fields
  - md: xml converted to markdown, null in case of any errors
- metadata.zst: jsonl with all metadata from stage2 outputs (see above) with the following modifications:
  - allowed: whether the document surpassed filtering by robots.txt (if present should be true, preserved for sanity checks)
  - openlid-v2: language predictions from lang.zst from stage2 
  - glotlid-v3, openlid-v3: predictions from two complementary LID models

All the outputs .zst files are multiframe .zst archives prepared by [t2sz](https://github.com/martinellimarco/t2sz) 
to enable partial download and random access.


[Instructions for running](src/hplt_textpipes/stage3/README.md)

### Stage4: TBD

## Further processing
After text and metadata are extracted from web crawls, they undergo further cleaning and deduplication. 
Code and description are available in a separate repository:
- [Monotextor](https://github.com/hplt-project/monotextor-slurm)

