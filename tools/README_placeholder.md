---
license: cc0-1.0
size_categories:
- n>1T
multilinguality:
- multilingual
task_categories:
- fill-mask
- text-generation
task_ids:
- language-modeling
language:
{{LANGUAGES_PLACEHOLDER}}
tags:
- web-crawled
---

<img align="right" src="https://hplt-project.org/_next/static/media/logo-hplt.d5e16ca5.svg" alt="HPLT" title="HPLT">

This is a large-scale collection of web-crawled documents in 198 world languages, produced by the [HPLT project](https://hplt-project.org/).
The source of the data is [Internet Archive](https://archive.org/) and [Common Crawl](https://commoncrawl.org/). 
For a detailed description of this and previous releases by HPLT, please refer to [our website](https://hplt-project.org/datasets/v3.0).

**NB: the HPLT datasets are not hosted on HuggingFace! See download instructions below.**

## Table of Contents

1. [HPLT release v3.0](#hplt-release-v3.0)
2. [New in this release compared to HPLT v2](#new-in-this-release-compared-to-hplt-v2)
3. [Downloading HPLT v3.0](#downloading-hplt-v3.0)
4. [Statistics and validation](#statistics-and-validation)
5. [License and takedown](#license-and-takedown)
6. [Cite us](#cite-us)
7. [Funding](#funding)

## HPLT release v3.0
In July 2025, the European HPLT initiative has completed a new release of its monolingual datasets, offering better data quality, more annotations and metadata, and greatly increased volume. 
HPLT Monolingual Datasets 3.0 comprise some 50 terabytes of compressed data, covering 198 languages. 
More than half of the data represents the English language. 
Not counting the English majority portion, the dataset offers about 11.5 billion documents, 40 trillion Unicode characters, or 13.5 trillion tokens (using the [Gemma 3 vocabulary](https://gemma-llm.readthedocs.io/en/latest/colab_tokenizer.html)). 
Overall, HPLT 3.0 is about three times larger than the previous release and likely constitutes the largest generally available multilingual dataset.

The dataset has been derived from some 7.2 petabytes of raw web crawls from the [Internet Archive](https://archive.org/) and the [Common Crawl](https://commoncrawl.org/), spanning the period between 2012 and 2024. 
Text extraction from HTML documents was performed through the [Trafilatura](https://trafilatura.readthedocs.io/en/latest/) library, language identification with [OpenLID 2.0](https://huggingface.co/datasets/laurievb/OpenLID-v2), and deduplication, annotation, and filtering through the [Monotextor pipeline](https://github.com/hplt-project/monotextor-slurm).

Except quality and size, other distinguishing properties of the HPLT Monolingual Dataset is its sorting by a [language-independent estimate of document quality](https://github.com/pablop16n/web-docs-scorer) and the rich annotations and metadata, including [web register labels](https://github.com/TurkuNLP/register-annotation-docs/) (for 104 of the languages in release 3.0), document- and segment-level language identification, annotation of personally identifiable information, and provenance information from the original crawl. 
Release 3.0 also fixes a deficiency in the Chinese data in the previous release, where double-width punctuation had been over-zealously normalized.

Except for Chinese, English, and Russian, each language-specific portion has been globally deduplicated.

Data processing was performed on dedicated storage and compute resources at the Czech and Norwegian national HPC infrastructures [CESNET](https://www.cesnet.cz/en) and [Sigma2 NRIS](https://www.sigma2.no/), as well as on the EuroHPC [LUMI](https://www.lumi-supercomputer.eu/) system. 
The HPLT download site is hosted at the Sigma2 NIRD datalake.

## New in this release compared to HPLT v2
 - Reflects substantially more raw web data, primarily from the Common Crawl
 - Additional metadata, including more information from the underlying crawl
 - Upgrade to Trafilatura 2.0 with empirical fine-tuning of extraction parameters
 - Plain-text and structured document representation, in simple, normalized XML
 - Better language identification; refined codes for Arabic and Chinese
 - Global deduplication for most languages; MinHash cluster size as metadata
 - Annotation with Turku web register labels for more than half the languages
 - Upgrade to newer, improved Web Docs Scorer (WDS) document quality estimates
 - Global sorting within each language by WDS and sharding into WDS bins (10–5)
 - Improved filtering for `robots.txt` opt-out, adult content, and credentials
 - Improved deduplication pipeline (global deduplication for most languages)

## Downloading HPLT v3.0

For each language, the data is organized in smaller shards, sorted by document quality estimates (WDS). 
For Russian (in Cyrillic script), for example, the file `rus_Cyrl/10_1.jsonl.zst` is the first (and only) shard in the top WDS bin (scored as exactly 10`), and `rus_Cyrl/9_1.jsonl.zst` … `rus_Cyrl/9_103.jsonl.zst` are the 103 shards in the bin for scores greater or equal to WDS `9` and less than `10`.

The easiest way to download the data for a specific language is to use a command like `wget -i` with a language-specific mapping file containing full download addresses for all shards of this particular language, for example (for Crimean Tatar in Latin script):

```
wget -O - https://data.hplt-project.org/three/sorted/crh_Latn.map | wget -x -nH --cut-dirs=2 -i -
```
The above command retrieves the map for `chr_Latn` and feeds it as a list of download addresses into a second `wget` invocation, requesting the creation of local directories `(-x)`, but cutting off the host and first two directory components (`-nH --cut-dirs=2`).

To download _all available data_, there is a larger mapping file for the full multilingual (excluding English) portion, amounting to a download of around 20 terabytes. 
The complete English data comprises some 30 terabytes and can be downloaded using its per-language mapping file. 
These can be retrieved using e.g. `wget`, and used as input directives for larger downloads, much like in the example above:

```
wget https://data.hplt-project.org/three/sorted/multilingual.map

wget https://data.hplt-project.org/three/sorted/eng_Latn.map
```
 
To speed up large downloads, it can be beneficial to use multiple parallel connections, for example using the `--max-threads` option in `wget`. 
We recommend to limit download parallelization to 16–32 threads, to avoid server-side rate limitations, which should allow download rates of around 250 gigabytes per hour.

### Language-specific download links

{{DOWNLOAD_PLACEHOLDER}}
### Downloading with HuggingFace Datasets
To load a monolingual portion of the **HPLT v3.0** dataset in the _Huggingface Datasets_ format, you can run the following code to download the dataset files map and then load the `.jsonl.zst` files directly using `load_datasets()`. 
The `Datasets` package will then handle downloading of the files. 
If you would like to **stream** the files instead of downloading them all at once, set `streaming=True` within the `load_dataset()` function.

```python
from datasets import load_dataset, Features, Value, Sequence, List
import requests

lang_code = "yor_Latn"  # Define your language-script code here, or "multilingual" for full multilingual portion minus English

r = requests.get(f"https://data.hplt-project.org/three/sorted/{lang_code}.map")

source_urls = r.text.strip().split("\n")

features = Features(
    {
        "f": Value("string"),
        "o": Value("int64"),
        "s": Value("int64"),
        "rs": Value("int64"),
        "u": Value("string"),
        "c": Value("string"),
        "ts": Value("timestamp[s]"),
        "de": Value("string"),
        "crawl_id": Value("string"),
        "lang": List(Value("string")),
        "prob": List(Value("float64")),
        "text": Value("string"),
        "xml": Value("string"),
        "html_lang": List(Value("string")),
        "cluster_size": Value("int64"),
        "seg_langs": List(Value("string")),
        "id": Value("string"),
        "filter": Value("string"),
        "pii": List(List(Value("int64"))),
        "doc_scores": List(Value("float64")),
        "web-register": {
            "MT": Value("float64"),
            "LY": Value("float64"),
            "SP": Value("float64"),
            "ID": Value("float64"),
            "NA": Value("float64"),
            "HI": Value("float64"),
            "IN": Value("float64"),
            "OP": Value("float64"),
            "IP": Value("float64"),
            "it": Value("float64"),
            "ne": Value("float64"),
            "sr": Value("float64"),
            "nb": Value("float64"),
            "re": Value("float64"),
            "en": Value("float64"),
            "ra": Value("float64"),
            "dtp": Value("float64"),
            "fi": Value("float64"),
            "lt": Value("float64"),
            "rv": Value("float64"),
            "ob": Value("float64"),
            "rs": Value("float64"),
            "av": Value("float64"),
            "ds": Value("float64"),
            "ed": Value("float64"),
            "cm": Value("float64"),
            "dp": Value("float64"),
            "dt": Value("float64"),
            "ib": Value("float64"),
            "oi": Value("float64"),
            "tr": Value("float64"),
            "ad": Value("float64"),
            "le": Value("float64"),
            "oo": Value("float64"),
            "ha": Value("float64"),
            "ma": Value("float64"),
            "on": Value("float64"),
            "pb": Value("float64"),
            "ss": Value("float64"),
            "tb": Value("float64"),
            "oe": Value("float64"),
            "pa": Value("float64"),
            "df": Value("float64"),
            "of": Value("float64"),
            "qa": Value("float64"),
            "rr": Value("float64"),
            "fh": Value("float64"),
            "ht": Value("float64"),
            "oh": Value("float64"),
            "ts": Value("float64"),
            "ol": Value("float64"),
            "po": Value("float64"),
            "pr": Value("float64"),
            "sl": Value("float64"),
            "fs": Value("float64"),
            "os": Value("float64"),
            "ta": Value("float64"),
            "tv": Value("float64")
        },
    }
)

ds = load_dataset("json", data_files=source_urls, features=features)

print(ds)
```

## Statistics and validation

Summary statistics per language are available for download as a structured [manifest.json](https://data.hplt-project.org/three/sorted/manifest.json), also including download links for the individual data files, per-language maps, and sample documents from various quality bins. 
Additionally, each language subdirectory provides compressed lists of unique domains, full URLs, and what are called normalized document signatures, together with their frequencies of occurence, for example [nob_Latn/.domains.zst](https://data.hplt-project.org/three/sorted/nob_Latn/.domains.zst), [nob_Latn/.urls.zst](https://data.hplt-project.org/three/sorted/nob_Latn/.urls.zst), and [nob_Latn/.signatures.zst](https://data.hplt-project.org/three/sorted/nob_Latn/.signatures.zst) for Norwegian Bokmål.

{{STATISTICS_PLACEHOLDER}}

The counts of documents per language or total storage sizes in the above statistics could be used to approximately validate each language sub-directory, but for more thorough validation of individual data files or full downloads, MD5 checksum files are provided with naming conventions parallel to the data and per-language map files. 
For example: [nob_Latn/.10_1.jsonl.md5](https://data.hplt-project.org/three/sorted/nob_Latn/.10_1.jsonl.md5) for the first data file in Norwegian Bokmål, and [nob_Latn.md5](https://data.hplt-project.org/three/sorted/nob_Latn.md5) for its full set of data files.

## License and takedown
### License

These data are released under this licensing scheme:
 - We do not own any of the text from which these text data has been extracted.*
 - We license the actual packaging of these text data under the [Creative Commons CC0 license ("no rights reserved")](https://creativecommons.org/share-your-work/public-domain/cc0/).

### Notice and take down policy

Notice: Should you consider that our data contains material that is owned by you and should therefore not be reproduced here, please:
 - Clearly identify yourself, with detailed contact data such as an address, telephone number or email address at which you can be contacted.
 - Clearly identify the copyrighted work claimed to be infringed.
 - Clearly identify the material that is claimed to be infringing and information reasonably sufficient to allow us to locate the material.
 - You can reach us at [hplt-datasets@ufal.mff.cuni.cz](mailto:hplt-datasets@ufal.mff.cuni.cz)

Take down: We will comply to legitimate requests by removing the affected sources from the next release of the corpora.

* _It is your responsibility that any use of the data complies with any applicable legal framework, such as, among others, the EU Copyright Directive 2019/790 and the General Data Protection Regulation 2018, as amended._

## Cite us

```
@misc{oepen2025hplt30largescalemultilingual,
      title={HPLT 3.0: Very Large-Scale Multilingual Resources for LLM and MT. Mono- and Bi-lingual Data, Multilingual Evaluation, and Pre-Trained Models}, 
      author={Stephan Oepen and Nikolay Arefev and Mikko Aulamo and Marta Bañón and Maja Buljan and Laurie Burchell and Lucas Charpentier and Pinzhen Chen and Mariya Fedorova and Ona de Gibert and Barry Haddow and Jan Hajič and Jindřich Helcl and Andrey Kutuzov and Veronika Laippala and Zihao Li and Risto Luukkonen and Bhavitvya Malik and Vladislav Mikhailov and Amanda Myntti and Dayyán O'Brien and Lucie Poláková and Sampo Pyysalo and Gema Ramírez Sánchez and Janine Siewert and Pavel Stepachev and Jörg Tiedemann and Teemu Vahtola and Dušan Variš and Fedor Vitiugin and Tea Vojtěchová and Jaume Zaragoza},
      year={2025},
      eprint={2511.01066},
      archivePrefix={arXiv},
      primaryClass={cs.CL},
      url={https://arxiv.org/abs/2511.01066}, 
}
```
## Funding
_This project has received funding from the European Union’s Horizon Europe research and innovation programme under grant agreement No 101070350 and from UK Research and Innovation (UKRI) under the UK government’s Horizon Europe funding guarantee (grant number 10052546)_
