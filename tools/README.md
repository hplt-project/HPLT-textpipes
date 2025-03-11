# HPLT-WP2

Collected information related to work package 2 on Data Ingest and Management (M1 - M36)


* [Notes from meetings on parallel data](https://docs.google.com/document/d/1tBUbn3iyFuM0kSFYDs-CfwgK6Js6lS3DM49frMvTw1w/edit?usp=sharing)
* [HPLT overleaf template](https://www.overleaf.com/7181487261rqbcnknkkppy)
* [Links to tools and other resources](TOOLS.md)

## Code

- `smart_rename.py`: a Python script to rename monotexted files using sequential numbering (after merging multiple crawls together)


## Objectives

This work package will source training data then organise it in consistent formats ready for cleaning by WP 3. Data will be gathered from a variety of sources including web archives, national libraries, CLARIN, ELRC-SHARE, and of course the OPUS repository run by UH. Key performance indicators are:

* 7 petabytes of web archive (WARC) files from web crawls on a supercomputer.
* 2.5 trillion words of monolingual text; a breakdown by language appeared in Table 1.
* 600 unique corpora ingested.

Having 7 petabytes of web archive data on a supercomputer is alone a major objective and we have been in touch with potential users interested in historical wage growth from job advertisements, URI usage patterns, disinformation, language evolution, speech recognition, and grammatical error correction. As discussed in Section 1.1.1, language resource repositories like ELRC-SHARE often have errors and inconsistencies that make them hard to use at scale. Therefore this work package aims to format data consistently, ready for machine-readable consumption. The organization of the resulting clean data will follow the DMP established in WP1.


### Task T2.1: Storage and Compute Infrastructure for the HPLT data space (CESNET, SIGMA2)

This task handles the storage and computational infrastructure operated by project partners. It includes standard operation of the systems, resolving operational issues, and handling necessary changes to the infrastructure induced by the fact that the datasets to process are of challenging size. Necessary activities include advanced user support (with respect to specific tools deployed for data processing) as well as advising users how to run data transfers and computation jobs on particular infrastructures including optimisation of the processes to efficiently utilise available resources.

### Task T2.2: Web Archive (UEDIN)

This task sources trillions of words of text by procuring, downloading, and extracting text from 7 petabytes of web archives. We will acquire 7 petabytes crawled from the web in Web
Archive (WARC) format, normalise character encoding, discard markup, classify language, and extract plain text using warc2text, which was developed in the context of the EU-funded ParaCrawl project. An indicative size of the text was shown in Table 1.

### Task T2.3: Monolingual data (UTU, UiO)

This task sources monolingual texts from existing non-web archives, targeting in particular lesser-resourced languages for which sufficient quantities of high quality text
for very large language model training may be challenging to assemble from web archives. To source tens or hundreds of billions of words of text in lesser-resourced languages, the task will primarily consider the digital archives of various National Libraries, addressing in particular the technical challenges involved in extracting plain text from archive formats, generating numerical data for language model training from text sources that may not permit copying, and maintaining secure communication and storage facilities for such data. The task will build on tools and protocols established in ongoing collaborations with the National Libraries of Finland (UTU) and Norway (UiO).

### Task T2.4: Parallel data (UH, UEDIN)

This task will focus on gathering parallel data from diverse sources and its organisation in a consistent and well-defined format. We will implement and streamline import procedures
that are able to handle translation data in various formats. The task includes the robust conversion of pre-aligned data sources like TMX and XLIFF as well as text extraction, document alignment and sentence alignment from non-aligned sources. Parallel text identification and extraction techniques developed in the ParaCrawl project will be reused and refined. We will emphasize high language coverage also for bitexts that are not English-centric. The uniform storage format and procedures will be designed to provide traightforward and optimal data access for batch runs and MT training procedures. Appropriate open access APIs and software libraries will be implemented and published. Upload options and efficient data contribution routines will be developed and released in order to provide a sustainable data hub for parallel text collections that can easily scale in future developments. We will also integrate data cleaning and quality estimation procedures developed in WP3 and make metadata available through the access APIs and data downloads.


## Deliverables

* D2.1:  Initial release of monolingual and parallel data sets, M12 (lead: UH) - August 2023
* D2.2:  Final release of monolingual and parallel data sets, M36 (lead: UH) - August 2025


## Milestones

* MS 1: Contract with web archive in place, initial compute allocated, project website, M3 -- done?
* MS 2: DMP, Initial models built on ingested data (WP2-5), M6
* MS 3: Corpus analytics running on data (WP2 and 3), M12
* MS 4: Models trained automatically, initial evaluations, basic dashboard ready, M18
* MS 5: All datasets ready and cleaned, cleaning tools released, advanced dashboard ready, 1-2 dissemination/exploitation events organized, metadata ready, M30
* MS 6: Models ready, M33



# Progress and links

## Updated OPUS

* [OPUS git repository](https://github.com/Helsinki-NLP/OPUS)
* [OPUS data ingest recipes and tools](https://github.com/Helsinki-NLP/OPUS-ingest)


## Collecting information about data sources:

* [google sheet with parallel data resources](https://docs.google.com/spreadsheets/d/1f0zcLfQ80uRrUGpmQHHZlS-p1iNeX691Qpw_sW66uDQ/edit#gid=0)
* [google sheet with MT test sets](https://docs.google.com/spreadsheets/d/1Xyk3dyocmjmY4__bLxZzAImg0MtkBpaPxgD09kmpcIg/edit#gid=0)
* [google sheet with monolingual data sources](https://docs.google.com/spreadsheets/d/1YO2QRk3TJjCqx4OcvfaTcR_ASzMli1A5PfcPVUm5i4E/edit#gid=0)


## Data sheets and metadata:

* [ideas for metadata in parallel data cards](https://docs.google.com/document/d/1UXLj2v9CLRPNd3nq_5dq8amqh4mz-CGRRH0NKvpfM4I/edit#heading=h.8qxxsbh5x98x)
* [dataset cards at huggingface](https://huggingface.co/docs/datasets/dataset_card)
* [dataset card template at huggingface](https://github.com/huggingface/datasets/tree/main/templates)
