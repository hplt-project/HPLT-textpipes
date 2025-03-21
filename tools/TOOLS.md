# Links to relevant tools


Pipelines / workflows:

* https://github.com/hplt-project/empty-train
* https://github.com/hplt-project/empty-trainer

NMT training and decoding:

* https://github.com/marian-nmt/marian-dev
  * https://github.com/sfantao/lumi-marian/tree/mydev AMD Lumi compatible port (Private)
* https://github.com/browsermt/bergamot-translator glue code between marian and software that wants to integrate marian
  * Python: https://pypi.org/project/bergamot/ ([API usage example](https://github.com/browsermt/bergamot-translator/blob/main/bindings/python/cmds.py#L71-L99))
  * Javascript (client side) https://www.npmjs.com/package/@browsermt/bergamot-translator
  * REST Server: https://github.com/ugermann/mts
* https://github.com/XapaJIaMnu/translateLocally desktop software to run marian models
  * https://addons.mozilla.org/en-GB/firefox/addon/translatelocally-for-firefox/ Firefox integration
* https://github.com/browsermt/firefox-translations-training training pipeline
* https://github.com/OpenNMT/CTranslate2


MT Data preparation

* https://github.com/thammegowda/mtdata for downloading data
* https://pypi.org/project/opustools/ for downloading and converting data
* https://github.com/paracrawl/tmxutil for working with TMX files
* https://github.com/sortiz/tmxt extract TMX to Moses tab-separated format.

Data filtering / curation

* https://github.com/Helsinki-NLP/OpusFilter
* https://github.com/bitextor/bicleaner-ai
* https://github.com/bitextor/bicleaner
* https://github.com/bitextor/bifixer
* http://corpus.tools/
  * http://corpus.tools/wiki/Justext - boilerplate removal
  * http://corpus.tools/wiki/Chared - character encoding detection
  * http://corpus.tools/wiki/Onion - duplicate removal
* https://huggingface.co/spaces/huggingface/text-data-filtering links to a pdf but most of its links are dead
  * https://github.com/bigscience-workshop/data-preparation/tree/main/preprocessing seems to be the related filtering code

Data conversion

* https://tika.apache.org/
* https://github.com/bitextor/pdf-extract
* https://github.com/jrmuizel/pdf-extract and https://github.com/Aleph-Alpha/pdf-extract


Language identification

* https://pypi.org/project/pycld2/
* https://pypi.org/project/pycld3/
* https://pypi.org/project/fasttext-langdetect/
* https://pypi.org/project/fastspell/ (FastText predictions refined with Hunspell dictionaries)
* https://pypi.org/project/langdetect/
* https://github.com/saffsd/langid.py ([pypi](https://pypi.org/project/langid/), [py3langid](https://pypi.org/project/py3langid/))
* https://pypi.org/project/polyglot/ (and https://github.com/aboSamoor/polyglot)
* [HeLI-OTS](https://zenodo.org/record/6077089)


Sentence boundary detection:

* Moses sentence splitter: [python](https://pypi.org/project/sentence-splitter/), [mosestokenizer/python](https://pypi.org/project/mosestokenizer/), [perl/CPAN](https://metacpan.org/pod/Lingua::Sentence), [original perl scripts](https://www.statmt.org/europarl/v7/tools.tgz) [ssplit-cpp](https://github.com/ugermann/ssplit-cpp)
* https://ufal.mff.cuni.cz/udpipe
* https://github.com/rewicks/ersatz
* https://github.com/loomchild/segment Loomchild SRX segmenter
  * Python wrapper: https://github.com/bitextor/loomchild-segment-py


Tokenization:

* https://pypi.org/project/polyglot/ (and https://github.com/aboSamoor/polyglot)
* https://pypi.org/project/fast-mosestokenizer/
* https://pypi.org/project/mosestokenizer/
* https://github.com/alvations/sacremoses and https://github.com/isi-nlp/sacremoses
* https://github.com/marian-nmt/moses-scripts
* http://corpus.tools/wiki/Unitok


Document alignment

* https://github.com/bitextor/bitextor/tree/master/document-aligner based on MT
* https://github.com/bitextor/neural-document-aligner based on sentence embeddings


Sentence alignment

* https://github.com/Helsinki-NLP/yasa
* https://github.com/danielvarga/hunalign based on bilingual dictionaries
* https://github.com/rsennrich/bleualign ([pypi](https://pypi.org/project/pypi-bleualign/))
* https://github.com/bitextor/bleualign-cpp based on MT
* https://github.com/bitextor/vecalign based on sentence embeddings


Word alignment

* https://github.com/robertostling/eflomal
* https://github.com/clab/fast_align
* https://github.com/cisnlp/simalign
* https://github.com/neulab/awesome-align


Sentence embeddings:
* https://github.com/facebookresearch/LASER/
* https://github.com/facebookresearch/LASER/tree/main/nllb LASER2 and LASER3
* https://huggingface.co/sentence-transformers/LaBSE
