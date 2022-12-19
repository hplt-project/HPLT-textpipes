# Links to relevant tools


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


Data filtering / curation

* https://github.com/Helsinki-NLP/OpusFilter
* https://github.com/bitextor/bicleaner
* https://github.com/bitextor/bifixer
* http://corpus.tools/
  * http://corpus.tools/wiki/Justext - boilerplate removal
  * http://corpus.tools/wiki/Chared - character encoding detection
  * http://corpus.tools/wiki/Onion - duplicate removal


Data conversion

* https://tika.apache.org/
* https://github.com/bitextor/pdf-extract
* https://github.com/jrmuizel/pdf-extract and https://github.com/Aleph-Alpha/pdf-extract


Language identification

* https://pypi.org/project/pycld2/
* https://pypi.org/project/pycld3/
* https://pypi.org/project/fasttext-langdetect/
* https://pypi.org/project/fastspell/
* https://pypi.org/project/langdetect/
* https://github.com/saffsd/langid.py ([pypi](https://pypi.org/project/langid/), [py3langid](https://pypi.org/project/py3langid/))
* https://pypi.org/project/polyglot/ (and https://github.com/aboSamoor/polyglot)
* [HeLI-OTS](https://zenodo.org/record/6077089)


Sentence boundary detection:

* Moses sentence splitter: [python](https://pypi.org/project/sentence-splitter/), [mosestokenizer/python](https://pypi.org/project/mosestokenizer/), [perl/CPAN](https://metacpan.org/pod/Lingua::Sentence), [original perl scripts](https://www.statmt.org/europarl/v7/tools.tgz) [ssplit-cpp](https://github.com/ugermann/ssplit-cpp)
* https://ufal.mff.cuni.cz/udpipe
* https://github.com/rewicks/ersatz


Tokenization:

* https://pypi.org/project/polyglot/ (and https://github.com/aboSamoor/polyglot)
* https://pypi.org/project/fast-mosestokenizer/
* https://pypi.org/project/mosestokenizer/
* https://github.com/alvations/sacremoses and https://github.com/isi-nlp/sacremoses
* https://github.com/marian-nmt/moses-scripts
* http://corpus.tools/wiki/Unitok


Document alignment

* https://github.com/bitextor/bitextor/tree/master/document-aligner


Sentence alignment

* https://github.com/Helsinki-NLP/yasa
* https://github.com/danielvarga/hunalign
* https://github.com/rsennrich/bleualign ([pypi](https://pypi.org/project/pypi-bleualign/))
* https://github.com/bitextor/bleualign-cpp


Word alignment

* https://github.com/robertostling/eflomal
* https://github.com/clab/fast_align
* https://github.com/cisnlp/simalign
* https://github.com/neulab/awesome-align
