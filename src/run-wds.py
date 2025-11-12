#REQUIRES:
# python3.12 -m pip install orjson xxhash
# git clone https://github.com/pablop16n/web-docs-scorer & cd web-docs-scorer & python3.12 -m pip install .

#RUN:
# zstdcat [documents.jsonl.zst] | python3.12 run-wds.py
# documents must contain at least the following metadata: lang, text and id
# outputs the same documents in the input with their doc_scores 



import sys
import orjson

from xxhash import xxh128_hexdigest
from docscorer import DocumentScorer, ScorerConfiguration

config = ScorerConfiguration()
scorer = DocumentScorer(config)

# Convert certain langcodes to a form that WDS supports
# because WDS may have the macro or the individual and openlidv2 otherwise
# so we try to match 'flores_code' in
# https://huggingface.co/datasets/laurievb/OpenLID-v2/blob/cc7b18c/hplt/hplt_200_lang_labels.jsonl
# to language_3_chars here
# https://github.com/pablop16n/web-docs-scorer/blob/37a3f3421cc7495b9d7cb5bdd2ba36dad7e7db5d/src/docscorer/configurations/language_adaption/medians_language.csv
# if languages are not present in that csv, WDS will use standard generic values
# this function assumes input is a code from flores_codes
def get_lang_wds(langcode_script):
    if langcode_script == 'und' or langcode_script == 'unk':
        return langcode_script
    try:
        langcode, script = langcode_script.split('_')
    except ValueError as e:
        raise ValueError(f"Could not parse {lancode_script}") from e
    if langcode in ('prs', 'pes'):
        # openlidv2 uses individual codes for persian
        return f'fas_{script}'
    if langcode in ('cmn', 'yue'):
        # openlidv2 uses individual code for mandarin and cantonese
        return f'zho_{script}'
    if langcode_script in 'zsm_Latn':
        # wds only has 'ind' and 'msa'
        return 'msa_Latn'
    if langcode_script == 'swa_Latn':
        # wds has swahili individual
        return 'swh_Latn'
    if langcode in ('ara', 'acm', 'acq', 'aeb', 'apc', 'arb', 'ars', 'ary', 'arz'):
        # wds uses individual for arabic (standard), but any variant should be possible to be scored
        return f'arb_{script}'
    if langcode in ('lvs', 'ltg'):
        # wds uses macro for latvian
        return 'lav_Latn'

    return langcode_script
    

for line in sys.stdin:
    doc = orjson.loads(line)
    
    #wds_seg_langs = list(map(get_lang_wds, doc["seg_langs"])) # --> USE THIS IF seg_langs IS IN THE DOCUMENT, otherwhise use the code below
    segments  = doc["text"].split("\n")
    wds_seg_langs = [get_lang_wds(doc["lang"][0])] * len(segments)

    doc["doc_scores"] = scorer.score_document(
            ref_lang=get_lang_wds(doc["lang"][0]).split("_")[0], 
            ref_script=doc["lang"][0].split("_")[1],
            lang_segments=wds_seg_langs,
            document_text=doc["text"],
            doc_id=doc["id"],            
            raw_score=False,
            )

    sys.stdout.buffer.write(orjson.dumps(doc, option=orjson.OPT_SERIALIZE_NUMPY | orjson.OPT_APPEND_NEWLINE))

