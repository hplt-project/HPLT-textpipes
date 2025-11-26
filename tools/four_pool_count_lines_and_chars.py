#!/usr/bin/env python3

import sys
import os
import io
import json
import zstandard as zstd

def main():
    if len(sys.argv) != 2:
        print("Usage: four_pool_count_lines_and_chars.py INPUT_DIR", file=sys.stderr)
        sys.exit(1)

    input_dir = sys.argv[1]
    text_file = os.path.join(input_dir, "text.zst")
    metadata_file = os.path.join(input_dir, "metadata.zst")
    markup_file = os.path.join(input_dir, "markup.zst")

    if not os.path.exists(text_file):
        print(f"Error: {text_file} not found", file=sys.stderr)
        sys.exit(1)

    if not os.path.exists(metadata_file):
        print(f"Error: {metadata_file} not found", file=sys.stderr)
        sys.exit(1)

    if not os.path.exists(markup_file):
        print(f"Error: {markup_file} not found", file=sys.stderr)
        sys.exit(1)

    text_size = os.path.getsize(text_file)
    metadata_size = os.path.getsize(metadata_file)
    markup_size = os.path.getsize(markup_file)

    line_count = 0
    char_count = 0
    lang_chars = {}
    lang_docs = {}
    null_text_count = 0
    null_lang_count = 0
    xml_char_count = 0
    md_char_count = 0
    null_xml_count = 0
    null_md_count = 0

    with open(text_file, 'rb') as text_fh, open(metadata_file, 'rb') as meta_fh, open(markup_file, 'rb') as markup_fh:
        text_dctx = zstd.ZstdDecompressor()
        text_reader = text_dctx.stream_reader(text_fh)
        text_stream = io.TextIOWrapper(text_reader, encoding='utf-8')

        meta_dctx = zstd.ZstdDecompressor()
        meta_reader = meta_dctx.stream_reader(meta_fh)
        meta_stream = io.TextIOWrapper(meta_reader, encoding='utf-8')

        markup_dctx = zstd.ZstdDecompressor()
        markup_reader = markup_dctx.stream_reader(markup_fh)
        markup_stream = io.TextIOWrapper(markup_reader, encoding='utf-8')

        for text_line, meta_line, markup_line in zip(text_stream, meta_stream, markup_stream):
            line_count += 1

            text_line = text_line.strip()
            meta_line = meta_line.strip()
            markup_line = markup_line.strip()

            text_data = json.loads(text_line)
            if text_data['text'] is None:
                text_len = 0
                null_text_count += 1
            else:
                text_len = len(text_data['text'])
            # text_len = len(text_line) - 11  # length of '{"text":"' plus length of '"}'
            char_count += text_len

            meta_data = json.loads(meta_line)
            lang_value = meta_data['openlid-v3']["lang"]

            if lang_value is None:
                lang = "null"
                null_lang_count += 1
            else:
                lang = lang_value[0]

            if lang not in lang_chars:
                lang_chars[lang] = 0
            lang_chars[lang] += text_len
            
            if lang not in lang_docs:
                lang_docs[lang] = 0
            lang_docs[lang] += 1

            markup_data = json.loads(markup_line)

            xml_value = markup_data.get('xml')
            if xml_value is None:
                null_xml_count += 1
            else:
                xml_char_count += len(xml_value)

            md_value = markup_data.get('md')
            if md_value is None:
                null_md_count += 1
            else:
                md_char_count += len(md_value)

    result = {
        'lines': line_count,
        'chars': char_count,
        'lang_chars': lang_chars,
        'lang_docs': lang_docs,
        'null_text': null_text_count,
        'null_lang': null_lang_count,
        'xml_chars': xml_char_count,
        'md_chars': md_char_count,
        'null_xml': null_xml_count,
        'null_md': null_md_count,
        'text_size': text_size,
        'metadata_size': metadata_size,
        'markup_size': markup_size
    }

    print(json.dumps(result))

if __name__ == "__main__":
    main()
