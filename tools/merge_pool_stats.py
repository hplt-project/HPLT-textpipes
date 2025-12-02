#!/usr/bin/env python3

import sys
import os
import json
import glob
import argparse
from tqdm import tqdm

def create_empty_stats():
    return {
        'lines': 0,
        'chars': 0,
        'lang_chars': {},
        'lang_docs': {},
        'null_text': 0,
        'null_lang': 0,
        'xml_chars': 0,
        'md_chars': 0,
        'null_xml': 0,
        'null_md': 0,
        'text_size': 0,
        'metadata_size': 0,
        'markup_size': 0
    }

def merge_two_stats(merged, stats):
    merged['lines'] += stats['lines']
    merged['chars'] += stats['chars']
    merged['null_text'] += stats['null_text']
    merged['null_lang'] += stats['null_lang']
    merged['xml_chars'] += stats['xml_chars']
    merged['md_chars'] += stats['md_chars']
    merged['null_xml'] += stats['null_xml']
    merged['null_md'] += stats['null_md']
    merged['text_size'] += stats['text_size']
    merged['metadata_size'] += stats['metadata_size']
    merged['markup_size'] += stats['markup_size']

    for lang, count in stats['lang_chars'].items():
        if lang not in merged['lang_chars']:
            merged['lang_chars'][lang] = 0
        merged['lang_chars'][lang] += count

    for lang, count in stats['lang_docs'].items():
        if lang not in merged['lang_docs']:
            merged['lang_docs'][lang] = 0
        merged['lang_docs'][lang] += count

def main(stats_dir, output_dir, merge_all):
    print(f"Input directory: {stats_dir}")
    print(f"Output directory: {output_dir}")
    print(f"Mode: {'merge all' if merge_all else 'merge by crawl'}")
    print()

    os.makedirs(output_dir, exist_ok=True)

    json_files = glob.glob(os.path.join(stats_dir, "*.json"))

    if not json_files:
        print(f"No JSON files found in {stats_dir}", file=sys.stderr)
        sys.exit(1)

    print(f"Found {len(json_files)} JSON files")

    crawl_files = {}
    if merge_all:
        crawl_files['total'] = json_files
    else:
        for json_file in json_files:
            basename = os.path.basename(json_file)
            crawl = basename.split('.')[0]
            if crawl not in crawl_files:
                crawl_files[crawl] = []
            crawl_files[crawl].append(json_file)

    print(f"Processing {len(crawl_files)} crawl(s)")
    print()

    for crawl, files in tqdm(crawl_files.items(), desc="Processing crawls"):
        merged = create_empty_stats()
        for json_file in files:
            with open(json_file, 'r') as f:
                stats = json.load(f)
                merge_two_stats(merged, stats)

        output_file = os.path.join(output_dir, f"{crawl}.json")
        with open(output_file, 'w') as f:
            json.dump(merged, f, indent=2)

    print(f"Done! Written to {output_dir}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Merge pool statistics JSON files")
    parser.add_argument('stats_dir', help='Input directory containing JSON stats files')
    parser.add_argument('output_dir', help='Output directory')
    parser.add_argument('--all', action='store_true',
                       help='Merge all files into a single total.json file instead of grouping by crawl')

    args = parser.parse_args()
    main(args.stats_dir, args.output_dir, args.all)
