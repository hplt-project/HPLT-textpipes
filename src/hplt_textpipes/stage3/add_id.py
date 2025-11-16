#!/usr/bin/env python3
import sys
import json
from xxhash import xxh128_hexdigest
import fire
from smart_open import open

def process(input_path="-"):
    """
    Reads JSON lines from an input file, adds a calculated 'id' field,
    and prints the modified JSON object to stdout.

    :param input_path: Path to the input file, or '-' for stdin (default).
    """
    with sys.stdin if input_path=='-' else open(input_path, 'rt', encoding='utf-8') as f_in:
        for line in f_in:
            doc = json.loads(line)
            doc["id"] = xxh128_hexdigest(doc["f"] + doc["u"] + doc["ts"])
            print(json.dumps(doc))

if __name__ == "__main__":
    fire.Fire(process)




