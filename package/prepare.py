#!/usr/bin/env python

# -*- coding: utf-8; -*-

import argparse;
import io;
import json;
import os;
import sys;
import time;
import zstandard as zstd;

def wds_prepend(path, key = "doc_scores"):

  name = os.path.basename(path);
  if name.endswith(".zst"):
    name = name[:-len(".zst")];
    dctx = zstd.ZstdDecompressor();
    with open(path, "rb") as stream:
      with dctx.stream_reader(stream) as stream:
        stream = io.TextIOWrapper(stream, encoding = "utf-8", errors = "replace");
        start = time.time();
        n = 0;
        errors = [];
        for i, line in enumerate(stream):
          line = line.rstrip();
          try:
            document = json.loads(line);
            wds = document[key][0];
          except Exception as error:
            errors.append(i);
            print(f"wds_prepend: ignoring line {i}: {error}",
                  file = sys.stderr);
            continue;
          print(f"{wds}\t{line}");
        print(f"{n} documents; {time.time() - start} seconds.",
              file = sys.stderr);
  else:
    print(f"wds_prepend(): invalid input {path}.",
          file = sys.stderr);

def main():

  parser = argparse.ArgumentParser(description = "HPLT Stage 5: Sort and Package for Release");
  parser.add_argument("input");
  arguments = parser.parse_args();

  wds_prepend(arguments.input);

if __name__ == "__main__":
  main();
