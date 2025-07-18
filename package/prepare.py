#!/usr/bin/env python

# -*- coding: utf-8; -*-

import io;
import glob;
import gzip;
import json;
import multiprocessing as mp;
import os;
import re;
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
          print(line);
          try:
            document = json.loads(line.rstrip());
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
