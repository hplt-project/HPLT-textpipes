#!/usr/bin/env python

# -*- coding: utf-8; -*-

import argparse;
import io;
from operator import itemgetter;
import os;
import sys;
import time;
import zstandard as zstd;

def parse(
def main():

  parser = argparse.ArgumentParser(description = "HPLT Stage 5: Sort and Package for Release");
  parser.add_argument("--level", type = int, default = 10);
  parser.add_argument("--start", type = int, default = 1);
  parser.add_argument("--max", type = int);
  parser.add_argument("--min", type = int);
  parser.add_argument("--n", type = int);
  parser.add_argument("inputs", nargs = "*");
  arguments = parser.parse_args();

  inputs = [];
  for file in arguments.inputs:
    decompressor = zstd.ZstdDecompressor();
    stream = decompressor.stream_reader(open(file, "rb"));
    stream = io.TextIOWrapper(stream, encoding = "utf-8", errors = "replace");
    try:
      line = next(stream);
      _ = line.find("\t");
      key = float(line[:_]);
      line = line[_ + 1:];
    except Exception as error:
      print(f"merge: ignoring input file {file}: {error}",
            file = sys.stderr);
      continue;
    inputs.append((key, {"stream": stream, "file": file, "n": 1, "top": line}));

  prefix = arguments.start;
  outputs = dict();
  while len(inputs):
    inputs.sort(key = itemgetter(0));
    key, input = inputs.pop();
    bin = int(key);
    if bin not in outputs:
      name = f"{prefix}_{bin}_jsonl.zst";
      compressor = zstd.ZstdCompressor(level = arguments.level);
      stream = compressor.stream_writer(open(name, "wb"));
      stream = io.TextIOWrapper(stream, encoding = "utf-8", errors = "replace");
      outputs[bin] = {"file": name, "stream": stream};
    outputs[bin]["stream"].write(input["top"]);
    line = next(input["stream"], None);
    if line is None:
      input["stream"].close();
    else:
      try:
        input["n"] += 1;
        _ = line.find("\t");
        key = float(line[:_]);
        line = line[_ + 1:];
      except Exception as error:
        print("merge: aborting input file {file}, #{}: {error}"
              "".format(input["file"], input["n"], error),
              file = sys.stderr);
        input["stream"].close();
        continue;
      input["top"] = line;
      inputs.append((key, input));


if __name__ == "__main__":
  main();
