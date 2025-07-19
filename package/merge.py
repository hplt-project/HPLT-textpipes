#!/usr/bin/env python

# -*- coding: utf-8; -*-

import argparse;
import io;
from operator import itemgetter;
import os;
import sys;
import time;
import zstandard as zstd;

def parse(input, min, max):
  line = next(input["stream"], None);
  if line is None:
    input["stream"].close();
    return None, None;
  input["n"] += 1;
  try:
    _ = line.find("\t");
    key = float(line[:_]);
    line = line[_ + 1:];
  except Exception as error:
    print("merge.py: aborting input from {file}, #{}: {error}"
          "".format(input["file"], input["n"], error),
          file = sys.stderr);
    input["stream"].close();
    return None, None;
  input["line"] = line;
  return key, input;
    
def main():

  start = time.time();

  parser = argparse.ArgumentParser(description = "HPLT Stage 5: Sort and Package for Release");
  parser.add_argument("--level", type = int, default = 10);
  parser.add_argument("--start", type = int, default = 1);
  parser.add_argument("--max", type = int);
  parser.add_argument("--min", type = int);
  parser.add_argument("--n", type = int);
  parser.add_argument("inputs", nargs = "*");
  arguments = parser.parse_args();

  #
  # open all input files and scan the first line
  #
  inputs = [];
  for file in arguments.inputs:
    decompressor = zstd.ZstdDecompressor();
    stream = decompressor.stream_reader(open(file, "rb"));
    stream = io.TextIOWrapper(stream, encoding = "utf-8", errors = "replace");
    input = {"stream": stream, "file": file, "n": 0};
    key, input = parse(input, arguments.min, arguments.max);
    if key is None: continue;
    inputs.append((key, input));

  n = 0;
  prefix = arguments.start;
  outputs = dict();
  while len(inputs):
    #
    # _fix_me_ should use a genuine priority queue
    # 
    inputs.sort(key = itemgetter(0));
    key, input = inputs.pop();
    bin = int(key);
    #
    # create one output file per bin, counting from _prefix_
    #
    if bin not in outputs:
      name = f"{prefix}_{bin}_jsonl.zst";
      compressor = zstd.ZstdCompressor(level = arguments.level);
      stream = compressor.stream_writer(open(name, "wb"));
      stream = io.TextIOWrapper(stream, encoding = "utf-8", errors = "replace");
      outputs[bin] = {"file": name, "stream": stream};
    #
    #
    #
    outputs[bin]["stream"].write(input["line"]);
    n += 1;
    #
    # 
    #
    key, input = parse(input, arguments.min, arguments.max);
    if key is None: continue;
    else: inputs.append((key, input));
  for output in outputs.values(): output["stream"].close();
  print("merge.py: {} documents; {} inputs; {} outputs; {} seconds."
        "".format(n, len(arguments.inputs), len(outputs), time.time() - start),
        file = sys.stderr);

if __name__ == "__main__":
  main();
