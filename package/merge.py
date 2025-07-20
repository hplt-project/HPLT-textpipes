#!/usr/bin/env python

# -*- coding: utf-8; -*-

import argparse;
import glob;
import io;
from operator import itemgetter;
import os;
import sys;
import time;
import zstandard as zstd;

def parse(input, min, max):
  while True:
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
    #
    # skip documents whose WDS _key_ lies outside the [_min_, _max_[ range
    #
    if min is not None and key < min or max is not None and key >= max:
      continue;
    input["line"] = line;
    return key, input;
    
def main():

  start = time.time();

  parser = argparse.ArgumentParser(description = "HPLT Stage 5: Sort and Package for Release");
  parser.add_argument("--suffix", type = str, default = ".s.zst");
  parser.add_argument("--level", type = int, default = 10);
  parser.add_argument("--cores", type = int, default = 8);
  parser.add_argument("--start", type = int, default = 1);
  parser.add_argument("--min", type = int);
  parser.add_argument("--max", type = int);
  parser.add_argument("--n", type = int);
  parser.add_argument("inputs", nargs = "*");
  arguments = parser.parse_args();

  #
  # open all input files and scan their first line(s)
  #
  files = [];
  for path in arguments.inputs:
    if os.path.isdir(path):
      files.extend(glob.glob(os.path.join(path, "*" + arguments.suffix)));
    elif path.endswith(arguments.suffix) and os.path.isfile(path):
      files.append(path);
    else:
      print(f"merge.py: ignoring invalid path {path}",
            file = sys.stderr);
  print("merge.py: reading {len(files)} inputs");
  inputs = [];
  for file in files:
    decompressor = zstd.ZstdDecompressor();
    stream = decompressor.stream_reader(open(file, "rb"));
    stream = io.TextIOWrapper(stream, encoding = "utf-8", errors = "replace");
    input = {"stream": stream, "file": file, "n": 0};
    key, input = parse(input, arguments.min, arguments.max);
    if key is None: continue;
    inputs.append((key, input));

  n = o = 0;
  outputs = dict();
  while len(inputs):
    #
    # _fix_me_ should use a genuine priority queue
    # 
    inputs.sort(key = itemgetter(0));
    key, input = inputs.pop();
    bin = int(key);
    #
    # create one output file per _bin_, counting from the starting index
    #
    if bin not in outputs:
      name = f"{arguments.start}_{bin}_jsonl.zst";
      compressor = zstd.ZstdCompressor(level = arguments.level,
                                       threads = arguments.cores);
      stream = compressor.stream_writer(open(name, "wb"));
      stream = io.TextIOWrapper(stream, encoding = "utf-8", errors = "replace");
      outputs[bin] = {"file": name, "stream": stream, "i": arguments.start, "n": 0};
      o += 1;
    #
    # write the current document (sans the _key_ prefix) to its WDS bin
    #
    output = outputs[bin];
    output["stream"].write(input["line"]);
    output["n"] += 1;
    #
    # advance to new output file after _arguments.n_ documents
    #
    if arguments.n is not None and output["n"] >= arguments.n:
      output["stream"].close();
      name = "{}_{}_jsonl.zst".format(bin, output["i"] + 1);
      compressor = zstd.ZstdCompressor(level = arguments.level);
      stream = compressor.stream_writer(open(name, "wb"));
      stream = io.TextIOWrapper(stream, encoding = "utf-8", errors = "replace");
      outputs[bin] = {"file": name, "stream": stream, "i": output["i"] + 1, "n": 0};
      o += 1;
    n += 1;
    #
    # update next line and key from current input file;
    # re-insert into the priority queue, unless exhausted
    #
    key, input = parse(input, arguments.min, arguments.max);
    if key is None: continue;
    else: inputs.append((key, input));
  for output in outputs.values(): output["stream"].close();
  print("merge.py: {} documents; {} inputs; {} outputs; {} seconds."
        "".format(n, len(files), o, time.time() - start));

if __name__ == "__main__":
  main();
