#!/usr/bin/env python

# -*- coding: utf-8; -*-

import argparse;
import glob;
import io;
import json;
import multiprocessing as mp;
from operator import itemgetter;
import os;
import shutil;
import sys;
import time;
import zstandard as zstd;

def now():
  return time.strftime("%H:%M:%S (%d-%b-%y)").lower();

def parse(input):
  while True:
    line = next(input["stream"], None);
    if line is None:
      input["stream"].close();
      return None;
    input["n"] += 1;
    try:
    return line.strip("\"\n"), input;
    
def main():

  start = time.time();

  parser = argparse.ArgumentParser(description = "HPLT 4.0 Merge & Filter: Find Duplicate Document IDs");
  parser.add_argument("--suffix", type = str, default = ".ids.zst");
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
      print(f"[{now()}] merge.py: ignoring invalid input path {path}.",
            file = sys.stderr, flush = True);
  print(f"[{now()}] merge.py: reading {len(files)} inputs.",
        file = sys.stderr, flush = True);
  inputs = [];
  for file in files:
    decompressor = zstd.ZstdDecompressor();
    stream = decompressor.stream_reader(open(file, "rb"));
    stream = io.TextIOWrapper(stream, encoding = "utf-8", errors = "replace");
    input = {"stream": stream, "file": file, "n": 0};
    key, input = parse(input);
    if key is None: continue;
    inputs.append((key, input));

  last, flag, n = None, False, 0;
  while len(inputs):
    #
    # _fix_me_ should use a genuine priority queue
    # 
    inputs.sort(key = itemgetter(0));
    key, input = inputs.pop();
    n += 1;
    #
    # record duplicate keys, though only once
    #
    if key == last:
      if not flag:
        print(key);
        flag = True;
    else:
      last, flag = key, False
        
    #
    # update next line and key from current input file;
    # re-insert into the priority queue, unless exhausted
    #
    key, input = parse(input);
    if key is None: continue;
    else: inputs.append((key, input));
      
  print("[{}] merge.py: {} documents; {} inputs; {:.2f} seconds."
        "".format(now(), n, len(files), time.time() - start),
        file = sys.stderr, flush = True);

if __name__ == "__main__":
  main();
