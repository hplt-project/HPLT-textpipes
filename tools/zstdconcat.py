#!/usr/bin/env python

# -*- coding: utf-8; -*-

import argparse;
import glob;
import io;
import orjson;
import os;
from subprocess import Popen, PIPE;
import sys;
import time;
import zstandard;

def connect(path, pipe, buffer):
  if not os.path.isfile(path):
    print("zstdconcat.py: invalid input file {}; exit"
          "".format(path),
          file = sys.stderr, flush = True);
    sys.exit(1);
  if pipe:
    _ = Popen(["zstdcat", path], bufsize = buffer,
              stdout = PIPE, encoding = "utf-8", errors = "strict",
              stderr = sys.stdout);
    return _.stdout;
  else:
    decompressor = zstandard.ZstdDecompressor();
    _ = decompressor.stream_reader(open(path, "rb"), read_size = buffer);
    _ = io.TextIOWrapper(_, encoding = "utf-8", errors = "strict");
    return _;

def main():

  start = time.time();

  parser = argparse.ArgumentParser(description = "HPLT ");
  parser.add_argument("--cores", type = int, default = 8);
  parser.add_argument("--buffer", type = int, default = 4 * 1024 ** 2);
  parser.add_argument("--pipe", action = "store_true");
  parser.add_argument("--raw", action = "store_true");
  parser.add_argument("--filter", type = str, default = None);
  parser.add_argument("inputs", nargs = "*");
  arguments = parser.parse_args();

  if not arguments.raw:
    print("zstdconcat.py: cooked (non-raw) processing not yet implemented; exit.",
          file = sys.stderr, flush = True);
    sys.exit(1);
    
  #
  # increase output buffer size
  #
  sys.stdout = open(1, "w", encoding = "utf-8", buffering = arguments.buffer);

  filter = None;
  if arguments.filter is not None:
    filter = connect(arguments.filter, arguments.pipe, arguments.buffer);
  #
  # open all input files and connect to a decompressing stream or pipe
  #
  streams = [connect(_, arguments.pipe, arguments.buffer)
             for _ in arguments.inputs];
  print("zstdconcat.py: {} {} input file(s)."
        "".format("filtering" if filter is not None else "reading",
                  len(streams)), file = sys.stderr, flush = True);
  n, s = len(streams), 0;
  if n:
    i = 0;
    for i, line in enumerate(streams[0]):
      if filter is not None:
        _ = next(filter);
        if _ is None:
          print("zstdconcat.py: premature end of file on {}; exit"
          "".format(arguments.filter),
          file = sys.stderr, flush = True);
          sys.exit(1);
        if arguments.raw and not "true" in _:
          for streap in streams[1:]: next(stream);
          s += 1;
          continue;
        
      line = line.rstrip();
      if arguments.raw:
        if n > 1: line = line[:-1];
        for j, stream in enumerate(streams[1:]):
          _ = next(stream);
          if _ is None:
            print("zstdconcat.py: premature end of file on {}; exit"
            "".format(arguments.inputs[j + 1]),
            file = sys.stderr, flush = True);
            sys.exit(1);
          if j < n - 2:
            line += "," + _.rstrip()[1:-1];
          else:
            line += "," + _.rstrip()[1:];
      try:
        print(line);
      except BrokenPipeError:
        break;
  print("zstdconcat.py: processed {} {}input lines(s); {:.2f} seconds."
        "".format(i + 1,
                  f"(- {s}) " if arguments.filter else "",
                  time.time() - start),
        file = sys.stderr, flush = True);
      

if __name__ == "__main__":
  main();
