#!/usr/bin/env python

# -*- coding: utf-8; -*-

import argparse;
import glob;
import io;
import json;
import os;
from subprocess import Popen, PIPE;
import sys;
import time;

def main():

  start = time.time();

  parser = argparse.ArgumentParser(description = "HPLT ");
  parser.add_argument("--cores", type = int, default = 8);
  parser.add_argument("--buffer", type = int, default = 4 * 1024 ** 2);
  parser.add_argument("--raw", action = "store_true");
  parser.add_argument("--filter", type = str, default = None);
  parser.add_argument("inputs", nargs = "*");
  arguments = parser.parse_args();

  #
  # increase output buffer size
  #
  sys.stdout = open(1, "w", encoding = "utf-8", buffering = arguments.buffer);
  
  #
  # open all input files and connect to a decompressing pipe
  #
  pipes = [];
  for path in arguments.inputs:
    if not os.path.isfile(path):
      print("zstdconcat.py: invalid input file {}; exit"
            "".format(path),
            file = sys.stderr, flush = True);
      sys.exit(1);
    pipes.append(Popen(["zstdcat", path], bufsize = arguments.buffer,
                       stdout = PIPE, encoding = "utf-8", errors = "strict",
                       stderr = sys.stdout));
  print("zstdconcat.py: reading {} input file(s)."
        "".format(len(pipes)), file = sys.stderr, flush = True);
  n = len(pipes);
  if n:
    for i, line in enumerate(pipes[0].stdout):
      line = line.rstrip();
      if arguments.raw:
        if n > 1: line = line[:-1];
        for j, pipe in enumerate(pipes[1:]):
          _ = next(pipe.stdout);
          if _ is None:
            print("zstdconcat.py: premature end of file on {}; exit"
            "".format(arguments.inputs[j + 1]),
            file = sys.stderr, flush = True);
            sys.exit(1);
          if j < n - 2:
            line += "," + _.rstrip()[1:-1];
          else:
            line += "," + _.rstrip()[1:];
      print(line);
  print("zstdconcat.py: concatenaded {} input lines(s); {:.2f} seconds."
        "".format(i + 1, time.time() - start),
        file = sys.stderr, flush = True);
      

if __name__ == "__main__":
  main();
