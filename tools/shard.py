#!/usr/bin/env python

# -*- coding: utf-8; -*-

import argparse;
import io;
import os;
import sys;
import time;
import zstandard;

class sharder():
  #
  # borrowed from monotextor, by jaume zaragoza (prompsit)
  #
  def __init__(self, path, size = 1e11, buffer = 4 * 1024 ** 2,
               prefix = None, level = 3, cores = 1):
    self.compressor = zstandard.ZstdCompressor(level = level, threads = cores);
    self.size = size;
    self.buffer = buffer;
    self.prefix = prefix;
    if not os.path.isdir(path): os.mkdir(path);
    self.path = path;
    self.bytes = 0;
    self.files = 0;
    self.stream = None;
    self.next();

  def next(self):
    if self.stream: self.stream.close();
    self.bytes = 0;
    self.files += 1;
    _ = (self.prefix + "_" if self.prefix is not None else "");
    file = os.path.join(self.path, _ + str(self.files) + ".jsonl.zst");
    self.stream = io.BufferedWriter(zstandard.open(file, "wb", cctx = self.compressor),
                                    buffer_size = self.buffer);

  def write(self, chunk):
    if self.bytes >= self.size: self.next();
    self.stream.write(chunk);
    self.stream.flush();
    self.bytes += len(chunk);

  def close(self):
    if self.stream is not None: self.stream.close();

  def __del__(self):
    self.close();

def main():

  def now():
    return time.strftime("%H:%M:%S (%d-%b-%y)").lower();

  start = time.time();

  parser = argparse.ArgumentParser(description = "re-package compressed files into ");
  parser.add_argument("--cores", type = int, default = 8);
  parser.add_argument("--level", type = int, default = 10);
  parser.add_argument("--size", type = int, default = 1e11);
  parser.add_argument("--buffer", type = int, default = 4 * 1024 ** 2);
  parser.add_argument("--target", default = ".");
  parser.add_argument("inputs", nargs = "*");
  arguments = parser.parse_args();

  if not os.path.isdir(arguments.target):
    print("[{}] sharder.py: invalid target directory {}; exit."
          "".format(now(), arguments.target),
          file = sys.stderr, flush = True);
    sys.exit(1);
      
  output = sharder(arguments.target, arguments.size, arguments.buffer,
                   None, arguments.level, max(1, arguments.cores - 1));
  print("[{}] sharder.py: reading {} files(s)."
        "".format(now(), len(arguments.inputs)),
        file = sys.stderr, flush = True);

  f, s, d = 0, 0, 0;
  for file in arguments.inputs:
    f += 1;
    if not os.path.isfile(file):
      print("[{}] sharder.py: invalid input {}; skip."
            "".format(now(), file),
            file = sys.stderr, flush = True);
      s += 1;
      continue;
    stream = io.BufferedReader(zstandard.open(file, "rb"),
                               buffer_size = arguments.buffer);
    for line in stream:
      d += 1;
      output.write(line);
    stream.close();
    
  print("shard.py: {} files (- {} skipped); {} documents; {:.2f} seconds."
        "".format(f, s, d, time.time() - start),
        file = sys.stderr, flush = True);

if __name__ == "__main__":
  main();
