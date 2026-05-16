#!/usr/bin/env python

# -*- coding: utf-8; -*-

import argparse;
import io;
import orjson;
import os;
import sys;
import time;
import zstandard;

class sharder():
  #
  # borrowed from monotextor, by jaume zaragoza (prompsit)
  #
  def __init__(self, path, size = 1e11, buffer = 4 * 1024 ** 2,
               prefix = None, infix = None, level = 3, cores = 1):
    self.compressor = zstandard.ZstdCompressor(level = level, threads = cores);
    self.size = size;
    self.buffer = buffer;
    self.prefix = prefix;
    self.infix = infix;
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
    _ = (self.prefix + "_" if self.prefix is not None else "") + str(self.files);
    if self.infix is not None: _ += f".{self.infix}";
    file = os.path.join(self.path, _ + ".jsonl.zst");
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

def now():
  return time.strftime("%H:%M:%S (%d-%b-%y)").lower();

def connect(path, buffer):
  if not os.path.isfile(path):
    print("[{}] shard.py: invalid input file {}; exit."
          "".format(now(), path),
          file = sys.stderr, flush = True);
    sys.exit(1);
  _ = zstandard.open(path, "r", encoding = "utf-8", errors = "strict");
  return _;

def main():

  start = time.time();

  parser = argparse.ArgumentParser(description = "HPLT 4.0 Merge & Shard");
  parser.add_argument("--cores", type = int, default = 1);
  parser.add_argument("--level", type = int, default = 3);
  parser.add_argument("--size", type = int, default = 1e11);
  parser.add_argument("--buffer", type = int, default = 4 * 1024 ** 2);
  parser.add_argument("--prefix", type = str);
  parser.add_argument("--infix", type = str);
  parser.add_argument("--trace", action = "count", default = 0);
  parser.add_argument("--output", type = str, required = True);
  parser.add_argument("inputs", nargs = "*");
  arguments = parser.parse_args();

  io.DEFAULT_BUFFER_SIZE = arguments.buffer;

  if not os.path.isdir(arguments.output):
    print("[{}] shard.py: invalid --output target directory {}; exit."
          "".format(now(), arguments.output),
          file = sys.stderr, flush = True);
    sys.exit(1);

  output = sharder(arguments.output, size = arguments.size,
                   buffer = arguments.buffer,
                   prefix = arguments.prefix, infix = arguments.infix,
                   level = arguments.level, cores = arguments.cores);
  n = 0;
  for file in arguments.inputs:
    if not os.path.isfile(file):
      print("[{}] shard.py: invalid input file {}; exit."
            "".format(now(), file),
            file = sys.stderr, flush = True);
      sys.exit(1);
    if arguments.trace > 0:
      print("[{}] shard.py: reading documents from {}."
            "".format(now(), file),
            file = sys.stderr, flush = True);
      
    with io.BufferedReader(zstandard.open(file, "rb"),
                           buffer_size = arguments.buffer) as stream:
      for line in stream:
        output.write(line);
        n += 1;

  if arguments.trace > 0:
    print("[{}] shard.py: processed {:,} documents in {:,} file(s);"
          " {:.2f} seconds."
          "".format(now(), n, len(arguments.inputs),
                    time.time() - start),
          file = sys.stderr, flush = True);
  sys.exit(0);

if __name__ == "__main__":
  main();
