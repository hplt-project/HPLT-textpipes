#!/usr/bin/env python

# -*- coding: utf-8; -*-

import argparse;
import glob;
import io;
import math;
import orjson;
import os;
from pathlib import Path;
import regex;
from subprocess import Popen, PIPE;
import sys;
import time;
import traceback;
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

def now():
  return time.strftime("%H:%M:%S (%d-%b-%y)").lower();

def compatible(openlid, glotlid):
  openlid = openlid.split("_");
  glotlid = glotlid.split("_");
  if openlid == glotlid:
    return True;
  elif (openlid[0] == glotlid[0]
        and openlid[1] in {"Hans", "Hant"}
        and glotlid[1] == "Hani"):
    return True;
  else:
    return False;

def connect(path, buffer):
  if not os.path.isfile(path):
    print("merge.py: invalid input file {}; exit."
          "".format(path),
          file = sys.stderr, flush = True);
    sys.exit(1);
  _ = io.BufferedReader(zstandard.open(path, "rb"), buffer_size = buffer);
  return _;

def parse(chunk, trace, i):
  result = None;
  try:
    if isinstance(chunk, bytes): chunk = chunk.decode("utf-8", errors = "strict");
    result = orjson.loads(chunk);
  except UnicodeError as error:
    if trace > 1:
      print("[{}] merge.py: failed to decode bytes object {}; skip."
            "".format(now(), chunk),
            file = sys.stderr, flush = True);
  except Exception as error:
    if trace > 1:
      print("[{}] merge.py: failed to parse string {}; skip."
            "".format(now, chunk),
            file = sys.stderr, flush = True);
  return result;

def shipout(document, blocked, noisy, clean, size, buffer, level, cores, trace, i):
  if ("filter" not in document
      or "doc_scores" not in document
      or "openlid_v3" not in document
      or "glotlid_v3" not in document):
    print("[{}] merge.py: missing obligatory field(s) (line #{}); skip."
          "".format(now(), i),
          file = sys.stderr, flush = True);
    return;

  filter = document["filter"];
  wds = document["doc_scores"][0];
  openlid = document["openlid_v3"];
  glotlid = document["glotlid_v3"];
  if filter == "optout":
    output = blocked;
  elif filter != "keep":
    noisy["f"] += 1;
    output = noisy;
  elif wds < 0.5:
    noisy["w"] += 1;
    output = noisy;
  elif openlid["prob"][0] < 0.5:
    noisy["p"] += 1;
    output = noisy;
  elif not compatible(openlid["lang"][0], glotlid["lang"][0]):
    noisy["l"] += 1;
    output = noisy;
  else:
    output = clean;
  wds = math.floor(wds * 10);
  if wds not in output:
    output[wds] = sharder(output["path"], size = size, buffer = buffer,
                          prefix = str(wds), level = level, cores = cores);
  output[wds].write(orjson.dumps(document, option = orjson.OPT_APPEND_NEWLINE));
  output["n"] += 1;

def main():

  start = time.time();

  parser = argparse.ArgumentParser(description = "HPLT 4.0 Merge & Filter");
  parser.add_argument("--cores", type = int, default = 1);
  parser.add_argument("--level", type = int, default = 3);
  parser.add_argument("--size", type = int, default = 1e11);
  parser.add_argument("--buffer", type = int, default = 4 * 1024 ** 2);
  parser.add_argument("--blocked", type = str, required = True);
  parser.add_argument("--noisy", type = str, required = True);
  parser.add_argument("--clean", type = str, required = True);
  parser.add_argument("--trace", action = "count", default = 0);
  parser.add_argument("inputs", nargs = "*");
  arguments = parser.parse_args();

  io.DEFAULT_BUFFER_SIZE = arguments.buffer;
  if not os.path.isdir(arguments.blocked):
    print("merge.py: invalid --blocked target directory {}; exit."
          "".format(arguments.blocked),
          file = sys.stderr, flush = True);
    sys.exit(1);
  blocked = {"path": arguments.blocked, "n": 0};
  if not os.path.isdir(arguments.noisy):
    print("merge.py: invalid --noisy target directory {}; exit."
          "".format(arguments.noisy),
          file = sys.stderr, flush = True);
    sys.exit(1);
  noisy = {"path": arguments.noisy, "f": 0, "w": 0, "p": 0, "l": 0, "n": 0};
  if not os.path.isdir(arguments.clean):
    print("merge.py: invalid --clean target directory {}; exit."
          "".format(arguments.clean),
          file = sys.stderr, flush = True);
    sys.exit(1);
  clean = {"path": arguments.clean, "n": 0};
  
  if arguments.trace > 0:
    print("[{}] merge.py: reading {} input file(s)."
          "".format(now(), len(arguments.inputs)),
          file = sys.stderr, flush = True);
  #
  # iterate over files provided on command line
  #
  annotations = [];
  for i, file in enumerate(arguments.inputs[1:]):
    stream = connect(file, arguments.buffer);
    table = dict();
    for j, line in enumerate(stream):
      if not line.startswith(b"{"):
        print("merge.py: invalid JSON object {} ({}: #{}); exit."
              "".format(line, arguments.inputs[i + 1], j),
              file = sys.stderr, flush = True);
        sys.exit(1);
      annotation = parse(line.rstrip(), arguments.trace, i);
      if "id" not in annotation:
        print("merge.py: missing .id. in annotation ({}: #{}); skip."
              "".format(arguments.inputs[i + 1], i),
              file = sys.stderr, flush = True);
      else:
        _ = annotation["id"];
        table[_] = annotation.pop("id");
    annotations.append(table);
    stream.close();
      
  documents = connect(arguments.inputs[0], arguments.buffer);
  #
  # process one document at a time, aligned by .id. across multiple files
  #
  for i, line in enumerate(documents):
    if not line.startswith(b"{"):
      print("merge.py: invalid JSON object {} ({}: #{}); exit."
            "".format(line, arguments.inputs[0], i),
            file = sys.stderr, flush = True);
      sys.exit(1);
    document = parse(line.rstrip(), arguments.trace, i);
    for table in annotations:
      annotation = table.get(id, None);
      if annotation is not None: document |= annotation;
    #
    # interpret annotations and route into various output bins
    #
    shipout(document, blocked, noisy, clean,
            arguments.size, arguments.buffer, arguments.level,
            arguments.cores, arguments.trace, i);
  #
  # wrap up: close all input and output streams
  #
  documents.close();
  for _ in blocked.values():
    if isinstance(_, sharder): _.close();
  for _ in noisy.values():
    if isinstance(_, sharder): _.close();
  for _ in clean.values():
    if isinstance(_, sharder): _.close();
  if arguments.trace > 0:
    print("[{}] merge.py: {} documents; blocked: {}; noisy: {} + {} + {} + {} = {}; clean: {}; {:.2f} seconds."
          "".format(now(), i + 1,
                    blocked["n"],
                    noisy["f"], noisy["w"], noisy["p"], noisy["l"], noisy["n"],
                    clean["n"], time.time() - start),
          file = sys.stderr, flush = True);
  sys.exit(0);

if __name__ == "__main__":
  main();
