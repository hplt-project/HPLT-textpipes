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

NOISE = ["adult_ut1", "adult_text", "length",
         "word_avg", "char_avg", "lang_ratio",
         "wds", "prob", "lid"];

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
    print("[{}] merge.py: invalid input file {}; exit."
          "".format(now(), path),
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
  output = clean;
  if filter == "optout":
    output = blocked;
  elif wds < 0.5:
    output = noisy;
    noisy["wds"] += 1;
  elif openlid["prob"][0] < 0.5:
    output = noisy;
    noisy["prob"] += 1;
  elif not compatible(openlid["lang"][0], glotlid["lang"][0]):
    output = noisy;
    noisy["lid"] += 1;
  else:
    for _ in NOISE:
      if _ not in {"wds", "prob", "lid"} and filter.startswith(_):
        output = noisy;
        noisy[_] += 1;
    #
    # guard against other, unknown .filter. values
    #
    if output != noisy and filter != "keep":
      output = noisy;
      noisy["filter"] += 1;
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
  parser.add_argument("--pattern", type = str, default = "*.zst");
  parser.add_argument("--blocked", type = str, required = True);
  parser.add_argument("--noisy", type = str, required = True);
  parser.add_argument("--clean", type = str, required = True);
  parser.add_argument("--trace", action = "count", default = 0);
  parser.add_argument("inputs", nargs = "*");
  arguments = parser.parse_args();

  io.DEFAULT_BUFFER_SIZE = arguments.buffer;
  if not os.path.isdir(arguments.blocked):
    print("[{}] merge.py: invalid --blocked target directory {}; exit."
          "".format(now(), arguments.blocked),
          file = sys.stderr, flush = True);
    sys.exit(1);
  blocked = {"path": arguments.blocked, "n": 0};
  if not os.path.isdir(arguments.noisy):
    print("[{}] merge.py: invalid --noisy target directory {}; exit."
          "".format(now(), arguments.noisy),
          file = sys.stderr, flush = True);
    sys.exit(1);
  noisy = {"path": arguments.noisy, "filter": 0, "n": 0};
  for _ in NOISE: noisy[_] = 0;
  if not os.path.isdir(arguments.clean):
    print("[{}] merge.py: invalid --clean target directory {}; exit."
          "".format(now(), arguments.clean),
          file = sys.stderr, flush = True);
    sys.exit(1);
  clean = {"path": arguments.clean, "n": 0};
  
  #
  # first positional argument is directory containing document batches
  #
  files = glob.glob(os.path.join(arguments.inputs[0], arguments.pattern));
  #
  # process one batch at a time, pairing up documents and annotations
  #
  n = 0;
  for file in files:
    name = os.path.basename(file);
    if arguments.trace > 0:
      print("[{}] merge.py: reading documents from {}, with {} annotations(s)."
            "".format(now(), name, len(arguments.inputs) - 1),
            file = sys.stderr, flush = True);
    annotations = [];
    keys = [];
    for path in arguments.inputs[1:]:
      _ = os.path.join(path, name);
      stream = connect(_, arguments.buffer);
      table = dict();
      key = None;
      for i, line in enumerate(stream):
        if not line.startswith(b"{"):
          print("[{}] merge.py: invalid JSON object {} ({}: #{}); exit."
                "".format(now(), line, _, i),
                file = sys.stderr, flush = True);
          sys.exit(1);
        annotation = parse(line.rstrip(), arguments.trace, i);
        if "id" not in annotation:
          print("[{}] merge.py: missing .id. in annotation ({}: #{}); skip."
                "".format(now(), _, i),
                file = sys.stderr, flush = True);
        else:
          if key is None:
            if "bsc-edu-1.0" in annotation:
              key = "bsc-edu-1.0";
              keys.append("bsc-edu");
            elif "finepdfs-edu" in annotation:
              key = "finepdfs-edu";
              keys.append(key);
            elif "fw2hq" in annotation:
              key = "fw2hq";
              keys.append("fineweb2-hq");
            elif "jql" in annotation:
              key = "jql";
              keys.append(key);
            elif "propella-4b" in annotation:
              key = "propella-4b";
              keys.append(key);
          table[annotation["id"]] = annotation[key]
      annotations.append(table);
      stream.close();

    if arguments.trace > 0:
      print("[{}] merge.py: using {} annotations(s)."
            "".format(now(), sum(len(_) for _ in annotations)),
            file = sys.stderr, flush = True);
    
    documents = connect(file, arguments.buffer);
    #
    # process one document at a time, aligned by .id. across multiple files
    #
    for i, line in enumerate(documents):
      if not line.startswith(b"{"):
        print("[{}] merge.py: invalid JSON object {} ({}: #{}); exit."
              "".format(now(), line, file, i),
              file = sys.stderr, flush = True);
        sys.exit(1);
      document = parse(line.rstrip(), arguments.trace, i);
      id = document["id"];
      for key, table in zip(keys, annotations):
        annotation = table.get(id, None);
        if annotation is not None: document[key] = annotation;
        elif arguments.trace > 0:
          print("[{}] merge.py: missing {} annotation for .id. {} ({}: #{}); exit."
                "".format(now(), key, id, file, i),
                file = sys.stderr, flush = True);

      #
      # interpret annotations and route into various output bins
      #
      shipout(document, blocked, noisy, clean,
              arguments.size, arguments.buffer, arguments.level,
              arguments.cores, arguments.trace, i);
    documents.close();
    n += (i + 1);
    if arguments.trace > 0:
      print("[{}] merge.py: processed {} documents(s)."
            "".format(now(), i + 1),
            file = sys.stderr, flush = True);

  #
  # wrap up: close all input and output streams
  #
  for _ in blocked.values():
    if isinstance(_, sharder): _.close();
  for _ in noisy.values():
    if isinstance(_, sharder): _.close();
  for _ in clean.values():
    if isinstance(_, sharder): _.close();
  if arguments.trace > 0:
    print("[{}] merge.py: {} documents; {} blocked; {} (+ {}) = {} noisy; {} clean; {:.2f} seconds."
          "".format(now(), n,
                    blocked["n"],
                    " + ".join(str(noisy.get(_, 0)) for _ in NOISE),
                    noisy["filter"], noisy["n"],
                    clean["n"], time.time() - start),
          file = sys.stderr, flush = True);
  sys.exit(0);

if __name__ == "__main__":
  main();
