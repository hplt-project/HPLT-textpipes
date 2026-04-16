#!/usr/bin/env python

# -*- coding: utf-8; -*-

import argparse;
import glob;
import io;
import json;
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

NOISE = ["wds", "lid", "prob",
         "adult_ut1", "adult_text", "length",
         "word_avg", "char_avg", "lang_ratio"];

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
    print("[{}] filter.py: invalid input file {}; exit."
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
      print("[{}] filter.py: failed to decode bytes object {}; skip."
            "".format(now(), chunk),
            file = sys.stderr, flush = True);
  except Exception as error:
    if trace > 1:
      print("[{}] filter.py: failed to parse string {}; skip."
            "".format(now, chunk),
            file = sys.stderr, flush = True);
  return result;

def shipout(document, blocked, noisy, clean, statistics,
            size, separate, buffer, level, cores, trace, i):
  if ("filter" not in document
      or "doc_scores" not in document
      or "openlid_v3" not in document
      or "glotlid_v3" not in document):
    print("[{}] filter.py: missing obligatory field(s) (line #{}); skip."
          "".format(now(), i),
          file = sys.stderr, flush = True);
    return;

  c = len(document["text"]);
  filter = document["filter"];
  wds = document["doc_scores"][0];
  openlid = document["openlid_v3"];
  glotlid = document["glotlid_v3"];
  output = clean;
  counts = statistics["clean"];
  if filter == "optout":
    output = blocked;
    counts = statistics["blocked"];
  elif wds < 0.5:
    output = noisy;
    document["noise"] = "wds";
    counts = statistics["noisy"];
    counts["wds"]["documents"] += 1;
    counts["wds"]["characters"] += c;
  elif not compatible(openlid["lang"][0], glotlid["lang"][0]):
    output = noisy;
    document["noise"] = "lid";
    counts = statistics["noisy"];
    counts["lid"]["documents"] += 1;
    counts["lid"]["characters"] += c;
  elif openlid["prob"][0] < 0.5:
    output = noisy;
    document["noise"] = "prob";
    counts = statistics["noisy"];
    counts["prob"]["documents"] += 1;
    counts["prob"]["characters"] += c;
  else:
    for _ in NOISE[3:]:
      if filter.startswith(_):
        output = noisy;
        document["noise"] = _;
        counts = statistics["noisy"];
        counts[_]["documents"] += 1;
        counts[_]["characters"] += c;
    #
    # guard against other, unexpected .filter. values
    #
    if output != noisy and filter != "keep":
      output = noisy;
      document["noise"] = filter;
      counts = statistics["noisy"];
      counts["filter"]["documents"] += 1;
      counts["filter"]["characters"] += c;
  key = wds = str(math.floor(wds * 10));
  if separate:
    infix = "cc";
    _ = document.get("crawl_id", None);
    if _ == "archivebot": infix = "ab";
    elif _ is not None and _.startswith("wide"): infix = "ia";
    key = wds + "." + infix;  
  if key not in output:
    output[key] = sharder(output["path"], size = size, buffer = buffer,
                          prefix = wds, infix = infix,
                          level = level, cores = cores);
  output[key].write(orjson.dumps(document, option = orjson.OPT_APPEND_NEWLINE));
  counts["documents"] += 1;
  counts["characters"] += c;
  #
  # finally, update our running tallies
  #
  statistics["documents"] += 1;
  statistics["characters"] += c;

def main():

  start = time.time();

  parser = argparse.ArgumentParser(description = "HPLT 4.0 Merge & Filter");
  parser.add_argument("--cores", type = int, default = 1);
  parser.add_argument("--level", type = int, default = 3);
  parser.add_argument("--size", type = int, default = 1e11);
  parser.add_argument("--buffer", type = int, default = 4 * 1024 ** 2);
  parser.add_argument("--separate", action = "store_true", default = False);
  parser.add_argument("--pattern", type = str, default = "*.zst");
  parser.add_argument("--blocked", type = str, required = True);
  parser.add_argument("--noisy", type = str, required = True);
  parser.add_argument("--clean", type = str, required = True);
  parser.add_argument("--trace", action = "count", default = 0);
  parser.add_argument("--skip", type = str);
  parser.add_argument("inputs", nargs = "*");
  arguments = parser.parse_args();

  io.DEFAULT_BUFFER_SIZE = arguments.buffer;

  if not os.path.isdir(arguments.blocked):
    print("[{}] filter.py: invalid --blocked target directory {}; exit."
          "".format(now(), arguments.blocked),
          file = sys.stderr, flush = True);
    sys.exit(1);
  blocked = {"path": arguments.blocked, "n": 0};
  if not os.path.isdir(arguments.noisy):
    print("[{}] filter.py: invalid --noisy target directory {}; exit."
          "".format(now(), arguments.noisy),
          file = sys.stderr, flush = True);
    sys.exit(1);
  noisy = {"path": arguments.noisy, "n": 0};
  if not os.path.isdir(arguments.clean):
    print("[{}] filter.py: invalid --clean target directory {}; exit."
          "".format(now(), arguments.clean),
          file = sys.stderr, flush = True);
    sys.exit(1);
  clean = {"path": arguments.clean, "n": 0};

  skip = set();
  if arguments.skip is not None:
    if not os.path.isfile(arguments.skip):
      print("[{}] filter.py: invalid --skip file {}; exit."
            "".format(now(), arguments.skip),
            file = sys.stderr, flush = True);
      sys.exit(1);
    with open(arguments.skip, "r", encoding = "utf-8") as stream:
      for line in stream: skip.add(line.rstrip());
    if len(skip):
      print("[{}] filter.py: skipping {:,} ids from {}."
              "".format(now(), len(skip), arguments.skip),
              file = sys.stderr, flush = True);
  #
  # first positional argument is directory containing document batches
  #
  files = glob.glob(os.path.join(arguments.inputs[0], arguments.pattern));
  if arguments.trace > 0:
    print("[{}] filter.py: found {:,} input file(s)."
          "".format(now(), len(files)),
          file = sys.stderr, flush = True);
  #
  # process one batch at a time, pairing up documents and annotations
  #
  total =  {"documents": 0, "characters": 0, "annotations": 0, "files": 0,
            "skipped": {"documents": 0, "characters": 0},
            "blocked": {"documents": 0, "characters": 0},
            "noisy": {"documents": 0, "characters": 0},
            "clean": {"documents": 0, "characters": 0}};
  for _ in NOISE: total["noisy"][_] = {"documents": 0, "characters": 0};
  total["noisy"]["filter"] =  {"documents": 0, "characters": 0};
  for file in files:
    statistics = {"documents": 0, "characters": 0, "annotations": 0,
                  "skipped": {"documents": 0, "characters": 0},
                  "blocked": {"documents": 0, "characters": 0},
                  "noisy": {"documents": 0, "characters": 0},
                  "clean": {"documents": 0, "characters": 0}};
    for _ in NOISE: statistics["noisy"][_] = {"documents": 0, "characters": 0};
    statistics["noisy"]["filter"] =  {"documents": 0, "characters": 0};
    name = os.path.basename(file);
    if arguments.trace > 0:
      print("[{}] filter.py: reading documents from {}, with {:,} annotations(s)."
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
          print("[{}] filter.py: invalid JSON object {} ({}: #{}); exit."
                "".format(now(), line, _, i),
                file = sys.stderr, flush = True);
          sys.exit(1);
        annotation = parse(line.rstrip(), arguments.trace, i);
        if "id" not in annotation:
          print("[{}] filter.py: missing .id. in annotation ({}: #{}); skip."
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
    if len(keys) > 0:
      if not "missing" in total:
        total["missing"] = dict();
        for _ in keys: total["missing"][_] = 0;
      statistics["missing"] = dict();
      for _ in keys: statistics["missing"][_] = 0;
        
    if arguments.trace > 0:
      print("[{}] filter.py: using {:,} annotations(s)."
            "".format(now(), sum(len(_) for _ in annotations)),
            file = sys.stderr, flush = True);
    
    documents = connect(file, arguments.buffer);
    #
    # process one document at a time, aligned by .id. across multiple files
    #
    for i, line in enumerate(documents):
      if not line.startswith(b"{"):
        print("[{}] filter.py: invalid JSON object {} ({}: #{}); exit."
              "".format(now(), line, file, i),
              file = sys.stderr, flush = True);
        sys.exit(1);
      document = parse(line.rstrip(), arguments.trace, i);
      id = document["id"];
      if id in skip:
        _ = len(document["text"]);
        statistics["documents"] += 1;
        statistics["characters"] += _;
        statistics["skipped"]["documents"] += 1;
        statistics["skipped"]["characters"] += _;
        continue;
      for key, table in zip(keys, annotations):
        annotation = table.get(id, None);
        if annotation is not None:
          document[key] = annotation;
          statistics["annotations"] += 1;
        else:
          statistics["missing"][key] += 1;
          if arguments.trace > 0:
            print("[{}] filter.py: missing {} annotation for .id. {} ({}: #{}); skip."
                  "".format(now(), key, id, file, i),
                  file = sys.stderr, flush = True);

      #
      # interpret annotations and route into various output bins
      #
      shipout(document, blocked, noisy, clean, statistics,
              arguments.size, arguments.separate,
              arguments.buffer, arguments.level,
              arguments.cores, arguments.trace, i);
    documents.close();
    total["files"] += 1;
    for key, value in statistics.items():
      if isinstance(value, dict):
        for _ in value.keys():
          if isinstance(value[_], dict):
            for __ in value[_].keys():
              total[key][_][__] += value[_][__];
          else:
            total[key][_] += value[_];
      else:
        total[key] += statistics[key];
    total[name] = statistics;
    for _ in [".zst", ".zstd", ".jsonl"]:
      if name.endswith(_): name = name[:-len(_)];
    with open(os.path.join(arguments.inputs[0], "." + name + ".filter.json"),
              "w", encoding = "utf-8") as _:
      json.dump(statistics, _, indent = 2);
    if arguments.trace > 0:
      print("[{}] filter.py: processed {:,} documents(s)."
            "".format(now(), i + 1),
            file = sys.stderr, flush = True);

  with open(os.path.join(arguments.inputs[0], ".filter.json"), "w", encoding = "utf-8") as _:
    json.dump(total, _, indent = 2);

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
    print("[{}] filter.py: {:,} documents in {:,} file(s); {:,} skipped;"
          " {:,} blocked; {} (+ {:,}) = {:,} noisy; {:,} clean;"
          " {:.2f} seconds."
          "".format(now(), total["documents"], total["files"],
                    total["skipped"]["documents"],
                    total["blocked"]["documents"],
                    " + ".join("{:,}".format(total["noisy"][_]["documents"]
                                             if _ in total["noisy"] else 0)
                               for _ in NOISE),
                    total["noisy"]["filter"]["documents"],
                    total["noisy"]["documents"], total["clean"]["documents"],
                    time.time() - start),
          file = sys.stderr, flush = True);
  sys.exit(0);

if __name__ == "__main__":
  main();
