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

from xml2md import xml_to_markdown;

def skip(path, bin, cores = 1, output = None, suffix = ".jsonl.s.zst"):

  if os.path.isdir(path) and output is None:
    files = glob.glob(os.path.join(path, "*" + suffix));
    with mp.Pool(cores) as pool:
      counts = pool.starmap(skip, ((file, bin, 1) for file in files));
    n = i = 0;
    for _ in counts: n += _[0]; i += _[1];
    return n, i;
  
  elif not (os.path.isfile(path) and path.endswith(".zst")):
    print(f"skip(): invalid path {path}; exit.",
          file = sys.stderr, flush = True);
    return -1, 0;

  if output is None:
    if path.endswith(".s.zst"):
      output = path[:-len("s.zst")] + str(bin) + ".zst";
    else:
      output = path[:-len("zst")] + str(bin) + ".zst";

  print(f"skip(): [{bin}] {path} -> {output}.", flush = True);

  decompressor = zstd.ZstdDecompressor();
  stream = decompressor.stream_reader(open(path, "rb"));
  stream = io.TextIOWrapper(stream, encoding = "utf-8", errors = "replace");
  compressor = zstd.ZstdCompressor(level = 10, threads = cores);
  output = compressor.stream_writer(open(output, "wb"));
  output = io.TextIOWrapper(output, encoding = "utf-8", errors = "replace");

  n = 0;
  for i, line in enumerate(stream):
    try:
      _ = line.find("\t");
      key = int(float(line[:_]));
    except Exception as error:
      print(f"skip(): invalid line #{i}: {line}; exit",
            file = sys.stderr, flush = True);
      return -1, 0;
    if key > bin: continue;
    elif key < bin: break;
    output.write(line);
    n += 1;
  stream.close();
  output.close();
  return n, i;

def split(path, cores = 1, suffix = ".jsonl.zst"):

  if os.path.isdir(path):
    files = glob.glob(os.path.join(path, "*" + suffix));
    with mp.Pool(cores) as pool:
      counts = pool.starmap(split, ((file, 1) for file in files));
    n = i = o = 0;
    for _ in counts: n += _[0]; i += _[1]; o += _[2];
    return n, i, o;
  
  elif not (os.path.isfile(path) and path.endswith(".zst")):
    print(f"split(): invalid path {path}; exit.",
          file = sys.stderr, flush = True);
    return -1, 0, 0;

  decompressor = zstd.ZstdDecompressor();
  stream = decompressor.stream_reader(open(path, "rb"));
  stream = io.TextIOWrapper(stream, encoding = "utf-8", errors = "replace");
  outputs = dict();
  for i, line in enumerate(stream):
    try:
      lang = json.loads(line)["lang"][0];
    except Exception as error:
      print(f"split(): invalid line #{i}: {line}; exit",
            file = sys.stderr, flush = True);
      return -1, 0, 0;
    if lang not in outputs:
      base = path.split(os.path.sep)[:-2];
      target = os.path.join(os.path.sep.join(base), lang);
      os.makedirs(target, exist_ok = True);
      name = os.path.basename(path)[:-len("zst")] + "l.zst";
      output = os.path.join(target, name);
      compressor = zstd.ZstdCompressor(level = 10, threads = cores);
      _ = compressor.stream_writer(open(output, "wb"));
      outputs[lang] = io.TextIOWrapper(_, encoding = "utf-8", errors = "replace");
    outputs[lang].write(line);
    
  stream.close();
  for _ in outputs.values(): _.close();
  return 1, i, len(outputs);

def shard(source, target, cores = 1,
          suffix = ".jsonl.l.zst", size = 128 * 1024 ** 3):

  if not os.path.isdir(target):
    print(f"shard(): invalid target directory {target}; exit.",
          file = sys.stderr, flush = True);
    return None;
          
  i = d = o = 0;
  for bin in range(0,11):
    files = sorted(glob.glob(os.path.join(source, str(bin) + "_*" + suffix)));
    if len(files) == 1:
      name = os.path.basename(files[0]);
      if name.endswith(".l.zst"): name = name[:-len("l.zst")] + "zst";
      print(f"{files[0]} == {os.path.join(target, name)}", flush = True);
      shutil.copy2(files[0], os.path.join(target, name));
    elif len(files) > 1:
      n = 1; b = 0;
      name = os.path.join(target, f"{bin}_{n}.jsonl.zst");
      compressor = zstd.ZstdCompressor(level = 10, threads = cores);
      output = compressor.stream_writer(open(name, "wb"));
      output = io.TextIOWrapper(output, encoding = "utf-8", errors = "replace");
      o += 1;
      for file in files:
        print(f"{file} -> {name}", flush = True);
        decompressor = zstd.ZstdDecompressor();
        input = decompressor.stream_reader(open(file, "rb"));
        input = io.TextIOWrapper(input, encoding = "utf-8", errors = "replace");
        for line in input:
          output.write(line);
          b += len(line);
          if b >= size:
            output.close(); b = 0;
            n += 1;
            name = os.path.join(target, f"{bin}_{n}.jsonl.zst");
            output = compressor.stream_writer(open(name, "wb"));
            output = io.TextIOWrapper(output, encoding = "utf-8", errors = "replace");
            o += 1;
            print(f"{file} -> {output}", flush = True);
          d += 1;
      output.close();
    i += len(files);
  return i, d, o;

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
      print("merge.py: aborting input from {}, #{}: {error}."
            "".format(input["file"], input["n"], error),
            file = sys.stderr, flush = True);
      input["stream"].close();
      return None, None;
    #
    # skip documents whose WDS _key_ lies outside the [_min_, _max_[ range
    #
    if max is not None and key >= max: continue;
    if min is not None and key < min: return None, None;
    input["line"] = line;
    return key, input;
    
def main():

  start = time.time();

  parser = argparse.ArgumentParser(description = "HPLT Stage 5: Sort and Package for Release");
  parser.add_argument("--suffix", type = str, default = "jsonl.s.zst");
  parser.add_argument("--level", type = int, default = 10);
  parser.add_argument("--cores", type = int, default = 8);
  parser.add_argument("--start", type = int, default = 1);
  parser.add_argument("--min", type = int);
  parser.add_argument("--max", type = int);
  parser.add_argument("--size", type = int, default = 128 * 1024 ** 3);
  parser.add_argument("--lines", type = int);
  parser.add_argument("--target", default = ".");
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
      print(f"merge.py: ignoring invalid path {path}.",
            file = sys.stderr, flush = True);
  print(f"merge.py: reading {len(files)} inputs.", flush = True);
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
    if False:
      document = md = None;
      try:
        document = json.loads(input["line"]);
      except Exception as error:
        print("merge.py: ignoring invalid JSON from {}, #{}: {}."
              "".format(input["file"], input["n"], error),
              file = sys.stderr, flush = True);
      try:
        xml = document["xml"];
        md = xml_to_markdown(xml);
      except Exception as error:
        print("merge.py: MD failure from {}, #{}: {}."
              "".format(input["file"], input["n"], error),
              file = sys.stderr, flush = True);
        document = None;
    
    bin = int(key);
    #
    # create one output file per _bin_, counting from the starting index
    # _fix_ oe: simplify, take into account that all inputs are sorted
    #
    if bin not in outputs:
      print(f"merge.py: advancing to bin #{bin}.", flush = True);
      name = os.path.join(arguments.target,
                          f"{bin}_{arguments.start}.jsonl.zst");
      compressor = zstd.ZstdCompressor(level = arguments.level,
                                       threads = arguments.cores);
      stream = compressor.stream_writer(open(name, "wb"));
      stream = io.TextIOWrapper(stream, encoding = "utf-8", errors = "replace");
      outputs[bin] = {"file": name, "stream": stream, "i": arguments.start, "n": 0, "s": 0};
      o += 1;
    #
    # write the current document (sans the _key_ prefix) to its WDS bin
    #
    output = outputs[bin];
    output["stream"].write(input["line"]);
    output["n"] += 1;
    n += 1;
    output["s"] += len(input["line"]);
    #
    # advance to new output file after _arguments.lines_ documents
    #
    if (arguments.lines is not None and output["n"] >= arguments.lines
        or arguments.size is not None and output["s"] >= arguments.size):
      output["stream"].close();
      name = os.path.join(arguments.target,
                          "{}_{}.jsonl.zst".format(bin, output["i"] + 1));
      compressor = zstd.ZstdCompressor(level = arguments.level,
                                       threads = arguments.cores);
      stream = compressor.stream_writer(open(name, "wb"));
      stream = io.TextIOWrapper(stream, encoding = "utf-8", errors = "replace");
      outputs[bin] = {"file": name, "stream": stream, "i": output["i"] + 1, "n": 0, "s": 0};
      o += 1;
    
    #
    # update next line and key from current input file;
    # re-insert into the priority queue, unless exhausted
    #
    key, input = parse(input, arguments.min, arguments.max);
    if key is None: continue;
    else: inputs.append((key, input));
  for output in outputs.values():
    output["stream"].close();
    if output["n"] == 0:
      os.remove(output["file"]);
      o -= 1;
      
  print("merge.py: {} documents; {} inputs; {} outputs; {} seconds."
        "".format(n, len(files), o, time.time() - start), flush = True);

if __name__ == "__main__":
  main();
