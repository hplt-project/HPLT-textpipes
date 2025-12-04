#!/usr/bin/env python

# -*- coding: utf-8; -*-

import argparse;
import glob;
import io;
import orjson;
import os;
from pathlib import Path;
import regex;
from subprocess import Popen, PIPE;
import sys;
import time;
import traceback;
from xxhash import xxh128_hexdigest;
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

def connect(path, mode, pipe, buffer):
  if not os.path.isfile(path):
    print("zstdconcat.py: invalid input file {}; exit"
          "".format(path),
          file = sys.stderr, flush = True);
    sys.exit(1);
  if pipe:
    if mode == "string":
      _ = Popen(["zstdcat", path], bufsize = buffer,
                stdout = PIPE, encoding = "utf-8", errors = "strict",
                stderr = sys.stdout);
    else:
      _ = Popen(["zstdcat", path], bufsize = buffer,
                stdout = PIPE, stderr = sys.stdout);
    return _.stdout;
  else:
    if mode == "string":
      _ = zstandard.open(path, "r", encoding = "utf-8", errors = "strict");
    else:
      _ = io.BufferedReader(zstandard.open(path, "rb"), buffer_size = buffer);
    return _;

def parse(chunk, trace, i):
  result = None;
  try:
    if isinstance(chunk, bytes): chunk = chunk.decode("utf-8", errors = "strict");
    result = orjson.loads(chunk);
  except UnicodeError as error:
    if trace > 1:
      print("[{}] zstdconcat.py: failed to decode bytes object {}; skip."
            "".format(now(), chunk),
            file = sys.stderr, flush = True);
  except Exception as error:
    if trace > 1:
      print("[{}] zstdconcat.py: failed to parse string {}; skip."
            "".format(now, chunk),
            file = sys.stderr, flush = True);
  return result;

NONWORD_REPLACE_PATTERN = regex.compile(r"[^\p{Word}\p{Zs}]|\d");
SPACE_PATTERN = regex.compile(r"\s\s+")

def lid(text, identity, model):
  if text in {None, ""}: return {"lang": None};
  if "openlid" in identity:
    text = text.strip().replace('\n', ' ').lower();
    text = SPACE_PATTERN.sub(" ", text);
    text = NONWORD_REPLACE_PATTERN.sub("", text);
  else:
    text = text.strip().replace("\n", " ");
  result = model.predict(text = text, k = 3, threshold = 0.0,
                         on_unicode_error = "strict");
  return {"lang": [_.removeprefix("__label__") for _ in result[0]],
          "prob": [float(round(_, 4)) for _ in result[1]]};

def pool(result, lids, i, md, outputs):
  if result is None or "t" not in result:
    print("zstdconcat.py: missing text field (#{}); exit"
          "".format(i),
          file = sys.stderr, flush = True);
    sys.exit(1);

  text = result["t"];
  if len(lids):
    try:
      for identity, model in lids:
        result[identity] = lid(text, identity, model);
    except Exception as error:
      print("zstdconcat.py: error in lid {} (#{}); exit"
            "".format(identity, i),
            file = sys.stderr, flush = True);
      print("".join(traceback.format_exception(error)),
            file = sys.stderr, flush = True);
      sys.exit(1);

  result["md"] = None;
  if "x" in result:
    _ = result["x"];
    try:
      if _ not in {None, ""}:
        result["md"] = md(_, line_num = i, raw = True, verbose = False);
    except Exception as error:
      print("[{}] zstdconcat.py: MD extraction failure, line #{} ({})."
            "".format(now(), i, error),
            file = sys.stderr, flush = True);
      print("".join(traceback.format_exception(error)),
            file = sys.stderr, flush = True);

  if "f" not in result or "u" not in result or "ts" not in result:
    print("zstdconcat.py: missing key(s) for id (#{}); exit"
          "".format(i),
          file = sys.stderr, flush = True);
    sys.exit(1);
  result["id"] = xxh128_hexdigest(result["f"] + result["u"] + result["ts"]);
  result["text"] = result.pop("t");

  if "x" in result: result["xml"] = result.pop("x");
  else: result["xml"] = None;
  if "htmllang" in result: result["html_lang"] = result.pop("htmllang");
  _ = {"lang": result["lang"]};
  result.pop("lang");
  if "prob" in result:
    _["prob"] = result["prob"];
    result.pop("prob");
  result["openlid-v2"] = _;

  outputs["text"].write(orjson.dumps({"text": result["text"]},
                                     option = orjson.OPT_APPEND_NEWLINE));
  result.pop("text");
  markup = dict();
  for _ in ["xml", "md"]:
    if _ in result: markup[_] = result[_]; result.pop(_);
  outputs["markup"].write(orjson.dumps(markup,
                                     option = orjson.OPT_APPEND_NEWLINE));
  outputs["metadata"].write(orjson.dumps(result, option = orjson.OPT_APPEND_NEWLINE));

OPENLID_PATTERN = \
  {"bytes": regex.compile(b"\"openlid-v3\": ?\\{\"lang\": ?\\[\"([a-z]{3}_[A-Z][a-z]{3})\","),
   "string": regex.compile("\"openlid-v3\": ?\\{\"lang\": ?\\[\"([a-z]{3}_[A-Z][a-z]{3})\",")};

def bin(result, bins, mode, size, buffer, level, cores, trace, i):
  lang = None;
  if mode == "json":
    if "openlid-v3" in result:
      _ = result["openlid-v3"];
      if _ is not None: lang = _["lang"][0];
  else:
    _ = OPENLID_PATTERN[mode].search(result);
    if _ is not None:
      try: 
        lang = _.group(1).decode("utf-8") if mode == "bytes" else _.group(1);
      except:
        if trace > 1:
          print("[{}] zstdconcat.py: failed to decode JSON (line #{}); skip."
                "".format(now(), i),
                file = sys.stderr, flush = True);
        return 1;
  if lang is None:
    if trace > 1:
      print("[{}] zstdconcat.py: failed to extract LID value (line #{}); skip."
            "".format(now(), i),
            file = sys.stderr, flush = True);
    return 1;
  if lang not in bins:
    bins[lang] = sharder(os.path.join(bins["path"], lang), size = size,
                         buffer = buffer, level = level, cores = cores);
  bins[lang].write(result + ("\n" if mode == "string" else b"\n"));
  return 0;

def main():

  start = time.time();

  parser = argparse.ArgumentParser(description = "HPLT ");
  parser.add_argument("--cores", type = int, default = 1);
  parser.add_argument("--level", type = int, default = 3);
  parser.add_argument("--size", type = int, default = 1e11);
  parser.add_argument("--buffer", type = int, default = 4 * 1024 ** 2);
  parser.add_argument("--pipe", action = "store_true");
  parser.add_argument("--mode", type = str, default = "bytes");
  parser.add_argument("--filter", type = str, default = None);
  parser.add_argument("--lid", action = "append", default = []);
  parser.add_argument("--pool", type = str);
  parser.add_argument("--compress", type = str);
  parser.add_argument("--bin", type = str);
  parser.add_argument("--trace", action = "count", default = 0);
  parser.add_argument("inputs", nargs = "*");
  arguments = parser.parse_args();

  io.DEFAULT_BUFFER_SIZE = arguments.buffer;
  outputs = dict();
  bins = dict();
  if arguments.pool:
    if not os.path.isdir(arguments.pool):
      print("zstdconcat.py: invalid --pool target directory {}; exit."
            "".format(arguments.pool),
            file = sys.stderr, flush = True);
      sys.exit(1);
    if arguments.compress is not None:
      print("zstdconcat.py: --pool and --compress are mutually incompatible {}; exit."
            "".format(arguments.pool),
            file = sys.stderr, flush = True);
      sys.exit(1);
    if arguments.bin is not None:
      print("zstdconcat.py: --pool and --bin are mutually incompatible {}; exit."
            "".format(arguments.pool),
            file = sys.stderr, flush = True);
      sys.exit(1);
    #
    # conditionally import conversion from xml to markdown
    #
    _ = os.path.dirname(__file__);
    sys.path.append(os.path.realpath(os.path.join(_, "../src/hplt_textpipes/stage3")));
    from xml2md import process_single;
    for _ in ["markup", "text", "metadata"]:
      file = os.path.join(arguments.pool, _ + ".zst");
      outputs[_] = io.BufferedWriter(zstandard.open(file, "wb"),
                                     buffer_size = arguments.buffer);
  elif len(arguments.lid):
    print("zstdconcat.py: --lid annotations require --pool output; exit.",
          file = sys.stderr, flush = True);
    sys.exit(1);
  if arguments.bin:
    if not os.path.isdir(arguments.bin):
      print("zstdconcat.py: invalid --bin target directory {}; exit."
            "".format(arguments.bin),
            file = sys.stderr, flush = True);
      sys.exit(1);
    if arguments.filter is not None:
      print("zstdconcat.py: --bin and --filter are mutually incompatible {}; exit."
            "".format(arguments.pool),
            file = sys.stderr, flush = True);
      sys.exit(1);
    bins["path"] = arguments.bin;
  #
  # increase output buffer size
  #
  mode = arguments.mode;
  if arguments.compress is not None:
    output = io.BufferedWriter(zstandard.open(arguments.compress, "wb"),
                               buffer_size = arguments.buffer);
  else:
    if mode in {"bytes", "json"}:
      output = open(1, "wb", buffering = arguments.buffer, closefd = False);
    else:
      output = open(1, "w", encoding = "utf-8",
                    buffering = arguments.buffer, closefd = False);
    
  filter = None;
  if arguments.filter is not None:
    filter = connect(arguments.filter, mode,
                     arguments.pipe, arguments.buffer);
  
  if arguments.trace > 0:
    print("[{}] zstdconcat.py: {} {} input {}(s)."
          "".format(now(),
                    "filtering" if filter is not None else "reading",
                    len(arguments.inputs),
                    "file" if arguments.bin is None else "directory"),
          file = sys.stderr, flush = True);
  #
  # initialize fastText model(s) if requested
  #
  lids = [];
  if len(arguments.lid):
    import fasttext;
    cache = os.path.join(Path.home(), ".cache", "hplt");
  for identity in arguments.lid:
    _ = os.path.join(cache, identity + ".bin");
    if not os.path.isfile(_):
      print("zstdconcat.py: missing model file for {}; exit."
            "".format(identity),
            file = sys.stderr, flush = True);
      sys.exit(1);
    try:
      model = fasttext.load_model(_);
      lids.append((identity, model));
    except:
      print("zstdconcat.py: failed to initialize LID {}; exit."
            "".format(identity),
            file = sys.stderr, flush = True);
      sys.exit(1);
  #
  # in --bin mode, iterate over directories and read all .zst files;
  # otherwise, just iterate over files provided on command line
  #
  for path in (arguments.inputs if arguments.bin is not None else [None]):
    #
    # open all input files and connect to a decompressing stream or pipe
    #
    if path is not None:
      inputs = glob.glob(os.path.join(path, "*.zst"));
      if arguments.trace > 0:
        print("[{}] zstdconcat.py: reading {} files(s) in {}."
              "".format(now(), len(inputs), path),
              file = sys.stderr, flush = True);
    else:
      inputs = arguments.inputs;
    streams = [connect(_, mode, arguments.pipe, arguments.buffer)
               for _ in inputs];
    #
    # process one document at a time, aligned across multiple files
    #
    i, n, f, s = 0, len(streams), 0, 0;
    if n:
      for i, line in enumerate(streams[0]):
        if not line.startswith("{" if mode == "string" else b"{"):
          print("zstdconcat.py: invalid JSON object {} ({}: #{}); exit"
                "".format(line, inputs[0], i),
                file = sys.stderr, flush = True);
          sys.exit(1);
        chunks = [];
        if filter is not None:
          _ = filter.readline();
          if not len(_):
            print("zstdconcat.py: premature end of file on {} (#{}); exit"
            "".format(arguments.filter, i),
            file = sys.stderr, flush = True);
            sys.exit(1);
          if not _.startswith("{" if mode == "string" else b"{"):
            print("zstdconcat.py: invalid JSON object {} ({}: #{}); exit"
                  "".format(_, arguments.filter, i),
                  file = sys.stderr, flush = True);
            sys.exit(1);
          if not ("true" if mode == "string" else b"true") in _:
            for stream in streams[1:]: stream.readline();
            f += 1;
            continue;
        #
        # collect and massage all line-aligned chunks
        #
        line = line.rstrip();
        if mode == "json": chunks.append(parse(line, arguments.trace, i));
        elif n > 1: chunks.append(line[:-1]);
        else: chunks.append(line);
        for j, stream in enumerate(streams[1:]):
          _ = stream.readline();
          if not len(_):
            print("zstdconcat.py: premature end of file on {} (#{}); exit"
                  "".format(inputs[j + 1], i),
                  file = sys.stderr, flush = True);
            sys.exit(1);
          if not _.startswith("{" if mode == "string" else b"{"):
            print("zstdconcat.py: invalid JSON object {} ({}: #{}); exit"
                  "".format(_, inputs[j + 1], i),
                  file = sys.stderr, flush = True);
            sys.exit(1);
          if mode == "json": chunks.append(parse(_, arguments.trace, i));
          else:
            #
            # avoid spurious commas before empty JSON objects
            #
            if len(chunks[-1]) > 1 and len(_) > 3:
              chunks.append("," if mode == "string" else b",");
            if j < n - 2:
              if len(_) > 3: chunks.append(_.rstrip()[1:-1]);
            else: chunks.append(_.rstrip()[1:]);
        #
        # finally, combine into one json representation, with minimal copying
        #
        if mode == "json":
          if None in chunks:
            s += 1;
          else:
            result = chunks.pop(0);
            for chunk in chunks: result |= chunk;
        else:
          result = ("" if mode == "string" else b"").join(chunks);
        #
        # optionally, hard-wire pool-level annotation and normalization:
        #
        if arguments.pool is not None:
          if mode != "json": result = parse(result, arguments.trace, i);
          pool(result, lids, i, process_single, outputs)
        #
        # or merge files and apply per-language binning, for monotexting
        #
        elif arguments.bin is not None:
          s += bin(result, bins, mode, arguments.size, arguments.buffer,
                   arguments.level, arguments.cores, arguments.trace, i);
        else:
          if mode == "json":
            output.write(orjson.dumps(result, option = orjson.OPT_APPEND_NEWLINE));
          else:
            output.write(result + ("\n" if mode == "string" else b"\n"));
    for _ in streams: _.close();
  #
  # wrap up: close all output streams
  #
  if filter is not None: filter.close();
  output.close();
  for _ in outputs.values(): _.close();
  for _ in bins.values():
    if isinstance(_, sharder): _.close();
  if arguments.trace > 0:
    print("[{}] zstdconcat.py: processed {} {}{}input lines(s); {:.2f} seconds."
          "".format(now(), i + 1,
                    f"(- {f} filtered) " if arguments.filter else "",
                    f"(- {s} skipped) " if s > 0 else "",
                    time.time() - start),
          file = sys.stderr, flush = True);
  sys.exit(0);

if __name__ == "__main__":
  main();
