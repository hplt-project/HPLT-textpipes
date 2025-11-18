#!/usr/bin/env python

# -*- coding: utf-8; -*-

import argparse;
import glob;
import io;
import orjson;
import os;
from pathlib import Path;
from subprocess import Popen, PIPE;
import sys;
import time;
import zstandard;

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

def lid(text, identity, model):
  if text in {None, ""}: return {"lang": None};
  if "openlid" in identity:
    from hplt_textpipes.stage2.fastertext_lid.patterns import NONWORD_REPLACE_PATTERN, SPACE_PATTERN;
    text = text.strip().replace('\n', ' ').lower();
    text = regex.sub(SPACE_PATTERN, " ", text);
    text = regex.sub(NONWORD_REPLACE_PATTERN, "", text);
  else:
    text = text.strip().replace("\n", " ");
  result = model.predict(text = text, k = 3, threshold = 0.0, on_unicode_error="strict");
  return {"lang": [_.removeprefix("__label__") for _ in result[0]],
          "prob": [float(round(_, 4)) for _ in result[1]]};

def main():

  start = time.time();

  parser = argparse.ArgumentParser(description = "HPLT ");
  parser.add_argument("--cores", type = int, default = 8);
  parser.add_argument("--buffer", type = int, default = 4 * 1024 ** 2);
  parser.add_argument("--pipe", action = "store_true");
  parser.add_argument("--mode", type = str, default = "bytes");
  parser.add_argument("--filter", type = str, default = None);
  parser.add_argument("--lid", action = "append", default = []);
  parser.add_argument("--pool", type = str);
  parser.add_argument("inputs", nargs = "*");
  arguments = parser.parse_args();

  io.DEFAULT_BUFFER_SIZE = arguments.buffer;
  mode = arguments.mode;
  if arguments.pool:
    if not os.path.isdir(arguments.pool):
      print("zstdconcat.py: invalid --pool target directory {}; exit."
            "".format(arguments.pool),
            file = sys.stderr, flush = True);
      sys.exit(1);
    if mode != "json":
      print("zstdconcat.py: --mode {} not compatible with --pool creation; exit."
            "".format(mode),
            file = sys.stderr, flush = True);
      sys.exit(1);
      
    
  #
  # increase output buffer size
  #
  if mode in {"bytes", "json"}:
    output = open(1, "wb", buffering = arguments.buffer, closefd = False);
  else:
    output = open(1, "w", encoding = "utf-8",
                  buffering = arguments.buffer, closefd = False);
    
  filter = None;
  if arguments.filter is not None:
    filter = connect(arguments.filter, mode,
                     arguments.pipe, arguments.buffer);
  #
  # open all input files and connect to a decompressing stream or pipe
  #
  streams = [connect(_, mode, arguments.pipe, arguments.buffer)
             for _ in arguments.inputs];
  print("zstdconcat.py: {} {} input file(s)."
        "".format("filtering" if filter is not None else "reading",
                  len(streams)),
        file = sys.stderr, flush = True);

  lids = [];
  if len(arguments.lid):
    import fasttext, regex;
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
      
  n, s = len(streams), 0;
  if n:
    for i, line in enumerate(streams[0]):
      if filter is not None:
        _ = filter.readline();
        if not len(_):
          print("zstdconcat.py: premature end of file on {}; exit"
          "".format(arguments.filter),
          file = sys.stderr, flush = True);
          sys.exit(1);
        if not (b"true" if mode == "bytes" else "true") in _:
          for stream in streams[1:]: stream.readline();
          s += 1;
          continue;
        
      line = line.rstrip();
      if mode == "json":
        result = orjson.loads(line);
      elif n > 1: result = line[:-1];
      else: result = line;
      for j, stream in enumerate(streams[1:]):
        _ = stream.readline();
        if not len(_):
          print("zstdconcat.py: premature end of file on {}; exit"
                "".format(arguments.inputs[j + 1]),
                file = sys.stderr, flush = True);
          sys.exit(1);
        if mode == "json":
          result |= orjson.loads(_);
        elif j < n - 2:
          result += (b"," if mode == "bytes" else ",") + _.rstrip()[1:-1];
        else:
          result += (b"," if mode == "bytes" else ",") + _.rstrip()[1:];

      #
      # optionally, perform a series of additional annotations
      #
      if len(lids):
        try:
          if "t" not in result:
            print("zstdconcat.py: missing text field; exit",
                  file = sys.stderr, flush = True);
            sys.exit(1);
            
          for identity, model in lids:
            result[identity] = lid(result["t"], identity, model);
        except Exception as error:
          print("zstdconcat.py: error in lid {}; exit"
                "".format(identity),
                file = sys.stderr, flush = True);
          print("".join(traceback.format_exception(error)),
                file = sys.stderr, flush = True);
          sys.exit(1);
              
      if mode == "json":
        output.write(orjson.dumps(result, option = orjson.OPT_APPEND_NEWLINE));
      else:
        output.write(result + ("\n" if mode == "string" else b"\n"));

  if filter is not None: filter.close();
  for _ in streams: _.close();
  output.close();
  print("zstdconcat.py: processed {} {}input lines(s); {:.2f} seconds."
        "".format(i + 1,
                  f"(- {s}) " if arguments.filter else "",
                  time.time() - start),
        file = sys.stderr, flush = True);

if __name__ == "__main__":
  main();
