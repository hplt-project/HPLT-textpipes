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

def parse(chunk):
  result = None;
  try:
    if isinstance(chunk, bytes): chunk = chunk.decode("utf-8", errors = "strict");
    result = orjson.loads(chunk);
  except UnicodeError as error:
    print("zstdconcat.py: failed to decode bytes object {}; skip."
          "".format(chunk),
          file = sys.stderr, flush = True);
  except Exception as error:
    print("zstdconcat.py: failed to parse string {}; skip."
          "".format(chunk),
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
  outputs = dict();
  if arguments.pool:
    if not os.path.isdir(arguments.pool):
      print("zstdconcat.py: invalid --pool target directory {}; exit."
            "".format(arguments.pool),
            file = sys.stderr, flush = True);
      sys.exit(1);
    _ = os.path.dirname(__file__);
    sys.path.append(os.path.realpath(os.path.join(_, "../src/hplt_textpipes/stage3")));
    from xml2md import process_single;
    for _ in ["xml", "md", "text", "metadata"]:
      file = os.path.join(arguments.pool, _ + ".zst");
      outputs[_] = io.BufferedWriter(zstandard.open(file, "wb"),
                                     buffer_size = arguments.buffer);
  else:
    if len(arguments.lid):
      print("zstdconcat.py: --lid annotations require --pool output; exit.",
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
      
  n, f, s = len(streams), 0, 0;
  if n:
    for i, line in enumerate(streams[0]):
      chunks = [];
      if filter is not None:
        _ = filter.readline();
        if not len(_):
          print("zstdconcat.py: premature end of file on {} (#{}); exit"
          "".format(arguments.filter, i),
          file = sys.stderr, flush = True);
          sys.exit(1);
        if not (b"true" if mode == "bytes" else "true") in _:
          for stream in streams[1:]: stream.readline();
          f += 1;
          continue;
        
      line = line.rstrip();
      if mode == "json": chunks.append(parse(line));
      elif n > 1: chunks.append(line[:-1]);
      else: chunks.append(line);
      for j, stream in enumerate(streams[1:]):
        _ = stream.readline();
        if not len(_):
          print("zstdconcat.py: premature end of file on {} (#{}); exit"
                "".format(arguments.inputs[j + 1], i),
                file = sys.stderr, flush = True);
          sys.exit(1);
        if mode == "json": chunks.append(parse(_));
        elif j < n - 2:
          chunks.append(b"," if mode == "bytes" else ",");
          chunks.append(_.rstrip()[1:-1]);
        else:
          chunks.append(b"," if mode == "bytes" else ",");
          chunks.append(_.rstrip()[1:]);

      if mode == "json" and not None in chunks:
        result = chunks.pop(0);
        for chunk in chunks: result |= chunk;
      else:
        result = (b"" if mode == "bytes" else "").join(chunks);
        
      #
      # optionally, hard-wire pool-level annotation and normalization:
      # + assign unique document id
      # + try to convert XML to markdown
      # + "t" -> "text", "x" -> "xml", "htmllang" -> "html_lang"
      # + write out textual represtations as separate files
      #
      if arguments.pool is not None:
        if mode != "json": result = parse(result);
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

        md = None;
        if "x" in result:
          _ = result["x"];
        try:
          if _ not in {None, ""}:
            md = process_single(_, line_num = i, raw = True, verbose = False);
        except Exception as error:
          print("zstdconcat.py: MD extraction failure, line #{} ({})."
                "".format(i, error),
                file = sys.stderr, flush = True);
          print("".join(traceback.format_exception(error)),
                file = sys.stderr, flush = True);
        result["md"] = md;
            
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
        
        for _ in ["xml", "md", "text"]:
          outputs[_].write(orjson.dumps({_: result[_]}, option = orjson.OPT_APPEND_NEWLINE));
          result.pop(_);
        outputs["metadata"].write(orjson.dumps(result, option = orjson.OPT_APPEND_NEWLINE));
      else:
        if mode == "json":
          output.write(orjson.dumps(result, option = orjson.OPT_APPEND_NEWLINE));
        else:
          output.write(result + ("\n" if mode == "string" else b"\n"));

  if filter is not None: filter.close();
  for _ in streams: _.close();
  output.close();
  for _ in outputs.values(): _.close();
  print("zstdconcat.py: processed {} {}input lines(s); {:.2f} seconds."
        "".format(i + 1,
                  f"(- {f} filtered) " if arguments.filter else "",
                  time.time() - start),
        file = sys.stderr, flush = True);

if __name__ == "__main__":
  main();
