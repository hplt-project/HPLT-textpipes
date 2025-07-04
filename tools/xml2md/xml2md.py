#!/usr/bin/env python3

import argparse
import sys
import jsonlines


def process(item):
    if "x" not in item:
        return item



    return item


def main():

    with jsonlines.Reader(sys.stdin) as reader, jsonlines.Writer(sys.stdout) as writer:
        for item in reader:
            writer.write(process(item))


if __name__ == "__main__":
    parser = argparse.ArgumentParser("Convert trafilatura XML output to markdown")

    args = parser.parse_args()
    main()

