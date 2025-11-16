#!/usr/bin/env python
"""
Multiplexes-demultiplexes a set of line-parallel jsonlines files into multiple output files.
"""
import sys
import orjson
import argparse
from contextlib import ExitStack
import smart_open


def _parse_spec(fields_str: str) -> dict:
    """Parses an output specification string."""
    if not fields_str:
        raise ValueError("Specification string cannot be empty.")

    spec = {'fields': [], 'rename': {}, 'all_fields': False}
    for field_spec in fields_str.split(','):
        part = field_spec.strip()
        if not part:
            continue
        if part == '*':
            spec['all_fields'] = True
        elif '=' in part:
            new_name, old_name = part.split('=', 1)
            if not new_name or not old_name:
                raise ValueError(f"Invalid rename format '{part}'. Expected 'new=old'.")
            spec['rename'][new_name] = old_name
        else:
            spec['fields'].append(part)

    if not spec['fields'] and not spec['rename'] and not spec['all_fields']:
        raise ValueError("Specification cannot be empty.")

    return spec


def _build_output_record(merged_record: dict, spec: dict) -> dict:
    """Builds an output record based on a spec."""
    output_record = merged_record.copy() if spec['all_fields'] else {}

    for field in spec['fields']:
        if field in merged_record:
            output_record[field] = merged_record[field]

    for new_name, old_name in spec['rename'].items():
        if old_name in merged_record:
            output_record[new_name] = merged_record[old_name]

    return output_record


def process_files(input_files: list, output_map: dict):
    """Reads, processes, and writes the jsonl files."""
    with ExitStack() as stack:
        in_handles = [
            sys.stdin.buffer if f == '-' else stack.enter_context(smart_open.open(f, 'rb'))
            for f in input_files
        ]
        out_handles = {
            path: sys.stdout.buffer if path == '-' else stack.enter_context(smart_open.open(path, 'wb'))
            for path in output_map
        }

        for lines in zip(*in_handles):
            merged_record = {}
            for line in lines:
                record = orjson.loads(line)
                if isinstance(record, dict):
                    merged_record.update(record)

            for path, spec in output_map.items():
                output_record = _build_output_record(merged_record, spec)
                out_handles[path].write(orjson.dumps(output_record, option=orjson.OPT_APPEND_NEWLINE))


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description="""
Reads multiple line-parallel JSONL files, merges them and re-distributes fields
to the output JSONL files according to the specification. Each output file is
followed by its corresponding specification as a separate argument.

The '--' separator is required to distinguish input files from output specifications.

A <spec> is a comma-separated list of fields to include in the output:
- `fieldname`: to include a field as is.
- `*`: to include all fields from the merged input.
- `new_name=old_name`: to rename a field from `old_name` to `new_name`.

This script uses smart_open; it works with local files, stdin ('-'), S3, and
compressed inputs (.zst).""",
        epilog="""
Examples:
  # Select specific fields
  python jsonl_muxdemux.py in.jsonl -- out.jsonl "id,text"

  # Include all fields
  python jsonl_muxdemux.py in.jsonl -- out.jsonl "*"

  # Rename a field and include another
  python jsonl_muxdemux.py in.jsonl -- out.jsonl "doc_id=id,text"

  # Combine, rename, and select all for different outputs
  python jsonl_muxdemux.py in1.jsonl in2.jsonl -- out1.jsonl "id,text_en,new_source=source" out2.jsonl "*,id_copy=id"
""",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    args = sys.argv[1:]

    try:
        separator_index = args.index('--')
    except ValueError:
        parser.error("The '--' separator is required to distinguish input files from output specifications.")

    input_files = args[:separator_index]
    output_args = args[separator_index + 1:]

    if not input_files:
        parser.error("At least one input file is required.")
    if not output_args:
        parser.error("At least one output file and specification is required.")
    if len(output_args) % 2 != 0:
        parser.error("Output files and their specifications must be provided in pairs.")

    output_map = {}
    for i in range(0, len(output_args), 2):
        path, fields_str = output_args[i], output_args[i + 1]
        try:
            output_map[path] = _parse_spec(fields_str)
        except ValueError as e:
            parser.error(f"Invalid output specification for '{path}': '{fields_str}'. {e}")

    process_files(input_files, output_map)


if __name__ == "__main__":
    main()