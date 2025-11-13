#!/usr/bin/env python
"""
Mutliplexes-demultiplexes a set of line-parallel jsonlines files into multiple output files.
"""
import sys
import json
from contextlib import ExitStack
import smart_open

def main():
    """Main function."""
    args = sys.argv[1:]
    
    usage = """
Usage:
  python jsonl_muxdemux.py <input1> [<input2> ...] -- <output1> <spec1> [<output2> <spec2> ...]

Reads multiple line-parallel JSONL files, merges them and re-distributes fields 
to the output JSONL files according to the specification. Each output file is followed
by its corresponding specification as a separate argument.

A <spec> is a comma-separated list of fields to include in the output.
You can use:
- `fieldname`: to include a field as is.
- `*`: to include all fields from the merged input.
- `new_name=old_name`: to rename a field from `old_name` to `new_name`.

This script uses smart_open; it works with local files, stdin ('-'), S3, and compressed inputs (.zst).

Examples:
  # Select specific fields
  python jsonl_muxdemux.py in.jsonl -- out.jsonl "id,text"

  # Include all fields
  python jsonl_muxdemux.py in.jsonl -- out.jsonl "*"

  # Rename a field and include another
  python jsonl_muxdemux.py in.jsonl -- out.jsonl "doc_id=id,text"

  # Combine, rename, and select all for different outputs
  python jsonl_muxdemux.py in1.jsonl in2.jsonl -- out1.jsonl "id,text_en,new_source=source" out2.jsonl "*,id_copy=id"
"""

    try:
        separator_index = args.index('--')
    except ValueError:
        print("The '--' separator is required to distinguish input files from output specifications.", file=sys.stderr)
        print(usage, file=sys.stderr)
        sys.exit(1)

    input_files = args[:separator_index]
    output_args = args[separator_index + 1:]

    if not input_files:
        print("At least one input file is required.", file=sys.stderr)
        print(usage, file=sys.stderr)
        sys.exit(1)
    if not output_args:
        print("At least one output file and specification is required.", file=sys.stderr)
        print(usage, file=sys.stderr)
        sys.exit(1)
    if len(output_args) % 2 != 0:
        print("Error: Output files and their specifications must be provided in pairs.", file=sys.stderr)
        print(usage, file=sys.stderr)
        sys.exit(1)

    output_map = {}
    for i in range(0, len(output_args), 2):
        path = output_args[i]
        fields_str = output_args[i+1]
        try:
            if not path or not fields_str:
                raise ValueError

            output_spec = {'fields': [], 'rename': {}, 'all_fields': False}
            field_specs = fields_str.split(',')

            for field_spec in field_specs:
                if field_spec == '*':
                    output_spec['all_fields'] = True
                elif '=' in field_spec:
                    new_name, old_name = field_spec.split('=', 1)
                    if not new_name or not old_name:
                        raise ValueError
                    output_spec['rename'][new_name] = old_name
                else:
                    if field_spec: # handle empty string from trailing comma
                        output_spec['fields'].append(field_spec)

            if not output_spec['fields'] and not output_spec['rename'] and not output_spec['all_fields']:
                raise ValueError

            output_map[path] = output_spec

        except ValueError:
            print(f"Error: Invalid output specification format for '{path}': '{fields_str}'. "
                  "Expected format: 'field1,new=old,*'", file=sys.stderr)
            sys.exit(1)

    with ExitStack() as stack:
        # Open all input files
        in_handles = []
        for f in input_files:
            if f == '-':
                in_handles.append(sys.stdin)
            else:
                in_handles.append(stack.enter_context(smart_open.open(f, 'rt', encoding='utf-8')))

        # Open all output files
        out_handles = {}
        for path in output_map.keys():
            if path == '-':
                out_handles[path] = sys.stdout
            else:
                out_handles[path] = stack.enter_context(smart_open.open(path, 'wt', encoding='utf-8'))

        # Process files line by line
        for i, lines in enumerate(zip(*in_handles)):
            merged_record = {}
            for line in lines:
                try:
                    record = json.loads(line)
                    if isinstance(record, dict):
                        merged_record.update(record)
                except json.JSONDecodeError:
                    print(f"Warning: Skipping malformed JSON on line {i+1}", file=sys.stderr)
                    continue

            for path, spec in output_map.items():
                output_record = {}
                
                # Handle wildcard '*'
                if spec['all_fields']:
                    output_record.update(merged_record)

                # Handle simple fields
                for field in spec['fields']:
                    if field in merged_record:
                        output_record[field] = merged_record[field]
                
                # Handle renamed fields
                for new_name, old_name in spec['rename'].items():
                    if old_name in merged_record:
                        output_record[new_name] = merged_record[old_name]

                out_handles[path].write(json.dumps(output_record, ensure_ascii=False) + '\n')


if __name__ == "__main__":
    main()
