#!/usr/bin/env python
"""
Multiplexes-demultiplexes a set of line-parallel jsonlines files into multiple output files.
"""
import sys
import orjson
import argparse
from contextlib import ExitStack
import smart_open
import time
from tqdm.auto import tqdm


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


def _update_progress(pbar, ema_list, ema_index, avg_duration, total_duration, alpha, original_desc, n=1):
    """Helper to update EMA, Total, and progress bar description for a batch of n items."""
    current_ema = ema_list[ema_index]
    if not pbar.n:  # first batch
        current_ema = avg_duration
    else:
        # Iteratively apply EMA for each item in the batch for correctness
        for _ in range(n):
            current_ema = alpha * avg_duration + (1 - alpha) * current_ema
    ema_list[ema_index] = current_ema

    ema_ms = ema_list[ema_index] * 1000
    desc = f"EMA: {ema_ms:8.3f}ms, Total: {total_duration:6.1f}s | {original_desc}"
    pbar.set_description(desc)
    pbar.update(n)


def _timed_iterator(handle, pbar, ema_list, ema_idx, alpha, original_desc, miniters=100):
    """A generator that yields lines, updating progress in batches."""
    count = 0
    batch_duration = 0.0
    total_duration = 0.0
    while True:
        start = time.monotonic()
        line = handle.readline()
        read_duration = time.monotonic() - start

        if not line:
            # Final update for any remaining lines at EOF
            if count > 0:
                avg_duration = batch_duration / count
                _update_progress(pbar, ema_list, ema_idx, avg_duration, total_duration, alpha, original_desc, n=count)
            return

        yield line

        batch_duration += read_duration
        total_duration += read_duration
        count += 1
        if count == miniters:
            avg_duration = batch_duration / count
            _update_progress(pbar, ema_list, ema_idx, avg_duration, total_duration, alpha, original_desc, n=count)
            # Reset batch counters
            count = 0
            batch_duration = 0.0


def process_files(input_files: list, output_specs: list):
    """Reads, processes, and writes the jsonl files with progress indicators."""
    num_inputs = len(input_files)
    num_outputs = len(output_specs)
    alpha = 0.01  # Smoothing factor for EMA
    miniters = 100

    # State
    input_emas = [0.0] * num_inputs
    output_emas = [0.0] * num_outputs

    # Create descriptions and progress bars
    all_original_descs = []
    progress_bars = []
    for i, f in enumerate(input_files):
        desc = f"In: ...{f[-30:]}" if len(f) > 33 else f"In: {f}"
        all_original_descs.append(desc)
        progress_bars.append(
            tqdm(position=i, unit=" lines", dynamic_ncols=True, leave=True, desc=desc)
        )

    for i, (path, spec) in enumerate(output_specs):
        spec_str_parts = (["*"] if spec.get('all_fields') else []) + \
                         spec.get('fields', []) + \
                         [f"{k}={v}" for k, v in spec.get('rename', {}).items()]
        spec_str = ",".join(spec_str_parts)
        desc = f"Out: {path} ({spec_str})"
        if len(desc) > 120:
            desc = f"Out: ...{path[-20:]} ({spec_str[:80]}...)"
        all_original_descs.append(desc)
        progress_bars.append(
            tqdm(position=num_inputs + i, unit=" lines", dynamic_ncols=True, leave=True, desc=desc)
        )

    try:
        with ExitStack() as stack:
            in_handles = [
                sys.stdin.buffer if f == '-' else stack.enter_context(smart_open.open(f, 'rb'))
                for f in input_files
            ]
            unique_paths = sorted(list(set(p for p, s in output_specs)))
            out_handles_map = {
                path: sys.stdout.buffer if path == '-' else stack.enter_context(smart_open.open(path, 'wb'))
                for path in unique_paths
            }

            timed_in_iters = [
                _timed_iterator(h, progress_bars[i], input_emas, i, alpha, all_original_descs[i], miniters)
                for i, h in enumerate(in_handles)
            ]

            line_count = 0
            batch_output_durations = [0.0] * num_outputs
            total_output_durations = [0.0] * num_outputs
            for lines in zip(*timed_in_iters):
                merged_record = {}
                for line in lines:
                    record = orjson.loads(line)
                    if isinstance(record, dict):
                        merged_record.update(record)

                for i, (path, spec) in enumerate(output_specs):
                    output_record = _build_output_record(merged_record, spec)
                    start = time.monotonic()
                    out_handles_map[path].write(orjson.dumps(output_record, option=orjson.OPT_APPEND_NEWLINE))
                    write_duration = time.monotonic() - start
                    batch_output_durations[i] += write_duration
                    total_output_durations[i] += write_duration

                line_count += 1
                if line_count == miniters:
                    for i in range(num_outputs):
                        pbar_idx = num_inputs + i
                        avg_duration = batch_output_durations[i] / line_count
                        _update_progress(
                            progress_bars[pbar_idx], output_emas, i, avg_duration,
                            total_output_durations[i], alpha, all_original_descs[pbar_idx], n=line_count
                        )
                    # Reset batch counters
                    line_count = 0
                    batch_output_durations = [0.0] * num_outputs

            # Final update for any remaining output lines
            if line_count > 0:
                for i in range(num_outputs):
                    pbar_idx = num_inputs + i
                    avg_duration = batch_output_durations[i] / line_count
                    _update_progress(
                        progress_bars[pbar_idx], output_emas, i, avg_duration,
                        total_output_durations[i], alpha, all_original_descs[pbar_idx], n=line_count
                    )
    finally:
        for pbar in progress_bars:
            pbar.close()


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

    output_specs = []
    for i in range(0, len(output_args), 2):
        path, fields_str = output_args[i], output_args[i + 1]
        try:
            output_specs.append((path, _parse_spec(fields_str)))
        except ValueError as e:
            parser.error(f"Invalid output specification for '{path}': '{fields_str}'. {e}")

    process_files(input_files, output_specs)


if __name__ == "__main__":
    main()