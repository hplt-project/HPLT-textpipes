#!/usr/bin/env python3

import argparse
import sys
import jsonlines
import xml.etree.ElementTree as ET
import re

MAX_LIST_DEPTH = 5


class ConversionError(Exception):
    """Custom exception for conversion errors."""
    pass


HANDLERS = {
    "main": lambda elem: handle_div(elem),
    "div": lambda elem: handle_div(elem),
    "body": lambda elem: handle_div(elem),
    "head": lambda elem: handle_head(elem),
    "p": lambda elem: handle_p(elem),
    "list": lambda elem: handle_list(elem),
    "item": lambda elem: handle_item(elem),
    "lb": lambda elem: handle_lb(elem),
    "hi": lambda elem: handle_inline_formatting(elem),
    "del": lambda elem: handle_inline_formatting(elem),
    "code": lambda elem: handle_code(elem)
}


def extract_text_content(elem):
    result = []
    if elem.text:
        result.append(elem.text)

    for child in elem:
        result.extend(extract_text_content(child))
        if child.tail:
            result.append(child.tail)

    return result


def handle_head(elem):
    rend = elem.get("rend", "h3")

    if rend.startswith("h") and len(rend) > 1 and rend[1:].isdigit():
        level = int(rend[1:])
    else:
        level = 3

    level = max(1, min(6, level))

    # Process children with inline context for formatting
    result = []
    if elem.text:
        result.append(elem.text)

    for child in elem:
        child_content = process_element(child, inline_context=True)
        result.extend(child_content)
        if child.tail:
            result.append(child.tail)

    text = "".join(result).strip()

    if text:
        return [f"{'#' * level} {text}"]
    else:
        return []


def handle_p(elem):
    result = []
    if elem.text:
        result.append(elem.text)

    for child in elem:
        child_content = process_element(child, inline_context=True)
        result.extend(child_content)
        if child.tail:
            result.append(child.tail)

    text = "".join(result).strip()
    if text:
        return [text]
    else:
        return []


def handle_div(elem):
    result = []
    for i, child in enumerate(elem):
        child_result = process_element(child)
        result.extend(child_result)

        # Add line break after block elements (except for the last element)
        if i < len(elem) - 1 and child.tag in ["p", "head", "list", "code"]:
            result.append("")

    return result


def handle_lb(elem):
    return [""]


def handle_code(elem, inline_context=False):
    """Handle code elements - inline code or code blocks."""
    text_parts = extract_text_content(elem)
    text = "".join(text_parts).rstrip()

    if not text:
        return []

    # Determine if this should be inline or block code
    # If we're in an inline context (inside a paragraph, etc.), use inline code
    if inline_context:
        return [f"`{text}`"]
    else:
        # Block-level code block
        lines = text.split('\n')
        result = ["```"]
        result.extend(lines)
        result.append("```")
        return result


def handle_inline_formatting(elem):
    result = []
    if elem.text:
        result.append(elem.text)

    for child in elem:
        child_content = process_element(child, inline_context=True)
        result.extend(child_content)
        if child.tail:
            result.append(child.tail)

    text = "".join(result).strip()

    if not text:
        return []

    if elem.tag == "hi":
        rend = elem.get("rend", "")
        if rend == "#b":
            return [f"**{text}**"]
        elif rend == "#i":
            return [f"*{text}*"]
        elif rend == "#u":
            return [f"__{text}__"]
        elif rend == "#t":
            return [f"`{text}`"]
        elif rend == "#sup":
            return [f"<sup>{text}</sup>"]
        elif rend == "#sub":
            return [f"<sub>{text}</sub>"]
        else:
            raise ConversionError(f"Unknown or missing rend attribute for hi element: '{rend}'")
    elif elem.tag == "del":
        return [f"~~{text}~~"]
    else:
        raise ConversionError(f"Unsupported inline formatting element: {elem.tag}")


def handle_list(elem, depth=0):
    if depth >= MAX_LIST_DEPTH:
        raise ConversionError(f"List nesting depth exceeded maximum of {MAX_LIST_DEPTH}")

    result = []
    list_type = elem.get("rend", "ul")

    for i, child in enumerate(elem):
        if child.tag != "item":
            raise ConversionError(f"Unexpected element '{child.tag}' in list, expected 'item'")

        item_content = handle_item(child, depth)
        if item_content:
            indent = "  " * depth
            if list_type == "ol":
                result.append(f"{indent}{i+1}. {item_content[0]}")
            else:
                result.append(f"{indent}- {item_content[0]}")

            for line in item_content[1:]:
                result.append(f"{indent}  {line}")

    return result


def handle_item(elem, depth=0):
    result = []

    if elem.text and elem.text.strip():
        result.append(elem.text.strip())

    for child in elem:
        if child.tag == "list":
            nested_list = handle_list(child, depth + 1)
            result.extend(nested_list)
        else:
            child_content = process_element(child, inline_context=True)
            result.extend(child_content)

        if child.tail and child.tail.strip():
            result.append(child.tail.strip())

    if not result:
        text_parts = extract_text_content(elem)
        text = "".join(text_parts).strip()
        if text:
            result.append(text)

    return result


def process_element(elem, inline_context=False):
    if elem.tag not in HANDLERS:
        raise ConversionError(f"No handler for element: {elem.tag}")

    handler = HANDLERS[elem.tag]

    # Special handling for code elements to pass context
    if elem.tag == "code":
        return handle_code(elem, inline_context)
    else:
        return handler(elem)


def xml_to_markdown(xml_string, line_num=None):
    try:
        root = ET.fromstring(xml_string)
        main_elem = root.find(".//main")
        if main_elem is None:
            line_prefix = f"Line {line_num}: " if line_num else ""
            print(f"{line_prefix}No main element found in XML", file=sys.stderr)
            return None

        return process_element(main_elem)

    except ET.ParseError as e:
        line_prefix = f"Line {line_num}: " if line_num else ""
        print(f"{line_prefix}Malformed XML content: {e}", file=sys.stderr)
        return None

    except ConversionError as e:
        line_prefix = f"Line {line_num}: " if line_num else ""
        print(f"{line_prefix}Conversion error: {e}", file=sys.stderr)
        return None


def process_single(item, line_num=None):
    if "x" not in item:
        return item

    markdown_content = xml_to_markdown(item["x"], line_num)

    if markdown_content:
        item["md"] = "\n".join(markdown_content)

    return item


def process_buffer(buffer, start_line_num):
    done = []
    for i, item in enumerate(buffer):
        line_num = start_line_num + i
        done.append(process_single(item, line_num))
    return done


def main(buffer_size=1000):
    with (jsonlines.Reader(sys.stdin) as reader,
          jsonlines.Writer(sys.stdout) as writer):

        buffer = []
        line_num = 1

        for line in reader:
            buffer.append(line)

            if len(buffer) >= buffer_size:
                start_line = line_num - len(buffer) + 1
                processed = process_buffer(buffer, start_line)
                for item in processed:
                    writer.write(item)
                buffer = []

            line_num += 1

        if buffer:
            start_line = line_num - len(buffer)
            processed = process_buffer(buffer, start_line)
            for item in processed:
                writer.write(item)


if __name__ == "__main__":
    parser = argparse.ArgumentParser("Convert trafilatura XML output to markdown")
    parser.add_argument("--buffer-size", type=int, default=1000, help="Buffer size for processing lines")
    parser.add_argument("--max-list-depth", type=int, default=5, help="Maximum nesting depth for lists")
    args = parser.parse_args()

    MAX_LIST_DEPTH = args.max_list_depth

    main(args.buffer_size)

