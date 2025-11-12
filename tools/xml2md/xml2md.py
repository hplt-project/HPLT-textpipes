#!/usr/bin/env python3

import argparse
import sys
import json
import xml.etree.ElementTree as ET
import re

MAX_LIST_DEPTH = 5
VERBOSITY_LEVEL = 2
XML_FIELD_NAME = "x"
MD_ONLY = False
TOTAL_LINES = 0
SUCCESSFUL_CONVERSIONS = 0

# Pre-compiled regex patterns for better performance
BLOCK_START_HASH = re.compile(r'^(\s*)#', re.MULTILINE)
BLOCK_START_PLUS = re.compile(r'^(\s*)\+(\s)', re.MULTILINE)
BLOCK_START_MINUS = re.compile(r'^(\s*)-(\s)', re.MULTILINE)
BLOCK_START_NUMBERED = re.compile(r'^(\s*)(\d+)\.(\s)', re.MULTILINE)
BLOCK_START_GT = re.compile(r'^(\s*)>', re.MULTILINE)
BLOCK_START_COLON = re.compile(r'^(\s*):', re.MULTILINE)
BLOCK_START_HR = re.compile(r'^(\s*)(-{3,}|\*{3,}|_{3,})(\s*)$', re.MULTILINE)

# Pre-compiled patterns for paired escaping
PAIRED_ASTERISK = re.compile(r'(\*)(?!\s)(.*?)(\*)')
PAIRED_UNDERSCORE = re.compile(r'(_)(?!\s)(.*?)(_)')
PAIRED_TILDE = re.compile(r'(~)(?!\s)(.*?)(~)')
PAIRED_ANGLE = re.compile(r'(<)(?!\s)(.*?)(>)')


class ConversionError(Exception):
    """Custom exception for conversion errors with severity levels."""

    # Severity levels (lower number = higher severity = more important)
    CRITICAL = 1    # Reserved for most critical errors
    HIGH = 2        # No handler errors, structure/parsing errors
    MEDIUM = 3      # Content validation errors
    LOW = 4         # Table-related errors - least important

    def __init__(self, message, severity=HIGH):
        super().__init__(message)
        self.severity = severity


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
    "code": lambda elem: handle_code(elem),
    "quote": lambda elem: handle_quote(elem),
    "table": lambda elem: handle_table(elem),
    "row": lambda elem: handle_row(elem),
    "cell": lambda elem: handle_cell(elem),
    "ref": lambda elem: handle_ref(elem)
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


def _apply_block_start_escaping(text):
    # start of block/line - we need to be careful about headings (#), items (+, -, *), blockquotes (>), horizontal rules (---)
    # maybe definitions (colon) in extended syntax

    text = BLOCK_START_HASH.sub(r'\1\\#', text)
    text = BLOCK_START_PLUS.sub(r'\1\\+\2', text)
    text = BLOCK_START_MINUS.sub(r'\1\\-\2', text)
    text = BLOCK_START_NUMBERED.sub(r'\1\2\\.\3', text)
    #text = re.sub(r'^(\s*)\*(\s)', r'\1\\*\2', text, flags=re.MULTILINE)  # this is already escaped
    text = BLOCK_START_GT.sub(r'\1\\>', text)
    text = BLOCK_START_COLON.sub(r'\1\\:', text)
    text = BLOCK_START_HR.sub(r'\1\\\2\3', text)

    return text


def apply_paired_escaping(text):
    """Escape paired markdown when the opening character is not followed by whitespace."""
    if not text:
        return text

    result = text

    # Use pre-compiled regex patterns
    result = PAIRED_ASTERISK.sub(r'\\*\2\\*', result)
    result = PAIRED_UNDERSCORE.sub(r'\\_\2\\_', result)
    result = PAIRED_TILDE.sub(r'\\~\2\\~', result)
    result = PAIRED_ANGLE.sub(r'\\<\2\\>', result)

    return result


def escape_markdown_text(text, context="inline"):
    # context is either inline, block_start, table_cell, or link_text (not url)

    if not text:
        return text

    always_escape = [
        ('\\', '\\\\'),  # backslash is the first to be escaped
        ('`', '\\`'),    # backtick can break code formatting
    ]

    result = text
    for char, escaped in always_escape:
        result = result.replace(char, escaped)

    # apply paired escaping for markdown
    result = apply_paired_escaping(result)

    if context == "block_start":
        result = _apply_block_start_escaping(result)

    if context == "inline":
        # escape block-start patterns only if it contains multiple lines (we are not at the beginning of a markdown line now)
        if '\n' in result:
            first_line, rest = result.split('\n', 1)
            escaped_rest = _apply_block_start_escaping(rest)
            result = first_line + '\n' + escaped_rest

    if context == "table_cell":
        # escape pipes inside tables
        result = result.replace('|', '\\|')

    if context == "link_text":
        result = result.replace('[', '\\[')
        result = result.replace(']', '\\]')

    return result


def handle_head(elem):
    rend = elem.get("rend", "h3")

    if rend.startswith("h") and len(rend) > 1 and rend[1:].isdigit():
        level = int(rend[1:])
    else:
        level = 3

    level = max(1, min(6, level))

    result = []
    if elem.text:
        result.append(escape_markdown_text(elem.text))

    for child in elem:
        child_content = process_element(child, inline_context=True)
        result.extend(child_content)
        if child.tail:
            result.append(escape_markdown_text(child.tail))

    text = "".join(result).strip()

    if text:
        return [f"{'#' * level} {text}"]
    else:
        return []


def handle_p(elem):
    result = []
    if elem.text:
        result.append(escape_markdown_text(elem.text, context="block_start"))

    for child in elem:
        child_content = process_element(child, inline_context=True)
        result.extend(child_content)
        if child.tail:
            result.append(escape_markdown_text(child.tail))

    text = "".join(result).strip()
    if text:
        return [text]
    else:
        return []


def handle_div(elem):
    result = []

    if elem.text and elem.text.strip():
        result.append(escape_markdown_text(elem.text.strip(), context="block_start"))

    for i, child in enumerate(elem):
        child_result = process_element(child)
        result.extend(child_result)

        if child.tail and child.tail.strip():
            result.append(escape_markdown_text(child.tail.strip()))

        # Add line break after block elements (except for the last element)
        if i < len(elem) - 1 and child.tag in ["p", "head", "list", "code", "quote", "table"]:
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


def handle_quote(elem):
    """Handle blockquotes."""
    result = []
    if elem.text:
        result.append(escape_markdown_text(elem.text, context="block_start"))

    for child in elem:
        child_content = process_element(child, inline_context=True)
        result.extend(child_content)
        if child.tail:
            result.append(escape_markdown_text(child.tail))

    text = "".join(result).strip()
    if text:
        # Split into lines and prefix each with > for blockquote
        lines = text.split('\n')
        quoted_lines = [f"> {line}" if line.strip() else ">" for line in lines]
        return quoted_lines
    else:
        return []


def handle_table(elem):
    if elem.find(".//table") is not None:
        raise ConversionError("Nested tables are not supported", ConversionError.LOW)

    rows = []
    first_row_checked = False

    for child in elem:
        if child.tag == "row":
            # Check if the first row has header cells
            if not first_row_checked:
                if not any(cell.get("role") == "head" for cell in child.findall("cell")):
                    raise ConversionError("Table does not have a header", ConversionError.LOW)
                first_row_checked = True

            row_data = handle_row(child)
            if row_data:
                rows.append(row_data)
        else:
            raise ConversionError(f"Unexpected element '{child.tag}' in table, expected 'row'", ConversionError.LOW)

    if not rows:
        raise ConversionError("Table element has no rows", ConversionError.LOW)

    result = []
    result.append("| " + " | ".join(rows[0]) + " |")
    result.append("|" + "---|" * len(rows[0]))

    for row_data in rows[1:]:
        while len(row_data) < len(rows[0]):
            row_data.append("")
        result.append("| " + " | ".join(row_data[:len(rows[0])]) + " |")

    return result


def handle_row(elem):
    if elem.get("span") or elem.get("colspan"):
        raise ConversionError("Row elements with span/colspan attributes are not supported", ConversionError.LOW)

    cells = []
    for child in elem:
        if child.tag == "cell":
            cell_content = handle_cell(child)
            if cell_content:
                cells.append(cell_content[0] if cell_content else "")
            else:
                cells.append("")
        else:
            raise ConversionError(f"Unexpected element '{child.tag}' in row, expected 'cell'", ConversionError.LOW)

    if not cells:
        raise ConversionError("Row element has no cells", ConversionError.LOW)

    return cells


def handle_cell(elem):
    if elem.get("span") or elem.get("colspan"):
        raise ConversionError("Cell elements with span/colspan attributes are not supported", ConversionError.LOW)

    result = []
    if elem.text:
        result.append(escape_markdown_text(elem.text, context="table_cell"))

    for child in elem:
        child_content = process_element(child, inline_context=True)
        result.extend(child_content)
        if child.tail:
            result.append(escape_markdown_text(child.tail, context="table_cell"))

    text = "".join(result).strip()
    # Clean up text for table cell (remove newlines, normalize spaces)
    text = " ".join(text.split())
    if text:
        return [text]
    else:
        return [""]


def handle_inline_formatting(elem):
    result = []
    if elem.text:
        result.append(escape_markdown_text(elem.text))

    for child in elem:
        child_content = process_element(child, inline_context=True)
        result.extend(child_content)
        if child.tail:
            result.append(escape_markdown_text(child.tail))

    text = "".join(result).strip()

    if not text:
        return []

    if elem.tag == "hi":
        rend = elem.get("rend", "#i")  # Default to italic if no rend attribute
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
            raise ConversionError(f"Unknown rend attribute for hi element: '{rend}'", ConversionError.MEDIUM)
    elif elem.tag == "del":
        return [f"~~{text}~~"]
    else:
        raise ConversionError(f"Unsupported inline formatting element: {elem.tag}", ConversionError.MEDIUM)


def handle_ref(elem):
    result = []
    if elem.text:
        result.append(escape_markdown_text(elem.text, context="link_text"))

    for child in elem:
        child_content = process_element(child, inline_context=True)
        result.extend(child_content)
        if child.tail:
            result.append(escape_markdown_text(child.tail, context="link_text"))

    link_text = "".join(result).strip()
    target = elem.get("target", "")

    if not target:
        raise ConversionError("ref element missing target attribute", ConversionError.MEDIUM)

    if not link_text:
        # If no text content, use the target as text
        link_text = escape_markdown_text(target, context="link_text")

    return [f"[{link_text}]({target})"]


def handle_list(elem, depth=0):
    if depth >= MAX_LIST_DEPTH:
        raise ConversionError(f"List nesting depth exceeded maximum of {MAX_LIST_DEPTH}", ConversionError.MEDIUM)

    result = []
    list_type = elem.get("rend", "ul")
    item_count = 1

    for child in elem:
        if child.tag == "item":
            item_content = handle_item(child, depth)
            if item_content:
                indent = "  " * depth
                if list_type == "ol":
                    result.append(f"{indent}{item_count}. {item_content[0]}")
                    item_count += 1
                else:
                    result.append(f"{indent}- {item_content[0]}")

                for line in item_content[1:]:
                    result.append(f"{indent}  {line}")
        elif child.tag == "list":
            # Handle nested lists directly within the parent list
            nested_list = handle_list(child, depth + 1)
            result.extend(nested_list)
        else:
            raise ConversionError(f"Unexpected element '{child.tag}' in list, expected 'item' or 'list'", ConversionError.MEDIUM)

    return result


def handle_item(elem, depth=0):
    result = []

    if elem.text and elem.text.strip():
        result.append(escape_markdown_text(elem.text.strip(), context="block_start"))

    for child in elem:
        if child.tag == "list":
            # Handle nested lists within items
            nested_list = handle_list(child, depth + 1)
            result.extend(nested_list)
        else:
            child_content = process_element(child, inline_context=True)
            result.extend(child_content)

        if child.tail and child.tail.strip():
            result.append(escape_markdown_text(child.tail.strip()))

    if not result:
        text_parts = extract_text_content(elem)
        text = "".join(text_parts).strip()
        if text:
            result.append(escape_markdown_text(text, context="block_start"))

    return result


def process_element(elem, inline_context=False):
    if elem.tag not in HANDLERS:
        raise ConversionError(f"No handler for element: {elem.tag}", ConversionError.HIGH)

    handler = HANDLERS[elem.tag]

    # Special handling for code elements to pass context
    if elem.tag == "code":
        return handle_code(elem, inline_context)
    else:
        return handler(elem)


def xml_to_markdown(xml_string):
    root = ET.fromstring(xml_string)
    main_elem = root.find(".//main")
    if main_elem is None:
        raise ConversionError("No main element found in XML", ConversionError.HIGH)

    if not main_elem.text and len(main_elem) == 0:
        raise ConversionError("Main element is empty", ConversionError.LOW)

    return process_element(main_elem)


def process_single(item, line_num=None):
    global TOTAL_LINES, SUCCESSFUL_CONVERSIONS

    TOTAL_LINES += 1
    line_prefix = f"Line {line_num}: " if line_num else ""

    try:
        if XML_FIELD_NAME not in item:
            raise ConversionError(f"No '{XML_FIELD_NAME}' field in item, skipping conversion", ConversionError.MEDIUM)

        xml_content = item[XML_FIELD_NAME]
        if xml_content is None:
            raise ConversionError(f"'{XML_FIELD_NAME}' field is null, skipping conversion", ConversionError.MEDIUM)

        markdown_content = xml_to_markdown(xml_content)
        if markdown_content:
            md_text = "\n".join(markdown_content)
            if MD_ONLY:
                item = {"md": md_text}
            else:
                item["md"] = md_text
            SUCCESSFUL_CONVERSIONS += 1
        else:
            # in case no markdown was generated but without error,
            # using empty string instead of null
            if MD_ONLY:
                item = {"md": ""}
            else:
                item["md"] = ""
            raise ConversionError("No markdown content generated", ConversionError.CRITICAL)

    except ET.ParseError as e:
        # Convert XML parse errors to ConversionError and let the handler below deal with it
        error = ConversionError(f"Malformed XML content: {e}", ConversionError.HIGH)
        if error.severity <= VERBOSITY_LEVEL:
            print(f"{line_prefix}Conversion error: {error}", file=sys.stderr)

    except ConversionError as e:
        # Only print errors with severity <= verbosity level
        if e.severity <= VERBOSITY_LEVEL:
            print(f"{line_prefix}Conversion error: {e}", file=sys.stderr)

    if "md" not in item:
        if MD_ONLY:
            item = {"md": None}
        else:
            item["md"] = None

    return item


def main():
    global TOTAL_LINES, SUCCESSFUL_CONVERSIONS

    line_num = 1
    for line in sys.stdin:
        line = line.strip()
        if not line:
            print(f"Line {line_num}: Empty line in input", file=sys.stderr)
            print()
            continue

        try:
            item = json.loads(line)
            processed = process_single(item, line_num)
            print(json.dumps(processed, ensure_ascii=False))

        except json.JSONDecodeError as e:
            # Output the original line if JSON parsing fails
            if VERBOSITY_LEVEL >= ConversionError.HIGH:
                print(f"Line {line_num}: JSON decode error: {e}", file=sys.stderr)
            print(line)

        line_num += 1

    if VERBOSITY_LEVEL >= ConversionError.CRITICAL:
        print(f"Conversion complete: {SUCCESSFUL_CONVERSIONS}/{TOTAL_LINES} lines successfully converted", file=sys.stderr)


if __name__ == "__main__":
    parser = argparse.ArgumentParser("Convert trafilatura XML output to markdown")
    parser.add_argument("--max-list-depth", type=int, default=5, help="Maximum nesting depth for lists")
    parser.add_argument("--xml-field", type=str, default="x", help="Name of the JSON field containing XML content (default: x)")
    parser.add_argument("--md-only", action="store_true", help="Output only the 'md' field instead of augmenting the original JSON")
    parser.add_argument("--verbosity", "-v", type=int, default=2,
                       help="Verbosity level (0=quiet, 1=critical only, 2=high+, 3=medium+, 4=all errors)")
    args = parser.parse_args()

    MAX_LIST_DEPTH = args.max_list_depth
    VERBOSITY_LEVEL = args.verbosity
    XML_FIELD_NAME = args.xml_field
    MD_ONLY = args.md_only

    main()

