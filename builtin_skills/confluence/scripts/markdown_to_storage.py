#!/usr/bin/env python3
"""Convert Markdown to Confluence storage format (XHTML-like).

Standalone converter using only Python stdlib — no third-party Markdown
libraries required.  Handles the subset of Markdown that maps cleanly to
Confluence storage format: headings, paragraphs, bold, italic, inline code,
code blocks, links, images, unordered/ordered lists, tables, horizontal rules,
and blockquotes.

Usage:
    python3 markdown_to_storage.py --input notes.md --output body.html
    python3 markdown_to_storage.py --input notes.md            # stdout
    echo "# Hello" | python3 markdown_to_storage.py            # stdin → stdout
    python3 markdown_to_storage.py --text "# Hello\n\nWorld"   # inline

Output: Confluence storage markup (XHTML) printed to stdout or written to --output.
"""

import argparse
import re
import sys


# ── inline formatting ──────────────────────────────────────────────

def _inline(text):
    """Convert inline Markdown to XHTML: bold, italic, code, links, images."""
    # inline code (must come before bold/italic to protect backtick content)
    text = re.sub(r'`([^`]+)`', r'<code>\1</code>', text)
    # images ![alt](url)
    text = re.sub(r'!\[([^\]]*)\]\(([^)]+)\)', r'<ac:image><ri:url ri:value="\2" /></ac:image>', text)
    # links [text](url)
    text = re.sub(r'\[([^\]]+)\]\(([^)]+)\)', r'<a href="\2">\1</a>', text)
    # bold **text** or __text__
    text = re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', text)
    text = re.sub(r'__(.+?)__', r'<strong>\1</strong>', text)
    # italic *text* or _text_
    text = re.sub(r'\*(.+?)\*', r'<em>\1</em>', text)
    text = re.sub(r'(?<!\w)_(.+?)_(?!\w)', r'<em>\1</em>', text)
    return text


# ── block-level parser ─────────────────────────────────────────────

def convert(md_text):
    """Convert Markdown text to Confluence storage format."""
    lines = md_text.split("\n")
    out = []
    i = 0

    while i < len(lines):
        line = lines[i]

        # ── fenced code block ```
        if line.strip().startswith("```"):
            lang = line.strip().lstrip("`").strip()
            code_lines = []
            i += 1
            while i < len(lines) and not lines[i].strip().startswith("```"):
                code_lines.append(lines[i])
                i += 1
            i += 1  # skip closing ```
            code = "\n".join(code_lines)
            # Escape HTML entities in code
            code = code.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            if lang:
                out.append(
                    f'<ac:structured-macro ac:name="code">'
                    f'<ac:parameter ac:name="language">{lang}</ac:parameter>'
                    f'<ac:plain-text-body><![CDATA[{code}]]></ac:plain-text-body>'
                    f'</ac:structured-macro>'
                )
            else:
                out.append(f'<ac:structured-macro ac:name="code">'
                           f'<ac:plain-text-body><![CDATA[{code}]]></ac:plain-text-body>'
                           f'</ac:structured-macro>')
            continue

        # ── heading # through ######
        m = re.match(r'^(#{1,6})\s+(.*)', line)
        if m:
            level = len(m.group(1))
            text = _inline(m.group(2).strip())
            out.append(f'<h{level}>{text}</h{level}>')
            i += 1
            continue

        # ── horizontal rule
        if re.match(r'^(-{3,}|\*{3,}|_{3,})\s*$', line.strip()):
            out.append('<hr />')
            i += 1
            continue

        # ── table (| col | col |)
        if line.strip().startswith("|"):
            table_lines = []
            while i < len(lines) and lines[i].strip().startswith("|"):
                table_lines.append(lines[i])
                i += 1
            out.append(_parse_table(table_lines))
            continue

        # ── blockquote >
        if line.strip().startswith(">"):
            bq_lines = []
            while i < len(lines) and lines[i].strip().startswith(">"):
                bq_lines.append(re.sub(r'^>\s?', '', lines[i]))
                i += 1
            inner = _inline(" ".join(bq_lines))
            out.append(
                f'<ac:structured-macro ac:name="info">'
                f'<ac:rich-text-body><p>{inner}</p></ac:rich-text-body>'
                f'</ac:structured-macro>'
            )
            continue

        # ── unordered list (- or *)
        if re.match(r'^[\s]*[-*]\s+', line):
            items, i = _collect_list(lines, i, ordered=False)
            out.append(_render_list(items, ordered=False))
            continue

        # ── ordered list (1. 2. etc.)
        if re.match(r'^[\s]*\d+\.\s+', line):
            items, i = _collect_list(lines, i, ordered=True)
            out.append(_render_list(items, ordered=True))
            continue

        # ── blank line
        if not line.strip():
            i += 1
            continue

        # ── paragraph (collect consecutive non-blank lines)
        para_lines = []
        while i < len(lines) and lines[i].strip() and not _is_block_start(lines[i]):
            para_lines.append(lines[i])
            i += 1
        text = _inline(" ".join(para_lines))
        out.append(f'<p>{text}</p>')

    return "\n".join(out)


def _is_block_start(line):
    """Check if a line starts a new block element."""
    s = line.strip()
    if s.startswith("#"):
        return True
    if s.startswith("```"):
        return True
    if s.startswith("|"):
        return True
    if s.startswith(">"):
        return True
    if re.match(r'^[-*]\s+', s):
        return True
    if re.match(r'^\d+\.\s+', s):
        return True
    if re.match(r'^(-{3,}|\*{3,}|_{3,})\s*$', s):
        return True
    return False


def _collect_list(lines, start, ordered=False):
    """Collect list items (handles simple single-level lists)."""
    items = []
    i = start
    if ordered:
        pattern = re.compile(r'^[\s]*\d+\.\s+(.*)')
    else:
        pattern = re.compile(r'^[\s]*[-*]\s+(.*)')

    while i < len(lines):
        m = pattern.match(lines[i])
        if m:
            items.append(m.group(1).strip())
            i += 1
        else:
            break
    return items, i


def _render_list(items, ordered=False):
    """Render list items to XHTML."""
    tag = "ol" if ordered else "ul"
    li = "".join(f'<li>{_inline(item)}</li>' for item in items)
    return f'<{tag}>{li}</{tag}>'


def _parse_table(table_lines):
    """Parse Markdown table lines to Confluence XHTML table."""
    rows = []
    for line in table_lines:
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        rows.append(cells)

    if len(rows) < 2:
        # Not enough rows for a valid table
        return "<p>" + " | ".join(rows[0]) + "</p>" if rows else ""

    # Check if second row is a separator (---|----|---)
    is_separator = all(re.match(r'^[-:]+$', c.strip()) for c in rows[1] if c.strip())

    out = ['<table>']
    if is_separator:
        # First row = header
        out.append('<thead><tr>')
        for cell in rows[0]:
            out.append(f'<th>{_inline(cell)}</th>')
        out.append('</tr></thead>')
        data_rows = rows[2:]
    else:
        data_rows = rows

    out.append('<tbody>')
    for row in data_rows:
        out.append('<tr>')
        for cell in row:
            out.append(f'<td>{_inline(cell)}</td>')
        out.append('</tr>')
    out.append('</tbody></table>')
    return "".join(out)


# ── CLI ────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Convert Markdown to Confluence storage format"
    )
    parser.add_argument("--input", "-i", default=None, help="Input Markdown file")
    parser.add_argument("--output", "-o", default=None, help="Output file (default: stdout)")
    parser.add_argument("--text", "-t", default=None,
                        help="Inline Markdown text (supports \\n escapes)")
    args = parser.parse_args()

    if args.text:
        md = args.text.replace("\\n", "\n")
    elif args.input:
        with open(args.input, "r", encoding="utf-8") as f:
            md = f.read()
    else:
        md = sys.stdin.read()

    result = convert(md)

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(result)
        print(f"Written to {args.output}", file=sys.stderr)
    else:
        print(result)


if __name__ == "__main__":
    main()
