#!/usr/bin/env python3
"""Section-level editor for Confluence pages.

Fetches a page, locates a section by heading text, replaces or inserts
content at that section, and optionally pushes the update. Preserves
all content outside the target section.

Usage:
    # Replace section content under heading "Rollback"
    python3 section_editor.py --page-id 123456 --heading "Rollback" \\
        --new-content "<p>Updated rollback steps...</p>"

    # Insert a new section before "Known Issues"
    python3 section_editor.py --page-id 123456 --heading "Known Issues" \\
        --insert-before --new-content "<h2>Rollback</h2><p>Steps...</p>"

    # Dry run — show the merged body without pushing
    python3 section_editor.py --page-id 123456 --heading "Rollback" \\
        --new-content "<p>Updated</p>" --dry-run

    # Read new content from a file
    python3 section_editor.py --page-id 123456 --heading "Rollback" \\
        --new-content-file /tmp/rollback.html

Output: JSON with updated page details or dry-run preview.
"""

import argparse
import json
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import confluence_client as cc


# ── section parsing ────────────────────────────────────────────────

_HEADING_RE = re.compile(r'(<h([1-6])\b[^>]*>)(.*?)(</h\2>)', re.IGNORECASE | re.DOTALL)


def _find_sections(body):
    """Find all heading positions in storage markup.

    Returns list of dicts with keys: start, end, level, title, heading_start, heading_end.
    Each section spans from its heading to the next heading of equal or higher level (or EOF).
    """
    headings = []
    for m in _HEADING_RE.finditer(body):
        headings.append({
            "heading_start": m.start(),
            "heading_end": m.end(),
            "level": int(m.group(2)),
            "title": re.sub(r'<[^>]+>', '', m.group(3)).strip(),  # strip inner tags
        })

    sections = []
    for idx, h in enumerate(headings):
        start = h["heading_start"]
        # Section ends where the next heading of equal or higher level starts, or at EOF
        end = len(body)
        for nh in headings[idx + 1:]:
            if nh["level"] <= h["level"]:
                end = nh["heading_start"]
                break
        sections.append({
            "start": start,
            "end": end,
            "level": h["level"],
            "title": h["title"],
            "heading_start": h["heading_start"],
            "heading_end": h["heading_end"],
        })

    return sections


def _match_section(sections, heading_text):
    """Find the best matching section for a heading text (case-insensitive)."""
    target = heading_text.lower().strip()
    # Exact match first
    for s in sections:
        if s["title"].lower().strip() == target:
            return s
    # Substring match
    for s in sections:
        if target in s["title"].lower().strip():
            return s
    return None


# ── edit operations ────────────────────────────────────────────────

def replace_section(body, heading_text, new_content, keep_heading=True):
    """Replace the content of a section identified by heading text.

    If keep_heading is True, the original heading tag is preserved and only
    the body between this heading and the next one is replaced.
    """
    sections = _find_sections(body)
    section = _match_section(sections, heading_text)
    if section is None:
        return None, f"Section '{heading_text}' not found"

    if keep_heading:
        # Preserve the heading, replace content after it until section end
        before = body[:section["heading_end"]]
        after = body[section["end"]:]
        return before + "\n" + new_content + "\n" + after, None
    else:
        # Replace entire section including heading
        before = body[:section["start"]]
        after = body[section["end"]:]
        return before + new_content + after, None


def insert_before_section(body, heading_text, new_content):
    """Insert content before the section identified by heading text."""
    sections = _find_sections(body)
    section = _match_section(sections, heading_text)
    if section is None:
        return None, f"Section '{heading_text}' not found"

    before = body[:section["start"]]
    after = body[section["start"]:]
    return before + new_content + "\n" + after, None


def append_after_section(body, heading_text, new_content):
    """Append content after the section identified by heading text."""
    sections = _find_sections(body)
    section = _match_section(sections, heading_text)
    if section is None:
        return None, f"Section '{heading_text}' not found"

    before = body[:section["end"]]
    after = body[section["end"]:]
    return before + "\n" + new_content + after, None


def list_sections(body):
    """Return a summary of all sections in the page body."""
    sections = _find_sections(body)
    return [{"level": s["level"], "title": s["title"]} for s in sections]


# ── main ───────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Section-level page editor")
    parser.add_argument("--page-id", required=True, help="Confluence page ID")
    parser.add_argument("--heading", help="Target section heading text")
    parser.add_argument("--list-sections", action="store_true",
                        help="Just list all sections in the page (no edit)")

    parser.add_argument("--insert-before", action="store_true",
                        help="Insert new content before the target section")
    parser.add_argument("--append-after", action="store_true",
                        help="Append new content after the target section")
    parser.add_argument("--replace-heading", action="store_true",
                        help="Replace the heading tag too (default: keep heading)")

    content_group = parser.add_mutually_exclusive_group()
    content_group.add_argument("--new-content", help="New section content (inline)")
    content_group.add_argument("--new-content-file", help="File with new section content")
    content_group.add_argument("--new-content-stdin", action="store_true",
                               help="Read new content from stdin")

    parser.add_argument("--dry-run", action="store_true",
                        help="Show merged body without pushing update")

    args = parser.parse_args()

    cc.init()

    # Fetch current page
    status, data = cc.get(
        f"/rest/api/content/{args.page_id}",
        expand="body.storage,version,space",
    )
    if status == 404:
        cc.fail(f"Page {args.page_id} not found", status_code=404)
    if status != 200:
        cc.fail(f"Failed to fetch page {args.page_id}", status_code=status, body=str(data))

    current_body = data.get("body", {}).get("storage", {}).get("value", "")
    current_version = data.get("version", {}).get("number", 1)
    current_title = data.get("title", "")

    # List sections mode
    if args.list_sections:
        sections = list_sections(current_body)
        cc.ok({
            "page_id": args.page_id,
            "title": current_title,
            "version": current_version,
            "sections": sections,
        })

    # Edit mode
    if not args.heading:
        cc.fail("Provide --heading to identify the target section, or use --list-sections")

    # Get new content
    new_content = None
    if args.new_content:
        new_content = args.new_content
    elif args.new_content_file:
        with open(args.new_content_file, "r", encoding="utf-8") as f:
            new_content = f.read()
    elif args.new_content_stdin:
        new_content = sys.stdin.read()

    if new_content is None:
        cc.fail("Provide --new-content, --new-content-file, or --new-content-stdin")

    # Perform edit
    if args.insert_before:
        merged, err = insert_before_section(current_body, args.heading, new_content)
    elif args.append_after:
        merged, err = append_after_section(current_body, args.heading, new_content)
    else:
        merged, err = replace_section(
            current_body, args.heading, new_content,
            keep_heading=not args.replace_heading,
        )

    if err:
        cc.fail(err)

    # Dry run
    if args.dry_run:
        cc.ok({
            "page_id": args.page_id,
            "title": current_title,
            "dry_run": True,
            "merged_body": merged,
            "sections_after": list_sections(merged),
        })

    # Push update
    payload = {
        "id": str(args.page_id),
        "type": "page",
        "title": current_title,
        "version": {"number": current_version + 1},
        "body": {
            "storage": {
                "value": merged,
                "representation": "storage",
            }
        },
    }

    status, resp = cc.put(f"/rest/api/content/{args.page_id}", json_body=payload)
    if status not in (200, 201):
        msg = "Section update failed"
        if isinstance(resp, dict):
            msg = resp.get("message", msg)
        cc.fail(msg, status_code=status, body=str(resp))

    result = {
        "page_id": resp.get("id"),
        "title": resp.get("title"),
        "version": resp.get("version", {}).get("number"),
        "section_edited": args.heading,
    }
    links = resp.get("_links", {})
    base = links.get("base", "")
    webui = links.get("webui", "")
    if base and webui:
        result["url"] = base + webui

    cc.ok(result)


if __name__ == "__main__":
    main()
