#!/usr/bin/env python3
"""Create a new Confluence page with safe JSON payload building.

Usage:
    python3 create_page.py --space ENG --title "New Runbook" --parent-id 987654 --body "<h1>Heading</h1><p>Body</p>"
    python3 create_page.py --space PLAT --title "Sync Notes" --parent-id 123456 --body-file /tmp/body.html
    echo "<p>inline</p>" | python3 create_page.py --space ENG --title "Quick Page" --body-stdin

Output: JSON with new page id, title, version, url.
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import confluence_client as cc


def create_page(space_key, title, body_storage, parent_id=None):
    """Create a Confluence page and return the response summary."""
    payload = {
        "type": "page",
        "title": title,
        "space": {"key": space_key},
        "body": {
            "storage": {
                "value": body_storage,
                "representation": "storage",
            }
        },
    }
    if parent_id:
        payload["ancestors"] = [{"id": str(parent_id)}]

    status, data = cc.post("/rest/api/content", json_body=payload)
    if status not in (200, 201):
        msg = "Create page failed"
        if isinstance(data, dict):
            msg = data.get("message", msg)
        cc.fail(msg, status_code=status, body=str(data))

    result = {
        "id": data.get("id"),
        "title": data.get("title"),
        "space_key": space_key,
    }
    ver = data.get("version")
    if ver:
        result["version"] = ver.get("number")

    links = data.get("_links", {})
    base = links.get("base", "")
    webui = links.get("webui", "")
    if base and webui:
        result["url"] = base + webui

    return result


def main():
    parser = argparse.ArgumentParser(description="Create a Confluence page")
    parser.add_argument("--space", required=True, help="Space key (e.g. ENG)")
    parser.add_argument("--title", required=True, help="Page title")
    parser.add_argument("--parent-id", default=None, help="Parent page ID")

    body_group = parser.add_mutually_exclusive_group(required=True)
    body_group.add_argument("--body", help="Body in Confluence storage format (inline)")
    body_group.add_argument("--body-file", help="Path to file containing body storage markup")
    body_group.add_argument("--body-stdin", action="store_true",
                            help="Read body from stdin")

    args = parser.parse_args()

    if args.body:
        body_storage = args.body
    elif args.body_file:
        with open(args.body_file, "r", encoding="utf-8") as f:
            body_storage = f.read()
    elif args.body_stdin:
        body_storage = sys.stdin.read()
    else:
        cc.fail("No body provided")

    cc.init()
    result = create_page(args.space, args.title, body_storage, parent_id=args.parent_id)
    cc.ok(result)


if __name__ == "__main__":
    main()
