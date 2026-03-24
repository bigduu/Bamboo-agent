#!/usr/bin/env python3
"""Update an existing Confluence page with automatic version handling.

Auto-fetches the current version and increments it.  On a 409 conflict,
retries once with the latest version.

Usage:
    python3 update_page.py --page-id 123456 --body "<p>New content</p>"
    python3 update_page.py --page-id 123456 --body-file /tmp/updated.html
    python3 update_page.py --page-id 123456 --body "<p>New</p>" --title "New Title"
    echo "<p>piped</p>" | python3 update_page.py --page-id 123456 --body-stdin

Output: JSON with updated page id, title, version, url.
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import confluence_client as cc


def _fetch_current(page_id):
    """Fetch current page metadata + body to prepare an update."""
    status, data = cc.get(
        f"/rest/api/content/{page_id}",
        expand="body.storage,version,space",
    )
    if status == 404:
        cc.fail(f"Page {page_id} not found", status_code=404)
    if status != 200:
        cc.fail(f"Failed to fetch page {page_id}", status_code=status, body=str(data))
    return data


def update_page(page_id, body_storage=None, title=None, max_retries=1):
    """Update a Confluence page, handling version conflicts.

    If body_storage is None, the current body is kept (useful for title-only updates).
    """
    current = _fetch_current(page_id)
    current_version = current.get("version", {}).get("number", 1)
    current_title = current.get("title", "")
    current_body = current.get("body", {}).get("storage", {}).get("value", "")

    payload = {
        "id": str(page_id),
        "type": "page",
        "title": title if title else current_title,
        "version": {"number": current_version + 1},
        "body": {
            "storage": {
                "value": body_storage if body_storage is not None else current_body,
                "representation": "storage",
            }
        },
    }

    for attempt in range(max_retries + 1):
        status, data = cc.put(f"/rest/api/content/{page_id}", json_body=payload)

        if status in (200, 201):
            result = {
                "id": data.get("id"),
                "title": data.get("title"),
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

        if status == 409 and attempt < max_retries:
            # Version conflict — refetch and retry
            current = _fetch_current(page_id)
            new_version = current.get("version", {}).get("number", 1)
            payload["version"]["number"] = new_version + 1
            # Also refresh body if we didn't provide a custom one
            if body_storage is None:
                payload["body"]["storage"]["value"] = (
                    current.get("body", {}).get("storage", {}).get("value", "")
                )
            continue

        msg = "Update page failed"
        if isinstance(data, dict):
            msg = data.get("message", msg)
        cc.fail(msg, status_code=status, body=str(data))


def main():
    parser = argparse.ArgumentParser(description="Update a Confluence page")
    parser.add_argument("--page-id", required=True, help="Page ID to update")
    parser.add_argument("--title", default=None, help="New page title (optional)")

    body_group = parser.add_mutually_exclusive_group()
    body_group.add_argument("--body", help="New body in storage format (inline)")
    body_group.add_argument("--body-file", help="Path to file with new body")
    body_group.add_argument("--body-stdin", action="store_true",
                            help="Read new body from stdin")

    args = parser.parse_args()

    body_storage = None
    if args.body:
        body_storage = args.body
    elif args.body_file:
        with open(args.body_file, "r", encoding="utf-8") as f:
            body_storage = f.read()
    elif args.body_stdin:
        body_storage = sys.stdin.read()

    if body_storage is None and args.title is None:
        cc.fail("Provide --body, --body-file, --body-stdin, or --title")

    cc.init()
    result = update_page(args.page_id, body_storage=body_storage, title=args.title)
    cc.ok(result)


if __name__ == "__main__":
    main()
