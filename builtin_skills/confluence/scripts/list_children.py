#!/usr/bin/env python3
"""List child pages under a given Confluence page with pagination.

Usage:
    python3 list_children.py --page-id 123456
    python3 list_children.py --page-id 123456 --limit 50 --start 0

Output: JSON with child pages (id, title, version, url).
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import confluence_client as cc


def list_children(page_id, limit=25, start=0):
    """List child pages under a parent page ID."""
    status, data = cc.get(
        f"/rest/api/content/{page_id}/child/page",
        limit=str(limit),
        start=str(start),
        expand="version,space",
    )
    if status == 404:
        cc.fail(f"Page {page_id} not found", status_code=404)
    if status != 200:
        cc.fail(f"Failed to list children (HTTP {status})",
                status_code=status, body=str(data))

    results = data.get("results", [])
    children = []
    for p in results:
        child = {
            "id": p.get("id"),
            "title": p.get("title"),
            "status": p.get("status"),
        }
        ver = p.get("version")
        if ver:
            child["version"] = ver.get("number")
            child["last_modified"] = ver.get("when")

        space = p.get("space")
        if space:
            child["space_key"] = space.get("key")

        links = p.get("_links", {})
        base = links.get("base", "")
        webui = links.get("webui", "")
        if base and webui:
            child["url"] = base + webui

        children.append(child)

    page_info = {
        "parent_id": page_id,
        "total_children": data.get("size", len(children)),
        "start": start,
        "limit": limit,
        "children": children,
    }

    # pagination info
    if data.get("_links", {}).get("next"):
        page_info["has_more"] = True
        page_info["next_start"] = start + limit
    else:
        page_info["has_more"] = False

    return page_info


def main():
    parser = argparse.ArgumentParser(description="List child pages")
    parser.add_argument("--page-id", required=True, help="Parent page ID")
    parser.add_argument("--limit", type=int, default=25, help="Max results (default 25)")
    parser.add_argument("--start", type=int, default=0, help="Start offset (default 0)")
    args = parser.parse_args()

    cc.init()
    result = list_children(args.page_id, limit=args.limit, start=args.start)
    cc.ok(result)


if __name__ == "__main__":
    main()
