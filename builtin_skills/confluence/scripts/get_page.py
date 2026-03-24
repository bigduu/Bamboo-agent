#!/usr/bin/env python3
"""Fetch a Confluence page by ID with configurable expand fields.

Usage:
    python3 get_page.py --page-id 123456
    python3 get_page.py --page-id 123456 --metadata-only
    python3 get_page.py --page-id 123456 --expand body.storage,version,space

Output: JSON with page id, title, version, body, ancestors, labels, url.
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import confluence_client as cc


def get_page(page_id, expand=None, metadata_only=False):
    """Fetch a single page by ID."""
    if expand:
        exp = expand
    elif metadata_only:
        exp = "version,space,ancestors,metadata.labels"
    else:
        exp = "body.storage,version,space,ancestors,metadata.labels"

    status, data = cc.get(f"/rest/api/content/{page_id}", expand=exp)
    if status == 404:
        cc.fail(f"Page {page_id} not found", status_code=404)
    if status != 200:
        cc.fail(f"Failed to fetch page {page_id} (HTTP {status})",
                status_code=status, body=str(data))

    info = {
        "id": data.get("id"),
        "title": data.get("title"),
        "type": data.get("type"),
        "status": data.get("status"),
    }

    # space
    space = data.get("space")
    if space:
        info["space_key"] = space.get("key")
        info["space_name"] = space.get("name")

    # version
    ver = data.get("version")
    if ver:
        info["version"] = ver.get("number")
        info["last_modified"] = ver.get("when")
        by = ver.get("by")
        if by:
            info["modified_by"] = by.get("displayName") or by.get("username")

    # ancestors
    ancestors = data.get("ancestors")
    if ancestors:
        info["ancestors"] = [
            {"id": a.get("id"), "title": a.get("title")} for a in ancestors
        ]

    # labels
    labels_obj = data.get("metadata", {}).get("labels")
    if labels_obj and "results" in labels_obj:
        info["labels"] = [lb.get("name") for lb in labels_obj["results"]]

    # links
    links = data.get("_links", {})
    base = links.get("base", "")
    webui = links.get("webui", "")
    if base and webui:
        info["url"] = base + webui

    # body
    body = data.get("body", {}).get("storage", {}).get("value")
    if body is not None:
        info["body_storage"] = body

    return info


def main():
    parser = argparse.ArgumentParser(description="Fetch a Confluence page by ID")
    parser.add_argument("--page-id", required=True, help="Confluence page ID")
    parser.add_argument("--expand", default=None,
                        help="Custom expand fields (comma-separated)")
    parser.add_argument("--metadata-only", action="store_true",
                        help="Skip body.storage, return metadata only")
    args = parser.parse_args()

    cc.init()
    page = get_page(args.page_id, expand=args.expand, metadata_only=args.metadata_only)
    cc.ok(page)


if __name__ == "__main__":
    main()
