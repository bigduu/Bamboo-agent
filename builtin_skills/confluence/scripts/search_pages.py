#!/usr/bin/env python3
"""Search Confluence pages by title, space, or CQL query.

Usage:
    python3 search_pages.py --title "VPN Onboarding" --space ENG
    python3 search_pages.py --cql 'space=ENG and label=runbook'
    python3 search_pages.py --title "onboarding" --space ENG --metadata-only
    python3 search_pages.py --cql 'space=PLAT and text~"rollback"' --limit 10

Output: JSON with matched pages (id, title, space, url, version, labels, ancestors).
"""

import argparse
import os
import sys

# Allow importing confluence_client from the same directory
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import confluence_client as cc


def _summarise_page(page, metadata_only=False):
    """Extract a compact summary dict from a Confluence page response."""
    info = {
        "id": page.get("id"),
        "title": page.get("title"),
        "type": page.get("type"),
        "status": page.get("status"),
    }
    # space
    space = page.get("space")
    if space:
        info["space_key"] = space.get("key")
        info["space_name"] = space.get("name")

    # version
    ver = page.get("version")
    if ver:
        info["version"] = ver.get("number")
        info["last_modified"] = ver.get("when")
        by = ver.get("by")
        if by:
            info["modified_by"] = by.get("displayName") or by.get("username")

    # ancestors (page tree context)
    ancestors = page.get("ancestors")
    if ancestors:
        info["ancestors"] = [
            {"id": a.get("id"), "title": a.get("title")} for a in ancestors
        ]

    # labels
    labels_obj = page.get("metadata", {}).get("labels")
    if labels_obj and "results" in labels_obj:
        info["labels"] = [lb.get("name") for lb in labels_obj["results"]]

    # self link / web UI link
    links = page.get("_links", {})
    base = links.get("base", "")
    webui = links.get("webui", "")
    if base and webui:
        info["url"] = base + webui

    # body (only if not metadata-only)
    if not metadata_only:
        body = page.get("body", {}).get("storage", {}).get("value")
        if body:
            info["body_storage"] = body

    return info


def search_by_title(title, space=None, limit=25, metadata_only=False):
    """Search using the content endpoint with title filter."""
    expand = "version,space,ancestors,metadata.labels"
    if not metadata_only:
        expand += ",body.storage"

    params = {"type": "page", "title": title, "limit": str(limit), "expand": expand}
    if space:
        params["spaceKey"] = space

    status, data = cc.get("/rest/api/content", **params)
    if status != 200:
        cc.fail(f"Search failed (HTTP {status})", status_code=status, body=str(data))

    results = data.get("results", [])
    return [_summarise_page(p, metadata_only) for p in results]


def search_by_cql(cql, limit=25, metadata_only=False):
    """Search using CQL."""
    expand = "version,space,ancestors,metadata.labels"
    if not metadata_only:
        expand += ",body.storage"

    params = {"cql": cql, "limit": str(limit), "expand": expand}
    status, data = cc.get("/rest/api/content/search", **params)
    if status != 200:
        cc.fail(f"CQL search failed (HTTP {status})", status_code=status, body=str(data))

    results = data.get("results", [])
    return [_summarise_page(p, metadata_only) for p in results]


def main():
    parser = argparse.ArgumentParser(description="Search Confluence pages")
    parser.add_argument("--title", help="Page title to search for")
    parser.add_argument("--space", help="Space key (e.g. ENG, OPS, PLAT)")
    parser.add_argument("--cql", help="CQL query string")
    parser.add_argument("--limit", type=int, default=25, help="Max results (default 25)")
    parser.add_argument("--metadata-only", action="store_true",
                        help="Skip body.storage, return metadata only")

    args = parser.parse_args()

    if not args.title and not args.cql:
        cc.fail("Provide --title or --cql")

    cc.init()

    if args.cql:
        pages = search_by_cql(args.cql, limit=args.limit, metadata_only=args.metadata_only)
    else:
        pages = search_by_title(args.title, space=args.space, limit=args.limit,
                                metadata_only=args.metadata_only)

    cc.ok({
        "total": len(pages),
        "pages": pages,
    })


if __name__ == "__main__":
    main()
