#!/usr/bin/env python3
"""View version history of a Confluence page.

Shows who changed what, when, and optionally includes the body of a
specific version for comparison.

Usage:
    python3 page_history.py --page-id 123456
    python3 page_history.py --page-id 123456 --limit 10
    python3 page_history.py --page-id 123456 --version 5 --expand-body

Output: JSON with version history entries.
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import confluence_client as cc


def get_history(page_id, limit=25):
    """Fetch version history for a page.

    Uses the /rest/api/content/{id}/version endpoint which returns
    all versions with author, message, and timestamp.
    """
    versions = []
    start = 0

    while True:
        status, data = cc.get(
            f"/rest/api/content/{page_id}/version",
            limit=str(limit),
            start=str(start),
        )
        if status == 404:
            cc.fail(f"Page {page_id} not found", status_code=404)
        if status != 200:
            cc.fail(f"Failed to get version history (HTTP {status})",
                    status_code=status, body=str(data))

        results = data.get("results", [])
        for v in results:
            entry = {
                "number": v.get("number"),
                "when": v.get("when"),
                "message": v.get("message", ""),
                "minorEdit": v.get("minorEdit", False),
            }
            by = v.get("by")
            if by:
                entry["author"] = by.get("displayName", by.get("username", "unknown"))
                entry["author_key"] = by.get("userKey", by.get("username"))
            versions.append(entry)

        # Pagination: if we got fewer results than limit, we're done
        if len(results) < limit:
            break
        start += limit

    return versions


def get_version_body(page_id, version_number):
    """Fetch a specific version's body.storage content.

    GET /rest/api/content/{id}?status=historical&version={n}&expand=body.storage,version
    """
    status, data = cc.get(
        f"/rest/api/content/{page_id}",
        status="historical",
        version=str(version_number),
        expand="body.storage,version",
    )
    if status == 404:
        cc.fail(f"Page {page_id} version {version_number} not found", status_code=404)
    if status != 200:
        cc.fail(f"Failed to fetch version {version_number} (HTTP {status})",
                status_code=status, body=str(data))

    body_val = ""
    body = data.get("body", {}).get("storage", {})
    if body:
        body_val = body.get("value", "")

    ver = data.get("version", {})
    return {
        "page_id": page_id,
        "version": ver.get("number"),
        "when": ver.get("when"),
        "author": ver.get("by", {}).get("displayName", "unknown"),
        "title": data.get("title"),
        "body_length": len(body_val),
        "body_storage": body_val,
    }


def main():
    parser = argparse.ArgumentParser(
        description="View version history of a Confluence page",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Examples:
  python3 page_history.py --page-id 123456
  python3 page_history.py --page-id 123456 --limit 5
  python3 page_history.py --page-id 123456 --version 3 --expand-body
""")
    parser.add_argument("--page-id", required=True, help="Page ID")
    parser.add_argument("--limit", type=int, default=25,
                        help="Max versions to fetch (default: 25)")
    parser.add_argument("--version", type=int, default=None,
                        help="Fetch a specific version number")
    parser.add_argument("--expand-body", action="store_true",
                        help="Include body.storage for the specified --version")
    args = parser.parse_args()

    cc.init()

    # If user asked for a specific version with body
    if args.version and args.expand_body:
        result = get_version_body(args.page_id, args.version)
        cc.ok(result)
        return

    # List version history
    versions = get_history(args.page_id, limit=args.limit)

    # If user asked for a specific version (metadata only)
    if args.version:
        target = [v for v in versions if v.get("number") == args.version]
        if not target:
            cc.fail(f"Version {args.version} not found in history",
                    available_versions=[v.get("number") for v in versions[:10]])
        cc.ok({"page_id": args.page_id, "version": target[0]})
        return

    cc.ok({
        "page_id": args.page_id,
        "total_versions": len(versions),
        "versions": versions,
    })


if __name__ == "__main__":
    main()
