#!/usr/bin/env python3
"""List all Confluence spaces.

Useful when the user doesn't know the space key. Returns space keys,
names, types, and optional descriptions.

Usage:
    python3 list_spaces.py
    python3 list_spaces.py --type global
    python3 list_spaces.py --type personal --limit 10
    python3 list_spaces.py --query "engineering"

Output: JSON with list of spaces.
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import confluence_client as cc


def list_spaces(space_type=None, limit=25, query=None):
    """Fetch spaces from Confluence.

    GET /rest/api/space?limit=25&type=global
    Handles pagination to return all matching spaces up to a cap.
    """
    spaces = []
    start = 0
    max_total = 500  # Safety cap

    while len(spaces) < max_total:
        params = {"limit": str(limit), "start": str(start)}
        if space_type:
            params["type"] = space_type

        status, data = cc.get("/rest/api/space", **params)
        if status != 200:
            cc.fail(f"Failed to list spaces (HTTP {status})",
                    status_code=status, body=str(data))

        results = data.get("results", [])
        for s in results:
            entry = {
                "key": s.get("key"),
                "name": s.get("name"),
                "type": s.get("type"),
                "status": s.get("status"),
            }
            desc = s.get("description", {})
            if desc and desc.get("plain", {}).get("value"):
                entry["description"] = desc["plain"]["value"][:200]

            # Filter by query if provided (client-side, since the API
            # space endpoint doesn't have a search parameter on all versions)
            if query:
                text = f"{entry.get('key', '')} {entry.get('name', '')} {entry.get('description', '')}".lower()
                if query.lower() not in text:
                    continue

            spaces.append(entry)

        if not data.get("_links", {}).get("next"):
            break
        start += limit

    return spaces


def main():
    parser = argparse.ArgumentParser(
        description="List Confluence spaces",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Examples:
  python3 list_spaces.py
  python3 list_spaces.py --type global
  python3 list_spaces.py --query "platform"
""")
    parser.add_argument("--type", choices=["global", "personal"],
                        help="Filter by space type")
    parser.add_argument("--limit", type=int, default=25,
                        help="Results per page (default: 25)")
    parser.add_argument("--query", type=str, default=None,
                        help="Filter spaces by keyword in key/name/description")
    args = parser.parse_args()

    cc.init()
    spaces = list_spaces(space_type=args.type, limit=args.limit, query=args.query)

    cc.ok({
        "total_spaces": len(spaces),
        "filter_type": args.type,
        "filter_query": args.query,
        "spaces": spaces,
    })


if __name__ == "__main__":
    main()
