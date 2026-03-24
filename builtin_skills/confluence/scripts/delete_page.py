#!/usr/bin/env python3
"""Delete or trash a Confluence page.

Confluence Server/DC "deletes" by moving to trash. The page can be
restored from the trash unless permanently purged by an admin.

Usage:
    python3 delete_page.py --page-id 123456
    python3 delete_page.py --page-id 123456 --dry-run

Output: JSON confirmation of deletion.
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import confluence_client as cc


def get_page_info(page_id):
    """Fetch basic page info before deletion (for confirmation)."""
    status, data = cc.get(
        f"/rest/api/content/{page_id}",
        expand="version,space,ancestors",
    )
    if status == 404:
        cc.fail(f"Page {page_id} not found", status_code=404)
    if status != 200:
        cc.fail(f"Failed to fetch page {page_id}", status_code=status, body=str(data))

    info = {
        "id": data.get("id"),
        "title": data.get("title"),
        "status": data.get("status"),
    }
    space = data.get("space")
    if space:
        info["space_key"] = space.get("key")
    ver = data.get("version")
    if ver:
        info["version"] = ver.get("number")
    ancestors = data.get("ancestors", [])
    if ancestors:
        info["parent_id"] = ancestors[-1].get("id")
        info["parent_title"] = ancestors[-1].get("title")
    return info


def delete_page(page_id):
    """Delete (trash) a page.

    Confluence Server/DC: DELETE /rest/api/content/{id}
    Returns 204 on success. The page is moved to trash.
    """
    status, data = cc.delete(f"/rest/api/content/{page_id}")
    if status == 404:
        cc.fail(f"Page {page_id} not found", status_code=404)
    if status == 403:
        cc.fail(f"Permission denied: cannot delete page {page_id}", status_code=403)
    if status not in (200, 204):
        msg = "Failed to delete page"
        if isinstance(data, dict):
            msg = data.get("message", msg)
        cc.fail(msg, status_code=status, body=str(data))

    return True


def main():
    parser = argparse.ArgumentParser(
        description="Delete (trash) a Confluence page",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Examples:
  python3 delete_page.py --page-id 123456
  python3 delete_page.py --page-id 123456 --dry-run
""")
    parser.add_argument("--page-id", required=True, help="Page ID to delete")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be deleted without deleting")
    args = parser.parse_args()

    cc.init()

    # Always fetch info first so the user sees what they're deleting
    info = get_page_info(args.page_id)

    if args.dry_run:
        cc.ok({
            "dry_run": True,
            "action": "would_delete",
            "page": info,
            "message": f"Would delete page '{info.get('title')}' (id={info.get('id')}) "
                       f"from space {info.get('space_key', '?')}. "
                       "Page would be moved to trash (restorable by admin).",
        })
        return

    delete_page(args.page_id)
    cc.ok({
        "action": "deleted",
        "page": info,
        "message": f"Page '{info.get('title')}' (id={info.get('id')}) "
                   f"moved to trash. It can be restored by a space admin.",
    })


if __name__ == "__main__":
    main()
