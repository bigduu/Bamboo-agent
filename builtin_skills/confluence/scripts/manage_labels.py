#!/usr/bin/env python3
"""Get, add, or remove labels on a Confluence page.

Usage:
    python3 manage_labels.py --page-id 123456 --action get
    python3 manage_labels.py --page-id 123456 --action add --labels release-notes,payments
    python3 manage_labels.py --page-id 123456 --action add --labels ops incident-review
    python3 manage_labels.py --page-id 123456 --action remove --labels outdated,draft

Output: JSON with current labels after the operation.
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import confluence_client as cc


def get_labels(page_id):
    """Fetch current labels for a page."""
    status, data = cc.get(f"/rest/api/content/{page_id}/label")
    if status == 404:
        cc.fail(f"Page {page_id} not found", status_code=404)
    if status != 200:
        cc.fail(f"Failed to get labels (HTTP {status})",
                status_code=status, body=str(data))

    results = data.get("results", [])
    return [{"name": lb.get("name"), "prefix": lb.get("prefix", "global")} for lb in results]


def add_labels(page_id, label_names):
    """Add labels to a page. Returns the full label list after adding."""
    payload = [{"prefix": "global", "name": name.strip()} for name in label_names if name.strip()]

    if not payload:
        cc.fail("No valid label names provided")

    status, data = cc.post(f"/rest/api/content/{page_id}/label", json_body=payload)
    if status == 404:
        cc.fail(f"Page {page_id} not found", status_code=404)
    if status not in (200, 201):
        msg = "Failed to add labels"
        if isinstance(data, dict):
            msg = data.get("message", msg)
        cc.fail(msg, status_code=status, body=str(data))

    # Re-fetch to get the full label list
    return get_labels(page_id)


def remove_labels(page_id, label_names):
    """Remove labels from a page. Returns (labels_after, removed, failed).

    NOTE: Confluence REST API only supports removing labels one at a time
    (DELETE /rest/api/content/{id}/label/{label}). There is no bulk-remove
    endpoint, so N labels = N DELETE calls. This is an API limitation, not
    a design choice. Typical usage is 1-5 labels so the impact is minimal.
    """
    removed = []
    failed = []

    for name in label_names:
        name = name.strip()
        if not name:
            continue
        # Confluence REST API only supports per-label DELETE — no bulk endpoint
        status, data = cc.delete(f"/rest/api/content/{page_id}/label/{name}")
        if status == 404:
            # Label didn't exist — not an error, just skip
            failed.append({"name": name, "reason": "not found"})
        elif status not in (200, 204):
            failed.append({"name": name, "reason": f"HTTP {status}"})
        else:
            removed.append(name)

    labels_after = get_labels(page_id)
    return labels_after, removed, failed


def main():
    parser = argparse.ArgumentParser(description="Manage Confluence page labels")
    parser.add_argument("--page-id", required=True, help="Page ID")
    parser.add_argument("--action", choices=["get", "add", "remove"], required=True,
                        help="Action: get, add, or remove labels")
    parser.add_argument("--labels", nargs="+", default=[],
                        help="Label names (space or comma separated)")
    args = parser.parse_args()

    cc.init()

    # Flatten comma-separated labels
    flat_labels = []
    for lb in args.labels:
        flat_labels.extend(lb.split(","))

    if args.action == "get":
        labels = get_labels(args.page_id)
        cc.ok({"page_id": args.page_id, "labels": labels})
    elif args.action == "add":
        if not flat_labels:
            cc.fail("Provide --labels when using --action add")
        labels = add_labels(args.page_id, flat_labels)
        cc.ok({"page_id": args.page_id, "labels": labels, "added": flat_labels})
    elif args.action == "remove":
        if not flat_labels:
            cc.fail("Provide --labels when using --action remove")
        labels_after, removed, failed = remove_labels(args.page_id, flat_labels)
        result = {"page_id": args.page_id, "labels": labels_after, "removed": removed}
        if failed:
            result["failed"] = failed
        cc.ok(result)


if __name__ == "__main__":
    main()
