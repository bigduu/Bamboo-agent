#!/usr/bin/env python3
"""
jira_comment.py — Add or list comments on a Jira issue.

Usage:
    python3 jira_comment.py OPS-132 --body "Scope clarified. Moving forward."
    python3 jira_comment.py OPS-132 --body-file update.md
    python3 jira_comment.py OPS-132 --list
    python3 jira_comment.py OPS-132 --list --last 5
"""

import argparse
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from jira_common import JiraClient, jira_info, jira_ok, jira_die, print_json, read_file


def main() -> None:
    parser = argparse.ArgumentParser(description="Add or list comments on a Jira issue")
    parser.add_argument("issue_key", help="Issue key (e.g. OPS-132)")
    parser.add_argument("--body", help="Comment body text")
    parser.add_argument("--body-file", help="Read comment body from file")
    parser.add_argument("--list", action="store_true", dest="list_comments", help="List comments")
    parser.add_argument("--last", type=int, default=0, help="Show only last N comments (with --list)")
    parser.add_argument("--dry-run", action="store_true", help="Preview without posting")
    args = parser.parse_args()

    client = JiraClient()
    client.validate()

    # Read body from file
    body = args.body
    if args.body_file:
        body = read_file(args.body_file)

    # List mode
    if args.list_comments:
        jira_info(f"Listing comments for {args.issue_key}...")
        result = client.get(f"/issue/{args.issue_key}/comment")

        comments = result.get("comments", [])
        if args.last > 0 and len(comments) > args.last:
            comments = comments[-args.last:]

        if not comments:
            print("No comments found.")
            return

        for c in comments:
            author = (c.get("author") or {}).get("displayName", "Unknown")
            created = c.get("created", "")
            cbody = c.get("body", "")
            print(f"--- {author} ({created}) ---")
            print(cbody)
            print()
        return

    # Add comment mode
    if not body:
        jira_die("--body or --body-file is required when adding a comment.")

    payload = {"body": body}

    if args.dry_run:
        jira_info(f"[DRY-RUN] Would add comment to {args.issue_key}:")
        print_json(payload)
        return

    jira_info(f"Adding comment to {args.issue_key}...")
    result = client.post(f"/issue/{args.issue_key}/comment", payload)

    jira_ok(f"Comment added to {args.issue_key} (comment id={result.get('id', '?')})")


if __name__ == "__main__":
    main()
