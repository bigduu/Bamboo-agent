#!/usr/bin/env python3
"""
jira_search.py — Search Jira issues using JQL.

Usage:
    python3 jira_search.py --jql "project = PROJ AND status = 'In Progress'"
    python3 jira_search.py --jql "assignee = currentUser()" --max 20
    python3 jira_search.py --jql "labels = pod-alpha" --count-only
"""

import argparse
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from jira_common import JiraClient, jira_info, jira_ok, print_json

DEFAULT_FIELDS = "key,summary,status,priority,assignee,updated"


def main() -> None:
    parser = argparse.ArgumentParser(description="Search Jira issues with JQL")
    parser.add_argument("--jql", required=True, help="JQL query string")
    parser.add_argument("--fields", default=DEFAULT_FIELDS, help="Comma-separated fields")
    parser.add_argument("--max", type=int, default=50, help="Max results (default: 50)")
    parser.add_argument("--start", type=int, default=0, help="Start at offset")
    parser.add_argument("--count-only", action="store_true", help="Only print total count")
    parser.add_argument("--raw", action="store_true", help="Output compact JSON")
    args = parser.parse_args()

    client = JiraClient()
    client.validate()

    body = {
        "jql": args.jql,
        "fields": [f.strip() for f in args.fields.split(",")],
        "maxResults": args.max,
        "startAt": args.start,
    }

    jira_info(f"Searching: {args.jql} (max={args.max}, start={args.start})")
    result = client.post("/search", body)

    total = result.get("total", 0)

    if args.count_only:
        print(total)
        jira_ok(f"Total matching issues: {total}")
        return

    print_json(result, raw=args.raw)
    jira_ok(f"Returned up to {result.get('maxResults', 0)} of {total} total matches")


if __name__ == "__main__":
    main()
