#!/usr/bin/env python3
"""
jira_get.py — Fetch a single Jira issue by key.

Usage:
    python3 jira_get.py PROJ-123
    python3 jira_get.py PROJ-123 --fields summary,status
    python3 jira_get.py PROJ-123 --expand changelog --raw
"""

import argparse
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from jira_common import JiraClient, jira_info, jira_ok, print_json

DEFAULT_FIELDS = "summary,description,status,assignee,reporter,priority,labels,components,issuetype,created,updated,duedate,parent,subtasks,comment"


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch a single Jira issue")
    parser.add_argument("issue_key", help="Issue key (e.g. PROJ-123)")
    parser.add_argument("--fields", default=DEFAULT_FIELDS, help="Comma-separated fields")
    parser.add_argument("--expand", default="", help="Expand sections (e.g. changelog, renderedFields)")
    parser.add_argument("--raw", action="store_true", help="Output compact JSON")
    args = parser.parse_args()

    client = JiraClient()
    client.validate()

    query = {"fields": args.fields}
    if args.expand:
        query["expand"] = args.expand

    jira_info(f"Fetching issue: {args.issue_key}")
    result = client.get(f"/issue/{args.issue_key}", query=query)

    print_json(result, raw=args.raw)
    jira_ok(f"Done: {args.issue_key}")


if __name__ == "__main__":
    main()
