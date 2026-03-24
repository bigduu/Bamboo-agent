#!/usr/bin/env python3
"""
jira_update.py — Update an existing Jira issue's fields.

Usage:
    python3 jira_update.py OPS-132 --summary "Clarified scope" --labels ops,clarified
    python3 jira_update.py PROJ-45 --add-labels reviewed --priority High
    python3 jira_update.py PROJ-45 --read-first --summary "Updated after review"
    python3 jira_update.py PROJ-45 --dry-run --description "New description..."
"""

import argparse
import json
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from jira_common import JiraClient, jira_info, jira_ok, jira_die, print_json, read_file, parse_csv, build_assignee_field


def main() -> None:
    parser = argparse.ArgumentParser(description="Update an existing Jira issue")
    parser.add_argument("issue_key", help="Issue key (e.g. OPS-132)")
    parser.add_argument("--summary", help="New summary/title")
    parser.add_argument("--description", help="New description")
    parser.add_argument("--description-file", help="Read description from file")
    parser.add_argument("--assignee", help="Assignee username (Server/DC)")
    parser.add_argument("--assignee-id", help="Assignee accountId (Cloud)")
    parser.add_argument("--priority", help="Priority name")
    parser.add_argument("--labels", help="Comma-separated labels (replaces all)")
    parser.add_argument("--add-labels", help="Comma-separated labels to add (merges with existing)")
    parser.add_argument("--components", help="Comma-separated component names")
    parser.add_argument("--due-date", help="Due date (YYYY-MM-DD)")
    parser.add_argument("--story-points", type=float, help="Story points value")
    parser.add_argument("--sp-field", default="customfield_10016", help="Story points field name")
    parser.add_argument("--json", dest="json_payload", help="Raw JSON payload")
    parser.add_argument("--dry-run", action="store_true", help="Preview payload without updating")
    parser.add_argument("--read-first", action="store_true", help="Read current state before updating")
    args = parser.parse_args()

    client = JiraClient()
    client.validate()

    # Read description from file
    description = args.description
    if args.description_file:
        description = read_file(args.description_file)

    # Read current state if requested
    if args.read_first:
        jira_info(f"Reading current state of {args.issue_key}...")
        current = client.get(
            f"/issue/{args.issue_key}",
            query={"fields": "summary,description,status,labels,priority,assignee,components,duedate"},
        )
        jira_info("Current issue state:")
        print_json(current)

    # Handle add-labels (merge with existing)
    labels = args.labels
    if args.add_labels:
        jira_info("Reading current labels for merge...")
        current_issue = client.get(f"/issue/{args.issue_key}", query={"fields": "labels"})
        existing = current_issue.get("fields", {}).get("labels", [])
        new_labels = parse_csv(args.add_labels)
        merged = sorted(set(existing + new_labels))
        labels = ",".join(merged)

    # Build payload
    if args.json_payload:
        payload = json.loads(args.json_payload)
    else:
        fields: dict = {}

        if args.summary:
            fields["summary"] = args.summary
        if description:
            fields["description"] = description
        assignee_field = build_assignee_field(args.assignee or "", args.assignee_id or "")
        if assignee_field:
            fields["assignee"] = assignee_field
        if args.priority:
            fields["priority"] = {"name": args.priority}
        if args.due_date:
            fields["duedate"] = args.due_date
        if labels:
            fields["labels"] = parse_csv(labels)
        if args.components:
            fields["components"] = [{"name": c} for c in parse_csv(args.components)]
        if args.story_points is not None:
            fields[args.sp_field] = args.story_points

        if not fields:
            jira_die("No fields to update.")

        payload = {"fields": fields}

    # Execute or dry-run
    if args.dry_run:
        jira_info(f"[DRY-RUN] Would update {args.issue_key} with:")
        print_json(payload)
        return

    jira_info(f"Updating issue: {args.issue_key}")
    client.put(f"/issue/{args.issue_key}", payload)

    jira_ok(f"Updated: {args.issue_key}")
    jira_info(f"URL: {client.base_url}/browse/{args.issue_key}")


if __name__ == "__main__":
    main()
