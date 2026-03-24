#!/usr/bin/env python3
"""
jira_create.py — Create a new Jira issue.

Usage:
    python3 jira_create.py --project PLAT --type Story --summary "Automate pod summary"
    python3 jira_create.py --project OPS --type Bug --summary "Login fails" --priority High --labels bug,safari
    python3 jira_create.py --json '{"fields":{"project":{"key":"PROJ"},...}}'
    python3 jira_create.py --project PLAT --type Story --summary "Test" --dry-run
"""

import argparse
import json
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from jira_common import JiraClient, jira_info, jira_ok, jira_die, print_json, read_file, parse_csv, build_assignee_field


def main() -> None:
    parser = argparse.ArgumentParser(description="Create a new Jira issue")
    parser.add_argument("--project", help="Project key (e.g. PLAT)")
    parser.add_argument("--type", dest="issue_type", help="Issue type (Story, Bug, Task, Epic)")
    parser.add_argument("--summary", help="Issue summary/title")
    parser.add_argument("--description", default="", help="Issue description")
    parser.add_argument("--description-file", help="Read description from file")
    parser.add_argument("--assignee", help="Assignee username (Server/DC)")
    parser.add_argument("--assignee-id", help="Assignee accountId (Cloud)")
    parser.add_argument("--priority", help="Priority name (e.g. High, Medium)")
    parser.add_argument("--labels", help="Comma-separated labels")
    parser.add_argument("--components", help="Comma-separated component names")
    parser.add_argument("--parent", help="Parent issue key (for subtasks)")
    parser.add_argument("--json", dest="json_payload", help="Raw JSON payload (overrides all other fields)")
    parser.add_argument("--dry-run", action="store_true", help="Preview payload without creating")
    args = parser.parse_args()

    client = JiraClient()
    client.validate()

    # Read description from file
    description = args.description
    if args.description_file:
        description = read_file(args.description_file)

    # Build payload
    if args.json_payload:
        payload = json.loads(args.json_payload)
    else:
        if not args.project:
            args.project = client.project_key
        if not args.project:
            jira_die("--project is required.")
        if not args.issue_type:
            jira_die("--type is required.")
        if not args.summary:
            jira_die("--summary is required.")

        fields: dict = {
            "project": {"key": args.project},
            "issuetype": {"name": args.issue_type},
            "summary": args.summary,
        }

        if description:
            fields["description"] = description
        assignee_field = build_assignee_field(args.assignee or "", args.assignee_id or "")
        if assignee_field:
            fields["assignee"] = assignee_field
        if args.priority:
            fields["priority"] = {"name": args.priority}
        if args.labels:
            fields["labels"] = parse_csv(args.labels)
        if args.components:
            fields["components"] = [{"name": c} for c in parse_csv(args.components)]
        if args.parent:
            fields["parent"] = {"key": args.parent}

        payload = {"fields": fields}

    # Execute or dry-run
    if args.dry_run:
        jira_info("[DRY-RUN] Would create issue with payload:")
        print_json(payload)
        return

    project = args.project or payload.get("fields", {}).get("project", {}).get("key", "?")
    jira_info(f"Creating issue in project {project}...")
    result = client.post("/issue", payload)

    key = result.get("key", "?")
    jira_ok(f"Created: {key} (id={result.get('id', '?')})")
    jira_info(f"URL: {client.base_url}/browse/{key}")
    print_json(result)


if __name__ == "__main__":
    main()
