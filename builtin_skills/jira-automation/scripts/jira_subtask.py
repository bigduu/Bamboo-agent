#!/usr/bin/env python3
"""
jira_subtask.py — Create one or more subtasks under a parent Jira issue.

Usage:
    python3 jira_subtask.py --parent PLAT-101 --summary "Draft pod summary query"
    python3 jira_subtask.py --parent PLAT-101 --summaries "Design schema|Implement API|Write tests"
    python3 jira_subtask.py --parent PLAT-101 --batch subtasks.json
"""

import argparse
import json
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from jira_common import JiraClient, jira_info, jira_ok, jira_die, print_json, read_file, parse_csv, build_assignee_field


def create_subtask(
    client: JiraClient,
    project: str,
    parent: str,
    summary: str,
    description: str = "",
    assignee: str = "",
    assignee_id: str = "",
    labels: str = "",
    subtask_type: str = "Sub-task",
    dry_run: bool = False,
) -> None:
    """Create a single subtask."""
    fields: dict = {
        "project": {"key": project},
        "parent": {"key": parent},
        "issuetype": {"name": subtask_type},
        "summary": summary,
    }

    if description:
        fields["description"] = description
    assignee_field = build_assignee_field(assignee, assignee_id)
    if assignee_field:
        fields["assignee"] = assignee_field
    if labels:
        fields["labels"] = parse_csv(labels)

    payload = {"fields": fields}

    if dry_run:
        jira_info(f"[DRY-RUN] Would create subtask: {summary}")
        print_json(payload)
        return

    result = client.post("/issue", payload)
    jira_ok(f"Created subtask: {result.get('key', '?')} - {summary}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Create subtasks under a parent Jira issue")
    parser.add_argument("--parent", required=True, help="Parent issue key (e.g. PLAT-101)")
    parser.add_argument("--project", help="Project key (auto-detected from parent if omitted)")
    parser.add_argument("--summary", help="Single subtask summary")
    parser.add_argument("--description", default="", help="Description (for single subtask)")
    parser.add_argument("--summaries", help="Pipe-separated summaries (e.g. 'A|B|C')")
    parser.add_argument("--batch", help="JSON file with subtask definitions [{summary, description}]")
    parser.add_argument("--assignee", help="Assignee username (Server/DC)")
    parser.add_argument("--assignee-id", help="Assignee accountId (Cloud)")
    parser.add_argument("--labels", help="Comma-separated labels")
    parser.add_argument("--subtask-type", default="Sub-task", help="Subtask issue type name")
    parser.add_argument("--dry-run", action="store_true", help="Preview without creating")
    args = parser.parse_args()

    client = JiraClient()
    client.validate()

    # Auto-detect project from parent key
    project = args.project
    if not project:
        project = args.parent.split("-")[0]
        jira_info(f"Auto-detected project: {project} (from {args.parent})")

    common_kwargs = dict(
        client=client,
        project=project,
        parent=args.parent,
        assignee=args.assignee or "",
        assignee_id=args.assignee_id or "",
        labels=args.labels or "",
        subtask_type=args.subtask_type,
        dry_run=args.dry_run,
    )

    # Batch mode from JSON file
    if args.batch:
        items = json.loads(read_file(args.batch))
        jira_info(f"Creating {len(items)} subtasks under {args.parent}...")
        for item in items:
            create_subtask(
                summary=item["summary"],
                description=item.get("description", ""),
                **common_kwargs,
            )

    # Multiple summaries mode
    elif args.summaries:
        items = [s.strip() for s in args.summaries.split("|") if s.strip()]
        jira_info(f"Creating {len(items)} subtask(s) under {args.parent}...")
        for item in items:
            create_subtask(summary=item, **common_kwargs)

    # Single subtask mode
    elif args.summary:
        jira_info(f"Creating 1 subtask under {args.parent}...")
        create_subtask(
            summary=args.summary,
            description=args.description,
            **common_kwargs,
        )
    else:
        jira_die("Provide --summary, --summaries, or --batch.")

    jira_ok(f"All subtasks created under {args.parent}")


if __name__ == "__main__":
    main()
