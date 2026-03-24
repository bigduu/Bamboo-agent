#!/usr/bin/env python3
"""
jira_transition.py — Transition a Jira issue's workflow status.

Usage:
    python3 jira_transition.py OPS-132 --list
    python3 jira_transition.py OPS-132 --to "In Progress"
    python3 jira_transition.py OPS-132 --id 31 --comment "Starting work"
"""

import argparse
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from jira_common import JiraClient, jira_info, jira_ok, jira_die, jira_error, print_json


def main() -> None:
    parser = argparse.ArgumentParser(description="Transition a Jira issue's workflow status")
    parser.add_argument("issue_key", help="Issue key (e.g. OPS-132)")
    parser.add_argument("--list", action="store_true", dest="list_transitions", help="List available transitions")
    parser.add_argument("--to", dest="to_name", help="Transition name or target status name (case-insensitive)")
    parser.add_argument("--id", dest="transition_id", help="Transition ID")
    parser.add_argument("--comment", help="Comment to add with the transition")
    parser.add_argument("--dry-run", action="store_true", help="Preview without executing")
    args = parser.parse_args()

    client = JiraClient()
    client.validate()

    # Fetch available transitions
    jira_info(f"Fetching transitions for {args.issue_key}...")
    trans_result = client.get(f"/issue/{args.issue_key}/transitions")
    transitions = trans_result.get("transitions", [])

    # List mode
    if args.list_transitions:
        if not transitions:
            print("No transitions available.")
            return

        print(f"{'ID':<8} {'Name':<30} {'To Status':<25}")
        print("-" * 63)
        for t in transitions:
            to_status = (t.get("to") or {}).get("name", "?")
            print(f"{t['id']:<8} {t['name']:<30} {to_status:<25}")
        return

    # Resolve transition ID from name
    transition_id = args.transition_id
    if args.to_name:
        target_lower = args.to_name.lower()
        matches = [
            t for t in transitions
            if t["name"].lower() == target_lower
            or (t.get("to") or {}).get("name", "").lower() == target_lower
        ]

        if not matches:
            available = ", ".join(t["name"] for t in transitions)
            jira_die(f"No transition matching: {args.to_name}. Available: {available}")
        elif len(matches) > 1:
            jira_error("Multiple transitions match. Use --id instead:")
            for m in matches:
                to_name = (m.get("to") or {}).get("name", "?")
                jira_error(f"  id={m['id']} name={m['name']} -> {to_name}")
            jira_die("Ambiguous transition name.")

        transition_id = matches[0]["id"]

    if not transition_id:
        jira_die("Either --to or --id is required.")

    # Build payload
    data: dict = {"transition": {"id": transition_id}}

    if args.comment:
        data["update"] = {
            "comment": [{"add": {"body": args.comment}}]
        }

    # Execute or dry-run
    if args.dry_run:
        jira_info(f"[DRY-RUN] Would transition {args.issue_key} with:")
        print_json(data)
        return

    jira_info(f"Transitioning {args.issue_key} (transition id={transition_id})...")
    client.post(f"/issue/{args.issue_key}/transitions", data)

    jira_ok(f"Transitioned {args.issue_key} successfully")
    jira_info(f"URL: {client.base_url}/browse/{args.issue_key}")


if __name__ == "__main__":
    main()
