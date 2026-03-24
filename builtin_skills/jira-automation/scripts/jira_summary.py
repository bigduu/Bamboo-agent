#!/usr/bin/env python3
"""
jira_summary.py — Query Jira issues for summary/reporting use cases.

Builds and executes JQL queries optimized for personal, pod, or team summaries.

Usage:
    python3 jira_summary.py --scope personal --time today
    python3 jira_summary.py --scope pod --project PLAT --label pod-alpha --time week
    python3 jira_summary.py --scope team --project MOBILE --time sprint --format table
"""

import argparse
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from jira_common import (
    JiraClient, jira_info, jira_ok, jira_warn, print_json,
    build_scope_jql, build_sprint_jql, build_time_jql,
)

DEFAULT_FIELDS = "key,summary,status,priority,assignee,updated,created,issuetype,labels,components"


def main() -> None:
    parser = argparse.ArgumentParser(description="Jira summary query helper")
    parser.add_argument("--scope", required=True, choices=["personal", "pod", "team"], help="Summary scope")
    parser.add_argument("--assignee", help="Filter by assignee (personal scope; defaults to currentUser())")
    parser.add_argument("--project", default=os.environ.get("JIRA_PROJECT_KEY", ""), help="Project key")
    parser.add_argument("--label", help="Filter by label")
    parser.add_argument("--component", help="Filter by component")
    parser.add_argument("--sprint", help="Sprint name/id (use 'open' for openSprints())")
    parser.add_argument("--time", choices=["today", "yesterday", "week", "month", "sprint", "custom"], help="Time period")
    parser.add_argument("--since", help="Start date (YYYY-MM-DD) for --time=custom")
    parser.add_argument("--until", help="End date (YYYY-MM-DD) for --time=custom")
    parser.add_argument("--fields", default=DEFAULT_FIELDS, help="Comma-separated fields")
    parser.add_argument("--max", type=int, default=100, help="Max results")
    parser.add_argument("--raw", action="store_true", help="Output compact JSON")
    parser.add_argument("--format", dest="output_format", default="json", choices=["table", "json"], help="Output format (default: json)")
    args = parser.parse_args()

    client = JiraClient()
    client.validate()

    # Build JQL using shared helpers
    jql_parts = build_scope_jql(
        scope=args.scope, project=args.project,
        assignee=args.assignee or "", label=args.label or "",
        component=args.component or "",
    )

    # Pod-scope warning for broad queries
    if args.scope == "pod" and not args.label and not args.component and not args.assignee:
        jira_warn("Pod scope without --label or --component may return broad results.")

    jql_parts += build_sprint_jql(args.sprint or "")
    jql_parts += build_time_jql(
        time=args.time or "", since=args.since or "",
        until=args.until or "", sprint=args.sprint or "",
    )

    # Guard against empty JQL (would produce invalid query)
    if not jql_parts:
        jira_warn("No filters specified — query would be unbounded. Add --project, --label, --time, or other filters.")
        jql_parts.append("updated >= startOfWeek()")
        jira_info("Defaulting to: updated >= startOfWeek()")

    # Assemble JQL
    jql = " AND ".join(jql_parts) + " ORDER BY priority DESC, updated DESC"

    jira_info(f"Scope: {args.scope} | Time: {args.time or 'any'}")
    jira_info(f"JQL: {jql}")

    # Execute search with pagination
    fields_list = [f.strip() for f in args.fields.split(",")]
    page_size = min(args.max, 100)
    all_issues: list[dict] = []
    start_at = 0

    while True:
        body = {
            "jql": jql,
            "fields": fields_list,
            "maxResults": page_size,
            "startAt": start_at,
        }
        result = client.post("/search", body)
        batch = result.get("issues", [])
        total = result.get("total", 0)
        all_issues.extend(batch)

        if start_at == 0:
            jira_info(f"Found {total} issues (fetching up to {args.max})")

        start_at += len(batch)
        if not batch or start_at >= total or len(all_issues) >= args.max:
            break

    # Trim to requested max
    all_issues = all_issues[: args.max]

    # Output
    if args.raw:
        print_json({"total": total, "issues": all_issues}, raw=True)
        return

    if args.output_format == "table":
        if not all_issues:
            print("No issues found.")
            return

        print(f"{'Key':<14} {'Type':<12} {'Status':<18} {'Priority':<10} {'Assignee':<20} {'Summary'}")
        print("-" * 110)

        for issue in all_issues:
            f = issue.get("fields", {})
            itype = (f.get("issuetype") or {}).get("name", "?")
            status = (f.get("status") or {}).get("name", "?")
            priority = (f.get("priority") or {}).get("name", "?")
            assignee = (f.get("assignee") or {}).get("displayName", "Unassigned")
            summary = f.get("summary", "")[:60]
            print(f"{issue['key']:<14} {itype:<12} {status:<18} {priority:<10} {assignee:<20} {summary}")

        print()
        print(f"Total: {len(all_issues)} issues (of {total} matching)")
    else:
        print_json({"total": total, "issues": all_issues})

    jira_ok(f"Summary query complete ({args.scope} scope, {len(all_issues)} of {total} issues)")


if __name__ == "__main__":
    main()
