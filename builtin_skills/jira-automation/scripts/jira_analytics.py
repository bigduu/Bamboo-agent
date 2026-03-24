#!/usr/bin/env python3
"""
jira_analytics.py — Batch query Jira issues and generate analytics output.

Outputs Mermaid diagrams (for AI rendering), ASCII text charts, Markdown
tables, CSV, or JSON. No third-party packages — Python 3 stdlib only.

The primary output format is **mermaid** — the AI embeds the generated
Mermaid syntax directly in its response for inline rendering.

Usage:
    # Personal report: my tickets over last 30 days → Mermaid pie + bar
    python3 jira_analytics.py --scope personal --time month

    # Team status breakdown → Mermaid pie chart
    python3 jira_analytics.py --scope team --project PLAT --time week --group-by status --chart pie

    # By assignee → Mermaid bar chart
    python3 jira_analytics.py --scope team --project PLAT --group-by assignee --chart bar

    # Markdown table (for AI to paste directly)
    python3 jira_analytics.py --scope pod --project OPS --label pod-alpha --format markdown

    # CSV for external tools
    python3 jira_analytics.py --scope team --project PLAT --group-by assignee --format csv -o report.csv

    # ASCII text chart for quick terminal review
    python3 jira_analytics.py --scope team --project PLAT --time month --format text
"""

import argparse
import csv
import io
import json
import os
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from jira_common import (
    JiraClient, jira_info, jira_ok, jira_warn, jira_die, pretty_json,
    build_scope_jql, build_sprint_jql, build_time_jql,
)

# ═══════════════════════════════════════════════════════════════════════════
# Data query layer
# ═══════════════════════════════════════════════════════════════════════════

FIELDS_FOR_ANALYTICS = (
    "key,summary,status,priority,assignee,reporter,issuetype,"
    "created,updated,resolutiondate,duedate,labels,components"
)

# Extra time windows only used by analytics (quarter, Nd)
_ANALYTICS_TIME_EXTRA = {
    "quarter": "updated >= startOfMonth(-2)",
}


def build_jql(args) -> str:
    """Build JQL from CLI args using shared helpers."""
    parts = build_scope_jql(
        scope=args.scope, project=args.project,
        assignee=args.assignee or "", label=args.label or "",
        component=args.component or "",
    )
    parts += build_sprint_jql(args.sprint or "")
    parts += build_time_jql(
        time=args.time or "", since=args.since or "",
        until=args.until or "", sprint=args.sprint or "",
        extra_map=_ANALYTICS_TIME_EXTRA,
    )

    # Handle Nd shorthand (e.g. "30d") — analytics-specific
    if args.time and args.time.endswith("d") and args.time[:-1].isdigit():
        days = int(args.time[:-1])
        parts.append(f"updated >= -{days}d")

    if args.jql_extra:
        parts.append(args.jql_extra)

    # Guard against empty JQL (would produce invalid query)
    if not parts:
        jira_warn("No filters specified — query would be unbounded. Add --project, --label, --time, or other filters.")
        parts.append("updated >= startOfWeek()")
        jira_info("Defaulting to: updated >= startOfWeek()")

    return " AND ".join(parts) + " ORDER BY updated DESC"


def fetch_all_issues(client: JiraClient, jql: str, max_total: int = 1000) -> list[dict]:
    """Paginate through all matching issues."""
    all_issues: list[dict] = []
    start = 0
    page_size = min(100, max_total)

    while start < max_total:
        body = {
            "jql": jql,
            "fields": [f.strip() for f in FIELDS_FOR_ANALYTICS.split(",")],
            "maxResults": page_size,
            "startAt": start,
        }
        result = client.post("/search", body)
        issues = result.get("issues", [])
        total = result.get("total", 0)
        all_issues.extend(issues)
        jira_info(f"  Fetched {len(all_issues)}/{total} issues...")
        if len(all_issues) >= total or not issues:
            break
        start += len(issues)
    return all_issues


# ═══════════════════════════════════════════════════════════════════════════
# Field extraction & aggregation
# ═══════════════════════════════════════════════════════════════════════════

def extract_field(issue: dict, field: str) -> str:
    """Extract a display-friendly value from an issue."""
    f = issue.get("fields", {})
    if field == "status":
        return (f.get("status") or {}).get("name", "Unknown")
    elif field == "assignee":
        return (f.get("assignee") or {}).get("displayName", "Unassigned")
    elif field == "reporter":
        return (f.get("reporter") or {}).get("displayName", "Unknown")
    elif field == "priority":
        return (f.get("priority") or {}).get("name", "None")
    elif field == "issuetype":
        return (f.get("issuetype") or {}).get("name", "Unknown")
    elif field == "created_week":
        created = f.get("created", "")
        if created:
            dt = datetime.fromisoformat(created.replace("Z", "+00:00"))
            return dt.strftime("%Y-W%W")
        return "Unknown"
    elif field == "created_month":
        created = f.get("created", "")
        if created:
            dt = datetime.fromisoformat(created.replace("Z", "+00:00"))
            return dt.strftime("%Y-%m")
        return "Unknown"
    elif field == "updated_week":
        updated = f.get("updated", "")
        if updated:
            dt = datetime.fromisoformat(updated.replace("Z", "+00:00"))
            return dt.strftime("%Y-W%W")
        return "Unknown"
    elif field == "labels":
        return ", ".join(f.get("labels", [])) or "None"
    elif field == "component":
        comps = f.get("components", [])
        return ", ".join(c.get("name", "") for c in comps) or "None"
    return str(f.get(field, "Unknown"))


def aggregate(issues: list[dict], group_by: str) -> dict[str, int]:
    """Count issues grouped by a field."""
    counter: Counter = Counter()
    for issue in issues:
        val = extract_field(issue, group_by)
        counter[val] += 1
    return dict(counter.most_common())


def multi_aggregate(issues: list[dict], group_by: str, split_by: str) -> dict[str, dict[str, int]]:
    """Group by one field, then split counts by another."""
    data: dict[str, Counter] = defaultdict(Counter)
    for issue in issues:
        g = extract_field(issue, group_by)
        s = extract_field(issue, split_by)
        data[g][s] += 1
    return {k: dict(v) for k, v in data.items()}


# ═══════════════════════════════════════════════════════════════════════════
# Mermaid output helpers
# ═══════════════════════════════════════════════════════════════════════════

def _safe_label(text: str) -> str:
    """Sanitize label for Mermaid (remove quotes, brackets, special chars)."""
    text = re.sub(r'["\[\]{}#;]', '', text)
    return text.strip() or "Other"


def render_mermaid_pie(title: str, data: dict[str, int]) -> str:
    """Render a Mermaid pie chart."""
    lines = [f'pie title {title}']
    for label, count in data.items():
        safe = _safe_label(label)
        lines.append(f'    "{safe}" : {count}')
    return "\n".join(lines)


def render_mermaid_bar(title: str, data: dict[str, int]) -> str:
    """Render a Mermaid xychart-beta bar chart."""
    labels = [f'"{_safe_label(k)}"' for k in data.keys()]
    values = list(data.values())

    lines = [
        "xychart-beta",
        f'    title "{title}"',
        f'    x-axis [{", ".join(labels)}]',
        f'    y-axis "Count" 0 --> {max(values) + 1}',
        f'    bar [{", ".join(str(v) for v in values)}]',
    ]
    return "\n".join(lines)


def render_mermaid_quadrant(title: str, issues: list[dict]) -> str:
    """Render a quadrant chart (priority × status) for team overview."""
    # Count issues per priority-status combo
    status_order = {"To Do": 0.2, "In Progress": 0.5, "In Review": 0.7, "Done": 0.9}
    priority_order = {"Critical": 0.9, "Highest": 0.85, "High": 0.7, "Medium": 0.5, "Low": 0.3, "Lowest": 0.15}

    lines = [
        "quadrantChart",
        f'    title {title}',
        '    x-axis "Low Priority" --> "High Priority"',
        '    y-axis "Not Started" --> "Completed"',
    ]

    seen = set()
    for issue in issues[:30]:  # cap to avoid overflow
        key = issue.get("key", "?")
        if key in seen:
            continue
        seen.add(key)
        status = extract_field(issue, "status")
        priority = extract_field(issue, "priority")
        x = priority_order.get(priority, 0.5)
        y = status_order.get(status, 0.4)
        lines.append(f'    {_safe_label(key)}: [{x:.2f}, {y:.2f}]')

    return "\n".join(lines)


def render_mermaid_full(
    scope: str,
    group_by: str,
    primary_data: dict[str, int],
    issues: list[dict],
    jql: str,
    total: int,
    chart_type: str = "auto",
    split_by: str = "",
) -> str:
    """Generate a complete Mermaid-ready analytics report.

    Returns Markdown text containing one or more ```mermaid blocks
    plus a summary statistics table and issue listing.
    """
    sections: list[str] = []

    # Header
    sections.append(f"## 📊 Jira Analytics — {scope}")
    sections.append(f"**JQL:** `{jql}`")
    sections.append(f"**Total issues:** {total}  |  **Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    sections.append("")

    # Summary stats table
    assignees = len(set(extract_field(i, "assignee") for i in issues))
    statuses = len(set(extract_field(i, "status") for i in issues))
    types = len(set(extract_field(i, "issuetype") for i in issues))

    sections.append("### Summary")
    sections.append(f"| Metric | Value |")
    sections.append(f"|--------|-------|")
    sections.append(f"| Total Issues | {total} |")
    sections.append(f"| Unique Assignees | {assignees} |")
    sections.append(f"| Status Categories | {statuses} |")
    sections.append(f"| Issue Types | {types} |")
    sections.append("")

    # Primary chart
    resolved_chart = chart_type
    if resolved_chart == "auto":
        if len(primary_data) <= 6:
            resolved_chart = "pie"
        else:
            resolved_chart = "bar"

    sections.append(f"### Issues by {group_by}")
    sections.append("")
    sections.append("```mermaid")
    if resolved_chart == "pie":
        sections.append(render_mermaid_pie(f"Issues by {group_by}", primary_data))
    else:
        sections.append(render_mermaid_bar(f"Issues by {group_by}", primary_data))
    sections.append("```")
    sections.append("")

    # Auto-generate secondary charts
    auto_fields = {"status", "assignee", "priority", "issuetype"} - {group_by}
    for field in sorted(auto_fields):
        extra_data = aggregate(issues, field)
        if len(extra_data) > 1:
            sections.append(f"### Issues by {field}")
            sections.append("")
            sections.append("```mermaid")
            if len(extra_data) <= 6:
                sections.append(render_mermaid_pie(f"Issues by {field}", extra_data))
            else:
                sections.append(render_mermaid_bar(f"Issues by {field}", extra_data))
            sections.append("```")
            sections.append("")

    # Split-by breakdown (if requested)
    if split_by:
        split_data = multi_aggregate(issues, group_by, split_by)
        sections.append(f"### Breakdown: {group_by} × {split_by}")
        sections.append("")
        for group, breakdown in split_data.items():
            sections.append(f"**{group}:**")
            for cat, count in breakdown.items():
                sections.append(f"  - {cat}: {count}")
        sections.append("")

    # Issue details table (top 50)
    top_n = min(len(issues), 50)
    sections.append(f"### Issue Details (top {top_n})")
    sections.append("")
    sections.append("| Key | Type | Status | Priority | Assignee | Summary |")
    sections.append("|-----|------|--------|----------|----------|---------|")
    for issue in issues[:top_n]:
        f = issue.get("fields", {})
        key = issue.get("key", "?")
        itype = (f.get("issuetype") or {}).get("name", "?")
        status = (f.get("status") or {}).get("name", "?")
        priority = (f.get("priority") or {}).get("name", "?")
        assignee = (f.get("assignee") or {}).get("displayName", "Unassigned")
        summary = f.get("summary", "")[:60]
        sections.append(f"| {key} | {itype} | {status} | {priority} | {assignee} | {summary} |")
    sections.append("")

    return "\n".join(sections)


# ═══════════════════════════════════════════════════════════════════════════
# ASCII text output
# ═══════════════════════════════════════════════════════════════════════════

def render_text_chart(title: str, data: dict[str, int], max_bar: int = 40) -> str:
    """Render an ASCII horizontal bar chart."""
    if not data:
        return f"{title}\n  (no data)\n"
    lines = [f"\n{'=' * 60}", title, "=" * 60]
    max_val = max(data.values()) if data else 1
    max_label = max(len(k) for k in data) if data else 5
    for label, count in data.items():
        bar_len = int((count / max_val) * max_bar) if max_val else 0
        bar = "█" * bar_len
        lines.append(f"  {label:<{max_label}}  {bar} {count}")
    lines.append(f"\n  Total: {sum(data.values())} issues\n")
    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════
# CSV output
# ═══════════════════════════════════════════════════════════════════════════

def render_csv(data: dict[str, int]) -> str:
    """Render data as CSV."""
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["Category", "Count"])
    for k, v in data.items():
        writer.writerow([k, v])
    return buf.getvalue()


# ═══════════════════════════════════════════════════════════════════════════
# Markdown table output
# ═══════════════════════════════════════════════════════════════════════════

def render_markdown_table(title: str, data: dict[str, int], issues: list[dict]) -> str:
    """Render a Markdown summary table."""
    lines = [f"## {title}", ""]
    lines.append("| Category | Count |")
    lines.append("|----------|-------|")
    for k, v in data.items():
        lines.append(f"| {k} | {v} |")
    lines.append(f"| **Total** | **{sum(data.values())}** |")
    lines.append("")

    # Issue list
    top_n = min(len(issues), 50)
    lines.append(f"### Issue Details (top {top_n})")
    lines.append("")
    lines.append("| Key | Type | Status | Priority | Assignee | Summary |")
    lines.append("|-----|------|--------|----------|----------|---------|")
    for issue in issues[:top_n]:
        f = issue.get("fields", {})
        key = issue.get("key", "?")
        itype = (f.get("issuetype") or {}).get("name", "?")
        status = (f.get("status") or {}).get("name", "?")
        priority = (f.get("priority") or {}).get("name", "?")
        assignee = (f.get("assignee") or {}).get("displayName", "Unassigned")
        summary = f.get("summary", "")[:60]
        lines.append(f"| {key} | {itype} | {status} | {priority} | {assignee} | {summary} |")

    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Batch query Jira issues and generate analytics with Mermaid diagrams"
    )

    # Scope / filters
    parser.add_argument("--scope", required=True, choices=["personal", "pod", "team"],
                        help="Query scope")
    parser.add_argument("--project", default=os.environ.get("JIRA_PROJECT_KEY", ""),
                        help="Project key")
    parser.add_argument("--assignee", help="Filter by assignee")
    parser.add_argument("--label", help="Filter by label")
    parser.add_argument("--component", help="Filter by component")
    parser.add_argument("--sprint", help="Sprint name/id (use 'open' for openSprints())")
    parser.add_argument("--jql-extra", help="Extra JQL clause to append")

    # Time
    parser.add_argument("--time",
                        help="Time period: today, week, month, quarter, sprint, custom, or Nd (e.g. 30d)")
    parser.add_argument("--since", help="Start date (YYYY-MM-DD) for --time=custom")
    parser.add_argument("--until", help="End date (YYYY-MM-DD) for --time=custom")

    # Grouping
    parser.add_argument("--group-by", default="status",
                        choices=["status", "assignee", "priority", "issuetype", "reporter",
                                 "created_week", "created_month", "updated_week", "labels", "component"],
                        help="Primary grouping field (default: status)")
    parser.add_argument("--split-by",
                        choices=["status", "assignee", "priority", "issuetype"],
                        help="Secondary field for breakdown table")

    # Output
    parser.add_argument("--format", dest="output_format", default="mermaid",
                        choices=["mermaid", "text", "csv", "json", "markdown"],
                        help="Output format (default: mermaid)")
    parser.add_argument("--chart", default="auto",
                        choices=["auto", "pie", "bar"],
                        help="Chart type for mermaid output (default: auto)")
    parser.add_argument("--output", "-o", help="Output file path (default: stdout)")
    parser.add_argument("--max-issues", type=int, default=1000,
                        help="Max issues to fetch (default: 1000)")

    args = parser.parse_args()

    client = JiraClient()
    client.validate()

    # Build & execute query
    jql = build_jql(args)
    jira_info(f"Scope: {args.scope} | Group by: {args.group_by}")
    jira_info(f"JQL: {jql}")

    issues = fetch_all_issues(client, jql, max_total=args.max_issues)
    total = len(issues)
    jira_info(f"Total issues fetched: {total}")

    if not issues:
        jira_warn("No issues found matching the query.")
        return

    # Aggregate
    primary_data = aggregate(issues, args.group_by)

    # ── Mermaid output (default) ─────────────────────────────────────────
    if args.output_format == "mermaid":
        output = render_mermaid_full(
            scope=args.scope,
            group_by=args.group_by,
            primary_data=primary_data,
            issues=issues,
            jql=jql,
            total=total,
            chart_type=args.chart,
            split_by=args.split_by or "",
        )

    # ── Text output ──────────────────────────────────────────────────────
    elif args.output_format == "text":
        title = f"Jira Analytics — {args.scope} | group by {args.group_by}"
        output = render_text_chart(title, primary_data)
        if args.split_by:
            split_data = multi_aggregate(issues, args.group_by, args.split_by)
            for group, breakdown in split_data.items():
                output += render_text_chart(f"  {group} — by {args.split_by}", breakdown, max_bar=30)

    # ── CSV output ───────────────────────────────────────────────────────
    elif args.output_format == "csv":
        output = render_csv(primary_data)

    # ── JSON output ──────────────────────────────────────────────────────
    elif args.output_format == "json":
        report = {
            "scope": args.scope,
            "jql": jql,
            "total": total,
            "group_by": args.group_by,
            "data": primary_data,
            "generated": datetime.now().isoformat(),
        }
        if args.split_by:
            report["split_by"] = args.split_by
            report["breakdown"] = multi_aggregate(issues, args.group_by, args.split_by)
        output = pretty_json(report)

    # ── Markdown output ──────────────────────────────────────────────────
    elif args.output_format == "markdown":
        title = f"Jira Analytics — {args.scope} | group by {args.group_by}"
        output = render_markdown_table(title, primary_data, issues)

    else:
        jira_die(f"Unknown format: {args.output_format}")
        return  # unreachable

    # Write or print
    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(output)
        jira_ok(f"Report saved: {args.output}")
    else:
        print(output)

    jira_ok(f"Analytics complete ({total} issues, grouped by {args.group_by})")


if __name__ == "__main__":
    main()
