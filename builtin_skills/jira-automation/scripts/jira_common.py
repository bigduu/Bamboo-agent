"""
jira_common.py — Shared library for Jira REST API Python scripts.

Import:
    from jira_common import JiraClient, jira_info, jira_ok, jira_warn, jira_error, jira_die

Required env vars:
    JIRA_BASE_URL       — e.g. https://yourcompany.atlassian.net

Auth (one of):
    JIRA_EMAIL + JIRA_API_TOKEN  — Jira Cloud basic auth
    JIRA_PAT                     — Personal Access Token (Server/DC)

Optional:
    JIRA_API_VERSION    — "2" (default) or "3"
    JIRA_PROJECT_KEY    — Default project key
"""

import json
import os
import sys
import urllib.request
import urllib.error
import urllib.parse
import base64
from typing import Any, Optional


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
_COLORS = {
    "info":  "\033[36m",   # cyan
    "ok":    "\033[32m",   # green
    "warn":  "\033[33m",   # yellow
    "error": "\033[31m",   # red
    "reset": "\033[0m",
}

def _supports_color() -> bool:
    return hasattr(sys.stderr, "isatty") and sys.stderr.isatty()

def _log(level: str, msg: str) -> None:
    tag = level.upper().ljust(5)
    if _supports_color():
        c = _COLORS.get(level, "")
        r = _COLORS["reset"]
        print(f"{c}[{tag}]{r} {msg}", file=sys.stderr)
    else:
        print(f"[{tag}] {msg}", file=sys.stderr)

def jira_info(msg: str) -> None:
    _log("info", msg)

def jira_ok(msg: str) -> None:
    _log("ok", msg)

def jira_warn(msg: str) -> None:
    _log("warn", msg)

def jira_error(msg: str) -> None:
    _log("error", msg)

def jira_die(msg: str) -> None:
    _log("error", msg)
    sys.exit(1)


# ---------------------------------------------------------------------------
# JiraClient
# ---------------------------------------------------------------------------
class JiraClient:
    """Lightweight Jira REST client using only stdlib (no requests needed)."""

    def __init__(self) -> None:
        self.base_url = os.environ.get("JIRA_BASE_URL", "").rstrip("/")
        self.api_version = os.environ.get("JIRA_API_VERSION", "2")
        self.email = os.environ.get("JIRA_EMAIL", "")
        self.api_token = os.environ.get("JIRA_API_TOKEN", "")
        self.pat = os.environ.get("JIRA_PAT", "")
        self.project_key = os.environ.get("JIRA_PROJECT_KEY", "")

    def validate(self) -> None:
        """Check that required env vars are set."""
        if not self.base_url:
            jira_die("JIRA_BASE_URL is not set.")
        if self.pat:
            pass  # PAT auth
        elif self.email and self.api_token:
            pass  # Basic auth
        else:
            jira_die("Auth not configured. Set JIRA_EMAIL + JIRA_API_TOKEN, or JIRA_PAT.")

    def _auth_header(self) -> str:
        if self.pat:
            return f"Bearer {self.pat}"
        pair = f"{self.email}:{self.api_token}"
        b64 = base64.b64encode(pair.encode()).decode()
        return f"Basic {b64}"

    def _api_url(self, path: str) -> str:
        return f"{self.base_url}/rest/api/{self.api_version}{path}"

    def request(
        self,
        method: str,
        path: str,
        body: Optional[dict] = None,
        query: Optional[dict] = None,
    ) -> Any:
        """Make an HTTP request to Jira REST API and return parsed JSON."""
        url = self._api_url(path)
        if query:
            url += "?" + urllib.parse.urlencode(query)

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": self._auth_header(),
        }

        data = None
        if body is not None:
            data = json.dumps(body).encode("utf-8")

        req = urllib.request.Request(url, data=data, headers=headers, method=method)

        try:
            with urllib.request.urlopen(req) as resp:
                resp_body = resp.read().decode("utf-8")
                if resp_body:
                    return json.loads(resp_body)
                return None
        except urllib.error.HTTPError as e:
            error_body = ""
            try:
                error_body = e.read().decode("utf-8")
            except Exception:
                pass
            jira_error(f"HTTP {e.code} from {method} {url}")
            if error_body:
                jira_error(error_body)
            sys.exit(1)
        except urllib.error.URLError as e:
            jira_die(f"Connection error: {e.reason}")

    def get(self, path: str, query: Optional[dict] = None) -> Any:
        return self.request("GET", path, query=query)

    def post(self, path: str, body: dict) -> Any:
        return self.request("POST", path, body=body)

    def put(self, path: str, body: dict) -> Any:
        return self.request("PUT", path, body=body)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def pretty_json(obj: Any) -> str:
    """Return pretty-printed JSON string."""
    return json.dumps(obj, indent=2, ensure_ascii=False)

def print_json(obj: Any, raw: bool = False) -> None:
    """Print JSON to stdout."""
    if raw:
        print(json.dumps(obj, ensure_ascii=False))
    else:
        print(pretty_json(obj))

def read_file(path: str) -> str:
    """Read a file and return contents."""
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

def parse_csv(value: str) -> list[str]:
    """Split comma-separated string, strip whitespace, remove empties."""
    return [s.strip() for s in value.split(",") if s.strip()]


def build_assignee_field(assignee: str = "", assignee_id: str = "") -> Optional[dict]:
    """Build the correct assignee payload for Cloud (accountId) or Server (name).

    Jira Cloud requires {"accountId": "..."} while Server/DC uses {"name": "..."}.
    Pass --assignee for Server/DC usernames, --assignee-id for Cloud account IDs.
    If both are given, accountId (Cloud) wins.
    """
    if assignee_id:
        return {"accountId": assignee_id}
    if assignee:
        return {"name": assignee}
    return None


# ---------------------------------------------------------------------------
# JQL builder helpers
# ---------------------------------------------------------------------------
def build_scope_jql(scope: str, project: str = "", assignee: str = "",
                    label: str = "", component: str = "") -> list[str]:
    """Build JQL clauses for scope (personal/pod/team) and common filters."""
    parts: list[str] = []

    if project:
        parts.append(f"project = {project}")

    if scope == "personal":
        if assignee:
            parts.append(f'assignee = "{assignee}"')
        else:
            parts.append("assignee = currentUser()")
    elif scope in ("pod", "team"):
        if label:
            parts.append(f'labels = "{label}"')
        if component:
            parts.append(f'component = "{component}"')
        if assignee:
            parts.append(f'assignee = "{assignee}"')

    return parts


def build_sprint_jql(sprint: str = "") -> list[str]:
    """Build JQL clause for sprint filter."""
    if not sprint:
        return []
    if sprint == "open":
        return ["sprint in openSprints()"]
    return [f'sprint = "{sprint}"']


# Standard time-window mapping for summary/analytics scripts
TIME_JQL_MAP = {
    "today": "updated >= startOfDay()",
    "yesterday": "updated >= startOfDay(-1d) AND updated < startOfDay()",
    "week": "updated >= startOfWeek()",
    "month": "updated >= startOfMonth()",
}


def build_time_jql(time: str = "", since: str = "", until: str = "",
                   sprint: str = "", extra_map: Optional[dict] = None) -> list[str]:
    """Build JQL clauses for time-window filter.
    
    extra_map allows adding more named time windows (e.g. quarter, Nd).
    """
    if not time:
        return []

    merged = dict(TIME_JQL_MAP)
    if extra_map:
        merged.update(extra_map)

    if time in merged:
        return [merged[time]]
    elif time == "sprint":
        if not sprint:
            return ["sprint in openSprints()"]
        return []
    elif time == "custom":
        parts: list[str] = []
        if since:
            parts.append(f'updated >= "{since}"')
        if until:
            parts.append(f'updated <= "{until}"')
        return parts

    return []
