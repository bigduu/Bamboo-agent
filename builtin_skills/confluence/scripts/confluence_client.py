#!/usr/bin/env python3
"""Core Confluence Server/Data Center REST API client.

Shared by all Confluence skill scripts. Handles authentication,
base URL resolution, and structured JSON responses.

Environment variables:
    CONFLUENCE_BASE_URL   - e.g. https://confluence.example.internal
    CONFLUENCE_USERNAME   - basic auth username
    CONFLUENCE_PASSWORD   - basic auth password
    CONFLUENCE_PAT        - personal access token (bearer)

Auth priority: PAT > basic auth.  Exactly one method must be available.
"""

import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
import ssl


def _base_url():
    url = os.environ.get("CONFLUENCE_BASE_URL", "").rstrip("/")
    if not url:
        _die("CONFLUENCE_BASE_URL is not set")
    return url


def _auth_headers():
    """Return auth header dict.  PAT wins over basic auth."""
    pat = os.environ.get("CONFLUENCE_PAT", "").strip()
    if pat:
        return {"Authorization": f"Bearer {pat}"}
    user = os.environ.get("CONFLUENCE_USERNAME", "").strip()
    pwd = os.environ.get("CONFLUENCE_PASSWORD", "").strip()
    if user and pwd:
        import base64
        cred = base64.b64encode(f"{user}:{pwd}".encode()).decode()
        return {"Authorization": f"Basic {cred}"}
    _die("No Confluence credentials: set CONFLUENCE_PAT or both CONFLUENCE_USERNAME and CONFLUENCE_PASSWORD")


def _ssl_context():
    """Create an SSL context that honors CONFLUENCE_VERIFY_SSL."""
    verify = os.environ.get("CONFLUENCE_VERIFY_SSL", "1").strip().lower()
    if verify in ("0", "false", "no"):
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        return ctx
    return None  # use default


def _die(msg, code=1):
    print(json.dumps({"ok": False, "error": msg}))
    sys.exit(code)


def ok(data):
    """Print successful JSON response and exit 0."""
    print(json.dumps({"ok": True, "data": data}, ensure_ascii=False, indent=2))
    sys.exit(0)


def fail(msg, status_code=None, body=None):
    """Print error JSON response and exit 1."""
    err = {"ok": False, "error": msg}
    if status_code is not None:
        err["status_code"] = status_code
    if body is not None:
        # Truncate very long error bodies
        err["response_body"] = body[:2000] if len(body) > 2000 else body
    print(json.dumps(err, ensure_ascii=False, indent=2))
    sys.exit(1)


# ── public helpers ──────────────────────────────────────────────────

BASE_URL = None
AUTH_HEADERS = None


def init():
    """Initialise module-level BASE_URL and AUTH_HEADERS. Call once at startup."""
    global BASE_URL, AUTH_HEADERS
    BASE_URL = _base_url()
    AUTH_HEADERS = _auth_headers()


def api_url(path, **params):
    """Build full URL for a REST path with optional query params."""
    url = f"{BASE_URL}{path}"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    return url


def request(method, path, params=None, json_body=None, extra_headers=None,
            raw_data=None, content_type=None):
    """Execute an HTTP request against the Confluence REST API.

    Returns (status_code, parsed_json | raw_text).
    """
    url = api_url(path, **(params or {}))
    headers = dict(AUTH_HEADERS)
    if json_body is not None:
        data = json.dumps(json_body, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json"
    elif raw_data is not None:
        data = raw_data
        if content_type:
            headers["Content-Type"] = content_type
    else:
        data = None
    if extra_headers:
        headers.update(extra_headers)

    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    ctx = _ssl_context()
    try:
        with urllib.request.urlopen(req, context=ctx) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            try:
                return resp.status, json.loads(body)
            except json.JSONDecodeError:
                return resp.status, body
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace") if e.fp else ""
        try:
            body = json.loads(body)
        except (json.JSONDecodeError, ValueError):
            pass
        return e.code, body


def get(path, **params):
    """GET helper."""
    return request("GET", path, params=params)


def post(path, json_body=None, **kwargs):
    """POST helper."""
    return request("POST", path, json_body=json_body, **kwargs)


def put(path, json_body=None, **kwargs):
    """PUT helper."""
    return request("PUT", path, json_body=json_body, **kwargs)


def delete(path, **kwargs):
    """DELETE helper."""
    return request("DELETE", path, **kwargs)


def multipart_upload(path, file_path, comment=None):
    """Upload a file as multipart/form-data.

    Confluence requires X-Atlassian-Token: no-check (or nocheck) to bypass
    XSRF protection for attachment uploads.
    """
    import mimetypes
    boundary = "----ConfluenceSkillBoundary"
    filename = os.path.basename(file_path)
    mime_type = mimetypes.guess_type(file_path)[0] or "application/octet-stream"

    parts = []
    # file part
    with open(file_path, "rb") as f:
        file_data = f.read()
    parts.append(
        f'--{boundary}\r\n'
        f'Content-Disposition: form-data; name="file"; filename="{filename}"\r\n'
        f'Content-Type: {mime_type}\r\n\r\n'
    )
    file_part_header = parts[-1].encode("utf-8")

    body_parts = [file_part_header, file_data, b"\r\n"]

    if comment:
        comment_part = (
            f'--{boundary}\r\n'
            f'Content-Disposition: form-data; name="comment"\r\n\r\n'
            f'{comment}\r\n'
        ).encode("utf-8")
        body_parts.append(comment_part)

    body_parts.append(f'--{boundary}--\r\n'.encode("utf-8"))
    raw = b"".join(body_parts)

    return request(
        "POST", path,
        raw_data=raw,
        content_type=f"multipart/form-data; boundary={boundary}",
        extra_headers={"X-Atlassian-Token": "nocheck"},
    )
