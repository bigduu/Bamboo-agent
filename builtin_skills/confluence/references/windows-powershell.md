# Windows and PowerShell Notes for Confluence Work

If the user is on Windows, prefer PowerShell over `cmd.exe` for Confluence work. PowerShell handles environment variables, multi-line commands, and JSON preparation more reliably.

1. Set environment variables with PowerShell syntax such as `$env:CONFLUENCE_BASE_URL = "https://confluence.example.internal"`.
2. Prefer `curl.exe` instead of bare `curl` so PowerShell does not route the call through an alias or wrapper unexpectedly.
3. For complex JSON payloads, do not hand-escape long storage markup inline. Prefer generating a JSON file with Python and then posting that file.
4. When the Unix examples use heredoc syntax like `python3 - <<'PY'`, translate that into a PowerShell here-string piped into Python, or write a temporary `.py` file first.
5. Be careful with Windows attachment paths such as `C:\Users\Alice\Downloads\rollout-checklist.pdf`, especially when the path contains spaces.
6. Write payload files as UTF-8 when possible so non-ASCII page titles and storage markup survive correctly.
7. In `command_only` mode for Windows users, prefer emitting PowerShell examples explicitly rather than Bash.

## PowerShell environment examples

```powershell
$env:CONFLUENCE_BASE_URL = "https://confluence.example.internal"
$env:CONFLUENCE_PAT = "your_token_here"
$env:CONFLUENCE_SPACE_KEY = "ENG"
$BASE_URL = $env:CONFLUENCE_BASE_URL.TrimEnd('/')

curl.exe -sS `
  -H "Authorization: Bearer $env:CONFLUENCE_PAT" `
  "$BASE_URL/rest/api/content?limit=1"
```

## PowerShell payload example

```powershell
@'
import json

payload = {
    "type": "page",
    "title": "Weekly Platform Sync - 2026-03-21",
    "space": {"key": "PLAT"},
    "ancestors": [{"id": 987654}],
    "body": {
        "storage": {
            "value": "<h1>Summary</h1><p>...</p>",
            "representation": "storage"
        }
    }
}

with open("confluence-create.json", "w", encoding="utf-8") as f:
    json.dump(payload, f, ensure_ascii=False)
'@ | python -

curl.exe -sS `
  -H "Content-Type: application/json" `
  -H "Authorization: Bearer $env:CONFLUENCE_PAT" `
  -d "@confluence-create.json" `
  "$BASE_URL/rest/api/content"
```
