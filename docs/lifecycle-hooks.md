# Lifecycle hooks

Bamboo can run ordered command or external script handlers at agent lifecycle
events. Configure them in the `lifecycle_hooks` object in
`$BAMBOO_DATA_DIR/hooks.json` (normally `~/.bamboo/hooks.json`). Changes are
validated and hot-reloaded. Engine-owned hooks are snapshotted when an
execution starts. Notification hooks read the current configuration, and a
background Bash completion reads the configuration available when it arrives.

Hook matching, process orchestration, and handler execution live in the
standalone `bamboo-hooks` crate. The engine owns the lifecycle seams and
applies returned decisions or context.

## Configuration

```json
{
  "lifecycle_hooks": {
    "enabled": true,
    "PreToolUse": [
      {
        "matcher": "^Bash$",
        "hooks": [
          {
            "type": "command",
            "command": ".bamboo/hooks/audit-bash.sh",
            "timeout_ms": 5000
          },
          {
            "type": "script",
            "path": ".bamboo/hooks/check-command.js",
            "runner": "bun",
            "timeout_ms": 5000
          }
        ]
      }
    ]
  }
}
```

Each event contains ordered groups. A group and the whole section can be
disabled without deleting them. `matcher` is a Rust regular expression over
the tool name and is supported only for `PreToolUse` and `PostToolUse`.

Handlers run sequentially in configuration order. A control decision stops
normal dispatch; observer events always run every matching handler.
Programmatic hooks and configured handlers share the same dispatcher and
priority ordering.

Handler settings:

| Type | Required field | Optional fields | Default timeout |
|---|---|---|---:|
| `command` | `command` | `timeout_ms` | 60000 ms |
| `script` | `path` | `runner`, `timeout_ms` | 60000 ms |

Every `timeout_ms` must be between 1 and 600000. Stdout and stderr are each
captured up to 64 KiB. A relative script path is resolved against the session
workspace. Server-owned hooks fall back to the configured default work area
and then Bamboo's data directory.

`runner` defaults to `auto`:

| Script extension | Auto runtime order | Explicit runner |
|---|---|---|
| `.js`, `.mjs`, `.cjs` | `node`, then `bun run` | `node` or `bun` |
| `.py` | `python3`, then `python`; Windows also tries `py -3` | `python` |
| `.sh` | system sh/Bash-compatible runtime | `bash` |
| `.ps1` | `pwsh`, then Windows PowerShell | `powershell` |
| `.bat`, `.cmd` | `cmd.exe` on Windows | `cmd` |

Bamboo does not bundle any of these runtimes. The selected executable must be
available in the environment prepared for Bamboo child processes. An explicit
runner must be compatible with the script extension. Batch files remain valid
in shared configuration but report a platform diagnostic when run outside
Windows.

Command handlers continue to run through Bamboo's preferred Bash-compatible
shell. Agent events use the session workspace; server-owned notification hooks
fall back to the configured default work area and then the server process
directory.

Supported events:

| Event | When it runs | Control behavior |
|---|---|---|
| `SessionStart` | A run is initialized or resumed | May inject context or stop the run. |
| `UserPromptSubmit` | Before a submitted prompt is persisted | May block or extend the effective prompt. |
| `PreToolUse` | After arguments are parsed, before permission and dispatch | `allow`, `block`, and `ask` participate in the parent-agent permission path. |
| `PostToolUse` | After a foreground or background tool completes | May attach feedback; background Bash completion uses `tool_name: "Bash"`. |
| `Stop` | Before the run emits its terminal completion | May force a bounded continuation. |
| `SessionEnd` | After a terminal status is known | Observer only; decisions cannot change the settled result. |
| `PreCompact` | Immediately before LLM context summarization | `additional_context` becomes custom summarizer instructions. Decisions are ignored because blocking compaction risks context overflow. |
| `Notification` | After notification policy and dedup, alongside desktop/ntfy/Bark delivery | Fire-and-forget observer; decisions and output are ignored. |

## Input envelope

Both handler types receive the same schema-versioned JSON object on stdin:

```json
{
  "schema_version": 1,
  "hook_event_name": "PreCompact",
  "session_id": "session-123",
  "workspace_path": "/work/project",
  "model": "claude-sonnet-4",
  "payload": {
    "type": "compression",
    "estimated_tokens": 170000,
    "usage_percent": 85.0,
    "max_context_tokens": 200000,
    "trigger_context_tokens": 160000,
    "trigger": "threshold",
    "phase": "pre-turn"
  },
  "timestamp": "2026-07-22T09:00:00Z"
}
```

Compression `trigger` is `threshold`, `forced_overflow_recovery`, or `manual`.
A delivered notification payload contains `id`, `category`, `priority`,
`title`, `body`, `dedup_key`, `created_at`, and an optional `click_url`.
Tool-oriented envelopes also keep the convenience fields `tool_name`,
`tool_input`, and `tool_response` for compatibility.

All handlers also receive:

- `BAMBOO_HOOK_EVENT`: the event name from the table above.
- `BAMBOO_SESSION_ID`: the owning session id.

Script handlers additionally receive `BAMBOO_HOOK_SCRIPT`, the resolved script
path.

## Output contract

For decision-capable events, a handler can write one response object to
stdout:

```json
{
  "decision": "block",
  "reason": "production deletion is forbidden",
  "additional_context": "Use the staging workspace instead."
}
```

`decision` is `allow`, `block`, or `ask`. `additional_context` can be returned
with or without a decision.

A handler exits 0 and writes either no stdout or exactly one JSON response.
Exit 2 blocks with stderr as the reason. Other non-zero exits, malformed or
truncated stdout, missing runtimes, and timeouts are logged and treated as
non-blocking failures. The dry-run endpoint returns these diagnostics without
persisting configuration.

Observer events (`SessionEnd`, `Notification`) never change control flow.
`PreCompact` consumes only `additional_context`; its decision and an exit-2
block signal are deliberately ignored.

## Script examples

Node.js or Bun (`.bamboo/hooks/check-command.js`):

```javascript
let raw = "";
process.stdin.setEncoding("utf8");
process.stdin.on("data", (chunk) => (raw += chunk));
process.stdin.on("end", () => {
  const input = JSON.parse(raw);
  const command = input.tool_input?.command ?? "";
  const response = command.includes("rm -rf /")
    ? { decision: "block", reason: "root deletion is forbidden" }
    : {};
  process.stdout.write(JSON.stringify(response));
});
```

Python (`.bamboo/hooks/add-context.py`):

```python
import json
import sys

payload = json.load(sys.stdin)
print(json.dumps({
    "additional_context": f"hooked {payload['hook_event_name']}"
}))
```

Shell (`.bamboo/hooks/block-dangerous-bash.sh`):

```bash
#!/usr/bin/env bash
set -euo pipefail
payload=$(cat)
command=$(printf '%s' "$payload" | jq -r '.tool_input.command // ""')
if printf '%s' "$command" | grep -Eq '(^|[;&|[:space:]])rm[[:space:]]+-rf[[:space:]]+(/|~)'; then
  printf '%s\n' 'dangerous recursive deletion is blocked' >&2
  exit 2
fi
```

## Process and security model

Every script invocation is a fresh child process. Bamboo supplies the prepared
environment and working directory, writes the envelope to stdin, drains
bounded stdout/stderr concurrently, enforces the configured wall-clock
deadline, and kills the process tree on timeout.

External scripts are not sandboxed. They run with the filesystem, network,
environment, and operating-system permissions of the Bamboo process user.
Only trusted, user-owned hook configuration and scripts should be enabled.
Project-local hook discovery requires a separate trust gate. Use an
OS/container sandbox or a restricted service account when stronger isolation
is required.

Auto-format after an edit:

```json
{
  "PostToolUse": [{
    "matcher": "^(Write|Edit)$",
    "hooks": [{"type": "command", "command": "cargo fmt", "timeout_ms": 30000}]
  }]
}
```

The lifecycle-hook dry-run endpoint accepts either handler type and uses the
production input schema, timeout, output limits, working directory, runtime
selection, and a deterministic synthetic payload for the selected event.
