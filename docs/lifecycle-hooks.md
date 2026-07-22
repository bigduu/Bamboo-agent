# Lifecycle command hooks

Bamboo can run user-configured shell commands at agent lifecycle events. Use
the Hooks settings page, or edit the `lifecycle_hooks` object in
`$BAMBOO_DATA_DIR/hooks.json` (normally `~/.bamboo/hooks.json`). Changes are
validated and hot-reloaded. Engine-owned hooks are snapshotted when an
execution starts. Notification hooks read the current configuration, and a
background Bash completion reads the configuration available when it arrives.

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
            "command": ".bamboo/hooks/block-dangerous-bash.sh",
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
the tool name and is supported only for `PreToolUse` and `PostToolUse`. A
command runs through Bamboo's preferred Bash-compatible shell. Agent events use
the session workspace; server-owned notification hooks fall back to the
configured default work area and then the server process directory. `timeout_ms`
defaults to 60000 and must be between 1 and 600000.

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

## Command protocol

The command receives one JSON object on stdin and these environment variables:

- `BAMBOO_HOOK_EVENT`: the event name from the table above.
- `BAMBOO_SESSION_ID`: the owning session id.

Every stdin envelope has stable common fields and an event-specific `payload`:

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

For decision-capable events, exit 0 with no stdout means continue. Exit 0 may
instead print exactly one JSON response:

```json
{
  "decision": "block",
  "reason": "production deletion is forbidden",
  "additional_context": "Use the staging workspace instead."
}
```

`decision` is `allow`, `block`, or `ask`. `additional_context` can be returned
with or without a decision. Exit 2 blocks with stderr as the reason. Other
non-zero exits, malformed stdout, and timeouts are logged and treated as
non-blocking failures. Stdout and stderr are each capped at 64 KiB.

Observer events (`SessionEnd`, `Notification`) never change control flow.
`PreCompact` consumes only `additional_context`; its decision and exit-2 block
signal are deliberately ignored.

## Examples

Block dangerous Bash commands (`.bamboo/hooks/block-dangerous-bash.sh`):

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

Auto-format after an edit:

```json
{
  "PostToolUse": [{
    "matcher": "^(Write|Edit)$",
    "hooks": [{"type": "command", "command": "cargo fmt", "timeout_ms": 30000}]
  }]
}
```

Send a desktop notification when a run stops:

```json
{
  "Stop": [{
    "hooks": [{"type": "command", "command": "notify-send 'Bamboo' 'Agent run stopped'"}]
  }]
}
```

Use the Hooks settings page's dry-run action before enabling a command. The
test uses the production shell, environment, timeout, output cap, and a
deterministic synthetic payload for the selected event.
