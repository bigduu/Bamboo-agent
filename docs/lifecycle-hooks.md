# Lifecycle hooks

Bamboo can run ordered command or embedded JavaScript handlers at agent
lifecycle events. Configure them in the `lifecycle_hooks` object in
`$BAMBOO_DATA_DIR/hooks.json` (normally `~/.bamboo/hooks.json`). Changes are
validated and hot-reloaded. Engine-owned hooks are snapshotted when an
execution starts. Notification hooks read the current configuration, and a
background Bash completion reads the configuration available when it arrives.

Hook matching and handler execution live in the standalone `bamboo-hooks`
crate. The engine owns the lifecycle seams and applies returned decisions or
context.

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
            "type": "javascript",
            "source": "function hook(input) { if ((input.tool_input.command || '').includes('rm -rf /')) return { decision: 'block', reason: 'root deletion is forbidden' }; return {}; }",
            "timeout_ms": 1000,
            "memory_limit_bytes": 16777216
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
normal dispatch; observer events always run every matching handler. Programmatic
hooks and configured handlers share the same dispatcher and priority ordering.

Handler settings:

| Type | Required field | Default timeout | Other limits |
|---|---|---:|---|
| `command` | `command` | 60000 ms | stdout and stderr are each capped at 64 KiB |
| `javascript` | `source` | 1000 ms | heap defaults to 16 MiB; result/error is capped at 64 KiB |

Every `timeout_ms` must be between 1 and 600000. JavaScript
`memory_limit_bytes` must be between 1 MiB and 256 MiB.

Command handlers run through Bamboo's preferred Bash-compatible shell. Agent
events use the session workspace; server-owned notification hooks fall back to
the configured default work area and then the server process directory.

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

Both handler types receive the same schema-versioned object. A command reads
its JSON representation from stdin. JavaScript receives the object as the
`input` argument to `hook(input)`.

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

Command handlers also receive:

- `BAMBOO_HOOK_EVENT`: the event name from the table above.
- `BAMBOO_SESSION_ID`: the owning session id.

## Output contract

For decision-capable events, a handler can return one response object:

```json
{
  "decision": "block",
  "reason": "production deletion is forbidden",
  "additional_context": "Use the staging workspace instead."
}
```

`decision` is `allow`, `block`, or `ask`. `additional_context` can be returned
with or without a decision.

A command exits 0 and writes either no stdout or exactly one JSON response.
Exit 2 blocks with stderr as the reason. Other non-zero exits, malformed
stdout, and timeouts are logged and treated as non-blocking failures.

A JavaScript handler defines a global `hook` function and returns the response
object directly. It may be synchronous or return a Promise:

```javascript
async function hook(input) {
  await Promise.resolve();
  if (input.hook_event_name === "PreToolUse" && input.tool_name === "Bash") {
    return {
      decision: "ask",
      reason: "Review this shell command before execution",
    };
  }
  return {};
}
```

Returning `undefined`, `null`, or an empty object continues. A thrown error,
rejected Promise, malformed response, resource-limit failure, or timeout is
logged and treated as a non-blocking failure.

Observer events (`SessionEnd`, `Notification`) never change control flow.
`PreCompact` consumes only `additional_context`; its decision and a command's
exit-2 block signal are deliberately ignored.

## JavaScript isolation

Each invocation gets a fresh QuickJS runtime and context. Bamboo installs no
module loader or host functions, so the script has no direct filesystem,
network, process, environment-variable, or timer API. Globals such as
`process`, `require`, `fetch`, `Deno`, and `Bun` are absent.

The runtime enforces a wall-clock deadline, a configurable heap limit, a fixed
stack limit, and a bounded serialized result. These are capability and resource
guardrails inside the Bamboo process, not an operating-system process or
container boundary. Only user-owned global hook configuration should be
enabled; project-local hook discovery requires a separate trust gate.

## Command example

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

The lifecycle-hook dry-run endpoint accepts either handler type and uses the
production input schema, timeout, memory/output limits, shell/runtime, and a
deterministic synthetic payload for the selected event.
