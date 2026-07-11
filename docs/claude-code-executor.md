# Driving Claude Code as an external agent — protocol reference

Status: reference knowledge for a future `ClaudeCodeExecutor` (`ChildExecutor` impl).
Source: distilled from `chenhg5/cc-connect` `agent/claudecode/` (Go, battle-tested against
Claude Code 2.x in production), verified against the repo at commit `main@2026-07-11`.
File references below are to that repo.

## Where this plugs into bamboo

`bamboo-subagent::provision::ExecutorSpec` already reserves the slot
(`provision.rs:221`):

```rust
/// Wrap an external CLI agent as the engine.
CliAdapter { command: String, args: Vec<String> },
```

No executor implements it yet. The work is:

1. `ClaudeCodeExecutor: ChildExecutor` (`executor.rs:169`) — owns the child process,
   translates `RunSpec` → stream-json stdin, stream-json stdout → `EventSink`,
   `SteerInbox` → mid-turn user injection / permission responses,
   `CancellationToken` → graceful shutdown (see §7).
2. Worker executor factory: map `ExecutorSpec::CliAdapter`(or a dedicated
   `ClaudeCode` variant carrying model/permission-mode/resume-id) to it.
3. `bamboo-engine/src/external_agents/runtime.rs:104,215` — accept executor kind
   `"claude_code"` alongside `"echo"` / `"bamboo_runtime"`.

## 1. Spawn

One **long-lived process per session** (NOT per message). Repeated turns are
repeated stdin writes to the same process; `--resume` is only for reattaching
after the process died.

```
claude \
  --output-format stream-json \
  --input-format  stream-json \
  --permission-prompt-tool stdio \
  --replay-user-messages \
  --verbose \
  [--permission-mode <acceptEdits|plan|bypassPermissions>]   # omit for "default"
  [--resume <session_id>]                                    # omit for a fresh session
  [--model <model>]
  [--allowedTools a,b] [--disallowedTools a,b]
  [--system-prompt <s>] [--append-system-prompt-file <path>]
```

(cc-connect: `agent/claudecode/session.go:234-322`)

Environment:
- **Strip `CLAUDECODE`** from the child env — otherwise Claude Code detects a
  nested session and misbehaves (`session.go:372`).
- Put the child in its own **process group** so shutdown can kill the whole tree
  (claude → its MCP servers) (`session.go:369`).
- Inject whatever env the executor wants the agent's shell tools to see
  (cc-connect injects `CC_PROJECT` / `CC_SESSION_KEY` for its send-back IPC).

Gotchas:
- Drop `--verbose` when routing through claude-code-router — router output
  corrupts the JSON stream (`claudecode.go:524-526`).
- `bypassPermissions` under euid 0 is rejected by the CLI; downgrade and surface
  a warning (`session.go:225-229`).

## 2. Wire protocol

Newline-delimited JSON both ways. **Reader must allow 10 MB lines**
(`session.go:472` uses a 10 MB scanner buffer; tool results can be huge).

### stdin → claude (user turn)

```json
{"type":"user","message":{"role":"user","content":"fix the failing test"}}
```

Multimodal content uses parts:

```json
{"type":"user","message":{"role":"user","content":[
  {"type":"text","text":"what is in this screenshot?"},
  {"type":"image","source":{"type":"base64","media_type":"image/png","data":"..."}}
]}}
```

Non-image files are NOT inlined: write them to a scratch dir and reference the
absolute paths in the prompt text ("Files saved locally, please read them: …") —
the agent opens them with its own Read tool (`core/message.go:103-141`).

### stdout → executor, dispatched on top-level `type`

| type | meaning | what to extract |
|---|---|---|
| `system` | session bootstrap | `session_id` (agent-assigned — persist it for resume), `model` |
| `assistant` | one model message | iterate `message.content[]`: `text` → token/text event; `thinking` → thinking event; `tool_use` → tool-start event (`name`, `input`). `message.usage` gives live context numbers (its `output_tokens` is a placeholder, ignore) |
| `user` | echoed tool results | `content[].type == "tool_result"` → tool-end event (`content` truncated, `is_error`) |
| `result` | turn end | final text, `session_id`, token totals. **`subtype: "compact"/"compaction"` is MID-turn** — do not treat as turn completion (cc-connect issue #481) |
| `control_request` | permission ask | see §3 |
| `control_cancel_request` | CLI withdrew a pending ask | drop the matching pending approval |

Unknown types: log at debug, never fail the stream (`session.go:587-594`).
On process exit, surface stderr as an error event and complete the run exactly
once (`session.go:512-535`).

## 3. Permission relay (`--permission-prompt-tool stdio`)

The CLI asks before each gated tool call:

```json
{"type":"control_request","request_id":"r1","request":{
  "subtype":"can_use_tool","tool_name":"Bash","input":{"command":"rm -rf build"}}}
```

Executor decides locally (auto-allow for `bypassPermissions`-equivalent modes,
auto-allow edit-tools for acceptEdits, etc.) or relays to the parent as a
NeedsHuman-style event, then answers on stdin:

```json
{"type":"control_response","response":{
  "subtype":"success","request_id":"r1",
  "response":{"behavior":"allow","updatedInput":{"command":"rm -rf build"}}}}
```

Deny: `{"behavior":"deny","message":"user denied"}`. On allow, echo the tool
`input` back as `updatedInput` (it may also be edited). `AskUserQuestion`
control_requests carry structured questions — map to bamboo's QuestionDialog
path rather than the permission path (`session.go:856-937`).

In bamboo terms: emit a `NeedsHuman` event with `request_id`, park the pending
approval in a map of oneshot channels, resolve it when the steer inbox delivers
the decision — same shape as the codex app-server adapter in cc-connect
(`appserver_session.go:542-617`).

## 4. Session identity & resume

- The session id is **agent-assigned**: read it from `system` / `result` events;
  persist per bamboo child.
- Live continuity = same process, more stdin writes. Resume after restart =
  `--resume <persisted_id>`; the resumed process may assign a NEW id — keep a
  history of prior ids if you need to recognize transcripts.
- Transcripts live at `~/.claude/projects/<hashed-workdir>/<session_id>.jsonl`
  if history listing is ever wanted (`claudecode.go:532-587`).

## 5. Cancellation / shutdown

Graceful 3-phase close (`session.go:1171-1228`):

1. close stdin (EOF lets the CLI run its Stop hooks),
2. wait up to ~120 s for exit,
3. SIGTERM the process group, wait 5 s, SIGKILL the group.

There is no reliable mid-turn interrupt over this protocol (cc-connect's `/stop`
kills the process and resumes by id). Map `CancellationToken` → full close;
rely on `--resume` for continuation.

## 6. Billing note

The spawned binary is official Claude Code with the user's own login; as of
2026-07 subscription auth still covers `claude -p`/stream-json usage (the
June 15 credit split was paused). If the host also configures `ANTHROPIC_API_KEY`
in the child env, the CLI bills the API key instead — decide explicitly which
one the executor forwards.

## 7. Minimal executor sketch

```rust
pub struct ClaudeCodeExecutor { /* binary path, defaults */ }

#[async_trait]
impl ChildExecutor for ClaudeCodeExecutor {
    async fn run(&self, spec: RunSpec, events: EventSink,
                 steer: SteerInbox, cancel: CancellationToken) -> ChildOutcome {
        // 1. spawn `claude` (fresh or --resume from spec metadata), own pgroup
        // 2. write the assignment as a stream-json user message
        // 3. select-loop:
        //    - stdout line  → parse → events.emit(...)   (§2 table)
        //      control_request → events.emit(NeedsHuman) + park oneshot
        //    - steer msg    → permission decision → control_response
        //                     | plain text → inject as next user message
        //    - cancel       → §5 shutdown → ChildOutcome::Cancelled
        //    - `result` with Done → drain, keep process warm or close per policy
    }
}
```
