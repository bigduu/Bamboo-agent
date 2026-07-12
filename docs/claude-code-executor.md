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
  --permission-mode <acceptEdits|plan|bypassPermissions|default>  # ALWAYS explicit — see below
  [--strict-mcp-config] [--setting-sources project]               # isolation — see below
  [--resume <session_id>]                                    # omit for a fresh session
  [--model <model>]
  [--allowedTools a,b] [--disallowedTools a,b]
  [--system-prompt <s>] [--append-system-prompt-file <path>]
```

(cc-connect: `agent/claudecode/session.go:234-322`)

**`--permission-mode` is ALWAYS passed explicitly (issue #443, CRITICAL).**
Real-machine e2e against claude 2.1.207 found that the headless stream-json
default — when the flag is omitted entirely — is `auto`, which self-approves
every tool and never emits a `can_use_tool` ask. `ClaudeCodeExecutor::build_command`
therefore always sends `permission_mode` when configured, else the literal
string `default` — never nothing. This is what makes the "no host bridge →
deny unless `bypassPermissions`" local-decide policy in §3 actually trigger;
before this fix it was unreachable dead code (every ask was auto-approved by
the CLI itself, so `control_request` never fired for anything the executor
would have denied).

**Isolation from the invoking user's `~/.claude`, by default (issue #443).**
The same e2e run showed the child loading the user's entire global config: 6
MCP servers (including a desktop-control server), every installed skill, and
memory paths — ~8k cache-creation tokens and a large ambient-authority surface
for a single `touch`. Unless `inherit_user_config: true` is set on the
`ClaudeCode` executor spec, `build_command` adds `--strict-mcp-config` and
`--setting-sources project`, so the child sees only project-scoped config, not
the user's global one.

Environment (issue #443 — env allowlist supersedes the earlier
strip-one-var approach):
- The child is spawned under `env_clear()` **plus an explicit allowlist**:
  `HOME`, `PATH`, `SHELL`, `TERM`, `LANG`, `LC_*` (prefix), `TMPDIR`, `USER`,
  `LOGNAME`. Everything else in the parent process env — including any
  `*_API_KEY` — is stripped by construction, not by a denylist.
- `forward_env: Vec<String>` on the executor spec names EXTRA variables to
  forward verbatim on top of the allowlist. Forwarding `ANTHROPIC_API_KEY`
  this way is an explicit opt-in that flips billing from the CLI's own
  subscription auth to the API key — see §6.
- `CLAUDECODE` is still explicitly `env_remove`d after the allowlist pass —
  redundant now that `env_clear()` means it can't leak in from the parent at
  all, kept as executable documentation of the nested-session hazard below.
- Put the child in its own **process group** so shutdown can kill the whole tree
  (claude → its MCP servers) (`session.go:369`).

Nested-session hazard: Claude Code detects its own `CLAUDECODE` env var and
misbehaves if it inherits one from an outer session (`session.go:372`).

Gotchas:
- Drop `--verbose` when routing through claude-code-router — router output
  corrupts the JSON stream (`claudecode.go:524-526`).
- `bypassPermissions` under euid 0 is rejected by the CLI; downgrade and surface
  a warning (`session.go:225-229`).
- Even in `default` mode, the CLI's own sandbox auto-runs plain read-only
  commands (e.g. a bare `echo`) without asking — exercising the permission
  relay requires a command with a real side effect (a file write).

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

**Relay timeout (issue #443).** When a host bridge IS attached,
`decide_and_respond` wraps `HostBridge::approval_call` in `tokio::time::timeout`
bounded by `APPROVAL_RELAY_TIMEOUT` (300s). A host approver that never replies
(crashed UI, orphaned session) no longer hangs the CLI turn forever — on
expiry the executor denies with `"approval relay timed out after 300s;
denying"` and the turn continues. This is distinct from `approval_call`'s
existing error path (the reply `oneshot` sender dropped), which is still
handled as an immediate deny.

## 4. Session identity & resume

Implemented (issue #444) in `ClaudeCodeExecutor`. `RunSpec.messages` is the
activation discriminant (`proto.rs:28`): empty on the first activation of an
actor, non-empty on a reactivation (`send_message`/`update`/`rerun`) that
ships the actor's prior conversation.

**Durable state.** The executor persists the agent-assigned session id in the
child's stable per-activation storage dir — resolved exactly like
`BambooRuntimeExecutor::build` (`subagent_worker.rs:194-202`): `spec.storage_dir`
when the parent already isolated it, else `$TMPDIR/bamboo-subagents/<child_id>`.
Both worker factory arms (`subagent_worker.rs`, `broker_agent.rs`) resolve this
dir and pass it into `ClaudeCodeExecutor::new`'s `state_dir` parameter.

State file: `<dir>/claude-code-session.json`

```json
{ "session_id": "...", "workspace": "...", "updated_at": "2026-..." }
```

Written atomically (tmp file + `rename` in the same dir) on EVERY `system` or
`result` frame that carries a `session_id` — a resumed session may be assigned
a brand-new id, so this always re-captures rather than assuming stability.
`workspace` is recorded alongside the id: Claude Code transcripts are
machine-local under `~/.claude/projects/<hashed-workdir>/`, so a later
activation against a DIFFERENT workspace (different project, or a different
machine entirely) treats the persisted id as unusable.

**Activation logic** (`ClaudeCodeExecutor::run`):

1. `messages` empty → fresh session; delete any stale state file first (a
   `rerun` must never accidentally resume).
2. `messages` non-empty AND the state file has an id recorded against the
   SAME `workspace` → spawn with `--resume <id>`, sending just the live
   assignment (the CLI already owns the transcript).
3. `messages` non-empty but no usable id (first run on this machine, storage
   GC'd, workspace changed) → **fallback rehydration**: the shipped history
   is rendered into a bounded text preamble (role-tagged, `**role**: content`,
   capped to the last ~40 messages / ~24k chars with oldest dropped first and
   an explicit `_[truncated: N earlier message(s) omitted]_` note), clearly
   delimited under `## Prior conversation (rehydrated)` / `## Current task`
   headings so the model doesn't confuse rehydrated context with the live
   task, and prepended to the assignment. The assignment's own trailing user
   message (shipped in `messages` per the wire contract) is excluded from the
   preamble so it isn't duplicated. A warning is logged; context is never
   silently dropped.
4. **Resume-failure retry:** if a `--resume` spawn exits before ever emitting
   a terminal `result` frame (bad/GC'd session id — the CLI errors out fast),
   the run retries ONCE without `--resume`, using the same fallback
   rehydration as step 3, after clearing the stale state file. No retry loop
   beyond this single attempt, and the retry only triggers for THIS specific
   failure mode (a `--resume` attempt that never produced a result) — any
   other error is returned as-is.

**Non-goals (still).** Mid-turn steering into a genuinely new turn on the same
session, and multimodal — unaffected by this change. Env-forwarding shipped
as part of #443 (see §1/§6). Cross-machine resume is out of scope by
construction (see the workspace/machine-locality note above); if the actor is
redeployed to a different host or workspace, activation falls through to
fallback rehydration automatically.

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
June 15 credit split was paused). Real-machine e2e confirmed this: the
`system.init` frame reported `apiKeySource: "none"` with no key in the child
env — the subscription is what actually gets billed.

**Env policy (issue #443, implemented).** The executor no longer forwards the
parent process env wholesale. `build_command` runs the child under
`env_clear()` plus a fixed allowlist — `HOME`, `PATH`, `SHELL`, `TERM`, `LANG`,
`LC_*` (prefix), `TMPDIR`, `USER`, `LOGNAME` — which is enough for the CLI and
its shell tools to function but excludes every `*_API_KEY` and other ambient
secret by construction. `forward_env: Vec<String>` on the executor spec (and
the matching `claude_code_forward_env` config field, §8) names EXTRA variables
to forward verbatim; forwarding `ANTHROPIC_API_KEY` this way is an EXPLICIT
opt-in that flips billing from the subscription to the API key — never the
implicit default.

## 7. Executor shape (as implemented)

```rust
pub struct ClaudeCodeExecutor {
    binary: String,
    model: Option<String>,
    permission_mode: Option<String>,
    workspace: Option<String>,
    /// Stable per-child dir the resumed-session state file lives in — see §4.
    /// `None` disables resume persistence (every activation is fresh).
    state_dir: Option<PathBuf>,
    /// Issue #443: `false` (default) adds `--strict-mcp-config` +
    /// `--setting-sources project` — see §1.
    inherit_user_config: bool,
    /// Issue #443: extra env var NAMES forwarded on top of the fixed
    /// allowlist — see §1/§6.
    forward_env: Vec<String>,
    /// Issue #443: bound on the permission-relay `HostBridge::approval_call`
    /// — see §3. Always `APPROVAL_RELAY_TIMEOUT` (300s) outside tests.
    relay_timeout: Duration,
}

#[async_trait]
impl ChildExecutor for ClaudeCodeExecutor {
    async fn run(&self, spec: RunSpec, events: EventSink,
                 steer: SteerInbox, cancel: CancellationToken) -> ChildOutcome {
        // steer is drained for the whole activation (both possible attempts
        // below) but not acted on — no reliable mid-turn interrupt (§5).
        //
        // §4 activation logic, then `run_once` (spawn → select-loop → §5
        // shutdown) is called once, or twice on a resume-failure retry:
        // 1. messages empty          → delete stale state; fresh spawn.
        // 2. messages non-empty + id → spawn `--resume <id>` with just the
        //                              live assignment.
        // 3. messages non-empty, no id → fallback: rendered history preamble
        //                                + assignment, fresh spawn.
        // 4. a `--resume` spawn that exited with no `result` frame → clear
        //    state, retry ONCE with the fallback body, no `--resume`.
        //
        // Inside each `run_once` attempt, the select-loop:
        //    - stdout line  → parse → events.emit(...)   (§2 table)
        //      `system`/`result` with a session_id → persist state (§4)
        //      control_request → events.emit(NeedsHuman) + park oneshot
        //    - cancel       → §5 shutdown → ChildOutcome::Cancelled
        //    - `result`     → §5 shutdown → ChildOutcome::Completed
    }
}
```

## 8. Config plumbing (issue #443)

`ExecutorSpec::ClaudeCode` (`bamboo-subagent::provision`) carries `binary`,
`model`, `permission_mode`, `inherit_user_config: Option<bool>`, and
`forward_env: Option<Vec<String>>`. Both factory arms that turn a spec into a
running `ClaudeCodeExecutor` (`src/subagent_worker.rs`, `src/broker_agent.rs`)
resolve `None` isolation/env fields to the hardened defaults
(`inherit_user_config.unwrap_or(false)`, `forward_env.unwrap_or_default()`).

Two config surfaces build a `ClaudeCode` spec from `executor = "claude_code"`:

- `bamboo_config::SubagentsConfig` — the built-in local actor worker
  (`subagents.claude_code_binary` / `_model` / `_permission_mode` /
  `_inherit_user_config` / `_forward_env`).
- `bamboo_engine::external_agents::config::ExternalAgentProfile` — a named
  `externalAgents` profile using the actor protocol (same `claude_code_*`
  field names).

Both are resolved into the spec in
`crates/engine/bamboo-engine/src/external_agents/runtime.rs`
(`build_local_actor_runner` and `build_external_child_runner` respectively).
