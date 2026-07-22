# Codex executor

`subagents.executor = "codex"` supports two transports. `codex_mode = "exec"`
(the backward-compatible default) runs one `codex exec --json` process per
activation. `codex_mode = "app_server"` keeps `codex app-server` alive and
relays interactive command/file approvals through the parent Bamboo approval
chain.

The minimum supported version is **Codex CLI 0.144.0**. Bamboo verifies the
installed version and the required `exec --json`, stdin prompt,
`--output-last-message`, `--config`, sandbox/danger flags, and
`exec resume --json` surfaces before a worker starts. The implementation and
live Bamboo-provider test are verified against 0.144.5. Codex 0.144 removed the
custom-provider Chat Completions wire, so `codex_wire_api` only accepts
`"responses"`.

The Lotus Sub-agents settings card exposes the same fields and validates the
pending patch through `POST /bamboo/config/validate` before saving. Its Detect
button calls `POST /bamboo/config/codex/detect` with an optional
`{"binary":"...","mode":"app_server"}` override. The response is the resolved `path` and
`version`; the endpoint and worker share one discovery implementation, so a
successful detection cannot bypass the worker's version/capability preflight.

## Transport contract

| Concern | Codex executor behavior |
|---|---|
| Process lifetime | Exec mode: one process group per activation. App-server mode: one long-lived process per warm worker, with logical threads isolated by Bamboo session id. |
| Prompt transport | The assignment is written to stdin (`-`), never placed in argv. |
| Output | `--json --color never` JSONL plus a bounded `--output-last-message` fallback file. |
| Model | `codex_model`, when set, becomes an explicit `--model`; otherwise the flag is omitted. |
| Repository guard | A real Git workspace needs no override. `--skip-git-repo-check` is allowed only for a Bamboo-owned non-Git workspace. |
| Cancellation | Bamboo snapshots the full descendant tree, sends SIGTERM deepest-first plus to the Codex process group, then escalates every survivor to SIGKILL after a bounded grace period. This also covers tool commands that create a new process group. |

In `app_server` mode, Bamboo speaks newline-delimited JSON-RPC over stdio:
`initialize`/`initialized`, `thread/start` or `thread/resume`, then
`turn/start`. A warm worker retains the process, while a session-keyed state
map prevents one logical child from inheriting another child's thread. Resume
failure starts one new thread with the same bounded history rehydration used by
exec mode. `turn/steer` handles live parent messages and cancellation first
requests `turn/interrupt`, then uses the standard process-group TERM/KILL
ladder if the server does not finish within the grace period.

`item/commandExecution/requestApproval` and
`item/fileChange/requestApproval` are converted to Bamboo `Bash` and
`ApplyPatch` approval calls. Missing bridges, relay errors, and the 300-second
deadline all answer `decline`; approval answers `accept`. Legacy
`execCommandApproval`/`applyPatchApproval` requests remain compatible. There
is no silent fallback to exec mode when app-server capability detection fails.

## Event mapping

| Codex JSONL event | Bamboo output |
|---|---|
| `thread.started` | Persists the native thread id and emits runner metadata including binary, version, model, auth/home mode, sandbox, approval policy, and forwarded environment names. |
| `turn.started` | `runner_progress` for round 1. |
| `item.started/updated/completed` | Agent text becomes token deltas; reasoning becomes reasoning deltas; command/MCP/web items become Bamboo tool start/output/complete/error events. |
| `turn.completed` | Captures input/output token usage and emits Bamboo `complete`. |
| `turn.failed` / `error` | A bounded error outcome with the Codex message and stderr tail. |

Unknown top-level or item event types are ignored rather than failing the run,
so additive Codex schemas are tolerated. Bamboo raises the minimum version or
changes the capability checks only when a required flag or event contract
changes; CI fixtures cover known events and the ignored real-machine suite is
the version-drift merge gate.

## Authentication and billing modes

`codex_auth_mode` has four values. Unset defaults to `"bamboo"`; inheriting a
personal Codex login is deliberately opt-in.

| Mode | `CODEX_HOME` and credential | Provider and billing | Secret boundary |
|---|---|---|---|
| `inherit` | Leaves `CODEX_HOME` unset, so Codex uses the invoking user's `~/.codex/config.toml` and `auth.json`. | The user's configured provider; a ChatGPT login uses that user's subscription. | Inherits the user's full Codex configuration, so use only when that ambient authority is intended. |
| `api_key` | Uses `<child-state>/codex-home`; `OPENAI_API_KEY` must be named explicitly in `codex_forward_env`. | OpenAI API billing for that key. | Bamboo never forwards `OPENAI_API_KEY` implicitly. The key is process-environment only. |
| `custom` | Uses the isolated home and a generated `model_providers.custom` entry. The key is selected by `codex_provider_key_ref`. | The configured third-party/proxy provider and its billing policy. | `config.toml` contains only `env_key = "BAMBOO_CODEX_PROVIDER_KEY"`; the referenced key is injected into that environment variable and never written to disk. |
| `bamboo` | Uses the isolated home and a generated `model_providers.bamboo` entry pointing at the parent loopback `/openai/v1` surface. | The parent Bamboo provider/routing configuration; recommended default. | The parent mints a fresh `bcx1_` token for each activation, binds it to the child session and Responses/models paths, and revokes it on every exit path. In app-server mode Codex command-auth reads that token from a Bamboo-owned `0600` file on demand; the file is cleared at turn end, so the long-lived process never freezes a revoked token in its environment. No upstream provider key reaches Codex. |

Because `bamboo` deliberately targets the parent through `127.0.0.1`, it
requires local actor placement. A remote worker must use `custom` with an
explicit provider URL reachable from that worker; Bamboo rejects the ambiguous
loopback combination before issuing a run token.

### Examples

Recommended parent-routed mode (also the default when the mode is absent):

```json
{
  "subagents": {
    "executor": "codex",
    "codex_auth_mode": "bamboo",
    "codex_model": "gpt-5.4"
  }
}
```

Interactive parent approval relay:

```json
{
  "subagents": {
    "executor": "codex",
    "codex_mode": "app_server",
    "codex_approval_policy": "on-request",
    "codex_auth_mode": "bamboo"
  }
}
```

Use the logged-in user's subscription and personal Codex configuration:

```json
{
  "subagents": {
    "executor": "codex",
    "codex_auth_mode": "inherit"
  }
}
```

Isolated OpenAI API-key billing requires the explicit forwarding opt-in:

```json
{
  "subagents": {
    "executor": "codex",
    "codex_auth_mode": "api_key",
    "codex_forward_env": ["OPENAI_API_KEY"]
  }
}
```

Custom provider credentials use an existing Bamboo credential reference:

```json
{
  "subagents": {
    "executor": "codex",
    "codex_auth_mode": "custom",
    "codex_base_url": "https://provider.example/v1",
    "codex_wire_api": "responses",
    "codex_provider_key_ref": "provider.openai.api_key"
  }
}
```

`codex_base_url` must be an absolute HTTP(S) URL without embedded credentials,
query parameters, or a fragment. It and `codex_provider_key_ref` are valid only
in `custom` mode. `OPENAI_API_KEY` is valid in `codex_forward_env` only in
`api_key` mode. Unknown modes and wire protocols fail settings validation.

## Sandbox and approval policy

`codex exec` has no interactive approval relay. Bamboo therefore resolves both
permission knobs before spawn and never relies on the CLI's implicit defaults:

| Child posture | Effective Codex invocation |
|---|---|
| default / restricted | `--sandbox workspace-write --config approval_policy="never"` |
| read-only / research / guardian | `--sandbox read-only --config approval_policy="never"` |
| bypass parent | `--full-auto`, which remains workspace-sandboxed |
| workspace network enabled | the workspace-write flags plus `--config sandbox_workspace_write.network_access=true` |

`codex_sandbox` can explicitly select `read-only`, `workspace-write`, or
`danger-full-access`. In exec mode, `codex_approval_policy` accepts only
`never` and `on-failure`; `on-request` is rejected with an instruction to use
app-server. In app-server mode, unset or `on-request` is accepted and every
other policy is rejected, so config cannot silently remove the relay.
`codex_network_access` applies only to workspace-write. The same fields are
available globally under `subagents` and per named `ExternalAgentProfile`.

Disabling the OS sandbox is double-gated. A `danger-full-access` request becomes
`--dangerously-bypass-approvals-and-sandbox` only when the child session's
parent is currently in bypass mode and `codex_allow_danger_bypass` is true.
Otherwise it is downgraded to `--full-auto` with an audit warning. Root workers
are always downgraded. A successful danger bypass emits a separate, loud
warning event as well as the bootstrap policy metadata.

For a non-Git workspace, Bamboo adds `--skip-git-repo-check` only when the
directory is under Bamboo's configured workspace root or a
`<project>/.bamboo/worktree/...` directory carrying the matching Bamboo
lifecycle ownership marker. Merely imitating that directory shape is not
enough. An arbitrary user-selected directory keeps Codex's repository check.

## Session identity and resume

Codex transcripts are machine-local. On every `thread.started`, Bamboo writes
the newest thread id atomically to `<child-state>/codex-session.json`:

```json
{
  "thread_id": "...",
  "workspace": "...",
  "codex_home_mode": "isolated",
  "updated_at": "2026-..."
}
```

`codex_home_mode` is `inherit` when Codex uses the invoking user's home and
`isolated` when it uses `<child-state>/codex-home`. A persisted id is usable
only from the same workspace and home mode; changing either falls back safely
instead of trying to resume a transcript Codex cannot see.

Activation follows four bounded branches:

1. Empty `RunSpec.messages` means a fresh activation. Bamboo deletes stale
   state before spawning, so rerun never resumes accidentally.
2. Non-empty messages plus a usable id invokes
   `codex exec ... resume <thread_id> -` and sends only the live assignment.
   Any new id from the resumed process replaces the prior state atomically.
3. Without a usable id, Bamboo prepends a shared, role-tagged history preamble.
   The newest ~40 messages are kept within ~24k characters, older entries are
   dropped with an explicit truncation note, and the trailing current user
   message is excluded to avoid duplication under `## Current task`.
4. If a resume process exits before either `turn.started` or `turn.completed`,
   Bamboo clears the bad id and retries exactly once as a fresh process with
   that fallback history. Failures after turn progress and failures of the
   fallback attempt are not retried.

The history renderer and atomic JSON replacement live in `bamboo-subagent` and
are shared with `ClaudeCodeExecutor`, keeping both adapters' fallback behavior
identical. Mid-turn steering, multimodal input, and cross-machine resume remain
out of scope.

## Isolation and environment policy

Every child starts after `env_clear()`. Bamboo restores only `HOME`, `PATH`,
`SHELL`, `TERM`, `LANG`, `LC_*`, `TMPDIR`, `USER`, and `LOGNAME`, followed by
validated `codex_forward_env` names. `CODEX_*` variables and
`BAMBOO_CODEX_PROVIDER_KEY` cannot be supplied through that escape hatch, so a
parent/nested Codex sentinel cannot leak into the child and callers cannot
replace Bamboo's managed provider credential.

For `api_key`, `custom`, and `bamboo`, Bamboo creates
`<child-state>/codex-home` with mode `0700`, removes any stale `auth.json`, and
writes a minimal `config.toml` with mode `0600`. Generated provider config names
the environment variable but never includes a key or run token. The executor
also passes `--ignore-rules`; `inherit` intentionally does neither and leaves
`CODEX_HOME` unset.

The process owns its own process group. Cancellation snapshots its descendant
tree, sends SIGTERM deepest-first and to the whole group, waits five seconds,
then escalates every survivor to SIGKILL. Tracking descendants separately is
required because a Codex tool shell may create its own process group; a
group-only signal would otherwise let that tool survive after Codex exits.

## Differences from the Claude Code executor

| Area | Codex | Claude Code |
|---|---|---|
| Activation | New `codex exec` process each turn; resume uses a persisted thread id. | Stream-JSON process per activation with Claude's session id and `--resume`. |
| Runtime approvals | No interactive approval relay in v1; only `never` and `on-failure` are accepted. | Permission prompts can be relayed over stdio. |
| Safety boundary | OS sandbox is the primary guard and unsandboxed mode is double-gated. | Claude permission mode plus Bamboo's permission relay is primary. |
| Personal config | Explicit `inherit` auth mode; isolated modes create a minimal `CODEX_HOME`. | Controlled by `claude_code_inherit_user_config`. |
| Parent provider | Scoped per-run Bamboo token can route Codex through `/openai/v1`. | Provider auth is controlled by Claude CLI login or explicitly forwarded environment names. |

## Bamboo-as-provider token and observability

The parent actor runner mints a token at the activation boundary, not when a
warm worker is provisioned. The token travels only in `RunSpec.secrets`, whose
debug representation is redacted. A drop guard revokes it after success,
error, cancellation, dispatch failure, or retry exhaustion.

The HTTP gate recognizes `bcx1_` credentials before the ordinary loopback
bypass. A valid token may call only `/openai/v1/responses` and
`/openai/v1/models`; revoked, expired, or out-of-scope credentials return 401
even from `127.0.0.1`. The bound child session becomes the upstream
`LLMRequestOptions.session_id`, while the normal OpenAI-compat forward metrics
record the request and outcome in the parent.

## Verification

Run the deterministic executor and security tests normally with `cargo test`.
Real-machine tests are ignored in routine CI because they require an
installed external binary:

```sh
# User-login/inherit smoke and workspace sandbox tests
cargo test --test e2e_codex_cli_manual -- --ignored --nocapture

# Live Codex -> Bamboo Responses path, metrics, and post-run revocation
cargo test -p bamboo-server \
  live_bamboo_codex_completes_records_metrics_and_rejects_revoked_token \
  --lib -- --ignored --nocapture
```

The consolidated checklist covered by those commands is:

1. Shared discovery reports the resolved path/version and an actionable missing-binary error.
2. A fresh temporary Git-repository run completes with final text, token usage, and bootstrap metadata.
3. A second activation resumes the native thread and recalls a nonce absent from fallback history.
4. `workspace-write` permits an in-workspace write and blocks an out-of-workspace write with a tool error.
5. `bamboo` auth completes through the parent `/openai/v1`, records parent metrics/session masking, and rejects the revoked scoped token.
6. Cancellation removes a live descendant process group, returns `Cancelled`, and the same session subsequently resumes successfully.
