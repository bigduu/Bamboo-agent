# Plugin command hooks: phase-one compatibility

Bamboo plugins can declare `provides.hooks` without copying anything to the
user's global `hooks.json`. This is a Bamboo capability declaration, not an
importer for `.claude-plugin/plugin.json` or `.codex-plugin/plugin.json`.
See `crates/infra/bamboo-plugin/examples/portable-hooks` for a minimal bundle.

```json
{"provides":{"hooks":[{"config":"hooks/hooks.json","scripts":["hooks/policy.py"]}]}}
```

Paths must remain inside the installed bundle. Hook bundles reject symlinks
and special files. `scripts` identifies code to inspect; the review digest
covers the **entire bundle**, entry types and Unicode paths (including empty directories), file bytes, Unix
permission bits (including directory traversal permissions; readonly state on other
platforms), plugin id and version. Non-Unicode bundle paths are rejected.
The bundle is capped at 64 MiB, each hook configuration at 64 KiB, each
configuration at 64 commands and each plugin at 16 configurations.
Persistent data is outside the reviewed bundle, at `plugins/.hook-data/<id>`.
Reviewing scripts does not sandbox their commands or pin external interpreters,
PATH binaries, downloaded code or dependencies outside the bundle.

## Review and execution

Installation and every successful update register hooks with no execution
trust. Existing installation journals, rollback and removal retain their
ownership; no independent hook-registration store is created. Incomplete
installations never execute. On every event, Bamboo reloads provenance and
checks the bundle digest against the exact reviewed receipt, again before
each command spawn. Configuration,
script, other bundle bytes, identity or version changes invalidate trust.
Command launch and process cleanup share the existing plugin-operation lock
with installation, review and removal; receipt state is rechecked at each
spawn. Full bundle integrity scans run on blocking workers rather than Tokio
workers. Scheduling hints inspect reviewed registrations and bounded configs,
without hashing the bundle; a changed bundle can conservatively retain the hint
until the spawn check rejects it. Removal persists disabling before cleanup. A command already executing
is bounded by its timeout rather than retroactively cancelled. Commands that
call the same host installation/review API cannot complete that reentrant
operation while they hold the execution boundary; their timeout releases it.
The parsed configuration must also match the config bytes verified under that
lock, so a failed update cannot execute a candidate captured before rollback.
If hooks become active after parallel tool admission, affected calls fail closed
and can be retried through hook-aware admission; sequential calls enforce the
new policy without losing their hook state.

`GET /api/v1/plugins/<id>/hooks` returns a compatibility/review report with the
current `config`, `digest` and state:

| State | Meaning |
| --- | --- |
| `needs-review` | No matching explicit review, or bytes changed |
| `disabled` | Explicitly disabled, or installation is incomplete |
| `active` | Enabled and the current digest matches the review |
| `unsupported` | Strict configuration or bundle validation failed |

Only after the user reviews these exact bytes, an authenticated caller can
send `POST /api/v1/plugins/<id>/hooks/review`:

```json
{"config":"hooks/hooks.json","digest":"<current digest>","enabled":true,"confirm_execution":true}
```

A stale digest or missing execution acknowledgment is rejected. Set `enabled`
to false to disable execution. These routes inherit the existing plugin API's
access-password middleware and installation operation lock. Installing this
example is not consent to run it. Do not enable execution merely to preview it.

## Compatibility report

The current upstream references are [Claude hooks](https://code.claude.com/docs/en/hooks),
[Claude plugin hooks](https://code.claude.com/docs/en/plugins-reference#hooks),
[Codex hooks](https://learn.chatgpt.com/docs/hooks), and
[Codex plugin packaging](https://developers.openai.com/plugins/build/plugins).
This implementation selects their small synchronous command intersection:

| Event | Accepted control/context |
| --- | --- |
| `UserPromptSubmit` | Exit 2 or JSON `decision: "block"` rejects the prompt; plain stdout or hook-specific `additionalContext` supplements context |
| `PreToolUse` | Exit 2 or hook-specific `permissionDecision: "deny"` rejects the tool; `allow` is a no-op; hook-specific context is supported |
| `PostToolUse` | Exit 2 or JSON `decision: "block"` supplies blocking feedback after execution; hook-specific context is supported; side effects cannot be undone |
| `Stop` | Exit 2 or JSON `decision: "block"` requests continuation; ignored when `stop_hook_active` is true, with the existing host continuation ceiling |

Exit 0 with empty stdout is a no-op. Plain stdout for tool/Stop events is ignored. Other exit codes, malformed JSON,
truncated output, timeout and unsupported output shapes are reported as hook
failures and contribute no control or context. Native hooks keep their
existing response interpretation and dispatch behavior. All matched portable
commands execute in declaration order; a rejection is sticky and cannot be
reversed by a later no-op or allow response. Native rejection remains final.

Accepted configuration has optional top-level `description`, an event map
`hooks`, groups with optional `matcher` and a `hooks` array, and handlers with
only `type: "command"`, `command`, and an explicit `timeout` of 1–600 seconds.
Timeouts cover stdin, process execution, descendant pipe drain and cleanup.
Both stdout and stderr use the native executor's 64 KiB capture limit.

Only tool events accept nontrivial matchers. Omitted, empty or `*` matchers
match everything. Other patterns must compile using Rust `regex`; lookaround
(including lookbehind) and backreferences are rejected. Groups first match the
selected Bamboo executor identity. Exact terminal tools also support the bounded
legacy shell projections described below. JSON stdin includes
`session_id`, nullable `transcript_path`, `cwd`, `hook_event_name` and
`permission_mode: "default"` (conservative portable policy; no permission
delegation). Event fields include `prompt`, or `tool_name`, `tool_use_id`,
executor `tool_input` (or the documented shell projection) and post-execution
`tool_response`, or
`stop_hook_active` and `last_assistant_message`. Tool response is Bamboo's
model-facing result text. No synthetic Claude `Edit` input is produced for
`apply_patch` or other tools.

Commands receive `PLUGIN_ROOT`, `PLUGIN_DATA`, `CLAUDE_PLUGIN_ROOT` and
`CLAUDE_PLUGIN_DATA` as environment variables. Quote variable expansions to
handle spaces. Commands run in the session workspace using Bamboo's existing
preferred shell and the same prepared login-shell/config environment as native
commands, with plugin root/data variables applied afterward. The first version does not support per-platform commands.

JSON output accepts only top-level `decision`/`reason` and
`hookSpecificOutput`. The latter accepts `hookEventName`, `additionalContext`,
and PreToolUse-only `permissionDecision`/`permissionDecisionReason`.
Event names and control combinations are checked. Rejecting decisions require
a reason. An allow result never creates a Bamboo permission override.

Supplemental context including provenance headers shares an 8 KiB UTF-8-safe
budget per seam across all
matched plugins/commands. It enters the existing `AgentHookContext` block and chronological model-context
ledger with plugin id/version/config provenance, without inserting conversation
messages into unfinished tool groups. The current snapshot retains at most
8 KiB of complete, sourced chunks; older ledger events follow existing budget
and epoch retention. Projected events remain compressible. It never enters the native
System injection path, never acquires `never_compress`, and creates no memory
service. Blocked prompt context is discarded. Jiandu native recall is unchanged.

Excluded: SessionStart/SessionEnd (Bamboo currently fires these per run), all
other events, async/asyncRewake, MCP/prompt/agent/HTTP handlers, `ask`, input
rewrites, persistent permission updates, env-file, executable handler objects,
`statusMessage`, `additionalContextLimit`, `systemMessage`, `continue`,
`suppressOutput`, and full upstream manifest import/export. Unsupported fields
are rejected instead of silently discarded. Raw stderr and context may contain
plugin-provided text; treat them as untrusted data.

### Terminal matcher compatibility

Portable tool groups first match the exact executor identity selected by Bamboo's
callable-set resolver. A group matching that identity, including an omitted,
empty or `*` matcher, executes once with the actual name and arguments. Provider
aliases resolve before this check; an exact custom registration such as
`vendor::exec_command` is not treated as the builtin shell tool.

If a group does not match the actual name, exact builtin `exec_command` and
`write_stdin` calls also check these legacy shell projections for both PreToolUse
and PostToolUse:

| Actual operation | Projected `tool_name` | Projected `tool_input` |
| --- | --- | --- |
| `exec_command` | `Bash` | Rename `cmd` to `command`; retain other arguments except the transport fields below |
| `write_stdin` with nonempty `chars` or `eof: true` | `BashInput` | Rename `session_id` to `bash_id`, rename `chars` to `input`, and set `append_newline: false`; retain true `eof` |
| `write_stdin` with exactly the lone Ctrl-C character (`\u0003`) | `KillShell` | Rename `session_id` to `bash_id`; omit `chars` and `eof` (Ctrl-C takes precedence over EOF) |
| `write_stdin` with empty/omitted `chars` and no true `eof` | `BashOutput` | Rename `session_id` to `bash_id`; omit `chars` and `eof` |

Every projection removes `yield_time_ms` and `max_output_tokens`. Missing
`chars` becomes empty input for an EOF projection; false `eof` is omitted.
An input/EOF or Ctrl-C continuation additionally has a `BashOutput` projection
containing only `bash_id`, since it also reads output. A legacy-only group runs
once for each matching projection, in primary-operation then output order. Thus
`^(BashInput|BashOutput)$` can execute its commands twice for one input call;
a group already matching `write_stdin` executes once instead.

These projected names and arguments are confined to Hook matching and Hook
stdin. Execution, tool results, events and stored transcripts keep their actual
identities and data. PostToolUse uses the identity and parsed input captured for
execution; its `tool_response` remains the actual result, including terminal
session JSON. Other tools retain their executor arguments; `apply_patch` is not
reshaped into Claude Edit input.

Every projected command still requires the exact reviewed bundle and trust
receipt, rechecked under the existing plugin-operation lock before each spawn.
All applicable portable denials remain sticky, including output-side denial of
a combined continuation after a native Ask. Portable `allow` never grants
permission, and portable `ask` remains unsupported. Reviewers of legacy shell
matchers must therefore review their commands for these terminal operations and
projected inputs as well as explicit calls to the legacy tools.

Persistent hook data uses the reserved `.hook-data` namespace, which cannot be
accepted as a plugin id. A valid plugin named `data` remains independent of that
storage; uninstalling it cannot remove another plugin's hook state.

The shared engine entry runs portable UserPromptSubmit before session setup and
provider calls, including headless/embedded runs. The server submission seam
still checks before persisting the user message and records an exact-prompt,
once-consumed fingerprint to prevent a second execution in the engine. Native
UserPromptSubmit invocation remains at the existing server seam.
Preappended, structured runtime-resume messages (such as child-completion or
retry notifications) do not represent new user submissions and skip this
fallback. Genuine follow-up user messages still run it.
