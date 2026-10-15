# Local Supervisor task client

From a Bamboo source checkout, use Node 22+ to call an **already running**
Bamboo or Bodhi-owned backend. There are no dependencies to install:

```bash
node scripts/bamboo-task.cjs inspect
node scripts/bamboo-task.cjs status --session-id bamboo-default-supervisor
```

The entry point is `scripts/bamboo-task.cjs`, also executable on POSIX. It is
not an added command in a previously installed native `bamboo` binary.
`bamboo -p --stream-json` starts its own in-process runtime; this client uses
HTTP on the selected backend. It creates no scheduler, task database, permission
grant, Work scope or session attachment. Native CLI and A2A adapters can follow.

## Compatibility and access

The default server is `http://127.0.0.1:9562`. Every command accepts
`--server-url http://127.0.0.1:PORT[/api/v1]` and `--timeout-ms N` (default
30000, maximum 600000). Only literal loopback IPv4/IPv6 addresses are accepted;
redirects, URL credentials, queries and arbitrary base paths are rejected.
The client does not read Bamboo configuration, log in, pair a device, load
environment credentials, install a CLI, start a server or change app settings.
An existing access denial is exit 4; use an already authorized local connection.

`inspect` first validates Bamboo's bootstrap envelope, then makes a read-only
GET to a random nonexistent Root's selected-output route, **without** a birth
header. Only the native API's structured 400 (`Exact output requires one
Bamboo-Session-Created-At header`) is a positive detection. This identifies the
Bamboo receipt/run API introduced after fenced HTTP stop (#1951/#1953). It is
feature detection for the supported Bamboo implementation, not authentication
or an attestation of arbitrary servers. Submit/stop repeat this check before
any POST. A bootstrap version of `0.0.0` or a realtime stop capability alone
does not establish this HTTP contract. There is no legacy mutation fallback or
override flag. `inspect` exits 0 for a valid inspection even when
`mutations_supported` is false; check that boolean before submitting.

Read-only MacBook verification on 2026-10-15 found Bodhi AI 2026.10.11 listening
on :9562 with a valid bootstrap, but the selected-output route returned 404.
That installed backend is unsupported for mutations by this client. No live
task was submitted and no provider was invoked in this verification.

## Submit and track

Put a task in a UTF-8 file (maximum 128 KiB), avoiding prompt text in shell
arguments. `--prompt-file -` reads stdin. `/goal` controls are rejected locally.
After verifying the existing provider/account cost boundary, choose one target:

```bash
node scripts/bamboo-task.cjs submit --supervisor --prompt-file /tmp/task.txt
node scripts/bamboo-task.cjs submit --session-id ROOT_ID --prompt-file /tmp/task.txt
node scripts/bamboo-task.cjs submit --new --workspace /absolute/public/fixture \
  --prompt-file /tmp/task.txt --request-id sample-1997-1
```

`--supervisor` uses the owner-only atomic `/supervisor/default` bootstrap. Its
actual incarnation receipt supplies identity; spelling a reserved ID does not
create authority. It preserves existing history and scope configuration.
The ordinary `--session-id` form refuses the reserved default Supervisor ID.
New ordinary Roots can select `--project-id ID` and `--workspace ABSOLUTE_PATH`;
existing sessions keep their workspace. Optional `--model ID --provider ID`
must appear together and explicitly select that pair for the submitted chat.
Omitting them uses existing backend defaults. This does not establish model
availability, pricing or a free allowance.

The success JSON contains `request_id`, `session_id`, `session_created_at`, the
server's `message_id`/`ingress_seq`, and its real `run_id`. There is no invented
task ID. Acceptance is not proof of successful task completion:

| `execution_scope` | Meaning |
|---|---|
| `selected_input` | Ordinary Native Root: chat obtains its server-generated receipt; execute sends that receipt and the Root birth and validates all acknowledgement headers. An existing owner/legacy wait is rejected rather than joined. |
| `session_activation` | Supervisor: chat admits a referenced User input; ordinary execute may start or observe an existing run. The receipt does not prove this input exclusively owns that run. |

New ordinary Roots are explicitly created first so their birth is known before
chat. All writes retain the server's authorization, permission and runtime
guards. A concurrent Root replacement can still accept chat before selected
execute rejects the old birth; the CLI reports partial admission and never
deletes that input. Supervisor activation has the existing session-level
lifetime semantics, not selected-input fencing.

`request_id` defaults to a UUID, or accepts an explicit 1–64-character safe ID.
The create/chat/execute requests use distinct stable Idempotency-Key values.
There are no automatic POST retries. Operator-directed retry requires the same
target, request ID, body and original server process/receipt window; the server
cache is bounded (currently ten minutes) and not durable across restart.
After an uncertain outcome, inspect known IDs first. Repeating a whole submit
after expiry/restart can admit another input; this is not exactly-once delivery.

## Status, visible output and events

```bash
node scripts/bamboo-task.cjs status --session-id SESSION_ID
node scripts/bamboo-task.cjs status --session-id ROOT_ID --run-id RUN_ID \
  --session-created-at ORIGINAL_RFC3339 --include-output
node scripts/bamboo-task.cjs events --session-id SESSION_ID --max-events 100
```

Plain `status` returns session metadata with `observation_scope=session`, omitting
titles, paths and raw errors. Exact ordinary Root status uses the original birth
and run ID, with nanosecond precision preserved. Its replacement snapshot exposes
runner status, visible version/terminal and `history_committed`; text is omitted
unless `--include-output` is explicit. An output terminal does not itself prove
history was saved. Replacement, eviction and restart can end output retention.
HTTP 409 (wrong birth/kind), 410 (unretained/unavailable) and 503 (verification
failure) remain distinct, with no successor/history fallback. The current
backend intentionally rejects this exact-output interface for Supervisor.

Events are bounded NDJSON over **session** SSE. Each record says
`event_scope=session`; run IDs appear only when the original event carries one.
Untagged tokens and Complete replay must never be attributed to a selected run.
`--include-output` adds visible Token content only, not reasoning, tool arguments,
tool results or approval parameters. Defaults omit all those contents.
Subscribe before execution to observe live tokens; later subscriptions can miss
text. This is especially relevant to Supervisor, whose exact visible-output
API is not implemented. Use an existing session's event command in another
terminal before submission. Selected ordinary output can instead be polled.

A stream ends at a session terminal, clarification pause, clean EOF, event limit
(default 1000, maximum 10000), timeout or interrupt. `session_boundary`,
`session_pause`, `event_limit` and `eof` describe only the observer, not goal
completion. A stream has 256 KiB frame and 16 MiB total byte limits. Disconnecting,
timing out or Ctrl-C closes the observer; it never implicitly stops the task.

## Stop and errors

```bash
node scripts/bamboo-task.cjs stop --session-id SESSION_ID --run-id RUN_ID
```

Stop always sends `Bamboo-Run-Id` and validates the exact session/run acknowledgement.
A stale/completed run's HTTP 409 does not cancel a successor. Cancellation
acknowledgement does not prove all producer cleanup has finished. No session
deletion, legacy token fallback or unfenced stop is used.

| Exit | Meaning |
|---|---|
| 0 | The JSON command/stream observation succeeded; task success is separate. |
| 2 | Invalid local arguments or prompt. |
| 3 | Transport/output failure. |
| 4 | HTTP 401/403 access denial. |
| 5 | Other HTTP rejection; `error.http_status` distinguishes 409/410/503. |
| 6 | Unsupported API or malformed/mismatched protocol. |
| 7 | A write was accepted before a later failure, or a write outcome is unknown. |
| 124 / 130 | Read timeout / Ctrl-C. |

Exit 7 takes precedence after accepted/uncertain writes, preserving
`cause_exit_code`, `request_id`, known real IDs, HTTP stage and per-stage admission
states. A lost/malformed acknowledgement is **unknown**, not success. Raw backend
error bodies, prompt text, cookies and credentials are never printed. Default
output is suitable for sanitized evidence; explicitly requested visible text
must be handled as user data.

## Lightweight verification and first real sample

```bash
node --test scripts/bamboo-task.test.cjs
```

The suite drives actual CLI subprocesses against local HTTP/SSE fixtures,
including old-backend refusal, exact receipt headers, Native versus referenced
ingress, partial/uncertain writes, denied bootstrap, successor-safe cancellation,
wrong birth, retention loss, redaction, redirects, UTF-8/CRLF fragmentation,
multiline data, malformed/oversize frames and observer interruption. It invokes
no Rust build, provider, tool runtime or paid model.

For a first real sample, prepare a disposable public directory whose README says
`This fixture contains two fruits: apple and pear.` Ask the Supervisor to delegate
a read-only task: report the README's SHA-256 and the two fruit names with a file
reference; make no edits, builds, commits or network tool calls. Record the
precomputed file hash, actual IDs, stages, exit codes and event types. Do not
approve a permission bypass or broaden any existing Work/session scope.

Before submitting, independently verify a compatible running backend and the
already configured provider/model's availability **and account fee boundary**.
Configured model metadata or a subscription name does not prove a free run.
If that cannot be established without credential setup, authentication, app/MCP
changes or new charges, keep the real sample unrun and retain the blocker.
