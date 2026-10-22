# Authorized Session directory and persisted progress (B1)

Issue [#2148](https://github.com/bigduu/Bamboo-agent/issues/2148), a bounded child
of [#1071](https://github.com/bigduu/Bamboo-agent/issues/1071) and #1048. Base is
actual dev `d694346aa42a688f204bb8e9fd50e573790361a2`. This is independent of the
goal-prompt change #2146. Related authority and receipt work: #1968, #1976,
#2000, #2072 and #2091; merged #2073/#2075/#2092 remain dependencies.

`SessionDiscoveryPort` is a Host-installed read capability, shared by the
`session_history` tool's `list` and `inspect` actions. The port accepts the
original `ToolCtx`; it never takes a caller role, grant or Supervisor ID from
model arguments. `SessionInspectorTool::authorized` exposes only these two
actions and never falls back to the ordinary legacy global-index viewer.

The first compatibility adapter is `SupervisorSessionDiscovery`, using the
current canonical Supervisor relationships. An Ordinary Root or Child requires
a separate explicit Host authorization adapter. This slice does not install
that adapter, mint grants, broaden permissions, attach Sessions or claim that
all callers have already been enabled. A's independent caller-authority work
supplies that later integration; supervision duties remain a separate concern.

## Requests and observations

```json
{"action":"list","limit":10,"query":"project-a"}
```

The result contains `caller_session_id`, `scope_revision`, `sessions` and
`next_cursor`. Each Session entry contains only `session_id`, `project_id`,
`created_at`, `metadata_version` and `link_revision`. Directory entries do not
contain titles, prompts, model settings, history or workspaces. A query matches
target ID or Project, case insensitively. The default and maximum page size is
20; query and cursor byte limits are 128 and 4096.

Supply the returned cursor with the same query and limit. The cursor binds the
caller incarnation/birth/tool revision/Session contract and management revision.
It is a continuation locator, never authorization. Current authority is checked
again on every request, including a cursor recovered after process restart.

```json
{"action":"inspect","session_id":"worker-a","max_chars":2000}
```

The result reports `source_updated_at`, `observed_at`, `last_persisted_status`,
`has_pending_question`, bounded title, task counts and an optional
`latest_visible_assistant_excerpt`. The excerpt contains content only, excluding
reasoning, hidden messages, tool arguments, image URLs and approval text. Its
default character budget is 2000 and maximum is 8000. This is a Session-scoped
transcript excerpt; it can be commentary or an earlier answer.

`status_is_live`, `input_specific` and `completion_verified` are **false** for
this persisted observation. A stored `completed` status does not establish
current owner termination, acceptance of a particular input or successful task
completion. No run ID is inferred. A pending question remains unanswered.

## Authorization and consistency

The adapter reads the caller's strict `load_root_authority` snapshot, whose
bounded management state has at most 256 links including tombstones. For every
enabled candidate it calls `inspect_supervisor_link`, reusing the existing
canonical lifecycle/Task/ordered Session locks, Project validation, target
birth and metadata checks. It rejects changed scope/reference/link observations
and corrupted authority. Only authorized entries are sorted and paginated.
No global-index scan or peer transcript read is used for directory discovery.

The adapter rechecks the caller binding after candidate observations. A page is
a set of independently authorized read observations, not a simultaneous live
snapshot of every target. Target progress may change between pages. A returned
link revision is not a durable read/write capability; every later operation
must reauthorize its target.

`inspect` uses the existing `load_supervisor_managed_session` reader, retaining
caller/link/Project/birth authority through the complete physical target read.
It does not perform a separate authorization preflight followed by an ordinary
unfenced history load. Existing bounded `read_messages`/`get_meta` remain
available to the compatibility Supervisor viewer.

## Failures and follow-up

| Reason prefix | Meaning and recovery |
| --- | --- |
| `session_discovery_invalid_request` | Malformed/beyond-bound query, cursor, target or budget; correct the request. |
| `session_discovery_authority_required` | The original executing caller is missing or does not match the installed adapter. No global directory fallback. |
| `session_discovery_authority_unavailable` | Strict caller/target authority cannot be established, including corrupt or unsupported storage and denied target reads. No success or empty-result inference. |
| `session_discovery_cursor_stale` | Caller, scope, relationship or query changed; discard the cursor and start a new page. |

This slice sends no messages and controls no runs. Existing #2000 Inbox and
`session_control followup` retain operation/message identity and admission
receipts; #1976 retains exact-run Loop control. Later role-neutral send/control
adapters must preserve those final durable fences. Delivery receipts never
mean consumption or completion. There is no second Inbox, manager or runtime
state writer here.

## Validation status

`tests/session_discovery.rs` adds nine synthetic persistence/tool cases for
pagination/filter/restart, stale cursor/scope, caller forgery/replacement,
target Project/birth/revoke, corrupt authority, Unicode/hidden-content bounds,
schema/execution request constraints, actual provider schema normalization and
ordinary-adapter denial with no global fallback. Provider-visible parameters
retain top-level `type`, `properties` and `required` because OpenAI transports
strip top-level `oneOf`; the complete schema and execution still reject mixed
action fields. The existing scoped-history mode/restart case now expects authorized
directory access. Fixtures use temporary Session stores and no provider.

The initial source freeze has only direct `rustfmt` parsing/format checks,
`git diff --check` and wire-fixture consistency evidence. **Cargo, Clippy and
Rust tests are unrun under the parent's single-window restriction.** The JSON
wire fixture is illustrative protocol data, not a handler execution receipt.
Current-head runtime checks, required dev compilation and applicable review
remain mandatory before normal admission. Keep the PR Draft until those gates
are met; do not report this source slice as end-to-end delivery.
