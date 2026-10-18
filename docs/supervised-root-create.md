# Create an independent supervised Root

`session_control` gains `scope` and `create` only when the Server Host installs
its canonical creation port. A generic SDK/other Host without that port keeps
its existing followup/loop catalog. Trusted Supervisor identity is required in
both Standard and Ultra; ordinary Roots and Children receive no new authority.

`scope` returns the current management `state_revision`, existing
`allowed_projects` and an actionable `project_scope_uninitialized` or
`project_scope_configured` code. Route exposure is distinct from Project scope
and does not claim a particular worker is ready. It observes authority and never grants it. No unrelated session directory
is returned.

`create` takes `request` with `operation_id` (non-nil UUID),
`expected_state_revision`, explicit `project_id`, explicit `workspace_path`,
`message`, and optional `title`. The Project must already be in the Supervisor's
Host-configured scope and active. The existing canonical workspace validator
requires an existing path already bound to that Project, retaining confinement
and ownership checks. A native ExecuteCommand permission check describes this
Project/workspace before the Host service runs. No existing security setting,
Project binding or permission is automatically changed.

The worker is a new Standard Ordinary Root. Its ordinary global defaults and
Project context come from the same pure creation use case as `POST /sessions`;
its initial session permission mode is Default. It inherits no Supervisor
history, private prompt, selected model, workspace, mode or privileges. A bounded
SubAgent creates a Child; a formal Work/Ticket uses the canonical Ticket API.
This entry creates neither a Ticket nor an isolated Git worktree. The Host must
prepare an appropriate workspace before invoking it.

The Host reuses the existing session-create operation store and cross-process
claim. The full source incarnation and request are fingerprinted. A separate
key domain and deterministic UUID bind the reservation to this operation; a
malformed outer receipt cannot select a historical Root. The reservation's
birth is immutable. The canonical lifecycle/Task/sorted Session locks retain
current Supervisor identity and Project scope through the normal Root save
and existing management publication. Only this new lifetime is attached.

The first instruction uses the existing lifetime-fenced Supervisor followup
Inbox admission and activation handshake. Its stable envelope is deduplicated
by that Inbox. The creation receipt records only the outer delivery ID,
generation and activation disposition. Completed retries return historical
admission without requesting another run. Pending recovery reconciles existing
Root/link/Inbox truth; it has no second queue, relationship registry or execution
grant. Caller cancellation does not cancel an already started creation drive.

A result distinguishes the blocked stage and reason, or reports durable creation,
link and first-input admission. Neither admission nor activation is completion.
After an uncertain result retry the entire unchanged request, including its
original revision and UUID. A different payload conflicts. Do not allocate
another operation to recover an uncertain worker. Activation failures retain
the same Root and first-input receipt for exact recovery.

The existing terminal receipt recovery window is 24 hours. Pending truth does
not expire. An expired retry cannot allocate a different UUID or bypass the
monotonic management CAS. Revocation, detached/re-attached link generations,
changed target birth/authority, deletion and replaced Supervisor incarnations
fail closed. Retries never reattach a revoked or unrelated session. Partial
creation/link publication remains visible by its reserved target ID for Host
recovery; no automatic deletion or permission repair is performed.

This slice addresses Bamboo #1058 under #1048/#1968/#2072. Work initialization,
status rendering and existing frontend actions remain separate canonical
Ticket/UI responsibilities; this feature does not remove their gates.
