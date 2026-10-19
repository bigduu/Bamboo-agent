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

## Explicit Server owner Project authorization

A normal browser and Lotus inside Bodhi use the same Bamboo
`GET /api/v1/supervisor/default/scope` and protected single
`PUT /api/v1/supervisor/default/scope`. Bodhi owns windows/process lifetime and
communication safety; it does not validate Project scope, interpret CAS or
provide business approval. A native dialog is not required for browser use.

PUT verifies the existing password-login cookie or device credential against
current durable AccessControl, independently of locality. Open/local access,
Worker/run tokens and frontend `approved` claims do not authenticate an owner.
No credential is enrolled and no security setting is changed by this operation.
The browser sends `application/json` and its exact backend Origin, or an exact
origin already explicitly configured in the existing CORS allowlist. General
loopback-development CORS and SameSite alone do not authorize the mutation.

The owner reviews the complete Project set and sends one PUT with the observed
incarnation/revision. There is no prepare endpoint, extra proof credential or
multi-round approval protocol. Disable automatic PUT retry; a lost/unknown
result requires a canonical read and a new explicit decision. Existing
Project/identity checks and storage CAS remain authoritative in Bamboo.

GET advertises `capabilities.project_scope_http_write_v1` and
`owner_authorization: {available, required: true, kind: "authenticated_owner",
http_registration: false, code}`. `available` reflects verified credentials on
this request, not `local_bypass`. Codes distinguish
`owner_authentication_required` (401 on PUT),
`owner_authentication_unconfigured` and `owner_authentication_unavailable`
(503). Invalid Web request Origin is 403 `owner_request_origin_invalid`.
An unconfigured real server needs the user to use the existing owner setup/login
flow; tests must not silently configure credentials or claim injected subjects
prove real browser authentication.

Trusted Rust embeddings may retain an explicitly installed exact-intent
`AppState::with_supervisor_project_scope_authorizer`. Ordinary browser and shell
clients do not require that optional port or the native channel. Its rejection
remains a rejection; the Web path does not bypass a custom Host verifier.

GET returns `scope: {supervisor, state_revision, allowed_projects}`, a
`project_scope_uninitialized` or `project_scope_configured` code, and
`capabilities.project_scope_v1` / `supervised_root_create_v1` / `project_scope_http_write_v1`. The observation
contains no target directory, history, credentials or permission configuration.

After explicit consent through the installed Host channel, PUT accepts exactly:

```json
{
  "incarnation_id": "the observed canonical Supervisor incarnation UUID",
  "expected_state_revision": 0,
  "allowed_projects": ["an-existing-active-project-id"]
}
```

This replaces the complete set, bounded to 64 typed Project IDs. Unknown,
archived or unreadable Projects are rejected before the existing canonical
management CAS. Removing a Project disables its links; regranting it never
revives the disabled links or attaches a historical Root. The original
Supervisor lifetime, management fences, proof publication and revision rules
remain in force. Workspace ownership is checked separately by Root creation.

A success returns `receipt` with the exact Supervisor reference,
`state_revision`, `changed` and `link_revision`. A fresh already-satisfied
desired set is a no-op. A stale revision returns 409
`scope_revision_conflict`, including when the desired set has since been
achieved. The owner must read again and explicitly confirm the complete set;
the client must not silently replay a stale write or add Projects. This scope
CAS retry rule is separate from the unchanged-request replay of an uncertain
Root creation operation.

GET and PUT both return 404 `supervisor_missing` before bootstrap without
creating an identity. Other refusals include 403 `supervisor_identity_mismatch`, 400
`project_scope_invalid`, 404 `supervisor_missing` / `project_unavailable`, 409
`project_archived`, and 503 `project_store_unavailable`,
`scope_authority_invalid` or `scope_unavailable`. Existing owner authentication
refusals and malformed JSON use the existing HTTP error boundary. No error
authorizes fallback to ordinary Session save, permission changes or storage
repair.

The model's `session_control scope` names this explicit owner action as
`model_callable: false`. Its Work projection is `availability: not_observed`:
Project scope does not imply that formal Work is ready. The owner reads
`GET /api/v1/tickets/scope` separately and offers initialization only from that
endpoint's `can_initialize=true` and exact `initialization` payload.
`binding_revision` is a Work authority revision; `state_revision` is the
management CAS revision. They are not interchangeable. Neither operation
grants shell/build permissions, binds a workspace, creates a worktree or
changes a feature/mode setting.

## Shared browser and shell acceptance (#2072)

Freeze and record Bamboo source/binary, Lotus resources and any shell version
independently. Preserve earlier native-only test/failed-trial receipts as
historical evidence; they do not prove the unified Web path. Use temporary
projects and data, no paid model, and keep the installed app untouched.

1. Run Lotus in a normal browser without Tauri globals. Read the exact Supervisor,
   revision and Project catalog. An existing authenticated owner uses one full-set
   preview and one PUT; verify its receipt, fresh canonical scope and restart
   persistence. The same UI/API is used inside Bodhi.
2. Reject local/open-only, missing/revoked/run credentials and wrong/missing Origin
   without scope or task side effects. Test-host injected principals prove those
   code paths only; report real owner login/configuration gaps separately.
3. Reject stale incarnation/revision, archived/missing Projects and unknown claims.
   Never auto-retry a conflict or uncertain result. Removing Projects revokes
   links; regrant never revives historical links.
4. Scope does not initialize Work or grant execution/tool permissions. The
   separate existing Root-creation and Work contracts remain in force.

The authenticated HTTP composition and browser integration require their own
current-source acceptance. Optional native channels, installed-app changes,
model routing and publication remain separate; they are not prerequisites for
normal-browser scope management.
