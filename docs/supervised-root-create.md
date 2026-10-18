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

The Server provides `GET /api/v1/supervisor/default/scope` and
`PUT /api/v1/supervisor/default/scope`. The existing owner-read transport
boundary rejects run-scoped Worker credentials and unauthenticated remote
requests; locality may still bypass that read gate. **This is not permission
to update scope.** PUT additionally requires exact owner consent verified by
the trusted embedding Host. Neither endpoint is a model tool. GET does not
open, promote or repair a Supervisor; an absent identity requires the existing
explicit open action.

The default Server has no mutation authorizer and returns 503
`host_owner_authorization_unavailable`. GET reports
`owner_authorization: {available, required: true, kind: "trusted_host_intent",
http_registration: false}`. Contract-version capabilities advertise the API
shape, not an available owner channel or worker readiness.

Rust embedding code can call
`AppState::with_supervisor_project_scope_authorizer` before serving requests.
Its `SupervisorProjectScopeOwnerAuthorizer` verifies owner consent for the
entire typed `SupervisorManagementRequest`: current Supervisor incarnation,
expected revision and complete desired Project set. The Host must obtain this
proof outside the HTTP control plane and must not approve by locality,
run/device token or ordinary access cookie alone. Existing pairing/password
administration has local-bypass behavior, so accepting its credentials alone
would permit a downgrade. Bamboo registers no HTTP/config/model mechanism to
install the verifier or mint its proof, and adds no credential/grant registry.
An installed verifier refusing a request returns 403
`host_owner_authorization_required` without mutating scope.

The trusted in-process `SupervisorSessionService::configure_project_scope`
remains available to an embedding Host. Wiring an actual Bodhi native owner
consent channel is a separately required consumer step; this backend does not
silently create credentials, change security settings or treat a test-native
approval as a production authentication implementation.

GET returns `scope: {supervisor, state_revision, allowed_projects}`, a
`project_scope_uninitialized` or `project_scope_configured` code, and
`capabilities.project_scope_v1` / `supervised_root_create_v1`. The observation
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

## Bodhi consumer integration boundary (#2072)

Read-only inventory on 2026-10-18 found installed Bodhi **2026.10.17** and its
external Lotus receipt **2026.10.15**, source
`58920d5b58b98847ef10f70e60b8f8c23afb9df4`, receipt SHA-256
`1f198f4e3f244c383e62f08c8b319f1fee60600ff52e8b5f7638a43cea90882b`.
That installed consumer is not claimed to contain this API or the newer UI.
The frontend owner's local candidate is
`8831769170784d85ea23c1ac9f834a48890de19c`; it is not pushed or published by
this backend slice. Its current Work v1 contract requires no additional field.

The next actual Bodhi integration must record the native backend source and
the external frontend receipt independently. Use a separate temporary data
directory, isolated existing workspace, loopback server and a deterministic
in-process provider; never use the user's Project grants, configuration,
history or a paid model. Keep the installed app untouched.

1. Record whether the actual Host has installed its native owner-consent
   verifier. An unavailable channel must stay explicitly blocked; no cookie,
   device setup or anonymous loopback call may substitute for it. Reproduce
   empty management scope and show the explicit owner selection route. Querying scope, opening Supervisor and initializing Work must leave
   the management set unchanged. Cover missing/ordinary identity and
   authenticated versus refused owner access.
2. Explicitly select one existing active Project through the actual Host owner
   channel and PUT, binding consent to its observed incarnation, management
   revision and entire Project set. In Standard and Ultra,
   issue the actual Host-wired `session_control create` under its separate
   native approval. Verify a visible independent Ordinary Root, exact Project
   and workspace, one managed link and one initial Inbox receipt. Retry and
   restart with the same full operation request; verify no duplicate Root or
   initial input and preserve all no-inheritance assertions.
3. Exercise formal Work states independently in the real side panel: ready
   with zero items, uninitialized, disabled, forbidden/mode required,
   binding mismatch, recovery required and I/O failure. Offer initialization
   only on a valid owner response; send the three captured fields once. On
   conflict/unknown outcome re-read canonical state without automatically
   repeating the POST. The present Standard Work mode gate remains unchanged;
   report `supervisor_mode_required` rather than claiming universal readiness.
4. Revoke management Project scope and verify creation/followup refusal with
   no worker, link, Work or permission side effects. Regrant must not revive
   links. An unknown grant outcome requires a canonical read and explicit owner
   decision. Tool/model guidance must report the requested route's blocker;
   a read-only Child is not proof of an independent implementation.

Native HTTP/model contract tests prove the backend composition in temporary
fixtures. They do not prove the installed Bodhi UI, real model routing,
publication or a completed implementation task. The owner selection UI, actual native Host consent verifier,
consumer integration and any release remain separately pending.
