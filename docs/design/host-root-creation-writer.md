# Host-scoped Root creation writer

Part of #2162, after #2169. This slice provides the canonical Root filesystem
writer for a current, exact Host creation permit. It installs no grant and does
not yet connect the application creation service or first-input Inbox purpose.
Those are mandatory before claiming that ordinary callers can complete spawn.
All Rust tests and compilation remain **UNRUN**.

Ordinary/Child reservation keys use immutable caller ID/birth and operation UUID,
with a separate `host-root:v1` hash domain. A genuine Supervisor retains its
existing `supervised-root:v1` ID/incarnation/operation address so changing from
legacy authorization to an installed Host scope cannot allocate a second Root.
The application must retain that route's existing complete-request fingerprint
and first-input message identity too. Rotating grant ID, issuer, scope
revision, conversation ledger, mode and workspace cannot select a second UUID.
The existing application operation store must freeze the complete request,
fingerprint, reserved birth and UUID before calling this writer; a key or receipt
is never authority by itself.

The final boundary retains the actual current installed scope, validates its
exact operation/Project/workspace permit, then uses the shared publication →
lifecycle → Task → sorted Session → sorted tree set, including the reserved
Root. It rereads the original caller and complete generation under the set.
Root, Child and Supervisor use identical Host checks. No Supervisor identity,
management link or same-tree authorization is fabricated.

The new Root uses the same canonical candidate check and default writer as
existing supervised creation. It must be an ordinary Standard independent Root
with explicit Project/workspace, zero metadata revision and no inherited reset
ledger. The application still owns active Project and canonical workspace
resolution. Existing Root retries require the immutable birth, Project,
workspace, version and generation; they preserve its current transcript and
tasks. A new operation supplies `Some(expected_scope_revision)`. An existing
durable reservation supplies `None`, which only verifies an existing exact Root.
A missing Root on that retry returns an explicit recovery blocker rather than
guessing that an uncertain operation never ran or recreating lost work. Normal
deletion revocation and partial/corrupt canonical pairs retain existing guards.

Default writer jobs, including the Actor-tree revision marker, retain the
complete Host physical set after caller cancellation. The shared save path
reuses an already-held tree only when the
retained physical guard reports that exact acquired Root tree; other writers
continue to acquire their existing tree guard. This prevents reacquiring its
own non-reentrant lock and is not a caller-supplied skip flag.

Creation does not imply ListMetadata, InspectBoundedProgress or SendPeer on the
new Root. `created_target_rights` defaults empty; a non-empty request is explicitly
Unsupported pending a separate trusted Host scope installation. The first
instruction must use a separate exact-creation Inbox purpose and the same durable
receipt/activation path. A creation response cannot substitute for its receipt
or task acceptance/completion.

Synthetic test source covers all roles, exact rights/Project/workspace/revision,
stable operation identity, preserved work on retry, changed birth and scope
revocation/replacement. Direct rustfmt/diff and independent source review are
the only available checks. No user data, provider, actual grant or deployment is
used.
