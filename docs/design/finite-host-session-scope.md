# Finite Host Session scope and final reads

Issue #2162. This source slice supplies a role-neutral Host installation and
integrated metadata/inspection reads. It does not expose an HTTP configuration
endpoint, install a default grant, create a Root, send cross-Root input, or enable
conversation reset. All Rust tests and compilation remain **UNRUN**.

## Explicit authority

The canonical Store owns one empty, in-process registry and issuer. Only a
trusted Host calls `install_host_session_scope(expected_revision, binding)`.
Ordinary Session saves, Project membership, parent/Child links, management
receipts and model arguments never call it. Root, Child and Supervisor use the
same finite binding: a stable caller identity and complete ancestors, at most
128 exact targets with separate ListMetadata/InspectBoundedProgress/ReadContextView/SendPeer
rights, plus at most 32 exact creation-operation/Project/workspace permits.
Creation and Send rights are recorded data; their final writers are separate
mandatory source slices and are not emulated by the read ports.

A serialized binding is an observation. The non-deserializable token additionally
binds an issuer instance and one installed Arc identity. Copying JSON, issuing a
new token from another issuer, or reissuing equivalent data does not yield the
current installed token. Revisions advance by explicit CAS; replacing/revoking
an installation waits for all retained uses. Lookup returns None only for a
never-installed scope. Stale caller identity and a revoked installation return
PermissionDenied, preventing a routing adapter from falling through to broader
legacy authority. A failed, uncommitted installation changes no revision.

This registry is not Session persistence and is not a second Session index.
Tokens are scoped to this Store/issuer instance, are not serialized to a worker
or disk, and do not survive a process restart. A Host that opts into these scopes
must explicitly reconstruct its authorized installations before admitting tools
in a new instance. No cross-process/global or durable-revocation guarantee is
claimed. Existing canonical Supervisor management/bootstrap remains unchanged.

## Final boundaries

The operation retains publication authority, then takes the existing lifecycle
and Task shared gates, canonical depth/ID Session locks and sorted Actor-tree
locks. A canonical preflight selects lock order only. Strict private reads then
recheck all observations and complete Child ancestor births, Project, metadata,
parent births and depth under the full set. There is no global-index fallback
or public-loader reentry while these locks are retained. Trees, Sessions, Task,
lifecycle and publication are released in reverse order. Future owned Inbox
jobs must retain this same complete guard through their physical completion.

Installation validates caller and all targets under the physical set before
publishing its in-process token. Metadata listing validates only the explicitly
ListMetadata-authorized targets, returns finite metadata observations and never
returns transcripts. Inspection reads one explicitly InspectBoundedProgress
target; the installed tool adapter must reuse the existing bounded persisted
progress projection and expose no full-history method through that right.
`ReadContextView` separately permits immutable status/brief snapshots containing
the exporter's bounded Task-list title, IDs and description previews. It does
not follow from `ListMetadata` or `InspectBoundedProgress`, and grants neither
of those rights, assistant-excerpt inspection, raw history nor `SendPeer`.
Scoped `export_context`/`read_context` use only this explicit right (or the
original Supervisor's independently authorized managed-target route when no
Host scope has ever been installed). Fixed revision/file selectors are not
grants; each operation revalidates the original caller, current scope epoch and
target lifetime. The reader admits only retained no-follow fixed bundle reads
on supported platforms and otherwise fails closed. Existing strict authority
validation may read Main; only the view-bundle reads have the 8/16/8 KiB caps.
Returned snapshots and cursors are observations, never admission receipts or
completion evidence. Tool pagination and scope/caller/query-bound cursors remain
in the separate discovery adapter, without injecting unrelated history.

## Reset identity

`HostSessionScopeIdentity` explicitly omits conversation generation. Scope
intent, identity and independent work survive a safe reset. In contrast, each
real `SessionCallerReference` now freezes the complete inline reset ledger;
zero/legacy JSON omits it. Its canonical comparison validates the ledger and
requires exact equality, including receipts at an unchanged counter. Existing
owned ToolCtx and retained-operation records carry this original observation.
A pre-reset operation cannot adopt a fresh generation simply because the stable
Host grant still exists. Legacy pending records lacking nonzero generation data
are refused when compared to a reset Session. This change alone does not make
all storage writers reset-safe; the C3b Task/recovery/admission audit remains a
mandatory predecessor to any reset writer.

Creation operation keys must use caller birth plus operation UUID, not a rotating
grant/issuer ID. Recorded creation permits require the existing Project store,
workspace resolver, idempotency claim/receipt and canonical Root writer before
consumption. `created_target_rights` defaults empty and cannot be inferred from
a creation receipt. Those create/send implementations are not delivered here.

## Evidence

Eight Domain test functions cover all roles/versions, exact rights and create
limits, ancestry, token copying and stable grants across pure reset preparation.
Four registry tests deterministically poll the publication fence for pending
revoke/replacement and verify stale/revoked lookup is an error. Five cold Store
tests cover real canonical files for Root/Child/Supervisor, distinct metadata and
inspection rights, target reconfiguration, bad ancestors and opposite cross-Root
lock order. These 17 test functions are source fixtures, **UNRUN**; no real user
Session, provider request or history clear was performed. Only direct rustfmt,
diff checks, source inventory and independent static review are available.
