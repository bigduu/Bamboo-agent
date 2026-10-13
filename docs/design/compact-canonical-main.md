# Compact canonical Main producer (v1)

The V2 host writes a leading authority section and the existing flat Session in
**one `session.json` replacement**. Runtime/proof/Task/Management lanes keep their
existing representations and publication boundaries. This is a Main producer
and full-reader compatibility slice. The separate #1339 Actor
snapshot observer reads only the leading section while retaining its Runtime,
proof, row and lineage checks. Neither slice introduces an action authorization
or ContextRefs consumer.

## Fixed framing and closed payload

Byte zero is the literal
`{"_bamboo_main_authority":{"version":1,"payload_bytes":"`, followed by exactly
10 ASCII decimal digits, then `","payload":`, exactly that many UTF-8 payload
bytes, then `},` and the original flat Session encoding without its initial `{`.
The number is a byte length, not a revision. The entire section, including
framing, must fit **512 KiB**. Whole history is not subject to this new cap, and
existing full-file/aggregate authority-reader budgets are unchanged.

All 15 payload members are required, including null, zero and false:

- `id`, `created_at`, `kind`, `parent_session_id`, `root_session_id`, `spawn_depth`;
- `authority_identity`, `metadata_version`, `project_id`;
- `root_orchestration_only`, `root_tool_authority_revision`,
  `root_mode_transition_epoch`, `root_mode_operations`;
- `supervisor_management`, `title_label`.

Values come from the exact typed Session admitted by the existing writer.
Project uses `Session::project_id_meta()` (typed Some first, then legacy map).
Raw logical IDs, stored empty legacy Root spelling, typed birth and identity,
existing receipts/order/bounds and management tombstones are preserved.
`title_label` removes controls/bidi controls and takes 160 Unicode scalars;
it is only a public label. No history/native context, prompts, Task state,
permissions, Inbox/lease, effort, budgets, workspace or health enters this section.

Unknown, duplicate or missing payload members, malformed nested shapes,
collapsed duplicate Project sets, unsupported versions, wrong numeric shapes,
noncanonical envelope order/escaping, overflow/truncation or wrong delimiters
are invalid. Nullable fields must be present, not silently defaulted.

## Producer preparation and existing durability boundaries

The shared host encoder preflights the section before the named publisher begins
its own new state mutations. Capacity failure rejects explicitly; it never
truncates authority, drops receipts, or publishes legacy Main as success.

- Full/initial/mode-operation/runtime-first fallback: preflight before writer
  directory preparation and Prepared proof/Runtime/Main publication.
- Copy: construct/rewrite the exact fresh target and preflight before its copy
  journal, staging, attachments, Runtime/proofs/Main; reuse the prepared bytes.
- Root recreation and Supervisor bootstrap: construct the exact new birth and
  preflight before revoked-directory removal/staging. Main and legacy Runtime
  have separate buffers; the reserved member never appears in Runtime.
- Clear: preflight before starting its attachment/Main/Runtime job, including
  newer metadata from the actual Runtime overlay.
- Explicit Actor append: validate/preserve the exact existing raw section bytes;
  only its already-supported transcript lanes change. Legacy append stays legacy.

Existing authority reads, physical lock-file acquisition, constructor work and
independent Task/copy recovery keep their order and are outside this publisher
preflight guarantee. Existing Runtime-first, Prepared/Committed proof and staged
copy/Root fault behavior remains. Each already-started filesystem job retains its
existing owned guards. This is not a promise that every later async phase finishes
on caller cancellation or runtime shutdown, and adds no recovery journal.

## Full-reader compatibility and observation limits

The named V2 full-Main compatibility readers validate a present section against the
flat authority fields before overlay/normalization, proof creation, index/cache
publication or protected reconstruction. The compatibility pass skips private
history allocations while checking full JSON syntax; each original consumer
then retains its own full/partial decoder and authority checks. It does not claim
that a partial reader validates private message/native semantics it never read.

A truly absent member preserves existing legacy full-reader semantics/defaults.
Present malformed, moved, escaped, duplicated or contradictory sections fail
closed; there is no read-side upgrade, cached fallback or repair. An old typed
writer may drop the member, producing legacy input unavailable to a future
compact-only consumer. Runtime-only/Task/Management/migration retain Main bytes.

The pure prefix decoder proves only observed frame/payload bytes. It cannot prove
an unseen suffix exists, is syntactically valid, or matches the flat birth/Project.
A raw external writer changing flat authority while retaining a valid prefix is
rejected by full compatibility; prefix observation alone cannot detect it. This
is not a checksum/body certificate or a blanket mixed-writer integrity guarantee.
The retained-FD/no-history consumer (#1339) explicitly accepts that narrower
public observation boundary; its Main FD stops at the exact section end and its
ancillary complete-file checks remain. Legacy absence is unsupported there,
without GET migration or a full-history fallback. ContextRefs (#1343) remains a
separate acceptance slice and cannot inherit an action grant from this view.
