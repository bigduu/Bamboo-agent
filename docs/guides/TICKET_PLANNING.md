# Ticket planning metadata

The Ticket snapshot is the sole durable authority for `priority` and
`expected_completion_at`. Priority reuses `bamboo_domain::TaskPriority`:
`low`, `medium`, `high`, or `critical`. A legacy Ticket defaults to medium and no
deadline. Default fields may be omitted on the wire to preserve legacy canonical
snapshot bytes; readers must apply those defaults.

`expected_completion_at` is the owner's desired deadline, not a model ETA,
progress estimate, execution budget, or promise. It is null/unset or a complete
RFC 3339 timestamp with a known explicit offset, such as
`2026-12-01T18:00:00+08:00`. The original offset is retained. Naive timestamps,
invalid dates and `-00:00` (unknown offset) are rejected. No local timezone is
inferred and no wall clock affects this mutation.

The existing owner-authenticated `POST /api/v1/tickets/update` accepts:

```json
{
  "operation_id": "owner-planning-stable-identity",
  "binding": {
    "scope_id": "existing-scope",
    "supervisor_session_id": "existing-supervisor",
    "binding_revision": 1
  },
  "expected_seq": 42,
  "expected_epoch": 1,
  "operations": [{
    "op": "set_planning_metadata",
    "ticket_id": "existing-ticket-id",
    "expected_record_revision": 7,
    "priority": "high",
    "expected_completion_at": "2026-12-01T18:00:00+08:00"
  }]
}
```

This replaces both planning fields. To clear the deadline, explicitly send
`expected_completion_at: null`; omitting that command field is an error. An
archived Ticket must first be explicitly unarchived. The command requires
verified User authority; Supervisor/model, Worker, Runtime and cross-scope
requests cannot change these owner fields. Scope sequence/epoch CAS and the
original Ticket record revision both apply. On conflict the client retains its
edit and reconciles; it must not silently refresh CAS and overwrite another edit.

The same operation ID and canonical payload replay the original receipt, even
after restart. Changing the payload or receipt subject fails. The existing
receipt stores the User principal, complete canonical command, hash and committed
sequence; ancestor snapshots retain previous values. No new audit store is added.

Planning edits can happen while running or submitted. They only change Ticket
planning fields, record revision, update sequence, schema and the operation
receipt. Priority and deadline do not participate in the current claim comparator.
Like any ordinary update, changing `updated_seq` can affect its existing oldest-update
ordering; no priority-based scheduling policy is enabled here. These edits do not
steer a Worker, replace a contract, invalidate submitted evidence, grant tools,
move Sprint membership, clear a resource claim or mark work complete. Inspect and Project board return the same
Ticket fields. Sorting/filtering and fixed-as-of overdue projections remain
separate query work; clients must not label an absent deadline as overdue.

The first explicit planning mutation, including a reset to medium/null, advances
the snapshot to schema 7 so schema-6 writers fail closed. This writer knows and
retains schema-6 Sprint planning and schema-5 Project/adoption facts. File-effect
artifacts and stopped offline transfer retain schema 7 and the shared audit
receipts; unknown future schemas remain read-only/rejected.

Manual controls still use semantic Pause/Ready/Cancel/Accept operations. This
metadata slice does not enable manual completion. A separate control change must
require the exact Submission's Assignment to have host-confirmed
`process_stopped`; `active_assignment: null`, an HTTP ACK, heartbeat timeout or
session disconnect cannot establish that fact. Explicit User acceptance and
unverified Submission/evidence history remain required and visible.
