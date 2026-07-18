# Durable child approval registry

Human-loop approvals from actor children are persisted under
`<data-dir>/approvals/child-approvals-v1.json` before they are exposed to clients. The versioned
registry records the monotonic transition:

```text
pending -> decision_recorded -> delivered | delivery_failed
pending -> expired | delivery_failed
```

The file is replaced atomically after flushing its temporary file. The previous complete file is
kept as a last-known-good backup. Unsupported schemas or corruption of both copies fail server
startup closed instead of silently dropping approvals.

Approval decisions are one-shot by `(child_session_id, request_id)`. The decision is committed
before delivery to the live worker, so duplicate and concurrent responses cannot execute twice.
On server startup there is no surviving proof of the old actor transport; any persisted pending or
decision-recorded request is therefore reconciled to `delivery_failed` with reason
`server_restart` and emitted into the durable account change feed. Authoritative pending snapshot
and browser reload hydration remain Phase 2 PR B of #592.
