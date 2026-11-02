# Supervisor current-user history binding

`GET /api/v1/supervisor/default/sessions` advertises
`capabilities.current_user_bound_history_v1` when its storage backend implements
an identity-bound read. A client must still authenticate as the owner and retain
both `scope.supervisor` and the selected row's `current_user_target`; neither
value is an authorization grant.

For a supported target, send one `POST` to
`/api/v1/supervisor/default/sessions/{target_id}/history`:

```json
{
  "supervisor": { "session_id": "bamboo-default-supervisor", "incarnation_id": "<uuid>" },
  "target": {
    "session_id": "session-123",
    "created_at": "2026-10-30T00:00:00Z",
    "kind": "root",
    "root_session_id": "session-123",
    "parent_session_id": null,
    "authority_sha256": "<64 lowercase hex characters>",
    "ancestry_sha256": "<64 lowercase hex characters>",
    "metadata_version": 0,
    "project_id": null
  },
  "query": { "since_message_id": "<optional cursor>", "projection": "messages" }
}
```

`query` and each of its fields are optional. The full target object must be
copied verbatim, including exact birth time, parent/root, project and the
identity and ancestry commitments. The server checks the canonical Supervisor
and obtains the Session plus its target observation in one guarded storage
read. It compares that observation before returning the normal history shape
with additive `supervisor`, `current_user_target` and
`history_snapshot_source` and `bound_token_usage` fields. `durable` is the
history and optional token state read together under the canonical guard. The
response always includes `bound_token_usage`, including explicit null. A cache
with matching identity can still be older than a durable append, edit or
truncation and is therefore not used for this endpoint. Active runs use the
latest persisted checkpoint, as the ordinary history handler does. A new append or run
in the same generation is not automatically a new Session.

A stale Supervisor or target returns HTTP 409 with no history. An unavailable
or no-longer-ordinary target also fails closed; a backend without this storage
port reports the capability as false and returns 501. Clients requiring bound
history must not silently fall back to an ID-only read, and should ask the user
to refresh and select again on conflict. A metadata change can require such a
refresh; it does not by itself mean that authorization was violated.

The existing nested and flat history GET routes retain their previous behavior
and response shape for old clients. This addition does not grant a worker,
change login/Origin/CSRF handling, create a Project grant or session attachment,
start a model, or provide an event inbox. It does not claim that the target can
never change after a snapshot is returned; callers must continue fencing local
navigation and stale asynchronous responses.
