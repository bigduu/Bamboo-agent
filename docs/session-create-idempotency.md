# Recoverable session creation

`POST /api/v1/sessions` supports an optional `Idempotency-Key` header so a
client can safely recover an ambiguous timeout or lost response. Existing
clients that omit the header retain the legacy one-request/one-session
behavior.

## API contract

The key must be 1–128 bytes and may contain ASCII letters, digits, `-`, `_`,
`.` or `:`. A client should generate one opaque, non-secret key for each
logical create action and retain it until that action reaches a terminal
status.

```http
POST /api/v1/sessions
Idempotency-Key: 550e8400-e29b-41d4-a716-446655440000
Content-Type: application/json

{"title":"New session"}
```

The first successful request returns the existing `201` response:

```json
{"session": {"id": "..."}}
```

An equivalent replay returns `200` with the same response shape and session
ID. Reusing a key with a different payload returns `409` with error code
`idempotency_key_conflict`. The fingerprint includes every caller-controlled
`CreateSessionRequest` field (`project_id`, title lifecycle, prompt, model,
provider, model reference, reasoning effort, Gold config and workspace), using
recursively canonical JSON. Server defaults and runtime credentials are not
part of the fingerprint.

After an uncertain response, query the authenticated status endpoint:

```http
GET /api/v1/session-create-operations/550e8400-e29b-41d4-a716-446655440000
```

It always returns `200` for a valid key:

```json
{"status":"pending"}
{"status":"succeeded","session":{"id":"..."}}
{"status":"failed","error":{"code":"...","message":"..."}}
{"status":"expired","error":{"code":"idempotency_key_expired","message":"..."}}
{"status":"unknown"}
```

Every valid-key status response carries `Cache-Control: no-store`, so a browser
or intermediary cannot pin an early `unknown` or `pending` result. The route
lives inside the normal `/api/v1` access-control scope. It is not a public
recovery bypass.

The registry namespace is one Bamboo data directory (the current local-account
security domain), shared by that account's authenticated devices. It does not
currently encode an individual device or principal in the key digest. If one
data directory is ever shared by mutually untrusted tenants, the namespace
must first be partitioned by authenticated tenant/principal identity so equal
client keys cannot collide or disclose another tenant's operation status.

## Durable recovery and ordering

The operation registry is independent of target session directories under
`$BAMBOO_DATA_DIR/session-create-operations/v1`. Filenames are full SHA-256
key digests. Records contain only the key digest, a canonical-payload digest,
the reserved session UUID, safe terminal error data and timestamps. They never
contain the raw key or request payload.

Each new key reserves and fsyncs one stable UUID before session creation.
Matching fixed lock shards in memory and on disk serialize same-key requests
across concurrent Bamboo processes without creating an unbounded lock-file
registry. An idempotent POST runs its claimed core in a detached Actix task and
the request handler awaits that task's join handle. Dropping/aborting the outer
request future therefore does not cancel a create that already entered the
core. The normal completion order remains:

1. validate Project/workspace and prepare the session;
2. durably save the authoritative session and atomically publish the rebuildable
   global session index;
3. populate the in-memory cache;
4. publish the runtime workspace;
5. durably publish `SessionCreated` to the account journal;
6. mark the create operation succeeded and return.

Recovery does not trust the rebuildable global index alone. It strictly reads
the reserved root session's authoritative `session.json`; missing, corrupt and
unreadable are distinct outcomes. Index repair rebases under one fixed
cross-process file claim and preserves a newer live summary while correcting
canonical identity/path. SessionRepository then performs its no-regression
cache merge. A corrupt/read failure returns `500` and leaves the durable
receipt unchanged for later repair.

Pending recovery is allowed only while the caller owns the same-key exclusive
claim. It may finish the remaining workspace and account-feed projections,
then mark the receipt succeeded. A status GET uses a nonblocking try-lock: when
the POST owns the claim it immediately reports the persisted `pending` state;
when it wins the claim it re-reads the receipt before recovering. Succeeded
GET/POST replay performs only strict authoritative/index/cache reconciliation;
it never republishes workspace or `SessionCreated` projections and never
replaces a newer live cache Arc.

`sessions.json` initialization, mutation and reset use the fixed index claim.
Every mutation re-reads disk, applies its change, atomically persists, and only
then updates that process's memory snapshot. Old/corrupt rebuilds publish a
crash-resumable marker under the same claim, then scan with short claimed
updates; lifecycle locks and no-regression merging prevent stale rebuild reads
from overwriting newer summaries or resurrecting a concurrent deletion.

Account-journal sequence allocation and append are also serialized across
processes. The writer resumes the newest underfilled journal, truncates a torn
tail, appends, flushes and syncs the file (and syncs the directory when creating
a new file). Session/workflow lifecycle events and `ConfigChanged` use durable
exact-once IDs; config health events deduplicate only consecutive equal states.
Confirmed enqueue plus durable acknowledgement share one bounded deadline.
The FTS index remains best-effort and is not part of the success barrier.

A pending reservation with no session can be retried with the same payload and
reserved UUID. A pending or succeeded reservation with a durable session is
reconciled to success. If a succeeded session is later deleted, replay returns
`410 session_result_gone` and the status becomes terminal `failed`; the same
key never silently creates a replacement during its retention window.

## Retention

Pending reservations do not expire. Expiring one could allocate a second UUID
for the same logical action after a long outage. Succeeded and failed receipts
are retained for 24 hours after becoming terminal. After that window, GET
reports the durable `expired` tombstone until it is physically pruned. A
same-key POST may acquire the claim, remove the expired receipt and start a new
logical operation; a later GET then observes that new operation (or `unknown`
after deletion before reuse). Clients must finish timeout recovery within 24
hours and must not reuse keys for unrelated work.

On startup Bamboo first identifies expired terminal candidates without taking
claims. It then uses a nonblocking same-key try-lock, skips busy candidates, and
re-reads each acquired candidate before deleting only records that are still
expired. Startup never waits on active/pending work. Pending and unexpired
records are preserved. Corrupt records are retained with a digest-only warning
for deliberate manual recovery rather than being silently treated as expired
or deleted.

## Observability

The server emits structured `bamboo.session_create` tracing events with a
non-sensitive 16-hex correlation prefix and fixed, low-cardinality `phase` and
`outcome` fields. Phases cover acceptance, durable reservation, save start,
claim acquisition, session commit, completion, replay, status recovery and
handler termination; elapsed/save durations are recorded in milliseconds, and
`lock_acquired` records `lock_wait_ms` separately. A `response_constructed`
event means only that the handler produced an HTTP result; it does not claim
that the client received the bytes. If the handler future is dropped first,
`handler_dropped`/`cancelled_or_disconnected` records that bounded fact. It can
result from a client abort or server cancellation and deliberately does not
guess which one occurred. Raw keys, titles, prompts, Gold config, workspace
paths/roots, providers and credentials are never traced by the idempotent
create/recovery path.

These are structured traces, not persisted aggregate histograms. A monitoring
deployment can derive counters and latency distributions from the fixed fields;
adding a dedicated metrics-store schema is intentionally outside this API
correctness change.
