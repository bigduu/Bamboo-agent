# Session history clear outcomes

First bounded diagnostic slice: #2147. Based on protected dev
`d694346aa42a688f204bb8e9fd50e573790361a2`. Independent of prompt PR #2146.

This slice does not enable clearing a previously activated Supervisor. It keeps
the existing #1350 default-writer fence and changes the HTTP outcome contract.
The separate conversation-reset transaction must provide a valid writer before
activated or unknown Actor context can be reset.

## Existing command and first-slice response

`POST /api/v1/sessions/{session_id}/clear` takes no new parameters. The successful
200 body remains:

```json
{"success":true,"session_id":"session-id"}
```

Success follows the existing durable reload and replacement of the process cache.
It is not a receipt for resetting all native/model context. No conversation
revision or operation ID is introduced by this slice.

Only `io::ErrorKind::Unsupported` carrying the typed `SessionAuthorityConflict`
cause becomes HTTP 409 with this body:

```json
{
  "error": {
    "type": "api_error",
    "code": "session_clear_authority_conflict",
    "message": "Session history clear is blocked by its current authority: <diagnostic>. Reload the session before deciding what to do next."
  },
  "session_id": "session-id",
  "status": "blocked",
  "refresh_required": true,
  "retryable": false
}
```

The existing precise `session_execution_incompatible` response wins when a fresh
canonical read confirms that conflict. No substring or untyped `Unsupported`
error is treated as a typed authority refusal. The historical Actor attempt and
current authority remain unchanged; there is no stop, delete/recreate or weaker
writer fallback. The warmed cache and account feed remain unchanged on refusal.

An ordinary error from the clear writer, a post-clear reload error, or a missing
post-clear Session returns HTTP 500 with:

```json
{
  "error": {
    "type": "api_error",
    "code": "session_clear_outcome_unknown",
    "message": "Session history clear outcome could not be confirmed: <diagnostic>. Reload the session before any further action; do not automatically retry clear."
  },
  "session_id": "session-id",
  "status": "outcome_unknown",
  "refresh_required": true,
  "retryable": false
}
```

There is no `success` or `not_executed` claim in either error. `retryable:false`
forbids automatic POST retries; it is not a permanent denial of a later explicit
user decision. The writer currently removes attachments before publishing the
runtime, main and search revision files, so an I/O error can follow partial work.
The old process cache is evicted on unknown outcomes to prevent it from becoming
the authoritative history observation. No `SessionCleared` event is published.

Pre-clear load failures retain their existing error response because the clear
writer has not been called. Missing Sessions retain their existing 404. This
slice does not add authorization, parameters, a recovery endpoint or permissions.

## Readback and client limits

The current storage operation keeps the first System message it finds (zero or
one), removes the other messages, and clears `token_usage` and
`conversation_summary`. It does not explicitly clear `compression_events`,
`model_context_state`, `provider_transcript` or `prompt_snapshot`.

The next-round context ledger detects history rewrites and may reset its epoch
and native transcript before provider admission. Therefore retained internal
fields alone do not establish that old native content reaches the next provider.
That claim requires a controlled actual-provider-payload test.

History exposes UI messages and compression events. Session detail exposes
`token_usage` (null after a successful ordinary clear); it does not expose the
conversation summary. A zero UI-message count does not prove every internal
context field was reset.

The existing account event is `SessionCleared {session_id}`. It has no clear
reason, operation ID or conversation revision. The internal
`.search-index-revision` is not a public history generation. Clients may fence
their own outstanding requests, invalidate local history on the event and
refetch. They must not claim server-enforced cross-client generation protection
or that an old response can never revive old visible messages.

## Validation status

`maintenance_clear_tests.rs` adds typed-versus-untyped response checks, an actual
HTTP fixture matrix for ordinary/Supervisor running, finished, retired and
unknown Actor authority, plus an attachment-cleanup I/O fixture. It checks no
provider calls, no successful feed publication, preserved refusal files/cache,
and eviction on uncertain failures. Existing legacy-success and incompatible
clear regressions remain in place.

The JSON examples in `docs/fixtures/session-history-clear-outcomes.json` are
synthetic contract fixtures. They are not captured server responses.

Cargo and runtime tests are unrun while the shared Rust window belongs to the
parent's active Ready/Prompt work. Direct rustfmt parsing/format checks and
`git diff --check` are lightweight static checks only. No actual user history,
account, permission, model, application installation or browser is modified.
