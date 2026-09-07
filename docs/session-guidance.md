# Queued user messages

The composer can submit text and images while a run is active. These messages use
`POST /api/v1/sessions/{session_id}/guidance` rather than modifying the running
transcript directly. The request includes a stable `id`, `text`, optional `images`
(the ordinary chat image shape), and `mode`:

- `after_round` (default): admit before the next available model call.
- `after_run`: leave the message pending while its recorded activation run owns
  the session, then admit it under a successor run. This is a run boundary, not a
  condition that the entire Goal has been achieved.

The queue uses the normal durable inbox and single-owner activation router.
Finalization checks authorized pending work as well as the transcript cursor, so
later round messages cannot hide an earlier deferred message. The existing
one-successor-per-generation guard remains in effect for failing checkpoints.
Deferred messages respect specific child or background-task waits.

Images are stored in the existing attachment directory. Immutable content and
MIME produce repeatable attachment references so replaying a submission preserves
its identity. Inbox envelopes contain multimodal parts with attachment references;
the normal per-round image preparation resolves them for the provider. The HTTP
body limit is unchanged and a queued message accepts at most 16 images.

`GET` on the same route lists pending text, mode, image attachment ids and creation
time. `DELETE /guidance/{id}` withdraws an unclaimed message. Cancellation retains
the deduplication receipt; attachments are retained because another message may
reference the same content.

The UI keeps accepted items in a collapsed queue menu with image previews. It
clears composer text and attachments only after a matching acknowledgement and
only when their draft revisions are unchanged. Retry receipts store a payload
fingerprint, not image bytes, in browser session storage. Queued content survives
reload through the server inbox; unsubmitted image drafts remain browser-local.

Goal commands use the existing chat command handler. `/goal` opens the settings
for the current session; `/goal <objective>` sets and advances a goal, and control
commands such as `/goal off` and `/goal clear` follow the server's response without
starting an unwanted execution.
