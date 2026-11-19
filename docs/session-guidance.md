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

## Run a prompt in the current chat

In a Standard chat, send `/loop Check the build and report changes` for an
adaptive loop. The first occurrence is due immediately. After each successful
occurrence, Bamboo can use the model's suggested delay of one minute to one
hour, measured from the end of the turn. Bamboo waits 20 minutes following a
failed turn or a turn without a valid suggestion and keeps scheduling later
occurrences. Bamboo saves a fallback before dispatch so an interrupted turn
without a completion record still has a next wakeup.

For a fixed interval, say “Check the build every five minutes and
only tell me if it changes,” or “每隔五分钟检查构建状态，有变化再告诉我”.
Give the task and its fixed interval in one message. Conversational intervals
accept common Chinese and English wording. You can also enter
`/loop 5m Check the build and report changes`. The first run is due after
one interval. Fixed intervals range from one minute to one day.

Each run uses the session's current model, workspace, and permissions. If
the session is busy or waiting for input, one scheduled run becomes due when
the session is free. Missed occurrences do not accumulate.

Ask “When will the loop run next?”, “Stop this loop”, or “Resume this loop”
to manage it conversationally. The commands below do the same directly.

- `/loop status` shows the next due time and the most recent known run state.
  The status API also includes an accepted adaptive delay and its reason.
- `/loop stop` stops scheduling future runs. Use the chat's normal cancel
  action to interrupt a run already in progress.
- `/loop resume` resumes an unexpired loop after a Bamboo restart. Time spent
  offline is skipped.

The loop ends after seven days or when its conversation is reset or deleted.
A session can have one active loop.

Clients can read the same state with `GET /api/v1/sessions/{session_id}/loop`
and send `start`, `start_adaptive`, `stop`, or `resume` with `POST` on that
endpoint. Both start actions take `prompt`. Fixed `start` also takes
`interval_seconds`.
