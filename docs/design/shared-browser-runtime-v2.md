# Shared browser runtime

The default account browser uses one managed Node host and one Chromium browser
instance per Bamboo data root. Each canonical chat owns its pages, active tab,
page epoch, frames, dialog, action queue and download scratch. Opening another
chat does not launch another complete browser.

`shared` is the default profile mode. These chats use the same persistent default
BrowserContext and therefore the same account site storage. Separate pages are
not a security boundary: shared cookies, localStorage, IndexedDB and service
workers intentionally remain shared. Document memory and sessionStorage retain
their normal page/session behavior.

`clean` creates a fresh temporary BrowserContext in that same browser. It imports
neither the account profile nor a previous clean context's state. Closing it
destroys only its own context. This uses the pinned Playwright/Chromium pairing;
it does not use `--single-process` or weaken Chromium's process isolation.

## Ownership and storage

`SharedManager` admits a single physical `SharedRuntime`. Its `BrowserSession`
owns the process, stdin, reaper, account profile lease and host scratch exactly
once. `SharedChat` owns a separate logical route and a stable canonical-chat
lifecycle lease; it never owns or signals a copy of the physical child.

The account profile is a reserved, domain-separated key under the existing
`browser-profiles` root. Canonical Session IDs cannot address that namespace.
Chat leases do not create per-chat site-data directories. There is no scan,
copy, import or migration of real legacy profiles.

Browser close, idle retirement and ordinary chat termination preserve the
account profile. Explicit canonical chat deletion writes that chat's permanent
terminal fence and retains the pre-existing bounded cleanup of its own legacy
directory, if any. It never deletes the shared account key. A separately
authorized complete history reset drains the physical owner before purging
account state, preserves stable lock inodes and leaves the account key reusable.
Unknown writer state still blocks reset before the canonical wipe.

## Transport and cancellation

Production account hosts set `BAMBOO_BROWSER_HOST_PROTOCOL=2`. Initial
`runtime_state` must report protocol 2, the exact host generation, a valid browser
instance ID, zero scopes and both supported modes. A standalone manager without
an account root retains its explicit single-scope ephemeral protocol; account
startup never falls back to it.

Every scoped request/reply/event carries version, host generation, opaque scope
ID and scope generation. Pending native requests bind the complete tuple.
Request IDs are allocated while holding the physical stdin write lock; the lock
is released before waiting for replies. Native buffering is bounded to 2 MiB,
pending requests to 64, and the host admits at most 16 chat scopes and 32 owned
pages. Late retired-generation frames are discarded, never assigned to another
chat. A known reply with a mismatched tuple fails the physical transport.

Cancelling a mutating caller fences and closes its own scope through an owned
task. Cancelling a read removes only its waiter. A partial write, corrupt
transport or unconfirmed scope close invalidates the entire runtime explicitly;
normal chat cancellation does not signal the physical child. The final chat
close gracefully drains the physical host and checkpoints the account profile.
EOF is owned by the physical runtime, not an individual chat.

Native reaping or a scope-close ACK is not an independent proof that every
Chromium descendant has exited. Forced/unknown physical exits retain the
existing durable writer fence, profile lease and scratch quarantine. The open
capacity finding remains unresolved: these safeguards are not a disk quota or a
cross-platform process-domain containment claim.

## Existing HTTP routes

- `PUT /api/v1/browser/sessions/{session_id}` accepts no body, `{}`, or a strict
  optional `profile_mode: "shared" | "clean"`. Null, unknown fields, duplicate
  fields and unknown modes return 400 `invalid_browser_request`.
- Omitted mode defaults to shared for a new scope and preserves an active
  scope's mode. An explicit active mode change returns 409
  `browser_profile_mode_conflict`, with no automatic close, switch or retry.
- Successful state responses include `profile_mode` and `storage_shared`, true
  exactly for shared mode. Existing navigation, tab and dialog responses retain
  those fields.
- `DELETE` closes this chat's scope and returns 204. Later `GET` returns 404
  `browser_not_open`; GET never creates a scope. Explicit PUT is required to
  reopen. Closing the final tab is different: it returns an empty-tabs state
  while retaining the scope and mode.

The existing canonical Session checks, access middleware and tool-context
identity stay authoritative. No new POST route, login, credential, filesystem
path argument or shell protocol is introduced.

## Validation status

Synthetic real-Chromium host coverage and targeted Node policy regressions are
recorded separately with their exact source hashes. New Rust manager, parser,
namespace, shutdown and reset tests are source preparation until their actual
commands complete in the coordinated Rust window. A Node component pass does
not establish HTTP/Lotus integration, cross-platform acceptance or publication.
