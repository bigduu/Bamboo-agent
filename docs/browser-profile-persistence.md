# Built-in browser state

The Lotus browser workbench and the agent browser tools use the same Bamboo
browser host. Bodhi supplies the packaged runtime; Nova's optional external
Chrome connection is a separate feature. Hiding the workbench or switching
chats stops the frontend observer, not the backend browser.

Bamboo stores one private Chromium profile per chat under
`<Bamboo data directory>/browser-profiles/<SHA-256 of chat ID>`. The backend
chooses that directory after the existing Session access checks. HTTP requests,
model arguments and ambient environment variables do not select a profile or
an installed Chrome directory. Different chats and different Bamboo data roots
remain isolated. Reopening a live chat browser reuses its context; idle
reclamation and application/process restart reuse its on-disk profile.
The Rust manager holds an operating-system file lease until its owned host has
been reaped; another manager cannot concurrently open the same chat profile.
Dropping the manager transfers the child, scratch directory and lease to a
reaper that can finish even after the async runtime has shut down.

Deleting a chat is terminal, including recursive root/child deletion and bulk
cleanup. It reaps the owned host and removes that chat's entire profile, even
when the browser was already closed or had an unconfirmed close. Normal close
and idle reclamation keep the profile. Open and delete share a stable per-chat
file lock outside the profile directory; deletion never removes that lock or
follows a profile symlink into another directory. Another live owner's lease
blocks deletion. Storage deletion and profile cleanup continue if the HTTP
waiter disconnects. Cleanup failures return an error with the affected session
IDs; retrying deletion by those IDs also retries profiles whose chat storage is
already gone. Recursive and bulk storage operations report their actual
candidate IDs under their lifecycle lock, including late children and partially
failed batches. On partial failure the backend checks canonical absence before
purging any candidate's browser data; existing or unreadable Sessions retain
their data. This removes files, without promising physical secure erasure of
underlying storage media or backups.

Chromium persists cookies, localStorage and IndexedDB. The bundled headless shell
does not restore session-only cookies by itself, so those cookies have a private,
atomic checkpoint inside the same profile. A successful host command waits for
that checkpoint; response/background checkpoints cover later cookie refreshes.
Checkpoint failure is reported instead of silently claiming persistence or
falling back to a fresh context. No cookie values are returned in browser state
or logs. Normal website expiry, logout and explicit site-data clearing still
apply; clearing does not restore older saved cookies on the next launch.
Normal close, idle reclamation and manager drop stop new commands and send EOF
outside the command queue. The host closes pages before its final checkpoint,
then closes Chromium. Shutdown has a bounded grace period. If final persistence
cannot be confirmed by a clean host exit, a private close-pending marker blocks
reopening that profile instead of importing a potentially stale cookie snapshot.
Its existing site data is retained; an unconfirmed close is reported as such.
On Unix, creating or removing that fence also syncs its containing directory;
terminal profile removal syncs its parent directory. A failed directory barrier
is reported, including on a retry where the marker/profile is already absent.
Controlled command timeouts and cancellation also mark a persistent profile
unconfirmed before killing the host. They do not replay a checkpoint that may
precede a logout. If the marker itself cannot be written, ordinary close keeps
the live owner attached. Destruction must retain the lease and wait for clean
exit or filesystem recovery before it can return without that durable fence.

This does not serialize page JavaScript, active DOM, sessionStorage or an open
tab list. A new tab or process starts a fresh document. Profile persistence is
not a guarantee that a website will keep a server-side login valid forever.
The existing download scratch directory remains temporary and is cleaned up;
it is separate from the durable profile.

The regression suite uses only a synthetic loopback site and new temporary
profiles. It covers host restart, forced retirement after confirmed commands,
last-tab reconstruction, repeated reads, cross-chat isolation, explicit site
clearing, corrupt state rejection and bounded downloads. Rust tests separately
cover stable directory selection, symlink rejection and profile lease conflicts.
The close regressions also cover background HttpOnly cookie deletion immediately
before close/idle/drop, blocked queued writes, unconfirmed exits and lease
ownership while a previously signalled host is still alive.
No installed browser, real account cookies or user credentials are used.
