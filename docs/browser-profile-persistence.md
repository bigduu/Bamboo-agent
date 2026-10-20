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

Chromium persists cookies, localStorage and IndexedDB. The bundled headless shell
does not restore session-only cookies by itself, so those cookies have a private,
atomic checkpoint inside the same profile. A successful host command waits for
that checkpoint; response/background checkpoints cover later cookie refreshes.
Checkpoint failure is reported instead of silently claiming persistence or
falling back to a fresh context. No cookie values are returned in browser state
or logs. Normal website expiry, logout and explicit site-data clearing still
apply; clearing does not restore older saved cookies on the next launch.

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
No installed browser, real account cookies or user credentials are used.
