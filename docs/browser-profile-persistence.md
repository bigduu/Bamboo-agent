# Built-in browser state

The Lotus browser workbench and agent browser tools use the same Bamboo browser
host. Bodhi supplies the packaged runtime; Nova's optional external Chrome
connection is a separate feature. Hiding the workbench or switching chats stops
the frontend observer while the backend retains its owned browser scopes.

The default `shared` mode uses one managed Chromium browser and one persistent
account context per Bamboo data root. Each canonical chat owns its own pages,
active tab, frames, dialogs, action queue and downloads. Opening another chat
reuses that browser. Chromium still creates its normal renderer and GPU child
processes.

Shared pages use the same account cookies, localStorage and IndexedDB. They are
not a security boundary between chats. Document memory and sessionStorage retain
their usual page/session behavior. The existing service-worker block remains.

An explicit `clean` mode creates a fresh temporary BrowserContext in that same
browser. It imports neither account storage nor a previous clean context's
state. Closing that chat's browser scope destroys its clean context. Changing
an active scope's mode requires an explicit close and reopen; a conflicting
request returns 409 without silently switching or clearing anything.

The backend chooses a reserved account key under
`<Bamboo data directory>/browser-profiles` after the existing Session access
checks. HTTP/model arguments cannot select profile paths or an installed Chrome
profile. Different Bamboo data roots remain separate. A physical owner holds
stable file leases; another manager cannot open a concurrent writer on the same
account profile. No existing per-chat profile or real login data is imported or
migrated automatically.

Ordinary scope close, idle retirement and chat deletion close only that chat's
pages and clean context. Sibling scopes and the shared account profile remain.
The final scope close drains the physical host and checkpoints account storage.
Canonical chat deletion keeps that chat's terminal lifecycle fence and bounded
cleanup of its own legacy directory, if present. A complete history reset drains
owners before purging account state; an uncertain writer or unreadable canonical
state prevents the corresponding destructive step.

Chromium persists account cookies, localStorage and IndexedDB. Session-only
cookies also have a private atomic checkpoint in the account profile because
the headless shell does not restore them itself. Commands report checkpoint
failures. Website expiry, logout and explicit site-data clearing remain effective.
Browser state and routine logs do not expose cookie values. DOM state, page
JavaScript and open tabs are not serialized.

A cancelled mutation retires its own scope through owned cleanup. Corrupt
transport, a partial write or an unconfirmed scope close fences the physical
runtime. Unknown/forced physical exits retain the profile lease, replay fence
and scratch quarantine. Failed directory barriers remain unconfirmed until
recovery. Download scratch belongs to the physical host and is released only
through its confirmed cleanup; ordinary chat close does not delete shared data.

The current-source contract and existing PUT/GET/DELETE behavior are detailed in
[Shared browser runtime](design/shared-browser-runtime-v2.md). This candidate is
not a claim that Bamboo 15 or Bodhi 19 already shipped shared profiles.

Validation uses fresh synthetic profiles and loopback sites, including shared
state across chats, clean-context isolation, manager/host restart, page ownership,
canonical retirement and I/O-failure fences. It uses no real account credentials.
Physical reboot/power-loss and native Windows/Linux browser runtime acceptance
remain separate. A clean Node exit or close callback is not an OS-level proof
that every Chromium writer exited. The macOS source now selects a fixed
536870912-byte backing image with a 402653184-byte APFS volume quota for the
account profile and its writable runtime paths. That implementation still needs
source-matched first-use and normal-restart acceptance; the earlier quota
experiment alone does not qualify it. Windows/Linux production admission
currently refuses with `browser_storage_provider_unsupported`, including clean
mode, rather than selecting an unbounded fallback. These platforms are not
qualified by the successful macOS component tests.

Only the original leased owner may clear its launch fence after confirmed
shutdown. A new process refuses an old active or unknown-writer fence; there is
no automatic recovery or fence-clearing API. Consumers must allow Bamboo's
graceful shutdown to complete. Force-killing the server skips that cleanup and
can leave future browser opens unavailable even after its child processes exit.
No production data has been migrated by the isolated validation.
