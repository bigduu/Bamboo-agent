# Bamboo-owned native Project-scope confirmation

This is an optional historical Host channel. Normal browsers and Lotus inside
Bodhi use the same authenticated Bamboo GET/PUT scope API; neither requires
this native path. See the shared [Web owner contract](../supervised-root-create.md#explicit-server-owner-project-authorization).

The private v1 channel requests one native confirmation for the complete typed
Supervisor Project-scope intent. It never imports a Bodhi/JavaScript approval,
creates a credential, registers trust, or enables the HTTP owner verifier.
The legacy pipe `owner_authorization_available` stays false; authenticated HTTP
owner availability is evaluated separately from the pipe. The exact
`bamboo_native_confirmation_v1` profile means the current Bamboo process can ask
once; it is not consent. Missing/legacy profiles remain unavailable.

## Entry points and presentation

Only `bamboo serve --supervisor-scope-native-confirmation` starts this channel.
Ordinary serve, WebService and SDK entry points stay disabled. The flag requires
a loopback bind and does not select a Project, bootstrap a Supervisor or grant
anything. Bodhi must enable it only for a known compatible producer build.

On macOS, Bamboo spawns its own canonical `current_exe` with the sole internal
argument `--bamboo-internal-scope-confirmation-v1` and private stdin/stdout.
The helper is intercepted on the executable's initial main thread, before Tokio,
configuration, AppState, networking or any model work. It cannot issue a mutation.
It uses NSAlert with a read-only, selectable, vertically scrollable NSTextView.
The complete intent, data directory, launch/request/challenge, previous Project
set and selected IDs/names are shown as escaped pretty JSON. Unicode bidi/paragraph controls are escaped while normal Unicode names remain
readable. Automatic link detection is disabled. Nothing is silently filtered or
truncated. Oversized/unrenderable previews fail closed. Cancel is
the first/default button; only the explicit second Allow button approves. There
is no remembered choice. Other modal results deny. The parent enforces a
120-second timeout and kills only its own UI-only helper on cancellation.

Apple documents NSView as main-thread-only, NSAlert's accessoryView, and the
modal button result mapping. The implementation follows the installed macOS SDK
headers NSAlert.h, NSScrollView.h, NSTextView.h and Security/AuthSession.h.
References: [NSAlert](https://developer.apple.com/documentation/appkit/nsalert),
[accessoryView](https://developer.apple.com/documentation/appkit/nsalert/accessoryview),
[thread safety](https://developer.apple.com/library/archive/documentation/Cocoa/Conceptual/Multithreading/ThreadSafetySummary/ThreadSafetySummary.html).
The readiness preflight checks graphics-session access and active console uid;
unsupported platforms, no GUI, unavailable thread/process/window default-deny.

## Binding, cancellation and publication

The helper result is read only from Bamboo's own child pipe and must echo the
entire immutable request exactly. It is not a wire proof and is never persisted.
Each request ID is spent before inspection/prompting, with 64 bounded IDs per
launch and one in-flight confirmation. A busy arrival cannot become a queued
future prompt. Restart changes the launch and never restores a decision.

Canonical identity, incarnation, management revision and active Project IDs are
read before presentation and again after approval. The entire presentation
context must still match. An observed change returns a conflict; no automatic
retry or new confirmation is issued. The existing SupervisorSessionService and
storage management CAS retain publication, lifetime and revocation semantics.
The adapter does not initialize Work, attach Sessions or grant tool permissions.

EOF/protocol failure and the submission boundary share a local close gate. A
late approval cannot cross a closed gate. Cancel/close/timeout returns the
explicit `cancelled` outcome with `not_submitted:true` only before canonical CAS
issuance. Once issued, the CAS is drained even if the pipe closes. A lost reply
or ambiguous I/O result remains `outcome_unconfirmed`; matching a later GET does
not fabricate a successful receipt or justify replay. Server shutdown cancels
pending UI work and drains any issued native CAS before AppState shutdown.

## Trust and acceptance boundaries

A model, HTTP client, Bodhi frontend, parent PID and pipe handshake cannot provide
an approval value. There is no application-level defense against a compromised
Bamboo binary, debugger, or another process already controlling the user's GUI
through system-granted automation privileges. This change grants no such system
privilege and never operates automation to approve itself.

Unit and temporary-store tests use cfg(test)-only mock decisions. They are not
real user consent. Positive native acceptance requires the user to operate the
actual native dialog after reviewing a one-time isolated fixture plan. No actual
user Project scope, credentials or security settings may be changed for tests.
