# Finite Host-scoped peer send

Issue [#2170](https://github.com/bigduu/Bamboo-agent/issues/2170). This bounded
slice adds the final SendPeer writer to the finite Host scope from #2162 and
reuses the complete-send adapter/permission assembly from #2155/#2161. Its
fixed combined dependency is `0c7ed2e05d388525fad82e26ee7d4335759695c1` on
`bamboo/integration/session-host-send-source-20261022`. It is independent of
prompt #2146 and does not rewrite #2144 lifecycle ownership.

## Routing and authority

`CanonicalSessionSendAdapter` first looks up the original executing caller's
Host namespace. An installed token selects the same SendPeer path for Root,
Child and Supervisor. Only an exact target with SendPeer is eligible; listing,
inspection, Project membership, management and a creation receipt confer no
right. SendOnly works without InspectBoundedProgress, a public Session load or
a discovery round trip. Host sends require explicit QueueOnly or TriggerTurn.

Only a never-installed namespace (`Ok(None)`) uses the existing same-tree or
original Supervisor followup route. A lookup refusal, revoked/replaced token,
stale identity, missing SendPeer or unsupported Inbox port never falls through
to legacy authority. The native permission evaluator, exact-owner approval
replay, typed Once handling and Plan restrictions retain their existing paths.
There is no additional evaluator or installation of actual user grants.

A None lookup is only a routing hint. The compatibility Inbox ports retain
an actual never-installed publication guard before acquiring the existing
peer/Supervisor physical authority, then recheck the complete original caller
under those locks. The same owned filesystem jobs carry both guards. An
installation that wins before that final fence refuses the old route; one
that arrives later waits for physical admission to finish. This uses the
Registry map's owned read guard, plus an existing empty slot's publication
guard, without allocating permanent caller slots for legacy traffic. It
conservatively delays other callers' installations in this Store/issuer while
that legacy physical work runs. The old non-Canonical APIs keep their existing
contracts, and the new observed compatibility ports default to Unsupported.

The default Domain Inbox implementation rejects the new port as unsupported.
The concrete Store retains the current token's publication guard, then the
existing lifecycle and Task shared gates, canonical depth/ID Session locks and
sorted Actor-tree locks. Under that set it checks the complete original caller
ledger and both stable identity/Project/metadata observations, including the
complete Child ancestor chain. It derives the Inbox path from the canonical
Root/Child location and uses the private existing Inbox transaction. No global
index lookup, public-loader reentry, `peer_pair` widening or second Inbox is
introduced. The actual acquired tree IDs are available to a separately owned
Root creation writer so that it can reuse these guards without reacquisition.

The Inbox's owned filesystem capability retains the same complete authority
through every started blocking job, including caller cancellation or runtime
shutdown. Replacement/revocation waits for retained publication uses. This is
the existing per-Store, in-memory issuer contract; it is not durable installation
or global cross-process revocation.

## Identity, intent and receipts

Ordinary Root/Child send retains the existing `session-peer-v1` address using
source ID/birth, target ID/birth and operation ID. A genuine Supervisor retains
the existing `supervisor_followup` address using its SupervisorReference, target
ID and operation ID, together with its unthreaded envelope. A pure Domain
identity projection is shared by the engine and final writer; the legacy
Supervisor writer uses that same identity helper. Host routing, delivery intent,
rotating grant UUID/revision and conversation generation cannot allocate another
address for the same logical operation. The final Host writer independently
checks the current target birth even though the historical Supervisor address
did not encode it. None of these identity bytes grants authority.

Supervisor TriggerTurn retains the exact existing RespectSpecificWait intent
wire, so an admitted legacy operation remains an exact retry after Host scope
installation. QueueOnly uses an explicit inert intent at the same address;
switching it to TriggerTurn is a conflict, as is changed text. Ordinary peer
intents keep their existing wire. The current grant and original invocation
still need separate final validation, including for every receipt replay. The
low-level port requires bounded plain text and the exact source/target/thread/
operation envelope; it does not authorize reply or steer.

The same durable semantic receipts and immutable DeliveryIntent govern QueueOnly,
TriggerTurn, activation failure and ACKed retries. QueueOnly may notify an
already active owner, while TriggerTurn uses the existing RespectSpecificWait
activation path. An ACKed retry cannot wake a new owner. A receipt proves durable
admission, not consumption, completion, additional authority or wait resolution.

## Source validation and remaining gates

Seventeen new Rust case sources cover SendOnly for all three roles across Projects,
installed-scope/no-fallback routing, distinct inspection rights, grant reissue
and frozen arguments, default unsupported adapters, activation failure/ACK retry,
canonical identity/Project/metadata/ancestors, forged raw envelopes, complete
ledger checks at unchanged revision, missing/wrong global index hints, and a
parked physical Maildir job after runtime shutdown retaining publication plus
all physical guards. Every fixture
uses a fresh temporary directory; the nonzero-generation fixture only seeds
synthetic future-producer bytes and is not a production reset writer.
Additional deterministic Registry polls and a four-way paused legacy writer
fixture cover absent/uncommitted slot publication, installation/revocation
winning before admission, and the absence guard surviving a physical job's
runtime shutdown without consuming permanent Registry caller capacity.
A fixed Supervisor wire identity fixture and two real Inbox/adapter source cases
cover legacy receipt → Host installation → exact retry, body/intent conflicts,
revocation and queue/trigger sharing one operation address. A Host-only target
with no Supervisor attachment proves that compatible wire identity does not
borrow the old Supervisor authority.

Direct `rustfmt --check`, `git diff --check` and source inventory are the only
checks run in this slice. **Compilation, Cargo, Clippy and all Rust tests are
UNRUN** while the parent holds the Rust execution window. Proposed affected
test commands, to be run only after that window is granted:

```text
cargo test --locked -p bamboo-domain host_session_scope
cargo test --locked -p bamboo-storage host_session_scope
cargo test --locked -p bamboo-storage session_inbox::host_scope_tests
cargo test --locked -p bamboo-server-tools --test scoped_session_messaging
cargo test --locked -p bamboo-engine session_messaging
```

Current-source compilation, affected behavior tests, current-head/base review
and normal required checks remain merge blockers. Keep the component PR Draft.
This slice does not implement Root creation/first-input admission, scope
configuration UX, history reset, control or deployment.
