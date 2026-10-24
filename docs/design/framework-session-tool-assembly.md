# Shared Session tool assembly (B4a)

Issue [#2174](https://github.com/bigduu/Bamboo-agent/issues/2174) continues
B3 #2167/#2173 and the fresh Root contract at `c68982ec`. It is independent of
prompt #2146, the Host-send writer and the storage/reset owners' changes.

`assemble_framework_session_tools` composes actual Host-routed history,
filtered current history and the caller-bound fresh Root entry around the
Host's existing permission-checking Base. It reuses `SpawnSessionTool` and
`VersionedRootTools`; it does not rebuild the alias/replay selector or create a
second permission policy. The supplied built-in permission executor is the
same instance used by that Base assembly. Existing scoped message composition
remains on the original canonical delivery plane.

The Server installs this once in Base. Agent defaults, schedule/default runs
and stored Child/WithTask surfaces share those concrete instances. Root adds
SubAgent, planning, scheduling and genuine Supervisor management; it no longer
reconstructs the history reader or Root spawn owner. The Project context and
one existing `SupervisedRootCreator` instance are built before Base, so its
caller port and Supervisor controls reuse the same durable writer. There is no
late mutable alias, second creation registry, copied history or new grant.

SDK defaults use the same factory after their final built-in/MCP composition.
`session_root_creator` is an optional trusted Host port, retained independently
of defaults setter order. Explicit tools, no-tools and default-executor choices
continue to win and never receive added Session tools. Without a caller port,
the real Root entry reports `session_root_creator_unconfigured`; it never falls
back to a Child. A blocked configured port preserves its exact scope result,
including `host_root_first_input_unavailable`, so an unavailable first-input
writer is not mistaken for a Project grant error. All original operation IDs,
argument bytes and retained replay bindings remain governed by the existing
fresh/retained contract.

## Explicit boundaries

Tool installation is not a scope or security configuration change. Every read
still requires B3's original-caller and final Host read authority. Root creation
still requires the installed caller port's exact Project/workspace scope and
canonical writer. The integrated App caller port now supplies the canonical
create-purpose first-input adapter through the same SessionMessenger/Inbox plane.
An unavailable backend still refuses at its final boundary; a scope observation
is not a grant. SDK Root creation still needs its explicit trusted caller port.
Source wiring does not establish end-to-end runtime success.

SDK messaging still requires explicit `session_delivery` for one coherent
Inbox/activation/messenger plane. This slice does not invent an Inbox, active
owner or default activation route merely to advertise `session_message`.
Delivery-backed defaults retain their existing scoped sender, one native send
permission evaluation and durable receipt semantics. Configless reads confer
no create/send grant, and accepted input is not task completion.

The integrated [B4b in-process path](local-child-framework-session-tools.md)
now retains concrete Host instance proofs through the complete outer permission
chain and uses the fresh/retained exact selector. Ordinary non-strict Child and
ordinary Ultra model surfaces can use those installed ports subject to their
existing live, permission, disabled and final scope checks. Readonly/Plan Child
execution receives only the two readers. Strict, named-profile and Ticket paths
receive no added ports; the immutable native eight-tool ceiling is unchanged.
Process IPC still requires its separate authenticated bridge. Neither names nor
Base registration alone establish this instance authority or runtime acceptance.

## Validation status

New composition fixture sources test all supported fresh versions against a
real shared wrapper and an explicit synthetic caller port, an installed Child
spy that must never receive fresh Root calls, missing/configured-specific
blockers and Plan/disabled gates before the writer. Real SDK assembly fixture
sources check default Root registration, the precise Host blocker, and all
three explicit tool-policy choices. Server assembly source checks shared exact
owner identity across application surfaces, no implicit Root scope and Host
read routing from the shared surfaces. The existing history-schema fixture is
updated while retaining current-history privacy and no-scope denial.

Only direct rustfmt, diff checks, source inspection and lightweight synthetic
source assertions are permitted in this window. **Cargo, compilation, Clippy,
and all Rust fixture execution remain UNRUN.** No provider/account session,
actual grant/security change, user-history clear, deployment or cache copy was
performed. Keep the stacked PR Draft until the required compilation, affected
runtime cases and current-head/base review are satisfied.
