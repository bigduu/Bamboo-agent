# Session capability integration entry

This source-only entry assembles the existing versioned operation/replay,
finite Host scope, discovery/send, and ordinary Root creation slices. It does
not install grants in any real Session, start a provider, or admit the Drafts.

The App creates one `SupervisedRootCreator` before common Base assembly and
passes its actual `caller_port()` into the shared Server Base/Root framework
assembly. SDK defaults use the same factory with their explicitly supplied
optional Root-create port; an absent port keeps the creation blocker. The App
port now delegates to `SessionMessenger::send_host_root_first_input`
and `SessionInboxPort::deliver_host_root_first_input`. The V2 final writer
checks the current exact creation permit, original caller's full generation,
canonical Project/workspace and reserved Root birth/generation under the
existing publication, lifecycle, Task, Session and tree guards. The same owned
Inbox jobs retain those guards after async cancellation or runtime shutdown.
There is no second queue or receipt store and no SendPeer/inspect grant is
inferred. Durable first input is not task completion.

The create-purpose Inbox preserves its final-lock `pending` observation. An
exact retry of ACKed, cancelled or terminal input returns the original receipt
with `activation=delivery_not_pending` and `activation_requested=false`; it
does not contact a fresh activation router or invent a coalesced run. Current
creation/caller/target authorization is still required even for that receipt.

Creation-only ordinary Root/Child permits use a birth-bound creation-input
address. Genuine Supervisors preserve the old `supervisor_followup` address,
thread and RespectSpecificWait wire across never-installed/Host route changes.
Generic Supervisor fallback also retains never-installed scope publication
through physical Root/link/Inbox jobs and checks the original full caller.
The independent explicit legacy Supervisor API keeps its prior contract.

The capability path is now assembled in this single branch. The minimum source
chain is fresh/retained dispatch (#2171), finite Host scope (#2169), routed
discovery (#2173), final send (#2175), Root writer (#2176), App/common Base/SDK
assembly, canonical creation input, and concrete Child instance admission.
Each component retains its ancestry; this branch has one integration writer.

| Source seam | Frozen component |
| --- | --- |
| Fresh Root / retained owner and arguments | #2171 `c68982ec` |
| Explicit finite Host scope | #2169 `9b1bc7a8` |
| Authorized pagination / metadata / bounded progress | #2173 `136631b9` |
| Final canonical send and compatible Supervisor address | #2175 `0f27f683` |
| Canonical independent Root writer | #2176 `5cdc1b71` |
| App creator before Base assembly / common Base and SDK | `2613f40c` / `893ac7d6` |
| Actual creation-purpose Inbox / terminal receipt correction | `2328a620` / `638ce22a` |
| Concrete Child instance and complete outer permission chain | `ff40756b` |

The Child checkpoint uses the same actual Host tool Arcs through an opaque
in-process proof. Fresh None/v1/v2 calls select Root; retained calls keep their
known original owner and bytes. Names, schemas and contract strings cannot
create a proof. The native gate evaluates once, and live Actor, readonly,
disabled and final finite-scope checks remain in force. The Server factory
installs the typed message overlay once. SDK defaults share that factory;
explicit SDK tool policies keep precedence.

Ordinary in-process Child ports are included. Plan/readonly ports expose only
the two readers. Strict, named-profile, Ticket and process/IPC Child ports remain
unsupported without their separate admitted bridge; the native eight-tool
ceiling is unchanged. Explicit `created_target_rights` still fails closed
pending a separate trusted Host installation; creation receipts do not become
grants. SDKs without a delivery plane retain an explicit send blocker. No real
scope is installed by this assembly.

Reset generation consumers also include #2168/#2172/#2178. Their presence does
not produce a reset. Complete resource closure around the actual Root outer
join, a full idle admission handshake, transactional context/ledger publication
with recovery, and the
preview/reset/status HTTP routes are still required. Terminal Actor status,
missing/expired activation, and `not running` do not prove physical idle.
Lotus C1 (#397) is already merged; it must not call C2 until that backend API and
its readback/error/revision contract are frozen and synthetically verified.

C4a is now included as an independently reviewed source checkpoint from
`7d40171b730f0fd6c6dfbc5f8a0272c1a4ae5107`: actual outer task handles are retained
and joined past Actor/finalization/callback tail, and installed-slot admission
reservations deny common removal/idle eviction. It still returns Unsupported
for complete physical resource closure and signs no reset write. The logical
router/preparation handshake, resource issuer, recovery writer and API remain
separate requirements. Its six new fixtures are UNRUN, including the corrected
real Actor-directory fixture.

Rust source compatibility impact of C4a: `AgentRunner` and `RunnerReservation`
now contain private physical-admission ownership. External struct literals,
including update syntax, require migration to `AgentRunner::new()`/`Default`
and the existing runner reservation functions. Repository literals were
inventoried; external consumers and integrated typechecking are unverified.
Publishing proof fields to preserve literals would weaken the intended private
ownership, so this checkpoint does not do that. This API impact needs review
before admission or publication.

All new Rust fixtures, compilation, Cargo checks, actual provider payload and
cross-client acceptance are **UNRUN**. Direct rustfmt and diff checks are only
source checks. The [bounded validation handoff](session-capability-integration-validation.md)
records the affected packages, fixture sources and C2 handoff conditions. The
next Rust window must verify one recorded integration SHA, run the affected
native fixtures, and satisfy required compile/review gates; no individual Draft
is ready merely because this source entry exists.
