# Session capability integration entry

This source-only entry assembles the existing versioned operation/replay,
finite Host scope, discovery/send, and ordinary Root creation slices. It does
not install grants in any real Session, start a provider, or admit the Drafts.

The App creates one `SupervisedRootCreator` before common Base assembly and
passes its actual `caller_port()` into the shared Base/Root/SDK framework
assembly. That port now delegates to `SessionMessenger::send_host_root_first_input`
and `SessionInboxPort::deliver_host_root_first_input`. The V2 final writer
checks the current exact creation permit, original caller's full generation,
canonical Project/workspace and reserved Root birth/generation under the
existing publication, lifecycle, Task, Session and tree guards. The same owned
Inbox jobs retain those guards after async cancellation or runtime shutdown.
There is no second queue or receipt store and no SendPeer/inspect grant is
inferred. Durable first input is not task completion.

Creation-only ordinary Root/Child permits use a birth-bound creation-input
address. Genuine Supervisors preserve the old `supervisor_followup` address,
thread and RespectSpecificWait wire across never-installed/Host route changes.
Generic Supervisor fallback also retains never-installed scope publication
through physical Root/link/Inbox jobs and checks the original full caller.
The independent explicit legacy Supervisor API keeps its prior contract.

Minimum source dependencies are fresh/retained dispatch (#2171), finite Host
scope (#2169), routed discovery (#2173), final send (#2175), Root writer (#2176),
the existing App/common assembly checkpoints and this final port integration.
Each component retains its own ancestry; this branch is the only integration
writer. Child framework-instance admission remains a separate dependency until
its exact source is incorporated. Explicit `created_target_rights` still fails
closed pending a separate trusted Host installation; creation receipts do not
become grants. SDKs without a delivery plane retain an explicit send blocker.

Reset generation consumers also include #2168/#2172/#2178. Their presence does
not produce a reset. Actual Root outer-join/resource closure, an idle admission
reservation, transactional context/ledger publication with recovery, and the
preview/reset/status HTTP routes are still required. Terminal Actor status,
missing/expired activation, and `not running` do not prove physical idle.
Lotus C1 (#397) is already merged; it must not call C2 until that backend API and
its readback/error/revision contract are frozen and synthetically verified.

All new Rust fixtures, compilation, Cargo checks, actual provider payload and
cross-client acceptance are **UNRUN**. Direct rustfmt and diff checks are only
source checks. The next Rust window must verify one recorded integration SHA,
run the affected native fixtures, and satisfy required compile/review gates;
no individual Draft is ready merely because this source entry exists.
