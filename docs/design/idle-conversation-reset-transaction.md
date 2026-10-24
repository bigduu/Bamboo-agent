# Idle conversation reset transaction

Issue #2152. Baseline protected dev
`d694346aa42a688f204bb8e9fd50e573790361a2`. Owner: the conversation-reset worker
in the cross-Session task. Independent of prompt PR #2146 and diagnostic #2147 /
Draft #2150.

## First source freeze and delivery boundary

The first freeze implements `session/conversation_reset.rs`: a pure request,
decision, prospective ledger and candidate transformation, registered in Domain.
Nine synthetic test functions cover preservation/purge, a legacy-only prompt
snapshot, exact replay, stale preview, pending/live/unknown refusal,
schema/overflow, digest ordering, Child
refusal, receipt eviction and rejection of force/role/permission arguments.

It changes no persisted `Session` field, Main frame/proof, storage publisher,
HTTP route, account event, permission or security configuration. `Prepared` is a
calculated candidate, never a successful reset or storage capability. Its
prospective receipt becomes authoritative only if a future transaction commits
and confirms the complete result. **An activated idle Supervisor cannot yet be
cleared through this code.** No user data or account has been used.

Direct rustfmt parsing/format checks and `git diff --check` are static evidence.
Cargo, compilation, the nine tests, transaction recovery and actual provider
payload acceptance are unrun until the parent transfers the shared Rust window.

## Existing authority and reusable seams

The current `SessionStoreV2::clear_session` acquires lifecycle, runtime-Task and
Session maintenance guards, then calls `check_default_actor_clear`. Accepted
#1350 denies protected default-writer mutations after activation or with unknown
Actor authority. That guard remains unchanged.

`RootActorWriteProof::validate` requires a Running **Ordinary** Root and its exact
activation fence. `RootModeContextWrite::capture` is also Ordinary-only and
authorizes only the exact mode-operation candidate. Neither authorizes a
Supervisor conversation reset. Renaming a capability, fabricating a mode
operation or calling the default clear writer would weaken the existing fence.

Reusable pieces are the existing owned blocking filesystem-job guards, canonical
Main/runtime and Root/Supervisor proofs, Actor record/initialized marker
validation, epoch-scoped Root-mode operation pattern, existing transaction
recovery discipline and rebuildable index/search publication. There is no new
Session identity, Inbox, scheduler, broker or permission-grant issuer.

## Minimum legal storage owner

The next freeze should add one private `IdleConversationResetWrite` in
`bamboo-storage/src/v2/conversation_reset.rs`. Only the new reset transaction may
mint it. Its private fields bind the exact canonical Root lifetime, source
digest, reset generation, operation, Actor witnesses and full prepared candidate.
An API/model argument cannot construct or broaden it.

Acquire the existing lifecycle, runtime-Task and Session writer boundaries in
their documented order, followed by the corresponding Actor-tree boundary.
Every already-started filesystem job must retain those same physical guards
until completion. Reuse the established owned-job pattern; an aborted async HTTP
caller must not let a successor publication overtake its still-running job.

Under those final guards, read canonical Main/runtime, Root tool proof,
Supervisor proof when present, Actor record/marker and current Inbox/operation
state. Validate birth, Project, role-independent Root shape, proof/sidecar
agreement and the exact request observation. Observe only; do not ensure,
initialize, repair, claim, retire or recreate Actor authority to make reset pass.

The first transaction permits a never-activated valid inert Cold Root or a
previously activated Root with a validated terminal activation and confirmed
idle runtime. A historical attempt number is retained. Any live Reserved/
Running activation, stopping/finalizing process, unresolved physical work or
unknown authority blocks reset. Lease expiry alone is not physical termination.
Retired/contradictory records remain blocked in the first implementation until a
separate explicit lifecycle recovery establishes its supported state.

The reset command never stops work implicitly. The caller may use the existing
explicit expected-run stop operation, wait for its canonical terminal result,
then obtain a fresh reset preview. `cancel_requested` or a UI running badge
disappearing is insufficient.

## Observation, operation and revision ownership

The pure request contains `session_id`, `operation_id`, `expected_birth_token`,
`expected_conversation_revision` and `expected_snapshot_sha256`. The opaque birth
token reuses the existing Root lifetime token, including trusted identity. The
operation ID is `<expected_conversation_revision>:<canonical non-nil UUID>`.
No force, stop, caller-role, permission-mode or idle assertion is accepted.

Two fences are necessary:

1. A canonical conversation generation advances on each reset. All subsequent
   writes, execution admissions and history responses carry it. It prevents
   old snapshots or responses from reviving a pre-reset conversation.
2. A digest of the complete canonical Session observed at preview detects new
   messages or control/management changes before reset. A generation that only
   advances on reset cannot detect those ordinary changes. Digest generation is
   explicitly proportional to history; metadata object order is canonicalized.

The inline canonical ledger keeps the last eight immutable operation receipts.
Their birth, source digest, previous/resulting generation and retained System
count are frozen. After eviction, the epoch-prefixed operation ID and current
generation still refuse execution of an old operation. An exact retained replay
returns historical completion data; it never clears messages added later.
Current access and lifetime are still checked before returning that data.

This first freeze passes the ledger as a pure argument. The next schema freeze
must choose and implement its single canonical inline location, zero/absence
compatibility and bounded Main/proof projection. A prospective plan is not a
second persistence store.

## Candidate semantics

The pure transform keeps the first System message (zero or one) and removes the
other messages. It clears token usage, summary, compression events, prompt
snapshot, old response continuation, prompt assembly mirrors and hook contexts.
It purges all native transcript payload groups, including groups anchored to the
retained System message. Then it reuses the coupled model-context/native reset
with `ExplicitHistoryRewrite`, preserving monotonic epoch boundaries rather
than restarting them at zero. Model counters that cannot advance fail closed.

It preserves identity, birth, behavior version, Project/workspace, title/pin,
permissions, Root mode and authority revisions, goals, TaskList, independent Work
references, Inbox dedupe/admission records and Supervisor management scope/links.
It does not reset run budget counters. Pending questions/approval replay,
parent/Bash/Plan waits, legacy runtime mirrors, unconsumed injected input and
uncertain operations block preparation. The storage owner separately checks the
canonical Inbox and physical owner state; passing `IdleObserved` to a pure
function is not authorization.

Attachments, workspace files, independent Roots and Jiandu memory are not
deleted. Fresh prompt assembly may select current authorized durable context;
that is separate from replaying cleared conversation payloads.

## Recoverable publication

The old clear operation removes attachments and independently replaces runtime,
Main and search revision. It is not the reset transaction.

Use a reset-specific transaction envelope under the existing recovery boundary,
with one Root owner. Stage exact new Main/runtime bytes, inline generation/receipt
and their hashes. Bind the journal to canonical birth, original source digest,
Actor witness hashes and unchanged scope/proofs. Before replacing targets,
durably publish a prepared proof that makes operational reads and older writers
fail closed. All recovery uses the original operation and staged candidate;
there is no blind new-operation retry or reinterpretation of a pending command.

Because reset deletes no attachment/workspace files, a staged, hash-verified
roll-forward can complete the same reset without copying or restoring unrelated
user artifacts. Validate source/witness identity before recovery; quarantine
unknown or contradictory state instead of synthesizing empty success. A normal
read of a prepared/partial Root remains blocked until that recovery completes.

Only after Main/runtime and the committed proof agree may canonical readback,
cache replacement and an operation/generation-tagged account event report reset.
Search/index are rebuildable projections, not reset authority. Unknown I/O,
commit or readback outcomes return a stable unknown result and retain recoverable
operation identity. A recorded replay must include fresh observation or require
refetch, so it does not tell the UI that newer messages are absent.

## Exact shared writer and reader touchpoints

The transaction owner remains this worker; changes outside this freeze require
integrator coordination before editing. Minimize edits through shared final
generation validation instead of copying reset logic into every writer.

| Source | Required next-generation contract |
| --- | --- |
| Domain `session/types.rs`, `session/persistence.rs` | Add inline reset state/defaults; reject stale generation before append/merge or admitted-message restoration. New Child/copy/new Root gets its own zero state. |
| Domain `session/tool_authority.rs` and Engine execute/provider admission | A stale running or cached snapshot must reload or refuse on generation mismatch; do not adopt a new generation while retaining old messages. Coordinate with the A owner. |
| Storage `v2/compact_main.rs`, `v2/root_context.rs` | Version bounded Main/Root proof when nonzero reset state is present, validate inline ledger/generation and reject ordinary mutation. Old runtimes must refuse this new frame/proof. Preserve existing zero-state framing. |
| Storage `v2/conversation_reset.rs` and `v2.rs` recovery hooks | Mint exact idle reset capability, stage/recover one transaction, publish exact state and readback. No default-clear or fake mode-operation route. |
| Storage default writer and `root_actor_runtime.rs` | Compare incoming generation with canonical generation in the final physical job, including a stale snapshot that obtains a fresh activation lease. |
| Storage raw `actor_transcript.rs`, `actor_input.rs`, `actor_model_context.rs`, checkpoint lineage/snapshot | Carry and compare the actual new generation even when splicing raw bytes or using bounded snapshots. Neither old format projection nor a current activation fence can authorize stale context. |
| Storage Task, Supervisor-management, parent-answer and `session_merge` publishers | Their existing control-plane ownership stays unchanged; reject stale reset generation before full runtime publication. They cannot drop the reset ledger or restore old native/summary state. |
| Storage startup migration, copy/recreate and recovery | Preserve a known committed generation; unknown/prepared state stays unavailable. Copy/recreate is a distinct lifetime and must not inherit reset receipts. Do not backfill reset state into old Sessions. |
| Server detail/history/reset/operation status; account events | New capability/preflight fields, exact typed reset/recovery calls, canonical generation/receipt and outcome semantics. Existing `/clear` remains its own legacy contract. |

Supervisor identity/management proof bytes should remain unchanged when their
covered fields do not change. A reset generation can be covered by the common
Root proof rather than create a Supervisor-only permission. Independent managed
workers continue with their own identities, grants and generations.

## Dependency slices after this pure freeze

Do not implement the whole touchpoint table in one PR. The next bounded freeze
introduces the optional inline generation/ledger and common final-generation
guards/projections, retaining all existing zero-state behavior and exposing no
reset writer. Its acceptance must include stale snapshot plus fresh lease and
all alternate publishers before a nonzero generation can be created. If that
guard inventory exceeds one bounded review, split projection/default writers
from the raw Actor/control-plane adapters and keep reset unavailable until both
are accepted.

The following freeze adds only the private idle reset capability, staging,
recoverable publication and exact-operation readback on top of those completed
guards. Its original failure/abort/restart fixtures bind the actual transaction
source. The final interface slice registers HTTP preview/reset/recovery,
detail/history generations and account events, then joins the independently
owned Lotus UI. Actual controlled-provider payload and cross-client acceptance
remain required before the user outcome is called delivered.

## Required actual tests after the Rust window

The nine current Domain tests are preparation-only source. They do not prove a
storage transaction or actual provider request.

The next affected suite must create a Supervisor and an Ordinary Root in isolated
temporary data, finish a real controlled activation, reset through the registered
transaction, reopen the same data and compare identity/proof/scope/work facts.
Exercise every staged-publication fault and interrupted owned filesystem job;
exact operation recovery yields one generation, while changed arguments and
old generations refuse without another clear. Valid live/pending/unknown cases
must reject before any destructive publication or provider call.

Use an in-process capturing provider through the actual next runner request for
both ordinary messages and native PromptIR/Responses input. Persist old sentinel
messages, summary, model-context events, a native discovery group anchored to the
retained System message, response continuation and hook contexts. After reset and
restart, send a fresh instruction and inspect the actual provider payload:
old sentinels and previous-response links are absent; fresh authorized Project,
goal and Task context remains. A serialized candidate alone is insufficient.

Test a pre-reset snapshot obtaining a fresh Actor lease after reset, every shared
writer's stale-generation refusal, cross-process finalization/activation races,
unconsumed Inbox and pending approval preservation, receipt eviction, malformed
proofs and mixed old/new runtime refusal. Cross-client delayed history responses
must be fenced by the actual server generation before UI acceptance can claim
that cleared history cannot reappear.
