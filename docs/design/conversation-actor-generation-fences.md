# Original execution generation at Actor boundaries (C3b1)

Stacks on C3a #2157 / `ec9bbe5c000b3c0982866cc51c63059e387bebe3`.
This is the next implementation slice of #2152. It adds no reset writer, HTTP
route, stop/claim override or actual data cleanup. All Rust/runtime tests remain
unrun until the parent hands over the build window. C3b2 recovery/control-plane
work and C4 reset transaction are still required before reset can be enabled.

## Immutable operation observation, separate from permission scope

`ConversationGenerationObservation` has a private complete optional ledger,
derives `Debug/Clone/PartialEq/Eq`, and has no Serialize, Deserialize or Default
implementation. `Session::conversation_generation_observation(&self)` captures
data only. `observation.check_canonical(&Session)` validates canonical state and
requires exact ledger equality, returning `ConversationGenerationError`.

This observes one operation's conversation. It does not grant permission or
prove identity/lifetime/Project. In particular, an absent old zero generation
does not replace those existing checks. Host authorization identity and scope
intent may remain stable across reset; a new operation separately observes the
current conversation. An already-running operation cannot refresh its frozen
observation from a newly claimed Actor lease or newer Session metadata.

`RootActorRuntimeWrite` now retains that original observation. Engine compares
the original Session with durable Root authority before claiming activation,
then captures from that original Session. The existing concrete write proof
checks canonical Main/runtime generation and the original observation on every
validation and checks the incoming candidate before context publication. Inbox
and event paths already retaining this proof therefore retain the same check.
No Weak/per-invocation acquire/closed shutdown behavior or lock order changes.

The Root tool-proof prepare/commit protocol is unchanged. A new generation check
does not reinterpret its existing temporary prepared selection proof during an
owned ordinary full save. The future reset/recovery protocol still needs its
separate quarantine and operational-read contract; this slice is not that proof.

## Raw final writers and readers

| Boundary | Implemented change |
| --- | --- |
| `ActorTranscriptAppend` | Requires the original `expected_conversation_generation`; current prefix equality is insufficient. A stale observation returns `ConversationChanged`, which existing prefix-race retry loops do not automatically retry. |
| `ActorInputCheckpoint`, `ActorClaimContextSeed` | Require the same original observation before Inbox claim filesystem/checkpoint/confirm processing. Refusal cannot consume, acknowledge or dispose an input claim. |
| `ActorModelContextCheckpoint` | Its existing complete `expected: Session` must match canonical generation before ledger/native/context mutation. A conflict returns `SnapshotConflict`. |
| Shared raw `Source::read_checked` | Validate canonical Main/runtime generation. Each initial operation compares its observation; retained final `unchanged` witnesses include the full Main, runtime and proof bytes, so replacement cannot cross another generation. Existing owned jobs retain physical guards after caller cancellation. |
| `PlainActorActivation` | Retain the initial observation across append/input/recheck requests. Engine conflict-readback, local tool checkpoint and already-admitted Inbox adoption refuse a different conversation before copying history. |
| Child Actor proof | Validate the complete generation of canonical pairs, incoming candidate and Root ancestor pairs. A Child cannot gain a nonzero Root ledger; an independent ancestor's valid generation is not copied into the Child. |
| Retained Actor sidecar `Identity` | Carry the inline ledger into the checked Main/runtime pair and v3 Root proof validation, instead of decoding it away. The public graph response is not a new conversation-generation feed. |
| Ordinary Root authority partial `MainIdentity` | Carry and compare the inline ledger; validate the side before Root-authority use. Full execution admission checks the canonical generation as well. The fixed Supervisor still uses its existing bounded committed proof. |
| Startup Root proof migration | Explicitly skip/refuse a nonzero pair if its proof is absent, even when the global migration marker is also absent. Matching files cannot manufacture a committed reset proof. |

All known Rust literals for the added Host-only request/owner fields are updated,
including existing Engine/Storage/worker fixture sources. The three worker
checkpoint literals occur in tests; no worker lifecycle or ticket code changed.
The semantic production changes touch 12 owners; remaining modified Rust paths
only keep existing fixture literals and new synthetic tests coherent.

## Tests and source qualification

Six new synthetic test cases extend the C3a fixture source:

- An original observation remains stale after its mutable source is replaced.
- A fresh Actor lease cannot refresh an old Root writer's observation.
- Raw append and model-context checkpoint refuse an old generation without
  writes; append with the current observation retains the ledger.
- Raw input refuses before admission/ACK, while a current observation can
  checkpoint and still leaves ACK to its owner.
- Startup cannot reconstruct a nonzero proof when both proof/marker are gone.
- Retained Actor projection accepts a valid v3 pair without erasing its state.

The fixtures use fresh synthetic TempDirs and the already disclosed private C3a
seeder. They do not exercise a production reset transaction or actual account.
All six new tests, all 13 inherited C3a tests, typechecking, Cargo/Clippy,
provider-payload and recovery/cross-client acceptance are **UNRUN**. Direct
rustfmt parsing/formatting, diff checking and exact-source review are only static
evidence. No compilation/runtime pass or protected-admission claim follows.

## Remaining source before a reset producer

C3b2 must still audit and fence every Task original/updated/current CAS and
recovery rollback, startup sidecar/Supervisor migrations, copy/recreate recovery,
Supervisor-management and parent-answer/broker/child-wait/scoped control-plane
publication. Common default fences do not alone prove recovery. Custom Host
persistence overrides and Core/SDK/Engine provider/tool admission need their
original-operation observation checks too; Host grants themselves are separate.

Only afterward may C4 implement private idle reset authority and durable staged
recovery with operation ownership, exact generation/snapshot CAS, fail-closed
unknown/prepared state and canonical readback. It must keep identity, scope,
managed independent Roots and unfinished work, preserve #1350's ordinary guard,
and reject active/pending/uncertain state without delete/recreate. Public
detail/history/feed generation and actual controlled provider payload acceptance
remain required. This slice cannot clear an activated-idle Supervisor yet.
