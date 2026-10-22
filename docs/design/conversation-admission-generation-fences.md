# Original conversation at SDK and Engine admission (C3b3)

This slice stacks on C3b2 #2168 /
`bd40b7ecf04513e6b6a294984d2c1e6d20c1a673`, under #2152. It reuses
the persisted ledger and immutable observation from C3a/C3b1. It adds no reset
writer or route, idle authority, scope grant, Actor claim protocol, storage
schema or lifecycle change. #1350 remains. Activated-idle reset is still pending.

## Original observation survives canonical readback

The Engine's private `check_conversation_readback(original, canonical)` validates
the original state and compares the complete ledger through the existing Domain
method. It never replaces the original observation with a fresh canonical one.
Same-revision receipt changes and unavailable canonical state for a nonzero
conversation are refused. A zero-generation in-memory Session may still have no
canonical snapshot, preserving the existing uncached embedder behavior. These
checks add no permission, birth evidence for legacy zero state, or write lease.

Generation failures use `AgentError::Tool` with the internal
`session_conversation_changed:` prefix. SDK admission recognizes this explicit
preparation refusal alongside execution-version refusal: it restores a borrowed
preappend snapshot and emits an error through stream APIs. Other existing error
semantics are unchanged. This is not an HTTP reset response or reset outcome.

| Existing boundary | Implemented check |
| --- | --- |
| SDK retained execution check | Compare the caller with canonical admission before String input append. Invalid state or nonzero state without canonical readback refuses execution. |
| Engine raw and registered admission | Compare before counted-loop early returns, including Sessions with no counted-loop metadata. The existing logical owner and physical binding checks remain. |
| SDK input seal | Capture immutable generation before invoking a custom checkpoint; after return (including an error), validate the mutable caller and then accepted readback against that original observation before publishing New input. Invalid generation and unavailable nonzero readback are explicit conversation refusals. Matching input id/digest alone is insufficient. A misbehaving custom persister may already have mutated its own data; this check cannot undo arbitrary Host writes or classify them as not executed. |
| Actual loop seal caller | Propagate an explicit conversation refusal out of the production loop before setup completion, provider or tool execution. Preserve the existing warning-and-continue behavior for ordinary optional Skill sealing failures. A later check cannot treat a custom Host's replaced caller as the original operation. Portable prompt hooks precede sealing; this refusal does not undo those earlier hooks or Host effects. |
| Initial/round Root authority adoption | Check generation before adopting mode/context authority or attempting initial Root publication. Scope identity itself is not revoked on reset. |
| Tool authority/permission refresh | Check before copying canonical posture or permitting the next tool boundary, including nonzero state without a configured storage reader. |
| Turn-boundary refresh | Check before permission audit/cursor adoption, queue migration, input claiming, history merge or input observation publication. A conflict is an explicit admission error with no adopted fields. |
| Legacy terminal queue migration | Refuse a different conversation before queue delivery or source clearing. Failure returns the existing unsuccessful/empty migration result. |
| Permanent Inbox duplicate | Check the durable message's conversation before copying it, merging admission state or ACK. A conflict leaves the claim recoverable and returns an admission error. |

No check changes an operation's generation from a new Actor lease, Host scope
or canonical snapshot. The new checks do not replace the physical final writer
guards from preceding slices or the future requirement for closed-and-joined
Host idleness before reset. A mere read does not freeze concurrent execution.

## Synthetic fixtures and current limits

Ten new test functions are supplied as source:

- Canonical readback compatibility, invalid state, unavailable nonzero state,
  exact receipt equality, and same-conversation append.
- Both public admission helpers with no counted-loop metadata, asserting no
  mutation/checkpoint on refusal and preserved zero-state behavior.
- SDK String, retained Session, stream and typed entry refusal before provider
  or action, using controlled canonical read-port payloads.
- SDK read race between preappend and admission, retaining borrowed memory and
  synthetic canonical files without checkpoint/provider/tool calls.
- Raw Engine `execute` and `execute_with_inputs` with recorded setup hook,
  provider and action counters remaining zero after generation refusal.
- SDK input seal where a custom checkpoint changes the caller or readback,
  including a different receipt at the same revision; no New input is emitted.
- Permanent Inbox receipt carrying matching typed message data from another
  generation, with no history adoption/write/ACK and retained claim.
- Root/tool/turn refresh refusing newer-generation authority, queued input and
  history without changing the original Session/runtime or synthetic storage.
- The complete production loop with custom checkpoint mutation of caller and/or
  canonical readback, same-revision receipt change or invalid incoming state:
  no AfterSessionSetup hook, provider, tool or Complete event follows refusal;
  fake Host data remains present. This is a primary loop execution fixture,
  not merely a direct call to the seal helper.
- A positive complete-loop fixture preserving ordinary optional seal failure
  behavior for a zero-state in-memory Host, with setup hook and synthetic
  provider actually reached when these tests are eventually run.

Controlled read ports model otherwise valid ledger payloads without performing
a production reset. Fresh TempDirs, an in-process probe provider and existing
synthetic Inbox fixtures are used; there are no real accounts or external
tools. These are source fixtures, not provider-payload acceptance after reset.

All ten new tests, inherited tests, typechecking, Cargo in every mode, Clippy,
mixed-runtime, fault/cancellation, actual controlled provider payload and
cross-client acceptance are **UNRUN** under the parent's build restriction.
Only direct rustfmt, diff checks and independent exact-source review may be
reported as static evidence. No compile/runtime pass or protected admission is
claimed by this Draft dependency.

## Remaining reset prerequisites

The lower-level tool/caller capability paths owned by A still need their
original operation ledger check at final use; adding Engine refresh checks does
not prove a custom direct tool caller safe. Scope grants remain separate from
the conversation observation. Broker/cold-reconciler and retained scoped
transport/Loop paths still need their final source audit. Default persistence
checks cannot constrain arbitrary custom Host overrides that bypass them.

Only after all writer/admission prerequisites are reviewed may a separate idle
reset writer be introduced. It must reuse actual Host closed-and-joined
lifecycle evidence and canonical pending/Actor witnesses, reject active or
uncertain work, fence old writers, and stage Main/runtime/proofs under a durable
operation-owned recovery/quarantine protocol. It must preserve identity, scope,
management, independent Roots and unfinished work. Terminal status, expired
lease, absent record or local absence is insufficient proof of idleness.

Canonical API/transaction and generation/readback/feed contracts must be stable
before the existing Lotus owner connects actual reset. C1 clear status handling
does not unlock activated Supervisor reset. Public cross-client versioning and
controlled actual provider request capture remain required; a local UI fence
and serializer-only context test cannot establish those outcomes.
