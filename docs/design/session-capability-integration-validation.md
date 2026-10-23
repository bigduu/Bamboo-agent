# Session capability validation handoff

This handoff covers the M1 union of the preserved `f2d2fd20` capability work,
the legacy prompt-snapshot repair and C1 clear diagnostics, plus the admitted
Workspace handoff interfaces. The frozen `6f9fb840b1fd2f0ec1529e45272541479216b1db`
(tree `d9e61c76ce7e408bc2c455b2757d519a891ec866`) compiled all twelve targets
successfully, then executed all 293 planned native cases: **283 passed, 10 failed,
0 ignored, 0 unrun**. This is a RED result. The follow-up fixture repairs below
have not been compiled or executed; these older results do not admit them.
The prior `991e264e` compile ended with SDK fixture UUID dependency errors and
ran zero tests; the preserved M1 dependency repair is not a passing result.
The previous Rust window was released. The parent must explicitly transfer a
new window before validation; this document starts no account Session or clear.

Use the exact integration SHA/tree recorded alongside the source receipt. Keep
one checkout and one build/output writer, preserve the existing lockfile and
feature/profile policy, and record actual toolchain, frontend mode/artifact and
test filters. Source-only postimage and rustfmt receipts are separate evidence.
Do not substitute an earlier component build for the integrated candidate.

## First bounded Rust window

Use the v3 source-bound handoff generated for the recorded freeze. It preserves
the original v2 plan's single combined eight-package library feature union plus
exactly the three named Server-tools integration targets. Preserve the
integration owner's toolchain, environment, lock, profile and output policy.
The older broad command is superseded; do not replace this union with separate
package builds or combine partial successful receipts.

After successful compilation, resolve the exact native artifacts and verify the
handoff's exact selections before execution. Preserve the original 262-case
mapping and explicitly inventory the nine pure Domain reset and six C1 clear
cases as its affected supplement. Include the existing Storage regression for
the nonzero ledger's legacy-clear refusal and independent zero-generation copy;
this closes the merged guard seam without adding a build target. Counts are
source planning, not listed or passed union tests; the separate small-slice
results are not union acceptance. The C1 six-case fixture does not itself test
an HTTP clear against a nonzero ledger.
Record actual selected/executed counts and failures; a zero-selection command
is not coverage. No full workspace, release matrix,
provider account, actual user history or actual permission configuration is part
of this first window.

| Package | Fixture source / selection | Required behavior |
| --- | --- | --- |
| `bamboo-domain` | `host_session_create`, `host_session_scope`, Session compatibility/tool authority and `conversation_reset` modules | Deterministic operation identity, finite rights, original caller generation, fresh Root selection and pure reset payload fences |
| `bamboo-agent-core` | `tools/execution_contract.rs` and retained-operation coverage | Frozen owner/arguments and actual instance availability |
| `bamboo-tools` | Executor permission and scoped-send groups | Deny/Ask/Once, exact identity and existing shell mapping |
| `bamboo-storage` | `host_session_scope`, `v2::host_root_create`, `session_inbox::host_scope_tests`, `session_inbox::host_create_tests` | Canonical final guards, no fallback after install/revoke, physical job retention, exact first input and receipt retry |
| `bamboo-server-tools` | Integration tests `host_session_discovery`, `scoped_session_messaging`, `framework_session_tools` | Bounded authorized list/inspect/send; real common factory instances; retained and fresh calls |
| `bamboo-engine` | `external_agents::local_task::tool_policy::tests::framework_session`; Session schema/per-call framework cases; `session_app::approval_replay`; `root_execution_lifetime` | Complete outer permission chain, live/readonly/disabled gates, unknown replay refusal, actual outer join without a reset grant |
| `bamboo-server` | `app_state::supervised_root_create::tests`; installed peer-tool tests in `app_state/tools.rs`; C1 clear diagnostics/compatibility/cache-history supplement | Actual App caller port to canonical Inbox, role-neutral independent Root, ACK retry, compatible Supervisor identity and definite/unknown clear outcomes |
| `bamboo-sdk` | `agent/builder.rs` affected default/discovery/explicit-policy cases; `approval_replay_tests::versioned_spawn` cases | Actual Builtin instances, policy precedence, absent delivery blocker and exact retained replay |

Minimum integration regressions include ordinary Root/Child/Supervisor with
explicit scope; creation without SendPeer/inspect rights; missing Project or
workspace; stale birth/full ledger/revoked scope; Deny/Ask and one typed Once;
ACKed creation input retried against a fresh activation router; legacy Supervisor
None-to-Host retry at one Inbox address; unknown legacy pending spawn preserved
and refused; same-named replacement without a proof; and awaited permission
checks followed by live Actor/config changes. Existing strict/IPC/native-eight
rejections must remain.

The current-head required `Test` compile-only CI and formal Codex head/base
review remain separate admission gates. This native window does not admit any
Draft or publish a release. A finding or source repair produces a new candidate
and an explicit affected revalidation record.

## Copy rollback review supplement

The `8c7a3130` union completed the full eleven-target build. Its native
selection guard then stopped before executing any test: two Engine response
cases used `respond::tests` instead of the actual `receipt_persistence_tests`
module. The corrected v5 manifest preserves all 278 function identities; its
full binary catalog is list-only evidence. Actual execution remains **0/278**.
The Rust window was returned for release priority before further execution.

PR #2187's copy-rollback P2 adds a separate source repair. A rollback first
retains the existing Main/runtime identity and zero-generation checks, durably
hides the canonical index row, and moves the directory to the transaction UUID
staging path. It synchronizes both rename parents and persists `.deleting`
before recursively removing staging. That phase never revisits canonical data
or removes an index row by ID, so a new Session at that ID survives a retry.
Same-UUID marker bodies must agree; `.deleting` wins over lower phases and is
deactivated last. A failed durable index hide retains the complete target and
recovery marker.

The next source-bound plan must preserve the original 278 cases and add all six
affected cases below, for 284 planned cases (259 library and 25 integration).
Their source presence is not an executed result; the repair has no native or
crash-recovery acceptance yet.

| Fixture | Source |
| --- | --- |
| `copy_rollback_deleting_resumes_after_partial_staging_cleanup` | `v2/conversation_generation_tests.rs`, new |
| `copy_rollback_deleting_preserves_a_recreated_target_and_wins_over_siblings` | same, new |
| `copy_rollback_prepared_staging_preserves_a_foreign_canonical` | same, new |
| `copy_rollback_unmarked_partial_canonical_refuses_without_mutation` | same, new |
| `a_retained_copy_rollback_marker_cannot_remove_a_reset_root` | same, updated real marker/EX guards |
| `copy_session_rolls_back_directory_when_index_publish_fails` | `v2.rs`, updated durable-hide refusal/reopen |

Unmarked legacy partial canonical data still lacks ownership evidence and is
retained with a refusal. V1 journals also lack an original birth witness for an
intact zero-generation target; this repair does not resolve that historical
ambiguity. Older binaries do not recognize `.deleting`; downgrade and mixed
version recovery are unqualified. These source changes do not implement C2 or
clear any real Session.

## Windows startup metrics repair remains unrun

On PR head `9bf7bd5fc1e3870f42c2bdaf42e9f4b10f67b74f`, the native
Windows job compiled and verified all 41 selected cases. Its checkout was PR
merge `361b75de4c113a9252cacec77357119b1be6ff82`, whose tree is exactly the
head tree `72e52a1da9429a84f8adf254f8d93f32a6bad3ae`. Execution produced nine
snapshot passes, two gateway passes and one gateway failure; the subsequent
29 cases were not executed. Artifact `12499637097` has SHA256
`2a0bc18528d804f3ed5a326e7ea047d831e740adc52ab8ca1fe6132d51512ff3`.
The failing gateway could not construct AppState: startup metrics reconciliation
returned SQLite `locking protocol`. It had not replaced the witness store or
written any corrupt authority input. This is not a copy-rollback assertion
failure, nor a passing result for the separate 284-case local plan.

The service initialized storage and then spawned a collector which initialized
the same database again. Its retention scheduler immediately queued a prune,
while AppState directly opened another connection for startup reconciliation.
The source repair sends reconciliation through that existing collector FIFO
and waits for its actual result. Initialization, preceding mutations, retention
and reconciliation use the same consumer, while initialization and channel
failure still return an error. Dropping a reconciliation receipt does not cancel
accepted database work. Standalone collector initialization, initial retention,
and owner-release drain semantics remain in place. No sleep, retry, weaker
SQLite isolation or gateway fixture exemption is introduced. The retained log
does not identify the failing SQL statement; this confirmed ordering gap is
not proof of the Windows failure's only cause.

Two new real-storage fixtures cover an uninitialized collector with queued
observations and a startup initialization I/O failure. Both keep their TempDir
alive until the real consumer releases its storage. The next bounded plan must
retain all prior 284 cases, add these two fixtures, the four existing collector
regressions below and the three gateway fixtures (293 unique cases). Compiling
the metrics unit fixtures adds one library target, for nine libraries and three
integration targets; no additional build is started without a new window.

- `metrics_service::tests::startup_reconciliation_waits_for_initialization_and_accepted_prefix`
- `metrics_service::tests::startup_reconciliation_reports_collector_initialization_failure`
- `collector::tests::final_owner_drop_before_initialization_drains_normal_and_prune_commands_in_order`
- `collector::tests::remaining_clone_keeps_collection_and_periodic_retention_alive`
- `collector::batch_tests::singleton_barriers_preserve_fifo_across_segments_and_retention_cascades`
- `collector::batch_tests::failed_ordinary_and_special_commands_do_not_discard_accepted_suffix`
- All three `handlers::agent::actor_snapshot::tests` gateway cases in the Windows job.

The exact `6f9fb840` twelve-target compilation subsequently passed with exit 0
and validation exit 0 at 14:48:30 UTC on 2026-10-23, in 2264.640 seconds,
source clean before and after. Receipt SHA256:
`36b589c4a45c6a9841bba23d3adaabc0d8db8799eb5f33f421fb95acb63bc028`;
log SHA256:
`2229e5b0a9566aa54880e93d79a0b011d354c3bbd552be24a99b163ec4fd06f1`.
The same head's Windows actor job passed all 41 selected cases. Local execution
was deferred for release priority, then resumed from those existing artifacts
without another Cargo command.

## Complete native RED and fixture contract repair

The original v7 manifest selected exactly 268 library and 25 integration cases.
The first failing library selector stopped after 16P/1F; the first failing
integration selector stopped after 1P/1F. A disjoint continuation selected only
the remaining 251 library and 23 integration cases, obtaining 244P/7F and
22P/1F respectively. Every selector accounted for its complete final native
summary. The union has 293 unique executed cases, with no duplicates, omissions,
extra cases or ignored cases. All four wrappers exited 1. No partial run or
zero-selection result is counted as passing.

The frozen source remained unchanged through all four runs. Aggregate RED
receipt SHA256:
`94941681fe605c01eb3984b31d41946a6b97d49a6a084615120956a94dc990ff`.
The Rust window was formally returned at 16:33:59 UTC after a fresh empty
Rust/linker/Session-fixture process check, with no queued heavy work. Return
receipt SHA256:
`167716e9d1578ce169038c0b63f8d64349dd0e858aa958676fc3db5ac8692bdd`.

Ten failures exposed fixture expectations that had not followed the integrated
contracts. This follow-up changes tests, not production authority:

- Four Storage/scoped-send tests must advance a Root's metadata revision when
  changing its already persisted Project. The recreated-birth first-input case
  likewise restores its Project using the next revision; final original caller,
  reserved-target, generation and retained physical-lock assertions remain.
- The discovery Child must receive the same explicit Project before its first
  save. Child construction does not infer that Host scope, and a published Child
  must not be rebound afterward.
- The stacked overlay fixture must expect fresh Root creation for all supported
  Session versions and refuse a missing Root owner without entering `SubAgent`.
  Actual application dispatch must bind the Session before evaluating aliases.
- The SDK canonical orchestration set now includes the common `session_history`.
  The test verifies its actual framework instance and preserves the exact
  registered-intersection/provider-schema and Skill-denial assertions.
- A Host-recorded approval keeps its exact original owner even if a new custom
  registration shadows its spelling. The SDK test verifies that owner's single
  authorized execution, independent fresh-custom permission, and an explicit
  old typed/no-Host-record mismatch refusal without execution.
- A latest-Session Server/Connect resume after a tool-authority revision change
  refuses the stale original caller before clearing its pending markers. Tests
  preserve the operation/request/receipt and verify zero execution, zero new
  provider calls and no restored once grant. The separate SDK stale-snapshot
  path still checks its later current-posture terminal refusal.

These fixture changes are source-only and **UNRUN**. A new exact-source build,
affected native verification, current-head CI and clean head/base review are
still required. They do not implement or validate C2 reset.

## Separate C2 source is not admitted

M1 contains generation consumers and C4a's retained outer JoinHandles and
admission reservations; it has no complete reset writer/API/idle issuer. A
terminal Actor state, expired lease or empty process list is not an idle grant.
External `AgentRunner`/`RunnerReservation` struct literals still need migration
because the ownership fields are private.

The separate C2 checkout is frozen at
`f3838a4cab2f54424b85e638320cd6fe36161c75`
(tree `5c4648f4b194d58147b7260cf6a894086c93d36c`). Its source now contains the
actual resource/logical admission composition, transactional writer and
preview/reset/status HTTP routes. Compilation, all native fixtures and actual
provider-payload acceptance remain **UNRUN**. This source is not part of M1;
a C2 build must first normally merge the final validated M1 repair chain.

Supervisor reset remains 501 Unsupported, including after actual same-Host
bootstrap and execution. An Actor-free birth cannot exclude a foreign Host
producer before its first durable checkpoint. History-only preparation-to-main
continuity also lacks its actual complete private bridge. A cold Host refuses an
active durable reset journal; automatic full crash recovery is not implemented.
Ordinary positive and Supervisor negative provider fixtures are source assertions
only. Stateful previous-response-ID positive acceptance and Anthropic remain
outside the existing Responses fixture.

Lotus owner `01a162d3-fbda-773a-b192-7c76bb545020` can receive the existing C1
outcome/readback fixtures and C2 contract for isolated client review. C1 PR397
keeps its existing `/clear` contract. Production C2 wiring remains blocked until
exact-source backend acceptance and the stable readback/error/revision handoff;
no frontend change may label a 501 refusal as successful Supervisor clearing.
The C2 source receipt explicitly records frontend handoff as NOT_READY.
