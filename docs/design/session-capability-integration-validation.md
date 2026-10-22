# Session capability validation handoff

This is the first validation plan for the assembled capability path. The parent
must explicitly transfer the shared Rust window before any command below runs.
The browser owner currently holds that window. All commands and runtime outcomes
in this document are **UNRUN**. This document starts no account Session or clear.

Use the exact integration SHA/tree recorded alongside the source receipt. Keep
one checkout and one build/output writer, preserve the existing lockfile and
feature/profile policy, and record actual toolchain, frontend mode/artifact and
test filters. Source-only postimage and rustfmt receipts are separate evidence.
Do not substitute an earlier component build for the integrated candidate.

## First bounded Rust window

Compile the affected native test targets once in the granted output directory:

```sh
cargo test --locked --no-run -p bamboo-domain -p bamboo-agent-core -p bamboo-tools -p bamboo-storage -p bamboo-engine -p bamboo-server-tools -p bamboo-server -p bamboo-sdk
```

Then execute the affected synthetic groups below using the resulting native
artifacts and normal Cargo filters. Record selected/executed counts and failures;
a zero-selection command is not coverage. No full workspace, release matrix,
provider account, actual user history or actual permission configuration is part
of this first window.

| Package | Fixture source / selection | Required behavior |
| --- | --- | --- |
| `bamboo-domain` | `host_session_create`, `host_session_scope`, Session compatibility and tool authority modules | Deterministic operation identity, explicit finite rights, original caller generation and fresh Root selection |
| `bamboo-agent-core` | `tools/execution_contract.rs` and retained-operation coverage | Frozen owner/arguments and actual instance availability |
| `bamboo-tools` | Executor permission and scoped-send groups | Deny/Ask/Once, exact identity and existing shell mapping |
| `bamboo-storage` | `host_session_scope`, `v2::host_root_create`, `session_inbox::host_scope_tests`, `session_inbox::host_create_tests` | Canonical final guards, no fallback after install/revoke, physical job retention, exact first input and receipt retry |
| `bamboo-server-tools` | Integration tests `host_session_discovery`, `scoped_session_messaging`, `framework_session_tools` | Bounded authorized list/inspect/send; real common factory instances; retained and fresh calls |
| `bamboo-engine` | `external_agents::local_task::tool_policy::tests::framework_session`; Session schema/per-call framework cases; `session_app::approval_replay`; `root_execution_lifetime` | Complete outer permission chain, live/readonly/disabled gates, unknown replay refusal, actual outer join without a reset grant |
| `bamboo-server` | `app_state::supervised_root_create::tests`; installed peer-tool tests in `app_state/tools.rs` | Actual App caller port to canonical Inbox, role-neutral independent Root, ACK retry without activation and compatible Supervisor identity |
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

## C2 remains a later backend transaction

C4a contains real retained outer JoinHandles, exact original conversation
observations and deny-only admission reservations. Its six fixtures are source
only. Complete physical closure still returns Unsupported. Terminal Actor state,
an expired lease or an empty process list cannot become an idle reset grant.
External `AgentRunner`/`RunnerReservation` struct literals need migration because
the new ownership fields are private; integrated and external type compatibility
must be checked before admission.

The next C2 backend sequence reuses the existing generation consumers and pure
reset transform:

1. Bind a complete, provenance-covered Root resource issuer to actual run/birth/
   Host ownership. Join ready work without cancelling independent work. Unknown
   or pending effects refuse reset. Reuse the existing resource registry and the
   #2144 lifetime work rather than replacing them.
2. Complete the short logical router/preparation/removal admission handshake,
   then release publication locks before the final Storage transaction. The
   operation holds its exact nonce through physical work and cancelled waiters.
3. Add one recoverable context/ledger writer. Preserve Session identity, scope,
   managed Roots and Tasks; never delete/recreate. Reset messages/token/summary
   and all selected native/model-context state under one recorded generation.
   Active runs, approvals, unknown operations and recovery ambiguity have explicit
   Busy/Unsupported/uncertain outcomes, without automatic execution or retry.
4. Freeze actual preview/reset/status routes, success/error/readback fields and
   history/feed generation semantics only after that writer exists. HTTP request
   cancellation cannot imply rollback or success; clients recover by operation ID
   and canonical generation/readback.

Lotus owner `01a162d3-fbda-773a-b192-7c76bb545020` can wire C2 only after steps 1–4
have exact-source synthetic acceptance and a stable API receipt. C1 PR397 keeps
its current `/clear` contract until then. No C2 endpoint or cross-client revision
guarantee is available from this candidate.

Actual provider acceptance must perform the real transaction, close caches,
cold-reopen Storage and run a new round through the real provider encoder with
synthetic credentials. Capture positive old sentinels before and their absence
after, while preserved identity/scope/tasks and legal new context still work.
Engine `store:false` already omits `previous_response_id`; that absence alone is
not evidence. System-anchored native groups need a Storage purge assertion.
Anthropic loopback URLs disable native replay, so a test transport must preserve
the existing official-mode capability policy rather than weakening its guard.
This provider/reset acceptance is **UNRUN** and depends on the actual C2 writer.
