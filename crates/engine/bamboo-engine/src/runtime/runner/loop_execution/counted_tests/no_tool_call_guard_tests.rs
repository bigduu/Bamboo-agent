//! Full-loop coverage for the bounded no-tool-call progress guard (#2425).
//!
//! The helper tests in `pipeline.rs` pin the streak arithmetic, but the bug this
//! guard fixes lived in the *loop*: the counter has to accumulate across rounds,
//! which a helper test cannot observe. These tests drive the real
//! `run_agent_loop_with_config` over several rounds.

use super::*;
use bamboo_agent_core::AgentHook;
use bamboo_domain::{AgentHookPoint, HookPayload};
use std::sync::atomic::Ordering;

/// The loop's own continuation budget, resolved through the module that owns it.
const BUDGET: u32 =
    crate::runtime::runner::loop_execution::pipeline::MAX_NO_TOOL_CALL_CONTINUATIONS;

/// Blocks every requested stop, so a no-tool-call round is told to continue
/// instead of completing. This is what makes the loop dispatch another round
/// without any tool use — the exact shape of the runaway in #2425.
struct AlwaysBlockStopHook;

#[async_trait]
impl AgentHook for AlwaysBlockStopHook {
    fn point(&self) -> AgentHookPoint {
        AgentHookPoint::BeforeFinalize
    }

    async fn run(
        &self,
        _point: AgentHookPoint,
        _payload: &HookPayload,
        _session: &Session,
    ) -> HookResult {
        HookResult::Deny {
            reason: "keep going".to_string(),
        }
    }
}

fn always_blocking_config() -> AgentLoopConfig {
    let mut runner = crate::runtime::hooks::HookRunner::new();
    runner.register(std::sync::Arc::new(AlwaysBlockStopHook));
    AgentLoopConfig {
        // No counted loop: this is the unbudgeted shape the guard protects.
        counted_loop: None,
        model_name: Some("fake-only".into()),
        prompt_memory_flags: PromptMemoryFlags {
            project_prompt_injection: false,
            relevant_recall: false,
            relevant_recall_rerank: false,
            project_first_dream: false,
            ledger_agenda: false,
        },
        hook_runner: std::sync::Arc::new(runner),
        ..Default::default()
    }
}

/// The guard must cut a non-productive loop short, and it must do so by
/// accumulating across rounds — the property that was broken when the counter
/// was declared inside the loop body and therefore reset every iteration.
#[tokio::test]
async fn unbudgeted_loop_stops_after_the_no_tool_call_budget() {
    let mut session = Session::new("no-tool-guard", "fake-only");
    // The default FakeProvider answers with a plain token: no tool calls.
    let provider = std::sync::Arc::new(FakeProvider::default());

    let (result, events) = run(
        &mut session,
        always_blocking_config(),
        provider.clone(),
        CancellationToken::new(),
    )
    .await;

    result.unwrap();
    let calls = provider.calls.load(Ordering::SeqCst);

    // The stop hook alone would allow MAX_STOP_HOOK_CONTINUATIONS (5) more
    // rounds. The guard must fire first, so the run cannot reach 5.
    assert!(
        calls <= BUDGET as usize,
        "guard must bound the loop: provider was called {calls} times, \
         budget is {BUDGET}"
    );
    assert!(
        calls > 1,
        "the guard must only act after the streak accumulates across rounds \
         (a per-round counter would stop after a single call); got {calls}"
    );
    assert_eq!(
        complete_count(&events),
        1,
        "a stopped run still emits exactly one terminal Complete"
    );
}

/// A productive loop is never cut short: every round that uses tools resets the
/// streak, so an agent doing real work keeps running past the budget.
#[tokio::test]
async fn productive_rounds_are_never_stopped_by_the_guard() {
    let mut session = Session::new("tool-guard", "fake-only");
    let provider = std::sync::Arc::new(FakeProvider {
        tools_forever: true,
        ..FakeProvider::default()
    });

    let config = always_blocking_config();

    let (result, _events) = run(
        &mut session,
        config,
        provider.clone(),
        CancellationToken::new(),
    )
    .await;

    // The run ends because the stop-hook continuation cap is reached, not
    // because of the no-tool-call guard.
    result.unwrap();
    let calls = provider.calls.load(Ordering::SeqCst);
    assert!(
        calls > BUDGET as usize,
        "a loop that uses tools every round must survive the no-tool-call \
         budget; got {calls} calls"
    );
}
