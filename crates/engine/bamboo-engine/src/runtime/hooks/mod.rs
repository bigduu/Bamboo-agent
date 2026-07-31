//! Hook runner — dispatches registered hooks at lifecycle points.

use std::sync::Arc;

use bamboo_agent_core::{AgentError, AgentEvent, AgentHook, Message, Session};
use bamboo_domain::{
    AgentHookPoint, AgentRuntimeState, AgentStatusState, HookCheckpoint, HookPayload, HookResult,
    SessionEndStatus, SuspensionState,
};
use chrono::Utc;
use tokio::sync::mpsc;

pub use bamboo_hooks::{
    test_lifecycle_handler, test_lifecycle_shell_command, HookRunOutcome, LifecycleHookEvent,
    LifecycleHookTestOutput, LifecycleScriptRunner, ScriptHook, ShellCommandHook, ShellHookEvent,
    ShellHookTestOutput,
};

/// Engine adapter around the standalone hook dispatcher.
///
/// `bamboo-hooks` owns matching and handler execution. This adapter translates
/// completed executions into engine checkpoints and lifecycle events.
#[derive(Clone)]
pub struct HookRunner {
    dispatcher: bamboo_hooks::HookDispatcher,
}

impl HookRunner {
    pub fn new() -> Self {
        Self {
            dispatcher: bamboo_hooks::HookDispatcher::new(),
        }
    }

    pub fn register(&mut self, hook: Arc<dyn AgentHook>) {
        self.dispatcher.register(hook);
    }

    /// Clone this registry and append configured handlers from one frozen config
    /// snapshot. The original registry remains reusable by future runs.
    pub fn with_lifecycle_config(
        &self,
        config: &bamboo_config::LifecycleHooksConfig,
        fallback_cwd: Option<std::path::PathBuf>,
    ) -> Self {
        Self {
            dispatcher: self.dispatcher.with_lifecycle_config(config, fallback_cwd),
        }
    }

    /// Run all hooks matching the given point.
    ///
    /// Records checkpoints in `runtime_state`. Returns the first
    /// `Suspend` or `Abort` result, or the aggregate result otherwise.
    pub async fn run_hooks(
        &self,
        point: AgentHookPoint,
        payload: &HookPayload,
        session: &Session,
        runtime_state: &mut AgentRuntimeState,
        event_tx: Option<&mpsc::Sender<AgentEvent>>,
    ) -> HookRunOutcome {
        let report = self.dispatcher.run_hooks(point, payload, session).await;
        record_dispatch_report(report, runtime_state, event_tx).await
    }

    /// Run every matching hook while recording checkpoints/events, but never
    /// short-circuit on a control decision. Observer/advisory seams such as
    /// `SessionEnd`, `PreCompact`, and `Notification` use this so a command's
    /// control-shaped output cannot suppress later hooks or reverse an
    /// operation that must proceed for correctness.
    pub async fn run_observer_hooks(
        &self,
        point: AgentHookPoint,
        payload: &HookPayload,
        session: &Session,
        runtime_state: &mut AgentRuntimeState,
        event_tx: Option<&mpsc::Sender<AgentEvent>>,
    ) -> HookRunOutcome {
        let report = self
            .dispatcher
            .run_observer_hooks(point, payload, session)
            .await;
        record_dispatch_report(report, runtime_state, event_tx).await
    }

    pub fn has_hooks_for(&self, point: AgentHookPoint) -> bool {
        self.dispatcher.has_hooks_for(point)
    }

    pub fn len(&self) -> usize {
        self.dispatcher.len()
    }

    pub fn is_empty(&self) -> bool {
        self.dispatcher.is_empty()
    }
}

async fn record_dispatch_report(
    report: bamboo_hooks::HookDispatchReport,
    runtime_state: &mut AgentRuntimeState,
    event_tx: Option<&mpsc::Sender<AgentEvent>>,
) -> HookRunOutcome {
    let bamboo_hooks::HookDispatchReport {
        outcome,
        executions,
    } = report;
    for execution in executions {
        runtime_state.checkpoints.push(HookCheckpoint {
            hook_point: format!("{:?}", execution.point),
            timestamp: Utc::now(),
            result: format!("{:?}", execution.result),
            duration_ms: execution.duration_ms,
        });
        if let Some(event_tx) = event_tx {
            let _ = event_tx
                .send(AgentEvent::HookLifecycle {
                    hook_name: execution.hook_name,
                    point: execution.point,
                    phase: "completed".to_string(),
                    duration_ms: execution.duration_ms,
                    decision: execution.result,
                })
                .await;
        }
    }
    outcome
}

/// Fire cleanup/notification hooks after a terminal run. Decisions and context
/// are intentionally ignored: `SessionEnd` observes a settled outcome and may
/// not reverse it. Suspended runs are non-terminal and do not fire this event.
pub(crate) async fn run_session_end_hooks(
    runner: &HookRunner,
    result: &Result<(), AgentError>,
    session: &mut Session,
    event_tx: &mpsc::Sender<AgentEvent>,
) {
    let suspended_non_terminal = result.is_ok()
        && session
            .metadata
            .get("runtime.suspend_reason")
            .is_some_and(|reason| !reason.trim().is_empty());
    if suspended_non_terminal || !runner.has_hooks_for(AgentHookPoint::AfterSessionEnd) {
        return;
    }

    let (status, completion_reason) = match result {
        Ok(()) => (
            SessionEndStatus::Completed,
            session
                .metadata
                .get("runtime.completion_reason")
                .cloned()
                .or_else(|| Some("completed".to_string())),
        ),
        Err(error) if error.is_cancelled() => {
            (SessionEndStatus::Cancelled, Some(error.to_string()))
        }
        Err(error) => (SessionEndStatus::Failed, Some(error.to_string())),
    };
    let mut runtime_state = session
        .agent_runtime_state
        .clone()
        .unwrap_or_else(|| AgentRuntimeState::new(&session.id));
    runner
        .run_observer_hooks(
            AgentHookPoint::AfterSessionEnd,
            &HookPayload::SessionEnd {
                status,
                completion_reason,
            },
            session,
            &mut runtime_state,
            Some(event_tx),
        )
        .await;
    session.agent_runtime_state = Some(runtime_state);
}

/// Apply context injections and non-tool control decisions consistently across
/// lifecycle seams.
pub(crate) fn apply_hook_outcome(
    point: AgentHookPoint,
    outcome: HookRunOutcome,
    session: &mut Session,
    runtime_state: &mut AgentRuntimeState,
) -> Result<(), AgentError> {
    if matches!(point, AgentHookPoint::AfterSessionSetup) {
        runtime_state.hook_contexts.extend(
            outcome
                .injected_contexts
                .into_iter()
                .filter(|text| !text.trim().is_empty()),
        );
    } else {
        inject_contexts(session, point, outcome.injected_contexts);
    }

    match outcome.decision {
        HookResult::Continue
        | HookResult::Mutated
        | HookResult::Allow
        | HookResult::InjectContext { .. } => Ok(()),
        HookResult::Suspend { reason } => {
            let hook_point = format!("{point:?}");
            runtime_state.status = AgentStatusState::Suspended;
            runtime_state.suspension = Some(SuspensionState {
                reason: reason.clone(),
                suspended_at: Utc::now(),
                resumable: true,
                hook_point: Some(hook_point.clone()),
            });
            session.metadata.insert(
                "runtime.suspend_reason".to_string(),
                "hook_suspended".to_string(),
            );
            Err(AgentError::HookSuspended(format!("{hook_point}: {reason}")))
        }
        HookResult::Abort { reason } => Err(AgentError::Tool(format!(
            "hook aborted at {point:?}: {reason}"
        ))),
        HookResult::Deny { reason } => Err(AgentError::Tool(format!(
            "hook denied lifecycle seam {point:?}: {reason}"
        ))),
        HookResult::Ask => Err(AgentError::Tool(format!(
            "hook requested parent approval at non-tool seam {point:?}"
        ))),
        HookResult::WithContext { result, text } => apply_hook_outcome(
            point,
            HookRunOutcome {
                decision: *result,
                injected_contexts: vec![text],
            },
            session,
            runtime_state,
        ),
    }
}

pub(crate) fn inject_contexts(
    session: &mut Session,
    point: AgentHookPoint,
    injected_contexts: Vec<String>,
) {
    for text in injected_contexts {
        if text.trim().is_empty() {
            continue;
        }
        let block =
            format!("\n\n<agent_hook_context point=\"{point:?}\">\n{text}\n</agent_hook_context>");
        if let Some(system_message) = session
            .messages
            .iter_mut()
            .find(|message| matches!(message.role, bamboo_agent_core::Role::System))
        {
            system_message.content.push_str(&block);
            system_message.never_compress = true;
        } else {
            let mut message = Message::system(block.trim().to_string());
            message.never_compress = true;
            message.metadata = Some(serde_json::json!({
                "runtime_kind": "hook_context",
                "hook_point": point,
            }));
            session.add_message(message);
        }
    }
}

/// Merge hook checkpoints produced through a session-local seam (notably
/// compression) into the runner-owned state without losing checkpoints written
/// directly by tool/round seams.
pub(crate) fn merge_session_hook_checkpoints(
    session: &Session,
    runtime_state: &mut AgentRuntimeState,
) {
    let Some(session_state) = session.agent_runtime_state.as_ref() else {
        return;
    };
    for checkpoint in &session_state.checkpoints {
        if !runtime_state.checkpoints.contains(checkpoint) {
            runtime_state.checkpoints.push(checkpoint.clone());
        }
    }
    if matches!(session_state.status, AgentStatusState::Suspended) {
        runtime_state.status = AgentStatusState::Suspended;
        runtime_state.suspension = session_state.suspension.clone();
    }
}

impl Default for HookRunner {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A no-op hook that always returns Continue.
    struct ContinueHook {
        point: AgentHookPoint,
        pri: u32,
        name: String,
    }

    #[async_trait::async_trait]
    impl AgentHook for ContinueHook {
        fn point(&self) -> AgentHookPoint {
            self.point
        }

        async fn run(
            &self,
            _point: AgentHookPoint,
            _payload: &HookPayload,
            _session: &Session,
        ) -> HookResult {
            HookResult::Continue
        }

        fn priority(&self) -> u32 {
            self.pri
        }

        fn name(&self) -> &str {
            &self.name
        }
    }

    /// A hook that always returns Abort.
    struct AbortHook;

    #[async_trait::async_trait]
    impl AgentHook for AbortHook {
        fn point(&self) -> AgentHookPoint {
            AgentHookPoint::BeforeLlmCall
        }

        async fn run(
            &self,
            _point: AgentHookPoint,
            _payload: &HookPayload,
            _session: &Session,
        ) -> HookResult {
            HookResult::Abort {
                reason: "test abort".to_string(),
            }
        }

        fn name(&self) -> &str {
            "abort_hook"
        }
    }

    fn test_session() -> Session {
        Session::new("test", "test-model")
    }

    #[tokio::test]
    async fn empty_runner_returns_continue() {
        let runner = HookRunner::new();
        let mut state = AgentRuntimeState::new("run-1");
        let session = test_session();
        let (tx, _rx) = mpsc::channel(4);

        let result = runner
            .run_hooks(
                AgentHookPoint::BeforeRound,
                &HookPayload::Round { round: 1 },
                &session,
                &mut state,
                Some(&tx),
            )
            .await;

        assert_eq!(result.decision, HookResult::Continue);
        assert!(state.checkpoints.is_empty());
    }

    #[tokio::test]
    async fn hooks_run_in_priority_order() {
        let mut runner = HookRunner::new();
        runner.register(Arc::new(ContinueHook {
            point: AgentHookPoint::BeforeRound,
            pri: 200,
            name: "slow".to_string(),
        }));
        runner.register(Arc::new(ContinueHook {
            point: AgentHookPoint::BeforeRound,
            pri: 50,
            name: "fast".to_string(),
        }));

        let mut state = AgentRuntimeState::new("run-2");
        let session = test_session();
        let (tx, mut rx) = mpsc::channel(4);

        let result = runner
            .run_hooks(
                AgentHookPoint::BeforeRound,
                &HookPayload::Round { round: 1 },
                &session,
                &mut state,
                Some(&tx),
            )
            .await;

        assert_eq!(result.decision, HookResult::Continue);
        assert_eq!(state.checkpoints.len(), 2);
        // Lower priority runs first
        assert!(state.checkpoints[0].result.contains("Continue"));
        assert!(matches!(
            rx.recv().await,
            Some(AgentEvent::HookLifecycle { hook_name, .. }) if hook_name == "fast"
        ));
    }

    #[tokio::test]
    async fn abort_short_circuits() {
        let mut runner = HookRunner::new();
        runner.register(Arc::new(AbortHook));

        let mut state = AgentRuntimeState::new("run-3");
        let session = test_session();
        let (tx, _rx) = mpsc::channel(4);

        let result = runner
            .run_hooks(
                AgentHookPoint::BeforeLlmCall,
                &HookPayload::None,
                &session,
                &mut state,
                Some(&tx),
            )
            .await;

        assert!(matches!(result.decision, HookResult::Abort { .. }));
        assert_eq!(state.checkpoints.len(), 1);
    }

    #[tokio::test]
    async fn wrong_point_hooks_are_skipped() {
        let mut runner = HookRunner::new();
        runner.register(Arc::new(AbortHook)); // registered for BeforeLlmCall

        let mut state = AgentRuntimeState::new("run-4");
        let session = test_session();
        let (tx, _rx) = mpsc::channel(4);

        let result = runner
            .run_hooks(
                AgentHookPoint::AfterRound,
                &HookPayload::Round { round: 1 },
                &session,
                &mut state,
                Some(&tx),
            )
            .await;

        assert_eq!(result.decision, HookResult::Continue);
        assert!(state.checkpoints.is_empty());
    }

    struct RecordingSessionEndHook {
        payloads: Arc<std::sync::Mutex<Vec<HookPayload>>>,
    }

    #[async_trait::async_trait]
    impl AgentHook for RecordingSessionEndHook {
        fn point(&self) -> AgentHookPoint {
            AgentHookPoint::AfterSessionEnd
        }

        async fn run(
            &self,
            _point: AgentHookPoint,
            payload: &HookPayload,
            _session: &Session,
        ) -> HookResult {
            self.payloads.lock().unwrap().push(payload.clone());
            // Decisions at SessionEnd are observability-only and must not
            // change the already-settled terminal outcome.
            HookResult::Deny {
                reason: "ignored cleanup decision".to_string(),
            }
        }
    }

    #[tokio::test]
    async fn session_end_fires_for_completed_failed_and_cancelled_and_ignores_decisions() {
        for (result, expected_status) in [
            (Ok(()), SessionEndStatus::Completed),
            (
                Err(AgentError::Tool("terminal failure".to_string())),
                SessionEndStatus::Failed,
            ),
            (Err(AgentError::Cancelled), SessionEndStatus::Cancelled),
        ] {
            let payloads = Arc::new(std::sync::Mutex::new(Vec::new()));
            let mut runner = HookRunner::new();
            runner.register(Arc::new(RecordingSessionEndHook {
                payloads: payloads.clone(),
            }));
            runner.register(Arc::new(RecordingSessionEndHook {
                payloads: payloads.clone(),
            }));
            let mut session = test_session();
            let (tx, _rx) = mpsc::channel(4);

            run_session_end_hooks(&runner, &result, &mut session, &tx).await;

            let recorded = payloads.lock().unwrap();
            assert_eq!(
                recorded.len(),
                2,
                "a denied observer must not suppress later cleanup hooks"
            );
            assert!(recorded.iter().all(|payload| matches!(
                payload,
                HookPayload::SessionEnd { status, .. } if *status == expected_status
            )));
            assert_eq!(
                session
                    .agent_runtime_state
                    .as_ref()
                    .map(|state| state.checkpoints.len()),
                Some(2)
            );
        }
    }

    #[tokio::test]
    async fn session_end_skips_suspended_non_terminal_runs() {
        let payloads = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut runner = HookRunner::new();
        runner.register(Arc::new(RecordingSessionEndHook {
            payloads: payloads.clone(),
        }));
        let mut session = test_session();
        session.metadata.insert(
            "runtime.suspend_reason".to_string(),
            "waiting_for_children".to_string(),
        );
        let (tx, _rx) = mpsc::channel(4);

        run_session_end_hooks(&runner, &Ok(()), &mut session, &tx).await;

        assert!(payloads.lock().unwrap().is_empty());
        assert!(session.agent_runtime_state.is_none());
    }
}
