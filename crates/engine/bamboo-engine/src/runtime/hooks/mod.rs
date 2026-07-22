//! Hook runner — dispatches registered hooks at lifecycle points.

use std::sync::Arc;

use bamboo_agent_core::{AgentError, AgentEvent, AgentHook, Message, Session};
use bamboo_domain::{
    AgentHookPoint, AgentRuntimeState, AgentStatusState, HookCheckpoint, HookPayload, HookResult,
    SuspensionState,
};
use chrono::Utc;
use tokio::sync::mpsc;

/// Aggregate output from every hook registered at one seam.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HookRunOutcome {
    pub decision: HookResult,
    pub injected_contexts: Vec<String>,
}

impl Default for HookRunOutcome {
    fn default() -> Self {
        Self {
            decision: HookResult::Continue,
            injected_contexts: Vec::new(),
        }
    }
}

/// Runs registered hooks at a given hook point.
#[derive(Clone)]
pub struct HookRunner {
    hooks: Vec<Arc<dyn AgentHook>>,
}

impl HookRunner {
    pub fn new() -> Self {
        Self { hooks: Vec::new() }
    }

    /// Register a hook. Hooks are sorted by priority (lower runs first).
    pub fn register(&mut self, hook: Arc<dyn AgentHook>) {
        self.hooks.push(hook);
        self.hooks.sort_by_key(|h| h.priority());
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
        let mut outcome = HookRunOutcome::default();

        for hook in &self.hooks {
            if hook.point() != point {
                continue;
            }

            let start = std::time::Instant::now();
            let result = hook.run(point, payload, session).await;
            let elapsed = start.elapsed();

            runtime_state.checkpoints.push(HookCheckpoint {
                hook_point: format!("{:?}", point),
                timestamp: Utc::now(),
                result: format!("{:?}", result),
                duration_ms: elapsed.as_millis() as u64,
            });

            if let Some(event_tx) = event_tx {
                let _ = event_tx
                    .send(AgentEvent::HookLifecycle {
                        hook_name: hook.name().to_string(),
                        point,
                        phase: "completed".to_string(),
                        duration_ms: elapsed.as_millis() as u64,
                        decision: result.clone(),
                    })
                    .await;
            }

            match &result {
                HookResult::Abort { .. }
                | HookResult::Suspend { .. }
                | HookResult::Deny { .. }
                | HookResult::Ask => {
                    outcome.decision = result;
                    return outcome;
                }
                HookResult::InjectContext { text } => {
                    outcome.injected_contexts.push(text.clone());
                    outcome.decision = result;
                }
                HookResult::Mutated => outcome.decision = HookResult::Mutated,
                HookResult::Allow => {
                    if matches!(outcome.decision, HookResult::Continue) {
                        outcome.decision = HookResult::Allow;
                    }
                }
                HookResult::Continue => {}
            }
        }

        outcome
    }

    /// Check if any hooks are registered for the given point.
    pub fn has_hooks_for(&self, point: AgentHookPoint) -> bool {
        self.hooks.iter().any(|h| h.point() == point)
    }

    /// Number of registered hooks.
    pub fn len(&self) -> usize {
        self.hooks.len()
    }

    /// Whether any hooks are registered.
    pub fn is_empty(&self) -> bool {
        self.hooks.is_empty()
    }
}

/// Apply context injections and non-tool control decisions consistently across
/// lifecycle seams.
pub(crate) fn apply_hook_outcome(
    point: AgentHookPoint,
    outcome: HookRunOutcome,
    session: &mut Session,
    runtime_state: &mut AgentRuntimeState,
) -> Result<(), AgentError> {
    inject_contexts(session, point, outcome.injected_contexts);

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
}
