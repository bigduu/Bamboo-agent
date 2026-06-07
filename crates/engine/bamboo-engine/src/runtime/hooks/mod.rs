//! Hook runner — dispatches registered hooks at lifecycle points.

use std::sync::Arc;

use bamboo_agent_core::AgentHook;
use bamboo_agent_core::Session;
use bamboo_domain::{AgentHookPoint, AgentRuntimeState, HookCheckpoint, HookResult};
use chrono::Utc;

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
        session: &Session,
        runtime_state: &mut AgentRuntimeState,
    ) -> HookResult {
        let mut final_result = HookResult::Continue;

        for hook in &self.hooks {
            if hook.point() != point {
                continue;
            }

            let start = std::time::Instant::now();
            let result = hook.run(point, session).await;
            let elapsed = start.elapsed();

            runtime_state.checkpoints.push(HookCheckpoint {
                hook_point: format!("{:?}", point),
                timestamp: Utc::now(),
                result: format!("{:?}", result),
                duration_ms: elapsed.as_millis() as u64,
            });

            match &result {
                HookResult::Abort { .. } | HookResult::Suspend { .. } => return result,
                HookResult::Mutated => final_result = HookResult::Mutated,
                HookResult::Continue => {}
            }
        }

        final_result
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

        async fn run(&self, _point: AgentHookPoint, _session: &Session) -> HookResult {
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

        async fn run(&self, _point: AgentHookPoint, _session: &Session) -> HookResult {
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

        let result = runner
            .run_hooks(AgentHookPoint::BeforeRound, &session, &mut state)
            .await;

        assert_eq!(result, HookResult::Continue);
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

        let result = runner
            .run_hooks(AgentHookPoint::BeforeRound, &session, &mut state)
            .await;

        assert_eq!(result, HookResult::Continue);
        assert_eq!(state.checkpoints.len(), 2);
        // Lower priority runs first
        assert!(state.checkpoints[0].result.contains("Continue"));
    }

    #[tokio::test]
    async fn abort_short_circuits() {
        let mut runner = HookRunner::new();
        runner.register(Arc::new(AbortHook));

        let mut state = AgentRuntimeState::new("run-3");
        let session = test_session();

        let result = runner
            .run_hooks(AgentHookPoint::BeforeLlmCall, &session, &mut state)
            .await;

        assert!(matches!(result, HookResult::Abort { .. }));
        assert_eq!(state.checkpoints.len(), 1);
    }

    #[tokio::test]
    async fn wrong_point_hooks_are_skipped() {
        let mut runner = HookRunner::new();
        runner.register(Arc::new(AbortHook)); // registered for BeforeLlmCall

        let mut state = AgentRuntimeState::new("run-4");
        let session = test_session();

        let result = runner
            .run_hooks(AgentHookPoint::AfterRound, &session, &mut state)
            .await;

        assert_eq!(result, HookResult::Continue);
        assert!(state.checkpoints.is_empty());
    }
}
