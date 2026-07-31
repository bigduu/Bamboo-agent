use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use bamboo_agent_core::{AgentHook, Session};
use bamboo_config::LifecycleHooksConfig;
use bamboo_domain::{AgentHookPoint, HookPayload, HookResult};

/// Aggregate control and context output from one lifecycle seam.
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

/// One completed handler execution, returned to the engine for observability.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HookExecution {
    pub hook_name: String,
    pub point: AgentHookPoint,
    pub duration_ms: u64,
    pub result: HookResult,
}

/// Complete dispatch result for one lifecycle seam.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct HookDispatchReport {
    pub outcome: HookRunOutcome,
    pub executions: Vec<HookExecution>,
}

/// Registry and deterministic dispatcher for programmatic and configured hooks.
#[derive(Clone, Default)]
pub struct HookDispatcher {
    hooks: Vec<Arc<dyn AgentHook>>,
}

impl HookDispatcher {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a hook. Lower numeric priorities run first.
    pub fn register(&mut self, hook: Arc<dyn AgentHook>) {
        self.hooks.push(hook);
        self.hooks.sort_by_key(|hook| hook.priority());
    }

    /// Clone this registry and append configured handlers from a frozen config
    /// snapshot. The original registry remains reusable by future runs.
    pub fn with_lifecycle_config(
        &self,
        config: &LifecycleHooksConfig,
        fallback_cwd: Option<PathBuf>,
    ) -> Self {
        let mut dispatcher = self.clone();
        crate::configured::register_configured_hooks(&mut dispatcher, config, fallback_cwd);
        dispatcher
    }

    pub async fn run_hooks(
        &self,
        point: AgentHookPoint,
        payload: &HookPayload,
        session: &Session,
    ) -> HookDispatchReport {
        self.run_hooks_with_control(point, payload, session, true)
            .await
    }

    /// Run all matching handlers without allowing a control decision to
    /// suppress later observer handlers.
    pub async fn run_observer_hooks(
        &self,
        point: AgentHookPoint,
        payload: &HookPayload,
        session: &Session,
    ) -> HookDispatchReport {
        self.run_hooks_with_control(point, payload, session, false)
            .await
    }

    async fn run_hooks_with_control(
        &self,
        point: AgentHookPoint,
        payload: &HookPayload,
        session: &Session,
        honor_control_decisions: bool,
    ) -> HookDispatchReport {
        let mut report = HookDispatchReport::default();

        for hook in &self.hooks {
            if hook.point() != point || !hook.matches(payload) {
                continue;
            }

            let started = Instant::now();
            let result = hook.run(point, payload, session).await;
            report.executions.push(HookExecution {
                hook_name: hook.name().to_string(),
                point,
                duration_ms: started.elapsed().as_millis() as u64,
                result: result.clone(),
            });

            let (result, mut contexts) = unwrap_context_result(result);
            report.outcome.injected_contexts.append(&mut contexts);

            match &result {
                HookResult::Abort { .. }
                | HookResult::Suspend { .. }
                | HookResult::Deny { .. }
                | HookResult::Ask => {
                    if honor_control_decisions {
                        report.outcome.decision = result;
                        return report;
                    }
                }
                HookResult::InjectContext { text } => {
                    report.outcome.injected_contexts.push(text.clone());
                }
                HookResult::Mutated => {
                    if matches!(report.outcome.decision, HookResult::Continue) {
                        report.outcome.decision = HookResult::Mutated;
                    }
                }
                HookResult::Allow => report.outcome.decision = HookResult::Allow,
                HookResult::Continue => {}
                HookResult::WithContext { .. } => unreachable!("context results are unwrapped"),
            }
        }

        report
    }

    pub fn has_hooks_for(&self, point: AgentHookPoint) -> bool {
        self.hooks.iter().any(|hook| hook.point() == point)
    }

    pub fn len(&self) -> usize {
        self.hooks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.hooks.is_empty()
    }
}

fn unwrap_context_result(mut result: HookResult) -> (HookResult, Vec<String>) {
    let mut contexts = Vec::new();
    while let HookResult::WithContext {
        result: inner,
        text,
    } = result
    {
        if !text.trim().is_empty() {
            contexts.push(text);
        }
        result = *inner;
    }
    (result, contexts)
}
