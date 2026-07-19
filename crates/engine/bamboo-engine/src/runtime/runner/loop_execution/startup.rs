use crate::runtime::config::{AgentLoopConfig, AuxiliaryModelConfig};
use crate::runtime::gold_evaluation::{AsyncGoldEvaluationRequest, AsyncGoldEvaluationResult};
use crate::runtime::runner::task_lifecycle::{
    AsyncTaskEvaluationRequest, AsyncTaskEvaluationResult,
};
use crate::runtime::task_context::TaskLoopContext;
use bamboo_agent_core::tools::ToolExecutor;
use bamboo_agent_core::Session;
use bamboo_domain::{AgentRuntimeState, AgentStatusState};
use bamboo_metrics::MetricsCollector;

use super::super::logging::DebugLogger;
use crate::runtime::runner::state_bridge;

const MAX_CONSECUTIVE_OVERFLOW_RECOVERIES: usize = 3;

#[derive(Debug, Clone, Default)]
pub(super) struct OverflowRecoveryState {
    pub(super) total_recoveries: usize,
    pub(super) consecutive_recoveries: usize,
    pub(super) last_recovered_round: Option<usize>,
}

impl OverflowRecoveryState {
    pub(super) fn can_attempt_recovery(&self) -> bool {
        self.consecutive_recoveries < MAX_CONSECUTIVE_OVERFLOW_RECOVERIES
    }

    pub(super) fn record_recovery(&mut self, round: usize) {
        self.total_recoveries += 1;
        self.consecutive_recoveries += 1;
        self.last_recovered_round = Some(round);
    }

    pub(super) fn reset_after_stable_round(&mut self) {
        self.consecutive_recoveries = 0;
    }
}

pub(super) struct InFlightTaskEvaluation {
    pub(super) request: AsyncTaskEvaluationRequest,
    // `Option<..>` output: the spawned future `select!`s on the run's
    // `CancellationToken`, so a cancelled run yields `None` (the eval work was
    // dropped at an await point, stopping the wasted LLM spend) instead of a
    // completed result. See `spawn_task_evaluation_request`.
    pub(super) join_handle: tokio::task::JoinHandle<Option<AsyncTaskEvaluationResult>>,
}

pub(super) struct InFlightGoldEvaluation {
    pub(super) request: AsyncGoldEvaluationRequest,
    // `Option<..>` output: `None` when the run was cancelled while the Gold
    // evaluation was in flight (see `spawn_gold_evaluation_request`).
    pub(super) join_handle: tokio::task::JoinHandle<Option<AsyncGoldEvaluationResult>>,
}

#[derive(Default)]
pub(super) struct TaskEvaluationState {
    pub(super) in_flight: Option<InFlightTaskEvaluation>,
    pub(super) completed: Option<AsyncTaskEvaluationResult>,
    pub(super) queued_request: Option<AsyncTaskEvaluationRequest>,
}

#[derive(Default)]
pub(super) struct GoldEvaluationState {
    pub(super) in_flight: Option<InFlightGoldEvaluation>,
    pub(super) completed: Option<AsyncGoldEvaluationResult>,
    pub(super) queued_request: Option<AsyncGoldEvaluationRequest>,
}

pub(super) struct LoopRunState {
    pub(super) session_id: String,
    pub(super) model_name: String,
    pub(super) metrics_collector: Option<MetricsCollector>,
    pub(super) debug_logger: DebugLogger,
    pub(super) task_context: Option<TaskLoopContext>,
    pub(super) overflow_recovery: OverflowRecoveryState,
    pub(super) task_evaluation: TaskEvaluationState,
    pub(super) gold_evaluation: GoldEvaluationState,
    pub(super) auxiliary_models: AuxiliaryModelConfig,
    /// Structured runtime state persisted alongside the session.
    pub(super) runtime_state: AgentRuntimeState,
}

pub(super) fn resolve_auxiliary_models(config: &AgentLoopConfig) -> AuxiliaryModelConfig {
    config
        .auxiliary_model_resolver
        .as_ref()
        .map(|resolver| resolver())
        .unwrap_or_else(|| AuxiliaryModelConfig {
            fast_model_name: config.fast_model_name.clone(),
            fast_model_provider: config.fast_model_provider.clone(),
            background_model_name: config.background_model_name.clone(),
            planning_model_name: config.planning_model_name.clone(),
            search_model_name: config.search_model_name.clone(),
            summarization_model_name: config.summarization_model_name.clone(),
            background_model_provider: config.background_model_provider.clone(),
            summarization_model_provider: config.summarization_model_provider.clone(),
        })
}

pub(super) async fn initialize_loop_state(
    session: &mut Session,
    initial_message: &str,
    config: &AgentLoopConfig,
    tools: &dyn ToolExecutor,
) -> super::super::Result<LoopRunState> {
    let debug_logger = DebugLogger::new(tracing::enabled!(tracing::Level::DEBUG));
    let session_id = session.id.clone();
    let metrics_collector = config.metrics_collector.clone();
    let model_name = config
        .model_name
        .clone()
        .unwrap_or_else(|| "unknown".to_string());

    super::super::metrics_lifecycle::record_session_started(
        metrics_collector.as_ref(),
        &session_id,
        &model_name,
        session.created_at,
        session.messages.len() as u32,
    );

    tracing::debug!(
        "[{}] Starting agent loop with message: {}",
        session_id,
        initial_message
    );
    debug_logger.log_event(
        &session_id,
        "agent_loop_start",
        serde_json::json!({
            "message": initial_message,
            "max_rounds": config.max_rounds,
            "initial_message_count": session.messages.len(),
        }),
    );

    let auxiliary_models = resolve_auxiliary_models(config);

    let must_resume_pinned_activation = session
        .agent_runtime_state
        .as_ref()
        .is_some_and(|previous| matches!(previous.status, AgentStatusState::Suspended));
    let mut runtime_state = AgentRuntimeState::new(&session_id);
    // "Bypass permissions" is a per-session sticky toggle (set via PATCH /sessions
    // and persisted in runtime.json). Each run rebuilds a fresh runtime state, so
    // carry the flag forward from the prior state instead of resetting it.
    runtime_state.bypass_permissions = session
        .agent_runtime_state
        .as_ref()
        .is_some_and(|prev| prev.bypass_permissions);
    // #73: "no interactive human approver" (headless / scheduled / deployed) is
    // likewise a sticky per-session flag; carry it forward so every run — and the
    // sub-agents it spawns (which inherit it) — route gated actions to the
    // off-loop model-reviewer instead of escalating to an absent human.
    runtime_state.no_human_approver = session
        .agent_runtime_state
        .as_ref()
        .is_some_and(|prev| prev.no_human_approver);
    runtime_state.llm.model_name = Some(model_name.clone());
    runtime_state.llm.provider_name = config.provider_name.clone();
    runtime_state.llm.fast_model_name = auxiliary_models.fast_model_name.clone();
    runtime_state.llm.background_model_name = auxiliary_models.background_model_name.clone();
    runtime_state.round.max_rounds = config.max_rounds as u32;
    state_bridge::sync_from_metadata(session, &mut runtime_state);
    runtime_state.status = AgentStatusState::Initializing;
    state_bridge::write_runtime_state(session, &runtime_state);
    runtime_state.status = AgentStatusState::Running;

    let task_context = super::super::session_setup::prepare_session_for_loop(
        session,
        initial_message,
        config,
        tools,
        metrics_collector.as_ref(),
        &session_id,
        &debug_logger,
        must_resume_pinned_activation,
    )
    .await?;

    Ok(LoopRunState {
        session_id,
        model_name,
        metrics_collector,
        debug_logger,
        task_context,
        overflow_recovery: OverflowRecoveryState::default(),
        task_evaluation: TaskEvaluationState::default(),
        gold_evaluation: GoldEvaluationState::default(),
        auxiliary_models,
        runtime_state,
    })
}

#[cfg(test)]
mod tests {
    use super::{initialize_loop_state, resolve_auxiliary_models, OverflowRecoveryState};
    use crate::runtime::config::{AgentLoopConfig, AuxiliaryModelConfig};
    use async_trait::async_trait;
    use bamboo_agent_core::tools::{
        FunctionSchema, ToolCall, ToolExecutor, ToolResult, ToolSchema,
    };
    use bamboo_agent_core::Session;
    use bamboo_domain::{AgentRuntimeState, AgentStatusState};
    use bamboo_skills::runtime_metadata::{
        LOADED_SKILL_IDS_METADATA_KEY, SKILL_RUNTIME_ACTIVATION_GENERATION_KEY,
        SKILL_RUNTIME_SELECTED_SKILL_MODE_KEY, SKILL_RUNTIME_SELECTED_SKILL_REVISIONS_KEY,
        SKILL_RUNTIME_SELECTION_SOURCE_KEY,
    };
    use bamboo_skills::{SkillManager, SkillStoreConfig};
    use std::collections::BTreeSet;
    use std::sync::{Arc, Mutex};

    struct SuccessfulLoadSkill;

    #[async_trait]
    impl ToolExecutor for SuccessfulLoadSkill {
        async fn execute(
            &self,
            _call: &ToolCall,
        ) -> bamboo_agent_core::tools::executor::Result<ToolResult> {
            Ok(ToolResult {
                success: true,
                result: serde_json::json!({"instructions": "pinned"}).to_string(),
                display_preference: None,
                images: Vec::new(),
            })
        }

        fn list_tools(&self) -> Vec<ToolSchema> {
            vec![ToolSchema {
                schema_type: "function".to_string(),
                function: FunctionSchema {
                    name: "load_skill".to_string(),
                    description: "load".to_string(),
                    parameters: serde_json::json!({"type": "object"}),
                },
            }]
        }
    }

    #[test]
    fn overflow_recovery_state_tracks_recoveries_and_resets() {
        let mut state = OverflowRecoveryState::default();
        assert!(state.can_attempt_recovery());

        state.record_recovery(0);
        assert_eq!(state.total_recoveries, 1);
        assert_eq!(state.consecutive_recoveries, 1);
        assert_eq!(state.last_recovered_round, Some(0));
        assert!(state.can_attempt_recovery());

        state.record_recovery(1);
        state.record_recovery(2);
        assert_eq!(state.total_recoveries, 3);
        assert_eq!(state.consecutive_recoveries, 3);
        assert!(!state.can_attempt_recovery());

        state.reset_after_stable_round();
        assert_eq!(state.consecutive_recoveries, 0);
        assert!(state.can_attempt_recovery());
        assert_eq!(state.total_recoveries, 3);
    }

    #[test]
    fn auxiliary_model_resolver_returns_latest_values() {
        let counter = Arc::new(Mutex::new(0usize));
        let counter_for_resolver = counter.clone();
        let config = AgentLoopConfig {
            auxiliary_model_resolver: Some(Arc::new(move || {
                let mut guard = counter_for_resolver.lock().expect("counter lock");
                *guard += 1;
                AuxiliaryModelConfig {
                    fast_model_name: Some(format!("fast-{}", *guard)),
                    background_model_name: Some(format!("bg-{}", *guard)),
                    summarization_model_name: Some(format!("sum-{}", *guard)),
                    ..Default::default()
                }
            })),
            ..Default::default()
        };

        let first = resolve_auxiliary_models(&config);
        let second = resolve_auxiliary_models(&config);

        assert_eq!(first.fast_model_name.as_deref(), Some("fast-1"));
        assert_eq!(first.background_model_name.as_deref(), Some("bg-1"));
        assert_eq!(first.summarization_model_name.as_deref(), Some("sum-1"));
        assert_eq!(second.fast_model_name.as_deref(), Some("fast-2"));
        assert_eq!(second.background_model_name.as_deref(), Some("bg-2"));
        assert_eq!(second.summarization_model_name.as_deref(), Some("sum-2"));
    }

    #[test]
    fn auxiliary_model_resolver_refreshes_summarization_model_between_calls() {
        let counter = Arc::new(Mutex::new(0usize));
        let counter_for_resolver = counter.clone();
        let config = AgentLoopConfig {
            auxiliary_model_resolver: Some(Arc::new(move || {
                let mut guard = counter_for_resolver.lock().expect("counter lock");
                *guard += 1;
                AuxiliaryModelConfig {
                    summarization_model_name: Some(format!("sum-{}", *guard)),
                    ..Default::default()
                }
            })),
            ..Default::default()
        };

        let first = resolve_auxiliary_models(&config);
        let second = resolve_auxiliary_models(&config);

        assert_eq!(first.summarization_model_name.as_deref(), Some("sum-1"));
        assert_eq!(second.summarization_model_name.as_deref(), Some("sum-2"));
    }

    #[tokio::test]
    async fn startup_reuses_retained_activation_and_explicit_selection_supersedes_it() {
        let directory = tempfile::tempdir().expect("tempdir");
        let skills_dir = directory.path().join("skills");
        for (id, prompt) in [("review-demo", "review N"), ("plan-demo", "plan N")] {
            let root = skills_dir.join(id);
            tokio::fs::create_dir_all(&root).await.expect("skill root");
            tokio::fs::write(
                root.join("SKILL.md"),
                format!("---\nname: {id}\ndescription: {id}\n---\n{prompt}\n"),
            )
            .await
            .expect("skill definition");
        }
        let manager = Arc::new(SkillManager::with_config(SkillStoreConfig {
            skills_dir: skills_dir.clone(),
            ..Default::default()
        }));
        manager.initialize().await.expect("initialize");
        let tools = SuccessfulLoadSkill;
        let review_config = AgentLoopConfig {
            skill_manager: Some(manager.clone()),
            selected_skill_ids: Some(vec!["review-demo".to_string()]),
            disabled_skill_ids: BTreeSet::new(),
            ..Default::default()
        };
        let mut session = Session::new("retained-session", "model");
        initialize_loop_state(&mut session, "review", &review_config, &tools)
            .await
            .expect("initial activation");
        let pinned_generation = session
            .metadata
            .get(SKILL_RUNTIME_ACTIVATION_GENERATION_KEY)
            .cloned()
            .expect("pinned generation metadata");
        let pinned_revisions = session
            .metadata
            .get(SKILL_RUNTIME_SELECTED_SKILL_REVISIONS_KEY)
            .cloned()
            .expect("pinned revision metadata");
        assert_eq!(
            manager
                .store()
                .pinned_activation_skills("retained-session")
                .await
                .expect("review pin")
                .0[0]
                .prompt,
            "review N"
        );

        tokio::fs::write(
            skills_dir.join("review-demo/SKILL.md"),
            "---\nname: review-demo\ndescription: review N+1\n---\nreview N+1\n",
        )
        .await
        .expect("review N+1");
        manager.store().reload().await.expect("reload N+1");
        let mut suspended = AgentRuntimeState::new("retained-session");
        suspended.status = AgentStatusState::Suspended;
        session.agent_runtime_state = Some(suspended);
        session.metadata.remove(LOADED_SKILL_IDS_METADATA_KEY);
        let continuation_config = AgentLoopConfig {
            skill_manager: Some(manager.clone()),
            disabled_skill_ids: BTreeSet::new(),
            ..Default::default()
        };
        initialize_loop_state(
            &mut session,
            "plain clarification reply",
            &continuation_config,
            &tools,
        )
        .await
        .expect("suspended continuation");
        assert_eq!(
            manager
                .store()
                .pinned_activation_skills("retained-session")
                .await
                .expect("retained review pin")
                .0[0]
                .prompt,
            "review N",
            "startup overwrites Suspended with Initializing, but retained pin must still win"
        );
        assert_eq!(
            session
                .metadata
                .get(SKILL_RUNTIME_SELECTION_SOURCE_KEY)
                .map(String::as_str),
            Some("explicit")
        );
        assert_eq!(
            session
                .metadata
                .get(SKILL_RUNTIME_ACTIVATION_GENERATION_KEY),
            Some(&pinned_generation)
        );
        assert_eq!(
            session
                .metadata
                .get(SKILL_RUNTIME_SELECTED_SKILL_REVISIONS_KEY),
            Some(&pinned_revisions)
        );
        assert!(!session
            .metadata
            .contains_key(SKILL_RUNTIME_SELECTED_SKILL_MODE_KEY));
        assert_eq!(
            session
                .metadata
                .get(LOADED_SKILL_IDS_METADATA_KEY)
                .map(String::as_str),
            Some("[\"review-demo\"]"),
            "retained explicit activation must deterministically preload again"
        );

        let plan_config = AgentLoopConfig {
            skill_manager: Some(manager.clone()),
            selected_skill_ids: Some(vec!["plan-demo".to_string()]),
            disabled_skill_ids: BTreeSet::new(),
            ..Default::default()
        };
        initialize_loop_state(&mut session, "plan instead", &plan_config, &tools)
            .await
            .expect("superseding activation");
        let (skills, descriptor) = manager
            .store()
            .pinned_activation_skills("retained-session")
            .await
            .expect("superseding plan pin");
        assert_eq!(skills.len(), 1);
        assert_eq!(skills[0].id, "plan-demo");
        assert_eq!(
            descriptor
                .skill_revisions
                .keys()
                .cloned()
                .collect::<Vec<_>>(),
            vec!["plan-demo"]
        );
    }
}
