use std::sync::Arc;

use crate::config::GoldConfig;
use crate::runtime::gold_evaluation::GoldEvaluationResult;
use crate::TaskLoopContext;
use bamboo_agent_core::{AgentEvent, Session};
use bamboo_domain::ProviderModelRef;
use bamboo_llm::{LLMProvider, LLMRequestOptions};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::app_context::AgentSessionContext;
use crate::session_app::provider_model::session_effective_model_ref;

use super::decision::{parse_gold_auto_answer_decision, GoldAutoAnswerDecision};
use super::prompt::{build_gold_auto_answer_messages, get_gold_auto_answer_tools};

pub(crate) struct GoldAuxiliaryTarget {
    pub(crate) provider: Arc<dyn LLMProvider>,
    pub(crate) model: String,
    pub(crate) timeout_context: crate::runtime::stream::handler::StreamTimeoutContext,
    pub(crate) configured_limit: usize,
}

pub(crate) async fn evaluate_gold_state_for_pending_question(
    state: &dyn AgentSessionContext,
    session_id: &str,
    session: &Session,
    gold_config: &GoldConfig,
) -> Result<GoldEvaluationResult, String> {
    let target = resolve_gold_provider_and_model(state, session, gold_config).await?;
    let iteration = session
        .agent_runtime_state
        .as_ref()
        .map(|runtime| runtime.round.current_round)
        .unwrap_or(0);

    let (event_tx, mut event_rx) = mpsc::channel::<AgentEvent>(8);
    let session_sender = state.get_session_event_sender(session_id).await;
    let forwarder = tokio::spawn(async move {
        while let Some(event) = event_rx.recv().await {
            let _ = session_sender.send(event);
        }
    });

    let result = evaluate_gold_state_with_target(
        session_id,
        session,
        gold_config,
        target,
        &event_tx,
        iteration,
    )
    .await
    .map_err(|error| error.to_string());

    drop(event_tx);
    let _ = forwarder.await;
    result
}

pub(crate) async fn evaluate_gold_state_with_target(
    session_id: &str,
    session: &Session,
    gold_config: &GoldConfig,
    target: GoldAuxiliaryTarget,
    event_tx: &mpsc::Sender<AgentEvent>,
    iteration: u32,
) -> Result<GoldEvaluationResult, bamboo_agent_core::AgentError> {
    let GoldAuxiliaryTarget {
        provider,
        model,
        timeout_context,
        configured_limit,
    } = target;
    let task_context = TaskLoopContext::from_session(session);
    let budget_provider = provider.clone();
    let budget_model = model.clone();
    let acquire_dispatch_guard = async move {
        crate::runtime::runner::auxiliary_budget::acquire(
            &budget_provider,
            &budget_model,
            configured_limit,
        )
        .await
    };

    crate::runtime::gold_evaluation::evaluate_gold_with_dispatch(
        session,
        task_context.as_ref(),
        gold_config,
        provider,
        &crate::runtime::gold_evaluation::GoldEvalFrame {
            event_tx,
            session_id,
            model: &model,
            timeout_context,
            reasoning_effort: session.reasoning_effort,
            checkpoint: bamboo_agent_core::GoldCheckpoint::Terminal,
            iteration,
        },
        acquire_dispatch_guard,
    )
    .await
}

pub(crate) async fn evaluate_gold_auto_answer_question(
    state: &dyn AgentSessionContext,
    session_id: &str,
    session: &Session,
    gold_config: &GoldConfig,
    state_evaluation: &GoldEvaluationResult,
) -> Result<GoldAutoAnswerDecision, String> {
    if session.pending_question.is_none() {
        return Ok(GoldAutoAnswerDecision::decline(
            "no pending question available",
        ));
    }

    let target = resolve_gold_provider_and_model(state, session, gold_config).await?;
    evaluate_gold_auto_answer_question_with_target(
        session_id,
        session,
        gold_config,
        state_evaluation,
        target,
    )
    .await
}

pub(crate) async fn evaluate_gold_auto_answer_question_with_target(
    session_id: &str,
    session: &Session,
    gold_config: &GoldConfig,
    state_evaluation: &GoldEvaluationResult,
    target: GoldAuxiliaryTarget,
) -> Result<GoldAutoAnswerDecision, String> {
    let Some(pending) = session.pending_question.as_ref() else {
        return Ok(GoldAutoAnswerDecision::decline(
            "no pending question available",
        ));
    };
    let messages = build_gold_auto_answer_messages(session, pending, state_evaluation, gold_config);
    let tools = get_gold_auto_answer_tools();
    let request_options = LLMRequestOptions {
        session_id: Some(session_id.to_string()),
        reasoning_effort: session.reasoning_effort,
        parallel_tool_calls: None,
        required_tool: None,
        responses: None,
        request_purpose: Some("gold_auto_answer".to_string()),
        cache: None,
    };

    let cancel_token = CancellationToken::new();
    let _dispatch_guard = crate::runtime::runner::auxiliary_budget::acquire(
        &target.provider,
        &target.model,
        target.configured_limit,
    )
    .await;
    let timeout_context = target.timeout_context.begin_request();
    let stream = crate::runtime::stream::handler::await_stream_bootstrap(
        target.provider.chat_stream_with_options(
            &messages,
            &tools,
            Some(gold_config.max_output_tokens),
            &target.model,
            Some(&request_options),
        ),
        &cancel_token,
        session_id,
        &timeout_context,
    );
    let stream = stream
        .await
        .map_err(|error| format!("provider bootstrap failed: {error}"))?
        .map_err(|error| format!("provider call failed: {error}"))?;
    let stream_output = crate::runtime::stream::handler::consume_llm_stream_silent_with_context(
        stream,
        &cancel_token,
        session_id,
        &timeout_context,
    )
    .await
    .map_err(|error| format!("stream handling failed: {error}"))?;

    Ok(
        parse_gold_auto_answer_decision(&stream_output.tool_calls).unwrap_or_else(|| {
            GoldAutoAnswerDecision::decline(
                "Gold auto-answer returned no structured tool result; declining.",
            )
        }),
    )
}

async fn resolve_gold_provider_and_model(
    state: &dyn AgentSessionContext,
    session: &Session,
    gold_config: &GoldConfig,
) -> Result<GoldAuxiliaryTarget, String> {
    let config_snapshot = state.config().read().await.clone();
    let provider_name = gold_provider_name(&config_snapshot, session);
    let stream_timeout = config_snapshot.stream_timeout;
    let configured_limit = crate::runtime::config::normalize_auxiliary_evaluation_max_concurrency(
        config_snapshot
            .extra
            .get("auxiliary_evaluation_max_concurrency")
            .and_then(serde_json::Value::as_u64),
    );
    let explicit_model = gold_config
        .model_name
        .as_deref()
        .map(str::trim)
        .filter(|model| !model.is_empty());
    // A dedicated bare Gold model belongs to the executing account. Otherwise
    // use the complete global Fast target; never carry only its model name into
    // a session/default provider, or reinterpret a rejected role as unset.
    if explicit_model.is_none() {
        if let Some(fast) = crate::model_config_helper::try_resolve_fast_model(
            &config_snapshot,
            &provider_name,
            state.provider_registry(),
        )
        .map_err(|error| format!("Configured Gold fallback route is unavailable: {error}"))?
        {
            let selected_provider = config_snapshot
                .defaults
                .as_ref()
                .map(|defaults| {
                    defaults
                        .fast
                        .as_ref()
                        .unwrap_or(&defaults.chat)
                        .provider
                        .clone()
                })
                .unwrap_or_else(|| provider_name.clone());
            return Ok(gold_auxiliary_target(
                fast.provider,
                fast.model_name,
                selected_provider,
                stream_timeout,
                configured_limit,
            ));
        }
    }
    let model = explicit_model
        .or_else(|| {
            let model = session.model.trim();
            (!model.is_empty() && model != "unknown").then_some(model)
        })
        .ok_or_else(|| "gold model name is required".to_string())?
        .to_string();
    let target = ProviderModelRef::new(&provider_name, &model);
    if let Some(provider) = state.get_provider_for_model_ref(&target) {
        return Ok(gold_auxiliary_target(
            provider,
            model,
            provider_name,
            stream_timeout,
            configured_limit,
        ));
    }
    // A standalone context may supply one direct provider without a registry.
    // Retain that SDK seam only without an explicit account/role configuration;
    // a rejected configured route must never escape through get_default().
    if state.provider_registry().is_empty()
        && config_snapshot.defaults.is_none()
        && config_snapshot.provider_instances.is_empty()
        && session_effective_model_ref(session).is_none()
    {
        return Ok(gold_auxiliary_target(
            state.get_provider().await,
            model,
            provider_name,
            stream_timeout,
            configured_limit,
        ));
    }
    Err(format!(
        "Configured Gold provider/model route is unavailable for '{provider_name}'"
    ))
}

fn gold_provider_name(config: &bamboo_config::Config, session: &Session) -> String {
    session_effective_model_ref(session)
        .map(|r| r.provider.clone())
        .unwrap_or_else(|| config.effective_default_provider().to_string())
}

fn gold_auxiliary_target(
    provider: Arc<dyn LLMProvider>,
    model: String,
    provider_name: String,
    stream_timeout: bamboo_config::StreamTimeoutConfig,
    configured_limit: usize,
) -> GoldAuxiliaryTarget {
    let timeout_context = crate::runtime::stream::handler::StreamTimeoutContext::new(
        stream_timeout,
        Some(&provider_name),
        Some(&model),
    );
    GoldAuxiliaryTarget {
        provider,
        model,
        timeout_context,
        configured_limit,
    }
}

#[cfg(test)]
mod provider_selection_tests {
    use std::sync::{Arc, Mutex};

    use super::gold_provider_name;
    use super::{
        evaluate_gold_auto_answer_question_with_target, evaluate_gold_state_with_target,
        GoldAuxiliaryTarget,
    };
    use crate::runtime::config::GoldConfig;
    use crate::runtime::gold_evaluation::GoldEvaluationResult;
    use crate::runtime::stream::handler::StreamTimeoutContext;
    use bamboo_agent_core::{
        GoldCheckpoint, GoldConfidence, GoldDecision, Message, PendingQuestion,
        PendingQuestionSource, Session, ToolSchema,
    };
    use bamboo_domain::ReasoningEffort;
    use bamboo_llm::{LLMError, LLMProvider, LLMRequestOptions, LLMStream};

    #[tokio::test]
    async fn routing_contract_gold_pending_question_keeps_the_fast_provider_model_pair() {
        use crate::app_context::model_routing_test_support::RoutingContext;
        for flag in [None, Some(false), Some(true)] {
            let state = RoutingContext::new(flag);
            let session = RoutingContext::session();
            super::evaluate_gold_state_for_pending_question(
                state.as_ref(),
                &session.id,
                &session,
                &GoldConfig::default(),
            )
            .await
            .unwrap();
            assert_eq!(
                state.requests("fast-b"),
                vec![("cheap-b".into(), "gold_evaluation".into())]
            );
            assert!(state.requests("chat-a").is_empty());
        }
    }

    #[tokio::test]
    async fn routing_contract_explicit_gold_wins_without_resolving_an_invalid_fast_role() {
        use crate::app_context::model_routing_test_support::RoutingContext;
        let state = RoutingContext::new(Some(false));
        state
            .config
            .write()
            .await
            .defaults
            .as_mut()
            .unwrap()
            .fast
            .as_mut()
            .unwrap()
            .provider = "missing-fast".into();
        let session = RoutingContext::session();
        let gold = GoldConfig {
            model_name: Some("explicit-gold".into()),
            ..Default::default()
        };
        super::evaluate_gold_state_for_pending_question(
            state.as_ref(),
            &session.id,
            &session,
            &gold,
        )
        .await
        .unwrap();
        assert_eq!(
            state.requests("chat-a"),
            vec![("explicit-gold".into(), "gold_evaluation".into())]
        );
        assert!(state.requests("fast-b").is_empty());

        let mut invalid_session = session.clone();
        invalid_session.model_ref.as_mut().unwrap().provider = "missing-session-account".into();
        assert!(super::evaluate_gold_state_for_pending_question(
            state.as_ref(),
            &invalid_session.id,
            &invalid_session,
            &gold
        )
        .await
        .is_err());
        assert_eq!(state.requests("chat-a").len(), 1);
        assert!(state.requests("fast-b").is_empty());
    }

    #[tokio::test]
    async fn routing_contract_gold_invalid_fast_and_provider_failures_never_switch_accounts() {
        use crate::app_context::model_routing_test_support::{Failure, RoutingContext};
        for flag in [None, Some(false), Some(true)] {
            for invalid in 0..3 {
                let state = RoutingContext::new(flag);
                match invalid {
                    0 => {
                        state
                            .config
                            .write()
                            .await
                            .defaults
                            .as_mut()
                            .unwrap()
                            .fast
                            .as_mut()
                            .unwrap()
                            .provider = "missing-account".into()
                    }
                    1 => {
                        state
                            .config
                            .write()
                            .await
                            .provider_instances
                            .get_mut("fast-b")
                            .unwrap()
                            .enabled = false;
                        state.registry.remove("fast-b");
                    }
                    _ => state
                        .registry
                        .set_runtime_models(std::collections::HashMap::from([
                            (
                                "chat-a".into(),
                                vec!["main-a".into(), "legacy-fast-a".into()],
                            ),
                            ("fast-b".into(), Vec::new()),
                        ])),
                }
                let session = RoutingContext::session();
                assert!(super::evaluate_gold_state_for_pending_question(
                    state.as_ref(),
                    &session.id,
                    &session,
                    &GoldConfig::default()
                )
                .await
                .is_err());
                state.assert_no_requests();
            }
            for failure in [Failure::Auth, Failure::Quota, Failure::Network] {
                let state = RoutingContext::new(flag);
                *state.providers["fast-b"].failure.lock().unwrap() = Some(failure);
                let session = RoutingContext::session();
                assert!(super::evaluate_gold_state_for_pending_question(
                    state.as_ref(),
                    &session.id,
                    &session,
                    &GoldConfig::default()
                )
                .await
                .is_err());
                assert_eq!(
                    state.requests("fast-b"),
                    vec![("cheap-b".into(), "gold_evaluation".into())]
                );
                assert!(state.requests("chat-a").is_empty());
                assert!(state.requests("memory-c").is_empty());
            }
        }
    }

    use crate::app_context::AgentSessionContext;

    async fn make_gold_target_stale(
        state: &crate::app_context::model_routing_test_support::RoutingContext,
        change: &str,
    ) {
        let mut config = state.config.write().await;
        match change {
            "disabled" => {
                config.provider_instances.get_mut("chat-a").unwrap().enabled = false;
            }
            "removed" => {
                assert!(config.provider_instances.remove("chat-a").is_some());
            }
            "revoked" => {
                config
                    .provider_instances
                    .get_mut("chat-a")
                    .unwrap()
                    .extra
                    .insert(
                        "runtime_models".to_string(),
                        serde_json::json!(["main-a", "legacy-fast-a"]),
                    );
            }
            "derived" => {
                config
                    .provider_instances
                    .get_mut("chat-a")
                    .unwrap()
                    .extra
                    .remove("runtime_models");
            }
            _ => unreachable!("fixture change"),
        }
        if change == "revoked" || change == "derived" {
            assert!(!bamboo_config::provider_runtime_models(&config, "chat-a")
                .unwrap()
                .iter()
                .any(|model| model == "explicit-gold"));
        }
        assert!(state.registry.get("chat-a").is_some());
        assert!(state
            .registry
            .runtime_models_for_provider("chat-a")
            .unwrap()
            .iter()
            .any(|model| model == "explicit-gold"));
    }

    fn synthetic_pending_gold_session() -> Session {
        let mut session = crate::app_context::model_routing_test_support::RoutingContext::session();
        session.pending_question = Some(PendingQuestion {
            tool_call_id: "synthetic-question".into(),
            tool_name: "conclusion_with_options".into(),
            question: "Continue the synthetic task?".into(),
            options: vec!["Continue".into()],
            allow_custom: false,
            source: PendingQuestionSource::PauseTool,
        });
        session
    }

    fn explicit_gold_config() -> GoldConfig {
        GoldConfig {
            model_name: Some("explicit-gold".into()),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn routing_contract_gold_state_rejects_stale_explicit_account_without_dispatch() {
        use crate::app_context::model_routing_test_support::RoutingContext;
        let mut failures = Vec::new();
        for flag in [None, Some(false), Some(true)] {
            for change in ["disabled", "removed", "revoked", "derived"] {
                let state = RoutingContext::new(flag);
                make_gold_target_stale(&state, change).await;
                let session = synthetic_pending_gold_session();
                let result = super::evaluate_gold_state_for_pending_question(
                    state.as_ref(),
                    &session.id,
                    &session,
                    &explicit_gold_config(),
                )
                .await;
                let requests = ["chat-a", "fast-b", "memory-c"].map(|id| (id, state.requests(id)));
                if result.is_ok() || requests.iter().any(|(_, calls)| !calls.is_empty()) {
                    failures.push(format!(
                        "flag={flag:?}, change={change}, result={result:?}, requests={requests:?}"
                    ));
                }
            }
        }
        assert!(
            failures.is_empty(),
            "stale Gold state route dispatched: {failures:#?}"
        );
    }

    #[tokio::test]
    async fn routing_contract_gold_answer_rechecks_live_target_after_state_evaluation() {
        use crate::app_context::model_routing_test_support::RoutingContext;
        let mut failures = Vec::new();
        for flag in [None, Some(false), Some(true)] {
            for change in ["disabled", "removed", "revoked", "derived"] {
                let state = RoutingContext::new(flag);
                let session = synthetic_pending_gold_session();
                let gold = explicit_gold_config();
                let evaluation = super::evaluate_gold_state_for_pending_question(
                    state.as_ref(),
                    &session.id,
                    &session,
                    &gold,
                )
                .await
                .expect("synthetic state evaluation should succeed before invalidation");
                let original = vec![("explicit-gold".to_string(), "gold_evaluation".to_string())];
                assert_eq!(state.requests("chat-a"), original);
                make_gold_target_stale(&state, change).await;
                let result = super::evaluate_gold_auto_answer_question(
                    state.as_ref(),
                    &session.id,
                    &session,
                    &gold,
                    &evaluation,
                )
                .await;
                let chat = state.requests("chat-a");
                let fast = state.requests("fast-b");
                let memory = state.requests("memory-c");
                if result.is_ok() || chat != original || !fast.is_empty() || !memory.is_empty() {
                    failures.push(format!("flag={flag:?}, change={change}, result={result:?}, chat={chat:?}, fast={fast:?}, memory={memory:?}"));
                }
            }
        }
        assert!(
            failures.is_empty(),
            "stale Gold answer route dispatched: {failures:#?}"
        );
    }

    struct GoldOverrideContext {
        inner: Arc<crate::app_context::model_routing_test_support::RoutingContext>,
        routes: Mutex<Vec<(String, String)>>,
    }

    #[async_trait::async_trait]
    impl crate::app_context::AgentSessionContext for GoldOverrideContext {
        fn sessions(&self) -> &crate::SessionCache {
            self.inner.sessions()
        }
        fn storage(&self) -> &Arc<dyn bamboo_agent_core::storage::Storage> {
            self.inner.storage()
        }
        fn persistence(&self) -> &Arc<bamboo_storage::LockedSessionStore> {
            self.inner.persistence()
        }
        fn agent_runners(
            &self,
        ) -> &Arc<
            tokio::sync::RwLock<
                std::collections::HashMap<
                    String,
                    crate::runtime::execution::runner_state::AgentRunner,
                >,
            >,
        > {
            self.inner.agent_runners()
        }
        fn account_sink(&self) -> &Arc<crate::events::AccountEventSink> {
            self.inner.account_sink()
        }
        fn config(&self) -> &Arc<tokio::sync::RwLock<bamboo_llm::Config>> {
            self.inner.config()
        }
        fn provider_registry(&self) -> &Arc<bamboo_llm::ProviderRegistry> {
            self.inner.provider_registry()
        }
        async fn get_session_event_sender(
            &self,
            id: &str,
        ) -> tokio::sync::broadcast::Sender<bamboo_agent_core::AgentEvent> {
            self.inner.get_session_event_sender(id).await
        }
        async fn load_session_merged(&self, id: &str) -> Option<Session> {
            self.inner.load_session_merged(id).await
        }
        async fn save_and_cache_session(&self, session: &mut Session) {
            self.inner.save_and_cache_session(session).await
        }
        async fn get_provider(&self) -> Arc<dyn LLMProvider> {
            self.inner.get_provider().await
        }
        fn get_provider_for_model_ref(
            &self,
            target: &bamboo_domain::ProviderModelRef,
        ) -> Option<Arc<dyn LLMProvider>> {
            self.routes
                .lock()
                .unwrap()
                .push((target.provider.clone(), target.model.clone()));
            if target.provider == "chat-a" && target.model == "explicit-gold" {
                Some(self.inner.providers["memory-c"].clone())
            } else {
                None
            }
        }
        async fn get_provider_for_endpoint(&self, name: &str) -> Option<Arc<dyn LLMProvider>> {
            self.inner.get_provider_for_endpoint(name).await
        }
        fn title_gen_acquire(&self, id: &str) -> bool {
            self.inner.title_gen_acquire(id)
        }
        fn title_gen_release(&self, id: &str) {
            self.inner.title_gen_release(id)
        }
    }

    #[tokio::test]
    async fn routing_contract_explicit_gold_preserves_embedder_provider_override() {
        use crate::app_context::model_routing_test_support::RoutingContext;
        let state = RoutingContext::new(Some(false));
        state.registry.remove("chat-a");
        let context = GoldOverrideContext {
            inner: state.clone(),
            routes: Mutex::new(Vec::new()),
        };
        let session = synthetic_pending_gold_session();
        let gold = explicit_gold_config();
        let evaluation =
            super::evaluate_gold_state_for_pending_question(&context, &session.id, &session, &gold)
                .await
                .unwrap();
        super::evaluate_gold_auto_answer_question(
            &context,
            &session.id,
            &session,
            &gold,
            &evaluation,
        )
        .await
        .unwrap();
        assert_eq!(
            context.routes.lock().unwrap().as_slice(),
            &[
                ("chat-a".into(), "explicit-gold".into()),
                ("chat-a".into(), "explicit-gold".into()),
            ]
        );
        assert_eq!(
            state.requests("memory-c"),
            vec![
                ("explicit-gold".into(), "gold_evaluation".into()),
                ("explicit-gold".into(), "gold_auto_answer".into()),
            ]
        );
        assert!(state.requests("chat-a").is_empty());
        assert!(state.requests("fast-b").is_empty());
    }

    #[tokio::test]
    async fn routing_contract_explicit_gold_accepts_enabled_legacy_provider() {
        use crate::app_context::model_routing_test_support::RoutingContext;
        use std::collections::HashMap;
        for flag in [None, Some(false), Some(true)] {
            let state = RoutingContext::new(flag);
            {
                let mut config = state.config.write().await;
                config.provider_instances.clear();
                config.default_provider_instance = None;
                config.provider = "openai".into();
                config.defaults = None;
                config.providers_mut().openai = Some(bamboo_config::OpenAIConfig {
                    model: Some("legacy-chat".into()),
                    fast_model: Some("legacy-fast".into()),
                    ..Default::default()
                });
                assert!(bamboo_config::synthesize_legacy_instances(&config)
                    .iter()
                    .any(|(id, instance)| id == "openai" && instance.enabled));
                assert!(bamboo_config::provider_runtime_models(&config, "openai")
                    .unwrap()
                    .iter()
                    .any(|model| model == "legacy-fast"));
            }
            state
                .registry
                .insert("openai".into(), state.providers["fast-b"].clone());
            state.registry.set_runtime_models(HashMap::from([(
                "openai".to_string(),
                vec!["legacy-chat".to_string(), "legacy-fast".to_string()],
            )]));
            let mut session = synthetic_pending_gold_session();
            session.model = "legacy-chat".into();
            session.model_ref = Some(bamboo_domain::ProviderModelRef::new(
                "openai",
                "legacy-chat",
            ));
            let gold = GoldConfig {
                model_name: Some("legacy-fast".into()),
                ..Default::default()
            };
            let evaluation = super::evaluate_gold_state_for_pending_question(
                state.as_ref(),
                &session.id,
                &session,
                &gold,
            )
            .await
            .unwrap();
            super::evaluate_gold_auto_answer_question(
                state.as_ref(),
                &session.id,
                &session,
                &gold,
                &evaluation,
            )
            .await
            .unwrap();
            assert_eq!(
                state.requests("fast-b"),
                vec![
                    ("legacy-fast".into(), "gold_evaluation".into()),
                    ("legacy-fast".into(), "gold_auto_answer".into()),
                ]
            );
            assert!(state.requests("chat-a").is_empty());
            assert!(state.requests("memory-c").is_empty());
        }
    }

    #[derive(Default)]
    struct RecordingRejectingProvider {
        requests: Mutex<Vec<(String, Option<ReasoningEffort>)>>,
    }

    #[async_trait::async_trait]
    impl LLMProvider for RecordingRejectingProvider {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> Result<LLMStream, LLMError> {
            panic!("Gold requests must carry request controls")
        }

        async fn chat_stream_with_options(
            &self,
            _messages: &[Message],
            _tools: &[ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
            options: Option<&LLMRequestOptions>,
        ) -> Result<LLMStream, LLMError> {
            let options = options.expect("request controls");
            self.requests.lock().unwrap().push((
                options.request_purpose.clone().expect("request purpose"),
                options.reasoning_effort,
            ));
            Err(LLMError::Api(
                "fixture rejected selected effort".to_string(),
            ))
        }
    }

    #[tokio::test]
    async fn gold_evaluator_requests_preserve_selected_reasoning_effort() {
        for effort in [
            None,
            Some(ReasoningEffort::Disabled),
            Some(ReasoningEffort::Low),
            Some(ReasoningEffort::Medium),
            Some(ReasoningEffort::High),
            Some(ReasoningEffort::Xhigh),
            Some(ReasoningEffort::Max),
        ] {
            let mut session = Session::new("gold-effort-selection", "session-model");
            session.reasoning_effort = effort;
            session.pending_question = Some(PendingQuestion {
                tool_call_id: "pending-question".to_string(),
                tool_name: "conclusion_with_options".to_string(),
                question: "Continue?".to_string(),
                options: vec!["Continue".to_string()],
                allow_custom: false,
                source: PendingQuestionSource::PauseTool,
            });
            let provider = Arc::new(RecordingRejectingProvider::default());
            let target = || GoldAuxiliaryTarget {
                provider: provider.clone(),
                model: "gold-effort-model".to_string(),
                timeout_context: StreamTimeoutContext::default(),
                configured_limit: 1,
            };
            let config = GoldConfig::default();
            let (event_tx, _event_rx) = tokio::sync::mpsc::channel(2);
            let error = evaluate_gold_state_with_target(
                &session.id,
                &session,
                &config,
                target(),
                &event_tx,
                0,
            )
            .await
            .unwrap_err();
            assert!(error
                .to_string()
                .contains("fixture rejected selected effort"));

            let state_evaluation = GoldEvaluationResult {
                checkpoint: GoldCheckpoint::Terminal,
                iteration: 0,
                decision: GoldDecision::Continue,
                confidence: GoldConfidence::High,
                reasoning: "fixture state evaluation".to_string(),
                missing_information: Vec::new(),
                next_action: None,
                prompt_tokens: 0,
                completion_tokens: 0,
            };
            let error = evaluate_gold_auto_answer_question_with_target(
                &session.id,
                &session,
                &config,
                &state_evaluation,
                target(),
            )
            .await
            .unwrap_err();
            assert!(error.contains("fixture rejected selected effort"));
            assert_eq!(
                provider.requests.lock().unwrap().as_slice(),
                &[
                    ("gold_evaluation".to_string(), effort),
                    ("gold_auto_answer".to_string(), effort)
                ],
            );
        }
    }

    #[test]
    fn gold_provider_fallback_uses_default_provider_instance() {
        let mut config = bamboo_config::Config::default();
        let instance = serde_json::from_value(serde_json::json!({
            "provider_type": "anthropic",
            "enabled": true
        }))
        .unwrap();
        config.provider = "openai".to_string();
        config
            .provider_instances
            .insert("gold-anthropic".to_string(), instance);
        config.default_provider_instance = Some("gold-anthropic".to_string());
        let session = Session::new("gold-provider-instance", "model");

        assert_eq!(gold_provider_name(&config, &session), "gold-anthropic");
    }
}
