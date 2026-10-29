use std::sync::Arc;

use async_trait::async_trait;
use bamboo_domain::{TaskItem, TaskItemStatus, TaskList};
use chrono::Utc;
use futures::stream;
use tokio::sync::{mpsc, Mutex};
use tokio_util::sync::CancellationToken;

use crate::runtime::config::AgentLoopConfig;
use bamboo_agent_core::tools::{FunctionCall, Tool, ToolCtx, ToolError, ToolOutcome, ToolResult};
use bamboo_agent_core::{Message, Session};
use bamboo_llm::{LLMChunk, LLMError, LLMProvider, LLMStream};
use bamboo_metrics::storage::MetricsStorage;
use bamboo_tools::BuiltinToolExecutorBuilder;

fn task_list_with_in_progress_item(session_id: &str, description: &str) -> TaskList {
    TaskList {
        session_id: session_id.to_string(),
        title: "Agent Tasks".to_string(),
        items: vec![TaskItem {
            id: "task-1".to_string(),
            description: description.to_string(),
            status: TaskItemStatus::InProgress,
            ..TaskItem::default()
        }],
        created_at: Utc::now(),
        updated_at: Utc::now(),
    }
}

/// Regression test: tool calls executed inside the agent loop MUST receive a ToolExecutionContext
/// with `session_id=Some(...)`, including ordinary custom tools.
#[tokio::test]
async fn agent_loop_passes_session_id_into_tool_execution_context() {
    let trace = bamboo_infrastructure::telemetry::test_support::TraceTest::default();
    struct QueueProvider {
        // Each `chat_stream` call pops one pre-baked stream.
        queue: Mutex<Vec<Vec<bamboo_llm::provider::Result<LLMChunk>>>>,
    }

    #[async_trait]
    impl LLMProvider for QueueProvider {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[bamboo_agent_core::tools::ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> bamboo_llm::provider::Result<LLMStream> {
            let mut guard = self.queue.lock().await;
            if guard.is_empty() {
                panic!("test provider queue exhausted");
            }
            let items = guard.remove(0);
            Ok(Box::pin(stream::iter(items)))
        }
    }

    struct SessionIdRequiredTool {
        seen_session_id: Arc<Mutex<Option<String>>>,
    }

    #[async_trait]
    impl Tool for SessionIdRequiredTool {
        fn name(&self) -> &str {
            "session_context_probe"
        }

        fn description(&self) -> &str {
            "test tool that requires session_id in ToolExecutionContext"
        }

        fn parameters_schema(&self) -> serde_json::Value {
            serde_json::json!({
                "type": "object",
                "properties": {
                    "goal": { "type": "string" }
                },
                "required": ["goal"]
            })
        }

        async fn invoke(
            &self,
            _args: serde_json::Value,
            ctx: ToolCtx,
        ) -> Result<ToolOutcome, ToolError> {
            let Some(session_id) = ctx.session_id else {
                return Err(ToolError::Execution(
                    "missing session_id in tool context".to_string(),
                ));
            };

            *self.seen_session_id.lock().await = Some(session_id.to_string());

            Ok(ToolOutcome::Completed(ToolResult {
                success: true,
                result: "ok".to_string(),
                display_preference: None,
                images: Vec::new(),
            }))
        }
    }

    let seen_session_id = Arc::new(Mutex::new(None));
    let tools = BuiltinToolExecutorBuilder::new()
        .with_tool(SessionIdRequiredTool {
            seen_session_id: seen_session_id.clone(),
        })
        .expect("register test tool")
        .build();

    let tool_call = bamboo_agent_core::tools::ToolCall {
        id: "call_context_probe".to_string(),
        tool_type: "function".to_string(),
        function: FunctionCall {
            name: "session_context_probe".to_string(),
            arguments: r#"{"goal":"do it"}"#.to_string(),
        },
    };

    let provider = Arc::new(QueueProvider {
        queue: Mutex::new(vec![
            vec![Ok(LLMChunk::ToolCalls(vec![tool_call])), Ok(LLMChunk::Done)],
            vec![Ok(LLMChunk::Token("done".to_string())), Ok(LLMChunk::Done)],
        ]),
    });

    let mut session = Session::new("session-ctx-test", "ignored");

    let (event_tx, _event_rx) = mpsc::channel(64);
    let config = AgentLoopConfig {
        run_budget: bamboo_config::RunBudgetConfig {
            max_rounds: Some(3),
            ..Default::default()
        },
        system_prompt: Some("sys".to_string()),
        model_name: Some("test-model".to_string()),
        ..Default::default()
    };

    trace
        .scope(super::run_agent_loop_with_config(
            &mut session,
            "hello".to_string(),
            event_tx,
            provider,
            Arc::new(tools),
            CancellationToken::new(),
            config,
        ))
        .await
        .expect("agent loop should succeed");

    assert_eq!(
        seen_session_id.lock().await.clone(),
        Some("session-ctx-test".to_string())
    );
    let spans = trace.spans();
    let agent = spans
        .iter()
        .find(|s| s.name == "invoke_agent")
        .expect("real invocation span");
    let calls: Vec<_> = spans
        .iter()
        .filter(|s| s.name.starts_with("chat "))
        .collect();
    let tools: Vec<_> = spans
        .iter()
        .filter(|s| s.name.starts_with("execute_tool "))
        .collect();
    assert_eq!(calls.len(), 2, "one span per actual provider request");
    assert_eq!(tools.len(), 1, "one span per actual tool dispatch");
    assert_eq!(tools[0].name, "execute_tool session_context_probe");
    for span in calls.into_iter().chain(tools) {
        assert_eq!(span.parent_span_id, agent.span_context.span_id());
        assert_eq!(span.span_context.trace_id(), agent.span_context.trace_id());
        assert!(span.events.is_empty());
    }
    let exported = format!("{spans:?}");
    assert!(
        !exported.contains("do it"),
        "tool arguments must not be exported"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn agent_loop_explicit_gold_requires_managed_account_admission() {
    use crate::runtime::config::{AuxiliaryModelConfig, GoldConfig, PromptMemoryFlags};
    use bamboo_domain::ProviderModelRef;
    use bamboo_llm::{Config, ProviderRegistry};
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, Request, ResponseTemplate};

    let account_a = MockServer::start().await;
    let account_b = MockServer::start().await;
    let main_calls = Arc::new(AtomicUsize::new(0));
    let main_counter = main_calls.clone();
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(move |request: &Request| {
            let body: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
            let model = body["model"].as_str().unwrap();
            let (delta, delay) = match model {
                "main-a" if main_counter.fetch_add(1, Ordering::SeqCst) % 2 == 0 => (
                    json!({"tool_calls": [{
                        "index": 0, "id": "fixture-update-goal", "type": "function",
                        "function": {
                            "name": "update_goal",
                            "arguments": json!({"status": "complete"}).to_string()
                        }
                    }]}),
                    false,
                ),
                "main-a" => (json!({"content": "synthetic done"}), true),
                "explicit-gold" => (
                    json!({"tool_calls": [{
                        "index": 0, "id": "fixture-gold", "type": "function",
                        "function": {
                            "name": "report_gold_evaluation",
                            "arguments": json!({
                                "decision": "achieved",
                                "confidence": "high",
                                "reasoning": "synthetic goal verified",
                                "missing_information": []
                            }).to_string()
                        }
                    }]}),
                    false,
                ),
                _ => panic!("unexpected model in synthetic account A: {model}"),
            };
            let frame = json!({
                "id": "synthetic-routing",
                "object": "chat.completion.chunk",
                "created": 0,
                "model": model,
                "choices": [{"index": 0, "delta": delta, "finish_reason": null}]
            });
            let response = ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(format!("data: {frame}\n\ndata: [DONE]\n\n"));
            if delay {
                // The post-round evaluation is detached. Hold the second main
                // response so it can be observed before run finalization.
                response.set_delay(Duration::from_secs(1))
            } else {
                response
            }
        })
        .mount(&account_a)
        .await;
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&account_b)
        .await;

    let root = tempfile::tempdir().expect("isolated provider data directory");
    let mut config = Config::default();
    config.provider = "chat-a".into();
    config.default_provider_instance = Some("chat-a".into());
    config.http_proxy = "http://127.0.0.1:9".into();
    config.https_proxy.clear();
    config.proxy_auth = None;
    config.defaults = Some(
        serde_json::from_value(json!({
            "chat": {"provider": "chat-a", "model": "main-a"},
            "fast": {"provider": "fast-b", "model": "cheap-b"}
        }))
        .unwrap(),
    );
    for (id, model, endpoint, admitted) in [
        (
            "chat-a",
            "main-a",
            account_a.uri(),
            vec!["main-a", "explicit-gold"],
        ),
        ("fast-b", "cheap-b", account_b.uri(), vec!["cheap-b"]),
    ] {
        config.provider_instances.insert(
            id.into(),
            serde_json::from_value(json!({
                "provider_type": "openai",
                "enabled": true,
                "api_key": "synthetic-only-not-a-credential",
                "base_url": endpoint,
                "model": model,
                "runtime_models": admitted
            }))
            .unwrap(),
        );
    }
    let registry = Arc::new(
        ProviderRegistry::from_config(&config, root.path().to_path_buf())
            .await
            .expect("synthetic managed registry"),
    );
    let main_ref = ProviderModelRef::new("chat-a", "main-a");
    let gold_ref = ProviderModelRef::new("chat-a", "explicit-gold");
    let main = registry
        .provider_for_configured_model(&config, &main_ref)
        .expect("main model is admitted on A");
    assert!(registry
        .provider_for_configured_model(&config, &gold_ref)
        .is_ok());

    // The closure exercises the same shared role resolvers as the Host, while
    // the genuine runner executes both Gold checkpoint consumers.
    let live_config = Arc::new(tokio::sync::RwLock::new(config));
    let resolver_config = live_config.clone();
    let resolver_registry = registry.clone();
    let resolver = Arc::new(move || {
        let config = resolver_config.try_read().unwrap();
        let areas =
            crate::model_areas::resolve_global_area_models(&config, "chat-a", &resolver_registry);
        let (fast_model_name, fast_model_provider) =
            crate::model_config_helper::resolve_fast_model_for_runner(
                Some(&config),
                "chat-a",
                &resolver_registry,
            );
        AuxiliaryModelConfig {
            fast_model_name,
            fast_model_provider,
            background_model_name: areas
                .background
                .as_ref()
                .map(|target| target.model_name.clone()),
            background_model_provider: areas.background.map(|target| target.provider),
            summarization_model_name: areas
                .summarization
                .as_ref()
                .map(|target| target.model_name.clone()),
            summarization_model_provider: areas.summarization.map(|target| target.provider),
            summarization_model_error: areas.summarization_error,
            ..Default::default()
        }
    });
    // Model the Host's per-run binding using the exact same guarded target,
    // leaving SDK runs without this hook unchanged.
    let gold_config_handle = live_config.clone();
    let gold_registry = registry.clone();
    let gold_expected = main.clone();
    let gold_resolver = Arc::new(move |model: &str, delegate: Arc<dyn LLMProvider>| {
        bamboo_llm::gold_admission::guarded_gold_target(
            gold_config_handle.clone(),
            gold_registry.clone(),
            ProviderModelRef::new("chat-a", model),
            gold_expected.clone(),
            delegate,
        )
    });
    let mut request_offset = 0usize;
    for revoked in [false, true] {
        if revoked {
            let mut current = live_config.write().await;
            current
                .provider_instances
                .get_mut("chat-a")
                .unwrap()
                .extra
                .insert("runtime_models".into(), json!(["main-a"]));
            assert!(registry
                .provider_for_configured_model(&current, &main_ref)
                .is_ok());
            assert!(registry
                .provider_for_configured_model(&current, &gold_ref)
                .is_err());
            assert!(Arc::ptr_eq(&main, &registry.get("chat-a").unwrap()));
        }
        let before_main = main_calls.load(Ordering::SeqCst);
        let mut session = Session::new(
            if revoked {
                "gold-revoked"
            } else {
                "gold-admitted"
            },
            "main-a",
        );
        session.model_ref = Some(main_ref.clone());
        let (event_tx, _event_rx) = mpsc::channel(128);
        let loop_config = AgentLoopConfig {
            model_name: Some("main-a".into()),
            provider_name: Some("chat-a".into()),
            gold_config: Some(GoldConfig {
                enabled: true,
                auto_continue_enabled: true,
                goal: Some("synthetic goal".into()),
                model_name: Some("explicit-gold".into()),
                ..Default::default()
            }),
            prompt_memory_flags: PromptMemoryFlags {
                project_prompt_injection: false,
                relevant_recall: false,
                relevant_recall_rerank: false,
                project_first_dream: false,
                ledger_agenda: false,
            },
            auxiliary_model_resolver: Some(resolver.clone()),
            gold_target_resolver: Some(gold_resolver.clone()),
            run_budget: bamboo_config::RunBudgetConfig {
                max_rounds: Some(4),
                ..Default::default()
            },
            ..Default::default()
        };
        tokio::time::timeout(
            Duration::from_secs(20),
            super::run_agent_loop_with_config(
                &mut session,
                "synthetic request".into(),
                event_tx,
                main.clone(),
                Arc::new(bamboo_tools::BuiltinToolExecutor::new()),
                CancellationToken::new(),
                loop_config,
            ),
        )
        .await
        .expect("synthetic real runner must finish")
        .expect("synthetic foreground run must succeed");
        assert_eq!(main_calls.load(Ordering::SeqCst) - before_main, 2);
        let requests = account_a.received_requests().await.unwrap();
        let recent = &requests[request_offset..];
        request_offset = requests.len();
        let gold: Vec<_> = recent
            .iter()
            .filter(|request| {
                serde_json::from_slice::<serde_json::Value>(&request.body).unwrap()["model"]
                    == "explicit-gold"
            })
            .collect();
        assert!(account_b.received_requests().await.unwrap().is_empty());
        if revoked {
            assert!(
                gold.is_empty(),
                "revoked explicit Gold model must not dispatch; got {} requests",
                gold.len()
            );
        } else {
            for checkpoint in ["checkpoint=post_round", "checkpoint=terminal"] {
                assert!(
                    gold.iter().any(|request| {
                        String::from_utf8_lossy(&request.body).contains(checkpoint)
                    }),
                    "admitted explicit Gold must reach {checkpoint} on A"
                );
            }
        }
    }
}

#[tokio::test]
async fn agent_loop_gold_checkpoints_keep_provider_and_model_paired() {
    use crate::app_context::model_routing_test_support::RoutingContext;
    use crate::runtime::config::{AuxiliaryModelConfig, GoldConfig, PromptMemoryFlags};
    use bamboo_domain::ProviderModelRef;
    use bamboo_llm::{LLMRequestOptions, ProviderRegistry};
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct PairProvider {
        account: &'static str,
        main_calls: AtomicUsize,
        evaluations: Arc<std::sync::Mutex<Vec<(String, String, String)>>>,
    }

    #[async_trait]
    impl LLMProvider for PairProvider {
        async fn chat_stream(
            &self,
            messages: &[Message],
            tools: &[bamboo_agent_core::tools::ToolSchema],
            max: Option<u32>,
            model: &str,
        ) -> bamboo_llm::provider::Result<LLMStream> {
            self.chat_stream_with_options(messages, tools, max, model, None)
                .await
        }

        async fn chat_stream_with_options(
            &self,
            messages: &[Message],
            _tools: &[bamboo_agent_core::tools::ToolSchema],
            _max: Option<u32>,
            model: &str,
            options: Option<&LLMRequestOptions>,
        ) -> bamboo_llm::provider::Result<LLMStream> {
            let purpose = options
                .and_then(|value| value.request_purpose.as_deref())
                .unwrap_or("agent_loop");
            if purpose == "gold_evaluation" {
                let checkpoint = messages
                    .iter()
                    .find_map(|message| {
                        if message.content.contains("checkpoint=post_round") {
                            Some("post_round")
                        } else if message.content.contains("checkpoint=terminal") {
                            Some("terminal")
                        } else {
                            None
                        }
                    })
                    .expect("Gold request has its checkpoint in the synthetic prompt");
                self.evaluations.lock().unwrap().push((
                    self.account.into(),
                    model.into(),
                    checkpoint.into(),
                ));
                let call = bamboo_agent_core::tools::ToolCall {
                    id: "synthetic-gold".into(),
                    tool_type: "function".into(),
                    function: FunctionCall {
                        name: "report_gold_evaluation".into(),
                        arguments: serde_json::json!({
                            "decision": "achieved", "confidence": "high",
                            "reasoning": "synthetic goal verified", "missing_information": []
                        })
                        .to_string(),
                    },
                };
                return Ok(Box::pin(stream::iter([
                    Ok(LLMChunk::ToolCalls(vec![call])),
                    Ok(LLMChunk::Done),
                ])));
            }
            assert_eq!(
                self.account, "chat-a",
                "only A may run the foreground agent"
            );
            assert_eq!(model, "main-a");
            if self.main_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                let call = bamboo_agent_core::tools::ToolCall {
                    id: "synthetic-goal".into(),
                    tool_type: "function".into(),
                    function: FunctionCall {
                        name: "update_goal".into(),
                        arguments: r#"{"status":"complete"}"#.into(),
                    },
                };
                return Ok(Box::pin(stream::iter([
                    Ok(LLMChunk::ToolCalls(vec![call])),
                    Ok(LLMChunk::Done),
                ])));
            }
            // The real post-round evaluator is detached. Keep the foreground
            // turn open until it calls a synthetic provider, so finalization
            // cannot cancel it before this test observes the actual target.
            tokio::time::timeout(std::time::Duration::from_secs(1), async {
                loop {
                    let seen = self
                        .evaluations
                        .lock()
                        .unwrap()
                        .iter()
                        .any(|(_, _, checkpoint)| checkpoint == "post_round");
                    if seen {
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                }
            })
            .await
            .expect("post-round Gold evaluation reaches a synthetic provider");
            Ok(Box::pin(stream::iter([
                Ok(LLMChunk::Token("synthetic done".into())),
                Ok(LLMChunk::Done),
            ])))
        }
    }

    let mut observations = Vec::new();
    for explicit_gold in [false, true] {
        let context = RoutingContext::new(None);
        let snapshot = context.config.read().await.clone();
        let evaluations = Arc::new(std::sync::Mutex::new(Vec::new()));
        let main = Arc::new(PairProvider {
            account: "chat-a",
            main_calls: AtomicUsize::new(0),
            evaluations: evaluations.clone(),
        });
        let fast = Arc::new(PairProvider {
            account: "fast-b",
            main_calls: AtomicUsize::new(0),
            evaluations: evaluations.clone(),
        });
        let providers: HashMap<String, Arc<dyn LLMProvider>> = HashMap::from([
            ("chat-a".into(), main.clone() as Arc<dyn LLMProvider>),
            ("fast-b".into(), fast as Arc<dyn LLMProvider>),
        ]);
        let registry = Arc::new(ProviderRegistry::new(providers, "chat-a".into()));
        registry.set_runtime_models(HashMap::from([
            (
                "chat-a".into(),
                vec!["main-a".into(), "explicit-gold".into()],
            ),
            ("fast-b".into(), vec!["cheap-b".into()]),
        ]));
        let resolver = Arc::new(move || {
            let areas =
                crate::model_areas::resolve_global_area_models(&snapshot, "chat-a", &registry);
            AuxiliaryModelConfig {
                fast_model_name: areas.fast.as_ref().map(|target| target.model_name.clone()),
                fast_model_provider: areas.fast.map(|target| target.provider),
                background_model_name: areas
                    .background
                    .as_ref()
                    .map(|target| target.model_name.clone()),
                background_model_provider: areas.background.map(|target| target.provider),
                summarization_model_name: areas
                    .summarization
                    .as_ref()
                    .map(|target| target.model_name.clone()),
                summarization_model_provider: areas.summarization.map(|target| target.provider),
                summarization_model_error: areas.summarization_error,
                ..Default::default()
            }
        });
        let mut session = Session::new("synthetic-gold-pair-session", "main-a");
        session.model_ref = Some(ProviderModelRef::new("chat-a", "main-a"));
        let (event_tx, _event_rx) = mpsc::channel(128);
        let config = AgentLoopConfig {
            model_name: Some("main-a".into()),
            provider_name: Some("chat-a".into()),
            gold_config: Some(GoldConfig {
                enabled: true,
                auto_continue_enabled: true,
                goal: Some("synthetic goal".into()),
                model_name: explicit_gold.then(|| "explicit-gold".into()),
                ..Default::default()
            }),
            prompt_memory_flags: PromptMemoryFlags {
                project_prompt_injection: false,
                relevant_recall: false,
                relevant_recall_rerank: false,
                project_first_dream: false,
                ledger_agenda: false,
            },
            auxiliary_model_resolver: Some(resolver),
            run_budget: bamboo_config::RunBudgetConfig {
                max_rounds: Some(4),
                ..Default::default()
            },
            ..Default::default()
        };
        super::run_agent_loop_with_config(
            &mut session,
            "synthetic request".into(),
            event_tx,
            main.clone(),
            Arc::new(bamboo_tools::BuiltinToolExecutor::new()),
            CancellationToken::new(),
            config,
        )
        .await
        .expect("synthetic goal loop should complete");
        assert!(main.main_calls.load(Ordering::SeqCst) >= 2);
        let mut observed = evaluations.lock().unwrap().clone();
        observed.sort();
        let (account, model) = if explicit_gold {
            ("chat-a", "explicit-gold")
        } else {
            ("fast-b", "cheap-b")
        };
        let mut expected = vec![
            (
                account.to_string(),
                model.to_string(),
                "post_round".to_string(),
            ),
            (
                account.to_string(),
                model.to_string(),
                "terminal".to_string(),
            ),
        ];
        expected.sort();
        observations.push((explicit_gold, observed, expected));
    }
    assert!(
        observations
            .iter()
            .all(|(_, actual, expected)| actual == expected),
        "each Gold checkpoint must use its configured account/model pair: {observations:#?}"
    );
}

#[tokio::test]
async fn agent_loop_rejects_explicit_invalid_fast_for_task_and_gold_evaluations() {
    use crate::app_context::model_routing_test_support::RoutingContext;
    use crate::runtime::config::{AuxiliaryModelConfig, GoldConfig};
    use bamboo_llm::LLMRequestOptions;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct SyntheticMain {
        main_calls: AtomicUsize,
        evaluations: std::sync::Mutex<Vec<(String, String)>>,
        fast: Arc<crate::app_context::model_routing_test_support::CaptureProvider>,
    }

    #[async_trait]
    impl LLMProvider for SyntheticMain {
        async fn chat_stream(
            &self,
            messages: &[Message],
            tools: &[bamboo_agent_core::tools::ToolSchema],
            max: Option<u32>,
            model: &str,
        ) -> bamboo_llm::provider::Result<LLMStream> {
            self.chat_stream_with_options(messages, tools, max, model, None)
                .await
        }

        async fn chat_stream_with_options(
            &self,
            _messages: &[Message],
            _tools: &[bamboo_agent_core::tools::ToolSchema],
            _max: Option<u32>,
            model: &str,
            options: Option<&LLMRequestOptions>,
        ) -> bamboo_llm::provider::Result<LLMStream> {
            let purpose = options
                .and_then(|value| value.request_purpose.as_deref())
                .unwrap_or("agent_loop");
            if matches!(purpose, "task_evaluation" | "gold_evaluation") {
                self.evaluations
                    .lock()
                    .unwrap()
                    .push((model.to_string(), purpose.to_string()));
                return Err(LLMError::Api("synthetic evaluation refusal".into()));
            }
            if self.main_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                let call = bamboo_agent_core::tools::ToolCall {
                    id: "task-write".into(),
                    tool_type: "function".into(),
                    function: FunctionCall {
                        name: "Task".into(),
                        arguments: serde_json::json!({
                            "tasks": [{"content": "Synthetic work", "status": "in_progress",
                                "activeForm": "Doing synthetic work"}]
                        })
                        .to_string(),
                    },
                };
                return Ok(Box::pin(stream::iter([
                    Ok(LLMChunk::ToolCalls(vec![call])),
                    Ok(LLMChunk::Done),
                ])));
            }
            // Keep the real foreground loop alive until both post-round tasks
            // have had a chance to reach their fake provider. A correct repair
            // may dispatch neither, so the bounded wait must also expire.
            let _ = tokio::time::timeout(std::time::Duration::from_millis(500), async {
                loop {
                    let main_count = self.evaluations.lock().unwrap().len();
                    let fast_count = self.fast.requests.lock().unwrap().len();
                    if main_count + fast_count >= 2 {
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                }
            })
            .await;
            Ok(Box::pin(stream::iter([
                Ok(LLMChunk::Token("synthetic done".into())),
                Ok(LLMChunk::Done),
            ])))
        }
    }

    for flag in [None, Some(false), Some(true)] {
        for invalid in [false, true] {
            let routing = RoutingContext::new(flag);
            let mut config_snapshot = routing.config.read().await.clone();
            // Preserve the old registry and target while the live config now
            // rejects Fast; the valid run is the actual-consumer positive.
            if invalid {
                config_snapshot
                    .provider_instances
                    .get_mut("fast-b")
                    .unwrap()
                    .enabled = false;
            }
            assert!(routing.registry.get("fast-b").is_some());
            let snapshot = Arc::new(std::sync::Mutex::new(config_snapshot));
            let resolver_snapshot = snapshot.clone();
            let registry = routing.registry.clone();
            let resolver = Arc::new(move || {
                let config = resolver_snapshot.lock().unwrap();
                let areas =
                    crate::model_areas::resolve_global_area_models(&config, "chat-a", &registry);
                let (fast_model_name, fast_model_provider) =
                    crate::model_config_helper::resolve_fast_model_for_runner(
                        Some(&config),
                        "chat-a",
                        &registry,
                    );
                AuxiliaryModelConfig {
                    fast_model_name,
                    fast_model_provider,
                    background_model_name: areas
                        .background
                        .as_ref()
                        .map(|target| target.model_name.clone()),
                    background_model_provider: areas.background.map(|target| target.provider),
                    summarization_model_name: areas
                        .summarization
                        .as_ref()
                        .map(|target| target.model_name.clone()),
                    summarization_model_provider: areas.summarization.map(|target| target.provider),
                    summarization_model_error: areas.summarization_error,
                    ..Default::default()
                }
            });
            let main = Arc::new(SyntheticMain {
                main_calls: AtomicUsize::new(0),
                evaluations: std::sync::Mutex::new(Vec::new()),
                fast: routing.providers["fast-b"].clone(),
            });
            let tools = BuiltinToolExecutorBuilder::new()
                .with_command_tool("Task")
                .expect("register Task tool")
                .build();
            let mut session = Session::new("synthetic-evaluation-session", "main-a");
            session.set_task_list(task_list_with_in_progress_item(
                &session.id,
                "Synthetic work",
            ));
            let (event_tx, _event_rx) = mpsc::channel(128);
            let loop_config = AgentLoopConfig {
                system_prompt: Some("synthetic system".into()),
                model_name: Some("main-a".into()),
                gold_config: Some(GoldConfig {
                    enabled: true,
                    ..Default::default()
                }),
                auxiliary_model_resolver: Some(resolver),
                run_budget: bamboo_config::RunBudgetConfig {
                    max_rounds: Some(3),
                    ..Default::default()
                },
                ..Default::default()
            };
            let outcome = super::run_agent_loop_with_config(
                &mut session,
                "synthetic goal".into(),
                event_tx,
                main.clone(),
                Arc::new(tools),
                CancellationToken::new(),
                loop_config,
            )
            .await;
            // Fail the fixture on unrelated runner errors: without an actual
            // foreground round, a no-dispatch observation would be meaningless.
            outcome.expect("synthetic foreground loop should complete");
            assert!(main.main_calls.load(Ordering::SeqCst) >= 2);
            let on_main = main.evaluations.lock().unwrap().clone();
            let on_fast = routing.requests("fast-b");
            if invalid {
                assert!(on_main.is_empty() && on_fast.is_empty(),
                    "explicit disabled Fast must not dispatch: flag={flag:?}, main={on_main:?}, fast={on_fast:?}");
            } else {
                assert!(
                    on_main.is_empty(),
                    "valid Fast must not use main account: {on_main:?}"
                );
                for purpose in ["task_evaluation", "gold_evaluation"] {
                    assert!(
                        on_fast
                            .iter()
                            .any(|(model, actual)| model == "cheap-b" && actual == purpose),
                        "valid Fast must reach {purpose}: flag={flag:?}, fast={on_fast:?}"
                    );
                }
            }
        }
    }
}

#[tokio::test]
async fn runner_fast_selection_distinguishes_unset_from_rejected_route() {
    use crate::app_context::model_routing_test_support::RoutingContext;
    use crate::model_config_helper::resolve_fast_model_for_runner;

    let routing = RoutingContext::new(None);
    let config = routing.config.read().await.clone();
    let (name, provider) =
        resolve_fast_model_for_runner(Some(&config), "chat-a", &routing.registry);
    let expected_provider: Arc<dyn LLMProvider> = routing.providers["fast-b"].clone();
    assert_eq!(name.as_deref(), Some("cheap-b"));
    assert!(Arc::ptr_eq(&provider.unwrap(), &expected_provider));

    let mut disabled = config.clone();
    disabled
        .provider_instances
        .get_mut("fast-b")
        .unwrap()
        .enabled = false;
    let (name, provider) =
        resolve_fast_model_for_runner(Some(&disabled), "chat-a", &routing.registry);
    assert_eq!(name.as_deref(), Some("cheap-b"));
    let provider = provider.expect("explicit refusal must not become an unset role");
    for _ in 0..2 {
        assert!(matches!(
            provider.chat_stream(&[], &[], None, "cheap-b").await,
            Err(LLMError::Auth(message)) if message.contains("disabled")
        ));
    }
    assert!(matches!(
        provider
            .chat_stream_with_options(&[], &[], None, "cheap-b", None)
            .await,
        Err(LLMError::Auth(_))
    ));
    assert!(!provider.supports_vision("cheap-b").await);
    assert!(provider.list_models().await.is_err());

    let mut unadmitted = config.clone();
    unadmitted
        .provider_instances
        .get_mut("fast-b")
        .unwrap()
        .extra
        .insert(
            "runtime_models".into(),
            serde_json::json!(["different-model"]),
        );
    let (_, provider) =
        resolve_fast_model_for_runner(Some(&unadmitted), "chat-a", &routing.registry);
    assert!(matches!(
        provider
            .unwrap()
            .chat_stream(&[], &[], None, "cheap-b")
            .await,
        Err(LLMError::Api(_))
    ));

    let mut missing = config.clone();
    missing
        .defaults
        .as_mut()
        .unwrap()
        .fast
        .as_mut()
        .unwrap()
        .provider = "missing-b".into();
    let (_, provider) = resolve_fast_model_for_runner(Some(&missing), "chat-a", &routing.registry);
    assert!(matches!(
        provider
            .unwrap()
            .chat_stream(&[], &[], None, "cheap-b")
            .await,
        Err(LLMError::Auth(_))
    ));

    let mut malformed = config.clone();
    malformed
        .defaults
        .as_mut()
        .unwrap()
        .fast
        .as_mut()
        .unwrap()
        .model
        .clear();
    let (_, provider) =
        resolve_fast_model_for_runner(Some(&malformed), "chat-a", &routing.registry);
    assert!(matches!(
        provider.unwrap().chat_stream(&[], &[], None, "").await,
        Err(LLMError::Api(_))
    ));

    let empty = bamboo_llm::Config::default();
    let (name, provider) =
        resolve_fast_model_for_runner(Some(&empty), "no-such-provider", &routing.registry);
    assert!(
        name.is_none() && provider.is_none(),
        "unset preserves legacy fallback"
    );
    let (name, provider) = resolve_fast_model_for_runner(None, "chat-a", &routing.registry);
    assert!(name.is_none());
    assert!(matches!(
        provider
            .unwrap()
            .chat_stream(&[], &[], None, "main-a")
            .await,
        Err(LLMError::Api(_))
    ));
    routing.assert_no_requests();
}

#[tokio::test]
async fn agent_loop_uses_refreshed_fast_model_for_between_round_task_evaluation() {
    struct RecordingRoundProvider {
        queue: Mutex<Vec<Vec<bamboo_llm::provider::Result<LLMChunk>>>>,
        fast_models: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl LLMProvider for RecordingRoundProvider {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[bamboo_agent_core::tools::ToolSchema],
            _max_output_tokens: Option<u32>,
            model: &str,
        ) -> bamboo_llm::provider::Result<LLMStream> {
            if model.starts_with("fast-") {
                self.fast_models.lock().await.push(model.to_string());
                return Err(LLMError::Api("intentional fast-model failure".to_string()));
            }

            // When the second chat round starts, the first task evaluator has
            // already been spawned but may not have received executor time yet.
            // Wait for that background request to enter the provider so this
            // test observes the between-round refresh deterministically without
            // relying on the finalize path to drain it.
            if self.queue.lock().await.len() == 2 {
                tokio::time::timeout(std::time::Duration::from_secs(1), async {
                    while self.fast_models.lock().await.is_empty() {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .expect("first between-round task evaluation should start");
            }

            let mut guard = self.queue.lock().await;
            if guard.is_empty() {
                panic!("test provider queue exhausted");
            }
            let items = guard.remove(0);
            Ok(Box::pin(stream::iter(items)))
        }
    }

    struct NoopTool;

    #[async_trait]
    impl Tool for NoopTool {
        fn name(&self) -> &str {
            "noop_tool"
        }

        fn description(&self) -> &str {
            "no-op tool for round boundary testing"
        }

        fn parameters_schema(&self) -> serde_json::Value {
            serde_json::json!({
                "type": "object",
                "properties": {},
                "additionalProperties": false
            })
        }

        async fn invoke(
            &self,
            _args: serde_json::Value,
            _ctx: ToolCtx,
        ) -> Result<ToolOutcome, ToolError> {
            Ok(ToolOutcome::Completed(ToolResult {
                success: true,
                result: "ok".to_string(),
                display_preference: None,
                images: Vec::new(),
            }))
        }
    }

    let tools = BuiltinToolExecutorBuilder::new()
        .with_tool(NoopTool)
        .expect("register test tool")
        .with_command_tool("Task")
        .expect("register Task tool")
        .build();

    // Task evaluation fires only on Task-tool writes. Two writes also exercise
    // coalescing while the first auxiliary request is in flight; normal
    // finalization intentionally cancels the queued final-round snapshot (#593).
    let tool_call = |id: &str| bamboo_agent_core::tools::ToolCall {
        id: id.to_string(),
        tool_type: "function".to_string(),
        function: FunctionCall {
            name: "Task".to_string(),
            arguments: serde_json::json!({
                "tasks": [{
                    "content": "Verify auxiliary refresh",
                    "status": "in_progress",
                    "activeForm": "Verifying auxiliary refresh"
                }]
            })
            .to_string(),
        },
    };

    let provider = Arc::new(RecordingRoundProvider {
        queue: Mutex::new(vec![
            vec![
                Ok(LLMChunk::ToolCalls(vec![tool_call("call-1")])),
                Ok(LLMChunk::Done),
            ],
            vec![
                Ok(LLMChunk::ToolCalls(vec![tool_call("call-2")])),
                Ok(LLMChunk::Done),
            ],
            vec![Ok(LLMChunk::Token("done".to_string())), Ok(LLMChunk::Done)],
        ]),
        fast_models: Mutex::new(Vec::new()),
    });

    let mut session = Session::new("session-fast-refresh", "sticky-chat-model");
    session.set_task_list(task_list_with_in_progress_item(
        &session.id,
        "Verify auxiliary refresh",
    ));

    let fast_counter = Arc::new(std::sync::Mutex::new(0usize));
    let fast_counter_for_resolver = fast_counter.clone();

    let (event_tx, _event_rx) = mpsc::channel(64);
    let config = AgentLoopConfig {
        run_budget: bamboo_config::RunBudgetConfig {
            max_rounds: Some(5),
            ..Default::default()
        },
        system_prompt: Some("sys".to_string()),
        model_name: Some("sticky-chat-model".to_string()),
        auxiliary_model_resolver: Some(Arc::new(move || {
            let mut guard = fast_counter_for_resolver.lock().expect("fast counter lock");
            *guard += 1;
            crate::runtime::config::AuxiliaryModelConfig {
                fast_model_name: Some(format!("fast-{}", *guard)),
                ..Default::default()
            }
        })),
        ..Default::default()
    };

    super::run_agent_loop_with_config(
        &mut session,
        "hello".to_string(),
        event_tx,
        provider.clone(),
        Arc::new(tools),
        CancellationToken::new(),
        config,
    )
    .await
    .expect("agent loop should succeed");

    let fast_models = provider.fast_models.lock().await.clone();
    // `fast-1` was resolved at startup; the between-round refresh must select
    // `fast-2` for the first evaluation. Depending on scheduling, that request
    // may finish before the next round polls it, allowing the second write to
    // launch with `fast-3`; otherwise it remains queued and is cancelled at
    // finalization. Both are valid, and neither requires a finalize-time drain.
    assert!(
        matches!(
            fast_models.as_slice(),
            [first] if first == "fast-2"
        ) || matches!(
            fast_models.as_slice(),
            [first, second] if first == "fast-2" && second == "fast-3"
        ),
        "between-round evaluations must use refreshed fast models in order, got {fast_models:?}"
    );
}

#[tokio::test]
async fn cancelled_pipeline_releases_activation_without_false_completion() {
    let trace = bamboo_infrastructure::telemetry::test_support::TraceTest::default();
    struct NeverCalledProvider;

    #[async_trait]
    impl LLMProvider for NeverCalledProvider {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[bamboo_agent_core::tools::ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> bamboo_llm::provider::Result<LLMStream> {
            panic!("pre-cancelled pipeline must not call the provider")
        }
    }

    let directory = tempfile::tempdir().expect("tempdir");
    let skill_root = directory.path().join("skills/cancel-demo");
    tokio::fs::create_dir_all(&skill_root)
        .await
        .expect("skill root");
    tokio::fs::write(
        skill_root.join("SKILL.md"),
        "---\nname: cancel-demo\ndescription: cancel\n---\ncancel\n",
    )
    .await
    .expect("skill");
    let manager = Arc::new(bamboo_skills::SkillManager::with_config(
        bamboo_skills::SkillStoreConfig {
            skills_dir: directory.path().join("skills"),
            ..Default::default()
        },
    ));
    manager.initialize().await.expect("initialize");
    manager
        .store()
        .pin_current_activation("cancelled-activation", &["cancel-demo".to_string()], None)
        .await
        .expect("pin activation");
    let mut session = Session::new("cancelled-activation", "model");
    session.metadata.insert(
        bamboo_skills::runtime_metadata::SKILL_RUNTIME_SELECTION_SOURCE_KEY.to_string(),
        "auto".to_string(),
    );
    let config = AgentLoopConfig {
        skill_manager: Some(manager.clone()),
        model_name: Some("model".to_string()),
        ..Default::default()
    };
    let cancel = CancellationToken::new();
    cancel.cancel();
    let (event_tx, mut event_rx) = mpsc::channel(16);
    let result = trace
        .scope(super::run_agent_loop_with_config(
            &mut session,
            "cancel".to_string(),
            event_tx,
            Arc::new(NeverCalledProvider),
            Arc::new(bamboo_tools::BuiltinToolExecutor::new()),
            cancel,
            config,
        ))
        .await;

    assert!(matches!(
        result,
        Err(bamboo_agent_core::AgentError::Cancelled)
    ));
    assert!(manager
        .store()
        .activation_descriptor("cancelled-activation")
        .await
        .is_none());
    assert!(!session.agent_runtime_state.as_ref().is_some_and(|state| {
        matches!(state.status, bamboo_domain::AgentStatusState::Completed)
    }));
    while let Ok(event) = event_rx.try_recv() {
        assert!(
            !matches!(event, bamboo_agent_core::AgentEvent::Complete { .. }),
            "cancelled pipeline must not emit Complete"
        );
    }
    let spans = trace.spans();
    assert_eq!(
        spans.len(),
        1,
        "pre-cancelled invocation must not fabricate provider/tool spans"
    );
    assert_eq!(spans[0].name, "invoke_agent");
    assert!(spans[0]
        .attributes
        .iter()
        .any(|kv| kv.key.as_str() == "error.type" && kv.value.to_string() == "cancelled"));
}

#[tokio::test]
async fn suspended_restart_without_retained_snapshot_continues_degraded() {
    struct CalledProvider {
        called: std::sync::atomic::AtomicBool,
        messages: std::sync::Mutex<Vec<Message>>,
    }

    #[async_trait]
    impl LLMProvider for CalledProvider {
        async fn chat_stream(
            &self,
            messages: &[Message],
            _tools: &[bamboo_agent_core::tools::ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> bamboo_llm::provider::Result<LLMStream> {
            self.called.store(true, std::sync::atomic::Ordering::SeqCst);
            self.messages
                .lock()
                .expect("provider messages lock")
                .extend_from_slice(messages);
            Ok(Box::pin(stream::iter(vec![
                Ok(LLMChunk::Token("continued without workflow".to_string())),
                Ok(LLMChunk::Done),
            ])))
        }
    }

    let directory = tempfile::tempdir().expect("tempdir");
    let skill_root = directory.path().join("skills/restart-demo");
    tokio::fs::create_dir_all(&skill_root)
        .await
        .expect("skill root");
    tokio::fs::write(
        skill_root.join("SKILL.md"),
        "---\nname: restart-demo\ndescription: restart\n---\nrestart N+1\n",
    )
    .await
    .expect("skill");
    let manager = Arc::new(bamboo_skills::SkillManager::with_config(
        bamboo_skills::SkillStoreConfig {
            skills_dir: directory.path().join("skills"),
            ..Default::default()
        },
    ));
    manager.initialize().await.expect("initialize");

    let mut session = Session::new("restart-missing-pin", "model");
    let mut suspended = bamboo_domain::AgentRuntimeState::new("restart-missing-pin");
    suspended.status = bamboo_domain::AgentStatusState::Suspended;
    session.agent_runtime_state = Some(suspended);
    session.metadata.insert(
        bamboo_skills::runtime_metadata::SKILL_RUNTIME_ACTIVATION_GENERATION_KEY.to_string(),
        "7".to_string(),
    );
    session.metadata.insert(
        bamboo_skills::runtime_metadata::SKILL_RUNTIME_SELECTION_SOURCE_KEY.to_string(),
        "explicit".to_string(),
    );
    session.metadata.insert(
        bamboo_skills::runtime_metadata::SKILL_RUNTIME_SELECTED_SKILL_REVISIONS_KEY.to_string(),
        r#"{"restart-demo":3}"#.to_string(),
    );
    let config = AgentLoopConfig {
        skill_manager: Some(manager.clone()),
        selected_skill_ids: Some(vec!["restart-demo".to_string()]),
        model_name: Some("model".to_string()),
        ..Default::default()
    };
    let (event_tx, mut event_rx) = mpsc::channel(64);
    let provider = Arc::new(CalledProvider {
        called: std::sync::atomic::AtomicBool::new(false),
        messages: std::sync::Mutex::new(Vec::new()),
    });
    let result = super::run_agent_loop_with_config(
        &mut session,
        "continue".to_string(),
        event_tx,
        provider.clone(),
        Arc::new(bamboo_tools::BuiltinToolExecutor::new()),
        CancellationToken::new(),
        config,
    )
    .await;

    result.expect("missing LKG degrades workflow without killing the main session");
    assert!(provider.called.load(std::sync::atomic::Ordering::SeqCst));
    let (has_degraded_diagnostic, has_workflow_runtime) = {
        let provider_messages = provider.messages.lock().expect("provider messages lock");
        (
            provider_messages
                .iter()
                .any(|message| message.content.contains("Workflow Activation Degraded")),
            provider_messages
                .iter()
                .any(|message| message.content.contains("context_type: workflow_runtime")),
        )
    };
    assert!(has_degraded_diagnostic);
    assert!(!has_workflow_runtime);
    assert!(session
        .metadata
        .get(bamboo_skills::runtime_metadata::SKILL_RUNTIME_ACTIVATION_ERROR_KEY)
        .is_some_and(|message| message.contains("snapshot")));
    assert!(manager
        .store()
        .activation_descriptor("restart-missing-pin")
        .await
        .is_none());
    assert!(!session
        .metadata
        .contains_key(bamboo_skills::ACTIVE_WORKFLOW_METADATA_KEY));
    assert!(!session
        .metadata
        .contains_key(bamboo_skills::ACTIVE_WORKFLOW_SNAPSHOT_METADATA_KEY));
    while let Ok(event) = event_rx.try_recv() {
        assert!(!matches!(
            event,
            bamboo_agent_core::AgentEvent::WorkflowActivated { .. }
        ));
    }
}

#[tokio::test]
async fn suspended_restart_with_empty_selection_does_not_require_snapshot() {
    struct CalledProvider(std::sync::atomic::AtomicBool);

    #[async_trait]
    impl LLMProvider for CalledProvider {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[bamboo_agent_core::tools::ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> bamboo_llm::provider::Result<LLMStream> {
            self.0.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(Box::pin(stream::iter(vec![
                Ok(LLMChunk::Token("continued".to_string())),
                Ok(LLMChunk::Done),
            ])))
        }
    }

    let directory = tempfile::tempdir().expect("tempdir");
    let manager = Arc::new(bamboo_skills::SkillManager::with_config(
        bamboo_skills::SkillStoreConfig {
            skills_dir: directory.path().join("skills"),
            ..Default::default()
        },
    ));
    manager.initialize().await.expect("initialize");
    let mut session = Session::new("restart-empty-selection", "model");
    let mut suspended = bamboo_domain::AgentRuntimeState::new("restart-empty-selection");
    suspended.status = bamboo_domain::AgentStatusState::Suspended;
    session.agent_runtime_state = Some(suspended);
    session.metadata.insert(
        bamboo_skills::runtime_metadata::SKILL_RUNTIME_ACTIVATION_GENERATION_KEY.to_string(),
        "7".to_string(),
    );
    session.metadata.insert(
        bamboo_skills::runtime_metadata::SKILL_RUNTIME_SELECTED_SKILL_REVISIONS_KEY.to_string(),
        "{}".to_string(),
    );
    let provider = Arc::new(CalledProvider(std::sync::atomic::AtomicBool::new(false)));
    let (event_tx, _event_rx) = mpsc::channel(8);
    super::run_agent_loop_with_config(
        &mut session,
        "continue".to_string(),
        event_tx,
        provider.clone(),
        Arc::new(bamboo_tools::BuiltinToolExecutor::new()),
        CancellationToken::new(),
        AgentLoopConfig {
            skill_manager: Some(manager),
            selected_skill_ids: Some(Vec::new()),
            model_name: Some("model".to_string()),
            run_budget: bamboo_config::RunBudgetConfig {
                max_rounds: Some(1),
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await
    .expect("empty selection continuation");
    assert!(provider.0.load(std::sync::atomic::Ordering::SeqCst));
}

#[tokio::test]
async fn activation_metadata_persistence_failure_releases_pin_before_provider_call() {
    struct FailingPersistence;
    #[async_trait]
    impl bamboo_domain::RuntimeSessionPersistence for FailingPersistence {
        async fn save_runtime_session(&self, _session: &mut Session) -> std::io::Result<()> {
            Err(std::io::Error::other("injected persistence failure"))
        }
    }
    struct NeverCalledProvider;
    #[async_trait]
    impl LLMProvider for NeverCalledProvider {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[bamboo_agent_core::tools::ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> bamboo_llm::provider::Result<LLMStream> {
            panic!("persistence failure must stop before provider execution")
        }
    }

    let directory = tempfile::tempdir().expect("tempdir");
    let manager = Arc::new(bamboo_skills::SkillManager::with_config(
        bamboo_skills::SkillStoreConfig {
            skills_dir: directory.path().join("skills"),
            ..Default::default()
        },
    ));
    manager.initialize().await.expect("initialize");
    let mut session = Session::new("persistence-failure", "model");
    let (event_tx, _event_rx) = mpsc::channel(4);
    let result = super::run_agent_loop_with_config(
        &mut session,
        "review".to_string(),
        event_tx,
        Arc::new(NeverCalledProvider),
        Arc::new(bamboo_tools::BuiltinToolExecutor::new()),
        CancellationToken::new(),
        AgentLoopConfig {
            skill_manager: Some(manager.clone()),
            selected_skill_ids: Some(vec!["review".to_string()]),
            persistence: Some(Arc::new(FailingPersistence)),
            model_name: Some("model".to_string()),
            ..Default::default()
        },
    )
    .await;
    assert!(result
        .expect_err("setup must fail closed")
        .to_string()
        .contains("could not be published before tool/model execution"));
    assert!(manager
        .store()
        .activation_descriptor("persistence-failure")
        .await
        .is_none());
}

#[tokio::test]
async fn activation_tool_call_checkpoint_failure_stops_before_tool_and_releases_pin() {
    struct FailAssistantToolCallSave(Arc<std::sync::atomic::AtomicUsize>);
    #[async_trait]
    impl bamboo_domain::RuntimeSessionPersistence for FailAssistantToolCallSave {
        async fn save_runtime_session(&self, session: &mut Session) -> std::io::Result<()> {
            let is_assistant_tool_call_boundary = session.messages.iter().rev().any(|message| {
                matches!(message.role, bamboo_agent_core::Role::Assistant)
                    && message.tool_calls.as_ref().is_some_and(|calls| {
                        calls.iter().any(|call| call.function.name == "load_skill")
                    })
            });
            if is_assistant_tool_call_boundary {
                self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Err(std::io::Error::other(
                    "assistant tool-call checkpoint failed",
                ))
            } else {
                Ok(())
            }
        }
    }
    struct NeverExecutedLoad;
    #[async_trait]
    impl bamboo_agent_core::tools::ToolExecutor for NeverExecutedLoad {
        async fn execute(
            &self,
            _call: &bamboo_agent_core::tools::ToolCall,
        ) -> bamboo_agent_core::tools::executor::Result<ToolResult> {
            panic!("assistant tool-call checkpoint must persist before load_skill executes")
        }
        fn list_tools(&self) -> Vec<bamboo_agent_core::tools::ToolSchema> {
            vec![bamboo_agent_core::tools::ToolSchema {
                schema_type: "function".to_string(),
                function: bamboo_agent_core::tools::FunctionSchema {
                    name: "load_skill".to_string(),
                    description: "load".to_string(),
                    parameters: serde_json::json!({"type":"object"}),
                },
            }]
        }
    }
    struct ActivationProvider;
    #[async_trait]
    impl LLMProvider for ActivationProvider {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[bamboo_agent_core::tools::ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> bamboo_llm::provider::Result<LLMStream> {
            let call = bamboo_agent_core::tools::ToolCall {
                id: "load-review".to_string(),
                tool_type: "function".to_string(),
                function: FunctionCall {
                    name: "load_skill".to_string(),
                    arguments: r#"{"skill_id":"review"}"#.to_string(),
                },
            };
            Ok(Box::pin(stream::iter(vec![
                Ok(LLMChunk::ToolCalls(vec![call])),
                Ok(LLMChunk::Done),
            ])))
        }
    }
    let directory = tempfile::tempdir().expect("tempdir");
    let manager = Arc::new(bamboo_skills::SkillManager::with_config(
        bamboo_skills::SkillStoreConfig {
            skills_dir: directory.path().join("skills"),
            ..Default::default()
        },
    ));
    manager.initialize().await.expect("initialize");
    let mut session = Session::new("activation-checkpoint-failure", "model");
    let save_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (event_tx, _event_rx) = mpsc::channel(4);
    let result = super::run_agent_loop_with_config(
        &mut session,
        "review".to_string(),
        event_tx,
        Arc::new(ActivationProvider),
        Arc::new(NeverExecutedLoad),
        CancellationToken::new(),
        AgentLoopConfig {
            skill_manager: Some(manager.clone()),
            selected_skill_ids: Some(vec!["review".to_string()]),
            persistence: Some(Arc::new(FailAssistantToolCallSave(save_count.clone()))),
            model_name: Some("model".to_string()),
            ..Default::default()
        },
    )
    .await;
    assert!(result
        .expect_err("activation tool-call checkpoint must fail")
        .to_string()
        .contains("assistant tool-call checkpoint could not be persisted"));
    assert_eq!(save_count.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert!(manager
        .store()
        .activation_descriptor("activation-checkpoint-failure")
        .await
        .is_none());
    assert!(!session
        .metadata
        .contains_key(bamboo_skills::ACTIVE_WORKFLOW_METADATA_KEY));
    assert!(!session
        .metadata
        .contains_key(bamboo_skills::ACTIVE_WORKFLOW_SNAPSHOT_METADATA_KEY));
}

#[tokio::test]
async fn unsuccessful_model_issued_explicit_activation_stops_before_answer_and_releases_pin() {
    struct UnsuccessfulLoad;
    #[async_trait]
    impl bamboo_agent_core::tools::ToolExecutor for UnsuccessfulLoad {
        async fn execute(
            &self,
            _call: &bamboo_agent_core::tools::ToolCall,
        ) -> bamboo_agent_core::tools::executor::Result<ToolResult> {
            Ok(ToolResult {
                success: false,
                result: "injected activation failure".to_string(),
                display_preference: None,
                images: Vec::new(),
            })
        }
        fn list_tools(&self) -> Vec<bamboo_agent_core::tools::ToolSchema> {
            vec![bamboo_agent_core::tools::ToolSchema {
                schema_type: "function".to_string(),
                function: bamboo_agent_core::tools::FunctionSchema {
                    name: "load_skill".to_string(),
                    description: "load".to_string(),
                    parameters: serde_json::json!({"type":"object"}),
                },
            }]
        }
    }
    struct ActivationProvider;
    #[async_trait]
    impl LLMProvider for ActivationProvider {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[bamboo_agent_core::tools::ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> bamboo_llm::provider::Result<LLMStream> {
            let call = bamboo_agent_core::tools::ToolCall {
                id: "load-review".to_string(),
                tool_type: "function".to_string(),
                function: FunctionCall {
                    name: "load_skill".to_string(),
                    arguments: r#"{"skill_id":"review"}"#.to_string(),
                },
            };
            Ok(Box::pin(stream::iter(vec![
                Ok(LLMChunk::ToolCalls(vec![call])),
                Ok(LLMChunk::Done),
            ])))
        }
    }
    let directory = tempfile::tempdir().expect("tempdir");
    let manager = Arc::new(bamboo_skills::SkillManager::with_config(
        bamboo_skills::SkillStoreConfig {
            skills_dir: directory.path().join("skills"),
            ..Default::default()
        },
    ));
    manager.initialize().await.expect("initialize");
    let mut session = Session::new("unsuccessful-model-activation", "model");
    let (event_tx, _event_rx) = mpsc::channel(64);
    let result = super::run_agent_loop_with_config(
        &mut session,
        "review".to_string(),
        event_tx,
        Arc::new(ActivationProvider),
        Arc::new(UnsuccessfulLoad),
        CancellationToken::new(),
        AgentLoopConfig {
            skill_manager: Some(manager.clone()),
            selected_skill_ids: Some(vec!["review".to_string()]),
            model_name: Some("model".to_string()),
            ..Default::default()
        },
    )
    .await;
    assert!(result
        .expect_err("model-issued activation must fail closed")
        .to_string()
        .contains("failed to activate; refusing to continue"));
    assert!(manager
        .store()
        .activation_descriptor("unsuccessful-model-activation")
        .await
        .is_none());
}

struct MetricsQueueProvider {
    queue: Mutex<Vec<Vec<bamboo_llm::provider::Result<LLMChunk>>>>,
    requested_messages: Mutex<Vec<Vec<Message>>>,
}

#[async_trait]
impl LLMProvider for MetricsQueueProvider {
    async fn chat_stream(
        &self,
        messages: &[Message],
        _tools: &[bamboo_agent_core::tools::ToolSchema],
        _max_output_tokens: Option<u32>,
        _model: &str,
    ) -> bamboo_llm::provider::Result<LLMStream> {
        self.requested_messages.lock().await.push(messages.to_vec());
        let mut queue = self.queue.lock().await;
        assert!(!queue.is_empty(), "metrics provider queue exhausted");
        Ok(Box::pin(stream::iter(queue.remove(0))))
    }
}

fn provider_usage(prompt: u64, completion: u64) -> LLMChunk {
    LLMChunk::ProviderUsage {
        input_tokens: Some(prompt),
        output_tokens: Some(completion),
        total_tokens: Some(prompt.saturating_add(completion)),
        reasoning_tokens: None,
        cache_creation_input_tokens: None,
        cache_read_input_tokens: None,
        cache_write_input_tokens: None,
    }
}

fn metric_execution_prefix(round_id: &str) -> &str {
    round_id
        .rsplit_once("-round-")
        .map(|(prefix, _)| prefix)
        .expect("round id suffix")
}

async fn create_runner_metrics() -> (
    tempfile::TempDir,
    bamboo_metrics::MetricsCollector,
    Arc<bamboo_metrics::SqliteMetricsStorage>,
) {
    let directory = tempfile::tempdir().expect("metrics tempdir");
    let storage = Arc::new(bamboo_metrics::SqliteMetricsStorage::new(
        directory.path().join("metrics.db"),
    ));
    storage.init().await.expect("initialize metrics storage");
    let collector = bamboo_metrics::MetricsCollector::spawn(storage.clone(), 7);
    (directory, collector, storage)
}

fn runner_metrics_config(collector: &bamboo_metrics::MetricsCollector) -> AgentLoopConfig {
    AgentLoopConfig {
        run_budget: bamboo_config::RunBudgetConfig {
            max_rounds: Some(2),
            ..Default::default()
        },
        system_prompt: Some("metrics regression test".to_string()),
        model_name: Some("test-model".to_string()),
        metrics_collector: Some(collector.clone()),
        prompt_memory_flags: crate::runtime::config::PromptMemoryFlags {
            project_prompt_injection: false,
            relevant_recall: false,
            relevant_recall_rerank: false,
            project_first_dream: false,
            ledger_agenda: false,
        },
        ..AgentLoopConfig::default()
    }
}

async fn wait_for_runner_metrics(
    storage: &bamboo_metrics::SqliteMetricsStorage,
    session_id: &str,
    expected_rounds: usize,
    expected_usage: bamboo_metrics::types::TokenUsage,
) -> bamboo_metrics::types::SessionDetail {
    for _ in 0..100 {
        if let Some(detail) = storage
            .session_detail(session_id)
            .await
            .expect("session metrics query")
        {
            if detail.rounds.len() == expected_rounds
                && detail
                    .rounds
                    .iter()
                    .all(|round| round.completed_at.is_some())
                && detail.session.total_token_usage == expected_usage
            {
                return detail;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    panic!(
        "session {session_id} did not persist {expected_rounds} completed rounds with usage {expected_usage:?}"
    );
}

#[tokio::test]
async fn repeated_execution_of_one_session_keeps_both_metric_rounds() {
    let trace = bamboo_infrastructure::telemetry::test_support::TraceTest::default();
    let session_id = "metrics-repeated-execution";
    let (_directory, collector, storage) = create_runner_metrics().await;
    let provider = Arc::new(MetricsQueueProvider {
        queue: Mutex::new(vec![
            vec![
                Ok(provider_usage(10, 2)),
                Ok(LLMChunk::Token("first answer".to_string())),
                Ok(LLMChunk::Done),
            ],
            vec![
                Ok(provider_usage(20, 3)),
                Ok(LLMChunk::Token("second answer".to_string())),
                Ok(LLMChunk::Done),
            ],
        ]),
        requested_messages: Mutex::new(Vec::new()),
    });
    let tools: Arc<dyn bamboo_agent_core::tools::ToolExecutor> =
        Arc::new(bamboo_tools::BuiltinToolExecutor::new());
    let mut session = Session::new(session_id, "test-model");

    let (first_tx, _first_rx) = mpsc::channel(64);
    trace
        .scope(super::run_agent_loop_with_config(
            &mut session,
            "first request".to_string(),
            first_tx,
            provider.clone(),
            tools.clone(),
            CancellationToken::new(),
            runner_metrics_config(&collector),
        ))
        .await
        .expect("first execution");

    let first_detail = wait_for_runner_metrics(
        storage.as_ref(),
        session_id,
        1,
        bamboo_metrics::types::TokenUsage {
            prompt_tokens: 10,
            completion_tokens: 2,
            total_tokens: 12,
        },
    )
    .await;
    let first_round_id = first_detail.rounds[0].round_id.clone();
    assert_eq!(
        first_detail.rounds[0].status,
        bamboo_metrics::types::RoundStatus::Success
    );

    let (second_tx, _second_rx) = mpsc::channel(64);
    trace
        .scope(super::run_agent_loop_with_config(
            &mut session,
            "second request".to_string(),
            second_tx,
            provider,
            tools,
            CancellationToken::new(),
            runner_metrics_config(&collector),
        ))
        .await
        .expect("second execution");

    let detail = wait_for_runner_metrics(
        storage.as_ref(),
        session_id,
        2,
        bamboo_metrics::types::TokenUsage {
            prompt_tokens: 30,
            completion_tokens: 5,
            total_tokens: 35,
        },
    )
    .await;
    let first = detail
        .rounds
        .iter()
        .find(|round| round.round_id == first_round_id)
        .expect("first execution round remains present");
    assert_eq!(
        first.token_usage,
        bamboo_metrics::types::TokenUsage {
            prompt_tokens: 10,
            completion_tokens: 2,
            total_tokens: 12,
        }
    );

    let second = detail
        .rounds
        .iter()
        .find(|round| round.round_id != first_round_id)
        .expect("second execution has a distinct round");
    assert_ne!(first.round_id, second.round_id);
    assert!(first.round_id.starts_with(&format!("{session_id}-run-")));
    assert!(second.round_id.starts_with(&format!("{session_id}-run-")));
    assert_eq!(
        second.token_usage,
        bamboo_metrics::types::TokenUsage {
            prompt_tokens: 20,
            completion_tokens: 3,
            total_tokens: 23,
        }
    );
    assert!(storage
        .prompt_memory_exposure(&first.round_id)
        .await
        .expect("query first execution exposure")
        .is_some());
    assert!(storage
        .prompt_memory_exposure(&second.round_id)
        .await
        .expect("query second execution exposure")
        .is_some());
    assert_eq!(
        session
            .agent_runtime_state
            .as_ref()
            .and_then(|state| state.round.last_round_id.as_deref()),
        Some(second.round_id.as_str())
    );
    let spans = trace.spans();
    let agents: Vec<_> = spans.iter().filter(|s| s.name == "invoke_agent").collect();
    let calls: Vec<_> = spans
        .iter()
        .filter(|s| s.name == "chat test-model")
        .collect();
    assert_eq!(agents.len(), 2);
    assert_eq!(calls.len(), 2);
    assert_ne!(
        agents[0].span_context.trace_id(),
        agents[1].span_context.trace_id()
    );
    for ((agent, call), (input, output)) in agents.iter().zip(&calls).zip([(10, 2), (20, 3)]) {
        assert_eq!(call.parent_span_id, agent.span_context.span_id());
        assert!(call
            .attributes
            .iter()
            .any(|kv| kv.key.as_str() == "gen_ai.usage.input_tokens"
                && kv.value.to_string() == input.to_string()));
        assert!(call
            .attributes
            .iter()
            .any(|kv| kv.key.as_str() == "gen_ai.usage.output_tokens"
                && kv.value.to_string() == output.to_string()));
        assert!(agent
            .attributes
            .iter()
            .any(|kv| kv.key.as_str() == "bamboo.execution.id"));
        assert!(agent
            .attributes
            .iter()
            .any(|kv| kv.key.as_str() == "bamboo.run.id"));
    }
    assert!(!format!("{spans:?}").contains("first request"));
    assert!(!format!("{spans:?}").contains("first answer"));
}

#[tokio::test]
async fn provider_bootstrap_persists_exact_project_ids_and_redacts_global_ids() {
    let session_id = "metrics-prompt-memory-exposure";
    let (_directory, collector, storage) = create_runner_metrics().await;
    let memory_directory = tempfile::tempdir().expect("memory tempdir");
    let memory_store = bamboo_memory::memory_store::MemoryStore::new(memory_directory.path());
    let project_a =
        bamboo_domain::ProjectId::parse("prompt-memory-project-a").expect("valid Project A id");
    let project_b =
        bamboo_domain::ProjectId::parse("prompt-memory-project-b").expect("valid Project B id");
    let project_c = bamboo_domain::ProjectId::parse("prompt-memory-project-c").expect("Project C");
    let project_a_store = memory_store.for_project(&project_a);
    let project_b_store = memory_store.for_project(&project_b);
    let query = "exposuretoken1077";
    let first = project_a_store
        .write_memory(
            bamboo_memory::memory_store::MemoryScope::Project,
            Some(project_a.as_str()),
            bamboo_memory::memory_store::DurableMemoryType::Project,
            "Exposuretoken1077 canonical Project A decision",
            &format!(
                "Project A compact summary. {} FULL_PRIVATE_BODY_A_MUST_NOT_RENDER",
                "a".repeat(300)
            ),
            &[query.to_string()],
            Some(session_id),
            "test-model",
            false,
            Some(bamboo_memory::memory_store::TemporalGranularity::Year),
        )
        .await
        .expect("write Project A memory");
    let mut project_a_ids = vec![first.frontmatter.id];
    for (index, granularity) in [
        bamboo_memory::memory_store::TemporalGranularity::Day,
        bamboo_memory::memory_store::TemporalGranularity::Quarter,
        bamboo_memory::memory_store::TemporalGranularity::Week,
    ]
    .into_iter()
    .enumerate()
    {
        let written = project_a_store
            .write_memory(
                bamboo_memory::memory_store::MemoryScope::Project,
                Some(project_a.as_str()),
                bamboo_memory::memory_store::DurableMemoryType::Project,
                &format!("Exposuretoken1077 Project A decision {}", index + 2),
                &format!(
                    "Project A compact summary {}. {} FULL_PRIVATE_BODY_A_MUST_NOT_RENDER",
                    index + 2,
                    "a".repeat(300)
                ),
                &[query.to_string()],
                Some(session_id),
                "test-model",
                false,
                Some(granularity),
            )
            .await
            .expect("write another Project A memory");
        project_a_ids.push(written.frontmatter.id);
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    let excluded = project_b_store
        .write_memory(
            bamboo_memory::memory_store::MemoryScope::Project,
            Some(project_b.as_str()),
            bamboo_memory::memory_store::DurableMemoryType::Project,
            "Exposuretoken1077 other Project decision",
            &format!(
                "Project B compact summary. {} FULL_PRIVATE_BODY_B_MUST_NOT_RENDER",
                "b".repeat(300)
            ),
            &[query.to_string()],
            Some("other-session"),
            "test-model",
            false,
            Some(bamboo_memory::memory_store::TemporalGranularity::Day),
        )
        .await
        .expect("write Project B memory");
    let global = memory_store
        .write_memory(
            bamboo_memory::memory_store::MemoryScope::Global,
            None,
            bamboo_memory::memory_store::DurableMemoryType::Reference,
            "Exposuretoken1077 global fallback",
            &format!(
                "Global compact summary. {} FULL_PRIVATE_GLOBAL_BODY_MUST_NOT_RENDER",
                "g".repeat(300)
            ),
            &[query.to_string()],
            Some("global-session"),
            "test-model",
            false,
            None,
        )
        .await
        .expect("write Global fallback memory");

    let provider = Arc::new(MetricsQueueProvider {
        queue: Mutex::new(vec![
            vec![
                Ok(provider_usage(17, 3)),
                Ok(LLMChunk::Token("observed answer".to_string())),
                Ok(LLMChunk::Done),
            ],
            vec![
                Ok(provider_usage(5, 1)),
                Ok(LLMChunk::Token("global answer".to_string())),
                Ok(LLMChunk::Done),
            ],
        ]),
        requested_messages: Mutex::new(Vec::new()),
    });
    let tools: Arc<dyn bamboo_agent_core::tools::ToolExecutor> =
        Arc::new(bamboo_tools::BuiltinToolExecutor::new());
    let mut session = Session::new(session_id, "test-model");
    session.set_project_id_meta(project_a.to_string());
    session.metadata.insert(
        "runtime_prompt_memory_observability".to_string(),
        serde_json::json!({"relevant_memory_ids":["forged-metadata-id"]}).to_string(),
    );
    session.metadata.insert(
        "external_memory_rendered".to_string(),
        "If selected: `memory` action=get id=forged-rendered-id".to_string(),
    );
    let mut config = runner_metrics_config(&collector);
    config.memory_store = memory_store.clone();
    config.prompt_memory_flags.relevant_recall = true;

    let (event_tx, _event_rx) = mpsc::channel(64);
    super::run_agent_loop_with_config(
        &mut session,
        format!("recall {query}"),
        event_tx,
        provider.clone(),
        tools,
        CancellationToken::new(),
        config,
    )
    .await
    .expect("provider-backed execution");

    let detail = wait_for_runner_metrics(
        storage.as_ref(),
        session_id,
        1,
        bamboo_metrics::types::TokenUsage {
            prompt_tokens: 17,
            completion_tokens: 3,
            total_tokens: 20,
        },
    )
    .await;
    let observation = storage
        .prompt_memory_exposure(&detail.rounds[0].round_id)
        .await
        .expect("query prompt-memory observation")
        .expect("successful bootstrap persists an observation");
    assert_eq!(observation.project_id.as_deref(), Some(project_a.as_str()));
    assert_eq!(
        observation.recall_outcome,
        bamboo_metrics::types::PromptMemoryRecallOutcome::Lexical
    );
    assert_eq!(observation.all_compact_exposed_count, 3);
    assert_eq!(observation.project_exposed_count, 3);
    assert!(!observation.out_of_project_only);
    assert_eq!(observation.project_items.len(), 3);

    let requests = provider.requested_messages.lock().await;
    assert_eq!(requests.len(), 1);
    let provider_text = requests[0]
        .iter()
        .map(|message| message.content.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    let mut provider_ids = project_a_ids
        .iter()
        .filter(|id| provider_text.contains(id.as_str()))
        .map(String::as_str)
        .collect::<Vec<_>>();
    provider_ids.sort_by_key(|id| provider_text.find(*id).expect("provider-visible id"));
    assert_eq!(
        provider_ids.len(),
        3,
        "one of four candidates stays outside top-k"
    );
    assert_eq!(
        observation
            .project_items
            .iter()
            .map(|item| item.memory_id.as_str())
            .collect::<Vec<_>>(),
        provider_ids,
        "persisted IDs and ranks must match final provider-visible order"
    );
    assert!(observation
        .project_items
        .iter()
        .enumerate()
        .all(|(index, item)| item.rank == index as u32 + 1));
    assert!(!provider_text.contains(&excluded.frontmatter.id));
    assert!(!provider_text.contains("forged-metadata-id"));
    assert!(!provider_text.contains("forged-rendered-id"));
    assert!(!provider_text.contains("FULL_PRIVATE_BODY_A_MUST_NOT_RENDER"));
    assert!(!provider_text.contains("FULL_PRIVATE_BODY_B_MUST_NOT_RENDER"));
    assert!(!provider_text.contains(&global.frontmatter.id));
    drop(requests);

    let global_session_id = "metrics-prompt-memory-global-fallback";
    let mut global_session = Session::new(global_session_id, "test-model");
    global_session.set_project_id_meta(project_c.to_string());
    let mut global_config = runner_metrics_config(&collector);
    global_config.memory_store = memory_store;
    global_config.prompt_memory_flags.relevant_recall = true;
    let (global_tx, _global_rx) = mpsc::channel(64);
    super::run_agent_loop_with_config(
        &mut global_session,
        format!("recall {query}"),
        global_tx,
        provider.clone(),
        Arc::new(bamboo_tools::BuiltinToolExecutor::new()),
        CancellationToken::new(),
        global_config,
    )
    .await
    .expect("Global-fallback provider execution");
    let global_detail = wait_for_runner_metrics(
        storage.as_ref(),
        global_session_id,
        1,
        bamboo_metrics::types::TokenUsage {
            prompt_tokens: 5,
            completion_tokens: 1,
            total_tokens: 6,
        },
    )
    .await;
    let global_observation = storage
        .prompt_memory_exposure(&global_detail.rounds[0].round_id)
        .await
        .expect("query Global-only observation")
        .expect("Global-only provider bootstrap persists a header");
    assert_eq!(
        global_observation.project_id.as_deref(),
        Some(project_c.as_str())
    );
    assert_eq!(global_observation.all_compact_exposed_count, 1);
    assert_eq!(global_observation.project_exposed_count, 0);
    assert!(global_observation.out_of_project_only);
    assert!(global_observation.project_items.is_empty());
    let requests = provider.requested_messages.lock().await;
    let global_text = requests[1]
        .iter()
        .map(|message| message.content.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(global_text.contains(&global.frontmatter.id));
    assert!(!global_text.contains("FULL_PRIVATE_GLOBAL_BODY_MUST_NOT_RENDER"));
}

struct PauseForHumanTool;

#[async_trait]
impl Tool for PauseForHumanTool {
    fn name(&self) -> &str {
        "pause_for_human"
    }

    fn description(&self) -> &str {
        "pause this execution until a human responds"
    }

    fn parameters_schema(&self) -> serde_json::Value {
        serde_json::json!({
            "type": "object",
            "properties": {},
            "additionalProperties": false
        })
    }

    async fn invoke(
        &self,
        _args: serde_json::Value,
        ctx: ToolCtx,
    ) -> Result<ToolOutcome, ToolError> {
        Ok(ToolOutcome::NeedsHuman {
            question: bamboo_agent_core::PendingQuestion {
                tool_call_id: ctx.tool_call_id.to_string(),
                tool_name: self.name().to_string(),
                question: "Continue?".to_string(),
                options: vec!["yes".to_string(), "no".to_string()],
                allow_custom: false,
                source: bamboo_agent_core::PendingQuestionSource::PauseTool,
            },
            result: ToolResult::text(false, "waiting for a response"),
        })
    }
}

struct ResumeCompleteTool;

#[async_trait]
impl Tool for ResumeCompleteTool {
    fn name(&self) -> &str {
        "complete_after_resume"
    }

    fn description(&self) -> &str {
        "complete one tool call after the suspended execution resumes"
    }

    fn parameters_schema(&self) -> serde_json::Value {
        serde_json::json!({
            "type": "object",
            "properties": {},
            "additionalProperties": false
        })
    }

    async fn invoke(
        &self,
        _args: serde_json::Value,
        _ctx: ToolCtx,
    ) -> Result<ToolOutcome, ToolError> {
        Ok(ToolOutcome::Completed(ToolResult::text(
            true,
            "resume tool completed",
        )))
    }
}

#[tokio::test]
async fn suspended_session_resume_uses_a_new_metric_round_and_preserves_tool_link() {
    let session_id = "metrics-suspend-resume";
    let (_directory, collector, storage) = create_runner_metrics().await;
    let pause_call = bamboo_agent_core::tools::ToolCall {
        id: "pause-call".to_string(),
        tool_type: "function".to_string(),
        function: FunctionCall {
            name: "pause_for_human".to_string(),
            arguments: "{}".to_string(),
        },
    };
    let resume_call = bamboo_agent_core::tools::ToolCall {
        id: "resume-call".to_string(),
        tool_type: "function".to_string(),
        function: FunctionCall {
            name: "complete_after_resume".to_string(),
            arguments: "{}".to_string(),
        },
    };
    let provider = Arc::new(MetricsQueueProvider {
        queue: Mutex::new(vec![
            vec![
                Ok(provider_usage(7, 2)),
                Ok(LLMChunk::ToolCalls(vec![pause_call])),
                Ok(LLMChunk::Done),
            ],
            vec![
                Ok(provider_usage(13, 4)),
                Ok(LLMChunk::ToolCalls(vec![resume_call])),
                Ok(LLMChunk::Done),
            ],
            vec![
                Ok(provider_usage(5, 1)),
                Ok(LLMChunk::Token("resumed answer".to_string())),
                Ok(LLMChunk::Done),
            ],
        ]),
        requested_messages: Mutex::new(Vec::new()),
    });
    let tools = BuiltinToolExecutorBuilder::new()
        .with_tool(PauseForHumanTool)
        .expect("register pause tool")
        .with_tool(ResumeCompleteTool)
        .expect("register resume tool")
        .build();
    let tools: Arc<dyn bamboo_agent_core::tools::ToolExecutor> = Arc::new(tools);
    let mut session = Session::new(session_id, "test-model");

    let (first_tx, _first_rx) = mpsc::channel(64);
    super::run_agent_loop_with_config(
        &mut session,
        "need a decision".to_string(),
        first_tx,
        provider.clone(),
        tools.clone(),
        CancellationToken::new(),
        runner_metrics_config(&collector),
    )
    .await
    .expect("suspending execution");

    assert!(session.has_pending_question());
    assert_eq!(
        session
            .metadata
            .get("runtime.suspend_reason")
            .map(String::as_str),
        Some("awaiting_clarification")
    );
    assert!(session.agent_runtime_state.as_ref().is_some_and(|state| {
        matches!(state.status, bamboo_domain::AgentStatusState::Suspended)
    }));

    let first_detail = wait_for_runner_metrics(
        storage.as_ref(),
        session_id,
        1,
        bamboo_metrics::types::TokenUsage {
            prompt_tokens: 7,
            completion_tokens: 2,
            total_tokens: 9,
        },
    )
    .await;
    let first_round_id = first_detail.rounds[0].round_id.clone();
    assert_eq!(first_detail.rounds[0].tool_calls.len(), 1);
    assert_eq!(
        first_detail.rounds[0].tool_calls[0].tool_call_id,
        "pause-call"
    );

    // Mirror the response boundary: the resolved question and suspend marker
    // are cleared, while the persisted Suspended control-plane state lets the
    // next invocation take the real resume path.
    session.clear_pending_question();
    session.metadata.remove("runtime.suspend_reason");

    let (second_tx, _second_rx) = mpsc::channel(64);
    super::run_agent_loop_with_config(
        &mut session,
        "yes".to_string(),
        second_tx,
        provider,
        tools,
        CancellationToken::new(),
        runner_metrics_config(&collector),
    )
    .await
    .expect("resumed execution");

    let detail = wait_for_runner_metrics(
        storage.as_ref(),
        session_id,
        3,
        bamboo_metrics::types::TokenUsage {
            prompt_tokens: 25,
            completion_tokens: 7,
            total_tokens: 32,
        },
    )
    .await;
    let first = detail
        .rounds
        .iter()
        .find(|round| round.round_id == first_round_id)
        .expect("suspended round remains present");
    assert_eq!(first.tool_calls.len(), 1);
    assert_eq!(first.tool_calls[0].tool_call_id, "pause-call");
    assert_eq!(
        first.token_usage,
        bamboo_metrics::types::TokenUsage {
            prompt_tokens: 7,
            completion_tokens: 2,
            total_tokens: 9,
        }
    );

    let resumed_tool_round = detail
        .rounds
        .iter()
        .find(|round| {
            round
                .tool_calls
                .iter()
                .any(|tool| tool.tool_call_id == "resume-call")
        })
        .expect("resume tool remains linked to its new round");
    assert_ne!(first.round_id, resumed_tool_round.round_id);
    assert_eq!(
        resumed_tool_round.token_usage,
        bamboo_metrics::types::TokenUsage {
            prompt_tokens: 13,
            completion_tokens: 4,
            total_tokens: 17,
        }
    );
    assert_eq!(resumed_tool_round.tool_calls.len(), 1);
    assert_eq!(resumed_tool_round.tool_calls[0].tool_call_id, "resume-call");

    let final_round_id = session
        .agent_runtime_state
        .as_ref()
        .and_then(|state| state.round.last_round_id.as_deref())
        .expect("resumed final round id");
    let final_round = detail
        .rounds
        .iter()
        .find(|round| round.round_id == final_round_id)
        .expect("resumed final round remains present");
    assert_eq!(
        final_round.token_usage,
        bamboo_metrics::types::TokenUsage {
            prompt_tokens: 5,
            completion_tokens: 1,
            total_tokens: 6,
        }
    );
    assert!(final_round.tool_calls.is_empty());

    assert_eq!(
        metric_execution_prefix(&resumed_tool_round.round_id),
        metric_execution_prefix(&final_round.round_id),
        "all rounds in one resumed execution share the namespace"
    );
    assert_ne!(
        metric_execution_prefix(&first.round_id),
        metric_execution_prefix(&resumed_tool_round.round_id),
        "the resume receives a fresh execution namespace"
    );
    assert_eq!(
        session
            .agent_runtime_state
            .as_ref()
            .and_then(|state| state.round.last_round_id.as_deref()),
        Some(final_round.round_id.as_str())
    );
}

#[tokio::test]
async fn synthetic_provider_usage_reaches_read_only_hook_and_same_local_span() {
    use bamboo_agent_core::AgentHook;
    use bamboo_domain::{AgentHookPoint, HookPayload, HookResult, ModelCallStatus};
    struct Capture {
        sender: mpsc::UnboundedSender<bamboo_domain::ModelCallUsage>,
        birth: chrono::DateTime<Utc>,
    }
    #[async_trait]
    impl AgentHook for Capture {
        fn point(&self) -> AgentHookPoint {
            AgentHookPoint::AfterModelCallUsage
        }
        async fn run(
            &self,
            _: AgentHookPoint,
            payload: &HookPayload,
            session: &Session,
        ) -> HookResult {
            assert!(session.messages.is_empty());
            assert!(session.workspace.is_none());
            assert_eq!(session.created_at, self.birth);
            assert_eq!(session.root_session_id, "synthetic-usage-hook");
            if let HookPayload::ModelCallUsage { observation } = payload {
                self.sender.send(observation.clone()).unwrap();
            }
            HookResult::Abort {
                reason: "observer must not affect the call".into(),
            }
        }
    }
    let trace = bamboo_infrastructure::telemetry::test_support::LocalTraceTest::isolated();
    let mut session = Session::new("synthetic-usage-hook", "synthetic-model");
    let birth = session.created_at;
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let mut hooks = crate::runtime::hooks::HookRunner::new();
    hooks.register(Arc::new(Capture {
        sender: observed_tx,
        birth,
    }));
    let config = AgentLoopConfig {
        hook_runner: Arc::new(hooks),
        model_name: Some("synthetic-model".into()),
        system_prompt: Some("synthetic system prompt".into()),
        ..Default::default()
    };
    let provider = Arc::new(MetricsQueueProvider {
        queue: Mutex::new(vec![vec![
            Ok(LLMChunk::ProviderUsage {
                input_tokens: Some(100),
                output_tokens: Some(20),
                total_tokens: Some(120),
                reasoning_tokens: Some(3),
                cache_creation_input_tokens: None,
                cache_read_input_tokens: Some(0),
                cache_write_input_tokens: Some(12),
            }),
            Ok(LLMChunk::Token("synthetic answer".into())),
            Ok(LLMChunk::Done),
        ]]),
        requested_messages: Mutex::new(Vec::new()),
    });
    let tools: Arc<dyn bamboo_agent_core::tools::ToolExecutor> =
        Arc::new(bamboo_tools::BuiltinToolExecutor::new());
    let (events, _receiver) = mpsc::channel(128);
    trace
        .scope(super::run_agent_loop_with_config(
            &mut session,
            "PRIVATE_PROMPT_SENTINEL".into(),
            events,
            provider,
            tools,
            CancellationToken::new(),
            config,
        ))
        .await
        .expect("observer abort cannot reject the model call");
    let event = tokio::time::timeout(std::time::Duration::from_secs(2), observed_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(event.status, ModelCallStatus::Success);
    let usage = event.usage.unwrap();
    assert_eq!(usage.cache_read_input_tokens, Some(0));
    assert_eq!(usage.cache_creation_input_tokens, None);
    assert_eq!(usage.cache_write_input_tokens, Some(12));
    assert_eq!(usage.reasoning_tokens, Some(3));
    let page = trace.store.page(&session.id, birth, 10, None);
    let trace_id = event.trace_id.unwrap();
    assert_eq!(page.traces.len(), 1);
    assert_eq!(page.traces[0].trace_id, trace_id);
    let detail = trace.store.detail(&session.id, birth, &trace_id).unwrap();
    let span = detail
        .spans
        .iter()
        .find(|span| span.span_id == event.span_id.as_deref().unwrap())
        .unwrap();
    assert_eq!(span.call_id.as_deref(), Some(event.call_id.as_str()));
    assert_eq!(span.usage.as_ref(), Some(&usage));
    assert!(!serde_json::to_string(&detail)
        .unwrap()
        .contains("PRIVATE_PROMPT_SENTINEL"));
}

#[tokio::test]
async fn unsafe_session_id_keeps_usage_hook_and_local_span_correlated() {
    use bamboo_agent_core::AgentHook;
    use bamboo_domain::{AgentHookPoint, HookPayload, HookResult, ModelCallStatus};

    struct Capture(mpsc::UnboundedSender<bamboo_domain::ModelCallUsage>);
    #[async_trait]
    impl AgentHook for Capture {
        fn point(&self) -> AgentHookPoint {
            AgentHookPoint::AfterModelCallUsage
        }
        async fn run(
            &self,
            _: AgentHookPoint,
            payload: &HookPayload,
            session: &Session,
        ) -> HookResult {
            if let HookPayload::ModelCallUsage { observation } = payload {
                assert_eq!(session.id, observation.session_id);
                self.0.send(observation.clone()).unwrap();
            }
            HookResult::Continue
        }
    }

    let trace = bamboo_infrastructure::telemetry::test_support::LocalTraceTest::isolated();
    let mut session = Session::new("用户 会话", "synthetic-model");
    let birth = session.created_at;
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let mut hooks = crate::runtime::hooks::HookRunner::new();
    hooks.register(Arc::new(Capture(observed_tx)));
    let config = AgentLoopConfig {
        hook_runner: Arc::new(hooks),
        model_name: Some("synthetic-model".into()),
        system_prompt: Some("synthetic system".into()),
        ..Default::default()
    };
    let provider = Arc::new(MetricsQueueProvider {
        queue: Mutex::new(vec![vec![
            Ok(LLMChunk::ProviderUsage {
                input_tokens: Some(100),
                output_tokens: Some(7),
                total_tokens: Some(107),
                reasoning_tokens: None,
                cache_creation_input_tokens: None,
                cache_read_input_tokens: Some(0),
                cache_write_input_tokens: None,
            }),
            Ok(LLMChunk::Token("synthetic answer".into())),
            Ok(LLMChunk::Done),
        ]]),
        requested_messages: Mutex::new(Vec::new()),
    });
    let tools: Arc<dyn bamboo_agent_core::tools::ToolExecutor> =
        Arc::new(bamboo_tools::BuiltinToolExecutor::new());
    let (events, _receiver) = mpsc::channel(128);
    trace
        .scope(super::run_agent_loop_with_config(
            &mut session,
            "synthetic prompt".into(),
            events,
            provider,
            tools,
            CancellationToken::new(),
            config,
        ))
        .await
        .unwrap();
    let event = tokio::time::timeout(std::time::Duration::from_secs(2), observed_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(event.status, ModelCallStatus::Success);
    assert_eq!(
        event.session_id,
        bamboo_infrastructure::telemetry::session_reference(&session.id).unwrap()
    );
    assert!(event.session_id.starts_with('~'));
    let detail = trace
        .store
        .detail(&session.id, birth, event.trace_id.as_deref().unwrap())
        .unwrap();
    let span = detail
        .spans
        .iter()
        .find(|span| span.span_id == event.span_id.as_deref().unwrap())
        .unwrap();
    assert_eq!(span.call_id.as_deref(), Some(event.call_id.as_str()));
    // This synthetic ordinary run uses the raw session ID as its run ID.
    // It is omitted because it does not meet the telemetry identifier policy.
    assert!(event.run_id.is_none());
    assert_eq!(span.run_id, event.run_id);
    assert_eq!(span.usage, event.usage);
    assert!(!serde_json::to_string(&event).unwrap().contains(&session.id));
    assert!(!serde_json::to_string(&detail)
        .unwrap()
        .contains(&session.id));
}

#[tokio::test]
async fn partial_provider_failure_reports_one_usage_event_and_matching_error_span() {
    use bamboo_agent_core::AgentHook;
    use bamboo_domain::{AgentHookPoint, HookPayload, HookResult, ModelCallStatus};

    struct Capture(mpsc::UnboundedSender<bamboo_domain::ModelCallUsage>);
    #[async_trait]
    impl AgentHook for Capture {
        fn point(&self) -> AgentHookPoint {
            AgentHookPoint::AfterModelCallUsage
        }
        async fn run(&self, _: AgentHookPoint, payload: &HookPayload, _: &Session) -> HookResult {
            if let HookPayload::ModelCallUsage { observation } = payload {
                self.0.send(observation.clone()).unwrap();
            }
            HookResult::Continue
        }
    }

    let trace = bamboo_infrastructure::telemetry::test_support::LocalTraceTest::isolated();
    let mut session = Session::new("synthetic-partial-usage", "synthetic-model");
    let birth = session.created_at;
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    // Keep the channel open after the dispatched observer finishes so that a
    // timeout proves no duplicate event arrived, rather than channel closure.
    let observer_sender_guard = observed_tx.clone();
    let mut hooks = crate::runtime::hooks::HookRunner::new();
    hooks.register(Arc::new(Capture(observed_tx)));
    let config = AgentLoopConfig {
        hook_runner: Arc::new(hooks),
        model_name: Some("synthetic-model".into()),
        system_prompt: Some("synthetic system".into()),
        ..Default::default()
    };
    let usage = LLMChunk::ProviderUsage {
        input_tokens: Some(100),
        output_tokens: Some(7),
        total_tokens: Some(107),
        reasoning_tokens: Some(2),
        cache_creation_input_tokens: None,
        cache_read_input_tokens: Some(0),
        cache_write_input_tokens: Some(12),
    };
    let provider = Arc::new(MetricsQueueProvider {
        queue: Mutex::new(vec![vec![
            Ok(LLMChunk::Token("synthetic partial answer".into())),
            Ok(usage.clone()),
            Ok(usage),
            Err(bamboo_llm::provider::LLMError::Stream(
                "synthetic interruption".into(),
            )),
        ]]),
        requested_messages: Mutex::new(Vec::new()),
    });
    let tools: Arc<dyn bamboo_agent_core::tools::ToolExecutor> =
        Arc::new(bamboo_tools::BuiltinToolExecutor::new());
    let (events, _receiver) = mpsc::channel(128);
    let result = trace
        .scope(super::run_agent_loop_with_config(
            &mut session,
            "PRIVATE_PROMPT_SENTINEL".into(),
            events,
            provider,
            tools,
            CancellationToken::new(),
            config,
        ))
        .await;
    assert!(result.is_err(), "partial stream failure must be surfaced");
    let event = tokio::time::timeout(std::time::Duration::from_secs(2), observed_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(event.status, ModelCallStatus::Error);
    let usage = event.usage.expect("observed partial provider usage");
    assert_eq!(usage.input_tokens, Some(100));
    assert_eq!(usage.output_tokens, Some(7));
    assert_eq!(usage.reasoning_tokens, Some(2));
    assert_eq!(usage.cache_read_input_tokens, Some(0));
    let detail = trace
        .store
        .detail(&session.id, birth, &event.trace_id.unwrap())
        .unwrap();
    let span = detail
        .spans
        .iter()
        .find(|span| span.span_id == event.span_id.as_deref().unwrap())
        .unwrap();
    assert_eq!(span.call_id.as_deref(), Some(event.call_id.as_str()));
    assert_eq!(span.status, "error");
    assert_eq!(span.usage.as_ref(), Some(&usage));
    assert!(!serde_json::to_string(&detail)
        .unwrap()
        .contains("PRIVATE_PROMPT_SENTINEL"));
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), observed_rx.recv())
            .await
            .is_err()
    );
    drop(observer_sender_guard);
}

#[tokio::test]
async fn anthropic_sse_error_preserves_reported_partial_usage() {
    use bamboo_agent_core::AgentHook;
    use bamboo_domain::{AgentHookPoint, HookPayload, HookResult, ModelCallStatus};
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    struct Capture(mpsc::UnboundedSender<bamboo_domain::ModelCallUsage>);
    #[async_trait]
    impl AgentHook for Capture {
        fn point(&self) -> AgentHookPoint {
            AgentHookPoint::AfterModelCallUsage
        }
        async fn run(&self, _: AgentHookPoint, payload: &HookPayload, _: &Session) -> HookResult {
            if let HookPayload::ModelCallUsage { observation } = payload {
                self.0.send(observation.clone()).unwrap();
            }
            HookResult::Continue
        }
    }

    for (start, delta, expected_input, expected_creation, expected_read) in [
        (
            r#"{"message":{"usage":{"input_tokens":10,"cache_creation_input_tokens":0,"cache_read_input_tokens":3}}}"#,
            r#"{"usage":{"output_tokens":7}}"#,
            Some(13),
            Some(0),
            Some(3),
        ),
        (
            r#"{"message":{"usage":{"input_tokens":10,"cache_creation_input_tokens":2,"cache_read_input_tokens":3}}}"#,
            r#"{"usage":{"output_tokens":7,"cache_read_input_tokens":-1}}"#,
            None,
            Some(2),
            None,
        ),
    ] {
        let body = format!(
            "event: message_start\ndata: {start}\n\n\
             event: content_block_start\ndata: {{\"index\":0,\"content_block\":{{\"type\":\"text\",\"text\":\"\"}}}}\n\n\
             event: content_block_delta\ndata: {{\"index\":0,\"delta\":{{\"type\":\"text_delta\",\"text\":\"synthetic partial\"}}}}\n\n\
             event: message_delta\ndata: {delta}\n\n\
             event: message_delta\ndata: {delta}\n\n\
             event: error\ndata: {{\"type\":\"error\",\"error\":{{\"type\":\"synthetic_error\"}}}}\n\n"
        );
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "text/event-stream")
                    .set_body_string(body),
            )
            .mount(&server)
            .await;
        let provider: Arc<dyn LLMProvider> = Arc::new(
            bamboo_llm::providers::anthropic::AnthropicProvider::new("synthetic-key")
                .with_base_url(server.uri()),
        );
        let trace = bamboo_infrastructure::telemetry::test_support::LocalTraceTest::isolated();
        let mut session = Session::new("synthetic-anthropic-interruption", "synthetic-model");
        let birth = session.created_at;
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let sender_guard = sender.clone();
        let mut hooks = crate::runtime::hooks::HookRunner::new();
        hooks.register(Arc::new(Capture(sender)));
        let config = AgentLoopConfig {
            hook_runner: Arc::new(hooks),
            provider_type: Some("anthropic".into()),
            model_name: Some("synthetic-model".into()),
            system_prompt: Some("synthetic system".into()),
            ..Default::default()
        };
        let tools: Arc<dyn bamboo_agent_core::tools::ToolExecutor> =
            Arc::new(bamboo_tools::BuiltinToolExecutor::new());
        let (events, _receiver) = mpsc::channel(128);
        let result = trace
            .scope(super::run_agent_loop_with_config(
                &mut session,
                "PRIVATE_PROMPT_SENTINEL".into(),
                events,
                provider,
                tools,
                CancellationToken::new(),
                config,
            ))
            .await;
        assert!(matches!(
            result,
            Err(bamboo_agent_core::AgentError::LLMNonRetryable(message))
                if message.contains("synthetic_error")
        ));
        let event = tokio::time::timeout(std::time::Duration::from_secs(2), receiver.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(event.status, ModelCallStatus::Error);
        let usage = event.usage.as_ref().expect("reported partial usage");
        assert_eq!(usage.input_tokens, expected_input);
        assert_eq!(usage.output_tokens, Some(7));
        assert_eq!(usage.cache_creation_input_tokens, expected_creation);
        assert_eq!(usage.cache_read_input_tokens, expected_read);
        let detail = trace
            .store
            .detail(&session.id, birth, event.trace_id.as_deref().unwrap())
            .unwrap();
        let span = detail
            .spans
            .iter()
            .find(|span| span.span_id == event.span_id.as_deref().unwrap())
            .unwrap();
        assert_eq!(span.status, "error");
        assert_eq!(span.usage.as_ref(), Some(usage));
        assert!(!serde_json::to_string(&detail)
            .unwrap()
            .contains("PRIVATE_PROMPT_SENTINEL"));
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), receiver.recv())
                .await
                .is_err()
        );
        drop(sender_guard);
    }
}

#[tokio::test]
async fn synthetic_anthropic_and_gemini_streams_reach_usage_hook_and_local_span() {
    use bamboo_agent_core::AgentHook;
    use bamboo_domain::{AgentHookPoint, HookPayload, HookResult};
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    struct Capture(mpsc::UnboundedSender<bamboo_domain::ModelCallUsage>);
    #[async_trait]
    impl AgentHook for Capture {
        fn point(&self) -> AgentHookPoint {
            AgentHookPoint::AfterModelCallUsage
        }
        async fn run(&self, _: AgentHookPoint, payload: &HookPayload, _: &Session) -> HookResult {
            if let HookPayload::ModelCallUsage { observation } = payload {
                self.0.send(observation.clone()).unwrap();
            }
            HookResult::Continue
        }
    }

    let anthropic = concat!(
        "event: message_start\ndata: {\"message\":{\"usage\":{\"input_tokens\":10,\"cache_creation_input_tokens\":2,\"cache_read_input_tokens\":3}}}\n\n",
        "event: content_block_start\ndata: {\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
        "event: content_block_delta\ndata: {\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"synthetic answer\"}}\n\n",
        "event: content_block_stop\ndata: {\"index\":0}\n\n",
        "event: message_delta\ndata: {\"usage\":{\"output_tokens\":7}}\n\n",
        "event: message_stop\ndata: {}\n\n",
    );
    let gemini = "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"synthetic answer\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":100,\"cachedContentTokenCount\":25,\"candidatesTokenCount\":20,\"thoughtsTokenCount\":3,\"totalTokenCount\":123}}\n\n";
    for (
        family,
        body,
        expected_input,
        expected_output,
        expected_read,
        expected_creation,
        expected_reasoning,
    ) in [
        ("anthropic", anthropic, 15, 7, 3, Some(2), None),
        ("gemini", gemini, 100, 23, 25, None, Some(3)),
    ] {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "text/event-stream")
                    .set_body_string(body),
            )
            .mount(&server)
            .await;
        let provider: Arc<dyn LLMProvider> = match family {
            "anthropic" => Arc::new(
                bamboo_llm::providers::anthropic::AnthropicProvider::new("synthetic-key")
                    .with_base_url(server.uri()),
            ),
            _ => Arc::new(
                bamboo_llm::providers::gemini::GeminiProvider::new("synthetic-key")
                    .with_base_url(server.uri()),
            ),
        };
        let trace = bamboo_infrastructure::telemetry::test_support::LocalTraceTest::isolated();
        let mut session = Session::new(&format!("synthetic-{family}"), "synthetic-model");
        let birth = session.created_at;
        let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
        let mut hooks = crate::runtime::hooks::HookRunner::new();
        hooks.register(Arc::new(Capture(observed_tx)));
        let config = AgentLoopConfig {
            hook_runner: Arc::new(hooks),
            provider_type: Some(family.into()),
            model_name: Some("synthetic-model".into()),
            system_prompt: Some("synthetic system".into()),
            ..Default::default()
        };
        let tools: Arc<dyn bamboo_agent_core::tools::ToolExecutor> =
            Arc::new(bamboo_tools::BuiltinToolExecutor::new());
        let (events, _receiver) = mpsc::channel(128);
        trace
            .scope(super::run_agent_loop_with_config(
                &mut session,
                "PRIVATE_PROMPT_SENTINEL".into(),
                events,
                provider,
                tools,
                CancellationToken::new(),
                config,
            ))
            .await
            .expect("synthetic provider call");
        let event = tokio::time::timeout(std::time::Duration::from_secs(2), observed_rx.recv())
            .await
            .unwrap()
            .unwrap();
        let usage = event.usage.expect("provider snapshot");
        assert_eq!(usage.input_tokens, Some(expected_input));
        assert_eq!(usage.output_tokens, Some(expected_output));
        assert_eq!(usage.cache_read_input_tokens, Some(expected_read));
        assert_eq!(usage.cache_creation_input_tokens, expected_creation);
        assert_eq!(usage.reasoning_tokens, expected_reasoning);
        let detail = trace
            .store
            .detail(&session.id, birth, &event.trace_id.unwrap())
            .unwrap();
        let span = detail
            .spans
            .iter()
            .find(|span| span.call_id.as_deref() == Some(event.call_id.as_str()))
            .unwrap();
        assert_eq!(span.usage.as_ref(), Some(&usage));
        assert!(!serde_json::to_string(&detail)
            .unwrap()
            .contains("PRIVATE_PROMPT_SENTINEL"));
        assert!(observed_rx.try_recv().is_err(), "one event per model call");
    }
}
