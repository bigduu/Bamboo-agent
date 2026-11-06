//! The real Native Ticket path, with a shortened default watchdog observation.
//! No production timeout override or simulated admission/stop receipt is used.
use super::*;
use bamboo_agent_core::AgentEvent;
use std::{collections::HashMap, time::Duration};
use tokio::sync::broadcast;

struct ReasoningThenResult(AtomicUsize);

#[async_trait]
impl LLMProvider for ReasoningThenResult {
    async fn chat_stream(
        &self,
        _: &[Message],
        _: &[ToolSchema],
        _: Option<u32>,
        _: &str,
    ) -> std::result::Result<LLMStream, LLMError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(Box::pin(futures::stream::unfold(0, |step| async move {
            let chunk = match step {
                0..=19 => {
                    tokio::time::sleep(Duration::from_millis(150)).await;
                    LLMChunk::ReasoningToken("Still verifying the assigned Work.".into())
                }
                20 => LLMChunk::Token(OUTPUT.into()),
                21 => LLMChunk::Done,
                _ => return None,
            };
            Some((Ok(chunk), step + 1))
        })))
    }
}

async fn native_work_runtime(
    f: &Fixture,
    provider: Arc<dyn LLMProvider>,
) -> (
    Arc<dyn ExternalChildRunner>,
    crate::execution::SpawnContext,
    broadcast::Receiver<AgentEvent>,
) {
    let config = Config::default();
    let tools: Arc<dyn ToolExecutor> = Arc::new(
        bamboo_tools::BuiltinToolExecutor::new()
            .with_native_tool_ceiling(vec!["Task".into()])
            .unwrap(),
    );
    let agent = Arc::new(
        Agent::builder()
            .storage(f.storage.clone())
            .persistence(f.runtime.persistence.clone())
            .session_inbox(f.runtime.inbox.clone())
            .activation_router(f.runtime.router.clone())
            .attachment_reader(f.storage.clone())
            .skill_manager(Arc::new(bamboo_skills::SkillManager::new()))
            .metrics_collector(bamboo_metrics::MetricsCollector::spawn(
                Arc::new(bamboo_metrics::SqliteMetricsStorage::new(
                    f.dir.path().join("metrics.db"),
                )),
                7,
            ))
            .config(Arc::new(RwLock::new(config.clone())))
            .provider(provider)
            .default_tools(tools.clone())
            .build()
            .unwrap(),
    );
    let runner =
        crate::external_agents::runtime::build_external_child_runner_with_native_tool_ceiling(
            &config,
            Arc::new(RwLock::new(config.clone())),
            None,
            None,
            None,
            None,
            Arc::new(TaskCeiling),
        );
    assert!(
        runner.supports_native_ticket_budget(),
        "production native-only Composite route"
    );
    runner.set_actor_directory_store(Some(f.storage.clone()));
    runner.set_session_inbox_runtime(Some(f.runtime.clone()));
    runner.set_ticket_service(Some(f.service.clone()));
    let sessions_cache = Arc::default();
    runner.set_local_task_runtime(Some(LocalTaskRuntime {
        agent: agent.clone(),
        tools: tools.clone(),
        provider_router: None,
        app_data_dir: Some(f.dir.path().join("host")),
        sessions_cache: Arc::clone(&sessions_cache),
    }));
    let (events, receive) = broadcast::channel(256);
    let ctx = crate::execution::SpawnContext {
        agent,
        tools,
        sessions_cache,
        agent_runners: Arc::new(RwLock::new(HashMap::new())),
        session_event_senders: Arc::new(RwLock::new(HashMap::from([
            (f.supervisor.id.clone(), events.clone()),
            (f.child.id.clone(), events),
        ]))),
        external_child_runner: runner.clone(),
        provider_router: None,
        app_data_dir: Some(f.dir.path().join("host")),
        completion_handler: None,
        child_run_launch_hook: None,
        account_feed_inbox: None,
    };
    (runner, ctx, receive)
}

#[tokio::test]
async fn admitted_native_work_outlives_implicit_child_total_and_publishes_result() {
    let mut f = Fixture::new().await;
    f.child.add_message(Message::system("Return the result."));
    f.child.add_message(Message::user("Complete this Ticket."));
    f.storage.save_session(&f.child).await.unwrap();
    let provider = Arc::new(ReasoningThenResult(AtomicUsize::new(0)));
    let (runner, ctx, mut events) = native_work_runtime(&f, provider.clone()).await;
    assert!(
        !runner
            .owns_current_native_work(&f.child.id, "unadmitted", f.child.created_at)
            .await
    );
    crate::sdk::spawn::run_child_spawn(
        ctx.clone(),
        SpawnJob {
            parent_session_id: f.supervisor.id.clone(),
            child_session_id: f.child.id.clone(),
            model: "fixture-model".into(),
            disabled_tools: None,
        },
    )
    .await
    .unwrap();
    let run_id = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let DispatchObservation::Admitted { receipt } =
                query_dispatch(f.storage.as_ref(), &f.service, &f.key, &f.spec)
                    .await
                    .unwrap()
            {
                if runner
                    .native_task_start(&f.child.id, &receipt.run_id, f.child.created_at)
                    .is_some()
                {
                    break receipt.run_id;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("real Ticket admission and Native owner");
    let entry = ctx.agent_runners.read().await[&f.child.id].clone();
    assert_eq!(entry.run_id, run_id);
    assert!(
        runner
            .owns_current_native_work(&f.child.id, &run_id, f.child.created_at)
            .await
    );
    assert!(
        !runner
            .owns_current_native_work(&f.child.id, "foreign-run", f.child.created_at)
            .await
    );
    assert!(
        !runner
            .owns_current_native_work(
                &f.child.id,
                &run_id,
                f.child.created_at - chrono::Duration::seconds(1)
            )
            .await
    );
    assert!(
        !runner
            .owns_current_native_work("forged-ticket-label", &run_id, f.child.created_at)
            .await
    );
    runner.set_ticket_service(None);
    assert!(
        !runner
            .owns_current_native_work(&f.child.id, &run_id, f.child.created_at)
            .await
    );
    runner.set_ticket_service(Some(f.service.clone()));
    let mut policy_source = f.child.clone();
    policy_source
        .metadata
        .insert("child_watchdog.check_interval_secs".into(), "1".into());
    let mut policy = crate::execution::spawn::watchdog_policy_for_session(&policy_source);
    // Test the default branch without installing an explicit total override.
    policy.max_total_secs = 1;
    let done = CancellationToken::new();
    let watchdog = tokio::spawn(crate::execution::spawn::watch_child_liveness(
        crate::execution::spawn::ChildWatchdogTarget {
            parent_session_id: f.supervisor.id.clone(),
            child_session_id: f.child.id.clone(),
            child_birth: f.child.created_at,
            run_id,
            runtime: runner.clone(),
        },
        ctx.agent_runners.clone(),
        entry.cancel_token.clone(),
        Arc::new(RwLock::new(None)),
        done.clone(),
        policy,
    ));
    let status = tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            if let AgentEvent::SubAgentCompleted {
                child_session_id,
                status,
                ..
            } = events.recv().await.unwrap()
            {
                assert_eq!(child_session_id, f.child.id);
                break status;
            }
        }
    })
    .await
    .expect("actual completion must notify its parent");
    done.cancel();
    watchdog.await.unwrap();
    runner.shutdown().await.unwrap();
    assert!(
        !runner
            .owns_current_native_work(&f.child.id, &entry.run_id, f.child.created_at)
            .await
    );
    assert_eq!(
        status, "completed",
        "active managed Work must outlive the implicit total"
    );
    assert_eq!(provider.0.load(Ordering::SeqCst), 1);
    let snapshot = f.service.published().unwrap().1;
    assert_eq!(snapshot.submissions.len(), 1);
    assert_eq!(
        snapshot.tickets[&f.spec.work_id].state,
        WorkState::Submitted
    );
    assert!(snapshot.assignments[&f.spec.assignment_id].process_stopped);
    assert!(snapshot.tickets[&f.spec.work_id]
        .accepted_submission
        .is_none());
}

struct SilentProvider(Arc<std::sync::atomic::AtomicBool>);

#[async_trait]
impl LLMProvider for SilentProvider {
    async fn chat_stream(
        &self,
        _: &[Message],
        _: &[ToolSchema],
        _: Option<u32>,
        _: &str,
    ) -> std::result::Result<LLMStream, LLMError> {
        self.0.store(true, Ordering::SeqCst);
        Ok(Box::pin(futures::stream::pending()))
    }
}

#[tokio::test]
async fn native_work_preserves_explicit_total_and_idle_timeouts_without_submission() {
    for idle in [false, true] {
        let mut f = Fixture::new().await;
        f.child.add_message(Message::system("Return the result."));
        f.child.add_message(Message::user("Complete this Ticket."));
        f.child
            .metadata
            .insert("child_watchdog.check_interval_secs".into(), "1".into());
        if !idle {
            f.child
                .metadata
                .insert("child_watchdog.max_total_secs".into(), "3600".into());
        }
        f.storage.save_session(&f.child).await.unwrap();
        let silent_entered = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let provider: Arc<dyn LLMProvider> = if idle {
            Arc::new(SilentProvider(silent_entered.clone()))
        } else {
            Arc::new(HeldReasoning {
                calls: AtomicUsize::new(0),
                finish: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            })
        };
        let (runner, ctx, mut events) = native_work_runtime(&f, provider).await;
        crate::sdk::spawn::run_child_spawn(
            ctx.clone(),
            SpawnJob {
                parent_session_id: f.supervisor.id.clone(),
                child_session_id: f.child.id.clone(),
                model: "fixture-model".into(),
                disabled_tools: None,
            },
        )
        .await
        .unwrap();
        // Start the observation only after real admission and provider entry;
        // queue/preparation latency is not the timeout condition under test.
        let receipt = tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                if let DispatchObservation::Admitted { receipt } =
                    query_dispatch(f.storage.as_ref(), &f.service, &f.key, &f.spec)
                        .await
                        .unwrap()
                {
                    if runner
                        .native_task_start(&f.child.id, &receipt.run_id, f.child.created_at)
                        .is_some()
                        && (!idle || silent_entered.load(Ordering::SeqCst))
                    {
                        break receipt;
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("actual admitted Native execution");
        assert!(
            runner
                .owns_current_native_work(&f.child.id, &receipt.run_id, f.child.created_at)
                .await
        );
        let done = CancellationToken::new();
        let reason = Arc::new(RwLock::new(None));
        let watchdog = if idle {
            let mut policy = crate::execution::spawn::watchdog_policy_for_session(&f.child);
            policy.max_idle_secs = 1;
            let entry = ctx.agent_runners.read().await[&f.child.id].clone();
            // Exercise the production idle watchdog against this entered task.
            // Its own reason is retained separately from the SDK's watchdog.
            Some(tokio::spawn(crate::execution::spawn::watch_child_liveness(
                crate::execution::spawn::ChildWatchdogTarget {
                    parent_session_id: f.supervisor.id.clone(),
                    child_session_id: f.child.id.clone(),
                    child_birth: f.child.created_at,
                    run_id: receipt.run_id,
                    runtime: runner.clone(),
                },
                ctx.agent_runners.clone(),
                entry.cancel_token,
                reason.clone(),
                done.clone(),
                policy,
            )))
        } else {
            // Same explicit value as the default, through the SDK's own
            // watchdog; age only its observation clock after Native admission.
            ctx.agent_runners
                .write()
                .await
                .get_mut(&f.child.id)
                .unwrap()
                .started_at = chrono::Utc::now() - chrono::Duration::seconds(3601);
            None
        };
        let error = tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                if let AgentEvent::SubAgentCompleted { status, error, .. } =
                    events.recv().await.unwrap()
                {
                    assert_eq!(status, if idle { "cancelled" } else { "timeout" });
                    break error;
                }
            }
        })
        .await
        .expect("watchdog must stop the actually admitted owned task");
        done.cancel();
        if let Some(watchdog) = watchdog {
            watchdog.await.unwrap();
        }
        if idle {
            assert!(reason
                .read()
                .await
                .as_ref()
                .is_some_and(|reason| reason.contains("idle timeout")));
        } else {
            assert!(error.unwrap().contains("max_total_secs=3600"));
        }
        runner.shutdown().await.unwrap();
        let snapshot = f.service.published().unwrap().1;
        assert!(snapshot.submissions.is_empty());
        assert!(snapshot.assignments[&f.spec.assignment_id].process_stopped);
    }
}

#[test]
fn total_limit_origin_is_preserved_for_direct_and_parent_backstop() {
    let mut child = Session::new("ordinary-child", "fixture-model");
    let default = crate::execution::spawn::watchdog_policy_for_session(&child);
    for slack in [0, 600] {
        assert!(default.total_timeout_reached(3600 + slack, slack, false));
        assert!(!default.total_timeout_reached(3600 + slack, slack, true));
        assert!(!default.total_timeout_reached(3599 + slack, slack, false));
    }
    for value in ["0", "-1", "invalid"] {
        child
            .metadata
            .insert("child_watchdog.max_total_secs".into(), value.into());
        let implicit = crate::execution::spawn::watchdog_policy_for_session(&child);
        assert!(!implicit.total_timeout_reached(4200, 600, true));
    }
    child
        .metadata
        .insert("child_watchdog.max_total_secs".into(), "3600".into());
    let explicit = crate::execution::spawn::watchdog_policy_for_session(&child);
    assert!(explicit.total_timeout_reached(3600, 0, true));
    assert!(explicit.total_timeout_reached(4200, 600, true));
    assert!(!explicit.needs_native_work_proof(4200, 600));
    assert_eq!(
        explicit.max_total_secs, 3600,
        "remote lease still sees the original value"
    );
}

#[tokio::test]
async fn real_parent_backstop_preserves_active_work_but_enforces_explicit_total() {
    for explicit in [false, true] {
        let mut f = Fixture::new().await;
        f.child.add_message(Message::system("Return the result."));
        f.child.add_message(Message::user("Complete this Ticket."));
        if explicit {
            f.child
                .metadata
                .insert("child_watchdog.max_total_secs".into(), "3600".into());
        }
        f.storage.save_session(&f.child).await.unwrap();
        let finish = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let provider = Arc::new(HeldReasoning {
            calls: AtomicUsize::new(0),
            finish: finish.clone(),
        });
        let (runner, ctx, mut events) = native_work_runtime(&f, provider.clone()).await;
        let registry = Arc::new(bamboo_llm::ProviderRegistry::new(
            HashMap::from([("test".into(), provider.clone() as Arc<dyn LLMProvider>)]),
            "test".into(),
        ));
        let coordinator = crate::ChildCompletionCoordinator::new(
            f.storage.clone(),
            f.runtime.parent_question_lock.as_ref().unwrap().clone(),
            ctx.sessions_cache.clone(),
            ctx.agent_runners.clone(),
            ctx.session_event_senders.clone(),
            ctx.agent.clone(),
            Arc::new(RwLock::new(Config::default())),
            registry.clone(),
            Arc::new(bamboo_llm::ProviderModelRouter::new(registry)),
            f.dir.path().join("host"),
            None,
        );
        let scheduler = Arc::new(crate::execution::SpawnScheduler::new(ctx.clone()));
        coordinator.set_spawn_scheduler(&scheduler).await;
        crate::sdk::spawn::run_child_spawn(
            ctx.clone(),
            SpawnJob {
                parent_session_id: f.supervisor.id.clone(),
                child_session_id: f.child.id.clone(),
                model: "fixture-model".into(),
                disabled_tools: None,
            },
        )
        .await
        .unwrap();
        let receipt = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let DispatchObservation::Admitted { receipt } =
                    query_dispatch(f.storage.as_ref(), &f.service, &f.key, &f.spec)
                        .await
                        .unwrap()
                {
                    if runner
                        .native_task_start(&f.child.id, &receipt.run_id, f.child.created_at)
                        .is_some()
                    {
                        break receipt;
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("real Native admission");
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if matches!(events.recv().await.unwrap(), AgentEvent::ReasoningToken { content } if !content.is_empty()) { break; }
            }
        }).await.expect("actual provider reasoning must be observed before the sweep");
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if ctx.agent_runners.read().await[&f.child.id]
                    .last_activity_at()
                    .is_some()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("actual reasoning event must reach the runner activity clock");
        let cancel = {
            let mut entries = ctx.agent_runners.write().await;
            let entry = entries.get_mut(&f.child.id).unwrap();
            // Only the watchdog observation clock ages. Native entry, receipt,
            // birth and current event activity remain real and unmodified.
            entry.started_at = chrono::Utc::now() - chrono::Duration::seconds(4201);
            assert!(entry.last_activity_at().is_some());
            entry.cancel_token.clone()
        };
        let mut wait = bamboo_domain::WaitingForChildrenState::for_children(
            vec![f.child.id.clone()],
            bamboo_domain::ChildWaitPolicy::All,
            chrono::Utc::now(),
        );
        wait.registered_at -= chrono::Duration::seconds(120);
        coordinator
            .sweep_child_wait_for_test(&f.supervisor.id, wait)
            .await;
        assert_eq!(
            cancel.is_cancelled(),
            explicit,
            "parent backstop must distinguish default from explicit total"
        );
        if !explicit {
            assert!(
                runner
                    .owns_current_native_work(&f.child.id, &receipt.run_id, f.child.created_at)
                    .await
            );
            assert!(
                matches!(query_dispatch(f.storage.as_ref(), &f.service, &f.key, &f.spec).await.unwrap(), DispatchObservation::Admitted { receipt: current } if current == receipt)
            );
        }
        finish.store(true, Ordering::SeqCst);
        tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                if let AgentEvent::SubAgentCompleted { .. } = events.recv().await.unwrap() {
                    break;
                }
            }
        })
        .await
        .expect("real terminal notification");
        runner.shutdown().await.unwrap();
        let snapshot = f.service.published().unwrap().1;
        assert_eq!(snapshot.submissions.len(), usize::from(!explicit));
        assert!(snapshot.assignments[&f.spec.assignment_id].process_stopped);
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    }
}

struct HeldReasoning {
    calls: AtomicUsize,
    finish: Arc<std::sync::atomic::AtomicBool>,
}

#[async_trait]
impl LLMProvider for HeldReasoning {
    async fn chat_stream(
        &self,
        _: &[Message],
        _: &[ToolSchema],
        _: Option<u32>,
        _: &str,
    ) -> std::result::Result<LLMStream, LLMError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(Box::pin(futures::stream::unfold(
            (self.finish.clone(), 0),
            |(finish, step)| async move {
                let (chunk, next) = match step {
                    0 if !finish.load(Ordering::SeqCst) => {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        (
                            LLMChunk::ReasoningToken("Still checking the owned Work.".into()),
                            0,
                        )
                    }
                    0 => (LLMChunk::Token(OUTPUT.into()), 1),
                    1 => (LLMChunk::Done, 2),
                    _ => return None,
                };
                Some((Ok(chunk), (finish, next)))
            },
        )))
    }
}
