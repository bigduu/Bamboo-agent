struct ExecReleaseGate(std::path::PathBuf);

impl Drop for ExecReleaseGate {
    fn drop(&mut self) {
        let _ = std::fs::write(&self.0, b"release");
    }
}

#[tokio::test]
async fn background_exec_terminal_result_is_durable_wakes_once_and_remains_pullable() {
    let (temp, store, inbox, coordinator, reservations, launches) =
        completion_inbox_fixture().await;
    let gate = temp.path().join("exec-release");
    let _release = ExecReleaseGate(gate.clone());
    let mut session = Session::new("background-exec-inbox", "model");
    let tools = bamboo_tools::BuiltinToolExecutor::new();
    let mut ctx = bamboo_agent_core::ToolCtx::none("start-background-inbox");
    ctx.session_id = Some(Arc::from(session.id.as_str()));
    let command = format!(
        "while [ ! -f '{}' ]; do sleep 0.01; done; printf inbox-result; exit 7",
        gate.to_string_lossy().replace('\'', "'\\''")
    );
    let bamboo_agent_core::ToolOutcome::Completed(started) = tools
        .registry()
        .get("exec_command")
        .unwrap()
        .invoke(
            serde_json::json!({"cmd": command, "yield_time_ms": 250}),
            ctx.clone(),
        )
        .await
        .unwrap()
    else {
        panic!("ordinary exec");
    };
    let started: serde_json::Value = serde_json::from_str(&started.result).unwrap();
    let handle = started["session_id"].as_str().unwrap().to_owned();
    let mut runtime = AgentRuntimeState::new("background-inbox-run");
    runtime.status = AgentStatusState::Suspended;
    runtime.waiting_for_bash = Some(bamboo_domain::WaitingForBashState::for_bash(
        vec![handle.clone()],
        Utc::now(),
    ));
    runtime.suspension = Some(SuspensionState {
        reason: "waiting_for_bash".into(),
        suspended_at: Utc::now(),
        resumable: true,
        hook_point: None,
    });
    write_runtime_state(&mut session, &runtime);
    session
        .metadata
        .insert("runtime.suspend_reason".into(), "waiting_for_bash".into());
    session
        .metadata
        .insert("runtime.background_exec_id".into(), handle.clone());
    store.save_session(&session).await.unwrap();
    let job =
        bamboo_tools::tools::exec_command::reserve_background_job(&session.id, &handle).unwrap();
    assert!(coordinator.supports_background_exec());
    coordinator.arrange_exec_self_resume(job, session.created_at);
    tokio::fs::write(&gate, b"release").await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while inbox.inspect(&session.id).await.unwrap().pending == 0
            || launches.load(Ordering::SeqCst) == 0
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let saved = store.load_session(&session.id).await.unwrap().unwrap();
    assert!(read_runtime_state(&saved).waiting_for_bash.is_none());
    assert!(!saved.metadata.contains_key("runtime.background_exec_id"));
    assert_eq!(reservations.load(Ordering::SeqCst), 1);
    assert_eq!(launches.load(Ordering::SeqCst), 1);
    let claim = inbox.claim(&session.id, 1).await.unwrap().remove(0);
    let SessionMessageBody::RuntimeInstruction(instruction) = &claim.envelope.body else {
        panic!("typed runtime completion");
    };
    assert_eq!(instruction.instruction, "background_exec_completed");
    let data = instruction.data.as_ref().unwrap();
    let result: serde_json::Value = serde_json::from_str(data["result"].as_str().unwrap()).unwrap();
    assert_eq!(result["exit_code"], 7);
    assert_eq!(result["output"], "inbox-result");
    let provider = claim.envelope.to_provider_message().unwrap();
    assert!(provider.content.contains("write_stdin"));
    assert!(!provider.content.contains("BashOutput"));
    assert!(provider.metadata.as_ref().unwrap()["hidden_from_ui"]
        .as_bool()
        .unwrap());
    coordinator
        .deliver_shell_completion(
            BashCompletionInfo {
                session_id: session.id.clone(),
                bash_id: handle.clone(),
                command,
                status: "completed".into(),
                exit_code: Some(7),
                output_tail: data["result"].as_str().unwrap().to_owned(),
            },
            ShellCompletionKind::Exec,
        )
        .await;
    assert_eq!(inbox.inspect(&session.id).await.unwrap().claimed, 1);
    assert_eq!(launches.load(Ordering::SeqCst), 1);
    let bamboo_agent_core::ToolOutcome::Completed(pulled) = tools
        .registry()
        .get("write_stdin")
        .unwrap()
        .invoke(serde_json::json!({"session_id": handle}), ctx)
        .await
        .unwrap()
    else {
        panic!("ordinary pull");
    };
    let pulled: serde_json::Value = serde_json::from_str(&pulled.result).unwrap();
    assert_eq!(pulled["output"], "inbox-result");
    assert_eq!(pulled["exit_code"], 7);
}

#[tokio::test]
async fn background_exec_delivery_does_not_overwrite_an_active_human_conversation() {
    let (_temp, store, inbox, coordinator, _, launches) = completion_inbox_fixture().await;
    let mut session = Session::new("active-background-conversation", "model");
    session.add_message(Message::user("Continue discussing the next step."));
    let mut runtime = AgentRuntimeState::new("human-conversation-run");
    runtime.status = AgentStatusState::Running;
    runtime.waiting_for_bash = Some(bamboo_domain::WaitingForBashState::for_bash(
        vec!["previous-exec".into()],
        Utc::now(),
    ));
    write_runtime_state(&mut session, &runtime);
    session
        .metadata
        .insert("runtime.background_exec_id".into(), "previous-exec".into());
    store.save_session(&session).await.unwrap();
    let router = coordinator.agent.activation_router().unwrap().clone();
    let mut registration = router
        .register_session_run(&session, "human-conversation-run")
        .await
        .unwrap();
    let mut runner = AgentRunner::new();
    runner.status = AgentStatus::Running;
    coordinator
        .agent_runners
        .write()
        .await
        .insert(session.id.clone(), runner);
    coordinator
        .deliver_shell_completion(
            BashCompletionInfo {
                session_id: session.id.clone(),
                bash_id: "previous-exec".into(),
                command: String::new(),
                status: "completed".into(),
                exit_code: Some(7),
                output_tail: r#"{"status":"completed","exit_code":7,"output":"late result"}"#
                    .into(),
            },
            ShellCompletionKind::Exec,
        )
        .await;
    let saved = store.load_session(&session.id).await.unwrap().unwrap();
    assert_eq!(read_runtime_state(&saved).status, AgentStatusState::Running);
    assert!(read_runtime_state(&saved).waiting_for_bash.is_none());
    assert_eq!(
        serde_json::to_value(&saved.messages).unwrap(),
        serde_json::to_value(&session.messages).unwrap()
    );
    assert_eq!(launches.load(Ordering::SeqCst), 0);
    assert_eq!(inbox.inspect(&session.id).await.unwrap().pending, 1);
    assert!(registration.notifications_mut().has_changed().unwrap());
}

#[test]
fn background_exec_restart_or_wait_ceiling_never_claims_command_completion() {
    for timed_out in [false, true] {
        let message = exec_wait_resume_message(&["original-exec".into()], timed_out);
        assert!(message.content.contains("Completion is not confirmed"));
        assert!(message.content.contains("write_stdin"));
        assert!(message.content.contains("unknown outcome"));
        assert!(!message.content.contains("have completed"));
    }
}

#[tokio::test]
async fn background_exec_does_not_deliver_into_a_recreated_session_lifetime() {
    let (_temp, store, inbox, coordinator, _, launches) = completion_inbox_fixture().await;
    let old = Session::new("reused-background-session", "model");
    let mut replacement = Session::new(&old.id, "model");
    replacement.created_at = old.created_at + chrono::Duration::seconds(1);
    store.save_session(&replacement).await.unwrap();
    coordinator
        .deliver_shell_completion_for_lifetime(
            BashCompletionInfo {
                session_id: old.id.clone(),
                bash_id: "old-command".into(),
                command: String::new(),
                status: "completed".into(),
                exit_code: Some(0),
                output_tail: "old result".into(),
            },
            ShellCompletionKind::Exec,
            Some(old.created_at),
        )
        .await;
    assert_eq!(inbox.inspect(&old.id).await.unwrap().pending, 0);
    assert_eq!(launches.load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn background_exec_cache_publication_holds_the_same_lock_as_human_append() {
    let (_temp, store, _inbox, coordinator, _, _) = completion_inbox_fixture().await;
    let mut session = Session::new("background-cache-publish-lock", "model");
    let mut runtime = AgentRuntimeState::new("background-cache-run");
    runtime.waiting_for_bash = Some(bamboo_domain::WaitingForBashState::for_bash(
        vec!["exec-lock".into()],
        Utc::now(),
    ));
    write_runtime_state(&mut session, &runtime);
    store.save_session(&session).await.unwrap();
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let completing = session.clone();
    let publishing = coordinator.clone();
    let clear = tokio::spawn(async move {
        let cache = publishing.sessions.clone();
        clear_completed_exec_wait(
            &publishing.persistence,
            &completing,
            "exec-lock",
            move |saved| {
                entered_tx.send(()).unwrap();
                release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                cache.insert(
                    saved.id.clone(),
                    Arc::new(crate::SessionSnapshot::new(saved.clone())),
                );
            },
        )
        .await
        .unwrap();
    });
    tokio::time::timeout(Duration::from_secs(5), entered_rx)
        .await
        .unwrap()
        .unwrap();
    let acquiring = coordinator.persistence.acquire_lock(&session.id);
    tokio::pin!(acquiring);
    assert!(
        futures::poll!(acquiring.as_mut()).is_pending(),
        "a Human writer must not enter between durable clear and cache publication"
    );
    release_tx.send(()).unwrap();
    clear.await.unwrap();
    drop(acquiring.await);
    coordinator
        .persistence
        .update_runtime_config_and_publish(
            &session.id,
            |latest| {
                latest.add_message(Message::user("new Human append after completion"));
            },
            |saved| {
                coordinator.sessions.insert(
                    saved.id.clone(),
                    Arc::new(crate::SessionSnapshot::new(saved.clone())),
                );
            },
        )
        .await
        .unwrap();
    let durable = store.load_session(&session.id).await.unwrap().unwrap();
    let cached = coordinator
        .sessions
        .get(&session.id)
        .unwrap()
        .value()
        .read();
    assert_eq!(
        serde_json::to_value(&durable.messages).unwrap(),
        serde_json::to_value(&cached.messages).unwrap()
    );
    assert!(cached
        .messages
        .iter()
        .any(|message| message.content == "new Human append after completion"));
}

#[derive(Default)]
struct ExactStartupProvider(StdMutex<Vec<Vec<String>>>);

#[async_trait]
impl bamboo_llm::LLMProvider for ExactStartupProvider {
    async fn chat_stream(
        &self,
        messages: &[Message],
        tools: &[bamboo_agent_core::tools::ToolSchema],
        max_output_tokens: Option<u32>,
        model: &str,
    ) -> Result<bamboo_llm::LLMStream, bamboo_llm::LLMError> {
        self.0
            .lock()
            .unwrap()
            .push(messages.iter().map(|message| message.id.clone()).collect());
        CompletedTestProvider
            .chat_stream(messages, tools, max_output_tokens, model)
            .await
    }
}

struct PausedExactStartup {
    coordinator: Arc<ChildCompletionCoordinator>,
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

#[async_trait]
impl SessionActivationSpawner for PausedExactStartup {
    async fn reserve_activation(
        &self,
        target: &str,
        generation: u64,
    ) -> Result<SessionActivationReserveOutcome, bamboo_domain::SessionActivationError> {
        self.coordinator
            .reserve_activation(target, generation)
            .await
    }

    async fn reserve_runtime_activation(
        &self,
        target: &str,
        startup: bamboo_domain::SessionRuntimeStartup,
    ) -> Result<SessionActivationReserveOutcome, bamboo_domain::SessionActivationError> {
        let result = self
            .coordinator
            .reserve_runtime_activation(target, startup)
            .await;
        self.entered.notify_one();
        self.release.notified().await;
        result
    }
}

fn fixture_exec_result(target: &str) -> BashCompletionInfo {
    BashCompletionInfo {
        session_id: target.into(),
        bash_id: "fixture-command".into(),
        command: String::new(),
        status: "completed".into(),
        exit_code: Some(0),
        output_tail: r#"{"status":"completed","exit_code":0,"output":"fixture"}"#.into(),
    }
}

async fn actor_exec_fixture() -> (
    tempfile::TempDir,
    Arc<bamboo_storage::SessionStoreV2>,
    Arc<dyn SessionInboxPort>,
    Arc<ChildCompletionCoordinator>,
    Arc<ExactStartupProvider>,
) {
    let (temp, store, inbox, coordinator, _, _) = completion_inbox_fixture().await;
    let mut coordinator = Arc::try_unwrap(coordinator).ok().unwrap();
    let router = coordinator.agent.activation_router().unwrap().clone();
    router.set_inbox(inbox.clone());
    let host = crate::SessionRepository::new(
        coordinator.sessions.clone(),
        store.clone(),
        coordinator.persistence.clone(),
    )
    .with_root_actor_directory(store.clone());
    coordinator.agent = Arc::new(coordinator.agent.with_execution_persistence(Arc::new(host)));
    let provider = Arc::new(ExactStartupProvider::default());
    let providers = HashMap::from([(
        "test".to_string(),
        provider.clone() as Arc<dyn bamboo_llm::LLMProvider>,
    )]);
    coordinator.config.write().await.provider = "test".into();
    coordinator.provider_registry = Arc::new(ProviderRegistry::new(providers, "test".into()));
    coordinator.provider_router = Arc::new(ProviderModelRouter::new(
        coordinator.provider_registry.clone(),
    ));
    coordinator.root_account_sink =
        Some(crate::events::AccountEventSink::new(temp.path().join("events")).unwrap());
    coordinator.set_root_tools(Arc::new(EmptyTools)).await;
    let coordinator = Arc::new(coordinator);
    router.set_spawner(coordinator.clone()).await;
    (temp, store, inbox, coordinator, provider)
}

async fn wait_for_actor_startup_to_settle(coordinator: &ChildCompletionCoordinator, target: &str) {
    let router = coordinator.agent.activation_router().unwrap();
    tokio::time::timeout(Duration::from_secs(8), async {
        while router.has_activation_owner_or_reservation(target).await {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn background_exec_owned_actor_calls_provider_only_after_exact_receipt() {
    use bamboo_domain::{is_matching_session_message, ActorActivationFinish};
    let (_temp, store, inbox, coordinator, provider) = actor_exec_fixture().await;
    let router = coordinator.agent.activation_router().unwrap();
    for case in ["producer", "recovery"] {
        let mut root = Session::new(format!("exact-background-{case}"), "model");
        root.add_message(Message::user("prior transcript"));
        let info = fixture_exec_result(&root.id);
        let mut envelope = shell_completion_envelope(&info, ShellCompletionKind::Exec);
        let mut runtime = AgentRuntimeState::new("prior-background-run");
        runtime.status = AgentStatusState::Suspended;
        runtime.waiting_for_bash = Some(bamboo_domain::WaitingForBashState::for_bash(
            vec![info.bash_id.clone()],
            Utc::now(),
        ));
        write_runtime_state(&mut root, &runtime);
        root.metadata
            .insert("runtime.suspend_reason".into(), "waiting_for_bash".into());
        store.save_session(&root).await.unwrap();
        let mut prior = coordinator
            .agent
            .persistence()
            .bind_root_actor_execution(&root, &runtime.run_id)
            .await
            .unwrap()
            .unwrap();
        prior
            .directory
            .finish_activation(
                &prior.owner.fence,
                Utc::now(),
                ActorActivationFinish::Succeeded,
            )
            .await
            .unwrap();
        prior.disarm_abandonment();

        match case {
            "producer" => {
                coordinator
                    .deliver_shell_completion_for_lifetime(
                        info,
                        ShellCompletionKind::Exec,
                        Some(root.created_at),
                    )
                    .await
            }
            "recovery" => {
                let receipt = inbox
                    .deliver_runtime_for_target(&envelope, root.created_at)
                    .await
                    .unwrap();
                inbox
                    .mark_message_activation_eligible(
                        &root.id,
                        &receipt,
                        SessionActivationPolicy::RespectSpecificWait,
                    )
                    .await
                    .unwrap();
                clear_completed_exec_wait(&coordinator.persistence, &root, &info.bash_id, |_| {})
                    .await
                    .unwrap()
                    .unwrap();
                let selected = inbox
                    .inspect_wake_readiness(&root.id, Utc::now())
                    .await
                    .unwrap()
                    .ready
                    .unwrap()
                    .runtime_startup()
                    .unwrap();
                assert_eq!(selected.target_created_at, root.created_at);
                assert_eq!(selected.receipt, receipt);
                router.reconcile_wake(&root.id, Utc::now()).await.unwrap();
            }
            _ => unreachable!(),
        }
        wait_for_actor_startup_to_settle(&coordinator, &root.id).await;

        assert!(inbox.was_admitted(&root.id, &envelope.id).await.unwrap());
        let saved = store.load_session(&root.id).await.unwrap().unwrap();
        let message = saved
            .messages
            .iter()
            .find(|message| message.id == envelope.id.as_str())
            .unwrap();
        // Producer retries keep the stable ID while stamping the arrival time.
        envelope.created_at = message.created_at;
        assert!(is_matching_session_message(message, &envelope));
        let mut calls = provider.0.lock().unwrap();
        assert_eq!(calls.len(), 1, "{case}: {:?}", saved.last_run_error());
        assert!(calls[0].iter().any(|id| id == envelope.id.as_str()));
        calls.clear();
    }
}

#[tokio::test]
async fn background_exec_owned_actor_rejects_missing_stale_and_human_gated_startup() {
    use bamboo_domain::{
        SessionActivationPort, SessionInboxConsumerId, SessionInboxLeaseRequest,
        SessionRuntimeStartup,
    };
    let (_temp, store, inbox, coordinator, provider) = actor_exec_fixture().await;
    let router = coordinator.agent.activation_router().unwrap().clone();

    for case in [
        "missing",
        "coalesced-missing",
        "leased-other-run",
        "human-after-reserve",
        "foreign-lifetime",
    ] {
        let mut root = Session::new(format!("exact-background-{case}"), "model");
        root.add_message(Message::user(
            "prior transcript must not start a model call",
        ));
        store.save_session(&root).await.unwrap();
        let envelope =
            shell_completion_envelope(&fixture_exec_result(&root.id), ShellCompletionKind::Exec);
        let delivery = inbox
            .deliver_runtime_for_target(&envelope, root.created_at)
            .await
            .unwrap();
        inbox
            .mark_message_activation_eligible(
                &root.id,
                &delivery,
                SessionActivationPolicy::RespectSpecificWait,
            )
            .await
            .unwrap();
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        router
            .set_spawner(Arc::new(PausedExactStartup {
                coordinator: coordinator.clone(),
                entered: entered.clone(),
                release: release.clone(),
            }))
            .await;
        let mut startup = SessionRuntimeStartup {
            receipt: delivery.clone(),
            target_created_at: root.created_at,
        };
        if matches!(case, "missing" | "coalesced-missing") {
            startup.receipt.id = bamboo_domain::SessionMessageId::new();
        } else if case == "foreign-lifetime" {
            startup.target_created_at = root.created_at - chrono::Duration::seconds(1);
        }
        let previous = if case == "coalesced-missing" {
            Some(
                router
                    .register_session_run(&root, "earlier-logical-owner")
                    .await
                    .unwrap(),
            )
        } else {
            None
        };
        let target = root.id.clone();
        let act = router.clone();
        let activating = tokio::spawn(async move {
            let outcome = act.request_runtime_startup(&target, startup).await.unwrap();
            if let Some(previous) = previous {
                assert_eq!(
                    outcome,
                    bamboo_domain::SessionActivationDisposition::ActiveNotified
                );
                previous.finish(0).await.unwrap().unwrap()
            } else {
                outcome
            }
        });
        tokio::time::timeout(Duration::from_secs(8), entered.notified())
            .await
            .unwrap();
        match case {
            "leased-other-run" => {
                let lease = SessionInboxLeaseRequest {
                    consumer: SessionInboxConsumerId::new(),
                    now: Utc::now(),
                    duration: chrono::Duration::seconds(15),
                };
                let selected = inbox
                    .claim_owned_receipt(&root.id, &delivery, Some("another-run"), &lease)
                    .await
                    .unwrap();
                assert!(selected.is_some());
            }
            "human-after-reserve" => {
                let mut latest = store.load_session(&root.id).await.unwrap().unwrap();
                latest.pending_question = Some(bamboo_domain::PendingQuestion {
                    tool_call_id: "latest-human-interaction".into(),
                    tool_name: "AskUserQuestion".into(),
                    question: "Choose whether to continue".into(),
                    options: vec!["Continue".into(), "Stop".into()],
                    allow_custom: false,
                    source: Default::default(),
                });
                store.save_session(&latest).await.unwrap();
            }
            _ => {}
        }
        release.notify_one();
        let disposition = activating.await.unwrap();
        assert_eq!(
            disposition,
            if case == "foreign-lifetime" {
                bamboo_domain::SessionActivationDisposition::ActivationCoalesced
            } else {
                bamboo_domain::SessionActivationDisposition::ActivationReserved
            },
            "{case}"
        );
        wait_for_actor_startup_to_settle(&coordinator, &root.id).await;
        assert!(provider.0.lock().unwrap().is_empty(), "{case}");
        assert!(
            !inbox.was_admitted(&root.id, &envelope.id).await.unwrap(),
            "{case}"
        );
        let saved = store.load_session(&root.id).await.unwrap().unwrap();
        assert_eq!(saved.messages.len(), 1, "{case}");
        assert_eq!(saved.has_pending_question(), case == "human-after-reserve");
    }
}

#[tokio::test]
async fn background_exec_owned_actor_preserves_native_human_receipt_selection() {
    use bamboo_domain::{ActorActivationFinish, SessionRuntimeStartup};
    let (_temp, store, inbox, coordinator, _) = actor_exec_fixture().await;
    let mut root = Session::new("exact-native-and-runtime", "model");
    store.save_session(&root).await.unwrap();
    let runtime =
        shell_completion_envelope(&fixture_exec_result(&root.id), ShellCompletionKind::Exec);
    let deferred = inbox
        .deliver_runtime_for_target(&runtime, root.created_at)
        .await
        .unwrap();
    inbox
        .mark_message_activation_eligible(
            &root.id,
            &deferred,
            SessionActivationPolicy::RespectSpecificWait,
        )
        .await
        .unwrap();
    let human = bamboo_domain::SessionMessageEnvelope::user_input(&root.id, "Selected Human input");
    let selected = inbox
        .deliver_with_activation_intent(
            &human,
            SessionActivationPolicy::InterruptSpecificWait,
            None,
        )
        .await
        .unwrap();
    let mut binding = coordinator
        .agent
        .persistence()
        .bind_root_actor_execution(&root, "native-selection-run")
        .await
        .unwrap()
        .unwrap();

    assert!(binding
        .persistence
        .admit_root_runtime_startup(
            &mut root,
            inbox.clone(),
            &SessionRuntimeStartup {
                receipt: deferred,
                target_created_at: binding.owner.created_at
            },
            "other-run",
        )
        .await
        .is_err());
    assert!(!inbox.was_admitted(&root.id, &runtime.id).await.unwrap());

    let exact = binding
        .persistence
        .bind_root_inbox_receipt(selected)
        .unwrap();
    let admission = exact
        .admit_root_inbox(&mut root, inbox.clone(), Some("native-selection-run"))
        .await
        .unwrap()
        .unwrap();
    assert!(admission.admission_error.is_none(), "{admission:?}");
    assert_eq!(admission.merged, 1);
    assert!(inbox.was_admitted(&root.id, &human.id).await.unwrap());
    assert!(!inbox.was_admitted(&root.id, &runtime.id).await.unwrap());
    binding
        .directory
        .finish_activation(
            &binding.owner.fence,
            Utc::now(),
            ActorActivationFinish::Succeeded,
        )
        .await
        .unwrap();
    binding.disarm_abandonment();
}

#[tokio::test]
async fn owned_child_wait_expiry_survives_commit_before_activation() {
    use bamboo_domain::{ActorActivationFinish, ChildWaitExpiry, CommittedChildWaitExpiry};

    let (temp, store, inbox, coordinator, provider) = actor_exec_fixture().await;
    for case in [
        "committed",
        "changed_wait",
        "live_actor",
        "changed_attempt",
        "failed_actor",
        "cancelled_actor",
        "missing_receipt",
        "removed_receipt",
        "wrong_generation",
        "wrong_record",
        "wrong_envelope",
        "human_gate",
        "managed_work",
    ] {
        let now = Utc::now();
        let mut root = Session::new(format!("expired-wait-{case}"), "model");
        root.add_message(Message::user("prior transcript"));
        root.set_last_run_status("suspended");
        let mut runtime = AgentRuntimeState::new(format!("prior-{case}"));
        let wait = WaitingForChildrenState::for_children(
            vec![format!("still-running-{case}")],
            ChildWaitPolicy::All,
            now - chrono::Duration::hours(7),
        );
        runtime.status = AgentStatusState::Suspended;
        let mut recorded_wait = wait.clone();
        if case == "changed_wait" {
            recorded_wait.child_session_ids.push("new-child".into());
        }
        runtime.waiting_for_children = Some(recorded_wait.clone());
        write_runtime_state(&mut root, &runtime);
        root.metadata.insert(
            "runtime.suspend_reason".into(),
            "waiting_for_children".into(),
        );
        store.save_session(&root).await.unwrap();
        let child_id = format!("still-running-{case}");
        let mut child = Session::new_child(&child_id, &root.id, "model", "Child");
        child.set_last_run_status("running");
        store.save_session(&child).await.unwrap();
        let main = store
            .sessions_root_dir()
            .join(&root.id)
            .join("session.json");

        let mut prior = coordinator
            .agent
            .persistence()
            .bind_root_actor_execution(&root, &runtime.run_id)
            .await
            .unwrap()
            .unwrap();
        if case != "live_actor" {
            let finish = match case {
                "failed_actor" => ActorActivationFinish::Failed,
                "cancelled_actor" => ActorActivationFinish::Cancelled,
                _ => ActorActivationFinish::Succeeded,
            };
            prior
                .directory
                .finish_activation(&prior.owner.fence, Utc::now(), finish)
                .await
                .unwrap();
            prior.disarm_abandonment();
        }
        let observed = store
            .observe_root_child_wait_expiry_activation(&root, &runtime.run_id)
            .await
            .unwrap();
        assert_eq!(
            observed.as_deref(),
            match case {
                "live_actor" | "failed_actor" | "cancelled_actor" => None,
                _ => Some(prior.owner.fence.activation_id.as_str()),
            },
            "{case}",
        );
        let activation_id = if case == "changed_attempt" {
            "older-attempt".into()
        } else {
            prior.owner.fence.activation_id.clone()
        };
        let expiry =
            ChildWaitExpiry::observe(&root, &runtime, activation_id, wait.clone()).unwrap();
        let envelope = expiry.envelope(&root.id).unwrap();
        let mut receipt = if case == "missing_receipt" {
            bamboo_domain::SessionInboxReceipt {
                id: envelope.id.clone(),
                generation: 1,
            }
        } else {
            let receipt = inbox
                .deliver_runtime_for_target(&envelope, root.created_at)
                .await
                .unwrap();
            inbox
                .mark_activation_eligible(
                    &root.id,
                    receipt.generation,
                    SessionActivationPolicy::RespectSpecificWait,
                )
                .await
                .unwrap();
            receipt
        };
        assert!(inbox
            .inspect_wake_readiness(&root.id, now)
            .await
            .unwrap()
            .ready
            .is_none());
        assert!(
            inbox
                .claim_for_turn(&root.id, 1, None)
                .await
                .unwrap()
                .is_empty(),
            "{case}: a generic legacy prefix consumed the private expiry",
        );
        if case == "committed" {
            // Before the wait commits, retrying the producer recovers the same
            // receipt without granting generic wake or inserting history.
            let retry = inbox
                .deliver_runtime_for_target(&envelope, root.created_at)
                .await
                .unwrap();
            assert_eq!(retry, receipt);
            assert!(provider.0.lock().unwrap().is_empty());
        }
        if case == "wrong_generation" {
            receipt.generation += 1;
        }
        if matches!(case, "removed_receipt" | "wrong_record" | "wrong_envelope") {
            let mut rows = tokio::fs::read_dir(main.parent().unwrap().join("inbox/new"))
                .await
                .unwrap();
            let path = rows.next_entry().await.unwrap().unwrap().path();
            if case == "removed_receipt" {
                tokio::fs::remove_file(path).await.unwrap();
            } else {
                let bytes = tokio::fs::read(&path).await.unwrap();
                let mut row: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
                if case == "wrong_envelope" {
                    row["body"]["created_at"] = serde_json::json!(Utc::now());
                } else {
                    row["from"]["role"] = serde_json::json!("unexpected-transport-metadata");
                }
                tokio::fs::write(path, serde_json::to_vec(&row).unwrap())
                    .await
                    .unwrap();
            }
        }
        if matches!(case, "human_gate" | "managed_work") {
            let mut latest = store.load_session(&root.id).await.unwrap().unwrap();
            if case == "human_gate" {
                latest.pending_question = Some(bamboo_domain::PendingQuestion {
                    tool_call_id: "later-human-question".into(),
                    tool_name: "AskUserQuestion".into(),
                    question: "Choose whether to continue".into(),
                    options: vec!["Continue".into(), "Stop".into()],
                    allow_custom: false,
                    source: Default::default(),
                });
            } else {
                latest.metadata.insert(
                    crate::ticket_runtime::TICKET_DISPATCH_KEY.into(),
                    "{}".into(),
                );
            }
            store.save_session(&latest).await.unwrap();
        }
        let prior_history = tokio::fs::read(&main).await.unwrap();
        let committed = CommittedChildWaitExpiry::new(expiry, &receipt);
        assert_eq!(
            store
                .expire_root_child_wait_control_plane(
                    &root,
                    &committed,
                    inbox.as_ref(),
                    now,
                    Arc::new(|_| {})
                )
                .await
                .unwrap(),
            case == "committed",
            "{case}",
        );
        assert_eq!(tokio::fs::read(&main).await.unwrap(), prior_history);

        let reopened = bamboo_storage::SessionStoreV2::new(temp.path().into())
            .await
            .unwrap();
        let control = reopened
            .load_runtime_control_plane(&root.id)
            .await
            .unwrap()
            .unwrap();
        if case != "committed" {
            assert_eq!(
                read_runtime_state(&control).waiting_for_children,
                Some(recorded_wait)
            );
            assert!(CommittedChildWaitExpiry::from_session(&control).is_none());
            if case == "human_gate" {
                let full = reopened.load_session(&root.id).await.unwrap().unwrap();
                assert!(full.has_pending_question());
            }
            assert!(inbox
                .inspect_wake_readiness(&root.id, now)
                .await
                .unwrap()
                .ready
                .is_none());
            continue;
        }

        assert!(read_runtime_state(&control).waiting_for_children.is_none());
        assert_eq!(
            CommittedChildWaitExpiry::from_session(&control),
            Some(committed)
        );
        assert!(inbox
            .inspect_wake_readiness(&root.id, now)
            .await
            .unwrap()
            .ready
            .is_none());
        assert_eq!(provider.0.lock().unwrap().len(), 0);

        // A crash here would leave the exact receipt staged and the wait
        // retired. The ordinary watchdog pass performs the recovery.
        coordinator
            .sweep_one_suspended_session(&root.id, None)
            .await;
        wait_for_actor_startup_to_settle(&coordinator, &root.id).await;
        assert_eq!(provider.0.lock().unwrap().len(), 1);
        let saved = store.load_session(&root.id).await.unwrap().unwrap();
        assert_eq!(
            store
                .load_session(&child_id)
                .await
                .unwrap()
                .unwrap()
                .last_run_status()
                .as_deref(),
            Some("running"),
        );
        assert_eq!(
            saved
                .messages
                .iter()
                .filter(|message| {
                    bamboo_domain::is_matching_session_message(message, &envelope)
                })
                .count(),
            1
        );

        coordinator
            .sweep_one_suspended_session(&root.id, None)
            .await;
        wait_for_actor_startup_to_settle(&coordinator, &root.id).await;
        assert_eq!(provider.0.lock().unwrap().len(), 1);
    }
}
