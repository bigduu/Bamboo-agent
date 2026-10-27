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
