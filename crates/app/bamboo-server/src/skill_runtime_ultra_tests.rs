use super::*;

struct UltraProvider {
    task: PathBuf,
    requests: Mutex<Vec<(Vec<Message>, BTreeSet<String>)>>,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}
#[async_trait]
impl LLMProvider for UltraProvider {
    async fn chat_stream(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        _: Option<u32>,
        _: &str,
    ) -> Result<LLMStream, LLMError> {
        let first = {
            let mut requests = self.requests.lock().unwrap();
            let first = requests.is_empty();
            requests.push((
                messages.to_vec(),
                tools
                    .iter()
                    .map(|tool| tool.function.name.clone())
                    .collect(),
            ));
            first
        };
        let chunks = if first {
            self.entered.notify_one();
            self.release.notified().await;
            vec![
                LLMChunk::ToolCalls(vec![ToolCall {
                    id: "ultra-real-read".into(),
                    tool_type: "function".into(),
                    function: FunctionCall {
                        name: "Read".into(),
                        arguments: json!({"file_path":self.task}).to_string(),
                    },
                }]),
                LLMChunk::Done,
            ]
        } else {
            let result = messages
                .iter()
                .find(|message| message.tool_call_id.as_deref() == Some("ultra-real-read"))
                .expect("actual allowed Root Read must return to the provider");
            assert_eq!(result.tool_success, Some(true), "{}", result.content);
            assert!(result.content.contains("ULTRA_VERIFIED_TASK"));
            vec![
                LLMChunk::Token("Ultra verified the local task".into()),
                LLMChunk::Done,
            ]
        };
        Ok(Box::pin(futures::stream::iter(chunks.into_iter().map(Ok))))
    }
}
fn ultra_provider(home: &Path) -> Arc<UltraProvider> {
    let task = home.join("ultra-task.txt");
    std::fs::write(&task, "ULTRA_VERIFIED_TASK").unwrap();
    Arc::new(UltraProvider {
        task,
        requests: Mutex::default(),
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
    })
}
async fn reserved_ultra(
    state: &web::Data<AppState>,
    provider: &UltraProvider,
    id: &str,
) -> Arc<NativeExecutor> {
    bindings()
        .lock()
        .unwrap()
        .insert(id.into(), std::sync::Weak::new());
    let mut request =
        json!({"session_id":id,"message":"Verify the local task","model":"test-model"});
    if state.storage.load_session(id).await.unwrap().is_none() {
        request["thinking_mode"] = "ultra".into();
    }
    let (status, body) = http(state, "/api/v1/chat", request).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let root = state.storage.load_session(id).await.unwrap().unwrap();
    assert!(registered_main(&root) && !ordinary_main(&root));
    assert!(root.root_tool_authority_revision > 0);
    let (status, body) = http(state, &format!("/api/v1/execute/{id}"), json!({})).await;
    assert_eq!(status, StatusCode::ACCEPTED, "{body}");
    tokio::time::timeout(
        std::time::Duration::from_secs(30),
        provider.entered.notified(),
    )
    .await
    .unwrap();
    let executor = bindings()
        .lock()
        .unwrap()
        .remove(id)
        .unwrap()
        .upgrade()
        .expect("actual Ultra execution must bind the finite Server host");
    assert_eq!(
        executor.run.reservation_id,
        body["run_id"].as_str().unwrap()
    );
    let current = executor.run.current.read().unwrap();
    assert!(current.execution_id.is_some() && current.current_input_id.is_some());
    drop(current);
    executor
}

#[actix_web::test]
async fn native_http_ultra_uses_finite_host_and_exact_registered_root_inventory() {
    let _proof = HTTP_PROOF.lock().await;
    let home = tempfile::tempdir().unwrap();
    write_skill(home.path(), "native-proof", true);
    let provider = ultra_provider(home.path());
    let state = state(home.path(), provider.clone()).await;
    let expected = state
        .tools_for(ToolSurface::Root)
        .list_tools()
        .into_iter()
        .map(|tool| tool.function.name)
        .filter(|name| bamboo_domain::orchestration_only_allows_execution_name(name))
        .collect::<BTreeSet<_>>();
    let mut root = Session::new("ultra-finite-inventory", "test-model");
    root.set_root_orchestration_only(true).unwrap();
    root.title_generated = true;
    for (key, value) in [
        ("skill_runtime_selected_skill_ids", "[\"native-proof\"]"),
        ("skill_runtime_selection_source", "explicit"),
        (
            "skill_runtime_last_load_summary",
            "OLD_SKILL_BYTES_MUST_STAY_INERT",
        ),
    ] {
        root.metadata.insert(key.into(), value.into());
    }
    state.storage.save_session(&root).await.unwrap();
    let executor = reserved_ultra(&state, &provider, "ultra-finite-inventory").await;
    let durable = state
        .storage
        .load_session("ultra-finite-inventory")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        durable.metadata["skill_runtime_selection_source"],
        "explicit"
    );
    assert!(executor.run.policy.orchestration_only);
    let actual = executor
        .list_tools()
        .into_iter()
        .map(|tool| tool.function.name)
        .collect::<BTreeSet<_>>();
    assert_eq!(actual, expected);
    assert_eq!(provider.requests.lock().unwrap()[0].1, expected);
    for name in [
        "skills_list",
        "skills_read",
        "load_skill",
        "read_skill_resource",
        "Write",
    ] {
        assert!(!executor.owns_exact_tool(name));
        assert!(executor.exact_tool_owner(name).is_none());
        let call = ToolCall {
            id: format!("ultra-denied-{name}"),
            tool_type: "function".into(),
            function: FunctionCall {
                name: name.into(),
                arguments: "{}".into(),
            },
        };
        let error = executor
            .execute_with_context(
                &call,
                ToolExecutionContext {
                    session_id: Some("ultra-finite-inventory"),
                    ..ToolExecutionContext::none(&call.id)
                },
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("outside its Root authority"));
    }
    provider.release.notify_one();
    assert!(matches!(
        done(&state, "ultra-finite-inventory").await,
        AgentStatus::Completed
    ));
    let requests = provider.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert!(requests.iter().all(|(messages, schemas)| {
        *schemas == expected
            && messages.iter().all(|message| {
                !message.content.contains("Using Skills")
                    && !message
                        .content
                        .contains("Required Explicit Workflow Activation")
                    && !message.content.contains("MAIN_EOF_native-proof")
                    && !message.content.contains("OLD_SKILL_BYTES_MUST_STAY_INERT")
            })
    }));
    assert!(!executor.run.live.load(Ordering::Acquire));
    let root = state
        .storage
        .load_session("ultra-finite-inventory")
        .await
        .unwrap()
        .unwrap();
    assert!(root
        .messages
        .iter()
        .any(|message| message.role == Role::Assistant
            && message.content.contains("Ultra verified the local task")));
}

#[actix_web::test]
async fn native_http_ultra_rejects_skill_selection_without_mutating_inbox() {
    let _proof = HTTP_PROOF.lock().await;
    let home = tempfile::tempdir().unwrap();
    write_skill(home.path(), "native-proof", true);
    let provider = ultra_provider(home.path());
    let state = state(home.path(), provider.clone()).await;
    let id = "ultra-reject-selected-skill";
    let mut root = Session::new(id, "test-model");
    root.set_root_orchestration_only(true).unwrap();
    root.title_generated = true;
    state.storage.save_session(&root).await.unwrap();
    let before = state.session_inbox.inspect(id).await.unwrap();
    let (status, body) = http(
        &state,
        "/api/v1/chat",
        json!({
            "session_id":id,"message":"must not become an accepted input",
            "workflow_selection":selection(&state,"native-proof").await,
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(
        body["error"]["code"],
        "root_orchestration_incompatible_mode"
    );
    let after = state.session_inbox.inspect(id).await.unwrap();
    assert_eq!(before, after);
    let current = state.storage.load_session(id).await.unwrap().unwrap();
    assert!(current.messages.is_empty());
    assert_eq!(
        current.root_tool_authority_revision,
        root.root_tool_authority_revision
    );
    assert!(provider.requests.lock().unwrap().is_empty());
}

#[actix_web::test]
async fn native_http_ultra_fences_durable_mode_changes_and_inerts_old_instruction_state() {
    let _proof = HTTP_PROOF.lock().await;
    let home = tempfile::tempdir().unwrap();
    write_skill(home.path(), "native-proof", true);
    let provider = ultra_provider(home.path());
    let state = state(home.path(), provider.clone()).await;
    let id = "ultra-legacy-and-revocation";
    let mut root = Session::new(id, "test-model");
    root.set_root_orchestration_only(true).unwrap();
    root.title_generated = true;
    for (key, value) in [
        ("skill_runtime_selected_skill_ids", "[\"native-proof\"]"),
        ("skill_runtime_selection_source", "explicit"),
        (
            "skill_runtime_last_load_summary",
            "OLD_SKILL_BYTES_MUST_STAY_INERT",
        ),
    ] {
        root.metadata.insert(key.into(), value.into());
    }
    state.storage.save_session(&root).await.unwrap();
    let executor = reserved_ultra(&state, &provider, id).await;
    let durable = state.storage.load_session(id).await.unwrap().unwrap();
    assert_eq!(
        durable.metadata["skill_runtime_selection_source"],
        "explicit"
    );
    assert!(provider.requests.lock().unwrap()[0]
        .0
        .iter()
        .all(
            |message| !message.content.contains("OLD_SKILL_BYTES_MUST_STAY_INERT")
                && !message
                    .content
                    .contains("Required Explicit Workflow Activation")
        ));
    let mut forged = durable.clone();
    forged.root_orchestration_only = false;
    assert!(
        executor
            .run
            .policy
            .check_execution_session(&forged)
            .is_err(),
        "same-revision mode changes cannot pass the captured host fence"
    );
    executor.run.check_durable_session().await.unwrap();
    let epoch = durable.root_mode_transition_epoch;
    let operation = format!(
        "/api/v1/sessions/{id}/root-mode-operations/{epoch}:{}",
        uuid::Uuid::new_v4()
    );
    let (status, body) = http(
        &state,
        &operation,
        json!({
            "birth_token":durable.root_mode_birth_token(),
            "expected_epoch":epoch,
            "thinking_mode":"standard",
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["status"], "committed");
    assert!(
        body["root_tool_authority_revision"].as_u64().unwrap() > executor.run.policy.root_revision
    );
    assert!(executor.run.check_durable_session().await.is_err());
    let (status, body) = http(&state, &format!("/api/v1/sessions/{id}/stop"), json!({})).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    provider.release.notify_one();
    assert!(matches!(done(&state, id).await, AgentStatus::Cancelled));
    assert!(executor.run.check_live().is_err());
}
