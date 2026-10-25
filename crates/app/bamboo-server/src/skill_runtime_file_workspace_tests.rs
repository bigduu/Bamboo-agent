//! Ordinary file operations must not retarget an admitted Native Source.

use super::*;

const WRITE: &str = "native-external-file-write";
const READ: &str = "native-external-file-read";
const EDIT: &str = "native-external-file-edit";
const BEFORE: &str = "Temporary issue body before editing\n";
const AFTER: &str = "Temporary issue body after editing\n";

struct ExternalFileProvider {
    inner: Arc<SkillsProvider>,
    file: PathBuf,
    stage: AtomicU64,
}

impl ExternalFileProvider {
    fn call(&self, id: &str, name: &str, arguments: Value) -> LLMStream {
        Box::pin(futures::stream::iter([
            Ok(LLMChunk::ToolCalls(vec![ToolCall {
                id: id.into(),
                tool_type: "function".into(),
                function: FunctionCall {
                    name: name.into(),
                    arguments: arguments.to_string(),
                },
            }])),
            Ok(LLMChunk::Done),
        ]))
    }

    fn result<'a>(&self, messages: &'a [Message], id: &str) -> &'a Message {
        let result = messages
            .iter()
            .find(|message| message.tool_call_id.as_deref() == Some(id))
            .expect("the actual external file operation must reach its next provider round");
        assert_eq!(result.tool_success, Some(true), "{}", result.content);
        result
    }

    fn file_change_result(&self, messages: &[Message], id: &str, operation: &str) {
        let result = self.result(messages, id);
        let payload: Value = serde_json::from_str(&result.content).unwrap();
        assert_eq!(payload["operation"], operation);
        assert_eq!(
            payload["file_path"],
            bamboo_config::paths::path_to_display_string(&self.file)
        );
        assert_eq!(
            payload["workspace"],
            bamboo_config::paths::path_to_display_string(self.file.parent().unwrap()),
            "the ordinary tool still reports its file's parent as result data"
        );
    }
}

#[async_trait]
impl LLMProvider for ExternalFileProvider {
    async fn chat_stream(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        _: Option<u32>,
        _: &str,
    ) -> Result<LLMStream, LLMError> {
        match self.stage.load(Ordering::Acquire) {
            0 if self.inner.has_warm_page(messages) => {
                assert!(tools.iter().any(|tool| tool.function.name == "Write"));
                self.stage.store(1, Ordering::Release);
                return Ok(self.call(
                    WRITE,
                    "Write",
                    json!({"file_path":self.file,"content":BEFORE}),
                ));
            }
            1 => {
                self.file_change_result(messages, WRITE, "Write");
                self.stage.store(2, Ordering::Release);
                return Ok(self.call(READ, "Read", json!({"file_path":self.file})));
            }
            2 => {
                assert!(self.result(messages, READ).content.contains(BEFORE.trim()));
                assert!(tools.iter().any(|tool| tool.function.name == "Edit"));
                self.stage.store(3, Ordering::Release);
                return Ok(self.call(
                    EDIT,
                    "Edit",
                    json!({"file_path":self.file,"old_string":BEFORE,"new_string":AFTER}),
                ));
            }
            3 => {
                self.file_change_result(messages, EDIT, "Edit");
                self.stage.store(4, Ordering::Release);
            }
            _ => {}
        }
        // Continue the original warm Reader cursor after the file operations.
        // An implicit Source change must not be hidden by starting a new run.
        Ok(Box::pin(futures::stream::iter(
            self.inner.next(messages, tools).into_iter().map(Ok),
        )))
    }
}

#[actix_web::test]
async fn native_http_external_write_and_edit_preserve_workspace_and_warm_reader() {
    let _proof = HTTP_PROOF.lock().await;
    let home = tempfile::tempdir().unwrap();
    let root = home.path().canonicalize().unwrap();
    let workspace = root.join("project");
    let temporary = root.join("temporary-files");
    std::fs::create_dir_all(&workspace).unwrap();
    std::fs::create_dir_all(&temporary).unwrap();
    write_skill(&root, "native-proof", true);
    let provider = Arc::new(ExternalFileProvider {
        inner: provider(&workspace, 4096, false),
        file: temporary.join("issue-body.md"),
        stage: AtomicU64::new(0),
    });
    let state = state(&root, provider.clone()).await;
    let project = state
        .project_store
        .create_with_project_path(
            "Native file workspace regression",
            None,
            workspace.to_string_lossy(),
            vec![],
        )
        .unwrap();
    let id = format!("native-file-workspace-{}", uuid::Uuid::new_v4());
    let mut session = Session::new(&id, "test-model");
    session.title_generated = true;
    session.set_project_id_meta(project.id.to_string());
    session.set_workspace_path_meta(workspace.to_string_lossy());
    session.metadata.insert(
        bamboo_engine::project_context::WORKSPACE_SOURCE_METADATA_KEY.into(),
        "explicit".into(),
    );
    state.storage.save_session(&session).await.unwrap();
    state
        .workspace_resolver
        .publish_resolved_workspace(&id, workspace.clone(), "test_initial");

    // Authorize only this disposable file; retain the real Native permission
    // gate and the ordinary Write/Edit implementations.
    state
        .permission_checker
        .permission_config()
        .unwrap()
        .grant_typed_scoped_session_permission(
            &id,
            bamboo_tools::permission::PermissionType::WriteFile,
            bamboo_tools::permission::PermissionMatcher {
                id: "native-exact-temporary-file".into(),
                kind: bamboo_tools::permission::PermissionMatcherKind::ExactResource,
                value: provider.file.to_string_lossy().into_owned(),
            },
        )
        .unwrap();
    let (status, input) = http(
        &state,
        "/api/v1/chat",
        json!({"session_id":id,"message":"Prepare a temporary issue body and finish the proof"}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{input}");
    let before = state.storage.load_session(&id).await.unwrap().unwrap();
    let (status, response) = http(&state, &format!("/api/v1/execute/{id}"), json!({})).await;
    assert_eq!(status, StatusCode::ACCEPTED, "{response}");
    let terminal = done(&state, &id).await;
    let router = state.agent.activation_router().unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while router.current_run_id(&id).await.is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("actual Root must release before fixture cleanup");

    assert!(matches!(terminal, AgentStatus::Completed), "{terminal:?}");
    assert_eq!(provider.stage.load(Ordering::Acquire), 4);
    assert_eq!(std::fs::read_to_string(&provider.file).unwrap(), AFTER);
    let durable = state.storage.load_session(&id).await.unwrap().unwrap();
    assert_eq!(
        workspace_identity(&durable).unwrap(),
        Some(workspace.clone())
    );
    assert_eq!(durable.project_id_meta(), before.project_id_meta());
    for key in [
        "workspace_path",
        bamboo_engine::project_context::WORKSPACE_SOURCE_METADATA_KEY,
        bamboo_engine::project_context::WORKSPACE_BINDING_STATUS_METADATA_KEY,
    ] {
        assert_eq!(durable.metadata.get(key), before.metadata.get(key), "{key}");
    }
    assert_eq!(
        bamboo_agent_core::workspace_state::peek_workspace(&id),
        Some(workspace)
    );
    for call_id in [WRITE, READ, EDIT] {
        assert_eq!(
            durable
                .messages
                .iter()
                .filter(|message| {
                    message.role == Role::Tool
                        && message.tool_call_id.as_deref() == Some(call_id)
                        && message.tool_success == Some(true)
                })
                .count(),
            1,
            "each completed file operation must be retained once: {call_id}"
        );
    }
    let trace = provider.inner.trace.lock().unwrap();
    assert!(trace.failures.is_empty(), "{:?}", trace.failures);
    assert!(trace.main_pages > 1 && trace.task_completed);
    assert_eq!(trace.eof, vec![("native-proof".into(), "SKILL.md".into())]);
}
