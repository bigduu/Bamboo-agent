//! A native Child asks its real direct parent through the installed SubAgent
//! tool. The provider is controlled; the tools, request authority, Inbox,
//! coordinator, local runner and canonical writers are the assembled server.
use super::AppState;
use async_trait::async_trait;
use bamboo_agent_core::tools::{FunctionCall, ToolCall, ToolSchema};
use bamboo_agent_core::{Message, Role, Session};
use bamboo_domain::{
    ActorActivationStatus, ActorDirectoryPort, ParentQuestion, ParentQuestionOutcome,
    ParentQuestionResolution, PendingQuestionSource, ProviderModelRef, SessionMessageEnvelope,
};
use bamboo_engine::execution::{AgentStatus, SpawnJob};
use bamboo_llm::{Config, LLMChunk, LLMError, LLMProvider, LLMStream, ProviderRegistry};
use serde_json::json;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::{mpsc, oneshot};

const PROVIDER: &str = "local-conversation-test";
const MODEL: &str = "local-conversation-model";
const CHILD_INPUT: &str = "Native child: ask the direct parent which output label to use.";
const QUESTION: &str = "Which output label should I use for this task?";
const ANSWER: &str = "Use the exact label bamboo-local-answer-1794.";
const CHILD_RESULT: &str = "Child completed with bamboo-local-answer-1794.";
const ROOT_RESULT: &str = "Parent answered its direct child's canonical question.";
const ASK_CALL: &str = "native-child-ask-parent";
const REPLY_CALL: &str = "native-root-reply-to-child";
const WAIT: Duration = Duration::from_secs(40);

#[derive(Debug)]
enum Reply {
    Tool(ToolCall),
    Complete(&'static str),
}

#[derive(Debug)]
struct Request {
    process_id: u32,
    messages: Vec<Message>,
    tools: Vec<ToolSchema>,
    reply: oneshot::Sender<Reply>,
}

impl Request {
    fn is_child(&self) -> bool {
        self.messages
            .iter()
            .any(|message| message.content == CHILD_INPUT)
    }

    fn subagent(self, call_id: &str, arguments: serde_json::Value) {
        assert!(
            self.tools
                .iter()
                .any(|tool| tool.function.name == "SubAgent"),
            "assembled execution must expose its real SubAgent tool"
        );
        self.reply
            .send(Reply::Tool(ToolCall {
                id: call_id.into(),
                tool_type: "function".into(),
                function: FunctionCall {
                    name: "SubAgent".into(),
                    arguments: arguments.to_string(),
                },
            }))
            .expect("live provider request");
    }

    fn complete(self, text: &'static str) {
        self.reply
            .send(Reply::Complete(text))
            .expect("live provider request");
    }
}

struct ConversationProvider {
    requests: mpsc::UnboundedSender<Request>,
}

#[async_trait]
impl LLMProvider for ConversationProvider {
    async fn chat_stream(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        _max_output_tokens: Option<u32>,
        model: &str,
    ) -> Result<LLMStream, LLMError> {
        assert_eq!(
            model, MODEL,
            "all reasoning stays on the injected local model"
        );
        let (reply, receive) = oneshot::channel();
        self.requests
            .send(Request {
                process_id: std::process::id(),
                messages: messages.to_vec(),
                tools: tools.to_vec(),
                reply,
            })
            .expect("test provider observer");
        let chunk = match receive.await.expect("test releases provider boundary") {
            Reply::Tool(call) => LLMChunk::ToolCalls(vec![call]),
            Reply::Complete(text) => LLMChunk::Token(text.into()),
        };
        Ok(Box::pin(futures::stream::iter([
            Ok(chunk),
            Ok(LLMChunk::Done),
        ])))
    }
}

async fn next_request(
    requests: &mut mpsc::UnboundedReceiver<Request>,
    state: &AppState,
    parent_id: &str,
    child_id: &str,
) -> Request {
    match tokio::time::timeout(WAIT, requests.recv()).await {
        Ok(Some(request)) => request,
        result => {
            let parent = state.storage.load_session(parent_id).await.unwrap();
            let child = state.storage.load_session(child_id).await.unwrap();
            panic!("real parent/child execution did not reach provider: {result:?}; parent={parent:?}; child={child:?}");
        }
    }
}

fn question_from_messages(messages: &[Message]) -> ParentQuestion {
    let questions = messages
        .iter()
        .filter_map(|message| {
            let value = message.metadata.as_ref()?.get("session_message")?.clone();
            let envelope: SessionMessageEnvelope = serde_json::from_value(value).ok()?;
            let question = ParentQuestion::from_envelope(&envelope)?;
            assert_eq!(message.id, question.id.as_str());
            Some(question)
        })
        .collect::<Vec<_>>();
    assert_eq!(
        questions.len(),
        1,
        "Root must receive exactly one authentic ParentQuestion"
    );
    questions.into_iter().next().unwrap()
}

async fn wait_until_finished(
    state: &AppState,
    parent_id: &str,
    child_id: &str,
    requests: &mut mpsc::UnboundedReceiver<Request>,
) {
    let result =
        tokio::time::timeout(WAIT, async {
            let mut completion_resume_seen = false;
            loop {
                if let Ok(request) = requests.try_recv() {
                    assert!(
                        !request.is_child(),
                        "answered Child must not be launched again"
                    );
                    assert!(
                        !completion_resume_seen,
                        "one Child terminal fact must resume Root at most once"
                    );
                    assert!(request.messages.iter().any(|message| {
                    message.content.contains(CHILD_RESULT)
                        && message.metadata.as_ref()
                            .and_then(|metadata| metadata.get("runtime_kind"))
                            .and_then(serde_json::Value::as_str)
                            == Some("child_completion_resume")
                }), "additional Root reasoning must consume the real canonical Child completion");
                    completion_resume_seen = true;
                    // Root finalization may legitimately wait for the answered
                    // Child to finish. Drive only its proven completion wake.
                    request.complete(ROOT_RESULT);
                }
                let parent = state
                    .storage
                    .load_session(parent_id)
                    .await
                    .unwrap()
                    .unwrap();
                let child = state.storage.load_session(child_id).await.unwrap().unwrap();
                let runners_done = {
                    let runners = state.agent_runners.read().await;
                    [parent_id, child_id].into_iter().all(|id| {
                        runners
                            .get(id)
                            .is_some_and(|runner| !matches!(runner.status, AgentStatus::Running))
                    })
                };
                if parent.last_run_status().as_deref() == Some("completed")
                    && child.last_run_status().as_deref() == Some("completed")
                    && runners_done
                    && state
                        .session_activation_router
                        .current_run_id(parent_id)
                        .await
                        .is_none()
                    && state
                        .session_activation_router
                        .current_run_id(child_id)
                        .await
                        .is_none()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
    if result.is_err() {
        for id in [parent_id, child_id] {
            let session = state.storage.load_session(id).await.unwrap().unwrap();
            eprintln!(
                "unfinished {id}: status={:?}, error={:?}, metadata={:?}, router={:?}, runner={:?}, actor={:?}, inbox={:?}, messages={:?}",
                session.last_run_status(), session.last_run_error(), session.metadata,
                state.session_activation_router.current_run_id(id).await,
                state.agent_runners.read().await.get(id).map(|runner| &runner.status),
                state.session_store.inspect_actor(id).await,
                state.session_inbox.inspect(id).await,
                session.messages.iter().filter(|message| message.role == Role::Tool || message.role == Role::Assistant).collect::<Vec<_>>()
            );
        }
        panic!("both real executions must finish their durable publication and router handoff");
    }
}

#[tokio::test]
async fn native_child_real_subagent_question_parent_reply_and_inbox_resume_without_broker() {
    let home = tempfile::tempdir().unwrap();
    let (send, mut requests) = mpsc::unbounded_channel();
    let provider = Arc::new(ConversationProvider { requests: send });
    let mut config = Config::from_data_dir(Some(home.path().to_path_buf()));
    config.provider = PROVIDER.into();
    config.subagents_mut().max_concurrent = Some(2);
    let state = AppState::new_with_provider(home.path().to_path_buf(), config, provider.clone())
        .await
        .expect("real native AppState assembly");
    // The supported shared-registry reload seam preserves the registry Arc
    // already captured by the native runner and both real coordinators.
    state.provider_registry.replace_with(ProviderRegistry::new(
        HashMap::from([(PROVIDER.into(), provider as Arc<dyn LLMProvider>)]),
        PROVIDER.into(),
    ));
    assert!(state.embedded_broker.is_none());
    assert!(state.health_monitor.is_none());
    assert!(state
        .process_registry
        .get_running_processes()
        .await
        .unwrap()
        .is_empty());

    let project = state
        .project_store
        .create("Native direct-parent conversation", None)
        .unwrap();
    let workspace = home.path().join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let workspace = std::fs::canonicalize(workspace).unwrap();
    let parent_id = format!("local-conversation-root-{}", uuid::Uuid::new_v4());
    let child_id = format!("{parent_id}-child");
    let mut parent = Session::new(&parent_id, MODEL);
    parent.model_ref = Some(ProviderModelRef::new(PROVIDER, MODEL));
    parent.set_provider_name(PROVIDER);
    parent.set_project_id_meta(project.id.to_string());
    parent.workspace = Some(workspace.to_string_lossy().into());
    parent.set_workspace_path_meta(workspace.to_string_lossy());
    parent.title = "Native direct-parent conversation".into();
    parent.title_generated = true;
    parent.add_message(Message::system(
        "Answer your direct child's question using its exact request id.",
    ));
    parent.add_message(Message::user(
        "Coordinate the existing child and give its requested label.",
    ));
    state.storage.save_session(&parent).await.unwrap();
    let mut child = Session::new_child_of(&child_id, &parent, MODEL, "Native question child");
    child.model_ref = Some(ProviderModelRef::new(PROVIDER, MODEL));
    child.set_provider_name(PROVIDER);
    child.set_project_id_meta(project.id.to_string());
    child.workspace = parent.workspace.clone();
    child.set_workspace_path_meta(workspace.to_string_lossy());
    child.set_last_run_status("queued");
    child.add_message(Message::system(
        "Ask your direct parent one clarification through SubAgent, then finish using its answer.",
    ));
    child.add_message(Message::user(CHILD_INPUT));
    state.storage.save_session(&child).await.unwrap();

    state
        .spawn_scheduler
        .enqueue_announced(
            SpawnJob {
                parent_session_id: parent_id.clone(),
                child_session_id: child_id.clone(),
                model: MODEL.into(),
                disabled_tools: None,
            },
            Some("Native question child".into()),
        )
        .await
        .unwrap();
    let ask = next_request(&mut requests, &state, &parent_id, &child_id).await;
    assert_eq!(ask.process_id, std::process::id());
    assert!(ask.is_child());
    ask.subagent(ASK_CALL, json!({"intent":"ask_parent", "message":QUESTION}));

    // No forged request, answer, completion source or stop marker is inserted.
    // The installed outbox reconciler activates the Root using durable input
    // emitted by the actual Child tool and its canonical writer.
    let root_reply = next_request(&mut requests, &state, &parent_id, &child_id).await;
    assert_eq!(root_reply.process_id, std::process::id());
    assert!(
        !root_reply.is_child(),
        "Child continued instead of suspending for its parent; tool results: {:?}",
        root_reply
            .messages
            .iter()
            .filter(|message| message.role == Role::Tool)
            .collect::<Vec<_>>()
    );
    let question = question_from_messages(&root_reply.messages);
    assert_eq!(question.question, QUESTION);
    assert_eq!(question.tool_call_id, ASK_CALL);
    assert_eq!(question.tool_name, "SubAgent");
    assert_eq!(question.child.session_id, child_id);
    assert_eq!(question.child.created_at, child.created_at);
    assert_eq!(question.parent.session_id, parent_id);
    assert_eq!(question.parent.created_at, parent.created_at);
    let waiting = state
        .storage
        .load_session(&child_id)
        .await
        .unwrap()
        .unwrap();
    let current_parent = state
        .storage
        .load_session(&parent_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        waiting.pending_question.as_ref().unwrap().source,
        PendingQuestionSource::DirectParent
    );
    assert_eq!(
        ParentQuestion::for_pending(&current_parent, &waiting),
        Some(question.clone())
    );
    assert_eq!(waiting.last_run_status().as_deref(), Some("suspended"));
    assert_eq!(
        waiting
            .metadata
            .get("runtime.local_task.state")
            .map(String::as_str),
        Some("stopped")
    );
    assert!(state
        .session_inbox
        .was_admitted(&parent_id, &question.id)
        .await
        .unwrap());
    // The canonical compact reply schema deliberately omits target and role:
    // reply_to already binds the exact Child, parent, birth and question.
    root_reply.subagent(
        REPLY_CALL,
        json!({"intent":"chat", "reply_to":question.id, "message":ANSWER}),
    );

    let mut child_resumed = false;
    let mut root_finished = false;
    for _ in 0..2 {
        let request = next_request(&mut requests, &state, &parent_id, &child_id).await;
        assert_eq!(request.process_id, std::process::id());
        if request.is_child() {
            assert!(
                !child_resumed,
                "answer must launch exactly one Child successor"
            );
            child_resumed = true;
            assert_eq!(
                request
                    .messages
                    .iter()
                    .filter(|message| {
                        message.role == Role::Tool
                            && message.tool_call_id.as_deref() == Some(ASK_CALL)
                            && message.content.contains(ANSWER)
                    })
                    .count(),
                1,
                "Child must reason over the one canonically answered tool result"
            );
            request.complete(CHILD_RESULT);
        } else {
            assert!(
                !root_finished,
                "Root reply needs only one follow-up provider round"
            );
            root_finished = true;
            let tool_result = request
                .messages
                .iter()
                .find(|message| {
                    message.role == Role::Tool
                        && message.tool_call_id.as_deref() == Some(REPLY_CALL)
                })
                .expect("real Root reply tool result");
            assert_ne!(
                tool_result.tool_success,
                Some(false),
                "{}",
                tool_result.content
            );
            assert!(
                tool_result.content.contains("answer_recorded"),
                "{}",
                tool_result.content
            );
            request.complete(ROOT_RESULT);
        }
    }
    assert!(child_resumed && root_finished);
    wait_until_finished(&state, &parent_id, &child_id, &mut requests).await;

    let saved_parent = state
        .storage
        .load_session(&parent_id)
        .await
        .unwrap()
        .unwrap();
    let saved_child = state
        .storage
        .load_session(&child_id)
        .await
        .unwrap()
        .unwrap();
    let resolution =
        ParentQuestionResolution::from_child(&saved_parent, &saved_child, &question.id)
            .expect("real reply must leave a canonical direct-parent resolution");
    assert_eq!(resolution.request, question);
    assert_eq!(
        resolution.outcome,
        ParentQuestionOutcome::Answer {
            text: ANSWER.into()
        }
    );
    assert!(saved_child.pending_question.is_none());
    assert!(!saved_child.metadata.contains_key("runtime.suspend_reason"));
    assert!(!saved_child
        .metadata
        .contains_key("clarification_resume_pending"));
    assert!(!saved_child
        .metadata
        .contains_key("execute.startup_handoff_at"));
    assert_eq!(
        saved_child
            .messages
            .iter()
            .filter(|message| message.content == CHILD_RESULT)
            .count(),
        1
    );
    assert_eq!(
        saved_parent
            .messages
            .iter()
            .filter(|message| message.id == question.id.as_str())
            .count(),
        1
    );
    assert_eq!(
        saved_parent
            .messages
            .iter()
            .filter(|message| message.content == ROOT_RESULT)
            .count(),
        1
    );
    let wake = resolution.answer_wake_envelope().unwrap();
    assert!(state
        .session_inbox
        .was_admitted(&child_id, &wake.id)
        .await
        .unwrap());
    assert_eq!(
        saved_child
            .messages
            .iter()
            .filter(|message| message.id == wake.id.as_str())
            .count(),
        1
    );
    for id in [&parent_id, &child_id] {
        let inbox = state.session_inbox.inspect(id).await.unwrap();
        assert_eq!(inbox.pending + inbox.claimed, 0);
    }
    let actor = state.session_store.inspect_actor(&child_id).await.unwrap();
    assert_eq!(actor.actor.current_attempt, 2);
    let activation = actor.activation.unwrap();
    assert_eq!(activation.status, ActorActivationStatus::Succeeded);
    assert!(activation.finished_at.is_some());
    assert!(activation.placement_ref.is_none());
    assert_eq!(
        saved_child
            .metadata
            .get("runtime.local_task.state")
            .map(String::as_str),
        Some("stopped")
    );
    uuid::Uuid::parse_str(&saved_child.metadata["runtime.local_task.instance"]).unwrap();
    let source: bamboo_engine::execution::ChildCompletionSource =
        serde_json::from_str(&saved_child.metadata["runtime.child_completion_source_v1"])
            .expect("real terminal checkpoint must publish a source, not a pending envelope");
    assert_eq!(source.activation_run_id, activation.run_id);
    assert_eq!(source.child_created_at, child.created_at);
    assert_eq!(source.status, "completed");
    assert!(matches!(
        requests.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    assert!(!home.path().join("broker").exists());
    assert!(!home.path().join("broker.json").exists());
    assert!(!home.path().join("subagents").exists());
    assert!(state
        .process_registry
        .get_running_processes()
        .await
        .unwrap()
        .is_empty());
    tokio::time::timeout(WAIT, state.shutdown())
        .await
        .expect("native question runtime shutdown");
}

async fn installed_root_subagent(
    state: &AppState,
    parent_id: &str,
    call_id: &str,
    args: serde_json::Value,
) -> serde_json::Value {
    use bamboo_agent_core::tools::{ToolExecutionContext, ToolOutcome};
    let call = ToolCall {
        id: call_id.into(),
        tool_type: "function".into(),
        function: FunctionCall {
            name: "SubAgent".into(),
            arguments: args.to_string(),
        },
    };
    let mut ctx = ToolExecutionContext::none(&call.id);
    ctx.session_id = Some(parent_id);
    ctx.root_session_id = Some(parent_id);
    let diagnostic = bamboo_server_tools::sub_agent::test_diagnostics::install(parent_id, call_id);
    let outcome = tokio::time::timeout(
        WAIT,
        state
            .tools_for(crate::tools::ToolSurface::Root)
            .execute_with_context_outcome(&call, ctx),
    )
    .await
    .expect("installed SubAgent call must settle")
    .unwrap_or_else(|error| {
        panic!(
            "installed SubAgent call failed: {error}; private test diagnostic: {:?}",
            diagnostic.take()
        )
    });
    let ToolOutcome::Completed(result) = outcome else {
        panic!("compact SubAgent must return its canonical synchronous receipt");
    };
    assert!(result.success, "{}", result.result);
    serde_json::from_str(&result.result).expect("compact SubAgent receipt")
}

#[tokio::test]
async fn native_subagent_installed_root_surface_creates_inspects_and_cancels_real_child() {
    let home = tempfile::tempdir().unwrap();
    let (send, mut requests) = mpsc::unbounded_channel();
    let provider = Arc::new(ConversationProvider { requests: send });
    let mut config = Config::from_data_dir(Some(home.path().to_path_buf()));
    config.provider = PROVIDER.into();
    config.subagents_mut().max_concurrent = Some(1);
    let state = AppState::new_with_provider(home.path().to_path_buf(), config, provider.clone())
        .await
        .unwrap();
    state.provider_registry.replace_with(ProviderRegistry::new(
        HashMap::from([(PROVIDER.into(), provider as Arc<dyn LLMProvider>)]),
        PROVIDER.into(),
    ));
    let project = state
        .project_store
        .create("Native SubAgent entry", None)
        .unwrap();
    let workspace = home.path().join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let workspace = std::fs::canonicalize(workspace).unwrap();
    let parent_id = format!("local-entry-root-{}", uuid::Uuid::new_v4());
    let mut parent = Session::new(&parent_id, MODEL);
    parent.model_ref = Some(ProviderModelRef::new(PROVIDER, MODEL));
    parent.set_provider_name(PROVIDER);
    parent.set_project_id_meta(project.id.to_string());
    parent.workspace = Some(workspace.to_string_lossy().into());
    parent.set_workspace_path_meta(workspace.to_string_lossy());
    parent.title_generated = true;
    parent.add_message(Message::user("Create, inspect, then cancel one child."));
    state.storage.save_session(&parent).await.unwrap();
    // This fixture explicitly authorizes exactly its Root lifecycle actions.
    // Inspect and direct-parent question/reply are not create permissions, and
    // neither permissions nor bypass flags are widened for the native Child.
    use bamboo_tools::permission::{PermissionMatcher, PermissionMatcherKind, PermissionType};
    for (kind, resource) in [
        (PermissionType::ExecuteCommand, "SubAgent create"),
        (PermissionType::WriteFile, "SubAgent cancel"),
    ] {
        state
            .permission_checker
            .permission_config()
            .unwrap()
            .grant_typed_scoped_session_permission(
                &parent_id,
                kind,
                PermissionMatcher {
                    id: format!("native-entry-{resource}"),
                    kind: PermissionMatcherKind::ExactResource,
                    value: resource.into(),
                },
            )
            .unwrap();
    }

    // Only the Root is a fixture. The installed facade, canonical adapter,
    // admission scheduler and native backend own the Child's entire lifecycle.
    let created = installed_root_subagent(
        &state,
        &parent_id,
        "native-entry-create",
        json!({"intent":"chat", "message":CHILD_INPUT}),
    )
    .await;
    let child_id = created["actor_id"].as_str().unwrap().to_owned();
    assert_eq!(created["observed_status"], "running_in_background");
    let child_call = next_request(&mut requests, &state, &parent_id, &child_id).await;
    assert_eq!(child_call.process_id, std::process::id());
    assert!(
        child_call
            .messages
            .iter()
            .any(|message| message.content.contains(CHILD_INPUT)),
        "real SubAgent creation must preserve the assignment in its rendered Child prompt"
    );
    let running = state
        .storage
        .load_session(&child_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(running.kind, bamboo_domain::SessionKind::Child);
    assert_eq!(
        running.parent_session_id.as_deref(),
        Some(parent_id.as_str())
    );
    assert_eq!(running.parent_created_at, Some(parent.created_at));
    assert_eq!(running.root_session_id, parent_id);
    assert_eq!(running.project_id_meta(), parent.project_id_meta());
    assert_eq!(running.workspace_path_meta(), parent.workspace_path_meta());
    assert_eq!(running.metadata["assignment_prompt"], CHILD_INPUT);
    assert_eq!(running.last_run_status().as_deref(), Some("running"));
    let active = state.session_store.inspect_actor(&child_id).await.unwrap();
    assert_eq!(
        active.activation.unwrap().status,
        ActorActivationStatus::Running
    );

    let inspected = installed_root_subagent(
        &state,
        &parent_id,
        "native-entry-inspect",
        json!({"intent":"inspect", "target":child_id, "message":"overview"}),
    )
    .await;
    assert_eq!(inspected["actor_id"], child_id);
    assert_eq!(inspected["observed_status"], "running");
    let cancelled = installed_root_subagent(
        &state,
        &parent_id,
        "native-entry-cancel",
        json!({"intent":"control", "target":child_id, "message":"cancel"}),
    )
    .await;
    assert_eq!(cancelled["actor_id"], child_id);
    assert_eq!(cancelled["observed_status"], "cancelled");
    tokio::time::timeout(WAIT, async {
        loop {
            let saved = state
                .storage
                .load_session(&child_id)
                .await
                .unwrap()
                .unwrap();
            if saved.last_run_status().as_deref() == Some("cancelled")
                && state
                    .session_activation_router
                    .current_run_id(&child_id)
                    .await
                    .is_none()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("real cancellation must finish canonical publication and router release");
    assert!(
        child_call.reply.is_closed(),
        "cancel must drop the active provider future"
    );
    drop(child_call);
    let stopped = state
        .storage
        .load_session(&child_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stopped.created_at, running.created_at);
    assert_eq!(stopped.metadata["runtime.local_task.state"], "stopped");
    uuid::Uuid::parse_str(&stopped.metadata["runtime.local_task.instance"]).unwrap();
    let actor = state.session_store.inspect_actor(&child_id).await.unwrap();
    let activation = actor.activation.unwrap();
    assert_eq!(activation.status, ActorActivationStatus::Cancelled);
    assert!(activation.finished_at.is_some());
    assert!(activation.placement_ref.is_none());
    let source: bamboo_engine::execution::ChildCompletionSource =
        serde_json::from_str(&stopped.metadata["runtime.child_completion_source_v1"]).unwrap();
    assert_eq!(source.activation_run_id, activation.run_id);
    assert_eq!(source.child_created_at, stopped.created_at);
    assert_eq!(source.status, "cancelled");
    assert!(matches!(
        requests.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    assert!(state.embedded_broker.is_none());
    assert!(state.health_monitor.is_none());
    assert!(state
        .process_registry
        .get_running_processes()
        .await
        .unwrap()
        .is_empty());
    assert!(!home.path().join("broker").exists());
    assert!(!home.path().join("broker.json").exists());
    assert!(!home.path().join("subagents").exists());
    tokio::time::timeout(WAIT, state.shutdown()).await.unwrap();
}

async fn settled_native_root(state: &AppState, parent_id: &str, status: &str) -> Session {
    tokio::time::timeout(WAIT, async {
        loop {
            let parent = state
                .storage
                .load_session(parent_id)
                .await
                .unwrap()
                .unwrap();
            if parent.last_run_status().as_deref() == Some(status)
                && state
                    .session_activation_router
                    .current_run_id(parent_id)
                    .await
                    .is_none()
            {
                return parent;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("real Root must durably settle and release its activation")
}

fn native_child_outcomes(messages: &[Message], child_id: &str) -> Vec<SessionMessageEnvelope> {
    messages
        .iter()
        .filter_map(|message| {
            let marker = message.metadata.as_ref()?.get("session_message")?;
            let envelope: SessionMessageEnvelope = serde_json::from_value(marker.clone()).ok()?;
            if matches!(&envelope.body, bamboo_domain::SessionMessageBody::ChildOutcome(outcome)
            if outcome.child_session_id == child_id)
            {
                assert!(bamboo_domain::is_matching_session_message(
                    message, &envelope
                ));
                Some(envelope)
            } else {
                None
            }
        })
        .collect()
}

#[tokio::test]
async fn native_fast_child_wakes_root_for_real_feedback_and_preserves_all_wait_for_slow_child() {
    const FAST: &str = "Native incremental fast assignment.";
    const SLOW: &str = "Native incremental slow assignment.";
    const FAST_RESULT: &str = "Native fast child has independently finished.";
    const SLOW_RESULT: &str = "Native slow child finished after the real parent feedback.";
    const FEEDBACK: &str = "Use the fast child's result in your remaining work.";
    const FEEDBACK_CALL: &str = "native-incremental-root-feedback";
    let home = tempfile::tempdir().unwrap();
    let (send, mut requests) = mpsc::unbounded_channel();
    let provider = Arc::new(ConversationProvider { requests: send });
    let mut config = Config::from_data_dir(Some(home.path().to_path_buf()));
    config.provider = PROVIDER.into();
    config.subagents_mut().max_concurrent = Some(2);
    let state = AppState::new_with_provider(home.path().to_path_buf(), config, provider.clone())
        .await
        .unwrap();
    state.provider_registry.replace_with(ProviderRegistry::new(
        HashMap::from([(PROVIDER.into(), provider as Arc<dyn LLMProvider>)]),
        PROVIDER.into(),
    ));
    let project = state
        .project_store
        .create("Native incremental All wait", None)
        .unwrap();
    let workspace = home.path().join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let workspace = std::fs::canonicalize(workspace).unwrap();
    let parent_id = format!("native-incremental-root-{}", uuid::Uuid::new_v4());
    let mut parent = Session::new(&parent_id, MODEL);
    parent.model_ref = Some(ProviderModelRef::new(PROVIDER, MODEL));
    parent.set_provider_name(PROVIDER);
    parent.set_project_id_meta(project.id.to_string());
    parent.workspace = Some(workspace.to_string_lossy().into());
    parent.set_workspace_path_meta(workspace.to_string_lossy());
    parent.title_generated = true;
    parent.add_message(Message::user(
        "Coordinate both children and preserve unfinished work.",
    ));
    state.storage.save_session(&parent).await.unwrap();
    use bamboo_tools::permission::{PermissionMatcher, PermissionMatcherKind, PermissionType};
    for resource in ["SubAgent create", "SubAgent send_message"] {
        state
            .permission_checker
            .permission_config()
            .unwrap()
            .grant_typed_scoped_session_permission(
                &parent_id,
                PermissionType::ExecuteCommand,
                PermissionMatcher {
                    id: format!("native-incremental-{resource}"),
                    kind: PermissionMatcherKind::ExactResource,
                    value: resource.into(),
                },
            )
            .unwrap();
    }
    // Only Root is seeded. Both Children, terminal seals, outcomes and ACKs
    // come from the installed facade, native backend and canonical writers.
    let fast = installed_root_subagent(
        &state,
        &parent_id,
        "native-incremental-create-fast",
        json!({"intent":"chat","message":FAST}),
    )
    .await;
    let slow = installed_root_subagent(
        &state,
        &parent_id,
        "native-incremental-create-slow",
        json!({"intent":"chat","message":SLOW}),
    )
    .await;
    let fast_id = fast["actor_id"].as_str().unwrap();
    let slow_id = slow["actor_id"].as_str().unwrap();
    let mut fast_call = None;
    let mut slow_call = None;
    for _ in 0..2 {
        let request = next_request(&mut requests, &state, &parent_id, slow_id).await;
        assert_eq!(request.process_id, std::process::id());
        if request
            .messages
            .iter()
            .any(|message| message.content.contains(FAST))
        {
            assert!(fast_call.replace(request).is_none());
        } else {
            assert!(request
                .messages
                .iter()
                .any(|message| message.content.contains(SLOW)));
            assert!(slow_call.replace(request).is_none());
        }
    }
    let fast_call = fast_call.unwrap();
    let slow_call = slow_call.unwrap();
    state
        .session_messenger
        .send(SessionMessageEnvelope::user_input(
            &parent_id,
            "Wait for both running children.",
        ))
        .await
        .unwrap();
    let root = next_request(&mut requests, &state, &parent_id, slow_id).await;
    assert_eq!(root.process_id, std::process::id());
    root.complete("Both native children are still working.");
    let waiting = settled_native_root(&state, &parent_id, "suspended").await;
    let original_wait = waiting
        .agent_runtime_state
        .unwrap()
        .waiting_for_children
        .unwrap();
    assert_eq!(original_wait.wait_for, bamboo_domain::ChildWaitPolicy::All);
    assert_eq!(original_wait.child_session_ids.len(), 2);
    assert!(original_wait
        .child_session_ids
        .contains(&fast_id.to_owned()));
    assert!(original_wait
        .child_session_ids
        .contains(&slow_id.to_owned()));

    fast_call.complete(FAST_RESULT);
    let root = next_request(&mut requests, &state, &parent_id, slow_id).await;
    assert_eq!(root.process_id, std::process::id());
    let fast_outcomes = native_child_outcomes(&root.messages, fast_id);
    assert_eq!(fast_outcomes.len(), 1);
    assert!(root
        .messages
        .iter()
        .any(|message| message.content.contains(FAST_RESULT)));
    assert!(native_child_outcomes(&root.messages, slow_id).is_empty());
    assert!(state
        .session_inbox
        .was_admitted(&parent_id, &fast_outcomes[0].id)
        .await
        .unwrap());
    assert!(
        !slow_call.reply.is_closed(),
        "slow execution must still be inside its real provider call"
    );
    root.subagent(
        FEEDBACK_CALL,
        json!({"intent":"chat","target":slow_id,"message":FEEDBACK}),
    );
    let root = next_request(&mut requests, &state, &parent_id, slow_id).await;
    let feedback = root
        .messages
        .iter()
        .find(|message| {
            message.role == Role::Tool && message.tool_call_id.as_deref() == Some(FEEDBACK_CALL)
        })
        .unwrap();
    assert_eq!(feedback.tool_success, Some(true), "{}", feedback.content);
    assert!(feedback.content.contains(slow_id), "{}", feedback.content);
    root.complete("Fast result processed; the slow child must continue.");
    let suspended = settled_native_root(&state, &parent_id, "suspended").await;
    assert_eq!(suspended.agent_runtime_state.unwrap().waiting_for_children.as_ref(), Some(&original_wait), "incremental reasoning must preserve the exact policy, deadline, timestamp and originating call");
    assert!(!slow_call.reply.is_closed());
    let slow_actor = state.session_store.inspect_actor(slow_id).await.unwrap();
    assert_eq!(
        slow_actor.activation.unwrap().status,
        ActorActivationStatus::Running
    );
    assert!(matches!(
        requests.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));

    slow_call.complete("Slow child reaches its next feedback boundary.");
    let slow_continued = next_request(&mut requests, &state, &parent_id, slow_id).await;
    assert_eq!(slow_continued.process_id, std::process::id());
    assert!(slow_continued
        .messages
        .iter()
        .any(|message| message.content.contains(SLOW)));
    assert_eq!(
        slow_continued
            .messages
            .iter()
            .filter(|message| message.content == FEEDBACK)
            .count(),
        1
    );
    slow_continued.complete(SLOW_RESULT);
    let root = next_request(&mut requests, &state, &parent_id, slow_id).await;
    assert_eq!(native_child_outcomes(&root.messages, fast_id).len(), 1);
    assert_eq!(native_child_outcomes(&root.messages, slow_id).len(), 1);
    assert!(root
        .messages
        .iter()
        .any(|message| message.content.contains(SLOW_RESULT)));
    root.complete("Both native children were handled incrementally.");
    let completed = settled_native_root(&state, &parent_id, "completed").await;
    assert!(completed
        .agent_runtime_state
        .as_ref()
        .unwrap()
        .waiting_for_children
        .is_none());
    for id in [fast_id, slow_id] {
        let outcomes = native_child_outcomes(&completed.messages, id);
        assert_eq!(outcomes.len(), 1);
        assert!(state
            .session_inbox
            .was_admitted(&parent_id, &outcomes[0].id)
            .await
            .unwrap());
        let saved = state.storage.load_session(id).await.unwrap().unwrap();
        assert_eq!(saved.last_run_status().as_deref(), Some("completed"));
        assert_eq!(saved.metadata["runtime.local_task.state"], "stopped");
        uuid::Uuid::parse_str(&saved.metadata["runtime.local_task.instance"]).unwrap();
        let activation = state
            .session_store
            .inspect_actor(id)
            .await
            .unwrap()
            .activation
            .unwrap();
        assert_eq!(activation.status, ActorActivationStatus::Succeeded);
        assert!(activation.placement_ref.is_none());
        let source: bamboo_engine::execution::ChildCompletionSource =
            serde_json::from_str(&saved.metadata["runtime.child_completion_source_v1"]).unwrap();
        assert_eq!(source.activation_run_id, activation.run_id);
        assert_eq!(source.child_created_at, saved.created_at);
    }
    for id in [parent_id.as_str(), fast_id, slow_id] {
        let backlog = state.session_inbox.inspect(id).await.unwrap();
        assert_eq!(backlog.pending + backlog.claimed, 0);
    }
    assert!(matches!(
        requests.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    assert!(state.embedded_broker.is_none());
    assert!(state.health_monitor.is_none());
    assert!(state
        .process_registry
        .get_running_processes()
        .await
        .unwrap()
        .is_empty());
    for name in ["broker", "broker.json", "subagents"] {
        assert!(!home.path().join(name).exists());
    }
    tokio::time::timeout(WAIT, state.shutdown()).await.unwrap();
}
