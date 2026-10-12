//! Actual owned Inbox/error/reload boundaries feeding the existing resume helper.
//! This fixture exercises startup logging, not the automatic router's dispatch E2E.

use super::initial_user_message_for_session;
use crate::runtime::config::{AgentLoopConfig, PromptMemoryFlags};
use bamboo_agent_core::tools::{ToolCall, ToolError, ToolExecutor, ToolResult, ToolSchema};
use bamboo_domain::{
    ActorActivationFinish, ActorSession, Message, Role, RootActorExecutionBinding, Session,
    SessionInboxLimits, SessionInboxPort, SessionMessageEnvelope, SessionMessageId,
    SessionMessageKind, SessionMessageSource, Storage,
};
use bamboo_llm::provider::{LLMError, LLMProvider, LLMStream};
use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Default)]
struct StartupCapture(Arc<Mutex<Vec<String>>>);

impl tracing::Subscriber for StartupCapture {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        *metadata.level() <= tracing::Level::DEBUG
    }

    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }

    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}

    fn event(&self, event: &tracing::Event<'_>) {
        struct Visitor(String);
        impl tracing::field::Visit for Visitor {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                self.0.push_str(&format!("{}={value:?};", field.name()));
            }
        }
        let mut visitor = Visitor(String::new());
        event.record(&mut visitor);
        if visitor.0.contains("Starting agent loop") || visitor.0.contains("agent_loop_start") {
            self.0.lock().unwrap().push(visitor.0);
        }
    }
}

#[derive(Default)]
struct FailBeforeOutput(Mutex<Vec<Vec<Message>>>);

#[async_trait::async_trait]
impl LLMProvider for FailBeforeOutput {
    async fn chat_stream(
        &self,
        messages: &[Message],
        _: &[ToolSchema],
        _: Option<u32>,
        _: &str,
    ) -> bamboo_llm::provider::Result<LLMStream> {
        self.0.lock().unwrap().push(messages.to_vec());
        Err(LLMError::Auth("synthetic failure before output".into()))
    }
}

struct NoTools;

#[async_trait::async_trait]
impl ToolExecutor for NoTools {
    async fn execute(&self, _: &ToolCall) -> Result<ToolResult, ToolError> {
        panic!("pre-output provider failure cannot invoke a tool")
    }

    fn list_tools(&self) -> Vec<ToolSchema> {
        Vec::new()
    }
}

async fn finish(mut binding: RootActorExecutionBinding) {
    binding
        .directory
        .finish_activation(
            &binding.owner.fence,
            chrono::Utc::now(),
            ActorActivationFinish::Failed,
        )
        .await
        .unwrap();
    binding.disarm_abandonment();
}

async fn failing_loop(
    session: &mut Session,
    binding: &RootActorExecutionBinding,
    store: Arc<bamboo_storage::SessionStoreV2>,
    inbox: Arc<dyn SessionInboxPort>,
    provider: Arc<FailBeforeOutput>,
    capture: StartupCapture,
) {
    let initial_message = initial_user_message_for_session(session);
    let config = AgentLoopConfig {
        skip_initial_user_message: true,
        selected_skill_ids: Some(Vec::new()),
        model_name: Some("startup-privacy-model".into()),
        storage: Some(store),
        persistence: Some(binding.persistence.clone()),
        guidance_active_run_id: Some(binding.owner.fence.run_id.clone()),
        session_inbox: Some(inbox),
        // No external memory/Project work is needed for this provider boundary.
        prompt_memory_flags: PromptMemoryFlags {
            project_prompt_injection: false,
            relevant_recall: false,
            relevant_recall_rerank: false,
            project_first_dream: false,
            ledger_agenda: false,
        },
        ..Default::default()
    };
    let (tx, mut rx) = tokio::sync::mpsc::channel(128);
    let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let result = {
        let _guard = tracing::subscriber::set_default(capture);
        tracing::callsite::rebuild_interest_cache();
        crate::runtime::runner::run_agent_loop_with_config(
            session,
            initial_message,
            tx,
            provider,
            Arc::new(NoTools),
            CancellationToken::new(),
            config,
        )
        .await
    };
    drain.await.unwrap();
    let error = result.expect_err("actual provider fails before any output");
    assert!(error
        .to_string()
        .contains("synthetic failure before output"));
    assert!(session.messages.iter().all(|m| m.role != Role::Assistant));
    session.set_last_run_status("error");
    session.set_last_run_error(error.to_string());
    // Same normal terminal-save boundary used by spawn_session_execution.
    super::save_finalized_runtime_with_inherited_child_wait(
        binding.persistence.as_ref(),
        session,
        None,
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn admitted_peer_survives_preoutput_error_and_resumed_startup_logs_no_body_or_route() {
    let temp = tempfile::tempdir().unwrap();
    let store = Arc::new(
        bamboo_storage::SessionStoreV2::new(temp.path().into())
            .await
            .unwrap(),
    );
    let mut root = Session::new("startup-privacy-root", "startup-privacy-model");
    root.set_project_id_meta("startup-privacy-project");
    store.save_session(&root).await.unwrap();
    let child = Session::new_child_of("PRIVATE_PEER_SOURCE", &root, "model", "source");
    store.save_session(&child).await.unwrap();
    let inbox = Arc::new(bamboo_storage::FileSessionInbox::new(
        store.clone(),
        SessionInboxLimits::default(),
    ));
    let host = crate::SessionRepository::new(
        Arc::default(),
        store.clone(),
        Arc::new(bamboo_storage::LockedSessionStore::new(store.clone())),
    )
    .with_root_actor_directory(store.clone());
    let mut peer =
        SessionMessageEnvelope::user_input(&root.id, "PRIVATE_PEER_BODY 界\nsecond line");
    peer.id = SessionMessageId::parse("PRIVATE_PEER_MESSAGE_ID").unwrap();
    peer.source = SessionMessageSource::Session {
        session_id: child.id.clone(),
    };
    peer.kind = SessionMessageKind::PeerMessage;
    peer.thread_id = Some("PRIVATE_PEER_THREAD".into());
    peer.correlation_id = Some("PRIVATE_PEER_OPERATION".into());
    peer.validate().unwrap();
    inbox
        .deliver_peer_message(
            &peer,
            &ActorSession::from_session(&child).unwrap(),
            &ActorSession::from_session(&root).unwrap(),
            None,
        )
        .await
        .unwrap();
    let binding = host
        .bind_root_actor_execution(&root, "first-peer-run")
        .await
        .unwrap()
        .unwrap();
    let admission = binding
        .persistence
        .admit_root_inbox(&mut root, inbox.clone(), Some(&binding.owner.fence.run_id))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(admission.merged, 1);
    assert!(admission.admission_error.is_none(), "{admission:?}");
    assert!(inbox.was_admitted(&root.id, &peer.id).await.unwrap());
    let exact_peer_content = peer.to_provider_message().unwrap().content;
    assert_eq!(initial_user_message_for_session(&root), exact_peer_content);
    let provider = Arc::new(FailBeforeOutput::default());
    failing_loop(
        &mut root,
        &binding,
        store.clone(),
        inbox.clone(),
        provider.clone(),
        StartupCapture::default(),
    )
    .await;
    finish(binding).await;
    root = store.load_session(&root.id).await.unwrap().unwrap();
    assert_eq!(root.last_run_status().as_deref(), Some("error"));
    assert_eq!(root.messages.last().unwrap().role, Role::User);
    assert_eq!(initial_user_message_for_session(&root), exact_peer_content);

    // A new legitimate delivery can activate this errored Root. Keep it pending
    // until the real loop boundary, so startup selects the first admitted peer.
    let mut next = peer.clone();
    next.id = SessionMessageId::new();
    next.created_at = chrono::Utc::now();
    next.body = bamboo_domain::SessionMessageBody::Content(
        bamboo_domain::SessionMessageContent::text("second valid peer"),
    );
    inbox
        .deliver_peer_message(
            &next,
            &ActorSession::from_session(&child).unwrap(),
            &ActorSession::from_session(&root).unwrap(),
            None,
        )
        .await
        .unwrap();
    assert!(inbox.inspect(&root.id).await.unwrap().activation_pending());
    let binding = host
        .bind_root_actor_execution(&root, "resumed-peer-run")
        .await
        .unwrap()
        .unwrap();
    let capture = StartupCapture::default();
    failing_loop(
        &mut root,
        &binding,
        store.clone(),
        inbox.clone(),
        provider.clone(),
        capture.clone(),
    )
    .await;
    finish(binding).await;
    assert!(inbox.was_admitted(&root.id, &next.id).await.unwrap());
    let calls = provider.0.lock().unwrap();
    assert_eq!(calls.len(), 2, "one real pre-output failure per owned run");
    for messages in calls.iter() {
        assert!(
            messages
                .iter()
                .any(|m| m.role == Role::User && m.content == exact_peer_content),
            "provider-facing peer body is preserved"
        );
    }
    let events = capture.0.lock().unwrap();
    assert_eq!(
        events.len(),
        2,
        "both actual startup diagnostics were captured"
    );
    let logs = events.join("\n");
    for sentinel in [
        "PRIVATE_PEER_BODY",
        "PRIVATE_PEER_SOURCE",
        "PRIVATE_PEER_MESSAGE_ID",
        "PRIVATE_PEER_THREAD",
        "PRIVATE_PEER_OPERATION",
    ] {
        assert!(
            !logs.contains(sentinel),
            "startup diagnostic leaked {sentinel}: {logs}"
        );
    }
}

#[tokio::test]
async fn ordinary_startup_preserves_provider_prompt_and_logs_only_metadata() {
    let temp = tempfile::tempdir().unwrap();
    let store = Arc::new(
        bamboo_storage::SessionStoreV2::new(temp.path().into())
            .await
            .unwrap(),
    );
    let mut session = Session::new("ordinary-startup-privacy", "startup-privacy-model");
    let prompt = "ORDINARY_PRIVATE_PROMPT 界\n\rpreserved";
    session.add_message(Message::user(prompt));
    store.save_session(&session).await.unwrap();
    let inbox = Arc::new(bamboo_storage::FileSessionInbox::new(
        store.clone(),
        SessionInboxLimits::default(),
    ));
    let host = crate::SessionRepository::new(
        Arc::default(),
        store.clone(),
        Arc::new(bamboo_storage::LockedSessionStore::new(store.clone())),
    )
    .with_root_actor_directory(store.clone());
    let binding = host
        .bind_root_actor_execution(&session, "ordinary-run")
        .await
        .unwrap()
        .unwrap();
    let provider = Arc::new(FailBeforeOutput::default());
    let capture = StartupCapture::default();
    assert_eq!(initial_user_message_for_session(&session), prompt);
    failing_loop(
        &mut session,
        &binding,
        store,
        inbox,
        provider.clone(),
        capture.clone(),
    )
    .await;
    finish(binding).await;
    assert!(provider.0.lock().unwrap()[0]
        .iter()
        .any(|m| m.role == Role::User && m.content == prompt));
    let events = capture.0.lock().unwrap();
    assert_eq!(events.len(), 2);
    assert!(!events.join("\n").contains("ORDINARY_PRIVATE_PROMPT"));
}
