//! Consumer-style compile coverage for the complete SessionInbox SDK facade.
//!
//! Bamboo domain/engine crates are intentionally not imported here: an
//! external SDK user must be able to construct every typed envelope and
//! implement activation from `bamboo_sdk` alone.

mod root_surface {
    use bamboo_sdk::*;

    pub struct ExternalSpawner;

    #[async_trait::async_trait]
    impl SessionActivationSpawner for ExternalSpawner {
        async fn reserve_activation(
            &self,
            target_session_id: &str,
            inbox_generation: u64,
        ) -> Result<SessionActivationReserveOutcome, SessionActivationError> {
            Ok(SessionActivationReserveOutcome::Reserved(
                SessionActivationLaunch::new(
                    format!("{target_session_id}-{inbox_generation}"),
                    || {},
                ),
            ))
        }
    }

    pub fn every_envelope_variant() -> Vec<SessionMessageEnvelope> {
        let user = SessionMessageEnvelope::user_input("target", "user input");

        let mut peer = SessionMessageEnvelope::user_input("target", "peer input");
        peer.id = SessionMessageId::parse("peer-message").unwrap();
        peer.source = SessionMessageSource::Session {
            session_id: "peer".to_string(),
        };
        peer.kind = SessionMessageKind::PeerMessage;
        peer.body = SessionMessageBody::Content(SessionMessageContent::text("peer input"));

        let mut provider_metadata = serde_json::Map::new();
        provider_metadata.insert(
            "runtime_kind".to_string(),
            serde_json::json!("child_resume"),
        );
        let mut child = SessionMessageEnvelope::user_input("target", "child outcome");
        child.id = SessionMessageId::parse("child-message").unwrap();
        child.source = SessionMessageSource::Session {
            session_id: "child".to_string(),
        };
        child.kind = SessionMessageKind::ChildOutcome;
        child.body = SessionMessageBody::ChildOutcome(SessionChildOutcome {
            child_session_id: "child".to_string(),
            status: "completed".to_string(),
            result: Some("done".to_string()),
            error: None,
            provider_message: Some(SessionProviderMessage {
                content: SessionMessageContent::text("exact provider presentation"),
                metadata: provider_metadata,
                never_compress: true,
            }),
        });

        let mut runtime = SessionMessageEnvelope::user_input("target", "runtime");
        runtime.id = SessionMessageId::parse("runtime-message").unwrap();
        runtime.source = SessionMessageSource::Runtime {
            subsystem: "external-sdk".to_string(),
        };
        runtime.kind = SessionMessageKind::RuntimeInstruction;
        runtime.body = SessionMessageBody::RuntimeInstruction(SessionRuntimeInstruction {
            instruction: "refresh".to_string(),
            content: Some(SessionMessageContent::text("refresh now")),
            data: Some(serde_json::json!({"nested": {"b": 2, "a": 1}})),
            provider_message: Some(SessionProviderMessage {
                content: SessionMessageContent::text("runtime provider presentation"),
                metadata: serde_json::Map::new(),
                never_compress: false,
            }),
        });

        vec![user, peer, child, runtime]
    }
}

mod agent_surface {
    use bamboo_sdk::agent::*;

    pub fn typecheck_agent_module() {
        let _: Option<SessionActivationLaunch> = None;
        let _: Option<SessionActivationReserveOutcome> = None;
        let _: Option<SessionMessageSource> = None;
        let _: Option<SessionMessageKind> = None;
        let _: Option<SessionMessageBody> = None;
        let _: Option<SessionMessageContent> = None;
        let _: Option<SessionChildOutcome> = None;
        let _: Option<SessionRuntimeInstruction> = None;
        let _: Option<SessionProviderMessage> = None;
    }
}

#[tokio::test]
async fn external_consumer_needs_no_internal_bamboo_crate_paths() {
    use bamboo_sdk::{
        Agent, FileSessionInbox, SessionActivationReserveOutcome, SessionActivationRouter,
        SessionActivationSpawner, SessionMessagingMetrics, SessionMessagingMetricsSnapshot,
    };

    let envelopes = root_surface::every_envelope_variant();
    assert_eq!(envelopes.len(), 4);
    for envelope in envelopes {
        envelope.validate().unwrap();
        envelope.to_provider_message().unwrap();
    }
    let result = root_surface::ExternalSpawner
        .reserve_activation("target", 7)
        .await
        .unwrap();
    assert!(matches!(
        result,
        SessionActivationReserveOutcome::Reserved(_)
    ));
    agent_surface::typecheck_agent_module();

    let _: Option<FileSessionInbox> = None;
    let metrics = SessionMessagingMetrics::default();
    let _: SessionMessagingMetricsSnapshot = metrics.snapshot();

    let temp = tempfile::tempdir().unwrap();
    std::fs::write(
        temp.path().join("config.json"),
        r#"{
            "provider": "anthropic",
            "providers": {
                "anthropic": {"api_key": "test-key", "model": "claude-test"}
            }
        }"#,
    )
    .unwrap();
    let router = SessionActivationRouter::new();
    let agent = Agent::builder()
        .session_delivery(router.clone())
        .with_defaults_for_data_dir(temp.path().to_path_buf())
        .await
        .unwrap()
        .build()
        .unwrap();
    assert!(agent.session_messenger().is_some());
    assert!(agent.session_inbox().is_some());
    assert!(std::sync::Arc::ptr_eq(
        agent.activation_router().unwrap(),
        &router
    ));
}
