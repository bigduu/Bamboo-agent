//! Public SDK API regressions; no real provider or physical tool is used.
use super::*;

fn with_version(session: Session, version: Option<u32>) -> Session {
    let mut raw = serde_json::to_value(session).unwrap();
    match version {
        Some(version) => raw["session_version"] = json!(version),
        None => { raw.as_object_mut().unwrap().remove("session_version"); }
    }
    serde_json::from_value(raw).unwrap()
}

#[tokio::test]
async fn session_version_unknown_is_refused_before_sdk_append_or_provider() {
    for ultra in [false, true] {
        let fixture = Fixture::new().await;
        let mut session = Session::new(format!("future-sdk-{ultra}"), "test-model");
        session.set_root_orchestration_only(ultra).unwrap();
        session.add_message(Message::user("Existing history"));
        fixture.repo.save(&mut session).await.unwrap();
        let mut session = with_version(session, Some(99));
        let before = serde_json::to_value(&session).unwrap();
        let agent = fixture.agent(None, false, false, None);
        let result = agent.run(&mut session, "Must not append").await;
        let error = result.expect_err("unknown behavior cannot execute");
        assert!(error.to_string().contains("session_execution_incompatible"), "{error}");
        assert_eq!(serde_json::to_value(&session).unwrap(), before);
        assert_eq!(fixture.probe.provider_calls.load(Ordering::SeqCst), 0);
        assert!(fixture.probe.calls.lock().unwrap().is_empty());
        assert!(!fixture.directory.path().join("actions.log").exists());
    }
}

#[tokio::test]
async fn session_version_unknown_preserves_legacy_replay_markers_without_action() {
    let fixture = Fixture::new().await;
    let (mut session, _) = parked("future-replay", false);
    fixture.repo.save(&mut session).await.unwrap();
    let agent = fixture.agent(None, false, false, None);
    let answered = agent.answer(&session.id, "Approve").await.unwrap().session;
    let mut session = with_version(answered, Some(99));
    let before = serde_json::to_value(&session).unwrap();
    let result = agent.run_session(&mut session).await;
    let error = result.expect_err("incompatible behavior cannot replay approved tools");
    assert!(error.to_string().contains("session_execution_incompatible"), "{error}");
    assert_eq!(serde_json::to_value(&session).unwrap(), before);
    assert_eq!(fixture.probe.provider_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.probe.actions.load(Ordering::SeqCst), 0);
    assert!(fixture.probe.calls.lock().unwrap().is_empty());
    assert!(!fixture.directory.path().join("actions.log").exists());
}

#[tokio::test]
async fn session_version_compatible_legacy_without_stamp_still_continues() {
    let fixture = Fixture::new().await;
    let mut session = with_version(Session::new("usable-legacy-sdk", "test-model"), None);
    session.add_message(Message::user("Existing history"));
    fixture.repo.save(&mut session).await.unwrap();
    let agent = fixture.agent(None, false, false, None);
    agent.run(&mut session, "Continue this compatible legacy session").await.unwrap();
    assert!(fixture.probe.provider_calls.load(Ordering::SeqCst) > 0);
    assert!(session.messages.iter().any(|message| message.content == "Existing history"));
    assert!(serde_json::to_value(&session).unwrap().get("session_version").is_none());
    let loaded = fixture.store.load_session(&session.id).await.unwrap().unwrap();
    assert!(serde_json::to_value(loaded).unwrap().get("session_version").is_none());
}

#[tokio::test]
async fn session_version_unsupported_retained_state_is_not_reset_before_dispatch() {
    for context_conflict in [false, true] {
        let fixture = Fixture::new().await;
        let mut session = with_version(Session::new("future-retained-state", "test-model"), None);
        session.add_message(Message::user("Existing preserved history"));
        fixture.repo.save(&mut session).await.unwrap();
        if context_conflict {
            session.model_context_state = Some(bamboo_domain::ModelContextState {
                schema_version: 99,
                state_revision: 7,
                ..Default::default()
            });
        } else {
            let mut runtime = AgentRuntimeState::new("retained-generation");
            runtime.version = 99;
            session.agent_runtime_state = Some(runtime);
        }
        let before = serde_json::to_value(&session).unwrap();
        let agent = fixture.agent(None, false, false, None);
        let error = agent.run(&mut session, "must not reset this retained schema").await
            .expect_err("unknown retained schema is a concrete conflict");
        let expected = if context_conflict {
            "unsupported_model_context_version"
        } else {
            "unsupported_runtime_state_version"
        };
        assert!(error.to_string().contains(expected), "{error}");
        assert_eq!(serde_json::to_value(&session).unwrap(), before);
        assert_eq!(fixture.probe.provider_calls.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.probe.actions.load(Ordering::SeqCst), 0);
        assert!(fixture.probe.calls.lock().unwrap().is_empty());
    }
}
