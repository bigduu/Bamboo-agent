use super::*;
use crate::model_entrypoint_routing_tests::Capture;
use bamboo_llm::{Config, LLMProvider, ProviderRegistry};
use std::collections::HashMap;
use std::sync::Arc;

async fn fixture(
    flag: bool,
    rejection: &str,
) -> (tempfile::TempDir, AppState, Arc<Capture>, Session) {
    let dir = tempfile::tempdir().unwrap();
    let main = Arc::new(Capture::default());
    let mut config = crate::model_entrypoint_routing_tests::config(flag, "chat-a");
    config.provider_instances.insert(
        "broken-b".into(),
        serde_json::from_value(serde_json::json!({
            "provider_type":"openai","enabled":rejection!="disabled",
        }))
        .unwrap(),
    );
    let state = AppState::new_with_provider_and_memory_store(
        dir.path().to_path_buf(),
        Config::default(),
        main.clone(),
        bamboo_memory::memory_store::MemoryStore::new(dir.path().join("jiandu")),
    )
    .await
    .unwrap();
    *state.config.write().await = config;
    let registry = ProviderRegistry::new(
        HashMap::from([("chat-a".into(), main.clone() as Arc<dyn LLMProvider>)]),
        "chat-a".into(),
    );
    registry.set_runtime_models(HashMap::from([(
        "chat-a".into(),
        vec!["main-a".into(), "cheap".into()],
    )]));
    state.provider_registry.replace_with(registry);
    let mut session = Session::new("synthetic-resume", "main-a");
    bamboo_engine::session_app::provider_model::persist_model_ref(
        &mut session,
        &bamboo_domain::ProviderModelRef::new("chat-a", "main-a"),
    );
    session.add_message(Message::user("Previous synthetic input"));
    state.storage.save_session(&session).await.unwrap();
    state.session_store.save_session(&session).await.unwrap();
    (dir, state, main, session)
}

fn args(dir: &std::path::Path, provider: Option<String>) -> HeadlessArgs {
    HeadlessArgs {
        prompt: "New synthetic input".into(),
        session: Some("synthetic-resume".into()),
        model: None,
        workspace: None,
        project_id: None,
        data_dir: dir.to_path_buf(),
        permission_mode: None,
        stream_json: true,
        reasoning_effort: None,
        skill_mode: None,
        provider,
        timeout_secs: Some(3),
    }
}

#[tokio::test]
async fn entrypoint_routing_headless_provider_only_rejects_bad_target_before_parent_pin_or_prompt_mutation(
) {
    for flag in [false, true] {
        for rejection in ["unknown", "disabled", "missing-model"] {
            let (dir, state, main, original) = fixture(flag, rejection).await;
            let storage = state.storage.clone();
            let provider = if rejection == "unknown" {
                "unknown-b"
            } else {
                "broken-b"
            };
            let result = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                run_with_state(args(dir.path(), Some(provider.into())), state),
            )
            .await
            .unwrap();
            assert!(
                result.is_err(),
                "explicit {rejection}, flag={flag} was accepted"
            );
            let calls = main.calls.lock().unwrap().clone();
            assert!(
                calls.is_empty(),
                "provider-only rejection dispatched the retained parent: {calls:?}"
            );
            let saved = storage.load_session(&original.id).await.unwrap().unwrap();
            assert_eq!(
                saved.messages.len(),
                original.messages.len(),
                "invalid target must not append the new prompt"
            );
        }
    }
}

#[tokio::test]
async fn entrypoint_routing_headless_unset_override_keeps_legitimate_parent_inheritance() {
    let (dir, state, main, _) = fixture(true, "missing-model").await;
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        run_with_state(args(dir.path(), None), state),
    )
    .await
    .unwrap();
    assert!(result.is_ok(), "{result:?}");
    assert_eq!(
        main.calls
            .lock()
            .unwrap()
            .iter()
            .map(|(model, _, _)| model.as_str())
            .collect::<Vec<_>>(),
        vec!["main-a"]
    );
}
