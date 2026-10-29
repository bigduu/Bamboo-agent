use super::*;
use crate::model_entrypoint_routing_tests::Capture;
use bamboo_llm::{Config, LLMProvider, ProviderRegistry};
use std::collections::HashMap;
use std::sync::Arc;

async fn fixture(
    flag: bool,
    rejection: &str,
) -> (
    tempfile::TempDir,
    AppState,
    Arc<Capture>,
    Arc<Capture>,
    Session,
) {
    let dir = tempfile::tempdir().unwrap();
    let main = Arc::new(Capture::default());
    let secondary = Arc::new(Capture::default());
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
        HashMap::from([
            ("chat-a".into(), main.clone() as Arc<dyn LLMProvider>),
            ("broken-b".into(), secondary.clone() as Arc<dyn LLMProvider>),
        ]),
        "chat-a".into(),
    );
    registry.set_runtime_models(HashMap::from([
        ("chat-a".into(), vec!["main-a".into(), "cheap".into()]),
        ("broken-b".into(), vec!["main-a".into(), "model-b".into()]),
    ]));
    state.provider_registry.replace_with(registry);
    let mut session = Session::new("synthetic-resume", "main-a");
    bamboo_engine::session_app::provider_model::persist_model_ref(
        &mut session,
        &bamboo_domain::ProviderModelRef::new("chat-a", "main-a"),
    );
    session.add_message(Message::user("Previous synthetic input"));
    state.storage.save_session(&session).await.unwrap();
    state.session_store.save_session(&session).await.unwrap();
    (dir, state, main, secondary, session)
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
    let mut outcomes = Vec::new();
    for flag in [false, true] {
        for rejection in ["unknown", "disabled", "missing-model"] {
            let (dir, state, main, secondary, original) = fixture(flag, rejection).await;
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
            let calls = main.calls.lock().unwrap().clone();
            let secondary_calls = secondary.calls.lock().unwrap().clone();
            let saved = storage.load_session(&original.id).await.unwrap().unwrap();
            outcomes.push(serde_json::json!({"flag":flag,"rejection":rejection,"resultError":result.err(),
                "mainCalls":calls,"selectedCalls":secondary_calls,"newMessages":saved.messages.len().saturating_sub(original.messages.len())}));
        }
    }
    assert!(
        outcomes.iter().all(|row| !row["resultError"].is_null()
            && row["mainCalls"].as_array().unwrap().is_empty()
            && row["selectedCalls"].as_array().unwrap().is_empty()
            && row["newMessages"] == 0),
        "{outcomes:#?}"
    );
}

#[tokio::test]
async fn entrypoint_routing_headless_unset_override_keeps_legitimate_parent_inheritance() {
    let (dir, state, main, _, _) = fixture(true, "missing-model").await;
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

#[tokio::test]
async fn entrypoint_routing_headless_resume_keeps_original_account_when_default_changes() {
    let mut outcomes = Vec::new();
    for flag in [false, true] {
        let (dir, state, original_provider, new_default, mut original) =
            fixture(flag, "missing-model").await;
        original.title_generated = true;
        state.storage.save_session(&original).await.unwrap();
        state.session_store.save_session(&original).await.unwrap();
        {
            let mut config = state.config.write().await;
            config.default_provider_instance = Some("broken-b".into());
            let instance = config.provider_instances.get_mut("broken-b").unwrap();
            instance.model = Some("main-a".into());
            instance
                .extra
                .insert("runtime_models".into(), serde_json::json!(["main-a"]));
        }
        let storage = state.storage.clone();
        let mut request = args(dir.path(), None);
        request.timeout_secs = Some(10);
        let outcome = tokio::time::timeout(
            std::time::Duration::from_secs(20),
            run_with_state(request, state),
        )
        .await
        .expect("synthetic headless consumer must finish");
        let old_calls = original_provider.calls.lock().unwrap().clone();
        let new_calls = new_default.calls.lock().unwrap().clone();
        let saved = storage.load_session(&original.id).await.unwrap().unwrap();
        let provider =
            bamboo_engine::session_app::provider_model::session_effective_model_ref(&saved)
                .map(|target| target.provider);
        outcomes.push((flag, outcome, old_calls, new_calls, provider));
    }
    assert!(
        outcomes.iter().all(
            |(_, outcome, old_calls, new_calls, provider)| outcome.is_ok()
                && old_calls
                    .iter()
                    .any(|(model, purpose, _)| model == "main-a" && purpose != "gold_evaluation")
                && new_calls.is_empty()
                && provider.as_deref() == Some("chat-a")
        ),
        "resuming must retain the original account for both flag values: {outcomes:?}"
    );
}
