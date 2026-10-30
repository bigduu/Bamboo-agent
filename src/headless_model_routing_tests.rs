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
                    .any(|(model, purpose, _)| model == "main-a" && purpose == "agent_loop")
                && new_calls.is_empty()
                && provider.as_deref() == Some("chat-a")
        ),
        "resuming must retain the original account for both flag values: {outcomes:?}"
    );
}

#[tokio::test]
async fn entrypoint_routing_headless_missing_model_uses_saved_account_default() {
    let mut outcomes = Vec::new();
    for flag in [false, true] {
        let (dir, state, original_provider, new_default, mut original) =
            fixture(flag, "missing-model").await;
        original.title_generated = true;
        original.model = "unknown".into();
        original.model_ref = None;
        original.set_provider_name("chat-a");
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
                    .any(|(model, purpose, _)| model == "main-a" && purpose == "agent_loop")
                && new_calls.is_empty()
                && provider.as_deref() == Some("chat-a")
        ),
        "resuming must retain the original account for both flag values: {outcomes:?}"
    );
}

#[tokio::test]
async fn entrypoint_routing_headless_host_uses_configured_vision_role() {
    let mut outcomes = Vec::new();
    for flag in [false, true] {
        let server = crate::model_entrypoint_routing_tests::vision_server(200).await;
        let (dir, state, main, _, mut original) = fixture(flag, "missing-model").await;
        *state.config.write().await =
            crate::model_entrypoint_routing_tests::legacy_config(flag, &server.uri());
        original.title_generated = true;
        let image = Message::user_with_parts(
            "Inspect synthetic pixel",
            vec![bamboo_domain::MessagePart::ImageUrl {
                image_url: bamboo_domain::ImageUrlRef {
                    url: crate::model_entrypoint_routing_tests::PIXEL.into(),
                    detail: None,
                },
            }],
        );
        let original_image = image.clone();
        original.add_message(image);
        state.storage.save_session(&original).await.unwrap();
        state.session_store.save_session(&original).await.unwrap();
        let storage = state.storage.clone();
        let mut request = args(dir.path(), None);
        request.timeout_secs = Some(10);
        let outcome = tokio::time::timeout(
            std::time::Duration::from_secs(20),
            run_with_state(request, state),
        )
        .await
        .expect("synthetic Host consumer must finish");
        let calls = main.calls.lock().unwrap().clone();
        let requests = server.received_requests().await.unwrap();
        let b_saw_pixel = requests.iter().any(|request| {
            request
                .body_json::<serde_json::Value>()
                .ok()
                .is_some_and(|body| body["model"] == "vision-b")
                && String::from_utf8_lossy(&request.body)
                    .contains(crate::model_entrypoint_routing_tests::PIXEL)
        });
        let b_models = requests
            .iter()
            .map(|request| request.body_json::<serde_json::Value>().unwrap()["model"].clone())
            .collect::<Vec<_>>();
        let saved = storage.load_session(&original.id).await.unwrap().unwrap();
        let preserved = saved
            .messages
            .iter()
            .find(|message| message.id == original_image.id)
            .is_some_and(|message| message.content_parts == original_image.content_parts);
        outcomes.push((flag, outcome, calls, b_models, b_saw_pixel, preserved));
    }
    assert!(
        outcomes.iter().all(
            |(_, outcome, calls, b_models, b_saw_pixel, preserved)| outcome.is_ok()
                && calls
                    .iter()
                    .any(|(model, purpose, images)| model == "main-a"
                        && purpose == "agent_loop"
                        && !images)
                && calls.iter().all(|(_, _, images)| !images)
                && b_models.iter().any(|model| model == "vision-b")
                && *b_saw_pixel
                && *preserved
        ),
        "Host fallback must use its configured Vision account: {outcomes:?}"
    );
}

#[tokio::test]
async fn entrypoint_routing_headless_invalid_saved_ref_refuses_before_prompt_or_dispatch() {
    let mut outcomes = Vec::new();
    for flag in [false, true] {
        let (dir, state, original_provider, new_default, mut original) =
            fixture(flag, "missing-model").await;
        original.title_generated = true;
        original.model = "unknown".into();
        original.model_ref = Some(bamboo_domain::ProviderModelRef::new("chat-a", "unknown"));
        original.set_provider_name("chat-a");
        state.storage.save_session(&original).await.unwrap();
        state.session_store.save_session(&original).await.unwrap();
        {
            let mut config = state.config.write().await;
            config.default_provider_instance = Some("broken-b".into());
            config.defaults.as_mut().unwrap().chat =
                bamboo_domain::ProviderModelRef::new("broken-b", "main-a");
            config
                .provider_instances
                .get_mut("chat-a")
                .unwrap()
                .extra
                .remove("runtime_models");
            let new_instance = config.provider_instances.get_mut("broken-b").unwrap();
            new_instance.model = Some("main-a".into());
            new_instance
                .extra
                .insert("runtime_models".into(), serde_json::json!(["main-a"]));
        }
        let registry = ProviderRegistry::new(
            HashMap::from([
                (
                    "chat-a".into(),
                    original_provider.clone() as Arc<dyn LLMProvider>,
                ),
                (
                    "broken-b".into(),
                    new_default.clone() as Arc<dyn LLMProvider>,
                ),
            ]),
            "broken-b".into(),
        );
        // An unmanaged embedded registry with no model table must not make an
        // explicit unknown structured reference silently fall back to B.
        state.provider_registry.replace_with(registry);
        let storage = state.storage.clone();
        let mut request = args(dir.path(), None);
        request.timeout_secs = Some(10);
        let outcome = tokio::time::timeout(
            std::time::Duration::from_secs(20),
            run_with_state(request, state),
        )
        .await
        .expect("synthetic invalid-ref consumer must finish");
        let old_calls = original_provider.calls.lock().unwrap().clone();
        let new_calls = new_default.calls.lock().unwrap().clone();
        let saved = storage.load_session(&original.id).await.unwrap().unwrap();
        outcomes.push((
            flag,
            outcome,
            old_calls,
            new_calls,
            saved.messages.len(),
            original.messages.len(),
        ));
    }
    assert!(
        outcomes.iter().all(
            |(_, outcome, old_calls, new_calls, saved_len, original_len)| outcome
                .as_ref()
                .err()
                .is_some_and(
                    |error| error.contains("invalid headless inherited provider/model reference")
                )
                && old_calls.is_empty()
                && new_calls.is_empty()
                && saved_len == original_len
        ),
        "invalid saved refs must stop before prompt or provider dispatch: {outcomes:?}"
    );
}
