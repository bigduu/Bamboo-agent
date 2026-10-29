use super::*;
use crate::app_context::model_routing_test_support::RoutingContext;
use bamboo_llm::LLMProvider;

fn assert_provider(
    actual: &Option<Arc<dyn LLMProvider>>,
    context: &RoutingContext,
    provider_id: &str,
) {
    let expected: Arc<dyn LLMProvider> = context.providers[provider_id].clone();
    assert!(
        Arc::ptr_eq(actual.as_ref().expect("provider must be set"), &expected),
        "wrong provider for {provider_id}"
    );
}

async fn assert_fast_rejected(models: &crate::AuxiliaryModelConfig) {
    let provider = models
        .fast_model_provider
        .as_ref()
        .expect("an unavailable Fast route must use the rejecting provider");
    assert!(provider.known_pre_dispatch_error().is_some());
    assert!(provider
        .chat_stream(
            &[],
            &[],
            None,
            models.fast_model_name.as_deref().unwrap_or("main-a"),
        )
        .await
        .is_err());
}

async fn assert_unavailable(models: &crate::AuxiliaryModelConfig) {
    assert!(models.fast_model_name.is_none());
    assert_fast_rejected(models).await;
    assert!(models.background_model_name.is_none());
    assert!(models.background_model_provider.is_none());
    assert!(models.summarization_model_name.is_none());
    assert!(models.summarization_model_provider.is_none());
    assert!(models.summarization_model_error.is_some());
    assert!(models.planning_model_name.is_none());
    assert!(models.search_model_name.is_none());
}

#[tokio::test]
async fn routing_contract_live_resolver_reloads_matching_provider_model_pairs_for_each_flag() {
    for flag in [None, Some(false), Some(true)] {
        let context = RoutingContext::new(flag);
        let resolver = live_auxiliary_model_resolver(
            context.config.clone(),
            context.registry.clone(),
            "chat-a".into(),
        );
        let initial = resolver();
        assert_eq!(initial.fast_model_name.as_deref(), Some("cheap-b"));
        assert_provider(&initial.fast_model_provider, &context, "fast-b");
        assert_eq!(
            initial.background_model_name.as_deref(),
            Some("background-c")
        );
        assert_provider(&initial.background_model_provider, &context, "memory-c");
        assert_eq!(initial.summarization_model_name.as_deref(), Some("cheap-b"));
        assert_provider(&initial.summarization_model_provider, &context, "fast-b");
        assert!(initial.summarization_model_error.is_none());
        assert_eq!(initial.planning_model_name.as_deref(), Some("cheap-b"));
        assert_eq!(initial.search_model_name.as_deref(), Some("cheap-b"));

        {
            let mut config = context.config.write().await;
            let defaults = config.defaults.as_mut().unwrap();
            defaults.fast = Some(ProviderModelRef::new("memory-c", "background-c"));
            defaults.memory_background = Some(ProviderModelRef::new("chat-a", "main-a"));
            defaults.task_summary = Some(ProviderModelRef::new("fast-b", "cheap-b"));
            defaults.planning = Some(ProviderModelRef::new("chat-a", "main-a"));
            defaults.search = Some(ProviderModelRef::new("memory-c", "background-c"));
        }

        let updated = resolver();
        assert_eq!(updated.fast_model_name.as_deref(), Some("background-c"));
        assert_provider(&updated.fast_model_provider, &context, "memory-c");
        assert_eq!(updated.background_model_name.as_deref(), Some("main-a"));
        assert_provider(&updated.background_model_provider, &context, "chat-a");
        assert_eq!(updated.summarization_model_name.as_deref(), Some("cheap-b"));
        assert_provider(&updated.summarization_model_provider, &context, "fast-b");
        assert!(updated.summarization_model_error.is_none());
        assert_eq!(updated.planning_model_name.as_deref(), Some("main-a"));
        assert_eq!(updated.search_model_name.as_deref(), Some("background-c"));
        context.assert_no_requests();
    }
}

#[tokio::test]
async fn routing_contract_live_resolver_fails_closed_during_initial_and_later_contention() {
    let context = RoutingContext::new(None);
    let mut guard = context.config.write().await;
    let resolver = live_auxiliary_model_resolver(
        context.config.clone(),
        context.registry.clone(),
        "chat-a".into(),
    );
    assert_unavailable(&resolver()).await;
    drop(guard);

    let ready = resolver();
    assert_eq!(ready.fast_model_name.as_deref(), Some("cheap-b"));
    assert_provider(&ready.fast_model_provider, &context, "fast-b");

    guard = context.config.write().await;
    guard.defaults.as_mut().unwrap().fast = Some(ProviderModelRef::new("memory-c", "background-c"));
    assert_unavailable(&resolver()).await;
    drop(guard);

    let updated = resolver();
    assert_eq!(updated.fast_model_name.as_deref(), Some("background-c"));
    assert_provider(&updated.fast_model_provider, &context, "memory-c");
    context.assert_no_requests();
}

#[tokio::test]
async fn routing_contract_live_resolver_fails_closed_without_provider_key() {
    let context = RoutingContext::new(None);
    for provider_key in ["", "   "] {
        let resolver = live_auxiliary_model_resolver(
            context.config.clone(),
            context.registry.clone(),
            provider_key.into(),
        );
        assert_unavailable(&resolver()).await;
    }
    context.assert_no_requests();
}

#[tokio::test]
async fn routing_contract_live_resolver_rejects_invalid_fast_without_account_fallback() {
    for flag in [None, Some(false), Some(true)] {
        for failure in ["disabled", "unadmitted", "missing"] {
            let context = RoutingContext::new(flag);
            let resolver = live_auxiliary_model_resolver(
                context.config.clone(),
                context.registry.clone(),
                "chat-a".into(),
            );
            let initial = resolver();
            assert_eq!(initial.fast_model_name.as_deref(), Some("cheap-b"));
            assert_provider(&initial.fast_model_provider, &context, "fast-b");
            {
                let mut config = context.config.write().await;
                match failure {
                    "disabled" => {
                        config.provider_instances.get_mut("fast-b").unwrap().enabled = false
                    }
                    "unadmitted" => {
                        config
                            .provider_instances
                            .get_mut("fast-b")
                            .unwrap()
                            .extra
                            .insert(
                                "runtime_models".into(),
                                serde_json::json!(["different-model"]),
                            );
                    }
                    "missing" => {
                        config.defaults.as_mut().unwrap().fast =
                            Some(ProviderModelRef::new("missing-account", "cheap-b"));
                    }
                    _ => unreachable!(),
                }
            }
            let models = resolver();
            assert_eq!(models.fast_model_name.as_deref(), Some("cheap-b"));
            assert_fast_rejected(&models).await;
            // Summary inherits the rejected Fast role, while explicit background stays valid.
            assert!(models.summarization_model_name.is_none());
            assert!(models.summarization_model_provider.is_none());
            assert!(models.summarization_model_error.is_some());
            assert_eq!(
                models.background_model_name.as_deref(),
                Some("background-c")
            );
            assert_provider(&models.background_model_provider, &context, "memory-c");
            context.assert_no_requests();
        }
    }
}

#[tokio::test]
async fn routing_contract_live_resolver_drops_invalid_background_pair_without_fallback() {
    for flag in [None, Some(false), Some(true)] {
        for failure in ["disabled", "unadmitted", "missing"] {
            let context = RoutingContext::new(flag);
            let resolver = live_auxiliary_model_resolver(
                context.config.clone(),
                context.registry.clone(),
                "chat-a".into(),
            );
            let initial = resolver();
            assert_eq!(
                initial.background_model_name.as_deref(),
                Some("background-c")
            );
            assert_provider(&initial.background_model_provider, &context, "memory-c");
            {
                let mut config = context.config.write().await;
                match failure {
                    "disabled" => {
                        config
                            .provider_instances
                            .get_mut("memory-c")
                            .unwrap()
                            .enabled = false
                    }
                    "unadmitted" => {
                        config
                            .provider_instances
                            .get_mut("memory-c")
                            .unwrap()
                            .extra
                            .insert(
                                "runtime_models".into(),
                                serde_json::json!(["different-model"]),
                            );
                    }
                    "missing" => {
                        config.defaults.as_mut().unwrap().memory_background =
                            Some(ProviderModelRef::new("missing-account", "background-c"));
                    }
                    _ => unreachable!(),
                }
            }
            let models = resolver();
            assert!(models.background_model_name.is_none());
            assert!(models.background_model_provider.is_none());
            assert_eq!(models.fast_model_name.as_deref(), Some("cheap-b"));
            assert_provider(&models.fast_model_provider, &context, "fast-b");
            assert_eq!(models.summarization_model_name.as_deref(), Some("cheap-b"));
            assert_provider(&models.summarization_model_provider, &context, "fast-b");
            assert!(models.summarization_model_error.is_none());
            context.assert_no_requests();
        }
    }
}

#[tokio::test]
async fn routing_contract_live_resolver_keeps_invalid_explicit_summary_as_error() {
    let context = RoutingContext::new(None);
    context
        .config
        .write()
        .await
        .defaults
        .as_mut()
        .unwrap()
        .task_summary = Some(ProviderModelRef::new("missing-account", "summary-model"));
    let resolver = live_auxiliary_model_resolver(
        context.config.clone(),
        context.registry.clone(),
        "chat-a".into(),
    );
    let models = resolver();
    assert!(models.summarization_model_name.is_none());
    assert!(models.summarization_model_provider.is_none());
    assert!(models.summarization_model_error.is_some());
    assert_eq!(models.fast_model_name.as_deref(), Some("cheap-b"));
    assert_provider(&models.fast_model_provider, &context, "fast-b");
    assert_eq!(
        models.background_model_name.as_deref(),
        Some("background-c")
    );
    assert_provider(&models.background_model_provider, &context, "memory-c");
    context.assert_no_requests();
}
