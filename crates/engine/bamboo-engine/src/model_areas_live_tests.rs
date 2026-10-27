use super::*;
use crate::app_context::model_routing_test_support::RoutingContext;

#[tokio::test]
async fn routing_contract_live_resolver_never_turns_initial_contention_into_empty_defaults() {
    let context = RoutingContext::new(None);
    let guard = context.config.write().await;
    let resolver = live_auxiliary_model_resolver(
        context.config.clone(),
        context.registry.clone(),
        "chat-a".into(),
    );
    let unavailable = resolver();
    assert!(unavailable.fast_model_error.is_some());
    assert!(unavailable.background_model_error.is_some());
    assert!(unavailable.summarization_model_error.is_some());
    assert!(unavailable
        .fast_target(context.providers["chat-a"].clone(), "main-a")
        .is_err());
    assert!(
        unavailable.exact_model_resolver.unwrap()(&ProviderModelRef::new(
            "chat-a",
            "explicit-gold"
        ))
        .is_err()
    );
    drop(guard);
    let ready = resolver();
    let target = ready
        .fast_target(context.providers["chat-a"].clone(), "main-a")
        .unwrap();
    assert_eq!(target.model_name, "cheap-b");
    let expected: Arc<dyn bamboo_llm::LLMProvider> = context.providers["fast-b"].clone();
    assert!(Arc::ptr_eq(&target.provider, &expected));
    context.assert_no_requests();
}

#[tokio::test]
async fn routing_contract_live_resolver_reloads_pairs_and_preserves_errors_across_contention() {
    for flag in [None, Some(false), Some(true)] {
        let context = RoutingContext::new(flag);
        let resolver = live_auxiliary_model_resolver(
            context.config.clone(),
            context.registry.clone(),
            "chat-a".into(),
        );
        assert_eq!(resolver().fast_model_name.as_deref(), Some("cheap-b"));
        let mut guard = context.config.write().await;
        guard.defaults.as_mut().unwrap().fast =
            Some(ProviderModelRef::new("memory-c", "background-c"));
        assert_eq!(resolver().fast_model_name.as_deref(), Some("cheap-b"));
        drop(guard);
        let updated = resolver()
            .fast_target(context.providers["chat-a"].clone(), "main-a")
            .unwrap();
        assert_eq!(updated.model_name, "background-c");
        let expected: Arc<dyn bamboo_llm::LLMProvider> = context.providers["memory-c"].clone();
        assert!(Arc::ptr_eq(&updated.provider, &expected));
        context.config.write().await.defaults.as_mut().unwrap().fast =
            Some(ProviderModelRef::new("unknown-account", "bad-model"));
        assert!(resolver().fast_model_error.is_some());
        let _guard = context.config.write().await;
        assert!(resolver().fast_model_error.is_some());
        context.assert_no_requests();
    }
}
