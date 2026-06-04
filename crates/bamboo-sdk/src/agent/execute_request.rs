//! Ergonomic builder for [`bamboo_engine::ExecuteRequest`].
//!
//! The builder itself now lives in `bamboo-engine` (beside `ExecuteRequest`) so
//! the in-crate server layer (e.g. the schedule manager) and this root SDK
//! facade construct requests through one shared builder — no forked assembly.
//! This module re-exports it so `bamboo_agent::ExecuteRequestBuilder` and
//! `bamboo_agent::agent::ExecuteRequestBuilder` stay stable.

pub use bamboo_engine::ExecuteRequestBuilder;

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use bamboo_agent_core::AgentEvent;

    use super::ExecuteRequestBuilder;

    #[test]
    fn build_with_only_required_fields_defaults_optionals_to_none() {
        let (tx, _rx) = mpsc::channel::<AgentEvent>(8);
        let req = ExecuteRequestBuilder::new("hello", tx, CancellationToken::new()).build();

        assert_eq!(req.initial_message, "hello");
        assert!(req.tools.is_none());
        assert!(req.provider_override.is_none());
        assert!(req.model.is_none());
        assert!(req.provider_name.is_none());
        assert!(req.provider_type.is_none());
        assert!(req.fast_model.is_none());
        assert!(req.fast_model_provider.is_none());
        assert!(req.background_model.is_none());
        assert!(req.background_model_provider.is_none());
        assert!(req.summarization_model.is_none());
        assert!(req.summarization_model_provider.is_none());
        assert!(req.reasoning_effort.is_none());
        assert!(req.auxiliary_model_resolver.is_none());
        assert!(req.disabled_tools.is_none());
        assert!(req.disabled_skill_ids.is_none());
        assert!(req.selected_skill_ids.is_none());
        assert!(req.selected_skill_mode.is_none());
        assert!(req.image_fallback.is_none());
        assert!(req.gold_config.is_none());
        assert!(req.app_data_dir.is_none());
    }

    #[test]
    fn setters_round_trip_into_request() {
        let (tx, _rx) = mpsc::channel::<AgentEvent>(8);
        let mut disabled = BTreeSet::new();
        disabled.insert("Edit".to_string());

        let req = ExecuteRequestBuilder::new("go", tx, CancellationToken::new())
            .model("claude-x")
            .provider_name("anthropic")
            .disabled_tools(disabled.clone())
            .build();

        assert_eq!(req.model.as_deref(), Some("claude-x"));
        assert_eq!(req.provider_name.as_deref(), Some("anthropic"));
        assert_eq!(req.disabled_tools, Some(disabled));
    }
}
