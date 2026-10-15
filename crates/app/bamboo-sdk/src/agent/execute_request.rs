//! Ergonomic builder for [`bamboo_engine::ExecuteRequest`].
//!
//! The builder itself now lives in `bamboo-engine` (beside `ExecuteRequest`) so
//! the in-crate server layer (e.g. the schedule manager) and this root SDK
//! facade construct requests through one shared builder — no forked assembly.
//! This module re-exports it so `bamboo_agent::ExecuteRequestBuilder` and
//! `bamboo_agent::agent::ExecuteRequestBuilder` stay stable.

pub use bamboo_engine::runtime::loop_control::{
    inspect_loop, LoopControlReceipt, LoopStatusSnapshot,
};
pub use bamboo_engine::{CountedLoopConfig, ExecuteRequestBuilder, LoopControlConfig};

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
        assert!(req.model_roster.model.is_none());
        assert!(req.model_roster.provider_name.is_none());
        assert!(req.model_roster.provider_type.is_none());
        assert!(req.model_roster.fast.is_none());
        assert!(req.model_roster.fast_model().is_none());
        assert!(req.model_roster.fast_model_provider().is_none());
        assert!(req.model_roster.background.is_none());
        assert!(req.model_roster.background_model().is_none());
        assert!(req.model_roster.background_model_provider().is_none());
        assert!(req.model_roster.summarization.is_none());
        assert!(req.model_roster.summarization_model().is_none());
        assert!(req.model_roster.summarization_model_provider().is_none());
        assert!(req.reasoning_effort.is_none());
        assert!(req.auxiliary_model_resolver.is_none());
        assert!(req.disabled_tools.is_none());
        assert!(req.disabled_skill_ids.is_none());
        assert!(req.selected_skill_ids.is_none());
        assert!(req.selected_skill_mode.is_none());
        assert!(req.image_fallback.is_none());
        assert!(req.gold_config.is_none());
        assert!(req.counted_loop.is_none());
        assert!(req.loop_control.is_none());
        assert!(req.app_data_dir.is_none());
    }

    #[test]
    fn self_loop_capability_is_explicit_and_does_not_prestart_a_generation() {
        let (tx, _rx) = mpsc::channel::<AgentEvent>(8);
        let cap: crate::LoopControlConfig = crate::agent::LoopControlConfig::new(3, 12);
        let req = ExecuteRequestBuilder::new("Inspect and decide", tx, CancellationToken::new())
            .loop_control(cap.clone())
            .build();
        assert!(req.counted_loop.is_none());
        let policy = req.loop_control.unwrap();
        assert_eq!((policy.max_iterations, policy.max_total_rounds), (3, 12));
        let session = bamboo_agent_core::Session::new("sdk-loop", "synthetic-only");
        let status: crate::LoopStatusSnapshot = crate::inspect_loop(&session).unwrap();
        assert!(status.generation.is_none());
        assert_eq!((status.control_revision, status.rounds_started), (0, 0));
    }

    #[test]
    fn setters_round_trip_into_request() {
        let (tx, _rx) = mpsc::channel::<AgentEvent>(8);
        let mut disabled = BTreeSet::new();
        disabled.insert("Edit".to_string());

        let counted: crate::CountedLoopConfig = crate::agent::CountedLoopConfig::new(2, 4);
        let req = ExecuteRequestBuilder::new("go", tx, CancellationToken::new())
            .counted_loop(counted)
            .model("claude-x")
            .provider_name("anthropic")
            .disabled_tools(disabled.clone())
            .build();

        assert_eq!(req.model_roster.model.as_deref(), Some("claude-x"));
        assert_eq!(req.model_roster.provider_name.as_deref(), Some("anthropic"));
        assert_eq!(req.disabled_tools, Some(disabled));
        let counted = req.counted_loop.unwrap();
        assert_eq!((counted.max_iterations, counted.max_total_rounds), (2, 4));
    }
}
