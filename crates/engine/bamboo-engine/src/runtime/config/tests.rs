use bamboo_agent_core::Session;
use bamboo_config::MemoryConfig;

use super::{AgentLoopConfig, PromptMemoryFlags};

#[test]
fn agent_loop_config_model_name_defaults_to_none() {
    let config = AgentLoopConfig::default();
    assert!(
        config.model_name.is_none(),
        "model_name should default to None, forcing explicit setting"
    );
    assert!(
        config.selected_skill_ids.is_none(),
        "selected_skill_ids should default to None"
    );
    assert!(
        config.selected_skill_mode.is_none(),
        "selected_skill_mode should default to None"
    );
    assert!(
        config.provider_type.is_none(),
        "provider_type should default to None"
    );
}

#[test]
fn agent_loop_config_can_set_model_name() {
    let config = AgentLoopConfig {
        model_name: Some("kimi-for-coding".to_string()),
        ..Default::default()
    };
    assert_eq!(config.model_name, Some("kimi-for-coding".to_string()));
}

#[test]
fn model_must_come_from_config_not_session() {
    let config = AgentLoopConfig {
        model_name: Some("config-model".to_string()),
        ..Default::default()
    };

    let session = Session::new("test", "session-model");
    let execution_model = config.model_name.as_deref().unwrap();

    assert_eq!(
        execution_model, "config-model",
        "Model must come from config.model_name, not session.model"
    );
    assert_eq!(
        session.model, "session-model",
        "session.model is just for recording, not execution"
    );
}

#[test]
fn prompt_memory_flags_map_from_memory_config() {
    let memory = MemoryConfig {
        project_prompt_injection: false,
        relevant_recall: false,
        relevant_recall_rerank: true,
        project_first_dream: false,
        ..MemoryConfig::default()
    };

    let flags = PromptMemoryFlags::from(&memory);
    assert!(!flags.project_prompt_injection);
    assert!(!flags.relevant_recall);
    assert!(flags.relevant_recall_rerank);
    assert!(!flags.project_first_dream);
}

#[test]
fn resolve_disabled_filters_uses_snapshot_without_resolver() {
    // #44 frozen behavior: with no resolver, the snapshotted sets are returned.
    let mut config = AgentLoopConfig::default();
    config.disabled_tools = std::collections::BTreeSet::from(["frozen_tool".to_string()]);
    config.disabled_skill_ids = std::collections::BTreeSet::from(["frozen_skill".to_string()]);

    let (tools, skills) = config.resolve_disabled_filters();
    assert!(tools.contains("frozen_tool"));
    assert!(skills.contains("frozen_skill"));
}

#[test]
fn resolve_disabled_filters_uses_live_resolver_and_reinvokes() {
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    // #136: a wired resolver overrides the snapshot AND is re-invoked each call,
    // so the set can change between rounds.
    let calls = Arc::new(AtomicU32::new(0));
    let c = calls.clone();
    let mut config = AgentLoopConfig::default();
    config.disabled_tools = std::collections::BTreeSet::from(["ignored_snapshot".to_string()]);
    config.disabled_filter_resolver = Some(Arc::new(move || {
        let n = c.fetch_add(1, Ordering::SeqCst);
        (
            std::collections::BTreeSet::from([format!("live-{n}")]),
            std::collections::BTreeSet::new(),
        )
    }));

    let (t1, _) = config.resolve_disabled_filters();
    assert!(t1.contains("live-0"), "resolver result is used");
    assert!(
        !t1.contains("ignored_snapshot"),
        "the resolver overrides the frozen snapshot"
    );

    let (t2, _) = config.resolve_disabled_filters();
    assert!(
        t2.contains("live-1"),
        "the resolver is re-invoked each call (live between rounds)"
    );
}
