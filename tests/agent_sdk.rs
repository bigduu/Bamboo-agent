//! Root SDK facade tests (ergonomic-sdk-plan §4: S-T4.1 .. S-T4.3).
//!
//! These exercise the `bamboo_agent::agent` facade end-to-end without network
//! I/O: the concise instruction/model/tools builder, the `ExecuteRequest`
//! builder, and default-dependency assembly via `with_defaults_for_data_dir`.

use bamboo_agent::agent::{
    builtin_tool_names, Agent, AgentBuilder, ExecuteRequestBuilder, ToolSpec,
};

use bamboo_agent_core::AgentEvent;
use bamboo_domain::subagent::{disabled_tools_for_profile, ToolPolicy};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// S-T4.1: the concise facade builder lets the caller freely choose which tools
/// to activate via `.tools([..])`, and that allowlist maps to `disabled_tools`
/// that exclude the non-listed tools (Edit/Write) while keeping the listed ones
/// (Read).
#[test]
fn s_t4_1_facade_builder_and_tool_selection() {
    // The builder accepts a free tool selection in the fluent chain.
    let _builder: AgentBuilder = Agent::builder()
        .model("test-model")
        .instruction("You are a careful research assistant.")
        .tools(["Read", "Grep", "WebSearch"]);

    // `.tool(..)` appends a single tool to the active selection.
    let _builder2: AgentBuilder = Agent::builder().tools(["Read"]).tool("WebSearch");

    // Selecting an allowlist translates into disabled_tools over the canonical
    // tool surface: Read stays enabled; Edit/Write are disabled.
    let policy = ToolPolicy::Allowlist {
        allow: vec![
            "Read".to_string(),
            "Grep".to_string(),
            "WebSearch".to_string(),
        ],
    };
    let disabled = disabled_tools_for_profile(&policy, &builtin_tool_names());
    assert!(disabled.iter().any(|t| t == "Edit"));
    assert!(disabled.iter().any(|t| t == "Write"));
    assert!(!disabled.iter().any(|t| t == "Read"));
}

/// S-T4.2: `ExecuteRequestBuilder` round-trip — required fields are enforced at
/// construction, every optional field defaults to `None`, and setters round
/// trip into the materialized `ExecuteRequest`.
#[test]
fn s_t4_2_execute_request_builder_round_trip() {
    let (tx, _rx) = mpsc::channel::<AgentEvent>(8);
    let req = ExecuteRequestBuilder::new("hello", tx, CancellationToken::new()).build();

    assert_eq!(req.initial_message, "hello");
    // All optional fields default to None.
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

    // Setters round-trip.
    let (tx2, _rx2) = mpsc::channel::<AgentEvent>(8);
    let req2 = ExecuteRequestBuilder::new("go", tx2, CancellationToken::new())
        .model("claude-x")
        .provider_name("anthropic")
        .build();
    assert_eq!(req2.model.as_deref(), Some("claude-x"));
    assert_eq!(req2.provider_name.as_deref(), Some("anthropic"));

    // ToolSpec is derived from the canonical names.
    let names = builtin_tool_names();
    assert!(names.iter().any(|n| n == "Read"));
    let spec = ToolSpec::new("Read");
    assert_eq!(spec.name, "Read");
    assert!(!spec.disabled);
}

/// S-T4.3: `with_defaults_for_data_dir(tmp)` assembles the eight runtime
/// dependencies (storage, persistence, attachment reader, skill manager,
/// metrics collector, config, provider, default tools) and builds an `Agent`
/// from a plain instruction (no profile).
///
/// A pre-seeded config selects the `anthropic` provider with a non-network
/// api key — `create_provider` constructs it without performing any I/O, and
/// `SkillManager::initialize` + `MetricsCollector::spawn` run against the temp
/// data dir.
#[tokio::test]
async fn s_t4_3_with_defaults_for_data_dir_builds_agent() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let data_dir = tmp.path().to_path_buf();

    // Seed a config that selects a provider constructible without network I/O.
    // `api_key` is `skip_serializing` but still deserialized, so plaintext here
    // hydrates the in-memory field.
    let config_json = r#"{
        "provider": "anthropic",
        "providers": {
            "anthropic": {
                "api_key": "test-key",
                "model": "claude-test"
            }
        }
    }"#;
    std::fs::write(data_dir.join("config.json"), config_json).expect("write config");

    let agent = Agent::builder()
        .instruction("You are a coding assistant.")
        .model("claude-test")
        .with_defaults_for_data_dir(data_dir.clone())
        .await
        .expect("defaults should assemble")
        .build()
        .expect("agent should build");

    // Storage + persistence handles are live (deps were assembled).
    let _storage = agent.storage();
    let _persistence = agent.persistence();
}
