//! Root SDK facade tests (ergonomic-sdk-plan §4: S-T4.1 .. S-T4.3).
//!
//! These exercise the `bamboo_agent::agent` surface end-to-end without network
//! I/O: profile resolution, the `ExecuteRequest` builder, and default-dependency
//! assembly via `with_defaults_for_data_dir`.

use bamboo_agent::agent::{
    builtin_tool_names, Agent, AgentBuilder, ExecuteRequestBuilder, ToolSpec,
};
use bamboo_agent::agent::profiles::builtin_profiles;

use bamboo_agent_core::AgentEvent;
use bamboo_domain::subagent::{disabled_tools_for_profile, ToolPolicy};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// S-T4.1: `Agent::builder().researcher().model("m")` resolves the researcher
/// profile (system prompt + allowlist tool policy) and applies the model
/// override. We assert via the profile registry + the disabled-tools derivation
/// the builder will perform (a researcher allowlist disables Edit/Write).
#[test]
fn s_t4_1_researcher_profile_and_model_override() {
    // The builder is profile-driven; verify the registry the builder consults.
    let researcher = builtin_profiles()
        .into_iter()
        .find(|p| p.id == "researcher")
        .expect("researcher profile must exist");

    // The researcher uses an allowlist (read-only + web + memory).
    let allow = match &researcher.tools {
        ToolPolicy::Allowlist { allow } => allow.clone(),
        other => panic!("expected allowlist, got {other:?}"),
    };
    assert!(allow.contains(&"Read".to_string()));
    assert!(!allow.iter().any(|t| t == "Edit" || t == "Write"));

    // The system prompt is non-empty and researcher-specific.
    assert!(researcher.system_prompt.contains("Researcher"));

    // The builder translates the allowlist into disabled_tools over the
    // canonical tool surface: Edit/Write must be disabled, Read must not.
    let disabled = disabled_tools_for_profile(&researcher.tools, &builtin_tool_names());
    assert!(disabled.iter().any(|t| t == "Edit"));
    assert!(disabled.iter().any(|t| t == "Write"));
    assert!(!disabled.iter().any(|t| t == "Read"));

    // The builder accepts the fluent chain without panicking; model override is
    // carried through to build time.
    let _builder: AgentBuilder = Agent::builder().researcher().model("test-model");
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
/// metrics collector, config, provider, default tools) and builds an `Agent`.
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
        .coder()
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
