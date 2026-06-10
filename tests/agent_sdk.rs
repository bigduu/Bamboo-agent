//! Root SDK facade tests (ergonomic-sdk-plan §4: S-T4.1 .. S-T4.3).
//!
//! These exercise the `bamboo_agent::agent` facade end-to-end without network
//! I/O: the concise instruction/model/tools builder (built-in catalog + custom
//! tools), the `ExecuteRequest` builder, and default-dependency assembly via
//! `with_defaults_for_data_dir`.

use std::sync::Arc;

use bamboo_agent::agent::{
    builtin_tool_names, Agent, AgentBuilder, BuiltinTool, ExecuteRequestBuilder, ToolSpec,
};

use bamboo_agent_core::tools::{SharedTool, Tool, ToolError, ToolResult};
use bamboo_agent_core::AgentEvent;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// A trivial custom tool, to prove user-defined `impl Tool`s are accepted by the
/// builder alongside built-ins.
struct EchoTool;

#[async_trait::async_trait]
impl Tool for EchoTool {
    fn name(&self) -> &str {
        "echo"
    }
    fn description(&self) -> &str {
        "Echo the input back."
    }
    fn parameters_schema(&self) -> serde_json::Value {
        serde_json::json!({ "type": "object", "properties": {} })
    }
    async fn execute(&self, _args: serde_json::Value) -> Result<ToolResult, ToolError> {
        Ok(ToolResult {
            success: true,
            result: "echo".to_string(),
            display_preference: None,
            images: Vec::new(),
        })
    }
}

/// S-T4.1: the `BuiltinTool` catalog vends real tool instances by name, and the
/// builder freely accepts a mix of built-in instances and a custom `impl Tool`.
#[test]
fn s_t4_1_tool_catalog_and_custom_tool_selection() {
    // The catalog resolves each variant to a real Tool whose advertised name
    // matches the canonical name.
    let web: SharedTool = BuiltinTool::WebSearch.tool();
    assert_eq!(web.name(), "WebSearch");
    assert_eq!(BuiltinTool::Read.tool().name(), "Read");

    // `.tools([..])` takes actual tools (Arc<dyn Tool>); `.tool(..)` adds a
    // custom one. The fluent chain compiles and runs without panicking.
    let _builder: AgentBuilder = Agent::builder()
        .model("test-model")
        .instruction("You are a careful research assistant.")
        .tools([BuiltinTool::Read.tool(), BuiltinTool::WebSearch.tool()])
        .tool(EchoTool);
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
    assert!(req.model_roster.model.is_none());
    assert!(req.model_roster.provider_name.is_none());
    assert!(req.model_roster.provider_type.is_none());
    assert!(req.model_roster.fast.is_none());
    assert!(req.model_roster.fast_model_provider().is_none());
    assert!(req.model_roster.background.is_none());
    assert!(req.model_roster.background_model_provider().is_none());
    assert!(req.model_roster.summarization.is_none());
    assert!(req.model_roster.summarization_model_provider().is_none());
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
    assert_eq!(req2.model_roster.model.as_deref(), Some("claude-x"));
    assert_eq!(
        req2.model_roster.provider_name.as_deref(),
        Some("anthropic")
    );

    // ToolSpec is derived from the canonical names.
    let names = builtin_tool_names();
    assert!(names.iter().any(|n| n == "Read"));
    let spec = ToolSpec::new("Read");
    assert_eq!(spec.name, "Read");
    assert!(!spec.disabled);
}

/// S-T4.3: `with_defaults_for_data_dir(tmp)` assembles the eight runtime
/// dependencies and builds an `Agent` from a plain instruction plus a selected
/// tool set (a built-in + a custom tool), exercising the build-time executor
/// assembly path.
///
/// A pre-seeded config selects the `anthropic` provider with a non-network
/// api key — `create_provider` constructs it without performing any I/O, and
/// `SkillManager::initialize` + `MetricsCollector::spawn` run against the temp
/// data dir.
#[tokio::test]
async fn s_t4_3_with_defaults_for_data_dir_builds_agent() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let data_dir = tmp.path().to_path_buf();

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

    let custom: Arc<dyn Tool> = Arc::new(EchoTool);
    let agent = Agent::builder()
        .instruction("You are a coding assistant.")
        .model("claude-test")
        .tools([BuiltinTool::Read.tool()])
        .tool_shared(custom)
        .with_defaults_for_data_dir(data_dir.clone())
        .await
        .expect("defaults should assemble")
        .build()
        .expect("agent should build");

    // Storage + persistence handles are live (deps were assembled).
    let _storage = agent.storage();
    let _persistence = agent.persistence();
}
