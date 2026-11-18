use std::sync::Arc;

use bamboo_agent_core::tools::{FunctionCall, FunctionSchema, ToolCall, ToolSchema};
use bamboo_agent_core::{Message, Session};
use bamboo_config::RequestOverridesConfig;
use bamboo_domain::DISCOVERY_CONTROL_FALLBACK_TOOL_NAME;
use bamboo_llm::providers::BodhiProvider;
use bamboo_llm::{AnthropicProvider, GeminiProvider, LLMProvider, OpenAIProvider};
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;
use wiremock::matchers::method;
use wiremock::{Mock, MockServer, ResponseTemplate};

use crate::runtime::config::AgentLoopConfig;

const MODEL: &str = "fixture-model";
const DENIED_SCHEMA_MARKER: &str = "PRIVATE_TOOL_DEFINITION_TEST_MARKER";

fn schema(name: &str) -> ToolSchema {
    ToolSchema {
        schema_type: "function".into(),
        function: FunctionSchema {
            name: name.into(),
            description: if name == "load_skill" {
                DENIED_SCHEMA_MARKER.into()
            } else {
                format!("Use {name}")
            },
            parameters: json!({"type":"object","properties":{},"additionalProperties":false}),
        },
    }
}

fn record_discovery(session: &mut Session, definitions: &[ToolSchema]) {
    session.add_message(Message::assistant(
        "",
        Some(vec![ToolCall {
            id: "discovery".into(),
            tool_type: "function".into(),
            function: FunctionCall {
                name: DISCOVERY_CONTROL_FALLBACK_TOOL_NAME.into(),
                arguments: r#"{"query":"files"}"#.into(),
            },
        }]),
    ));
    let mut result = Message::tool_result_with_status(
        "discovery",
        format!(
            "<loaded_tools>{}</loaded_tools>",
            json!({"tools": definitions})
        ),
        true,
    );
    result.never_compress = true;
    result.metadata = Some(json!({
        "runtime_kind":"sticky_capability_discovery",
        "version":1,
        "canonical_new_names":definitions.iter().map(|tool| &tool.function.name).collect::<Vec<_>>(),
    }));
    session.add_message(result);
}

async fn send(session: &mut Session, provider: &Arc<dyn LLMProvider>, catalog: &[ToolSchema]) {
    let config = AgentLoopConfig {
        model_name: Some(MODEL.into()),
        ..Default::default()
    };
    let (events, _receiver) = tokio::sync::mpsc::channel(128);
    let session_id = session.id.clone();
    let result = super::execute_llm_round(
        session,
        &config,
        provider,
        &events,
        &CancellationToken::new(),
        &session_id,
        MODEL,
        catalog,
        None,
        None,
        None,
    )
    .await;
    assert!(
        result.is_err(),
        "the fixture rejects every generation request"
    );
}

async fn capture(server: &MockServer) -> Vec<Value> {
    server
        .received_requests()
        .await
        .unwrap()
        .into_iter()
        .filter(|request| request.method.as_str() == "POST")
        .map(|request| request.body_json().unwrap())
        .collect()
}

fn tool_names(body: &Value) -> Vec<&str> {
    body["tools"]
        .as_array()
        .into_iter()
        .flatten()
        .flat_map(|entry| {
            if let Some(declarations) = entry["functionDeclarations"].as_array() {
                declarations
                    .iter()
                    .map(|definition| definition["name"].as_str().unwrap())
                    .collect()
            } else {
                vec![entry
                    .get("name")
                    .unwrap_or(&entry["function"]["name"])
                    .as_str()
                    .unwrap()]
            }
        })
        .collect()
}

#[tokio::test]
async fn roles_use_on_demand_declarations_on_every_compatibility_wire() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(400).set_body_string("fixture request captured"))
        .mount(&server)
        .await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let providers: Vec<Arc<dyn LLMProvider>> = vec![
        Arc::new(
            OpenAIProvider::new("fixture")
                .with_base_url(server.uri())
                .with_client(client.clone())
                .with_sticky_tool_loading(false),
        ),
        Arc::new(
            OpenAIProvider::new("fixture")
                .with_base_url(server.uri())
                .with_client(client.clone())
                .with_responses_only_models(vec![MODEL.into()]),
        ),
        Arc::new(
            AnthropicProvider::new("fixture")
                .with_base_url(server.uri())
                .with_client(client.clone()),
        ),
        Arc::new(
            GeminiProvider::new("fixture")
                .with_base_url(server.uri())
                .with_client(client.clone()),
        ),
        Arc::new(
            BodhiProvider::new("fixture")
                .with_base_url(server.uri())
                .with_client(client.clone()),
        ),
        Arc::new(
            BodhiProvider::new("fixture")
                .with_base_url(server.uri())
                .with_client(client.clone())
                .with_target_provider("anthropic"),
        ),
        Arc::new(
            BodhiProvider::new("fixture")
                .with_base_url(server.uri())
                .with_client(client)
                .with_target_provider("gemini"),
        ),
    ];
    let catalog = ["Read", "Glob", "GetFileInfo", "load_skill"].map(schema);
    for (index, provider) in providers.iter().enumerate() {
        let mut session = Session::new(&format!("wire-role-{index}"), MODEL);
        session.add_message(Message::user("Find a file"));
        session.set_root_orchestration_only(true).unwrap();
        send(&mut session, provider, &catalog).await;
        record_discovery(&mut session, &[catalog[1].clone(), catalog[3].clone()]);
        send(&mut session, provider, &catalog).await;
    }
    let bodies = capture(&server).await;
    assert_eq!(bodies.len(), providers.len() * 2);
    for pair in bodies.chunks_exact(2) {
        assert_eq!(tool_names(&pair[0]), ["Read", "discover_capabilities"]);
        assert_eq!(
            tool_names(&pair[1]),
            ["Read", "Glob", "discover_capabilities"]
        );
        assert!(pair
            .iter()
            .all(|body| !body.to_string().contains(DENIED_SCHEMA_MARKER)));
    }
}

#[tokio::test]
async fn named_and_allowlist_choices_reach_the_wire_only_with_eligible_declarations() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(400).set_body_string("fixture request captured"))
        .mount(&server)
        .await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let catalog = ["Read", "Glob", "GetFileInfo", "load_skill"].map(schema);
    let config = |path: &str, value: Value| -> RequestOverridesConfig {
        serde_json::from_value(json!({"common":{"body_patch":[{"path":path,"value":value}]}}))
            .unwrap()
    };
    let named = json!({"type":"function","function":{"name":"Glob"}});
    let allowed = json!({"type":"allowed_tools","mode":"required","tools":[
        {"type":"function","name":"Glob"}, {"type":"function","name":"GetFileInfo"}
    ]});
    let anth = json!({"type":"tool","name":"Glob"});
    let gemini =
        json!({"functionCallingConfig":{"mode":"ANY","allowedFunctionNames":["GetFileInfo"]}});
    let providers: Vec<(Arc<dyn LLMProvider>, &str, Value, Vec<&str>)> = vec![
        (
            Arc::new(
                OpenAIProvider::new("fixture")
                    .with_base_url(server.uri())
                    .with_client(client.clone())
                    .with_request_overrides(Some(config("tool_choice", named.clone()))),
            ),
            "tool_choice",
            named,
            vec!["Glob"],
        ),
        (
            Arc::new(
                OpenAIProvider::new("fixture")
                    .with_base_url(server.uri())
                    .with_client(client.clone())
                    .with_responses_only_models(vec![MODEL.into()])
                    .with_request_overrides(Some(config("tool_choice", allowed.clone()))),
            ),
            "tool_choice",
            allowed,
            vec!["Glob", "GetFileInfo"],
        ),
        (
            Arc::new(
                AnthropicProvider::new("fixture")
                    .with_base_url(server.uri())
                    .with_client(client.clone())
                    .with_request_overrides(Some(config("tool_choice", anth.clone()))),
            ),
            "tool_choice",
            anth,
            vec!["Glob"],
        ),
        (
            Arc::new(
                GeminiProvider::new("fixture")
                    .with_base_url(server.uri())
                    .with_client(client.clone())
                    .with_request_overrides(Some(config("toolConfig", gemini.clone()))),
            ),
            "toolConfig",
            gemini,
            vec!["GetFileInfo"],
        ),
    ];
    for (provider, _, _, _) in &providers {
        let mut session = Session::new("constrained-wire", MODEL);
        session.add_message(Message::user("Find a file"));
        session.set_root_orchestration_only(true).unwrap();
        send(&mut session, provider, &catalog).await;
    }
    let mut forced = Session::new("forced-load-skill", MODEL);
    forced.add_message(Message::user("Run the selected skill"));
    forced.metadata.insert(
        bamboo_skills::runtime_metadata::SKILL_RUNTIME_SELECTION_SOURCE_KEY.into(),
        "explicit".into(),
    );
    forced.metadata.insert(
        bamboo_skills::runtime_metadata::SKILL_RUNTIME_SELECTED_SKILL_IDS_KEY.into(),
        r#"["fixture-skill"]"#.into(),
    );
    assert_eq!(
        super::required_tool_for_session(&forced),
        Some("load_skill")
    );
    send(&mut forced, &providers[0].0, &catalog).await;

    let denied: Arc<dyn LLMProvider> = Arc::new(
        OpenAIProvider::new("fixture")
            .with_base_url(server.uri())
            .with_client(client)
            .with_request_overrides(Some(config(
                "tool_choice",
                json!({
                    "type":"allowed_tools","allowed_tools":{"mode":"required","tools":[
                        {"type":"function","function":{"name":"Glob"}},
                        {"type":"function","function":{"name":"load_skill"}}
                    ]}
                }),
            ))),
    );
    let mut ultra = Session::new("denied-wire", MODEL);
    ultra.set_root_orchestration_only(true).unwrap();
    ultra.add_message(Message::user("Find a file"));
    send(&mut ultra, &denied, &catalog).await;

    let bodies = capture(&server).await;
    assert_eq!(
        bodies.len(),
        providers.len(),
        "the denied choice must fail before HTTP"
    );
    for (body, (_, key, choice, names)) in bodies.iter().zip(providers) {
        assert_eq!(body[key], choice);
        assert_eq!(tool_names(body), names);
    }
}
