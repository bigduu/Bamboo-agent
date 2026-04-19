use actix_web::{test, web, App};
use bamboo_agent::agent::{Message, Session};
use bamboo_agent_core::tools::{FunctionCall, ToolCall};
use bamboo_agent::server::handlers;

#[actix_web::test]
async fn test_metrics_usage_breakdown_endpoint() {
    let state = crate::e2e::common::create_test_app().await;

    let mut session = Session::new("usage-session-1", "gpt-5");
    session.created_at = chrono::DateTime::parse_from_rfc3339("2026-04-02T10:00:00Z")
        .expect("valid timestamp")
        .with_timezone(&chrono::Utc);
    session.updated_at = session.created_at;
    session.messages.push(Message::assistant(
        "",
        Some(vec![ToolCall {
            id: "tool-1".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "load_skill".to_string(),
                arguments: r#"{"skill_id":"ui-styling"}"#.to_string(),
            },
        }]),
    ));
    session.messages.push(Message::assistant(
        "",
        Some(vec![ToolCall {
            id: "tool-2".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "mcp__playwright__browser_snapshot".to_string(),
                arguments: "{}".to_string(),
            },
        }]),
    ));
    session.messages.push(Message::assistant(
        "",
        Some(vec![ToolCall {
            id: "tool-3".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "Read".to_string(),
                arguments: r#"{"file_path":"/tmp/demo.txt"}"#.to_string(),
            },
        }]),
    ));
    state
        .storage
        .save_session(&session)
        .await
        .expect("save session");

    let app = test::init_service(App::new().app_data(state).route(
        "/api/v1/metrics/usage-breakdown",
        web::get().to(handlers::metrics::usage_breakdown),
    ))
    .await;

    let req = test::TestRequest::get()
        .uri("/api/v1/metrics/usage-breakdown?start_date=2026-04-01&end_date=2026-04-03")
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());

    let body: bamboo_agent::server::handlers::agent::metrics::MetricsUsageBreakdownResponse =
        test::read_body_json(resp).await;

    assert_eq!(body.total_sessions, 1);
    assert_eq!(body.total_tool_calls, 3);
    assert_eq!(body.skill_load_calls, 1);
    assert_eq!(body.mcp_calls, 1);
    assert_eq!(body.core_tool_calls, 1);
    assert_eq!(body.unique_skills, 1);
    assert_eq!(body.unique_mcp_servers, 1);
    assert_eq!(body.unique_mcp_tools, 1);
    assert_eq!(body.sessions_with_skill_loads, 1);
    assert_eq!(body.sessions_with_mcp_calls, 1);
    assert_eq!(
        body.top_skills.first().map(|item| item.skill_id.as_str()),
        Some("ui-styling")
    );
    assert_eq!(
        body.top_mcp_servers
            .first()
            .map(|item| item.server_id.as_str()),
        Some("playwright")
    );
    assert_eq!(
        body.top_mcp_tools.first().map(|item| item.alias.as_str()),
        Some("mcp__playwright__browser_snapshot")
    );
    assert_eq!(
        body.top_core_tools.first().map(|item| item.name.as_str()),
        Some("Read")
    );
}
