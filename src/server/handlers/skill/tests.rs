use crate::agent::tools::BuiltinToolExecutor;

use super::tools::{resolve_session_identifier, select_tools_by_allowlist, to_openai_tools};
use super::types::FilteredToolsQuery;

#[test]
fn resolve_session_identifier_prefers_session_id() {
    let query = FilteredToolsQuery {
        session_id: Some("session-123".to_string()),
        chat_id: Some("chat-123".to_string()),
    };

    assert_eq!(resolve_session_identifier(&query), Some("session-123"));
}

#[test]
fn resolve_session_identifier_falls_back_to_chat_id() {
    let query = FilteredToolsQuery {
        session_id: None,
        chat_id: Some("chat-456".to_string()),
    };

    assert_eq!(resolve_session_identifier(&query), Some("chat-456"));
}

#[test]
fn select_tools_by_allowlist_returns_all_when_allowlist_is_empty() {
    let all_tools = BuiltinToolExecutor::tool_schemas();
    let expected_len = all_tools.len();

    let selected = select_tools_by_allowlist(all_tools, &[]);
    assert_eq!(selected.len(), expected_len);
}

#[test]
fn select_tools_by_allowlist_filters_to_exact_tool_names() {
    let all_tools = BuiltinToolExecutor::tool_schemas();
    let allowed_tool = all_tools
        .first()
        .expect("Builtin tools should not be empty")
        .function
        .name
        .clone();
    let allowlist = vec![allowed_tool.clone()];

    let selected = select_tools_by_allowlist(all_tools, &allowlist);
    assert!(!selected.is_empty());
    assert!(selected
        .iter()
        .all(|tool| tool.function.name == allowed_tool));
}

#[test]
fn to_openai_tools_maps_function_fields() {
    let mut all_tools = BuiltinToolExecutor::tool_schemas();
    let first = all_tools
        .drain(..1)
        .next()
        .expect("Builtin tools should not be empty");
    let expected_name = first.function.name.clone();
    let expected_description = first.function.description.clone();

    let mapped = to_openai_tools(vec![first]);
    assert_eq!(mapped.len(), 1);
    assert_eq!(mapped[0].tool_type, "function");
    assert_eq!(mapped[0].function.name, expected_name);
    assert_eq!(mapped[0].function.description, expected_description);
}
