mod cancel;
mod events_sse;
mod execute;
mod jsonl;
mod running;

pub use cancel::cancel_claude_execution;
pub use events_sse::claude_events;
pub use execute::execute_claude_code;
pub use jsonl::get_session_jsonl;
pub use running::{list_running_claude_sessions, list_running_claude_sessions_stateful};

use uuid::Uuid;

use super::types::ExecuteRequest;

fn resolve_session_ids(requested_session_id: Option<String>) -> (String, String, bool) {
    let client_session_id = requested_session_id.unwrap_or_else(|| Uuid::new_v4().to_string());

    match Uuid::parse_str(&client_session_id) {
        Ok(_) => (client_session_id.clone(), client_session_id, false),
        Err(_) => (client_session_id, Uuid::new_v4().to_string(), true),
    }
}

fn resolve_anthropic_base_url(request: &ExecuteRequest, port: u16) -> String {
    request
        .anthropic_base_url
        .clone()
        .unwrap_or_else(|| format!("http://127.0.0.1:{}/anthropic", port))
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::{resolve_anthropic_base_url, resolve_session_ids};
    use crate::server::handlers::agent_api::ExecuteRequest;

    fn base_execute_request() -> ExecuteRequest {
        ExecuteRequest {
            project_path: "/tmp".to_string(),
            prompt: "hello".to_string(),
            session_id: None,
            anthropic_base_url: None,
            json_schema: None,
            dangerously_skip_permissions: None,
            include_partial_messages: None,
        }
    }

    #[test]
    fn resolve_session_ids_keeps_uuid_input() {
        let input = Uuid::new_v4().to_string();
        let (client, claude, alias_used) = resolve_session_ids(Some(input.clone()));

        assert_eq!(client, input);
        assert_eq!(claude, input);
        assert!(!alias_used);
    }

    #[test]
    fn resolve_session_ids_generates_uuid_for_non_uuid_alias() {
        let (client, claude, alias_used) = resolve_session_ids(Some("friendly-name".to_string()));

        assert_eq!(client, "friendly-name");
        assert!(Uuid::parse_str(&claude).is_ok());
        assert!(alias_used);
    }

    #[test]
    fn resolve_anthropic_base_url_prefers_explicit_value() {
        let mut request = base_execute_request();
        request.anthropic_base_url = Some("http://example.com/anthropic".to_string());

        let url = resolve_anthropic_base_url(&request, 3000);
        assert_eq!(url, "http://example.com/anthropic");
    }

    #[test]
    fn resolve_anthropic_base_url_uses_local_default() {
        let request = base_execute_request();

        let url = resolve_anthropic_base_url(&request, 3000);
        assert_eq!(url, "http://127.0.0.1:3000/anthropic");
    }
}
