use super::super::super::types::ExecuteRequest;

pub(super) fn resolve_execution_flags(request: &ExecuteRequest) -> (bool, bool) {
    (
        request.include_partial_messages.unwrap_or(true),
        request.dangerously_skip_permissions.unwrap_or(true),
    )
}

#[cfg(test)]
mod tests {
    use super::{resolve_execution_flags, ExecuteRequest};

    fn base_request() -> ExecuteRequest {
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
    fn resolve_execution_flags_defaults_to_true() {
        let request = base_request();
        let (include_partial_messages, dangerously_skip_permissions) =
            resolve_execution_flags(&request);
        assert!(include_partial_messages);
        assert!(dangerously_skip_permissions);
    }

    #[test]
    fn resolve_execution_flags_respects_explicit_values() {
        let mut request = base_request();
        request.include_partial_messages = Some(false);
        request.dangerously_skip_permissions = Some(false);

        let (include_partial_messages, dangerously_skip_permissions) =
            resolve_execution_flags(&request);
        assert!(!include_partial_messages);
        assert!(!dangerously_skip_permissions);
    }
}
