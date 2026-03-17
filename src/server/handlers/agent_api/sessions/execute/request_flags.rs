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

    #[test]
    fn resolve_execution_flags_both_true() {
        let mut request = base_request();
        request.include_partial_messages = Some(true);
        request.dangerously_skip_permissions = Some(true);

        let (include_partial_messages, dangerously_skip_permissions) =
            resolve_execution_flags(&request);
        assert!(include_partial_messages);
        assert!(dangerously_skip_permissions);
    }

    #[test]
    fn resolve_execution_flags_mixed_true_false() {
        let mut request = base_request();
        request.include_partial_messages = Some(true);
        request.dangerously_skip_permissions = Some(false);

        let (include_partial_messages, dangerously_skip_permissions) =
            resolve_execution_flags(&request);
        assert!(include_partial_messages);
        assert!(!dangerously_skip_permissions);
    }

    #[test]
    fn resolve_execution_flags_mixed_false_true() {
        let mut request = base_request();
        request.include_partial_messages = Some(false);
        request.dangerously_skip_permissions = Some(true);

        let (include_partial_messages, dangerously_skip_permissions) =
            resolve_execution_flags(&request);
        assert!(!include_partial_messages);
        assert!(dangerously_skip_permissions);
    }

    #[test]
    fn resolve_execution_flags_only_include_partial_false() {
        let mut request = base_request();
        request.include_partial_messages = Some(false);
        // dangerously_skip_permissions remains None

        let (include_partial_messages, dangerously_skip_permissions) =
            resolve_execution_flags(&request);
        assert!(!include_partial_messages);
        assert!(dangerously_skip_permissions); // Default is true
    }

    #[test]
    fn resolve_execution_flags_only_include_partial_true() {
        let mut request = base_request();
        request.include_partial_messages = Some(true);
        // dangerously_skip_permissions remains None

        let (include_partial_messages, dangerously_skip_permissions) =
            resolve_execution_flags(&request);
        assert!(include_partial_messages);
        assert!(dangerously_skip_permissions); // Default is true
    }

    #[test]
    fn resolve_execution_flags_only_skip_permissions_false() {
        let mut request = base_request();
        // include_partial_messages remains None
        request.dangerously_skip_permissions = Some(false);

        let (include_partial_messages, dangerously_skip_permissions) =
            resolve_execution_flags(&request);
        assert!(include_partial_messages); // Default is true
        assert!(!dangerously_skip_permissions);
    }

    #[test]
    fn resolve_execution_flags_only_skip_permissions_true() {
        let mut request = base_request();
        // include_partial_messages remains None
        request.dangerously_skip_permissions = Some(true);

        let (include_partial_messages, dangerously_skip_permissions) =
            resolve_execution_flags(&request);
        assert!(include_partial_messages); // Default is true
        assert!(dangerously_skip_permissions);
    }

    #[test]
    fn resolve_execution_flags_handles_multiple_calls() {
        let mut request = base_request();

        // First call with defaults
        let (p1, s1) = resolve_execution_flags(&request);
        assert!(p1);
        assert!(s1);

        // Modify and call again
        request.include_partial_messages = Some(false);
        let (p2, s2) = resolve_execution_flags(&request);
        assert!(!p2);
        assert!(s2);

        // Modify again
        request.dangerously_skip_permissions = Some(false);
        let (p3, s3) = resolve_execution_flags(&request);
        assert!(!p3);
        assert!(!s3);
    }

    #[test]
    fn resolve_execution_flags_with_different_project_paths() {
        let request = ExecuteRequest {
            project_path: "/different/path".to_string(),
            prompt: "test".to_string(),
            session_id: Some("session-123".to_string()),
            anthropic_base_url: Some("http://localhost:8080".to_string()),
            json_schema: Some("{}".to_string()),
            dangerously_skip_permissions: Some(false),
            include_partial_messages: Some(true),
        };

        let (include_partial_messages, dangerously_skip_permissions) =
            resolve_execution_flags(&request);
        assert!(include_partial_messages);
        assert!(!dangerously_skip_permissions);
    }

    #[test]
    fn resolve_execution_flags_with_all_optional_fields_set() {
        let request = ExecuteRequest {
            project_path: "/project".to_string(),
            prompt: "Full request".to_string(),
            session_id: Some("session-456".to_string()),
            anthropic_base_url: Some("https://api.anthropic.com".to_string()),
            json_schema: Some(r#"{"type": "object"}"#.to_string()),
            dangerously_skip_permissions: Some(true),
            include_partial_messages: Some(false),
        };

        let (include_partial_messages, dangerously_skip_permissions) =
            resolve_execution_flags(&request);
        assert!(!include_partial_messages);
        assert!(dangerously_skip_permissions);
    }

    #[test]
    fn resolve_execution_flags_empty_prompt() {
        let mut request = base_request();
        request.prompt = String::new();
        request.include_partial_messages = Some(false);
        request.dangerously_skip_permissions = Some(false);

        let (include_partial_messages, dangerously_skip_permissions) =
            resolve_execution_flags(&request);
        assert!(!include_partial_messages);
        assert!(!dangerously_skip_permissions);
    }
}
