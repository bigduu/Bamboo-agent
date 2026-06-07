use bamboo_domain::reasoning::ReasoningEffort;
use bamboo_domain::ProviderModelRef;
use serde::Deserialize;

/// Request payload for submitting a user response.
///
/// # Fields
///
/// * `response` - The user's response text or selected option
#[derive(Debug, Deserialize)]
pub struct RespondRequest {
    /// The user's response - either one of the options or custom input
    pub response: String,
    /// Optional model to auto-resume execution after recording response.
    pub model: Option<String>,
    /// Optional provider name for model resolution.
    #[serde(default)]
    pub provider: Option<String>,
    /// Optional provider+model reference (takes priority when present).
    #[serde(default)]
    pub model_ref: Option<ProviderModelRef>,
    /// Optional reasoning effort to use when auto-resuming execution.
    /// Falls back to the value stored in session metadata if not provided.
    #[serde(default)]
    pub reasoning_effort: Option<ReasoningEffort>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_respond_request_deserialization() {
        let json = r#"{"response":"yes"}"#;
        let req: RespondRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.response, "yes");
        assert_eq!(req.model, None);
    }

    #[test]
    fn test_respond_request_with_custom_text() {
        let json = r#"{"response":"I choose option 3"}"#;
        let req: RespondRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.response, "I choose option 3");
        assert_eq!(req.model, None);
    }

    #[test]
    fn test_respond_request_with_optional_model() {
        let json = r#"{"response":"yes","model":"gpt-5-mini"}"#;
        let req: RespondRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.response, "yes");
        assert_eq!(req.model.as_deref(), Some("gpt-5-mini"));
    }

    #[test]
    fn test_respond_request_with_multiline() {
        let json = r#"{"response":"This is a\nmultiline\nresponse"}"#;
        let req: RespondRequest = serde_json::from_str(json).unwrap();
        assert!(req.response.contains("multiline"));
    }

    #[test]
    fn test_respond_request_debug() {
        let req = RespondRequest {
            response: "test response".to_string(),
            model: None,
            provider: None,
            model_ref: None,
            reasoning_effort: None,
        };
        let debug_str = format!("{:?}", req);
        assert!(debug_str.contains("RespondRequest"));
        assert!(debug_str.contains("test response"));
    }

    #[test]
    fn test_respond_request_empty_string() {
        let json = r#"{"response":""}"#;
        let req: RespondRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.response, "");
    }

    #[test]
    fn test_respond_request_whitespace() {
        let json = r#"{"response":"   "}"#;
        let req: RespondRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.response, "   ");
    }

    #[test]
    fn test_respond_request_with_reasoning_effort() {
        let json = r#"{"response":"yes","model":"gpt-5-mini","reasoning_effort":"high"}"#;
        let req: RespondRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.response, "yes");
        assert_eq!(req.model.as_deref(), Some("gpt-5-mini"));
        assert_eq!(
            req.reasoning_effort,
            Some(bamboo_domain::reasoning::ReasoningEffort::High)
        );
    }

    #[test]
    fn test_respond_request_without_reasoning_effort() {
        let json = r#"{"response":"yes","model":"gpt-5-mini"}"#;
        let req: RespondRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.reasoning_effort, None);
    }
}
