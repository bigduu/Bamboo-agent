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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_respond_request_deserialization() {
        let json = r#"{"response":"yes"}"#;
        let req: RespondRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.response, "yes");
    }

    #[test]
    fn test_respond_request_with_custom_text() {
        let json = r#"{"response":"I choose option 3"}"#;
        let req: RespondRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.response, "I choose option 3");
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
}
