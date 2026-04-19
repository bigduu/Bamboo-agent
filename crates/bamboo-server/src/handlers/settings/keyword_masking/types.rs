use bamboo_infrastructure::keyword_masking::KeywordEntry;
use serde::{Deserialize, Serialize};

/// Response for keyword masking configuration.
#[derive(Debug, Serialize, Deserialize)]
pub(super) struct KeywordMaskingResponse {
    pub(super) entries: Vec<KeywordEntry>,
}

impl KeywordMaskingResponse {
    pub(super) fn new(entries: Vec<KeywordEntry>) -> Self {
        Self { entries }
    }
}

/// Validation error for keyword entries.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct ValidationError {
    pub(super) index: usize,
    pub(super) message: String,
}

pub(super) fn map_validation_errors(errors: Vec<(usize, String)>) -> Vec<ValidationError> {
    errors
        .into_iter()
        .map(|(idx, msg)| ValidationError {
            index: idx,
            message: msg,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_infrastructure::keyword_masking::{KeywordEntry, MatchType};

    fn create_test_entry(pattern: &str) -> KeywordEntry {
        KeywordEntry {
            pattern: pattern.to_string(),
            match_type: MatchType::Exact,
            enabled: true,
        }
    }

    #[test]
    fn keyword_masking_response_new_creates_response_with_entries() {
        let entries = vec![create_test_entry("secret"), create_test_entry("password")];

        let response = KeywordMaskingResponse::new(entries.clone());

        assert_eq!(response.entries.len(), 2);
        assert_eq!(response.entries[0].pattern, "secret");
        assert_eq!(response.entries[1].pattern, "password");
    }

    #[test]
    fn keyword_masking_response_new_with_empty_entries() {
        let entries: Vec<KeywordEntry> = vec![];
        let response = KeywordMaskingResponse::new(entries);

        assert_eq!(response.entries.len(), 0);
    }

    #[test]
    fn keyword_masking_response_new_preserves_single_entry() {
        let entry = create_test_entry("test");

        let response = KeywordMaskingResponse::new(vec![entry.clone()]);

        assert_eq!(response.entries.len(), 1);
        assert_eq!(response.entries[0].pattern, entry.pattern);
        assert_eq!(response.entries[0].enabled, entry.enabled);
    }

    #[test]
    fn keyword_masking_response_can_be_serialized() {
        let entries = vec![create_test_entry("secret")];

        let response = KeywordMaskingResponse::new(entries);
        let json = serde_json::to_string(&response);

        assert!(json.is_ok());
        assert!(json.unwrap().contains("secret"));
    }

    #[test]
    fn keyword_masking_response_can_be_deserialized() {
        let json = r#"{"entries":[{"pattern":"test","match_type":"exact","enabled":true}]}"#;
        let response: Result<KeywordMaskingResponse, _> = serde_json::from_str(json);

        assert!(response.is_ok());
        let response = response.unwrap();
        assert_eq!(response.entries.len(), 1);
        assert_eq!(response.entries[0].pattern, "test");
    }

    #[test]
    fn validation_error_can_be_created() {
        let error = ValidationError {
            index: 5,
            message: "Invalid keyword".to_string(),
        };

        assert_eq!(error.index, 5);
        assert_eq!(error.message, "Invalid keyword");
    }

    #[test]
    fn validation_equality_works_correctly() {
        let error1 = ValidationError {
            index: 1,
            message: "test".to_string(),
        };
        let error2 = ValidationError {
            index: 1,
            message: "test".to_string(),
        };
        let error3 = ValidationError {
            index: 2,
            message: "test".to_string(),
        };

        assert_eq!(error1, error2);
        assert_ne!(error1, error3);
    }

    #[test]
    fn validation_error_can_be_serialized() {
        let error = ValidationError {
            index: 3,
            message: "Duplicate keyword".to_string(),
        };

        let json = serde_json::to_string(&error);

        assert!(json.is_ok());
        let json_str = json.unwrap();
        assert!(json_str.contains("3"));
        assert!(json_str.contains("Duplicate keyword"));
    }

    #[test]
    fn validation_error_can_be_deserialized() {
        let json = r#"{"index":7,"message":"Empty keyword"}"#;
        let error: Result<ValidationError, _> = serde_json::from_str(json);

        assert!(error.is_ok());
        let error = error.unwrap();
        assert_eq!(error.index, 7);
        assert_eq!(error.message, "Empty keyword");
    }

    #[test]
    fn map_validation_errors_converts_empty_vector() {
        let errors: Vec<(usize, String)> = vec![];
        let result = map_validation_errors(errors);

        assert_eq!(result.len(), 0);
    }

    #[test]
    fn map_validation_errors_converts_single_error() {
        let errors = vec![(0, "Empty keyword".to_string())];
        let result = map_validation_errors(errors);

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].index, 0);
        assert_eq!(result[0].message, "Empty keyword");
    }

    #[test]
    fn map_validation_errors_converts_multiple_errors() {
        let errors = vec![
            (0, "Empty keyword".to_string()),
            (2, "Duplicate keyword".to_string()),
            (5, "Invalid replacement".to_string()),
        ];
        let result = map_validation_errors(errors);

        assert_eq!(result.len(), 3);
        assert_eq!(result[0].index, 0);
        assert_eq!(result[1].index, 2);
        assert_eq!(result[2].index, 5);
    }

    #[test]
    fn map_validation_errors_preserves_order() {
        let errors = vec![
            (10, "Error at 10".to_string()),
            (1, "Error at 1".to_string()),
            (5, "Error at 5".to_string()),
        ];
        let result = map_validation_errors(errors);

        // Order should be preserved
        assert_eq!(result[0].index, 10);
        assert_eq!(result[1].index, 1);
        assert_eq!(result[2].index, 5);
    }

    #[test]
    fn map_validation_errors_handles_large_index() {
        let errors = vec![(1000000, "Large index".to_string())];
        let result = map_validation_errors(errors);

        assert_eq!(result[0].index, 1000000);
    }

    #[test]
    fn map_validation_errors_handles_empty_message() {
        let errors = vec![(3, "".to_string())];
        let result = map_validation_errors(errors);

        assert_eq!(result[0].message, "");
    }

    #[test]
    fn map_validation_errors_handles_unicode_message() {
        let errors = vec![(1, "错误消息 🎯".to_string())];
        let result = map_validation_errors(errors);

        assert_eq!(result[0].message, "错误消息 🎯");
    }

    #[test]
    fn map_validation_errors_handles_long_message() {
        let long_message = "This is a very long error message. ".repeat(20);
        let errors = vec![(0, long_message.clone())];
        let result = map_validation_errors(errors);

        assert_eq!(result[0].message, long_message);
    }

    #[test]
    fn validation_error_debug_trait_works() {
        let error = ValidationError {
            index: 1,
            message: "test".to_string(),
        };

        let debug_str = format!("{:?}", error);

        assert!(debug_str.contains("ValidationError"));
        assert!(debug_str.contains("index"));
        assert!(debug_str.contains("message"));
    }

    #[test]
    fn validation_error_clone_works() {
        let error = ValidationError {
            index: 5,
            message: "original".to_string(),
        };

        let cloned = error.clone();

        assert_eq!(error, cloned);
        assert_eq!(cloned.index, 5);
        assert_eq!(cloned.message, "original");
    }
}
