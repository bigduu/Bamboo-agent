use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Anthropic model mapping configuration.
///
/// Used to map OpenAI-compatible model ids (e.g. "claude-3-opus") to the actual
/// upstream Anthropic model id that should be used by the provider.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AnthropicModelMapping {
    #[serde(default)]
    pub mappings: HashMap<String, String>,
}

/// Gemini model mapping configuration.
///
/// Used to map OpenAI-compatible model ids (e.g. "gemini-pro") to the actual
/// upstream Gemini model id that should be used by the provider.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GeminiModelMapping {
    #[serde(default)]
    pub mappings: HashMap<String, String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_anthropic_model_mapping_default() {
        let mapping = AnthropicModelMapping::default();
        assert!(mapping.mappings.is_empty());
    }

    #[test]
    fn test_gemini_model_mapping_default() {
        let mapping = GeminiModelMapping::default();
        assert!(mapping.mappings.is_empty());
    }

    #[test]
    fn test_anthropic_model_mapping_with_entries() {
        let mut mappings = HashMap::new();
        mappings.insert(
            "claude-3-opus".to_string(),
            "claude-3-opus-20240229".to_string(),
        );
        mappings.insert(
            "claude-3-sonnet".to_string(),
            "claude-3-sonnet-20240229".to_string(),
        );

        let mapping = AnthropicModelMapping { mappings };
        assert_eq!(mapping.mappings.len(), 2);
        assert_eq!(
            mapping.mappings.get("claude-3-opus"),
            Some(&"claude-3-opus-20240229".to_string())
        );
    }

    #[test]
    fn test_gemini_model_mapping_with_entries() {
        let mut mappings = HashMap::new();
        mappings.insert("gemini-pro".to_string(), "gemini-1.5-pro".to_string());
        mappings.insert("gemini-flash".to_string(), "gemini-1.5-flash".to_string());

        let mapping = GeminiModelMapping { mappings };
        assert_eq!(mapping.mappings.len(), 2);
        assert_eq!(
            mapping.mappings.get("gemini-pro"),
            Some(&"gemini-1.5-pro".to_string())
        );
    }

    #[test]
    fn test_anthropic_model_mapping_serialization() {
        let mut mappings = HashMap::new();
        mappings.insert("test-model".to_string(), "actual-model".to_string());

        let mapping = AnthropicModelMapping { mappings };
        let json = serde_json::to_string(&mapping).unwrap();

        assert!(json.contains("test-model"));
        assert!(json.contains("actual-model"));
    }

    #[test]
    fn test_gemini_model_mapping_serialization() {
        let mut mappings = HashMap::new();
        mappings.insert("gemini-test".to_string(), "gemini-actual".to_string());

        let mapping = GeminiModelMapping { mappings };
        let json = serde_json::to_string(&mapping).unwrap();

        assert!(json.contains("gemini-test"));
        assert!(json.contains("gemini-actual"));
    }

    #[test]
    fn test_anthropic_model_mapping_deserialization() {
        let json = r#"{"mappings":{"key":"value"}}"#;
        let mapping: AnthropicModelMapping = serde_json::from_str(json).unwrap();

        assert_eq!(mapping.mappings.len(), 1);
        assert_eq!(mapping.mappings.get("key"), Some(&"value".to_string()));
    }

    #[test]
    fn test_gemini_model_mapping_deserialization() {
        let json = r#"{"mappings":{"g-key":"g-value"}}"#;
        let mapping: GeminiModelMapping = serde_json::from_str(json).unwrap();

        assert_eq!(mapping.mappings.len(), 1);
        assert_eq!(mapping.mappings.get("g-key"), Some(&"g-value".to_string()));
    }

    #[test]
    fn test_anthropic_model_mapping_empty_json() {
        let json = r#"{}"#;
        let mapping: AnthropicModelMapping = serde_json::from_str(json).unwrap();

        assert!(mapping.mappings.is_empty());
    }

    #[test]
    fn test_gemini_model_mapping_empty_json() {
        let json = r#"{}"#;
        let mapping: GeminiModelMapping = serde_json::from_str(json).unwrap();

        assert!(mapping.mappings.is_empty());
    }

    #[test]
    fn test_anthropic_model_mapping_clone() {
        let mut mappings = HashMap::new();
        mappings.insert("key".to_string(), "value".to_string());

        let mapping = AnthropicModelMapping { mappings };
        let cloned = mapping.clone();

        assert_eq!(mapping.mappings, cloned.mappings);
    }

    #[test]
    fn test_gemini_model_mapping_clone() {
        let mut mappings = HashMap::new();
        mappings.insert("gkey".to_string(), "gvalue".to_string());

        let mapping = GeminiModelMapping { mappings };
        let cloned = mapping.clone();

        assert_eq!(mapping.mappings, cloned.mappings);
    }

    #[test]
    fn test_anthropic_model_mapping_debug() {
        let mapping = AnthropicModelMapping::default();
        let debug_str = format!("{:?}", mapping);

        assert!(debug_str.contains("AnthropicModelMapping"));
    }

    #[test]
    fn test_gemini_model_mapping_debug() {
        let mapping = GeminiModelMapping::default();
        let debug_str = format!("{:?}", mapping);

        assert!(debug_str.contains("GeminiModelMapping"));
    }

    #[test]
    fn test_anthropic_model_mapping_multiple_entries() {
        let mut mappings = HashMap::new();
        for i in 0..10 {
            mappings.insert(format!("model-{}", i), format!("actual-model-{}", i));
        }

        let mapping = AnthropicModelMapping { mappings };
        assert_eq!(mapping.mappings.len(), 10);
    }

    #[test]
    fn test_gemini_model_mapping_multiple_entries() {
        let mut mappings = HashMap::new();
        for i in 0..10 {
            mappings.insert(format!("gemini-{}", i), format!("gemini-actual-{}", i));
        }

        let mapping = GeminiModelMapping { mappings };
        assert_eq!(mapping.mappings.len(), 10);
    }
}
