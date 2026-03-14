use super::conversion::convert_gemini_tools;
use crate::agent::llm::protocol::gemini::{GeminiFunctionDeclaration, GeminiTool};

#[test]
fn convert_gemini_tools_returns_empty_for_none() {
    let schemas = convert_gemini_tools(&None).expect("conversion should succeed");
    assert!(schemas.is_empty());
}

#[test]
fn convert_gemini_tools_flattens_function_declarations() {
    let tools = vec![GeminiTool {
        function_declarations: vec![
            GeminiFunctionDeclaration {
                name: "a".to_string(),
                description: Some("A".to_string()),
                parameters: serde_json::json!({"type":"object"}),
            },
            GeminiFunctionDeclaration {
                name: "b".to_string(),
                description: None,
                parameters: serde_json::json!({"type":"object"}),
            },
        ],
    }];

    let schemas = convert_gemini_tools(&Some(tools)).expect("conversion should succeed");
    assert_eq!(schemas.len(), 2);
    assert_eq!(schemas[0].function.name, "a");
    assert_eq!(schemas[1].function.name, "b");
}
