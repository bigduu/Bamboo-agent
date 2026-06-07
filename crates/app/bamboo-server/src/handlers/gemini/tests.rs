use super::conversion::convert_gemini_tools;
use bamboo_llm::protocol::gemini::{GeminiFunctionDeclaration, GeminiTool};

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
                parameters_json_schema: Some(serde_json::json!({"type":"object"})),
                parameters: None,
            },
            GeminiFunctionDeclaration {
                name: "b".to_string(),
                description: None,
                parameters_json_schema: None,
                parameters: Some(serde_json::json!({"type":"object"})),
            },
        ],
    }];

    let schemas = convert_gemini_tools(&Some(tools)).expect("conversion should succeed");
    assert_eq!(schemas.len(), 2);
    assert_eq!(schemas[0].function.name, "a");
    assert_eq!(schemas[1].function.name, "b");
}
