use crate::agent::core::tools::ToolSchema;
use crate::agent::core::Message;
use crate::agent::llm::protocol::gemini::{GeminiContent, GeminiTool};
use crate::agent::llm::protocol::FromProvider;
use crate::server::error::AppError;

/// Convert Gemini contents to internal messages.
pub(super) fn convert_gemini_to_messages(
    contents: &[GeminiContent],
) -> Result<Vec<Message>, AppError> {
    contents
        .iter()
        .map(|content| {
            Message::from_provider(content.clone()).map_err(|error| {
                AppError::ToolExecutionError(format!("Failed to convert message: {error}"))
            })
        })
        .collect()
}

/// Convert Gemini tools to internal tool schemas.
pub(super) fn convert_gemini_tools(
    tools: &Option<Vec<GeminiTool>>,
) -> Result<Vec<ToolSchema>, AppError> {
    match tools {
        Some(tools) => {
            let mut all_schemas = Vec::new();
            for tool in tools {
                for function_declaration in &tool.function_declarations {
                    let schema = ToolSchema {
                        schema_type: "function".to_string(),
                        function: crate::agent::core::tools::FunctionSchema {
                            name: function_declaration.name.clone(),
                            description: function_declaration
                                .description
                                .clone()
                                .unwrap_or_default(),
                            parameters: function_declaration.parameters.clone(),
                        },
                    };
                    all_schemas.push(schema);
                }
            }
            Ok(all_schemas)
        }
        None => Ok(vec![]),
    }
}
