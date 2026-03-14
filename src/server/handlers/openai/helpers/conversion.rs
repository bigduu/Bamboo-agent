use crate::agent::core::tools::ToolSchema;
use crate::agent::llm::api::models::ChatMessage;
use crate::agent::llm::protocol::FromProvider;
use crate::server::error::AppError;

pub(super) fn convert_messages(
    messages: Vec<ChatMessage>,
) -> Result<Vec<crate::agent::core::Message>, AppError> {
    messages
        .into_iter()
        .map(|message| {
            crate::agent::core::Message::from_provider(message).map_err(|error| {
                AppError::InternalError(anyhow::anyhow!("Failed to convert message: {}", error))
            })
        })
        .collect()
}

pub(super) fn convert_tools(
    tools: Option<Vec<crate::agent::llm::api::models::Tool>>,
) -> Result<Vec<ToolSchema>, AppError> {
    match tools {
        Some(tools) => tools
            .into_iter()
            .map(|tool| {
                ToolSchema::from_provider(tool).map_err(|error| {
                    AppError::InternalError(anyhow::anyhow!("Failed to convert tool: {}", error))
                })
            })
            .collect(),
        None => Ok(vec![]),
    }
}
