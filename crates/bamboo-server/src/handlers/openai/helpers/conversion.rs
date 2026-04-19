use bamboo_agent_core::tools::ToolSchema;
use bamboo_infrastructure::api::models::ChatMessage;
use bamboo_infrastructure::protocol::FromProvider;
use crate::error::AppError;

pub(super) fn convert_messages(
    messages: Vec<ChatMessage>,
) -> Result<Vec<bamboo_agent_core::Message>, AppError> {
    messages
        .into_iter()
        .map(|message| {
            bamboo_agent_core::Message::from_provider(message).map_err(|error| {
                AppError::InternalError(anyhow::anyhow!("Failed to convert message: {}", error))
            })
        })
        .collect()
}

pub(super) fn convert_tools(
    tools: Option<Vec<bamboo_infrastructure::api::models::Tool>>,
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
