use crate::error::AppError;
use bamboo_agent_core::tools::{FunctionSchema, ToolSchema};
use bamboo_llm::api::models::ChatMessage;
use bamboo_llm::protocol::FromProvider;

use super::super::types::ResponsesToolParam;

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

fn nested_tool_to_schema(tool: bamboo_llm::api::models::Tool) -> Result<ToolSchema, AppError> {
    ToolSchema::from_provider(tool).map_err(|error| {
        AppError::InternalError(anyhow::anyhow!("Failed to convert tool: {}", error))
    })
}

pub(super) fn convert_tools(
    tools: Option<Vec<bamboo_llm::api::models::Tool>>,
) -> Result<Vec<ToolSchema>, AppError> {
    match tools {
        Some(tools) => tools.into_iter().map(nested_tool_to_schema).collect(),
        None => Ok(vec![]),
    }
}

/// Convert Responses-API `tools[]` entries into internal tool schemas.
///
/// Flat function entries (the spec shape) map directly; nested
/// Chat-Completions entries are tolerated for legacy clients; non-function
/// tool types are skipped rather than failing the request. A function-typed
/// entry that matched neither shape is malformed and gets a targeted error —
/// the untagged-enum serde message would be useless to the caller. #525.
pub(super) fn convert_responses_tools(
    tools: Option<Vec<ResponsesToolParam>>,
) -> Result<Vec<ToolSchema>, AppError> {
    let Some(tools) = tools else {
        return Ok(vec![]);
    };

    let mut converted = Vec::with_capacity(tools.len());
    for tool in tools {
        match tool {
            ResponsesToolParam::Flat(flat) => {
                if flat.tool_type != "function" {
                    // warn, not debug: a skipped `custom` (freeform) tool is a
                    // capability the client expects the model to call — the
                    // operator needs a visible signal, not a silent gap.
                    tracing::warn!(
                        tool_type = %flat.tool_type,
                        name = %flat.name,
                        "Skipping unsupported Responses tool type (not forwarded to the model)"
                    );
                    continue;
                }
                // An omitted `parameters` deserializes to JSON null; normalize
                // to an empty object schema so providers doing schema
                // validation don't choke on `parameters: null`.
                let parameters = if flat.parameters.is_null() {
                    serde_json::json!({"type": "object", "properties": {}})
                } else {
                    flat.parameters
                };
                converted.push(ToolSchema {
                    schema_type: flat.tool_type,
                    function: FunctionSchema {
                        name: flat.name,
                        description: flat.description.unwrap_or_default(),
                        parameters,
                    },
                });
            }
            ResponsesToolParam::Nested(tool) => {
                converted.push(nested_tool_to_schema(tool)?);
            }
            ResponsesToolParam::Other(value) => {
                // A missing/non-string `type` is garbage, not an unsupported
                // feature — fail fast like the old strict parsing did.
                let Some(tool_type) = value.get("type").and_then(|v| v.as_str()) else {
                    return Err(AppError::BadRequest(
                        "Malformed tools[] entry: missing string `type` field".to_string(),
                    ));
                };
                if tool_type == "function" {
                    return Err(AppError::BadRequest(
                        "Malformed function tool: expected the flat Responses shape \
                         {\"type\":\"function\",\"name\":…,\"parameters\":…} (or the legacy \
                         nested {\"type\",\"function\":{…}})"
                            .to_string(),
                    ));
                }
                tracing::warn!(
                    %tool_type,
                    "Skipping unsupported Responses tool type (not forwarded to the model)"
                );
            }
        }
    }
    Ok(converted)
}
