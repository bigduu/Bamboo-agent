use crate::server::error::AppError;

/// Parses JSON content with consistent error wrapping.
pub(in crate::server::handlers::agent_api) fn parse_json(
    content: &str,
    label: &str,
) -> Result<serde_json::Value, AppError> {
    serde_json::from_str(content).map_err(|error| {
        AppError::InternalError(anyhow::anyhow!("Failed to parse {}: {}", label, error))
    })
}

/// Serializes JSON content with pretty formatting and consistent error wrapping.
pub(in crate::server::handlers::agent_api) fn serialize_json_pretty(
    value: &serde_json::Value,
    label: &str,
) -> Result<String, AppError> {
    serde_json::to_string_pretty(value).map_err(|error| {
        AppError::InternalError(anyhow::anyhow!("Failed to serialize {}: {}", label, error))
    })
}
