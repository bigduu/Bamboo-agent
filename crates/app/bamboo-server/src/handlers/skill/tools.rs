use actix_web::{web, HttpResponse};
use bamboo_agent_core::tools::ToolSchema;
use bamboo_tools::BuiltinToolExecutor;
use tracing::{debug, info};

use crate::app_state::AppState;
use crate::error::AppError;

use super::types::{
    AvailableToolsResponse, FilteredToolsQuery, FilteredToolsResponse, OpenAiFunction, OpenAiTool,
};

/// GET /skills/available-tools - Get available built-in tools
pub async fn get_available_tools(_state: web::Data<AppState>) -> Result<HttpResponse, AppError> {
    let tool_names: Vec<String> = BuiltinToolExecutor::tool_schemas()
        .into_iter()
        .map(|tool| tool.function.name)
        .collect();

    Ok(HttpResponse::Ok().json(AvailableToolsResponse { tools: tool_names }))
}

/// GET /skills/filtered-tools - Get tools filtered by enabled skills
pub async fn get_filtered_tools(
    state: web::Data<AppState>,
    query: web::Query<FilteredToolsQuery>,
) -> Result<HttpResponse, AppError> {
    let session_id = resolve_session_identifier(&query);
    let session = match session_id {
        Some(session_id) => state.load_session(session_id).await,
        None => None,
    };
    let selected_skill_ids = session.as_ref().and_then(|session| {
        session
            .metadata
            .get("selected_skill_ids")
            .and_then(|raw| bamboo_skills::selection::parse_selected_skill_ids_metadata(raw))
    });
    let selected_skill_mode = session.as_ref().and_then(|session| {
        session
            .metadata
            .get("skill_mode")
            .or_else(|| session.metadata.get("mode"))
            .map(|mode| mode.trim())
            .filter(|mode| !mode.is_empty())
            .map(str::to_string)
    });
    let workspace = session
        .as_ref()
        .and_then(|session| session.workspace_path_meta())
        .map(std::path::PathBuf::from);
    let disabled_skill_ids = {
        let config = state.config.read().await;
        config.disabled_skill_ids()
    };
    let allowed_tools = if let Some(workspace) = workspace.as_deref() {
        state
            .skill_manager
            .as_ref()
            .get_allowed_tools_for_workspace_selection_with_mode(
                workspace,
                &disabled_skill_ids,
                selected_skill_ids.as_deref(),
                selected_skill_mode.as_deref(),
            )
            .await
            .map_err(|error| AppError::BadRequest(format!("Invalid session workspace: {error}")))?
    } else {
        state
            .skill_manager
            .as_ref()
            .get_allowed_tools_for_selection_with_mode(
                &disabled_skill_ids,
                selected_skill_ids.as_deref(),
                selected_skill_mode.as_deref(),
            )
            .await
    };
    debug!("Skill filtered tools allowed list: {:?}", allowed_tools);

    let all_tools = BuiltinToolExecutor::tool_schemas();
    let all_tool_names: Vec<String> = all_tools
        .iter()
        .map(|tool| tool.function.name.clone())
        .collect();
    debug!("Built-in tools discovered: {:?}", all_tool_names);

    let tools = to_openai_tools(select_tools_by_allowlist(all_tools, &allowed_tools));
    Ok(HttpResponse::Ok().json(FilteredToolsResponse { tools }))
}

pub(super) fn resolve_session_identifier(query: &FilteredToolsQuery) -> Option<&str> {
    query.session_id.as_deref().or(query.chat_id.as_deref())
}

pub(super) fn select_tools_by_allowlist(
    all_tools: Vec<ToolSchema>,
    allowed_tools: &[String],
) -> Vec<ToolSchema> {
    if allowed_tools.is_empty() {
        info!("No enabled skills; returning all {} tools", all_tools.len());
        return all_tools;
    }

    let filtered: Vec<_> = all_tools
        .into_iter()
        .filter(|tool| {
            allowed_tools
                .iter()
                .any(|allowed| allowed == &tool.function.name)
        })
        .collect();
    info!(
        "Filtered tools: allowed={}, matched={}",
        allowed_tools.len(),
        filtered.len()
    );
    filtered
}

pub(super) fn to_openai_tools(tools: Vec<ToolSchema>) -> Vec<OpenAiTool> {
    tools
        .into_iter()
        .map(|tool| OpenAiTool {
            tool_type: "function".to_string(),
            function: OpenAiFunction {
                name: tool.function.name,
                description: tool.function.description,
                parameters: tool.function.parameters,
            },
        })
        .collect()
}
