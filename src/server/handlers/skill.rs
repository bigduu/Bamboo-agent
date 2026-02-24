use crate::agent::server::state::AppState as AgentAppState;
use crate::agent::skill::{SkillDefinition, SkillFilter};
use crate::agent::tools::BuiltinToolExecutor;
use actix_web::{web, HttpResponse};
use log::{debug, info};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::server::app_state::AppState;
use crate::server::error::AppError;

/// Configure skill routes
pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.route("/skills", web::get().to(list_skills))
        .route("/skills/{id}", web::get().to(get_skill))
        .route(
            "/skills/available-tools",
            web::get().to(get_available_tools),
        )
        .route("/skills/filtered-tools", web::get().to(get_filtered_tools))
        .route(
            "/skills/available-workflows",
            web::get().to(get_available_workflows),
        );
}

#[derive(Serialize)]
struct SkillListResponse {
    skills: Vec<SkillDefinition>,
    total: usize,
}

#[derive(Deserialize)]
pub struct ListSkillsQuery {
    category: Option<String>,
    search: Option<String>,
    refresh: Option<bool>,
}

#[derive(Serialize)]
struct AvailableToolsResponse {
    tools: Vec<String>,
}

#[derive(Serialize)]
struct FilteredToolsResponse {
    tools: Vec<OpenAiTool>,
}

#[derive(Serialize)]
struct OpenAiTool {
    #[serde(rename = "type")]
    tool_type: String,
    function: OpenAiFunction,
}

#[derive(Serialize)]
struct OpenAiFunction {
    name: String,
    description: String,
    parameters: Value,
}

#[derive(Serialize)]
struct AvailableWorkflowsResponse {
    workflows: Vec<String>,
}

/// GET /skills - List all skills
pub async fn list_skills(
    agent_state: web::Data<AgentAppState>,
    query: web::Query<ListSkillsQuery>,
) -> Result<HttpResponse, AppError> {
    let mut filter = SkillFilter::new();
    if let Some(category) = query.category.clone() {
        filter = filter.with_category(category);
    }
    if let Some(search) = query.search.clone() {
        filter = filter.with_search(search);
    }

    let refresh = query.refresh.unwrap_or(false);
    let skills = agent_state
        .skill_manager
        .as_ref()
        .store()
        .list_skills(Some(filter), refresh)
        .await;

    Ok(HttpResponse::Ok().json(SkillListResponse {
        total: skills.len(),
        skills,
    }))
}

/// GET /skills/{id} - Get skill detail
pub async fn get_skill(
    agent_state: web::Data<AgentAppState>,
    path: web::Path<String>,
) -> Result<HttpResponse, AppError> {
    let id = path.into_inner();
    let skill = agent_state
        .skill_manager
        .as_ref()
        .store()
        .get_skill(&id)
        .await
        .map_err(|_| AppError::NotFound(format!("Skill {} not found", id)))?;

    Ok(HttpResponse::Ok().json(skill))
}

/// GET /skills/available-tools - Get available built-in tools
pub async fn get_available_tools(
    _agent_state: web::Data<AgentAppState>,
) -> Result<HttpResponse, AppError> {
    let tool_names: Vec<String> = BuiltinToolExecutor::tool_schemas()
        .into_iter()
        .map(|tool| tool.function.name)
        .collect();

    Ok(HttpResponse::Ok().json(AvailableToolsResponse { tools: tool_names }))
}

#[derive(Deserialize)]
pub struct FilteredToolsQuery {
    chat_id: Option<String>,
}

/// GET /skills/filtered-tools - Get tools filtered by enabled skills
pub async fn get_filtered_tools(
    agent_state: web::Data<AgentAppState>,
    query: web::Query<FilteredToolsQuery>,
) -> Result<HttpResponse, AppError> {
    let allowed_tools = agent_state
        .skill_manager
        .as_ref()
        .get_allowed_tools(query.chat_id.as_deref())
        .await;
    debug!("Skill filtered tools allowed list: {:?}", allowed_tools);

    let all_tools = BuiltinToolExecutor::tool_schemas();
    let all_tool_names: Vec<String> = all_tools
        .iter()
        .map(|tool| tool.function.name.clone())
        .collect();
    debug!("Built-in tools discovered: {:?}", all_tool_names);

    let filtered = if allowed_tools.is_empty() {
        info!("No enabled skills; returning all {} tools", all_tools.len());
        all_tools
    } else {
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
    };

    let tools = filtered
        .into_iter()
        .map(|tool| OpenAiTool {
            tool_type: "function".to_string(),
            function: OpenAiFunction {
                name: tool.function.name,
                description: tool.function.description,
                parameters: tool.function.parameters,
            },
        })
        .collect();

    Ok(HttpResponse::Ok().json(FilteredToolsResponse { tools }))
}

/// GET /skills/available-workflows - Get available workflows
pub async fn get_available_workflows(
    app_state: web::Data<AppState>,
    _agent_state: web::Data<AgentAppState>,
) -> Result<HttpResponse, AppError> {
    let workflows = crate::server::services::skill_service::list_workflows(&app_state.app_data_dir)
        .await
        .map_err(|e| AppError::InternalError(anyhow::anyhow!("Failed to list workflows: {}", e)))?;

    Ok(HttpResponse::Ok().json(AvailableWorkflowsResponse { workflows }))
}
