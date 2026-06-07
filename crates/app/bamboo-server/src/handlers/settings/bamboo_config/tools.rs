use actix_web::{web, HttpResponse};
use serde::Serialize;

use crate::{app_state::AppState, error::AppError};

#[derive(Serialize)]
struct AvailableBambooToolsResponse {
    tools: Vec<String>,
}

/// Lists all tool names that Bamboo can expose to the agent runtime.
///
/// This returns the discovered tool schemas (built-in + server overlays + MCP aliases),
/// independent of the user's disabled list.
pub async fn get_bamboo_tools(app_state: web::Data<AppState>) -> Result<HttpResponse, AppError> {
    let mut tools: Vec<String> = app_state
        .get_all_tool_schemas()
        .into_iter()
        .map(|schema| schema.function.name)
        .collect();
    tools.sort();
    tools.dedup();

    Ok(HttpResponse::Ok().json(AvailableBambooToolsResponse { tools }))
}
