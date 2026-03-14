use actix_web::{web, HttpResponse};

use crate::server::app_state::AppState;
use crate::server::error::AppError;

use super::super::path::home_dir;
use super::super::store::{load_recent_store, save_recent_store};
use super::super::types::{
    AddRecentWorkspaceRequest, PathSuggestion, PathSuggestionsResponse, WorkspacePathRequest,
};

mod store_ops;
mod suggestions;
#[cfg(test)]
mod tests;
mod workspace_info;

use store_ops::{current_unix_timestamp, upsert_recent_workspace};
use suggestions::{dedupe_suggestions_by_path, default_path_suggestions, recent_suggestion_name};
use workspace_info::build_workspace_info;

pub async fn validate_workspace(
    _app_state: web::Data<AppState>,
    payload: web::Json<WorkspacePathRequest>,
) -> Result<HttpResponse, AppError> {
    let path = payload.path.trim();
    if path.is_empty() {
        return Err(AppError::BadRequest("Path cannot be empty".to_string()));
    }

    let info = build_workspace_info(path).await;
    Ok(HttpResponse::Ok().json(info))
}

pub async fn get_recent_workspaces(
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, AppError> {
    let store = load_recent_store(&app_state.app_data_dir).await?;
    let mut infos = Vec::new();
    for item in &store.items {
        let mut info = build_workspace_info(&item.path).await;
        if info.workspace_name.is_none() {
            info.workspace_name = item
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.workspace_name.clone());
        }
        infos.push(info);
    }
    Ok(HttpResponse::Ok().json(infos))
}

pub async fn add_recent_workspace(
    app_state: web::Data<AppState>,
    payload: web::Json<AddRecentWorkspaceRequest>,
) -> Result<HttpResponse, AppError> {
    let mut store = load_recent_store(&app_state.app_data_dir).await?;
    upsert_recent_workspace(&mut store, &payload, current_unix_timestamp());
    save_recent_store(&app_state.app_data_dir, &store).await?;

    Ok(HttpResponse::NoContent().finish())
}

pub async fn get_workspace_suggestions(
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, AppError> {
    let home = home_dir()?;
    let mut suggestions = default_path_suggestions(&home).await;

    let store = load_recent_store(&app_state.app_data_dir).await?;
    for item in &store.items {
        suggestions.push(PathSuggestion {
            path: item.path.clone(),
            name: recent_suggestion_name(item),
            description: None,
            suggestion_type: "recent".to_string(),
        });
    }

    dedupe_suggestions_by_path(&mut suggestions);
    Ok(HttpResponse::Ok().json(PathSuggestionsResponse { suggestions }))
}
