//! User worktree management, separate from ephemeral runtime-child cleanup.
mod git;

use crate::app_state::{AppState, ConfigUpdateEffects};
use crate::error::AppError;
use actix_web::{web, HttpResponse};
use bamboo_domain::{ProjectId, ProjectManifest, ProjectStatus};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::path::{Component, Path, PathBuf};

type Response = Result<HttpResponse, AppError>;

fn project(state: &AppState, id: &str) -> Result<ProjectManifest, AppError> {
    let id = ProjectId::parse(id).map_err(|e| AppError::BadRequest(e.to_string()))?;
    state
        .project_store
        .get(&id)
        .map_err(|e| AppError::NotFound(e.to_string()))
}

async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> Result<T, String> + Send + 'static,
) -> Result<T, AppError> {
    web::block(f)
        .await
        .map_err(|e| AppError::InternalError(e.into()))?
        .map_err(AppError::BadRequest)
}

pub async fn get_preferences(state: web::Data<AppState>) -> Response {
    Ok(HttpResponse::Ok().json(&state.config.read().await.worktrees))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreferenceRequest {
    /// Absent selects the global default; null root clears only this scope.
    pub project_id: Option<String>,
    #[serde(deserialize_with = "required_nullable_root")]
    pub root: Option<String>,
}

fn required_nullable_root<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<String>, D::Error> {
    Option::<String>::deserialize(deserializer)
}

pub async fn set_preferences(
    state: web::Data<AppState>,
    request: web::Json<PreferenceRequest>,
) -> Response {
    let request = request.into_inner();
    if let Some(id) = &request.project_id {
        project(&state, id)?;
    }
    let root = match request.root {
        Some(raw) => Some(
            blocking(move || {
                let canonical = git::root(Path::new(&raw))?;
                // Check actual write access without changing permissions or leaving
                // a probe behind. The selected directory must already exist.
                let probe = tempfile::NamedTempFile::new_in(&canonical).map_err(|e| {
                    format!("Worktree root {} is not writable: {e}", canonical.display())
                })?;
                probe
                    .close()
                    .map_err(|e| format!("Could not remove worktree root probe: {e}"))?;
                canonical
                    .to_str()
                    .map(str::to_owned)
                    .ok_or("Worktree root must be UTF-8".into())
            })
            .await?,
        ),
        None => None,
    };
    let config = state
        .update_config(
            move |config| {
                if let Some(id) = request.project_id {
                    if let Some(root) = root {
                        config.worktrees.project_roots.insert(id, root);
                    } else {
                        config.worktrees.project_roots.remove(&id);
                    }
                } else {
                    config.worktrees.default_root = root;
                }
                Ok(())
            },
            ConfigUpdateEffects::default(),
        )
        .await?;
    Ok(HttpResponse::Ok().json(config.worktrees.clone()))
}

pub async fn list(state: web::Data<AppState>, id: web::Path<String>) -> Response {
    let project = project(&state, &id)?;
    let sessions = state.session_store.list_index_entries().await;
    let running: HashSet<_> = state
        .agent_runners
        .read()
        .await
        .iter()
        .filter(|(_, runner)| runner.completed_at.is_none())
        .map(|(id, _)| id.clone())
        .collect();
    let preferences = state.config.read().await.worktrees.clone();
    let project_id = project.id.to_string();
    let default_root = preferences
        .project_roots
        .get(&project_id)
        .cloned()
        .or(preferences.default_root);
    let repositories = blocking(move || {
        // Inspect only namespaces selected by this Project or discovered by
        // native worktree inventory. Historical Session paths elsewhere may be
        // offline and must not trigger filesystem/Git work for this request.
        let bound_sources: Vec<_> = project.workspace_roots().map(PathBuf::from).collect();
        let mut session_checkouts = HashMap::<PathBuf, Option<PathBuf>>::new();
        let mut repositories = Vec::<Value>::new();
        let mut seen = HashSet::new();
        for source in project.workspace_roots() {
            let path = PathBuf::from(source);
            let common = match git::repository(&path) {
                Ok(common) => common,
                Err(error) => {
                    repositories.push(json!({"path": source, "worktrees": [], "error": error}));
                    continue;
                }
            };
            if !seen.insert(common.clone()) { continue; }
            match git::list(&path) {
                Ok(entries) => {
                    for session in &sessions {
                        let Some(workspace) = session.workspace_path.as_deref().map(Path::new) else { continue; };
                        // A lexical parent component cannot establish scoped
                        // membership without following unrelated filesystem paths.
                        if !workspace.is_absolute() || workspace.components().any(|c| matches!(c, Component::ParentDir)) { continue; }
                        let within = |root: &Path| root.is_absolute()
                            && !root.components().any(|c| matches!(c, Component::ParentDir))
                            && workspace.starts_with(root);
                        if !entries.iter().any(|entry| within(&entry.path))
                            && !bound_sources.iter().any(|source| within(source)) { continue; }
                        // Deduplicate across Sessions and repositories. An exact
                        // inventory path already has a known checkout root; only
                        // scoped subdirectories/aliases need an additional query.
                        session_checkouts.entry(workspace.to_path_buf()).or_insert_with(|| {
                            if entries.iter().any(|entry| entry.path == workspace) {
                                Some(workspace.to_path_buf())
                            } else {
                                git::workspace_root(workspace).ok()
                            }
                        });
                    }
                    let worktrees: Vec<_> = entries.into_iter().map(|entry| {
                        let related: Vec<_> = sessions.iter().filter(|session| {
                            let Some(workspace) = session.workspace_path.as_deref().map(Path::new) else { return false; };
                            match session_checkouts.get(workspace) {
                                Some(Some(checkout)) => checkout == &entry.path,
                                Some(None) => workspace == entry.path.as_path(),
                                None => false,
                            }
                        }).map(|session| json!({"id": session.id, "title": session.title,
                            "active": running.contains(&session.id)
                                || matches!(session.last_run_status.as_deref(), Some("running" | "pending"))
                                || session.has_pending_question})).collect();
                        let occupied = entry.locked || related.iter().any(|s| s["active"] == true);
                        json!({"path": git::path_label(&entry.path), "path_encoding": if entry.path.to_str().is_some() { "utf-8" } else { "escaped" }, "branch": entry.branch, "head": entry.head,
                            "locked": entry.locked, "prunable": entry.prunable, "dirty": entry.dirty,
                            "error": entry.error, "occupied": occupied, "sessions": related})
                    }).collect();
                    repositories.push(json!({"path": source, "common_dir": git::path_label(&common), "worktrees": worktrees, "error": null}));
                }
                Err(error) => repositories.push(json!({"path": source, "common_dir": git::path_label(&common), "worktrees": [], "error": error})),
            }
        }
        Ok(repositories)
    }).await?;
    Ok(HttpResponse::Ok().json(json!({"project_id": project_id, "default_root": default_root, "repositories": repositories})))
}

#[derive(Deserialize)]
pub struct CreateRequest {
    pub repository: String,
    pub root: Option<String>,
    pub name: String,
    pub branch: String,
}

pub async fn create(
    state: web::Data<AppState>,
    id: web::Path<String>,
    request: web::Json<CreateRequest>,
) -> Response {
    let project = project(&state, &id)?;
    if project.status != ProjectStatus::Active {
        return Err(AppError::BadRequest(
            "Archived Projects cannot create worktrees".into(),
        ));
    }
    let request = request.into_inner();
    let preferences = state.config.read().await.worktrees.clone();
    let root = request
        .root
        .or_else(|| preferences.project_roots.get(id.as_str()).cloned())
        .or(preferences.default_root)
        .ok_or_else(|| AppError::BadRequest("Select a worktree root directory first".into()))?;
    let path = blocking(move || {
        let repo = PathBuf::from(&request.repository);
        let canonical = repo
            .canonicalize()
            .map_err(|e| format!("Repository is unavailable: {e}"))?;
        // Only an explicitly bound source directory may be used as repository.
        // Matching a common .git directory alone would admit arbitrary paths.
        if !project
            .workspace_roots()
            .any(|source| Path::new(source).canonicalize().ok().as_ref() == Some(&canonical))
        {
            return Err("Repository is not bound to this Project".into());
        }
        git::create(&canonical, Path::new(&root), &request.name, &request.branch)
    })
    .await?;
    // Do not add another registry or implicitly mutate Project bindings. Native
    // Git discovery already includes the new checkout on the next list call.
    Ok(HttpResponse::Created().json(json!({"path": path})))
}

#[cfg(test)]
mod tests;
