//! User worktree management, separate from ephemeral runtime-child cleanup.
mod forge;
mod git;
mod merge_state;

use crate::app_state::{AppState, ConfigUpdateEffects};
use crate::error::AppError;
use actix_web::{web, HttpResponse};
use bamboo_domain::{ProjectId, ProjectManifest, ProjectStatus};
use serde::{Deserialize, Serialize};
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

// Inventory diagnostics are data in a successful response, not native HTTP
// errors. Keep their existing string/null wire contract explicit and separate
// from the AppError envelope used when the request itself fails.
#[derive(Serialize)]
struct RepositoryInventory {
    path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    common_dir: Option<String>,
    worktrees: Vec<WorktreeInventory>,
    error: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    warnings: Vec<String>,
}

#[derive(Serialize)]
struct WorktreeInventory {
    path: String,
    path_encoding: &'static str,
    openable: bool,
    branch: Option<String>,
    head: String,
    locked: bool,
    prunable: bool,
    dirty: Option<bool>,
    error: Option<String>,
    occupied: bool,
    /// The checkout is already covered by this Project's registered roots.
    /// Clients can open it without trying to register an overlapping child root.
    project_owned: bool,
    sessions: Vec<Value>,
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
        // One wall-clock budget for the whole inventory, shared by every
        // repository: a per-repository budget alone would leave the response
        // unbounded in the number of repositories a Project folder contains.
        let scan_deadline = std::time::Instant::now() + git::inventory_budget();
        // Discover repositories within registered Project roots without adding
        // overlapping workspace bindings or another repository registry.
        let bound_sources: Vec<_> = project.workspace_roots().map(PathBuf::from).collect();
        let canonical_roots: Vec<_> = bound_sources.iter().filter_map(|root| root.canonicalize().ok()).collect();
        let mut sources = Vec::new();
        let mut diagnostics = Vec::new();
        for source in &bound_sources {
            let discovered = git::discover_until(source, scan_deadline);
            sources.extend(discovered.repositories);
            diagnostics.extend(discovered.diagnostics);
        }
        let mut session_checkouts = HashMap::<PathBuf, Option<PathBuf>>::new();
        let mut repositories = Vec::new();
        let mut seen = HashSet::new();
        for path in sources {
            let source = git::path_label(&path);
            let common = match git::repository_until(&path, scan_deadline) {
                Ok(common) => common,
                Err(error) => {
                    repositories.push(RepositoryInventory {
                        path: source, common_dir: None,
                        worktrees: Vec::new(), error: Some(error), warnings: Vec::new(),
                    });
                    continue;
                }
            };
            if !seen.insert(common.clone()) { continue; }
            match git::list_until(&path, scan_deadline) {
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
                                git::workspace_root_until(workspace, scan_deadline).ok()
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
                        let project_owned = entry.path.canonicalize().is_ok_and(|path|
                            canonical_roots.iter().any(|root| path.starts_with(root)));
                        WorktreeInventory {
                            path: git::path_label(&entry.path),
                            path_encoding: if entry.path.to_str().is_some() { "utf-8" } else { "escaped" },
                            openable: entry.openable,
                            branch: entry.branch, head: entry.head, locked: entry.locked,
                            prunable: entry.prunable, dirty: entry.dirty, error: entry.error,
                            occupied, project_owned, sessions: related,
                        }
                    }).collect();
                    repositories.push(RepositoryInventory {
                        path: source, common_dir: Some(git::path_label(&common)),
                        worktrees, error: None, warnings: Vec::new(),
                    });
                }
                Err(error) => repositories.push(RepositoryInventory {
                    path: source, common_dir: Some(git::path_label(&common)),
                    worktrees: Vec::new(), error: Some(error), warnings: Vec::new(),
                }),
            }
        }
        // A scan warning can share a path with a valid source (for example when
        // its descendants reach the scan limit). Keep that source selectable and
        // keep one row per path for clients that use paths as list identities.
        for (path, diagnostic) in diagnostics {
            let path = git::path_label(&path);
            if let Some(repository) = repositories.iter_mut().find(|repo| repo.path == path) {
                if let Some(error) = &mut repository.error {
                    error.push('\n');
                    error.push_str(&diagnostic);
                } else {
                    repository.warnings.push(diagnostic);
                }
            } else {
                repositories.push(RepositoryInventory {
                    path, common_dir: None, worktrees: Vec::new(),
                    error: Some(diagnostic), warnings: Vec::new(),
                });
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
        // Re-run the same bounded discovery used by inventory. A path merely
        // being below a root, or sharing a common .git directory, is insufficient:
        // ignored trees, symlink escapes and stale inventory must not be admitted.
        if !project.workspace_roots().any(|source| {
            git::discover_repositories(Path::new(source))
                .repositories
                .contains(&canonical)
        }) {
            return Err(
                "Repository is not a discovered source of this Project; refresh the worktree list"
                    .into(),
            );
        }
        git::create(&canonical, Path::new(&root), &request.name, &request.branch)
    })
    .await?;
    // Do not add another registry or implicitly mutate Project bindings. Native
    // Git discovery already includes the new checkout on the next list call.
    Ok(HttpResponse::Created().json(json!({"path": path})))
}

/// Capabilities this build serves to an independently released frontend.
///
/// Advertised in `GET /sessions`, which every client already calls at boot, so a
/// client can gate an optional request without probing and collecting a 404. An
/// older server omits the list entirely; absence means "do not ask".
pub const FEATURES: &[&str] = &["worktree_merge_state"];

/// Per-checkout merge state, derived from local refs and optionally corroborated
/// by a forge.
#[derive(Serialize)]
struct MergeStateInventory {
    path: String,
    state: &'static str,
    /// Stable machine reason for `state`; diagnostic provenance, not UI copy.
    reason: &'static str,
    /// Stable machine reason for the forge enrichment, or why it was skipped.
    forge: &'static str,
    branch: Option<String>,
    head: String,
}

/// Owned intermediate result, before the optional forge pass: that pass mutates
/// entries after the blocking Git work has already finished.
struct ComputedEntry {
    path: String,
    state: merge_state::MergeState,
    reason: &'static str,
    forge: &'static str,
    branch: Option<String>,
    head: String,
}

struct ComputedRepository {
    path: String,
    base_ref: Option<String>,
    base_commit: Option<String>,
    error: Option<&'static str>,
    remote: Result<forge::Remote, forge::ForgeReason>,
    entries: Vec<ComputedEntry>,
}

/// How the optional forge query was requested.
#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct MergeStateQuery {
    /// `auto` (default) consults a GitHub remote when one is configured and a
    /// credential exists; `off` answers from local refs only.
    pub forge: Option<String>,
}

/// Upper bound on forge round trips per request. A Project can own hundreds of
/// checkouts; a sidebar poll must not become a batch job.
const FORGE_QUERY_BUDGET: usize = 12;

#[derive(Serialize)]
struct MergeStateRepository {
    path: String,
    /// Present when the integration branch could be resolved.
    #[serde(skip_serializing_if = "Option::is_none")]
    base_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    base_commit: Option<String>,
    /// Present when the whole repository could not be answered.
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<&'static str>,
    worktrees: Vec<MergeStateInventory>,
}

/// `GET /projects/{project_id}/worktrees/merge-state`
///
/// Read-only for the repository: local Git objects only, never a fetch, never a
/// ref mutation. A missing base, timeout or Git error degrades that entry to
/// `unknown` rather than failing the request. The optional forge pass may only
/// upgrade a locally-unproven `in_progress` to `merged` and never fails the call.
pub async fn merge_state(
    state: web::Data<AppState>,
    id: web::Path<String>,
    query: web::Query<MergeStateQuery>,
) -> Response {
    let project = project(&state, &id)?;
    let project_id = project.id.to_string();
    // An unrecognised value falls back to the default: this is a presentation
    // hint, not a command, so a typo must not fail the inventory.
    let forge_mode = query.forge.as_deref() != Some("off");
    let mut repositories = blocking(move || {
        let deadline = std::time::Instant::now() + merge_state::SCAN_BUDGET;
        let mut repositories = Vec::new();
        let mut seen = HashSet::new();
        for source in project.workspace_roots() {
            // Same scoping rule as `list`: only namespaces this Project owns.
            let root = PathBuf::from(source);
            // A registered root may be a checkout or a container of several
            // repositories. Assuming it is always a checkout would return no
            // state for a container Project whose checkouts the inventory does
            // list, so discover within the root, bounded like everything here.
            for path in merge_state::discover_bounded(&root, deadline) {
                let Ok(common) = merge_state::repository_bounded(&path, deadline) else {
                    continue;
                };
                // Several bound roots may resolve to one repository.
                if !seen.insert(common) {
                    continue;
                }
                let label = git::path_label(&path);
                // A forge answer is only meaningful when the remote names one
                // this build may query; resolve that once per repository.
                let remote = forge::github_remote(&path);
                let base = match merge_state::merge_base(&path, deadline) {
                    Ok(base) => base,
                    Err(reason) => {
                        repositories.push(ComputedRepository {
                            path: label,
                            base_ref: None,
                            base_commit: None,
                            error: Some(reason),
                            remote,
                            entries: Vec::new(),
                        });
                        continue;
                    }
                };
                // The primary checkout is the workspace itself, never outstanding work.
                let main_checkout = std::fs::canonicalize(&path).unwrap_or_else(|_| path.clone());
                let entries = match merge_state::list_metadata(&path, deadline) {
                    Ok(entries) => entries,
                    Err(_) => {
                        repositories.push(ComputedRepository {
                            path: label,
                            base_ref: Some(base.label()),
                            base_commit: Some(base.commit.clone()),
                            error: Some("inventory_unavailable"),
                            remote,
                            entries: Vec::new(),
                        });
                        continue;
                    }
                };
                let entries = entries
                    .into_iter()
                    .filter(|entry| entry.path.to_str().is_some())
                    .map(|entry| {
                        let verdict = merge_state::merge_verdict(
                            &path,
                            &entry,
                            &main_checkout,
                            &base,
                            // Never a fresh full allowance: the request budget bounds
                            // every entry, so later entries degrade promptly.
                            merge_state::entry_deadline(deadline),
                        );
                        ComputedEntry {
                            path: git::path_label(&entry.path),
                            state: verdict.state,
                            reason: verdict.reason,
                            forge: forge::ForgeReason::NotRequired.as_str(),
                            branch: entry.branch,
                            head: entry.head,
                        }
                    })
                    .collect();
                repositories.push(ComputedRepository {
                    path: label,
                    base_ref: Some(base.label()),
                    base_commit: Some(base.commit),
                    error: None,
                    remote,
                    entries,
                });
            }
        }
        Ok(repositories)
    })
    .await?;

    if forge_mode {
        enrich_with_forge(&mut repositories).await;
    }

    let repositories: Vec<MergeStateRepository> = repositories
        .into_iter()
        .map(|repository| MergeStateRepository {
            path: repository.path,
            base_ref: repository.base_ref,
            base_commit: repository.base_commit,
            error: repository.error,
            worktrees: repository
                .entries
                .into_iter()
                .map(|entry| MergeStateInventory {
                    path: entry.path,
                    state: entry.state.as_str(),
                    reason: entry.reason,
                    forge: entry.forge,
                    branch: entry.branch,
                    head: entry.head,
                })
                .collect(),
        })
        .collect();
    Ok(HttpResponse::Ok().json(json!({
        "project_id": project_id,
        "repositories": repositories,
    })))
}

/// Record one forge reason on every entry that would have benefited from a query.
fn mark_forge_reason(repositories: &mut [ComputedRepository], reason: &'static str) {
    for entry in repositories
        .iter_mut()
        .filter(|repository| repository.remote.is_ok())
        .flat_map(|repository| repository.entries.iter_mut())
        .filter(|entry| forge::needs_forge(entry.state))
    {
        entry.forge = reason;
    }
}

/// Apply the optional forge enrichment.
///
/// Every branch here is a degradation path: no credential, an unsupported remote,
/// an exhausted budget or a failed request all leave the local verdict untouched
/// and record a reason code. Nothing here may fail the endpoint.
async fn enrich_with_forge(repositories: &mut [ComputedRepository]) {
    // A remote this build may not query is a *static* fact, so record it before
    // any credential question: reporting `no_credentials` for a GitLab checkout
    // would send the operator hunting for a token that would not help.
    for repository in repositories.iter_mut() {
        if let Err(reason) = &repository.remote {
            let reason = reason.as_str();
            for entry in repository
                .entries
                .iter_mut()
                .filter(|entry| forge::needs_forge(entry.state))
            {
                entry.forge = reason;
            }
        }
    }
    if !repositories
        .iter()
        .any(|repository| repository.remote.is_ok())
    {
        // Nothing here can benefit from a credential; do not even look for one.
        return;
    }
    let Some(token) = forge::credential_token() else {
        // No credential is a normal state: the local answer stands and the client
        // simply keeps the muted presentation.
        mark_forge_reason(repositories, forge::ForgeReason::NoCredentials.as_str());
        return;
    };
    let client = match reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(8))
        .build()
    {
        Ok(client) => client,
        Err(_) => {
            mark_forge_reason(repositories, forge::ForgeReason::RequestFailed.as_str());
            return;
        }
    };
    let mut budget = FORGE_QUERY_BUDGET;
    for repository in repositories.iter_mut() {
        let remote = match &repository.remote {
            Ok(remote) => remote.clone(),
            // Already annotated above.
            Err(_) => continue,
        };
        for entry in repository
            .entries
            .iter_mut()
            .filter(|entry| forge::needs_forge(entry.state))
        {
            let Some(branch) = entry.branch.clone() else {
                // A detached checkout has no branch a pull request could track.
                entry.forge = forge::ForgeReason::NoPullRequest.as_str();
                continue;
            };
            if budget == 0 {
                // Scanned enough for this poll; the rest keep their local answer.
                entry.forge = forge::ForgeReason::NotRequired.as_str();
                continue;
            }
            budget -= 1;
            // The PR must cover this exact checkout against this exact base, so
            // a merge into another branch or a since-moved branch is not proof.
            let base_ref = repository.base_ref.clone().unwrap_or_default();
            let pr = forge::query_github(&client, &remote, &branch, &entry.head, &base_ref, &token)
                .await;
            let local = merge_state::MergeVerdict {
                state: entry.state,
                reason: entry.reason,
            };
            let (state, reason, forge_reason) = forge::resolve(&local, pr);
            entry.state = state;
            entry.reason = reason;
            entry.forge = forge_reason.as_str();
        }
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod discovery_tests;

#[cfg(test)]
mod merge_state_tests;

#[cfg(test)]
mod forge_tests;
