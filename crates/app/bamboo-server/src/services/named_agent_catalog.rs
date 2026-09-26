//! Durable Session/Project observation. Existing storage acquisition/recovery
//! stays unchanged; this service adds no writer or profile-application authority.
use bamboo_domain::ProjectStatus;
use bamboo_engine::project_context::{ProjectContextResolver, SessionProjectIdentity};
use bamboo_skills::named_agents::{
    NamedAgentDiagnosticCode, NamedAgentLimits, ScopedNamedAgentCatalog,
};
use serde::Serialize;

use crate::app_state::AppState;

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CatalogError {
    SessionNotFound,
    SessionUnavailable,
    ProjectUnavailable,
    CatalogUnavailable,
    CatalogRejected(NamedAgentDiagnosticCode),
}

pub(crate) async fn discover(
    state: &AppState,
    session_id: &str,
    limits: NamedAgentLimits,
) -> Result<ScopedNamedAgentCatalog, CatalogError> {
    let session = state
        .storage
        .load_session(session_id)
        .await
        .map_err(|_| CatalogError::SessionUnavailable)?
        .ok_or(CatalogError::SessionNotFound)?;
    if session.id != session_id {
        return Err(CatalogError::SessionUnavailable);
    }
    let project_id = match ProjectContextResolver::session_project_identity(&session) {
        SessionProjectIdentity::Unassigned => None,
        SessionProjectIdentity::Assigned(id) => Some(id),
        SessionProjectIdentity::Invalid { .. } => return Err(CatalogError::ProjectUnavailable),
    };
    let projects = state.project_store.clone();
    let global = state.app_data_dir.clone();
    tokio::task::spawn_blocking(move || {
        let project = project_id
            .map(|id| {
                let manifest = projects
                    .get(&id)
                    .map_err(|_| CatalogError::ProjectUnavailable)?;
                if manifest.id != id || manifest.status != ProjectStatus::Active {
                    return Err(CatalogError::ProjectUnavailable);
                }
                let home = projects.paths().project_home(&id);
                Ok((id, home))
            })
            .transpose()?;
        ScopedNamedAgentCatalog::discover(
            &global,
            project.as_ref().map(|(id, home)| (id, home.as_path())),
            limits,
        )
        .map_err(CatalogError::CatalogRejected)
    })
    .await
    .map_err(|_| CatalogError::CatalogUnavailable)?
}
