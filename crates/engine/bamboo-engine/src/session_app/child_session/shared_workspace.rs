//! Compact delegation shares a selected parent directory, never a new default.
use super::{ChildSessionError, ChildSessionPort};
use crate::project_context::{ProjectContextResolver, SessionProjectIdentity, WorkspaceSource};
use bamboo_domain::Session;
use std::path::{Path, PathBuf};

fn invalid() -> ChildSessionError {
    ChildSessionError::InvalidArguments(
        "shared_parent_workspace_unavailable: SubAgent requires the parent's existing absolute workspace; explicitly select a usable parent workspace before delegating".into(),
    )
}

fn parent_workspace(parent: &Session) -> Result<PathBuf, ChildSessionError> {
    // Typed/legacy metadata wins even if empty or unusable. Such an explicit
    // selection cannot be replaced with a legacy field or the process cwd.
    let selected = parent
        .workspace_path_meta()
        .or_else(|| parent.workspace.clone());
    let selected = selected
        .as_deref()
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(invalid)?;
    let path = Path::new(selected);
    if !path.is_absolute() || !path.is_dir() {
        return Err(invalid());
    }
    path.canonicalize().map_err(|_| invalid())
}

/// Resolve from a freshly loaded canonical parent, then apply the Host's
/// existing Project/confinement checks. A successful redirect is not sharing.
pub async fn resolve_shared_child_workspace(
    port: &dyn ChildSessionPort,
    parent: &Session,
) -> Result<(String, WorkspaceSource), ChildSessionError> {
    let project = match ProjectContextResolver::session_project_identity(parent) {
        SessionProjectIdentity::Assigned(id) => Some(id),
        SessionProjectIdentity::Unassigned => None,
        SessionProjectIdentity::Invalid { .. } => return Err(invalid()),
    };
    let selected = parent_workspace(parent)?;
    let selected = bamboo_config::paths::path_to_display_string(&selected);
    let validated = port
        .validate_child_workspace(project.as_ref(), &selected)
        .await?;
    require_same_workspace(&selected, &validated)?;
    // Pin this Session selection; a later Project-default refresh must not
    // reinterpret it as an instruction to move the child to another checkout.
    Ok((selected, WorkspaceSource::Session))
}

pub(super) fn require_same_workspace(
    selected: &str,
    validated: &str,
) -> Result<(), ChildSessionError> {
    // Canonicalize both sides: Windows display paths omit the verbatim prefix
    // even when canonicalize() returns it. Compare paths, not their spelling.
    let expected = Path::new(selected).canonicalize().map_err(|_| invalid())?;
    let actual = Path::new(validated);
    if !actual.is_absolute()
        || !actual.is_dir()
        || actual.canonicalize().map_err(|_| invalid())? != expected
    {
        return Err(invalid());
    }
    Ok(())
}
