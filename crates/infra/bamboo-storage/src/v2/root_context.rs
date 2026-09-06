//! Final writer fence for the existing Root context revision and birth marker.
//! This uses the small canonical sidecar, never the transcript or index.

use super::*;
use bamboo_domain::SessionAuthorityConflict;

fn conflict(message: impl Into<String>) -> io::Error {
    io::Error::new(
        io::ErrorKind::WouldBlock,
        SessionAuthorityConflict(format!(
            "Root context changed or unavailable: {}",
            message.into()
        )),
    )
}

async fn regular_file_exists(path: &Path) -> io::Result<bool> {
    let metadata = match fs::symlink_metadata(path).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(conflict(format!("{}: {error}", path.display()))),
    };
    if !metadata.file_type().is_file() {
        return Err(conflict("canonical Root context is not a regular file"));
    }
    Ok(true)
}

async fn empty_creation_layout(directory: &Path) -> io::Result<bool> {
    let mut entries = fs::read_dir(directory).await?;
    while let Some(entry) = entries.next_entry().await? {
        if !matches!(entry.file_name().to_str(), Some("children" | "attachments"))
            || !entry.file_type().await?.is_dir()
            || fs::read_dir(entry.path())
                .await?
                .next_entry()
                .await?
                .is_some()
        {
            return Ok(false);
        }
    }
    Ok(true)
}

impl SessionStoreV2 {
    /// The caller holds either the ordinary per-session writer lock or the
    /// exclusive Task/lifecycle boundary that excludes all ordinary writers.
    /// A missing sidecar beside an existing main file is ambiguous: it may be
    /// legacy, or may have lost a newer Project revision. History readers may
    /// fall back to main, but no writer may republish that fallback as authority.
    pub(super) async fn validate_root_context_for_save(
        &self,
        incoming: &Session,
    ) -> io::Result<()> {
        self.validate_root_context_for_write(incoming, false).await
    }

    /// A full save may finish an interrupted create, or restore a missing main
    /// file from a still-valid runtime fence. It cannot advance that fence while
    /// completing the pair. Ordinary runtime/Task writes cannot do this repair.
    pub(super) async fn validate_root_context_for_full_save(
        &self,
        incoming: &Session,
    ) -> io::Result<()> {
        self.validate_root_context_for_write(incoming, true).await
    }

    async fn validate_root_context_for_write(
        &self,
        incoming: &Session,
        full: bool,
    ) -> io::Result<()> {
        validate_session_id(&incoming.id)?;
        let directory = self.sessions_dir.join(&incoming.id);
        match fs::symlink_metadata(&directory).await {
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(conflict(error.to_string())),
            Ok(metadata) if !metadata.file_type().is_dir() => {
                return Err(conflict("canonical Root directory is not a real directory"));
            }
            Ok(_) => {}
        }
        if incoming.kind != SessionKind::Root {
            return Err(conflict(
                "an existing Root cannot be overwritten as a Child",
            ));
        }
        let has_main = regular_file_exists(&directory.join("session.json")).await?;
        let runtime = directory.join(RUNTIME_SIDECAR_FILE);
        let has_runtime = regular_file_exists(&runtime).await?;
        if !has_main {
            if !full {
                return Err(conflict("canonical main file is missing"));
            }
            if !has_runtime
                && empty_creation_layout(&directory)
                    .await
                    .map_err(|error| conflict(error.to_string()))?
            {
                return Ok(());
            }
        }
        if !has_runtime {
            return Err(conflict("canonical runtime file is missing"));
        }
        let bytes = fs::read(runtime)
            .await
            .map_err(|error| conflict(error.to_string()))?;
        let current: Session = serde_json::from_slice(&bytes)
            .map_err(|error| conflict(format!("invalid canonical runtime: {error}")))?;
        if current.id != incoming.id
            || current.kind != SessionKind::Root
            || current.parent_session_id.is_some()
            || current.spawn_depth != 0
            || (!current.root_session_id.is_empty() && current.root_session_id != current.id)
            || incoming.parent_session_id.is_some()
            || incoming.spawn_depth != 0
            || (!incoming.root_session_id.is_empty() && incoming.root_session_id != incoming.id)
            || current.created_at != incoming.created_at
            || current.authority_identity != incoming.authority_identity
        {
            return Err(conflict(
                "writer does not match the durable Root creation identity",
            ));
        }
        if incoming.metadata_version < current.metadata_version {
            return Err(conflict(
                "metadata revision regressed; reload before saving",
            ));
        }
        if incoming.project_id_meta() != current.project_id_meta()
            && current.metadata_version.checked_add(1) != Some(incoming.metadata_version)
        {
            return Err(conflict(
                "Project changes require the next metadata revision",
            ));
        }
        if !has_main
            && (incoming.metadata_version != current.metadata_version
                || incoming.project_id_meta() != current.project_id_meta())
        {
            return Err(conflict(
                "completing a partial Root cannot advance its context",
            ));
        }
        Ok(())
    }
}
