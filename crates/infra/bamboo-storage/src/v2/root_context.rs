//! Final writer fence for the existing Root context revision and birth marker.
//! This uses the small canonical sidecar, never the transcript or index.

use super::*;
use bamboo_domain::SessionAuthorityConflict;

/// Deserialize only the Root authority fields. Tool boundaries compare the
/// canonical pair without allocating the possibly large message transcript.
#[derive(Deserialize)]
struct RootToolAuthorityMain {
    id: String,
    created_at: DateTime<Utc>,
    #[serde(default)]
    kind: SessionKind,
    #[serde(default)]
    root_session_id: String,
    #[serde(default)]
    parent_session_id: Option<String>,
    #[serde(default)]
    spawn_depth: u32,
    #[serde(default)]
    authority_identity: SessionAuthorityIdentity,
    #[serde(default)]
    root_orchestration_only: bool,
    #[serde(default)]
    root_tool_authority_revision: u64,
}

impl From<&Session> for RootToolAuthorityMain {
    fn from(session: &Session) -> Self {
        Self {
            id: session.id.clone(),
            created_at: session.created_at,
            kind: session.kind,
            root_session_id: session.root_session_id.clone(),
            parent_session_id: session.parent_session_id.clone(),
            spawn_depth: session.spawn_depth,
            authority_identity: session.authority_identity.clone(),
            root_orchestration_only: session.root_orchestration_only,
            root_tool_authority_revision: session.root_tool_authority_revision,
        }
    }
}

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
    /// A Root with an unavailable runtime sidecar has no provable live tool
    /// authority. Legacy main-only Roots remain visible in the index, but
    /// operational reads must report a recovery error instead of reopening
    /// their possibly stale unrestricted main snapshot.
    pub(super) fn validate_root_tool_authority_overlay(
        main: &Session,
        side: Option<&Session>,
    ) -> io::Result<()> {
        if main.kind != SessionKind::Root {
            return Ok(());
        }
        let side = side.ok_or_else(|| conflict("canonical runtime file is missing or corrupt"))?;
        Self::validate_root_tool_authority_pair(&RootToolAuthorityMain::from(main), side)
    }

    fn validate_root_tool_authority_pair(
        main: &RootToolAuthorityMain,
        side: &Session,
    ) -> io::Result<()> {
        if main.kind != SessionKind::Root
            || main.parent_session_id.is_some()
            || main.spawn_depth != 0
            || (!main.root_session_id.is_empty() && main.root_session_id != main.id)
            || (main.root_orchestration_only && main.root_tool_authority_revision == 0)
            || side.id != main.id
            || side.kind != SessionKind::Root
            || side.parent_session_id.is_some()
            || side.spawn_depth != 0
            || (!side.root_session_id.is_empty() && side.root_session_id != side.id)
            || side.created_at != main.created_at
            || side.authority_identity != main.authority_identity
            || side.root_tool_authority_revision < main.root_tool_authority_revision
            || (side.root_tool_authority_revision == main.root_tool_authority_revision
                && side.root_orchestration_only != main.root_orchestration_only)
            || (side.root_orchestration_only && side.root_tool_authority_revision == 0)
        {
            return Err(conflict(
                "canonical Root tool authority is stale or inconsistent",
            ));
        }
        Ok(())
    }

    /// Runtime control-plane loads and final writers must also prove that a
    /// parseable sidecar has not regressed behind the canonical main file.
    pub(super) async fn validate_root_tool_authority_against_main(
        &self,
        side: &Session,
    ) -> io::Result<()> {
        if side.kind != SessionKind::Root {
            return Ok(());
        }
        validate_session_id(&side.id)?;
        let path = self.sessions_dir.join(&side.id).join("session.json");
        if !regular_file_exists(&path).await? {
            return Err(conflict("canonical main file is missing"));
        }
        let bytes = fs::read(path)
            .await
            .map_err(|error| conflict(format!("canonical main file: {error}")))?;
        let main: RootToolAuthorityMain = serde_json::from_slice(&bytes)
            .map_err(|error| conflict(format!("invalid canonical main: {error}")))?;
        Self::validate_root_tool_authority_pair(&main, side)
    }

    /// The caller holds either the ordinary per-session writer lock or the
    /// exclusive Task/lifecycle boundary that excludes all ordinary writers.
    /// A missing sidecar beside an existing main file is ambiguous: it may be
    /// legacy, or may have lost a newer Project or tool revision. Operational
    /// readers and writers both reject that ambiguous Root state.
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
        if incoming.root_orchestration_only && incoming.root_tool_authority_revision == 0 {
            return Err(conflict("Root tool authority has no selection revision"));
        }
        supervisor::validate_identity(incoming).map_err(|error| conflict(error.to_string()))?;
        self.validate_root_lifetime_for_write(incoming).await?;
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
        supervisor::validate_identity(&current).map_err(|error| conflict(error.to_string()))?;
        if current.supervisor_management != incoming.supervisor_management {
            return Err(conflict(
                "Supervisor management changed; reload before saving",
            ));
        }
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
        if has_main {
            self.validate_root_tool_authority_against_main(&current)
                .await?;
        }
        if incoming.metadata_version < current.metadata_version {
            return Err(conflict(
                "metadata revision regressed; reload before saving",
            ));
        }
        if (current.root_orchestration_only && current.root_tool_authority_revision == 0)
            || incoming.root_tool_authority_revision < current.root_tool_authority_revision
        {
            return Err(conflict("Root tool authority revision regressed"));
        }
        if !full && incoming.root_tool_authority_revision != current.root_tool_authority_revision {
            return Err(conflict(
                "Root tool authority selection requires a full Session save",
            ));
        }
        if incoming.root_orchestration_only != current.root_orchestration_only {
            if current.root_tool_authority_revision.checked_add(1)
                != Some(incoming.root_tool_authority_revision)
            {
                return Err(conflict(
                    "Root tool authority change requires the next revision",
                ));
            }
        } else if incoming.root_tool_authority_revision != current.root_tool_authority_revision {
            return Err(conflict(
                "Root tool authority revision changed without a selection",
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
                || incoming.project_id_meta() != current.project_id_meta()
                || incoming.root_tool_authority_revision != current.root_tool_authority_revision)
        {
            return Err(conflict(
                "completing a partial Root cannot advance its context",
            ));
        }
        Ok(())
    }
}
