//! Final writer fence for the existing Root context revision and birth marker.
//! This uses the small canonical sidecar, never the transcript or index.

use super::*;
use bamboo_domain::{
    RootModeOperationOutcome, RootModeOperationReceipt, SessionAuthorityConflict,
    ROOT_MODE_OPERATION_HISTORY_LIMIT,
};

pub(super) const ROOT_TOOL_AUTHORITY_PROOF_FILE: &str = "root-tool-authority.json";
const ROOT_TOOL_AUTHORITY_PROOF_MIGRATION_MARKER: &str = ".root_tool_authority_proof_v1";
const ROOT_TOOL_AUTHORITY_PROOF_MAX_BYTES: u64 = 4096;

fn root_tool_proof_version(session: &Session) -> u32 {
    // Pre-mode Roots retain their readable v1 proof. The first terminal mode
    // operation upgrades it to v2, which an older v1-only final writer rejects
    // instead of dropping the operation epoch/history during a mixed rollout.
    if session.root_mode_transition_epoch > 0 || !session.root_mode_operations.is_empty() {
        2
    } else {
        1
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum ProofState {
    Prepared,
    Committed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum RootToolProofFault {
    Prepared,
    Runtime,
    Main,
    Committed,
}

#[derive(Deserialize, Serialize)]
struct RootToolAuthorityProof {
    version: u32,
    state: ProofState,
    id: String,
    created_at: DateTime<Utc>,
    authority_identity: SessionAuthorityIdentity,
    root_orchestration_only: bool,
    root_tool_authority_revision: u64,
    #[serde(default)]
    root_mode_transition_epoch: u64,
    #[serde(default)]
    root_mode_operations: Vec<RootModeOperationReceipt>,
}

impl RootToolAuthorityProof {
    fn from_session(session: &Session, state: ProofState) -> Self {
        Self {
            version: root_tool_proof_version(session),
            state,
            id: session.id.clone(),
            created_at: session.created_at,
            authority_identity: session.authority_identity.clone(),
            root_orchestration_only: session.root_orchestration_only,
            root_tool_authority_revision: session.root_tool_authority_revision,
            root_mode_transition_epoch: session.root_mode_transition_epoch,
            root_mode_operations: session.root_mode_operations.clone(),
        }
    }

    fn matches(&self, session: &Session) -> bool {
        self.version == root_tool_proof_version(session)
            && self.id == session.id
            && self.created_at == session.created_at
            && self.authority_identity == session.authority_identity
            && self.root_orchestration_only == session.root_orchestration_only
            && self.root_tool_authority_revision == session.root_tool_authority_revision
            && self.root_mode_transition_epoch == session.root_mode_transition_epoch
            && self.root_mode_operations == session.root_mode_operations
    }
}

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
    #[serde(default)]
    root_mode_transition_epoch: u64,
    #[serde(default)]
    root_mode_operations: Vec<RootModeOperationReceipt>,
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
            root_mode_transition_epoch: session.root_mode_transition_epoch,
            root_mode_operations: session.root_mode_operations.clone(),
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
    /// A missing rebuildable index row is not proof that a deterministic Root
    /// directory is absent. Keep an incomplete legacy Root visible as a
    /// recovery error instead of silently reporting a nonexistent Session.
    pub(super) async fn ensure_no_unindexed_root(&self, id: &str) -> io::Result<()> {
        validate_session_id(id)?;
        // A stale index path may be repaired by the caller, but only if the
        // deterministic Root still has a complete, proven canonical pair.
        // A missing main file with a surviving index row remains an error.
        if self.get_index_entry(id).await.is_some()
            && self.load_root_authority_unchecked(id).await?.is_some()
        {
            return Ok(());
        }
        let directory = self.sessions_dir.join(id);
        match fs::symlink_metadata(&directory).await {
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(conflict(format!("canonical Root directory: {error}"))),
            Ok(metadata) if !metadata.file_type().is_dir() => {
                return Err(conflict("canonical Root placement is invalid"));
            }
            Ok(_) => {}
        }
        // A revocation can leave the physical directory behind until deletion
        // reconciliation. Its ordinary lifetime helper reads the full main
        // transcript, so use the bounded sidecar and committed proof here.
        // Missing authority beside a retained main remains a recovery error.
        if let Some(cutoff) = self
            .root_revocation(id)
            .await
            .map_err(|error| conflict(error.to_string()))?
        {
            let main_exists = regular_file_exists(&directory.join("session.json")).await?;
            let side = Self::read_runtime_sidecar_at(&directory.join(RUNTIME_SIDECAR_FILE), id)
                .await
                .map_err(|error| conflict(error.to_string()))?;
            if let Some(side) = side {
                if side.id != id {
                    return Err(conflict("revoked Root sidecar identity mismatch"));
                }
                self.validate_root_tool_proof(&side).await?;
                if side.created_at <= cutoff {
                    return Ok(());
                }
            } else if !main_exists {
                return Ok(());
            }
        }
        Err(conflict(
            "Root index is missing while canonical files require recovery",
        ))
    }
    pub(super) fn maybe_fail_root_tool_proof(&self, fault: RootToolProofFault) -> io::Result<()> {
        #[cfg(test)]
        {
            let mut pending = self
                .root_tool_proof_fault
                .lock()
                .expect("Root tool proof fault lock");
            if pending.as_ref() == Some(&fault) {
                *pending = None;
                return Err(other_io_error(format!(
                    "injected Root tool proof fault: {fault:?}"
                )));
            }
        }
        #[cfg(not(test))]
        let _ = fault;
        Ok(())
    }
    async fn read_root_tool_proof(&self, id: &str) -> io::Result<RootToolAuthorityProof> {
        let path = self
            .sessions_dir
            .join(id)
            .join(ROOT_TOOL_AUTHORITY_PROOF_FILE);
        let metadata = fs::symlink_metadata(&path)
            .await
            .map_err(|error| conflict(format!("canonical Root authority proof: {error}")))?;
        if !metadata.file_type().is_file() || metadata.len() > ROOT_TOOL_AUTHORITY_PROOF_MAX_BYTES {
            return Err(conflict("canonical Root authority proof is invalid"));
        }
        let bytes = fs::read(&path)
            .await
            .map_err(|error| conflict(format!("canonical Root authority proof: {error}")))?;
        serde_json::from_slice(&bytes)
            .map_err(|error| conflict(format!("invalid canonical Root authority proof: {error}")))
    }

    pub(super) async fn validate_root_tool_proof(&self, side: &Session) -> io::Result<()> {
        Self::validate_root_tool_proof_identity(side)?;
        let proof = self.read_root_tool_proof(&side.id).await?;
        Self::validate_root_tool_proof_value(side, proof)
    }

    #[cfg(any(target_os = "linux", target_os = "macos", windows))]
    pub(super) fn validate_snapshot_root_pair(main: &Session, side: &Session) -> io::Result<()> {
        Self::validate_root_tool_authority_pair(&RootToolAuthorityMain::from(main), side)
    }

    /// Pure validation for callers that already read the proof under their own
    /// bounded capability/transaction boundary. This never opens or repairs files.
    #[cfg(any(target_os = "linux", target_os = "macos", windows))]
    pub(super) fn validate_snapshot_root_proof(side: &Session, bytes: &[u8]) -> io::Result<()> {
        let proof = serde_json::from_slice(bytes).map_err(|_| conflict("invalid proof"))?;
        Self::validate_root_tool_proof_value(side, proof)
    }

    fn validate_root_tool_proof_value(
        side: &Session,
        proof: RootToolAuthorityProof,
    ) -> io::Result<()> {
        Self::validate_root_tool_proof_identity(side)?;
        if proof.state != ProofState::Committed || !proof.matches(side) {
            return Err(conflict(
                "canonical Root authority proof is pending or stale",
            ));
        }
        Ok(())
    }

    fn validate_root_tool_proof_identity(side: &Session) -> io::Result<()> {
        if side.kind != SessionKind::Root
            || side.parent_session_id.is_some()
            || side.spawn_depth != 0
            || (!side.root_session_id.is_empty() && side.root_session_id != side.id)
            || (side.root_orchestration_only && side.root_tool_authority_revision == 0)
        {
            return Err(conflict("canonical Root authority proof identity mismatch"));
        }
        Ok(())
    }

    async fn write_root_tool_proof_at(
        directory: &Path,
        session: &Session,
        state: ProofState,
    ) -> io::Result<()> {
        let bytes = serde_json::to_vec(&RootToolAuthorityProof::from_session(session, state))
            .map_err(|error| conflict(error.to_string()))?;
        durable_atomic_write(&directory.join(ROOT_TOOL_AUTHORITY_PROOF_FILE), &bytes).await
    }

    /// Staging directories are unpublished until both Session files and this
    /// committed proof are durable. They need no visible prepared state.
    pub(super) async fn write_staged_root_tool_proof(
        directory: &Path,
        session: &Session,
    ) -> io::Result<()> {
        if session.kind == SessionKind::Root {
            Self::write_root_tool_proof_at(directory, session, ProofState::Committed).await?;
        }
        Ok(())
    }

    async fn write_default_root_tool_proof(
        &self,
        directory: &Path,
        session: &Session,
        state: ProofState,
        guards: &Arc<DefaultWriterGuards>,
    ) -> io::Result<()> {
        let bytes = serde_json::to_vec(&RootToolAuthorityProof::from_session(session, state))
            .map_err(|error| conflict(error.to_string()))?;
        self.write_default_bytes(
            &directory.join(ROOT_TOOL_AUTHORITY_PROOF_FILE),
            bytes,
            guards,
        )
        .await
    }

    /// A selection publishes a durable Prepared marker before the sidecar and
    /// main writes. Any interrupted phase is unavailable to operational reads.
    pub(super) async fn prepare_root_tool_proof_for_full_save(
        &self,
        directory: &Path,
        incoming: &Session,
        guards: &Arc<DefaultWriterGuards>,
    ) -> io::Result<bool> {
        if incoming.kind != SessionKind::Root {
            return Ok(false);
        }
        let path = directory.join(ROOT_TOOL_AUTHORITY_PROOF_FILE);
        if regular_file_exists(&path).await? {
            let current = self.read_root_tool_proof(&incoming.id).await?;
            if current.state == ProofState::Committed && current.matches(incoming) {
                return Ok(false);
            }
        }
        self.write_default_root_tool_proof(directory, incoming, ProofState::Prepared, guards)
            .await?;
        Ok(true)
    }

    pub(super) async fn commit_root_tool_proof_after_full_save(
        &self,
        directory: &Path,
        incoming: &Session,
        prepared: bool,
        guards: &Arc<DefaultWriterGuards>,
    ) -> io::Result<()> {
        if prepared {
            self.write_default_root_tool_proof(directory, incoming, ProofState::Committed, guards)
                .await?;
        }
        Ok(())
    }

    /// Upgrade pre-proof V2 Roots once, before serving requests. A missing
    /// sidecar, corrupt pair, or existing damaged proof is never reconstructed.
    /// Subsequent boots trust only the durable marker and each Root's proof;
    /// otherwise loss of a newer proof could be mistaken for a fresh upgrade.
    pub(super) async fn migrate_root_tool_authority_proofs(
        &self,
        publish_marker: bool,
    ) -> io::Result<()> {
        let marker = self
            .bamboo_home_dir
            .join(ROOT_TOOL_AUTHORITY_PROOF_MIGRATION_MARKER);
        match fs::symlink_metadata(&marker).await {
            Ok(metadata) if metadata.file_type().is_file() => return Ok(()),
            Ok(_) => return Err(conflict("Root authority proof migration marker is invalid")),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        let mut roots = fs::read_dir(&self.sessions_dir).await?;
        while let Some(entry) = roots.next_entry().await? {
            if !entry.file_type().await?.is_dir() {
                continue;
            }
            let Ok(id) = entry.file_name().into_string() else {
                continue;
            };
            if validate_session_id(&id).is_err() {
                continue;
            }
            let directory = entry.path();
            match fs::symlink_metadata(directory.join(ROOT_TOOL_AUTHORITY_PROOF_FILE)).await {
                Ok(_) => continue,
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
            let pair = async {
                let main_path = directory.join("session.json");
                let side_path = directory.join(RUNTIME_SIDECAR_FILE);
                if !regular_file_exists(&main_path).await?
                    || !regular_file_exists(&side_path).await?
                {
                    return Err(conflict("legacy Root canonical pair is incomplete"));
                }
                let main_bytes = fs::read(main_path).await?;
                let side_bytes = fs::read(side_path).await?;
                compact_main::validate_full_main(&main_bytes)?;
                let main: Session = serde_json::from_slice(&main_bytes)?;
                let side: Session = serde_json::from_slice(&side_bytes)?;
                Self::validate_root_tool_authority_pair(
                    &RootToolAuthorityMain::from(&main),
                    &side,
                )?;
                // An old sidecar-first full save may have stopped before main
                // publication. Do not turn that incomplete selection, especially
                // an explicit disable, into a committed proof during upgrade.
                if main.root_tool_authority_revision != side.root_tool_authority_revision
                    || main.root_orchestration_only != side.root_orchestration_only
                {
                    return Err(conflict("legacy Root tool selection is incomplete"));
                }
                Ok::<Session, io::Error>(side)
            }
            .await;
            match pair {
                Ok(side) if side.id == id => {
                    Self::write_root_tool_proof_at(&directory, &side, ProofState::Committed)
                        .await?;
                }
                Ok(_) => {
                    tracing::warn!(session_id = %id, "Root authority proof migration skipped misplaced Root")
                }
                Err(error) => {
                    tracing::warn!(session_id = %id, %error, "Root authority proof migration skipped unavailable Root")
                }
            }
        }
        if publish_marker {
            durable_atomic_write(&marker, b"root-tool-authority-proof-v1\n").await?;
        }
        Ok(())
    }

    /// A Root with an unavailable runtime sidecar has no provable live tool
    /// authority. Legacy main-only Roots remain visible in the index, but
    /// operational reads must report a recovery error instead of reopening
    /// their possibly stale unrestricted main snapshot.
    pub(super) async fn validate_root_tool_authority_overlay(
        &self,
        requested_id: &str,
        main: &Session,
        side: Option<&Session>,
    ) -> io::Result<()> {
        validate_session_id(requested_id)?;
        if main.id != requested_id || side.is_some_and(|side| side.id != requested_id) {
            return Err(conflict(
                "canonical Session ID does not match requested placement",
            ));
        }
        if main.kind != SessionKind::Root {
            let root_placement = match fs::symlink_metadata(self.sessions_dir.join(requested_id))
                .await
            {
                Ok(_) => true,
                Err(error) if error.kind() == io::ErrorKind::NotFound => false,
                Err(error) => return Err(conflict(format!("canonical Root directory: {error}"))),
            };
            if root_placement || side.is_some_and(|side| side.kind == SessionKind::Root) {
                return Err(conflict("canonical Root placement claims Child identity"));
            }
            return Ok(());
        }
        let side = side.ok_or_else(|| conflict("canonical runtime file is missing or corrupt"))?;
        Self::validate_root_tool_authority_pair(&RootToolAuthorityMain::from(main), side)?;
        self.validate_root_tool_proof(side).await?;
        self.validate_supervisor_proof(side).await
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
            || side.root_mode_transition_epoch != main.root_mode_transition_epoch
            || side.root_mode_operations != main.root_mode_operations
            || (side.root_orchestration_only && side.root_tool_authority_revision == 0)
        {
            return Err(conflict(
                "canonical Root tool authority is stale or inconsistent",
            ));
        }
        Ok(())
    }

    /// The one-time Supervisor upgrade verifies the existing Root proof and
    /// canonical pair before the new Supervisor proof exists. Normal readers
    /// must use `validate_root_tool_authority_overlay` instead.
    pub(super) async fn validate_root_tool_pair_for_supervisor_migration(
        &self,
        main: &Session,
        side: &Session,
    ) -> io::Result<()> {
        Self::validate_root_tool_authority_pair(&RootToolAuthorityMain::from(main), side)?;
        self.validate_root_tool_proof(side).await
    }

    /// Operational control-plane loads use only the bounded runtime sidecar
    /// and committed proof. The main transcript is checked on full loads.
    pub(super) async fn validate_root_tool_authority_against_proof(
        &self,
        requested_id: &str,
        side: &Session,
    ) -> io::Result<()> {
        validate_session_id(requested_id)?;
        if side.id != requested_id {
            return Err(conflict(
                "runtime sidecar does not match the requested Session",
            ));
        }
        if side.kind != SessionKind::Root {
            // A Root's physical directory is durable identity evidence even if
            // its runtime JSON claims to be a Child. Genuine children live
            // under sessions/<root>/children/<child>, never sessions/<child>.
            match fs::symlink_metadata(self.sessions_dir.join(requested_id)).await {
                Ok(_) => return Err(conflict("canonical Root sidecar claims Child identity")),
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(conflict(format!("canonical Root directory: {error}"))),
            }
            return Ok(());
        }
        let path = self.sessions_dir.join(requested_id).join("session.json");
        if !regular_file_exists(&path).await? {
            return Err(conflict("canonical main file is missing"));
        }
        self.validate_root_tool_proof(side).await?;
        self.validate_supervisor_proof(side).await
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
        self.validate_root_tool_proof(&current).await?;
        self.validate_supervisor_proof(&current).await?;
        if full && has_main {
            // A full save already serializes the transcript and must not
            // replace damaged or stale canonical history from a caller's
            // snapshot. Only runtime-only paths skip this large read.
            let bytes = fs::read(directory.join("session.json"))
                .await
                .map_err(|error| conflict(format!("canonical main file: {error}")))?;
            compact_main::validate_full_main(&bytes)
                .map_err(|error| conflict(error.to_string()))?;
            let main: Session = serde_json::from_slice(&bytes)
                .map_err(|error| conflict(format!("invalid canonical main: {error}")))?;
            supervisor::validate_overlay(&main, Some(&current))
                .map_err(|error| conflict(error.to_string()))?;
            Self::validate_root_tool_authority_pair(&RootToolAuthorityMain::from(&main), &current)?;
        }
        if incoming.metadata_version < current.metadata_version {
            return Err(conflict(
                "metadata revision regressed; reload before saving",
            ));
        }
        if incoming.root_mode_operations.len() > ROOT_MODE_OPERATION_HISTORY_LIMIT {
            return Err(conflict("Root mode operation history exceeds its bound"));
        }
        if incoming.root_mode_transition_epoch == current.root_mode_transition_epoch {
            if incoming.root_mode_operations != current.root_mode_operations {
                return Err(conflict(
                    "Root mode operation changed without advancing its epoch",
                ));
            }
        } else {
            if !full
                || current.root_mode_transition_epoch.checked_add(1)
                    != Some(incoming.root_mode_transition_epoch)
            {
                return Err(conflict("Root mode transition epoch is stale or skipped"));
            }
            let Some(receipt) = incoming.root_mode_operations.last() else {
                return Err(conflict("Root mode transition has no terminal receipt"));
            };
            if receipt.expected_epoch != current.root_mode_transition_epoch
                || receipt.resulting_epoch != incoming.root_mode_transition_epoch
                || receipt.enabled_at_completion != incoming.root_orchestration_only
                || receipt.tool_authority_revision != incoming.root_tool_authority_revision
                || current
                    .root_mode_operations
                    .iter()
                    .any(|previous| previous.operation_id == receipt.operation_id)
            {
                return Err(conflict(
                    "Root mode terminal receipt does not match its transition",
                ));
            }
            let mut expected_history = current.root_mode_operations.clone();
            expected_history.push(receipt.clone());
            if expected_history.len() > ROOT_MODE_OPERATION_HISTORY_LIMIT {
                expected_history.remove(0);
            }
            if incoming.root_mode_operations != expected_history {
                return Err(conflict(
                    "Root mode terminal history is stale or inconsistent",
                ));
            }
            match receipt.outcome {
                RootModeOperationOutcome::Committed
                    if receipt.requested_enabled == incoming.root_orchestration_only => {}
                RootModeOperationOutcome::Fenced
                | RootModeOperationOutcome::RejectedIncompatible
                    if incoming.root_orchestration_only == current.root_orchestration_only
                        && incoming.root_tool_authority_revision
                            == current.root_tool_authority_revision => {}
                _ => return Err(conflict("Root mode terminal outcome is inconsistent")),
            }
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
