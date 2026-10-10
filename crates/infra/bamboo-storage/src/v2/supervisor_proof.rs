//! Bounded durable authority for the fixed default Supervisor. This record is
//! independent of conversation history and never grants authority by itself:
//! operational readers require the matching canonical runtime sidecar and a
//! real main file at the deterministic Root placement.

use super::supervisor_management::SupervisorManagementGuards;
use super::*;
use bamboo_domain::{SessionAuthorityConflict, SupervisorManagementState};

pub(super) const SUPERVISOR_PROOF_FILE: &str = "supervisor-authority.json";
pub(super) const SUPERVISOR_PROOF_MIGRATION_MARKER: &str = ".supervisor_authority_proof_v1";
// 256 bounded links, each with a <=256-byte Session ID and <=64-byte Project
// ID, fit comfortably below this limit even at full tombstone capacity.
const SUPERVISOR_PROOF_MAX_BYTES: u64 = 256 * 1024;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum ProofState {
    Prepared,
    Committed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SupervisorProofFault {
    Prepared,
    Runtime,
    Committed,
}

#[derive(Deserialize, Serialize)]
struct SupervisorAuthorityProof {
    version: u32,
    state: ProofState,
    id: String,
    created_at: DateTime<Utc>,
    authority_identity: SessionAuthorityIdentity,
    root_orchestration_only: bool,
    root_tool_authority_revision: u64,
    supervisor_management: Option<SupervisorManagementState>,
}

impl SupervisorAuthorityProof {
    fn from_session(session: &Session, state: ProofState) -> Self {
        Self {
            version: 1,
            state,
            id: session.id.clone(),
            created_at: session.created_at,
            authority_identity: session.authority_identity.clone(),
            root_orchestration_only: session.root_orchestration_only,
            root_tool_authority_revision: session.root_tool_authority_revision,
            supervisor_management: session.supervisor_management.clone(),
        }
    }

    fn matches(&self, session: &Session) -> bool {
        self.version == 1
            && self.id == session.id
            && self.created_at == session.created_at
            && self.authority_identity == session.authority_identity
            && self.root_orchestration_only == session.root_orchestration_only
            && self.root_tool_authority_revision == session.root_tool_authority_revision
            && self.supervisor_management == session.supervisor_management
    }
}

fn conflict(message: impl Into<String>) -> io::Error {
    io::Error::new(
        io::ErrorKind::WouldBlock,
        SessionAuthorityConflict(format!(
            "Supervisor authority changed or unavailable: {}",
            message.into()
        )),
    )
}

impl SessionStoreV2 {
    pub(super) fn maybe_fail_supervisor_proof(
        &self,
        fault: SupervisorProofFault,
    ) -> io::Result<()> {
        #[cfg(test)]
        {
            let mut pending = self
                .supervisor_proof_fault
                .lock()
                .expect("Supervisor proof fault lock");
            if pending.as_ref() == Some(&fault) {
                *pending = None;
                return Err(other_io_error(format!(
                    "injected Supervisor proof fault: {fault:?}"
                )));
            }
        }
        #[cfg(not(test))]
        let _ = fault;
        Ok(())
    }

    async fn read_supervisor_proof(&self) -> io::Result<SupervisorAuthorityProof> {
        let path = self
            .sessions_dir
            .join(DEFAULT_SUPERVISOR_SESSION_ID)
            .join(SUPERVISOR_PROOF_FILE);
        let metadata = fs::symlink_metadata(&path)
            .await
            .map_err(|error| conflict(format!("canonical proof: {error}")))?;
        if !metadata.file_type().is_file() || metadata.len() > SUPERVISOR_PROOF_MAX_BYTES {
            return Err(conflict("canonical proof is not a bounded regular file"));
        }
        let bytes = fs::read(&path)
            .await
            .map_err(|error| conflict(format!("canonical proof: {error}")))?;
        serde_json::from_slice(&bytes)
            .map_err(|error| conflict(format!("invalid canonical proof: {error}")))
    }

    pub(super) async fn validate_supervisor_proof(&self, side: &Session) -> io::Result<()> {
        if !matches!(
            side.authority_identity,
            SessionAuthorityIdentity::Supervisor { .. }
        ) {
            return Ok(());
        }
        supervisor::validate_identity(side).map_err(|error| conflict(error.to_string()))?;
        let proof = self.read_supervisor_proof().await?;
        Self::validate_supervisor_proof_value(side, proof)
    }

    #[cfg(any(target_os = "linux", target_os = "macos", windows))]
    pub(super) fn validate_snapshot_supervisor_proof(
        side: &Session,
        bytes: &[u8],
    ) -> io::Result<()> {
        supervisor::validate_identity(side).map_err(|_| conflict("invalid identity"))?;
        let proof = serde_json::from_slice(bytes).map_err(|_| conflict("invalid proof"))?;
        Self::validate_supervisor_proof_value(side, proof)
    }

    fn validate_supervisor_proof_value(
        side: &Session,
        proof: SupervisorAuthorityProof,
    ) -> io::Result<()> {
        if proof.state != ProofState::Committed || !proof.matches(side) {
            return Err(conflict("canonical proof is pending or stale"));
        }
        Ok(())
    }

    async fn write_proof_at(
        directory: &Path,
        session: &Session,
        state: ProofState,
    ) -> io::Result<()> {
        let bytes = serde_json::to_vec(&SupervisorAuthorityProof::from_session(session, state))
            .map_err(|error| conflict(error.to_string()))?;
        if bytes.len() as u64 > SUPERVISOR_PROOF_MAX_BYTES {
            return Err(conflict("Supervisor proof exceeds bounded capacity"));
        }
        durable_atomic_write(&directory.join(SUPERVISOR_PROOF_FILE), &bytes).await
    }

    pub(super) async fn write_staged_supervisor_proof(
        directory: &Path,
        session: &Session,
    ) -> io::Result<()> {
        if matches!(
            session.authority_identity,
            SessionAuthorityIdentity::Supervisor { .. }
        ) {
            Self::write_proof_at(directory, session, ProofState::Committed).await?;
        }
        Ok(())
    }

    async fn write_default_supervisor_proof(
        &self,
        directory: &Path,
        session: &Session,
        state: ProofState,
        guards: &Arc<DefaultWriterGuards>,
    ) -> io::Result<()> {
        let bytes = serde_json::to_vec(&SupervisorAuthorityProof::from_session(session, state))
            .map_err(|error| conflict(error.to_string()))?;
        if bytes.len() as u64 > SUPERVISOR_PROOF_MAX_BYTES {
            return Err(conflict("Supervisor proof exceeds bounded capacity"));
        }
        self.write_default_bytes(&directory.join(SUPERVISOR_PROOF_FILE), bytes, guards)
            .await
    }

    pub(super) async fn prepare_supervisor_proof_for_full_save(
        &self,
        directory: &Path,
        incoming: &Session,
        guards: &Arc<DefaultWriterGuards>,
    ) -> io::Result<bool> {
        if !matches!(
            incoming.authority_identity,
            SessionAuthorityIdentity::Supervisor { .. }
        ) {
            return Ok(false);
        }
        let proof = self.read_supervisor_proof().await?;
        if proof.state == ProofState::Committed && proof.matches(incoming) {
            return Ok(false);
        }
        self.write_default_supervisor_proof(directory, incoming, ProofState::Prepared, guards)
            .await?;
        Ok(true)
    }

    pub(super) async fn commit_supervisor_proof_after_full_save(
        &self,
        directory: &Path,
        incoming: &Session,
        prepared: bool,
        guards: &Arc<DefaultWriterGuards>,
    ) -> io::Result<()> {
        if prepared {
            self.write_default_supervisor_proof(directory, incoming, ProofState::Committed, guards)
                .await?;
        }
        Ok(())
    }

    pub(super) async fn prepare_supervisor_management_proof(
        &self,
        updated: &Session,
        guards: &Arc<SupervisorManagementGuards>,
    ) -> io::Result<()> {
        let current = self.read_supervisor_proof().await?;
        if current.state != ProofState::Committed {
            return Err(conflict("canonical proof is pending"));
        }
        self.write_management_supervisor_proof(updated, ProofState::Prepared, guards)
            .await
    }

    pub(super) async fn commit_supervisor_management_proof(
        &self,
        updated: &Session,
        guards: &Arc<SupervisorManagementGuards>,
    ) -> io::Result<()> {
        self.write_management_supervisor_proof(updated, ProofState::Committed, guards)
            .await
    }

    async fn write_management_supervisor_proof(
        &self,
        updated: &Session,
        state: ProofState,
        guards: &Arc<SupervisorManagementGuards>,
    ) -> io::Result<()> {
        let bytes = serde_json::to_vec(&SupervisorAuthorityProof::from_session(updated, state))
            .map_err(|error| conflict(error.to_string()))?;
        if bytes.len() as u64 > SUPERVISOR_PROOF_MAX_BYTES {
            return Err(conflict("Supervisor proof exceeds bounded capacity"));
        }
        let stage = match state {
            ProofState::Prepared => SupervisorProofFault::Prepared,
            ProofState::Committed => SupervisorProofFault::Committed,
        };
        self.write_management_bytes(
            &self
                .sessions_dir
                .join(&updated.id)
                .join(SUPERVISOR_PROOF_FILE),
            bytes,
            stage,
            guards,
        )
        .await
    }

    /// One verified upgrade of the old fixed-ID pair. Run under the exclusive
    /// startup gate both before and after Task/copy recovery. A damaged existing
    /// proof is never regenerated, and the durable marker prevents later loss
    /// from being mistaken for an old installation.
    pub(super) async fn migrate_supervisor_proof(&self, publish_marker: bool) -> io::Result<()> {
        let marker = self.bamboo_home_dir.join(SUPERVISOR_PROOF_MIGRATION_MARKER);
        match fs::symlink_metadata(&marker).await {
            Ok(metadata) if metadata.file_type().is_file() => return Ok(()),
            Ok(_) => return Err(conflict("migration marker is invalid")),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        let directory = self.sessions_dir.join(DEFAULT_SUPERVISOR_SESSION_ID);
        if matches!(fs::symlink_metadata(&directory).await, Err(error) if error.kind() == io::ErrorKind::NotFound)
        {
            if publish_marker {
                durable_atomic_write(&marker, b"supervisor-authority-proof-v1\n").await?;
            }
            return Ok(());
        }
        let proof_path = directory.join(SUPERVISOR_PROOF_FILE);
        match fs::symlink_metadata(&proof_path).await {
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let pair = async {
                    let main_bytes = fs::read(directory.join("session.json")).await?;
                    let side_bytes = fs::read(directory.join(RUNTIME_SIDECAR_FILE)).await?;
                    compact_main::validate_full_main(&main_bytes)?;
                    let main: Session = serde_json::from_slice(&main_bytes)?;
                    let side: Session = serde_json::from_slice(&side_bytes)?;
                    if main.id != DEFAULT_SUPERVISOR_SESSION_ID
                        || !matches!(
                            main.authority_identity,
                            SessionAuthorityIdentity::Supervisor { .. }
                        )
                    {
                        return Err(conflict("no legacy default Supervisor pair"));
                    }
                    supervisor::validate_overlay(&main, Some(&side))?;
                    self.validate_root_tool_pair_for_supervisor_migration(&main, &side)
                        .await?;
                    Ok::<Session, io::Error>(side)
                }
                .await;
                match pair {
                    Ok(side) => {
                        Self::write_proof_at(&directory, &side, ProofState::Committed).await?
                    }
                    Err(error) => {
                        tracing::warn!(%error, "Supervisor proof migration skipped unavailable legacy pair")
                    }
                }
            }
            Err(error) => return Err(error),
        }
        if publish_marker {
            durable_atomic_write(&marker, b"supervisor-authority-proof-v1\n").await?;
        }
        Ok(())
    }
}
