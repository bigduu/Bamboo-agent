//! Trusted singleton identity. Canonical Session files are the only authority;
//! the global index and unpublished staging directories never grant a role.

use super::*;
use bamboo_domain::{
    SessionAuthorityConflict, SessionAuthorityIdentity, SupervisorBootstrapReceipt,
    DEFAULT_SUPERVISOR_SESSION_ID,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SupervisorBootstrapFault {
    BeforePublish,
    BeforeIndex,
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("session_authority: {message}"),
    )
}

fn writer_conflict(error: io::Error) -> io::Error {
    io::Error::new(error.kind(), SessionAuthorityConflict(error.to_string()))
}

pub(super) fn validate_identity(session: &Session) -> io::Result<()> {
    if let SessionAuthorityIdentity::Supervisor { incarnation_id } = &session.authority_identity {
        if incarnation_id.is_nil()
            || session.id != DEFAULT_SUPERVISOR_SESSION_ID
            || session.kind != SessionKind::Root
            || session.root_session_id != session.id
            || session.parent_session_id.is_some()
            || session.spawn_depth != 0
        {
            return Err(invalid("invalid Supervisor identity or Root lineage"));
        }
    }
    Ok(())
}

/// All overlay/recovery paths must reject a missing or divergent authority.
/// Ordinary legacy Sessions keep their existing compatibility semantics.
pub(super) fn validate_overlay(main: &Session, side: Option<&Session>) -> io::Result<()> {
    validate_identity(main)?;
    if let Some(side) = side {
        validate_identity(side)?;
    }
    let has_authority = !matches!(main.authority_identity, SessionAuthorityIdentity::Ordinary)
        || side.is_some_and(|side| {
            !matches!(side.authority_identity, SessionAuthorityIdentity::Ordinary)
        });
    if has_authority {
        let side =
            side.ok_or_else(|| invalid("published Supervisor is missing runtime authority"))?;
        if main.id != side.id || main.authority_identity != side.authority_identity {
            return Err(invalid(
                "runtime authority does not match the published Session",
            ));
        }
    }
    Ok(())
}

async fn real_directory(path: &Path) -> io::Result<bool> {
    match fs::symlink_metadata(path).await {
        Ok(metadata) if metadata.file_type().is_dir() => Ok(true),
        Ok(_) => Err(invalid("expected a real authority directory")),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

async fn read_regular(path: &Path) -> io::Result<Vec<u8>> {
    let metadata = fs::symlink_metadata(path).await.map_err(|error| {
        if error.kind() == io::ErrorKind::NotFound {
            invalid("published Root has missing canonical files")
        } else {
            error
        }
    })?;
    if !metadata.file_type().is_file() {
        return Err(invalid("canonical authority is not a regular file"));
    }
    fs::read(path).await
}

// Deserialize only identity fields from main. In particular, this does not
// allocate its potentially large message history or use it as a fallback.
#[derive(Deserialize)]
struct MainIdentity {
    id: String,
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
}

impl SessionStoreV2 {
    /// Caller owns the guards for its storage operation. Never consult the
    /// rebuildable index, and never recover a runtime authority from main.
    pub(super) async fn load_root_authority_unchecked(
        &self,
        id: &str,
    ) -> io::Result<Option<Session>> {
        validate_session_id(id)?;
        if !real_directory(&self.sessions_dir).await? {
            return Err(invalid("sessions directory is missing"));
        }
        let directory = self.sessions_dir.join(id);
        if !real_directory(&directory).await? {
            return Ok(None);
        }
        let main: MainIdentity =
            serde_json::from_slice(&read_regular(&directory.join("session.json")).await?)
                .map_err(|_| invalid("invalid canonical session.json"))?;
        let mut side: Session =
            serde_json::from_slice(&read_regular(&directory.join(RUNTIME_SIDECAR_FILE)).await?)
                .map_err(|_| invalid("invalid canonical runtime.json"))?;
        validate_identity(&side)?;
        let ordinary_legacy_root =
            matches!(main.authority_identity, SessionAuthorityIdentity::Ordinary)
                && main.root_session_id.is_empty();
        let main_root = if ordinary_legacy_root {
            id
        } else {
            &main.root_session_id
        };
        let side_root = if matches!(side.authority_identity, SessionAuthorityIdentity::Ordinary)
            && side.root_session_id.is_empty()
        {
            id
        } else {
            &side.root_session_id
        };
        if main.id != id
            || side.id != id
            || main.kind != SessionKind::Root
            || side.kind != SessionKind::Root
            || main_root != id
            || side_root != id
            || main.parent_session_id.is_some()
            || side.parent_session_id.is_some()
            || main.spawn_depth != 0
            || side.spawn_depth != 0
            || main.authority_identity != side.authority_identity
        {
            return Err(invalid("canonical Root identity mismatch"));
        }
        side.root_session_id = id.to_string();
        side.messages.clear();
        side.clear_stale_root_token_budget();
        Ok(Some(side))
    }

    /// The only code path allowed to assign Supervisor authority. The fixed ID
    /// is protected by the same per-session file lock as ordinary writers.
    pub(super) async fn bootstrap_default_supervisor(
        &self,
        initial_model: &str,
    ) -> io::Result<SupervisorBootstrapReceipt> {
        let _lifecycle = self.lock_session_lifecycle_shared().await?;
        let _task = self.lock_runtime_task_sidecar_shared().await?;
        let _session = self
            .acquire_session_maintenance_lock(DEFAULT_SUPERVISOR_SESSION_ID)
            .await?;
        let existing = self
            .load_root_authority_unchecked(DEFAULT_SUPERVISOR_SESSION_ID)
            .await;
        if existing.is_err() {
            // A legacy Ordinary owner is a conflict even without a sidecar.
            // This classification never repairs or grants authority, and the
            // strict authority port still rejects the incomplete file pair.
            let directory = self.sessions_dir.join(DEFAULT_SUPERVISOR_SESSION_ID);
            if matches!(fs::symlink_metadata(directory.join(RUNTIME_SIDECAR_FILE)).await,
                Err(error) if error.kind() == io::ErrorKind::NotFound)
            {
                if let Ok(bytes) = read_regular(&directory.join("session.json")).await {
                    if let Ok(main) = serde_json::from_slice::<Session>(&bytes) {
                        if main.id == DEFAULT_SUPERVISOR_SESSION_ID
                            && main.kind == SessionKind::Root
                            && main.parent_session_id.is_none()
                            && main.spawn_depth == 0
                            && (main.root_session_id.is_empty() || main.root_session_id == main.id)
                            && main.authority_identity.is_ordinary()
                        {
                            return Err(io::Error::new(
                                io::ErrorKind::AlreadyExists,
                                "default Supervisor ID is occupied by a legacy Ordinary Session",
                            ));
                        }
                    }
                }
            }
        }
        if let Some(existing) = existing? {
            let SessionAuthorityIdentity::Supervisor { incarnation_id } =
                existing.authority_identity
            else {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "default Supervisor ID is occupied by an Ordinary Session",
                ));
            };
            // Authority is already strictly verified. The rebuildable index
            // still needs the real transcript count, never the empty CP view.
            let full = self
                .load_authoritative_root_session(DEFAULT_SUPERVISOR_SESSION_ID)
                .await?
                .ok_or_else(|| invalid("published Supervisor disappeared"))?;
            self.repair_index_from_authoritative_session(
                &full,
                Self::root_rel_path(DEFAULT_SUPERVISOR_SESSION_ID),
            )
            .await?;
            return Ok(SupervisorBootstrapReceipt {
                session_id: existing.id,
                incarnation_id,
                created: false,
            });
        }
        if initial_model.trim().is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "a model is required to bootstrap the default Supervisor",
            ));
        }
        let incarnation_id = Uuid::new_v4();
        let mut session = Session::new(DEFAULT_SUPERVISOR_SESSION_ID, initial_model.trim());
        session.title = "Supervisor".to_string();
        session.title_generated = true;
        session.authority_identity = SessionAuthorityIdentity::Supervisor { incarnation_id };
        let staging = self
            .bamboo_home_dir
            .join(format!(".supervisor-bootstrap-{}", Uuid::new_v4()));
        let destination = self.sessions_dir.join(DEFAULT_SUPERVISOR_SESSION_ID);
        fs::create_dir(&staging).await?;
        let result = async {
            fs::create_dir(staging.join("children")).await?;
            fs::create_dir(staging.join("attachments")).await?;
            let bytes = serde_json::to_vec_pretty(&session)
                .map_err(|error| other_io_error(error.to_string()))?;
            durable_atomic_write(&staging.join("session.json"), &bytes).await?;
            durable_atomic_write(&staging.join(RUNTIME_SIDECAR_FILE), &bytes).await?;
            sync_directory(&staging).await?;
            self.maybe_fail_supervisor_bootstrap(SupervisorBootstrapFault::BeforePublish)?;
            atomic_rename(&staging, &destination).await?;
            sync_parent_directory_entry(&staging).await?;
            sync_parent_directory_entry(&destination).await?;
            self.maybe_fail_supervisor_bootstrap(SupervisorBootstrapFault::BeforeIndex)?;
            self.repair_index_from_authoritative_session(
                &session,
                Self::root_rel_path(DEFAULT_SUPERVISOR_SESSION_ID),
            )
            .await?;
            Ok(SupervisorBootstrapReceipt {
                session_id: session.id.clone(),
                incarnation_id,
                created: true,
            })
        }
        .await;
        if result.is_err() {
            // Only this call's unpublished directory is removable. A complete
            // published Root survives index failures and is repaired on retry.
            let _ = fs::remove_dir_all(&staging).await;
        }
        result
    }

    fn maybe_fail_supervisor_bootstrap(&self, fault: SupervisorBootstrapFault) -> io::Result<()> {
        #[cfg(test)]
        {
            let mut pending = self
                .supervisor_bootstrap_fault
                .lock()
                .expect("supervisor fault lock");
            if pending.as_ref() == Some(&fault) {
                *pending = None;
                return Err(other_io_error(format!(
                    "injected Supervisor bootstrap failure: {fault:?}"
                )));
            }
        }
        #[cfg(not(test))]
        let _ = fault;
        Ok(())
    }

    /// Called inside the final cross-process write lock. Merging callers must
    /// adopt durable authority in their own snapshot before entering this writer;
    /// silently repairing only the serialized copy would leave their cache stale.
    pub(super) async fn validate_authority_for_save(&self, incoming: &Session) -> io::Result<()> {
        validate_identity(incoming).map_err(writer_conflict)?;
        if incoming.id != DEFAULT_SUPERVISOR_SESSION_ID {
            return Ok(());
        }
        let current = self
            .load_root_authority_unchecked(&incoming.id)
            .await
            .map_err(writer_conflict)?;
        match current {
            Some(current) if current.authority_identity == incoming.authority_identity => Ok(()),
            Some(_) => Err(writer_conflict(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "writer identity does not match the durable Session; reload before saving",
            ))),
            None if matches!(
                incoming.authority_identity,
                SessionAuthorityIdentity::Ordinary
            ) =>
            {
                Ok(())
            }
            None => Err(writer_conflict(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "only trusted bootstrap can create Supervisor authority",
            ))),
        }
    }
}
