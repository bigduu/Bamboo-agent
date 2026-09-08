//! Canonical evidence for deleted Root lifetimes. This records revocation only;
//! active Sessions and Supervisor grants still have no separate registry.

use super::*;
use bamboo_domain::SessionAuthorityConflict;

const ROOT_REVOCATIONS_DIR: &str = ".root-revocations";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RootPublicationFault {
    BeforePublish,
    BeforeIndex,
}

#[derive(Serialize, Deserialize)]
struct RootRevocation {
    version: u32,
    session_id: String,
    revoked_through: DateTime<Utc>,
}

#[derive(Deserialize)]
struct RootBirth {
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
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

fn conflict(error: impl std::fmt::Display) -> io::Error {
    io::Error::new(
        io::ErrorKind::WouldBlock,
        SessionAuthorityConflict(format!("Root lifetime is revoked or unavailable: {error}")),
    )
}

async fn real_directory(path: &Path) -> io::Result<bool> {
    match fs::symlink_metadata(path).await {
        Ok(metadata) if metadata.file_type().is_dir() => Ok(true),
        Ok(_) => Err(invalid("Root lifetime directory is not a real directory")),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

async fn regular_file_exists(path: &Path) -> io::Result<bool> {
    match fs::symlink_metadata(path).await {
        Ok(metadata) if metadata.file_type().is_file() => Ok(true),
        Ok(_) => Err(invalid("Root lifetime evidence is not a regular file")),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

async fn read_regular(path: &Path) -> io::Result<Option<Vec<u8>>> {
    if regular_file_exists(path).await? {
        fs::read(path).await.map(Some)
    } else {
        Ok(None)
    }
}

impl SessionStoreV2 {
    pub(super) fn maybe_fail_root_publication(
        &self,
        fault: RootPublicationFault,
    ) -> io::Result<()> {
        #[cfg(test)]
        {
            let mut pending = self
                .root_publication_fault
                .lock()
                .expect("Root publication fault lock");
            if pending.as_ref() == Some(&fault) {
                *pending = None;
                return Err(other_io_error(format!(
                    "injected Root publication failure: {fault:?}"
                )));
            }
        }
        #[cfg(not(test))]
        let _ = fault;
        Ok(())
    }

    fn root_revocation_path(&self, session_id: &str) -> PathBuf {
        self.bamboo_home_dir
            .join(ROOT_REVOCATIONS_DIR)
            .join(format!("{session_id}.json"))
    }

    pub(super) async fn root_revocation(
        &self,
        session_id: &str,
    ) -> io::Result<Option<DateTime<Utc>>> {
        validate_session_id(session_id)?;
        let directory = self.bamboo_home_dir.join(ROOT_REVOCATIONS_DIR);
        if !real_directory(&directory).await? {
            return Ok(None);
        }
        let Some(bytes) = read_regular(&self.root_revocation_path(session_id)).await? else {
            return Ok(None);
        };
        let evidence: RootRevocation = serde_json::from_slice(&bytes)
            .map_err(|error| invalid(format!("invalid Root revocation: {error}")))?;
        if evidence.version != 1 || evidence.session_id != session_id {
            return Err(invalid("Root revocation identity or version mismatch"));
        }
        Ok(Some(evidence.revoked_through))
    }

    /// Read birth markers directly from canonical files, never the index. Both
    /// markers are revoked if an interrupted/corrupt pair disagrees. Unreadable
    /// or unparseable files fail closed before deletion changes anything.
    pub(super) async fn canonical_root_birth(
        &self,
        session_id: &str,
    ) -> io::Result<Option<DateTime<Utc>>> {
        validate_session_id(session_id)?;
        let directory = self.sessions_dir.join(session_id);
        if !real_directory(&directory).await? {
            return Ok(None);
        }
        let mut birth = None;
        for name in ["session.json", RUNTIME_SIDECAR_FILE] {
            let Some(bytes) = read_regular(&directory.join(name)).await? else {
                continue;
            };
            let identity: RootBirth = serde_json::from_slice(&bytes)
                .map_err(|error| invalid(format!("invalid Root deletion identity: {error}")))?;
            if identity.id != session_id
                || identity.kind != SessionKind::Root
                || (!identity.root_session_id.is_empty() && identity.root_session_id != session_id)
                || identity.parent_session_id.is_some()
                || identity.spawn_depth != 0
            {
                return Err(invalid(
                    "Root deletion identity does not match canonical placement",
                ));
            }
            birth = Some(
                birth.map_or(identity.created_at, |previous: DateTime<Utc>| {
                    previous.max(identity.created_at)
                }),
            );
        }
        Ok(birth)
    }

    /// Caller owns lifecycle + Task exclusivity. Publication is the logical
    /// deletion point: a crash before file/index removal cannot restore access.
    pub(super) async fn revoke_root_lifetime(&self, session_id: &str) -> io::Result<bool> {
        let previous = self.root_revocation(session_id).await?;
        let birth = self.canonical_root_birth(session_id).await?;
        let Some(birth) = birth else {
            return Ok(previous.is_some());
        };
        let revoked_through = previous.map_or(birth, |previous| previous.max(birth));
        let directory = self.bamboo_home_dir.join(ROOT_REVOCATIONS_DIR);
        if !real_directory(&directory).await? {
            fs::create_dir(&directory).await?;
            sync_parent_directory_entry(&directory).await?;
        }
        let bytes = serde_json::to_vec(&RootRevocation {
            version: 1,
            session_id: session_id.to_string(),
            revoked_through,
        })
        .map_err(|error| invalid(error.to_string()))?;
        durable_atomic_write(&self.root_revocation_path(session_id), &bytes).await?;
        Ok(true)
    }

    pub(super) async fn root_birth_is_live(
        &self,
        session_id: &str,
        created_at: DateTime<Utc>,
    ) -> io::Result<bool> {
        Ok(self
            .root_revocation(session_id)
            .await?
            .is_none_or(|cutoff| created_at > cutoff))
    }

    pub(super) async fn root_directory_is_revoked(&self, session_id: &str) -> io::Result<bool> {
        let Some(cutoff) = self.root_revocation(session_id).await? else {
            return Ok(false);
        };
        Ok(self
            .canonical_root_birth(session_id)
            .await?
            .is_none_or(|birth| birth <= cutoff))
    }

    /// Repair only the derived index after a crash between revocation and its
    /// removal. The caller already owns startup lifecycle/Task exclusivity.
    pub(super) async fn reconcile_root_revocations(&self) -> io::Result<()> {
        let directory = self.bamboo_home_dir.join(ROOT_REVOCATIONS_DIR);
        if !real_directory(&directory).await? {
            return Ok(());
        }
        let mut entries = fs::read_dir(&directory).await?;
        let mut revoked = HashSet::new();
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.extension().and_then(|value| value.to_str()) != Some("json") {
                continue; // Unpublished atomic-write temporary files are inert.
            }
            let id = path
                .file_stem()
                .and_then(|value| value.to_str())
                .ok_or_else(|| invalid("invalid Root revocation filename"))?;
            match self.root_directory_is_revoked(id).await {
                Ok(true) => {
                    revoked.insert(id.to_string());
                }
                Ok(false) => {}
                Err(error) => {
                    // A damaged Root must remain unavailable without making
                    // unrelated canonical Sessions unavailable at startup.
                    // Keep its evidence/files intact for strict refusal and
                    // targeted repair; only its derived rows are removable.
                    tracing::warn!(
                        "Root revocation reconciliation: unavailable Root {id}: {error}"
                    );
                    revoked.insert(id.to_string());
                }
            }
        }
        if !revoked.is_empty() {
            self.update_index(|index| {
                index.sessions.retain(|id, entry| {
                    !revoked.contains(id) && !revoked.contains(&entry.root_session_id)
                });
                Ok(())
            })
            .await?;
        }
        Ok(())
    }

    /// All historical/rebuild/recovery readers suppress a revoked Root and its
    /// remaining children after interrupted removal. A Child cannot occupy a
    /// former Root ID, even when the former Root directory is absent.
    pub(super) async fn session_lifetime_is_live(&self, session: &Session) -> io::Result<bool> {
        if session.kind == SessionKind::Root {
            return self
                .root_birth_is_live(&session.id, session.created_at)
                .await;
        }
        if self.root_revocation(&session.id).await?.is_some() {
            return Ok(false);
        }
        let root_id = &session.root_session_id;
        let Some(cutoff) = self.root_revocation(root_id).await? else {
            return Ok(true);
        };
        // This only fences a Child against a deleted parent. It grants no
        // Root/Supervisor authority, so never read the parent's potentially
        // large transcript here. Strict Root authorization still validates
        // the complete canonical main/runtime pair through its separate port.
        let directory = self.sessions_dir.join(root_id);
        if !real_directory(&self.sessions_dir).await?
            || !real_directory(&directory).await?
            || !regular_file_exists(&directory.join("session.json")).await?
        {
            return Ok(false);
        }
        let Some(bytes) = read_regular(&directory.join(RUNTIME_SIDECAR_FILE)).await? else {
            return Ok(false);
        };
        let parent: Session = serde_json::from_slice(&bytes)
            .map_err(|error| invalid(format!("invalid parent Root runtime: {error}")))?;
        supervisor::validate_identity(&parent)?;
        if parent.id != *root_id
            || parent.kind != SessionKind::Root
            || (!parent.root_session_id.is_empty() && parent.root_session_id != *root_id)
            || parent.parent_session_id.is_some()
            || parent.spawn_depth != 0
        {
            return Err(invalid(
                "parent Root runtime identity does not match canonical placement",
            ));
        }
        Ok(parent.created_at > cutoff)
    }

    /// Called under the final ordinary writer or exclusive lifecycle boundary.
    /// An absent/empty directory is creation only for a never-deleted ID. Even
    /// a newly constructed ordinary snapshot must use explicit recreation once
    /// deletion provenance exists.
    pub(super) async fn validate_root_lifetime_for_write(
        &self,
        incoming: &Session,
    ) -> io::Result<()> {
        let evidence = self.root_revocation(&incoming.id).await.map_err(conflict)?;
        if let Some(cutoff) = evidence {
            if incoming.kind != SessionKind::Root || incoming.created_at <= cutoff {
                return Err(conflict("writer belongs to a deleted Root lifetime"));
            }
            let directory = self.sessions_dir.join(&incoming.id);
            if !regular_file_exists(&directory.join("session.json"))
                .await
                .map_err(conflict)?
                && !regular_file_exists(&directory.join(RUNTIME_SIDECAR_FILE))
                    .await
                    .map_err(conflict)?
            {
                return Err(conflict(
                    "a deleted Root ID requires trusted explicit recreation",
                ));
            }
        }
        if incoming.kind == SessionKind::Child
            && !self
                .session_lifetime_is_live(incoming)
                .await
                .map_err(conflict)?
        {
            return Err(conflict("child belongs to a deleted Root"));
        }
        Ok(())
    }

    /// Generate the new birth at the trusted host boundary. Clock rollback and
    /// a future old marker cannot reuse any revoked lifetime; overflow is an
    /// error before staging/publication.
    pub(super) async fn fresh_root_birth(&self, session_id: &str) -> io::Result<DateTime<Utc>> {
        let now = Utc::now();
        match self.root_revocation(session_id).await? {
            Some(cutoff) if now <= cutoff => cutoff
                .checked_add_signed(chrono::Duration::nanoseconds(1))
                .ok_or_else(|| invalid("Root lifetime birth marker exhausted")),
            _ => Ok(now),
        }
    }

    /// A revoked directory may survive a crash after the revocation commit.
    /// Remove only that old publication while holding lifecycle/Task exclusivity.
    pub(super) async fn remove_revoked_root_directory(&self, session_id: &str) -> io::Result<()> {
        let Some(cutoff) = self.root_revocation(session_id).await? else {
            return Ok(());
        };
        if self
            .canonical_root_birth(session_id)
            .await?
            .is_some_and(|birth| birth > cutoff)
        {
            return Err(invalid("cannot remove a live Root during recreation"));
        }
        let directory = self.sessions_dir.join(session_id);
        match fs::remove_dir_all(&directory).await {
            Ok(()) => sync_parent_directory_entry(&directory).await,
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
        }
    }

    pub(super) async fn recreate_ordinary_root(
        &self,
        session_id: &str,
        initial_model: &str,
    ) -> io::Result<Session> {
        validate_session_id(session_id)?;
        if session_id == DEFAULT_SUPERVISOR_SESSION_ID || initial_model.trim().is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "ordinary Root recreation requires a non-reserved ID and a model",
            ));
        }
        let _lifecycle = self.lock_session_lifecycle_exclusive().await?;
        let _task = self.lock_runtime_task_transaction_exclusive().await?;
        self.recover_all_runtime_task_transactions_locked().await?;
        self.recover_all_session_copy_transactions_locked().await?;
        if self.root_revocation(session_id).await?.is_none() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Root ID has not been deleted; use ordinary first creation",
            ));
        }
        if let Some(existing) = self.load_root_authority_unchecked(session_id).await? {
            let full = self
                .load_authoritative_root_session(session_id)
                .await?
                .ok_or_else(|| invalid("recreated Root disappeared"))?;
            self.repair_index_from_authoritative_session(&full, Self::root_rel_path(session_id))
                .await?;
            debug_assert_eq!(existing.created_at, full.created_at);
            return Ok(full);
        }
        self.remove_revoked_root_directory(session_id).await?;
        let mut session = Session::new(session_id, initial_model.trim());
        session.created_at = self.fresh_root_birth(session_id).await?;
        session.updated_at = session.created_at;
        let staging = self
            .bamboo_home_dir
            .join(format!(".root-recreation-{}", Uuid::new_v4()));
        let destination = self.sessions_dir.join(session_id);
        fs::create_dir(&staging).await?;
        let result = async {
            fs::create_dir(staging.join("children")).await?;
            fs::create_dir(staging.join("attachments")).await?;
            let bytes =
                serde_json::to_vec_pretty(&session).map_err(|error| invalid(error.to_string()))?;
            durable_atomic_write(&staging.join("session.json"), &bytes).await?;
            durable_atomic_write(&staging.join(RUNTIME_SIDECAR_FILE), &bytes).await?;
            sync_directory(&staging).await?;
            self.maybe_fail_root_publication(RootPublicationFault::BeforePublish)?;
            atomic_rename(&staging, &destination).await?;
            sync_parent_directory_entry(&staging).await?;
            sync_parent_directory_entry(&destination).await?;
            self.maybe_fail_root_publication(RootPublicationFault::BeforeIndex)?;
            self.repair_index_from_authoritative_session(&session, Self::root_rel_path(session_id))
                .await?;
            Ok(session.clone())
        }
        .await;
        if result.is_err() {
            let _ = fs::remove_dir_all(&staging).await;
        }
        result
    }
}
