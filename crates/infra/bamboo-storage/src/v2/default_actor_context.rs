//! Default writers observe Actor authority; they never initialize or repair it.
//! This fence does not cover the independent Task/management/startup writers.

use super::*;
use bamboo_domain::{ActorDirectoryEntry, ActorLogicalState, SessionAuthorityConflict};

// Field order releases Session, Task, lifecycle, reversing acquisition order.
pub(super) struct DefaultWriterGuards {
    // Keep this before Session/Task/lifecycle so it drops first. A cancelled
    // async caller cannot release the Root lock while a spawned write remains.
    tree: std::sync::Mutex<Option<ActorTreeWriteGuard>>,
    _session: SessionWriteGuard,
    _task: TaskGuard,
    _lifecycle: LifecycleGuard,
}

enum TaskGuard {
    Shared {
        _guard: RuntimeTaskTransactionReadGuard,
    },
    Exclusive {
        _guard: RuntimeTaskTransactionWriteGuard,
    },
}
enum LifecycleGuard {
    Shared { _guard: SessionLifecycleReadGuard },
    Exclusive { _guard: SessionLifecycleWriteGuard },
}

impl DefaultWriterGuards {
    pub(super) fn shared(
        lifecycle: SessionLifecycleReadGuard,
        task: RuntimeTaskTransactionReadGuard,
        session: SessionWriteGuard,
    ) -> Arc<Self> {
        Arc::new(Self {
            tree: std::sync::Mutex::new(None),
            _session: session,
            _task: TaskGuard::Shared { _guard: task },
            _lifecycle: LifecycleGuard::Shared { _guard: lifecycle },
        })
    }

    pub(super) fn exclusive(
        lifecycle: SessionLifecycleWriteGuard,
        task: RuntimeTaskTransactionWriteGuard,
        session: SessionWriteGuard,
    ) -> Arc<Self> {
        Arc::new(Self {
            tree: std::sync::Mutex::new(None),
            _session: session,
            _task: TaskGuard::Exclusive { _guard: task },
            _lifecycle: LifecycleGuard::Exclusive { _guard: lifecycle },
        })
    }

    pub(super) fn hold_tree(&self, tree: ActorTreeWriteGuard) {
        *self.tree.lock().expect("Actor tree writer guard") = Some(tree);
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InitializedMarker {
    schema_version: u32,
    actor_id: String,
    session_created_at: DateTime<Utc>,
}

// Invalid includes nonregular files, read errors and malformed bytes. Two
// absent files alone retain the legacy boundary; arbitrary external deletion
// of both files is indistinguishable from legacy with the existing schema.
enum ObservedFile {
    Absent,
    Bytes(Vec<u8>),
    Invalid,
}

fn permits_unfenced_context(
    record: &ObservedFile,
    marker: &ObservedFile,
    durable: Option<&Session>,
) -> bool {
    if matches!(
        (record, marker),
        (ObservedFile::Absent, ObservedFile::Absent)
    ) {
        return true;
    }
    let (ObservedFile::Bytes(record), ObservedFile::Bytes(marker), Some(durable)) =
        (record, marker, durable)
    else {
        return false;
    };
    let Ok(record) = serde_json::from_slice::<ActorDirectoryEntry>(record) else {
        return false;
    };
    let Ok(marker) = serde_json::from_slice::<InitializedMarker>(marker) else {
        return false;
    };
    record.validate().is_ok()
        && record.actor.matches_session(durable)
        && marker.schema_version == bamboo_domain::ACTOR_DIRECTORY_SCHEMA_VERSION
        && marker.actor_id == durable.id
        && marker.session_created_at == durable.created_at
        && record.actor.current_attempt == 0
        && record.actor.state == ActorLogicalState::Cold
        && record.activation.is_none()
}

fn context_conflict() -> io::Error {
    io::Error::new(
        io::ErrorKind::Unsupported,
        SessionAuthorityConflict(
            "Unfenced Actor context mutation is unsupported after activation or when authority is unknown".into(),
        ),
    )
}

async fn observe_file(path: &Path) -> ObservedFile {
    match fs::symlink_metadata(path).await {
        Ok(meta) if meta.file_type().is_file() => match fs::read(path).await {
            Ok(bytes) => ObservedFile::Bytes(bytes),
            Err(_) => ObservedFile::Invalid,
        },
        Err(error) if error.kind() == io::ErrorKind::NotFound => ObservedFile::Absent,
        _ => ObservedFile::Invalid,
    }
}

fn parse_session(file: &ObservedFile, main: bool) -> Option<Session> {
    match file {
        ObservedFile::Bytes(bytes) => {
            if main {
                compact_main::validate_full_main(bytes).ok()?;
            }
            let mut session: Session = serde_json::from_slice(bytes).ok()?;
            // Same canonical legacy Root spelling accepted by strict V2 reads.
            if session.kind == SessionKind::Root && session.root_session_id.is_empty() {
                session.root_session_id = session.id.clone();
            }
            Some(session)
        }
        _ => None,
    }
}

fn runtime_context(session: &Session) -> io::Result<serde_json::Value> {
    serde_json::to_value((
        &session.conversation_summary,
        &session.compression_events,
        &session.model_context_state,
    ))
    .map_err(|error| other_io_error(error.to_string()))
}

fn main_context(session: &Session) -> io::Result<serde_json::Value> {
    serde_json::to_value((
        &session.messages,
        &session.provider_transcript,
        session
            .runtime_metadata
            .as_ref()
            .and_then(|meta| meta.session_inbox_admission.as_ref()),
    ))
    .map_err(|error| other_io_error(error.to_string()))
}

impl SessionStoreV2 {
    pub(super) async fn lock_default_writer_lifecycle(
        &self,
    ) -> io::Result<SessionLifecycleReadGuard> {
        match self.lock_session_lifecycle_shared().await {
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                // Preserve the existing Task-lock first-use recovery when an
                // embedding removes an idle home. This initializes the lock
                // parent only; no Session/Actor context is published here.
                let home = self.bamboo_home_dir.clone();
                tokio::task::spawn_blocking(move || std::fs::create_dir_all(home))
                    .await
                    .map_err(|error| {
                        other_io_error(format!("join default lock-parent creation: {error}"))
                    })??;
                self.lock_session_lifecycle_shared().await
            }
            result => result,
        }
    }

    // This deterministic placement helper does no directory or authority work.
    pub(super) fn default_writer_rel_path(session: &Session) -> io::Result<String> {
        validate_session_id(&session.id)?;
        match session.kind {
            SessionKind::Root => Ok(Self::root_rel_path(&session.id)),
            SessionKind::Child => {
                let root = session.root_session_id.trim();
                let parent = session.parent_session_id.as_deref().unwrap_or("").trim();
                if root.is_empty() || parent.is_empty() {
                    return Err(other_io_error(
                        "child session missing root_session_id/parent_session_id",
                    ));
                }
                validate_session_id(root)?;
                Ok(Self::child_rel_path(root, &session.id))
            }
        }
    }

    async fn default_actor_observation(&self, directory: &Path) -> (ObservedFile, ObservedFile) {
        // Read-only: do not call ActorDirectoryPort or a load/repair path here.
        let record = observe_file(&directory.join("actor-authority.json")).await;
        let marker = observe_file(&directory.join("actor-authority.initialized.json")).await;
        (record, marker)
    }

    pub(super) async fn check_default_actor_context(
        &self,
        incoming: &Session,
        directory: &Path,
        full: bool,
    ) -> io::Result<()> {
        let (record, marker) = self.default_actor_observation(directory).await;
        if permits_unfenced_context(&record, &marker, None) {
            return Ok(());
        }
        // Main is the durable birth and history authority; runtime is the
        // actual published summary/compression/model context. No fallback to
        // embedded main context may authorize a protected reconstruction.
        let main = parse_session(&observe_file(&directory.join("session.json")).await, true);
        if permits_unfenced_context(&record, &marker, main.as_ref()) {
            return Ok(());
        }
        let side = parse_session(
            &observe_file(&directory.join(RUNTIME_SIDECAR_FILE)).await,
            false,
        );
        let (Some(main), Some(side)) = (main, side) else {
            return Err(context_conflict());
        };
        // Incoming birth/Project/tool authority still pass the original final
        // guards below this classifier. Here compare the actual canonical pair.
        if main.id != incoming.id
            || side.id != main.id
            || side.created_at != main.created_at
            || side.kind != main.kind
            || side.root_session_id != main.root_session_id
            || side.parent_session_id != main.parent_session_id
            || side.spawn_depth != main.spawn_depth
            || side.authority_identity != main.authority_identity
            || runtime_context(incoming)? != runtime_context(&side)?
            || (full && main_context(incoming)? != main_context(&main)?)
        {
            return Err(context_conflict());
        }
        Ok(())
    }

    // Missing runtime has no exact-context shortcut: only legacy or proven
    // inert Cold authority can admit reconstruction from the actual main.
    pub(super) async fn check_actor_reconstruction(
        &self,
        main: &Session,
        directory: &Path,
    ) -> io::Result<()> {
        let (record, marker) = self.default_actor_observation(directory).await;
        if permits_unfenced_context(&record, &marker, Some(main)) {
            Ok(())
        } else {
            Err(context_conflict())
        }
    }

    pub(super) async fn check_default_actor_clear(&self, directory: &Path) -> io::Result<()> {
        let (record, marker) = self.default_actor_observation(directory).await;
        if permits_unfenced_context(&record, &marker, None) {
            return Ok(());
        }
        let main = parse_session(&observe_file(&directory.join("session.json")).await, true);
        if permits_unfenced_context(&record, &marker, main.as_ref()) {
            Ok(())
        } else {
            Err(context_conflict())
        }
    }

    pub(super) async fn default_writer_job<T: Send + 'static>(
        guards: &Arc<DefaultWriterGuards>,
        job: impl FnOnce() -> io::Result<T> + Send + 'static,
    ) -> io::Result<T> {
        let guards = Arc::clone(guards);
        tokio::task::spawn_blocking(move || {
            let _guards = guards;
            job()
        })
        .await
        .map_err(|error| other_io_error(format!("join default Session filesystem job: {error}")))?
    }

    pub(super) async fn write_default_bytes(
        &self,
        path: &Path,
        bytes: Vec<u8>,
        guards: &Arc<DefaultWriterGuards>,
    ) -> io::Result<()> {
        let path = path.to_path_buf();
        #[cfg(test)]
        let hook = self.default_write_hook.lock().unwrap().clone();
        Self::default_writer_job(guards, move || {
            durable_atomic_write_blocking(&path, &bytes, |phase| {
                #[cfg(test)]
                if let Some(hook) = &hook {
                    return hook.visit(&path, phase);
                }
                let _ = phase;
                Ok(())
            })
        })
        .await
    }

    pub(super) async fn ensure_default_writer_dirs(
        &self,
        session: &Session,
        guards: &Arc<DefaultWriterGuards>,
    ) -> io::Result<String> {
        let rel = Self::default_writer_rel_path(session)?;
        let directory = self.abs_path_from_rel(&rel);
        let root = session.kind == SessionKind::Root;
        Self::default_writer_job(guards, move || {
            std::fs::create_dir_all(directory.join("attachments"))?;
            if root {
                std::fs::create_dir_all(directory.join("children"))?;
            }
            Ok(())
        })
        .await?;
        Ok(rel)
    }

    pub(super) async fn publish_default_search_revision(
        &self,
        directory: &Path,
        guards: &Arc<DefaultWriterGuards>,
    ) -> io::Result<(PathBuf, String)> {
        let path = directory.join(SEARCH_INDEX_REVISION_FILE);
        let revision = Uuid::new_v4().to_string();
        self.write_default_bytes(&path, revision.as_bytes().to_vec(), guards)
            .await?;
        Ok((path, revision))
    }

    pub(super) async fn clear_default_session_files(
        &self,
        directory: &Path,
        session: &Session,
        guards: &Arc<DefaultWriterGuards>,
    ) -> io::Result<(PathBuf, String)> {
        let directory = directory.to_path_buf();
        let runtime = serde_json::to_vec_pretty(&runtime_sidecar_snapshot(session))
            .map_err(|error| other_io_error(error.to_string()))?;
        let main = compact_main::serialize_main(session)?;
        let revision_path = directory.join(SEARCH_INDEX_REVISION_FILE);
        let revision = Uuid::new_v4().to_string();
        let published_revision = revision.clone();
        let published_path = revision_path.clone();
        #[cfg(test)]
        let hook = self.default_write_hook.lock().unwrap().clone();
        Self::default_writer_job(guards, move || {
            let attachments = directory.join("attachments");
            #[cfg(test)]
            if let Some(hook) = &hook {
                hook.visit(&attachments, DurableWritePhase::BeforeReplace)?;
            }
            match std::fs::remove_dir_all(&attachments) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
            #[cfg(unix)]
            std::fs::File::open(&directory)?.sync_all()?;
            std::fs::create_dir_all(&attachments)?;
            #[cfg(unix)]
            std::fs::File::open(&directory)?.sync_all()?;
            for (path, bytes) in [
                (directory.join(RUNTIME_SIDECAR_FILE), runtime),
                (directory.join("session.json"), main),
                (published_path, published_revision.into_bytes()),
            ] {
                durable_atomic_write_blocking(&path, &bytes, |phase| {
                    #[cfg(test)]
                    if let Some(hook) = &hook {
                        return hook.visit(&path, phase);
                    }
                    let _ = phase;
                    Ok(())
                })?;
            }
            Ok(())
        })
        .await?;
        Ok((revision_path, revision))
    }
}
