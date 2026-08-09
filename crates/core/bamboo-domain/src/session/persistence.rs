use std::io;
use std::sync::Arc;

use crate::session::task::TaskList;
use crate::session::types::Session;
use crate::session::PermissionAuditSeed;

/// Merge messages from a live runner snapshot into an already-durable
/// transcript without ever removing or rewriting a durable message.
///
/// Runtime sessions are append-oriented, and every newly-created message has a
/// stable id.  A runner may nevertheless be holding a snapshot that predates a
/// concurrent append (for example, an injected child-completion message).  A
/// terminal/error checkpoint must not full-save that stale snapshot: doing so
/// would shrink the transcript.  Keep the durable ordering and append only the
/// live messages whose ids are not durable yet.
pub fn append_missing_runtime_messages(session: &mut Session, durable: &Session) -> usize {
    let mut seen = durable
        .messages
        .iter()
        .map(|message| message.id.clone())
        .collect::<std::collections::HashSet<_>>();
    let missing = session
        .messages
        .iter()
        .filter(|message| seen.insert(message.id.clone()))
        .cloned()
        .collect::<Vec<_>>();
    let appended = missing.len();
    session.messages = durable.messages.iter().cloned().chain(missing).collect();
    appended
}

/// Merge the durable SessionInbox admitted-id cursor into a writer snapshot.
///
/// Runtime writers can hold a session clone from before another run admitted
/// an inbox message. No later full save may erase that durable dedupe state.
pub fn merge_session_inbox_admission(session: &mut Session, durable: &Session) {
    let Some(durable_state) = durable.session_inbox_admission().cloned() else {
        return;
    };
    session
        .session_inbox_admission_mut()
        .merge_from(&durable_state);
}

/// Restore durable provider messages identified by their typed
/// `metadata.session_message` marker into a stale writer without preserving
/// unrelated durable suffixes. The bounded cursor is only a fast recent index;
/// the transcript marker is the unbounded source of truth after cursor
/// eviction.
///
/// Insertion follows durable transcript neighbors so an admitted user/runtime
/// message remains ahead of any later assistant output held by the stale
/// runner. This is narrower than [`append_missing_runtime_messages`], retaining
/// the historical shrink semantics for unrelated concurrent messages while
/// making a cursor/tombstone incapable of outliving its transcript entry.
pub fn restore_missing_admitted_inbox_messages(session: &mut Session, durable: &Session) -> usize {
    let admission = durable.session_inbox_admission();
    let mut restored = 0;
    for (durable_index, message) in durable.messages.iter().enumerate() {
        let typed_marker = message
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.get("session_message"))
            .is_some_and(|marker| {
                marker.get("id").and_then(serde_json::Value::as_str) == Some(message.id.as_str())
                    && marker
                        .get("target_session_id")
                        .and_then(serde_json::Value::as_str)
                        == Some(durable.id.as_str())
                    && crate::SessionMessageId::parse(message.id.clone()).is_ok()
            });
        let recent_cursor = admission.is_some_and(|state| state.contains_str(&message.id));
        if !(typed_marker || recent_cursor)
            || session
                .messages
                .iter()
                .any(|current| current.id == message.id)
        {
            continue;
        }

        let insertion = durable.messages[..durable_index]
            .iter()
            .rev()
            .find_map(|predecessor| {
                session
                    .messages
                    .iter()
                    .position(|current| current.id == predecessor.id)
                    .map(|index| index + 1)
            })
            .or_else(|| {
                durable.messages[durable_index + 1..]
                    .iter()
                    .find_map(|successor| {
                        session
                            .messages
                            .iter()
                            .position(|current| current.id == successor.id)
                    })
            })
            .unwrap_or(session.messages.len());
        session.messages.insert(insertion, message.clone());
        restored += 1;
    }
    restored
}

/// Port for runtime (non-authoritative) session persistence.
///
/// Implementors must:
/// - Serialize concurrent saves per session ID.
/// - Merge on-disk authoritative metadata (`title`, `title_generated`, `pinned`, `title_version`,
///   `metadata_version`) before writing, so UI edits are never clobbered.
#[async_trait::async_trait]
pub trait RuntimeSessionPersistence: Send + Sync {
    /// Persist the session, merging any newer authoritative metadata from disk.
    async fn save_runtime_session(&self, session: &mut Session) -> io::Result<()>;

    /// Authoritatively seed one validated actor activation.
    ///
    /// Unlike an ordinary runtime save, the incoming RunSpec posture and its
    /// complete audit record must replace any posture left by a previous warm
    /// activation. Implementations must still preserve durable SessionInbox
    /// admission/transcript proof and serialize the operation per session.
    ///
    /// There is no safe generic implementation through
    /// [`Self::save_runtime_session`]: that primitive is explicitly allowed to
    /// adopt a newer disk posture, which would make warm workers sticky across
    /// runs. Custom persisters therefore fail closed until they implement this
    /// authority boundary deliberately.
    async fn seed_runtime_activation(&self, _session: &mut Session) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "runtime persistence does not support authoritative activation seeding",
        ))
    }

    /// Atomically persist a worker-declared executor mapping for the current
    /// host-authoritative permission posture.
    ///
    /// The caller supplies the audit revision it observed before dispatch.
    /// Implementations must load and compare that revision while holding the
    /// per-session lock, reject a concurrent posture update, and allocate a new
    /// host revision/timestamp themselves. Remote audit clocks are never an
    /// authority at this boundary.
    async fn record_permission_posture_activation(
        &self,
        _session_id: &str,
        _expected_audit_revision: Option<u64>,
        _seed: &PermissionAuditSeed,
    ) -> io::Result<Option<Session>> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "runtime persistence does not support atomic permission posture activation",
        ))
    }

    /// Persist only the runtime control-plane for a session.
    ///
    /// Task lists and other runtime metadata belong to the control-plane and do
    /// not require rewriting the potentially large message transcript. Built-in
    /// persistence implementations with a runtime sidecar should override this
    /// operation with their sidecar-only path. Custom/legacy implementations
    /// remain source-compatible and safely fall back to the full runtime save.
    ///
    /// Callers must not rely on this operation to persist message or
    /// `model_context_state` changes. The durable ledger is checkpoint-owned;
    /// sidecar implementations must preserve its latest committed value while
    /// applying the caller's narrow control-plane mutation.
    async fn save_runtime_control_plane(&self, session: &mut Session) -> io::Result<()> {
        self.save_runtime_session(session).await
    }

    /// Load the representation paired with
    /// [`save_runtime_control_plane`](Self::save_runtime_control_plane).
    ///
    /// Sidecar-capable implementations should return their message-free
    /// control-plane snapshot. The default deliberately returns the full
    /// runtime session: when the paired save also falls back to a full save,
    /// retaining the transcript makes that fallback safe rather than replacing
    /// durable messages with an empty sidecar-shaped snapshot.
    async fn load_runtime_control_plane(&self, session_id: &str) -> io::Result<Option<Session>> {
        self.load_runtime_session(session_id).await
    }

    /// Atomically update only the shared Task list and its version.
    ///
    /// The default is safe for custom/legacy persistence: it loads the full
    /// runtime session, changes only Task-owned fields, then uses the paired
    /// control-plane save (which itself defaults to a full save). Returning
    /// `false` means the implementation could not load the target; callers that
    /// also hold a [`Storage`](crate::storage::Storage) may retain legacy
    /// behavior with an explicit full-load/full-save fallback.
    ///
    /// Implementations with per-session transactions should override this so
    /// the load, narrow mutation and save share one critical section.
    async fn update_task_list_control_plane(
        &self,
        session_id: &str,
        task_list: &TaskList,
        version: &str,
    ) -> io::Result<bool> {
        let Some(mut session) = self.load_runtime_session(session_id).await? else {
            return Ok(false);
        };
        session.set_task_list(task_list.clone());
        session.set_task_list_version_meta(version.to_string());
        self.save_runtime_control_plane(&mut session).await?;
        Ok(true)
    }

    /// Atomically update Task-owned control-plane fields only when the durable
    /// Task generation and exact list still match the expected snapshot.
    ///
    /// `false` covers an unsupported atomic compare-and-patch, a missing target,
    /// or a version conflict. Callers must treat it as a stale write and must
    /// not publish their staged Task state. The default fails closed because a
    /// load followed by a separately locked save is not an atomic CAS.
    async fn update_task_list_control_plane_if_version(
        &self,
        session_id: &str,
        expected_version: &str,
        expected_task_list: &TaskList,
        task_list: &TaskList,
        version: &str,
    ) -> io::Result<bool> {
        let _ = (
            session_id,
            expected_version,
            expected_task_list,
            task_list,
            version,
        );
        Ok(false)
    }

    /// Recoverably compare-and-patch the executing session and its shared root.
    /// Implementations must validate both generations before either target is
    /// written and may return `Ok(true)` only after both Task generations are
    /// durable with no undo record that could later revert them. An error after
    /// one physical write must restore both originals before returning or retain
    /// durable recovery state and fail subsequent paired access closed until
    /// recovery completes. Root-session callers pass the same id twice and
    /// receive the single-target CAS semantics above.
    async fn update_task_list_control_planes_if_version(
        &self,
        session_id: &str,
        shared_session_id: &str,
        expected_version: &str,
        expected_task_list: &TaskList,
        task_list: &TaskList,
        version: &str,
    ) -> io::Result<bool> {
        if session_id == shared_session_id {
            return self
                .update_task_list_control_plane_if_version(
                    session_id,
                    expected_version,
                    expected_task_list,
                    task_list,
                    version,
                )
                .await;
        }
        let _ = (
            session_id,
            shared_session_id,
            expected_version,
            expected_task_list,
            task_list,
            version,
        );
        Ok(false)
    }

    /// Append-safe checkpoint used at the shared engine execute boundary.
    ///
    /// Unlike [`save_runtime_session`](Self::save_runtime_session), this must
    /// preserve messages that were appended durably by a concurrent writer
    /// after the runner loaded its snapshot.  Implementations that can provide
    /// a per-session transaction should override this method and perform the
    /// load/merge/save under one lock.  The default still reconciles against a
    /// latest snapshot for lightweight/custom SDK persisters; the built-in
    /// storage implementation supplies the atomic variant.
    async fn checkpoint_runtime_session(&self, session: &mut Session) -> io::Result<()> {
        if let Some(durable) = self.load_runtime_session(&session.id).await? {
            append_missing_runtime_messages(session, &durable);
            merge_session_inbox_admission(session, &durable);
        }
        self.save_runtime_session(session).await
    }

    /// Load the latest runtime-visible session snapshot when the persistence
    /// implementation can coordinate reads. Tools may update a repository-owned
    /// clone while an agent loop holds its own live Session; the loop uses this
    /// hook to merge narrowly-scoped tool side effects before its next save.
    async fn load_runtime_session(&self, _session_id: &str) -> io::Result<Option<Session>> {
        Ok(None)
    }

    /// Clear the bounded compatibility queue iff it still equals the entries
    /// that were durably copied into SessionInbox. Implementations with a
    /// per-session transaction should override this method.
    async fn clear_legacy_pending_messages(
        &self,
        session_id: &str,
        expected: &[serde_json::Value],
    ) -> io::Result<bool> {
        let Some(mut latest) = self.load_runtime_session(session_id).await? else {
            return Ok(false);
        };
        if latest.pending_injected_messages().as_deref() != Some(expected) {
            return Ok(false);
        }
        latest.clear_pending_injected_messages();
        self.save_runtime_session(&mut latest).await?;
        Ok(true)
    }

    /// Append one JSON-line analysis record to the session's append-only
    /// token-usage log (see [`Storage::append_token_usage_record`]). Defaults to
    /// a no-op so non-file-backed persisters are unaffected.
    ///
    /// [`Storage::append_token_usage_record`]: crate::storage::Storage::append_token_usage_record
    async fn append_token_usage_record(&self, session_id: &str, json_line: &str) -> io::Result<()> {
        let _ = (session_id, json_line);
        Ok(())
    }
}

#[async_trait::async_trait]
impl<T: RuntimeSessionPersistence + ?Sized> RuntimeSessionPersistence for Arc<T> {
    async fn save_runtime_session(&self, session: &mut Session) -> io::Result<()> {
        (**self).save_runtime_session(session).await
    }

    async fn seed_runtime_activation(&self, session: &mut Session) -> io::Result<()> {
        (**self).seed_runtime_activation(session).await
    }

    async fn record_permission_posture_activation(
        &self,
        session_id: &str,
        expected_audit_revision: Option<u64>,
        seed: &PermissionAuditSeed,
    ) -> io::Result<Option<Session>> {
        (**self)
            .record_permission_posture_activation(session_id, expected_audit_revision, seed)
            .await
    }

    async fn save_runtime_control_plane(&self, session: &mut Session) -> io::Result<()> {
        (**self).save_runtime_control_plane(session).await
    }

    async fn load_runtime_control_plane(&self, session_id: &str) -> io::Result<Option<Session>> {
        (**self).load_runtime_control_plane(session_id).await
    }

    async fn update_task_list_control_plane(
        &self,
        session_id: &str,
        task_list: &TaskList,
        version: &str,
    ) -> io::Result<bool> {
        (**self)
            .update_task_list_control_plane(session_id, task_list, version)
            .await
    }

    async fn update_task_list_control_plane_if_version(
        &self,
        session_id: &str,
        expected_version: &str,
        expected_task_list: &TaskList,
        task_list: &TaskList,
        version: &str,
    ) -> io::Result<bool> {
        (**self)
            .update_task_list_control_plane_if_version(
                session_id,
                expected_version,
                expected_task_list,
                task_list,
                version,
            )
            .await
    }

    async fn update_task_list_control_planes_if_version(
        &self,
        session_id: &str,
        shared_session_id: &str,
        expected_version: &str,
        expected_task_list: &TaskList,
        task_list: &TaskList,
        version: &str,
    ) -> io::Result<bool> {
        (**self)
            .update_task_list_control_planes_if_version(
                session_id,
                shared_session_id,
                expected_version,
                expected_task_list,
                task_list,
                version,
            )
            .await
    }

    async fn checkpoint_runtime_session(&self, session: &mut Session) -> io::Result<()> {
        (**self).checkpoint_runtime_session(session).await
    }

    async fn load_runtime_session(&self, session_id: &str) -> io::Result<Option<Session>> {
        (**self).load_runtime_session(session_id).await
    }

    async fn clear_legacy_pending_messages(
        &self,
        session_id: &str,
        expected: &[serde_json::Value],
    ) -> io::Result<bool> {
        (**self)
            .clear_legacy_pending_messages(session_id, expected)
            .await
    }

    async fn append_token_usage_record(&self, session_id: &str, json_line: &str) -> io::Result<()> {
        (**self)
            .append_token_usage_record(session_id, json_line)
            .await
    }
}
