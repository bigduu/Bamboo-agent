//! Explicit, idle-only behavior-stamp migration. This is a Host operation,
//! never an ordinary checkpoint, model capability, or automatic legacy load.
use super::*;
use bamboo_domain::{
    ActorDirectoryEntry, ActorLogicalState, AgentRuntimeState, AgentStatusState,
    CURRENT_SESSION_VERSION,
};
use serde_json::Value;
use std::collections::BTreeMap;

pub(super) const MIGRATION_DIR: &str = ".session-version-migrations";
const RECEIPT_FILE: &str = "session-version-migration.json";
const MAX_STATE_BYTES: usize = 64 * 1024 * 1024;
const MAX_JOURNAL_BYTES: usize = MAX_STATE_BYTES * 8 + 32 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionVersionMigrationRequest {
    pub operation_id: String,
    pub birth_token: String,
    pub expected_session_version: Option<u32>,
    pub expected_metadata_version: u64,
    pub expected_snapshot_sha256: String,
}

impl SessionVersionMigrationRequest {
    pub fn valid(&self) -> bool {
        Uuid::parse_str(&self.operation_id).is_ok_and(|id| id.to_string() == self.operation_id)
            && digest_valid(&self.birth_token)
            && digest_valid(&self.expected_snapshot_sha256)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionVersionMigrationBlocker {
    pub code: String,
    pub message: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SessionVersionMigrationObservation {
    pub session_id: String,
    pub birth_token: String,
    pub session_version: Option<u32>,
    pub current_session_version: u32,
    pub metadata_version: u64,
    pub snapshot_sha256: String,
    pub eligible: bool,
    pub blockers: Vec<SessionVersionMigrationBlocker>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pending_operation: Option<SessionVersionMigrationRequest>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub committed_operation: Option<SessionVersionMigrationReceipt>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionVersionMigrationReceipt {
    pub status: String,
    pub operation_id: String,
    pub session_id: String,
    pub source_version: Option<u32>,
    pub target_version: u32,
    pub before_sha256: String,
    pub after_sha256: String,
    pub birth_token: String,
    // Retain the exact request so same-ID/different-payload retries cannot
    // turn a historical receipt into authority for another snapshot.
    pub request: SessionVersionMigrationRequest,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SessionVersionMigrationDecision {
    Committed(SessionVersionMigrationReceipt),
    NotFound,
    PreconditionFailed,
    OperationConflict,
    Blocked(Vec<SessionVersionMigrationBlocker>),
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Journal {
    version: u32,
    session_id: String,
    relative_directory: String,
    request: SessionVersionMigrationRequest,
    before: Pair,
    after: Pair,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Pair {
    main: String,
    runtime: String,
    sha256: String,
}

impl Pair {
    fn new(main: Vec<u8>, runtime: Vec<u8>) -> io::Result<Self> {
        let main = String::from_utf8(main).map_err(invalid)?;
        let runtime = String::from_utf8(runtime).map_err(invalid)?;
        let sha256 = pair_digest(main.as_bytes(), runtime.as_bytes());
        Ok(Self {
            main,
            runtime,
            sha256,
        })
    }
    fn valid(&self) -> bool {
        self.main.len() <= MAX_STATE_BYTES
            && self.runtime.len() <= MAX_STATE_BYTES
            && self.sha256 == pair_digest(self.main.as_bytes(), self.runtime.as_bytes())
    }
}

fn invalid(error: impl std::fmt::Display) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("Session version migration: {error}"),
    )
}
fn pending() -> io::Error {
    io::Error::new(
        io::ErrorKind::WouldBlock,
        "Session version migration recovery is required",
    )
}
fn digest_valid(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn pair_digest(main: &[u8], runtime: &[u8]) -> String {
    let mut hash = Sha256::new();
    hash.update(b"session-version-migration-pair-v1\0");
    for bytes in [main, runtime] {
        hash.update((bytes.len() as u64).to_le_bytes());
        hash.update(bytes);
    }
    format!("{:x}", hash.finalize())
}
fn blocker(code: &str, message: &str) -> SessionVersionMigrationBlocker {
    SessionVersionMigrationBlocker {
        code: code.into(),
        message: message.into(),
    }
}

fn pending_response(session: &Session) -> bool {
    session.pending_question.is_some()
        || session.has_pending_injected_messages()
        || session.metadata.iter().any(|(key, value)| {
            key == "permission.replay_save_recovery_required"
                || ((key.contains("permission_reexecute")
                    || key.starts_with("permission.reexecute")
                    || matches!(
                        key.as_str(),
                        "retry_resume_pending"
                            | "clarification_resume_pending"
                            | "conclusion_with_options_resume_pending"
                    ))
                    && !value.is_empty()
                    && value != "false")
        })
}

fn runtime_not_idle(session: &Session) -> bool {
    let busy = |state: &AgentRuntimeState| {
        !matches!(
            state.status,
            AgentStatusState::Idle
                | AgentStatusState::Completed
                | AgentStatusState::Cancelled
                | AgentStatusState::Failed
        ) || state.tools.active_tool_calls != 0
            || state.waiting_for_children.is_some()
            || state.waiting_for_bash.is_some()
            || state.suspension.is_some()
            || state.plan_mode.is_some()
    };
    if let Some(state) = &session.agent_runtime_state {
        return busy(state);
    }
    // Match the engine's typed-first fallback without inventing an idle state
    // when the retained legacy control plane cannot be decoded.
    session
        .metadata
        .get("agent.runtime.state")
        .is_some_and(|raw| {
            serde_json::from_str::<AgentRuntimeState>(raw).map_or(true, |state| busy(&state))
        })
}

/// Preserve every original JSON value (including unknown fields and retained
/// operation argument strings). Only the top-level behavior stamp changes.
/// Compact authority excludes this stamp, so retain its exact framed prefix.
fn current_stamp(bytes: &[u8], main: bool) -> io::Result<Vec<u8>> {
    let framed = main && compact_main::validate_full_main(bytes)?.is_some();
    let mut object: BTreeMap<String, Box<serde_json::value::RawValue>> =
        serde_json::from_slice(bytes).map_err(invalid)?;
    if main {
        object.remove(compact_main::MEMBER);
    }
    object.insert(
        "session_version".into(),
        serde_json::value::RawValue::from_string(CURRENT_SESSION_VERSION.to_string())
            .map_err(invalid)?,
    );
    let flat = serde_json::to_vec_pretty(&object).map_err(invalid)?;
    if !framed {
        return Ok(flat);
    }
    let length = compact_main::section_length(
        bytes
            .get(..compact_main::HEADER_BYTES)
            .ok_or_else(|| invalid("short Main frame"))?,
    )?;
    let mut result = bytes
        .get(..length)
        .ok_or_else(|| invalid("short Main section"))?
        .to_vec();
    result.extend_from_slice(
        flat.strip_prefix(b"{")
            .ok_or_else(|| invalid("invalid Main suffix"))?,
    );
    compact_main::validate_full_main(&result)?;
    Ok(result)
}

async fn regular(path: &Path, limit: usize) -> io::Result<Vec<u8>> {
    let metadata = fs::symlink_metadata(path).await?;
    if !metadata.file_type().is_file() || metadata.len() > limit as u64 {
        return Err(invalid("state is not a bounded regular file"));
    }
    let bytes = fs::read(path).await?;
    if bytes.len() > limit {
        return Err(invalid("state grew beyond its byte bound"));
    }
    Ok(bytes)
}

async fn inbox_nonempty(directory: &Path) -> io::Result<bool> {
    for queue in ["new", "cur"] {
        let path = directory.join("inbox").join(queue);
        match fs::symlink_metadata(&path).await {
            Ok(metadata) if !metadata.file_type().is_dir() => {
                return Err(invalid("Inbox queue is not a real directory"))
            }
            Ok(_) => {
                if fs::read_dir(path).await?.next_entry().await?.is_some() {
                    return Ok(true);
                }
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    Ok(false)
}

fn sync_parent(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    std::fs::File::open(
        path.parent()
            .ok_or_else(|| invalid("journal has no parent"))?,
    )?
    .sync_all()?;
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum MigrationFault {
    Prepared,
    Runtime,
    Main,
    Receipt,
}

impl SessionStoreV2 {
    fn version_migration_directory(&self) -> PathBuf {
        self.bamboo_home_dir.join(MIGRATION_DIR)
    }

    pub(super) async fn ensure_session_version_migration_dir(&self) -> io::Result<()> {
        match fs::symlink_metadata(self.version_migration_directory()).await {
            Ok(metadata) if !metadata.file_type().is_dir() => {
                return Err(invalid(
                    "migration journal directory is not a real directory",
                ))
            }
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        self.ensure_private_transaction_dir(self.version_migration_directory())
            .await
            .map(|_| ())
    }

    pub(super) async fn session_version_migration_journals(&self) -> io::Result<Vec<PathBuf>> {
        match fs::symlink_metadata(self.version_migration_directory()).await {
            Ok(metadata) if !metadata.file_type().is_dir() => {
                return Err(invalid("migration directory changed"))
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(error),
            _ => {}
        }
        let mut entries = match fs::read_dir(self.version_migration_directory()).await {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(error),
        };
        let mut paths = Vec::new();
        while let Some(entry) = entries.next_entry().await? {
            if entry.path().extension().is_some_and(|ext| ext == "json") {
                if !entry.file_type().await?.is_file() || paths.len() >= 256 {
                    return Err(invalid("invalid or excessive migration journals"));
                }
                paths.push(entry.path());
            }
        }
        paths.sort();
        Ok(paths)
    }

    pub(super) async fn ensure_no_session_version_migration_locked(&self) -> io::Result<()> {
        if self.session_version_migration_journals().await?.is_empty() {
            Ok(())
        } else {
            Err(pending())
        }
    }

    async fn migration_guards(&self, id: &str) -> io::Result<Arc<DefaultWriterGuards>> {
        validate_session_id(id)?;
        let lifecycle = self.lock_session_lifecycle_exclusive().await?;
        let task = self.lock_runtime_task_transaction_exclusive().await?;
        // Never recover an unrelated protocol on behalf of a refused request.
        if !self.runtime_task_journal_paths().await?.is_empty()
            || !self.session_copy_journal_paths().await?.is_empty()
        {
            return Err(pending());
        }
        let session = self.acquire_session_maintenance_lock(id).await?;
        Ok(DefaultWriterGuards::exclusive(lifecycle, task, session))
    }

    async fn migration_pair(&self, relative: &str) -> io::Result<Pair> {
        let directory = self.abs_path_from_rel(relative);
        // Canonical and staging selectors never accept symlink directories.
        let mut parent = self.bamboo_home_dir.clone();
        for part in Path::new(relative).components() {
            let std::path::Component::Normal(name) = part else {
                return Err(invalid("invalid canonical path"));
            };
            parent.push(name);
            if !fs::symlink_metadata(&parent).await?.file_type().is_dir() {
                return Err(invalid("canonical directory is not a real directory"));
            }
        }
        Pair::new(
            regular(&directory.join("session.json"), MAX_STATE_BYTES).await?,
            regular(&directory.join(RUNTIME_SIDECAR_FILE), MAX_STATE_BYTES).await?,
        )
    }

    async fn migration_source(&self, id: &str) -> io::Result<Option<(String, Session, Pair)>> {
        let Some(relative) = self.resolve_rel_path(id).await else {
            return Ok(None);
        };
        let pair = self.migration_pair(&relative).await?;
        let session = self.validate_migration_pair(id, &relative, &pair).await?;
        Ok(Some((relative, session, pair)))
    }

    async fn validate_migration_pair(
        &self,
        id: &str,
        relative: &str,
        pair: &Pair,
    ) -> io::Result<Session> {
        if !pair.valid() {
            return Err(invalid("state-pair hash or bounds mismatch"));
        }
        compact_main::validate_full_main(pair.main.as_bytes())?;
        let main: Session = serde_json::from_str(&pair.main).map_err(invalid)?;
        let side: Session = serde_json::from_str(&pair.runtime).map_err(invalid)?;
        if main.id != id
            || side.id != id
            || main.created_at != side.created_at
            || main.kind != side.kind
            || main.parent_session_id != side.parent_session_id
            || main.parent_created_at != side.parent_created_at
            || main.root_session_id != side.root_session_id
            || main.spawn_depth != side.spawn_depth
            || main.authority_identity != side.authority_identity
            || main.project_id_meta() != side.project_id_meta()
            || side.metadata_version < main.metadata_version
            || Self::default_writer_rel_path(&side)? != relative
            || !self.session_lifetime_is_live(&main).await?
        {
            return Err(invalid("canonical lifetime, lineage or Project mismatch"));
        }
        supervisor::validate_overlay(&main, Some(&side))?;
        main.check_conversation_generation(&side).map_err(invalid)?;
        self.validate_root_tool_authority_overlay(id, &main, Some(&side))
            .await?;
        Ok(overlay_runtime_sidecar(main, Some(side)))
    }

    async fn migration_blockers(
        &self,
        session: &Session,
        relative: &str,
    ) -> io::Result<Vec<SessionVersionMigrationBlocker>> {
        let mut reasons = Vec::new();
        if !session.execution_compatibility().execution_allowed {
            reasons.push(blocker(
                "session_execution_incompatible",
                "Resolve the concrete Session/schema conflict before migration.",
            ));
        } else if !matches!(session.session_version, None | Some(1)) {
            reasons.push(blocker(
                "already_current",
                "This Session already uses the current behavior version.",
            ));
        }
        if pending_response(session) {
            reasons.push(blocker(
                "pending_response",
                "Resolve pending input, clarification, approval or retained replay first.",
            ));
        }
        if runtime_not_idle(session) {
            reasons.push(blocker(
                "execution_not_idle",
                "Retire the current execution, Child/Bash wait or Plan before migration.",
            ));
        }
        let mut latest_calls = BTreeMap::new();
        let mut results = BTreeMap::new();
        for (index, message) in session.messages.iter().enumerate() {
            for call in message.tool_calls.as_deref().unwrap_or_default() {
                latest_calls.insert(call.id.as_str(), index);
            }
            if let Some(id) = message.tool_call_id.as_deref() {
                results.insert(id, index);
            }
            if message.role == Role::Tool
                && serde_json::from_str::<Value>(&message.content)
                    .ok()
                    .is_some_and(|value| {
                        value.get("requires_approval").and_then(Value::as_bool) == Some(true)
                            || value
                                .get("permission_request")
                                .is_some_and(Value::is_object)
                    })
            {
                reasons.push(blocker(
                    "pending_tool_response",
                    "A retained tool permission request still needs its original response.",
                ));
            }
        }
        if latest_calls
            .iter()
            .any(|(id, index)| results.get(id).is_none_or(|result| result < index))
        {
            reasons.push(blocker(
                "unfinished_tool_call",
                "A retained tool call has no completed result.",
            ));
        }
        let directory = self.abs_path_from_rel(relative);
        if session.kind == SessionKind::Root {
            let children = self.canonical_root_child_ids(&session.id).await?;
            if children.len() > 256 {
                reasons.push(blocker(
                    "child_state_unavailable",
                    "The Child tree exceeds the bounded migration inspection limit.",
                ));
            } else {
                for id in children {
                    let child = directory.join("children").join(&id);
                    let state: Session = serde_json::from_slice(
                        &regular(&child.join(RUNTIME_SIDECAR_FILE), MAX_STATE_BYTES).await?,
                    )
                    .map_err(invalid)?;
                    if state.id != id
                        || state.root_session_id != session.id
                        || state.project_id_meta() != session.project_id_meta()
                    {
                        return Err(invalid("Child identity or Project changed"));
                    }
                    let runtime_busy = !state.execution_compatibility().execution_allowed
                        || pending_response(&state)
                        || runtime_not_idle(&state);
                    let actor_busy =
                        match regular(&child.join("actor-authority.json"), MAX_STATE_BYTES).await {
                            Ok(bytes) => {
                                let row: ActorDirectoryEntry =
                                    serde_json::from_slice(&bytes).map_err(invalid)?;
                                row.validate().map_err(invalid)?;
                                if !row.actor.matches_session(&state) {
                                    return Err(invalid("Child Actor lifetime changed"));
                                }
                                row.actor.state == ActorLogicalState::Active
                                    || row.activation.as_ref().is_some_and(|a| a.status.is_live())
                            }
                            Err(error) if error.kind() == io::ErrorKind::NotFound => false,
                            Err(error) => return Err(error),
                        };
                    if runtime_busy || actor_busy || inbox_nonempty(&child).await? {
                        reasons.push(blocker(
                            "child_execution_pending",
                            "Retire pending Child execution before migrating its parent.",
                        ));
                        break;
                    }
                }
            }
        }
        if inbox_nonempty(&directory).await? {
            reasons.push(blocker(
                "inbox_not_empty",
                "Consume or explicitly resolve pending Inbox entries before migration.",
            ));
        }
        let actor = directory.join("actor-authority.json");
        let marker = directory.join("actor-authority.initialized.json");
        match (
            fs::try_exists(&actor).await?,
            fs::try_exists(&marker).await?,
        ) {
            (false, false) => {}
            (true, true) => {
                let row: ActorDirectoryEntry =
                    serde_json::from_slice(&regular(&actor, MAX_STATE_BYTES).await?)
                        .map_err(invalid)?;
                row.validate().map_err(invalid)?;
                let evidence = regular(&marker, 4096).await?;
                let project = session
                    .project_id_meta()
                    .map(bamboo_domain::ProjectId::parse)
                    .transpose()
                    .map_err(invalid)?;
                actor_directory::validate_census_witnesses(
                    &regular(&actor, MAX_STATE_BYTES).await?,
                    &evidence,
                    session,
                    project.as_ref(),
                )?;
                if row.actor.observed_metadata_version != session.metadata_version {
                    return Err(invalid("Actor observation revision is stale"));
                }
                if row.actor.state == ActorLogicalState::Active
                    || row
                        .activation
                        .as_ref()
                        .is_some_and(|activation| activation.status.is_live())
                {
                    reasons.push(blocker(
                        "actor_activation_not_retired",
                        "Retire the Actor activation through its current owner before migration.",
                    ));
                }
            }
            _ => return Err(invalid("incomplete Actor authority")),
        }
        Ok(reasons)
    }

    async fn migration_receipt(
        &self,
        relative: &str,
    ) -> io::Result<Option<SessionVersionMigrationReceipt>> {
        let bytes = match regular(
            &self.abs_path_from_rel(relative).join(RECEIPT_FILE),
            32 * 1024,
        )
        .await
        {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        let receipt: SessionVersionMigrationReceipt =
            serde_json::from_slice(&bytes).map_err(invalid)?;
        if !receipt.request.valid()
            || receipt.status != "committed"
            || receipt.target_version != CURRENT_SESSION_VERSION
            || receipt.operation_id != receipt.request.operation_id
            || receipt.source_version != receipt.request.expected_session_version
            || receipt.birth_token != receipt.request.birth_token
            || receipt.before_sha256 != receipt.request.expected_snapshot_sha256
            || !digest_valid(&receipt.after_sha256)
        {
            return Err(invalid("invalid committed migration receipt"));
        }
        Ok(Some(receipt))
    }

    fn migration_observation(
        session: &Session,
        pair: &Pair,
        blockers: Vec<SessionVersionMigrationBlocker>,
    ) -> SessionVersionMigrationObservation {
        SessionVersionMigrationObservation {
            session_id: session.id.clone(),
            birth_token: session.root_mode_birth_token(),
            session_version: session.session_version,
            current_session_version: CURRENT_SESSION_VERSION,
            metadata_version: session.metadata_version,
            snapshot_sha256: pair.sha256.clone(),
            eligible: blockers.is_empty(),
            blockers,
            pending_operation: None,
            committed_operation: None,
        }
    }

    async fn read_migration_journal(&self, path: &Path) -> io::Result<Journal> {
        let journal: Journal =
            serde_json::from_slice(&regular(path, MAX_JOURNAL_BYTES).await?).map_err(invalid)?;
        validate_session_id(&journal.session_id)?;
        if journal.version != 1
            || !journal.request.valid()
            || !matches!(journal.request.expected_session_version, None | Some(1))
            || path.file_stem().and_then(|s| s.to_str())
                != Some(journal.request.operation_id.as_str())
            || !journal.before.valid()
            || !journal.after.valid()
            || journal.before.sha256 != journal.request.expected_snapshot_sha256
            || current_stamp(journal.before.main.as_bytes(), true)? != journal.after.main.as_bytes()
            || current_stamp(journal.before.runtime.as_bytes(), false)?
                != journal.after.runtime.as_bytes()
        {
            return Err(invalid(
                "journal request, pair hashes or stamp-only transformation mismatch",
            ));
        }
        let source = self
            .validate_migration_pair(
                &journal.session_id,
                &journal.relative_directory,
                &journal.before,
            )
            .await?;
        if source.root_mode_birth_token() != journal.request.birth_token
            || source.session_version != journal.request.expected_session_version
            || source.metadata_version != journal.request.expected_metadata_version
        {
            return Err(invalid("journal source witness mismatch"));
        }
        let current = self.migration_pair(&journal.relative_directory).await?;
        if ![&journal.before.main, &journal.after.main].contains(&&current.main)
            || ![&journal.before.runtime, &journal.after.runtime].contains(&&current.runtime)
        {
            return Err(invalid("canonical bytes changed outside the migration"));
        }
        Ok(journal)
    }

    /// Read-only readiness. A pending exact intent is returned for explicit
    /// recovery after a client timeout/reload; GET never rolls the pair forward.
    pub async fn inspect_session_version_migration(
        &self,
        id: &str,
    ) -> io::Result<Option<SessionVersionMigrationObservation>> {
        let _guards = self.migration_guards(id).await?;
        for path in self.session_version_migration_journals().await? {
            let journal = self.read_migration_journal(&path).await?;
            if journal.session_id == id {
                let source = self
                    .validate_migration_pair(id, &journal.relative_directory, &journal.before)
                    .await?;
                let mut view = Self::migration_observation(
                    &source,
                    &journal.before,
                    vec![blocker(
                        "migration_pending",
                        "Confirm this exact migration operation to recover its result.",
                    )],
                );
                view.pending_operation = Some(journal.request);
                return Ok(Some(view));
            }
        }
        let Some((relative, session, pair)) = self.migration_source(id).await? else {
            return Ok(None);
        };
        let reasons = self.migration_blockers(&session, &relative).await?;
        let mut observation = Self::migration_observation(&session, &pair, reasons);
        observation.committed_operation = self.migration_receipt(&relative).await?;
        if observation
            .committed_operation
            .as_ref()
            .is_some_and(|receipt| {
                receipt.session_id != id || receipt.birth_token != observation.birth_token
            })
        {
            return Err(invalid("migration receipt lifetime mismatch"));
        }
        Ok(Some(observation))
    }

    async fn finish_version_migration(
        &self,
        path: &Path,
        journal: &Journal,
        guards: &Arc<DefaultWriterGuards>,
    ) -> io::Result<SessionVersionMigrationReceipt> {
        let directory = self.abs_path_from_rel(&journal.relative_directory);
        let source: Session = serde_json::from_str(&journal.before.runtime).map_err(invalid)?;
        guards.hold_tree(
            self.acquire_actor_tree_write_guard(if source.kind == SessionKind::Root {
                &source.id
            } else {
                &source.root_session_id
            })
            .await?,
        );
        // Both complete pairs were checked before either canonical publication.
        for (name, before, after) in [
            (
                RUNTIME_SIDECAR_FILE,
                &journal.before.runtime,
                &journal.after.runtime,
            ),
            ("session.json", &journal.before.main, &journal.after.main),
        ] {
            let current = regular(&directory.join(name), MAX_STATE_BYTES).await?;
            if current != before.as_bytes() && current != after.as_bytes() {
                return Err(invalid(
                    "canonical file changed before migration publication",
                ));
            }
            if current != after.as_bytes() {
                self.write_default_bytes(&directory.join(name), after.as_bytes().to_vec(), guards)
                    .await?;
            }
            #[cfg(test)]
            self.migration_fault(if name == RUNTIME_SIDECAR_FILE {
                MigrationFault::Runtime
            } else {
                MigrationFault::Main
            })?;
        }
        let receipt = SessionVersionMigrationReceipt {
            status: "committed".into(),
            operation_id: journal.request.operation_id.clone(),
            session_id: journal.session_id.clone(),
            source_version: journal.request.expected_session_version,
            target_version: CURRENT_SESSION_VERSION,
            before_sha256: journal.before.sha256.clone(),
            after_sha256: journal.after.sha256.clone(),
            birth_token: journal.request.birth_token.clone(),
            request: journal.request.clone(),
        };
        if let Some(previous) = self.migration_receipt(&journal.relative_directory).await? {
            if previous != receipt {
                return Err(invalid(
                    "a different migration receipt already owns this lifetime",
                ));
            }
        } else {
            self.write_default_bytes(
                &directory.join(RECEIPT_FILE),
                serde_json::to_vec_pretty(&receipt).map_err(invalid)?,
                guards,
            )
            .await?;
        }
        #[cfg(test)]
        self.migration_fault(MigrationFault::Receipt)?;
        let path = path.to_path_buf();
        Self::default_writer_job(guards, move || {
            std::fs::remove_file(&path)?;
            sync_parent(&path)
        })
        .await?;
        Ok(receipt)
    }

    pub async fn migrate_session_version(
        &self,
        id: &str,
        request: &SessionVersionMigrationRequest,
    ) -> io::Result<SessionVersionMigrationDecision> {
        if !request.valid() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid Session migration intent",
            ));
        }
        let guards = self.migration_guards(id).await?;
        let journals = self.session_version_migration_journals().await?;
        let path = self
            .version_migration_directory()
            .join(format!("{}.json", request.operation_id));
        if journals.contains(&path) {
            let journal = self.read_migration_journal(&path).await?;
            if journal.session_id != id || journal.request != *request {
                return Ok(SessionVersionMigrationDecision::OperationConflict);
            }
            return self
                .finish_version_migration(&path, &journal, &guards)
                .await
                .map(SessionVersionMigrationDecision::Committed);
        }
        if !journals.is_empty() {
            return Err(pending());
        }
        let Some((relative, session, before)) = self.migration_source(id).await? else {
            return Ok(SessionVersionMigrationDecision::NotFound);
        };
        if let Some(receipt) = self.migration_receipt(&relative).await? {
            if receipt.session_id != id || receipt.birth_token != session.root_mode_birth_token() {
                return Err(invalid("migration receipt lifetime mismatch"));
            }
            return Ok(if receipt.request == *request {
                SessionVersionMigrationDecision::Committed(receipt)
            } else {
                SessionVersionMigrationDecision::OperationConflict
            });
        }
        if request.birth_token != session.root_mode_birth_token()
            || request.expected_session_version != session.session_version
            || request.expected_metadata_version != session.metadata_version
            || request.expected_snapshot_sha256 != before.sha256
        {
            return Ok(SessionVersionMigrationDecision::PreconditionFailed);
        }
        let blockers = self.migration_blockers(&session, &relative).await?;
        if !blockers.is_empty() {
            return Ok(SessionVersionMigrationDecision::Blocked(blockers));
        }
        let after = Pair::new(
            current_stamp(before.main.as_bytes(), true)?,
            current_stamp(before.runtime.as_bytes(), false)?,
        )?;
        let journal = Journal {
            version: 1,
            session_id: id.into(),
            relative_directory: relative,
            request: request.clone(),
            before,
            after,
        };
        let bytes = serde_json::to_vec(&journal).map_err(invalid)?;
        if bytes.len() > MAX_JOURNAL_BYTES {
            return Err(invalid("migration journal exceeds byte bound"));
        }
        self.write_default_bytes(&path, bytes, &guards).await?;
        #[cfg(test)]
        self.migration_fault(MigrationFault::Prepared)?;
        // Re-read the durable manifest and exact pair before roll-forward.
        let journal = self.read_migration_journal(&path).await?;
        self.finish_version_migration(&path, &journal, &guards)
            .await
            .map(SessionVersionMigrationDecision::Committed)
    }

    pub(super) async fn recover_session_version_migrations(&self) -> io::Result<()> {
        // Enumeration belongs to the same global gate as the active publisher.
        // Otherwise a constructor can retain a path which a waiting migration
        // has legitimately committed and removed before we acquire authority.
        let lifecycle = self.lock_session_lifecycle_exclusive().await?;
        let task = self.lock_runtime_task_transaction_exclusive().await?;
        let journals = self.session_version_migration_journals().await?;
        if !journals.is_empty()
            && (!self.runtime_task_journal_paths().await?.is_empty()
                || !self.session_copy_journal_paths().await?.is_empty())
        {
            return Err(pending());
        }
        for path in journals {
            let journal = self.read_migration_journal(&path).await?;
            let session = self
                .acquire_session_maintenance_lock(&journal.session_id)
                .await?;
            let guards = DefaultWriterGuards::exclusive(lifecycle.clone(), task.clone(), session);
            self.finish_version_migration(&path, &journal, &guards)
                .await?;
        }
        Ok(())
    }

    #[cfg(test)]
    fn migration_fault(&self, point: MigrationFault) -> io::Result<()> {
        let mut fault = self.session_version_migration_fault.lock().unwrap();
        if *fault == Some(point) {
            *fault = None;
            return Err(io::Error::other("injected Session migration crash"));
        }
        Ok(())
    }
}
