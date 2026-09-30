//! Coherent, bounded, read-only projection of one canonical durable Root tree.
//! No index lookup, ensure/inspect_actor, journal recovery, or Session loader.

use super::SessionStoreV2;
use async_trait::async_trait;
use bamboo_domain::{
    ActorSnapshotError as Error, ActorSnapshotLimits, ActorSnapshotPort, ActorSnapshotPrincipal,
    PublicActorSubtreeSnapshot,
};

#[async_trait]
impl ActorSnapshotPort for SessionStoreV2 {
    async fn actor_subtree_snapshot(
        &self,
        principal: ActorSnapshotPrincipal,
        root_id: &str,
        subtree_id: &str,
        limits: ActorSnapshotLimits,
    ) -> Result<PublicActorSubtreeSnapshot, Error> {
        if !limits.is_bounded() {
            return Err(Error::BudgetExceeded);
        }
        #[cfg(any(target_os = "linux", target_os = "macos", windows))]
        {
            supported::selector(root_id)?;
            supported::selector(subtree_id)?;
            // Same lock order as writers. The exclusive Task gate excludes
            // Session/actor commits; lifecycle read excludes deletion/rebirth.
            // These acquisition methods do not load or recover any journal.
            let lifecycle = self
                .lock_session_lifecycle_shared()
                .await
                .map_err(|_| Error::StorageUnavailable)?;
            let transactions = self
                .lock_runtime_task_transaction_exclusive()
                .await
                .map_err(|_| Error::StorageUnavailable)?;
            let home = self.bamboo_home_dir.clone();
            let root = root_id.to_owned();
            let subtree = subtree_id.to_owned();
            tokio::task::spawn_blocking(move || {
                // A cancelled HTTP future cannot release these guards while
                // spawn_blocking is still reading durable authority.
                let _guards = (lifecycle, transactions);
                supported::read_snapshot(&home, &principal, &root, &subtree, limits)
            })
            .await
            .map_err(|_| Error::StorageUnavailable)?
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
        {
            let _ = (principal, root_id, subtree_id);
            Err(Error::UnsupportedAuthority)
        }
    }
}

impl SessionStoreV2 {
    /// Cheap, read-only poll position for the host-authorized Root tree stream.
    /// Callers first authorize the full subtree once. Each poll revalidates
    /// canonical Root identity and reads the same marker as the full snapshot.
    pub async fn actor_tree_cursor(&self, root_id: &str) -> Result<Option<(String, u64)>, Error> {
        #[cfg(any(target_os = "linux", target_os = "macos", windows))]
        {
            supported::selector(root_id)?;
            let lifecycle = self
                .lock_session_lifecycle_shared()
                .await
                .map_err(|_| Error::StorageUnavailable)?;
            let transactions = self
                .lock_runtime_task_transaction_exclusive()
                .await
                .map_err(|_| Error::StorageUnavailable)?;
            let home = self.bamboo_home_dir.clone();
            let root = root_id.to_owned();
            tokio::task::spawn_blocking(move || {
                let _guards = (lifecycle, transactions);
                supported::read_cursor(&home, &root)
            })
            .await
            .map_err(|_| Error::StorageUnavailable)?
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
        {
            let _ = root_id;
            Err(Error::UnsupportedAuthority)
        }
    }
}

#[cfg(any(target_os = "linux", target_os = "macos", windows))]
mod supported {
    use super::super::{
        actor_snapshot_reader::{Directory, ReadBudget},
        root_context::ROOT_TOOL_AUTHORITY_PROOF_FILE,
        supervisor,
        supervisor_proof::SUPERVISOR_PROOF_FILE,
        ActorTreeRevision, ACTOR_TREE_REVISION_FILE, RUNTIME_SIDECAR_FILE,
        RUNTIME_TASK_TRANSACTION_DIR, SESSION_COPY_TRANSACTION_DIR,
    };
    use super::*;
    use bamboo_domain::{
        ActorAncestorObservation, ActorDirectoryEntry, ActorLogicalState, ActorSnapshotAuthority,
        PublicActorActivation, PublicActorRevision, PublicActorSnapshotNode,
        RootModeOperationReceipt, Session, SessionAuthorityIdentity, SessionKind,
        SupervisorManagementState, ACTOR_DIRECTORY_SCHEMA_VERSION, ACTOR_SNAPSHOT_SCHEMA_VERSION,
    };
    use chrono::{DateTime, Utc};
    use serde::{de::DeserializeOwned, Deserialize};
    use sha2::{Digest, Sha256};
    use std::collections::{BTreeMap, HashSet};
    use std::ffi::OsStr;
    use std::path::Path;
    use uuid::Uuid;

    pub(super) fn selector(id: &str) -> Result<(), Error> {
        if id.is_empty()
            || id.trim() != id
            || id.contains(['/', '\\'])
            || id.contains("..")
            || id.len() > 256
            || id.chars().any(char::is_control)
        {
            return Err(Error::InvalidSelector);
        }
        Ok(())
    }

    fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, Error> {
        serde_json::from_slice(bytes).map_err(|_| Error::InconsistentAuthority)
    }

    fn cursor_for_root(
        directory: &Directory,
        root: &Session,
        budget: &mut ReadBudget,
    ) -> Result<Option<(String, u64)>, Error> {
        Ok(directory
            .read(ACTOR_TREE_REVISION_FILE, 4096, budget)?
            .map(|bytes| decode::<ActorTreeRevision>(&bytes))
            .transpose()?
            .and_then(|marker| {
                marker
                    .matches_root(root)
                    .then(|| (marker.cursor(), marker.revision))
            }))
    }

    pub(super) fn read_cursor(home: &Path, root: &str) -> Result<Option<(String, u64)>, Error> {
        let home = Directory::open_absolute(home)?;
        let mut budget = ReadBudget::new(ActorSnapshotLimits::default());
        for name in [RUNTIME_TASK_TRANSACTION_DIR, SESSION_COPY_TRANSACTION_DIR] {
            if let Some(dir) = home.child(OsStr::new(name))? {
                if !dir.entries(&mut budget)?.is_empty() {
                    return Err(Error::PendingTransaction);
                }
            }
        }
        let sessions = home.child(OsStr::new("sessions"))?.ok_or(Error::NotFound)?;
        let root_dir = sessions.child(OsStr::new(root))?.ok_or(Error::NotFound)?;
        let source = read_source(&root_dir, root, root, true, &mut budget)?;
        if let Some(revocations) = home.child(OsStr::new(".root-revocations"))? {
            if let Some(bytes) = revocations.read(&format!("{root}.json"), 4096, &mut budget)? {
                let evidence = decode::<Revocation>(&bytes)?;
                if evidence.version != 1 || evidence.session_id != root {
                    return Err(Error::InconsistentAuthority);
                }
                if source.session.created_at <= evidence.revoked_through {
                    return Err(Error::NotFound);
                }
            }
        }
        cursor_for_root(&root_dir, &source.session, &mut budget)
    }

    #[derive(Default, Deserialize)]
    struct ProjectMetadata {
        #[serde(default)]
        project_id: Option<String>,
    }

    /// Skip messages, prompts, tools, and all metadata except Project identity.
    /// Whole-file I/O is bounded before deserialization; discarded JSON values
    /// are skipped by serde rather than allocated as an unbounded generic Value.
    #[derive(Deserialize)]
    struct Identity {
        id: String,
        created_at: DateTime<Utc>,
        #[serde(default)]
        title: String,
        #[serde(default)]
        metadata_version: u64,
        #[serde(default)]
        kind: SessionKind,
        #[serde(default)]
        parent_session_id: Option<String>,
        #[serde(default)]
        root_session_id: String,
        #[serde(default)]
        spawn_depth: u32,
        #[serde(default)]
        authority_identity: SessionAuthorityIdentity,
        #[serde(default)]
        supervisor_management: Option<SupervisorManagementState>,
        #[serde(default)]
        root_orchestration_only: bool,
        #[serde(default)]
        root_tool_authority_revision: u64,
        #[serde(default)]
        root_mode_transition_epoch: u64,
        #[serde(default)]
        root_mode_operations: Vec<RootModeOperationReceipt>,
        #[serde(default)]
        metadata: ProjectMetadata,
        #[serde(default)]
        runtime_metadata: Option<ProjectMetadata>,
    }

    impl Identity {
        fn session(self) -> Session {
            let mut session = Session::new(self.id, "");
            session.created_at = self.created_at;
            session.title = self.title;
            session.metadata_version = self.metadata_version;
            session.kind = self.kind;
            session.parent_session_id = self.parent_session_id;
            session.root_session_id = self.root_session_id;
            session.spawn_depth = self.spawn_depth;
            session.authority_identity = self.authority_identity;
            session.supervisor_management = self.supervisor_management;
            session.root_orchestration_only = self.root_orchestration_only;
            session.root_tool_authority_revision = self.root_tool_authority_revision;
            session.root_mode_transition_epoch = self.root_mode_transition_epoch;
            session.root_mode_operations = self.root_mode_operations;
            if let Some(project) = self
                .runtime_metadata
                .and_then(|m| m.project_id)
                .or(self.metadata.project_id)
            {
                session.set_project_id_meta(project);
            }
            session
        }
    }

    struct Source {
        session: Session,
        row: Option<ActorDirectoryEntry>,
    }

    fn live_fence_matches(source: &Source, fence: &bamboo_domain::ActorActivationFence) -> bool {
        source.row.as_ref().is_some_and(|row| {
            row.actor.state == ActorLogicalState::Active
                && row.activation.as_ref().is_some_and(|activation| {
                    activation.status.is_live()
                        && activation.lease_expires_at > Utc::now()
                        && activation.matches_fence(fence)
                })
        })
    }

    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Marker {
        schema_version: u32,
        actor_id: String,
        session_created_at: DateTime<Utc>,
    }

    fn read_source(
        directory: &Directory,
        id: &str,
        root: &str,
        is_root: bool,
        budget: &mut ReadBudget,
    ) -> Result<Source, Error> {
        let bytes = directory
            .read_main_section(budget)?
            .ok_or(Error::InconsistentAuthority)?;
        let main = super::super::compact_main::decode_v1_section(&bytes, budget.limits.file_bytes)
            .map_err(|_| Error::InconsistentAuthority)?
            .into_snapshot_session();
        let side = directory
            .read(RUNTIME_SIDECAR_FILE, budget.limits.file_bytes, budget)?
            .map(|bytes| decode::<Identity>(&bytes).map(Identity::session))
            .transpose()?;
        if main.id != id
            || main.kind
                != if is_root {
                    SessionKind::Root
                } else {
                    SessionKind::Child
                }
        {
            return Err(Error::InconsistentAuthority);
        }
        if let Some(side) = &side {
            if side.id != main.id
                || side.created_at != main.created_at
                || side.kind != main.kind
                || side.parent_session_id != main.parent_session_id
                || side.root_session_id != main.root_session_id
                || side.spawn_depth != main.spawn_depth
                || side.metadata_version < main.metadata_version
                || side.authority_identity != main.authority_identity
                || side.project_id_meta() != main.project_id_meta()
            {
                return Err(Error::StaleAuthority);
            }
        }
        supervisor::validate_overlay(&main, side.as_ref())
            .map_err(|_| Error::InconsistentAuthority)?;
        if is_root {
            let side = side.as_ref().ok_or(Error::InconsistentAuthority)?;
            SessionStoreV2::validate_snapshot_root_pair(&main, side)
                .map_err(|_| Error::StaleAuthority)?;
            let proof = directory
                .read(ROOT_TOOL_AUTHORITY_PROOF_FILE, 4096, budget)?
                .ok_or(Error::InconsistentAuthority)?;
            SessionStoreV2::validate_snapshot_root_proof(side, &proof)
                .map_err(|_| Error::StaleAuthority)?;
            if !matches!(side.authority_identity, SessionAuthorityIdentity::Ordinary) {
                let proof = directory
                    .read(SUPERVISOR_PROOF_FILE, 256 * 1024, budget)?
                    .ok_or(Error::InconsistentAuthority)?;
                SessionStoreV2::validate_snapshot_supervisor_proof(side, &proof)
                    .map_err(|_| Error::StaleAuthority)?;
            }
        }
        let mut session = side.unwrap_or(main);
        if is_root && session.root_session_id.is_empty() {
            session.root_session_id = id.to_owned();
        }
        if session.root_session_id != root {
            return Err(Error::InconsistentAuthority);
        }
        bamboo_domain::ActorSession::from_session(&session)
            .map_err(|_| Error::InconsistentAuthority)?;
        let row = directory.read("actor-authority.json", budget.limits.file_bytes, budget)?;
        let marker = directory.read("actor-authority.initialized.json", 4096, budget)?;
        let row = match (row, marker) {
            (None, None) => None, // Historical/cold absence stays unknown: never ensure/create.
            (Some(row), Some(marker)) => {
                let row = decode::<ActorDirectoryEntry>(&row)?;
                let marker = decode::<Marker>(&marker)?;
                row.validate().map_err(|_| Error::InconsistentAuthority)?;
                if marker.schema_version != ACTOR_DIRECTORY_SCHEMA_VERSION
                    || marker.actor_id != id
                    || marker.session_created_at != session.created_at
                    || !row.actor.matches_session(&session)
                    || row.actor.project_id != session.project_id_meta()
                    || row.actor.observed_metadata_version != session.metadata_version
                {
                    return Err(Error::StaleAuthority);
                }
                if row
                    .activation
                    .as_ref()
                    .is_some_and(|a| Uuid::parse_str(&a.activation_id).is_err())
                {
                    return Err(Error::InconsistentAuthority);
                }
                Some(row)
            }
            _ => return Err(Error::InconsistentAuthority),
        };
        Ok(Source { session, row })
    }

    fn lineage<'a>(
        id: &'a str,
        sources: &'a BTreeMap<String, Source>,
    ) -> Result<Vec<&'a Source>, Error> {
        let mut chain = Vec::new();
        let mut current = sources.get(id).ok_or(Error::NotFound)?;
        let mut seen = HashSet::new();
        while let Some(parent) = &current.session.parent_session_id {
            if !seen.insert(&current.session.id) || chain.len() >= sources.len() {
                return Err(Error::InconsistentAuthority);
            }
            let parent = sources.get(parent).ok_or(Error::InconsistentAuthority)?;
            if parent.session.spawn_depth.checked_add(1) != Some(current.session.spawn_depth)
                || parent.session.created_at > current.session.created_at
                || parent.session.project_id_meta() != current.session.project_id_meta()
            {
                return Err(Error::InconsistentAuthority);
            }
            chain.push(parent);
            current = parent;
        }
        if current.session.kind != SessionKind::Root || current.session.spawn_depth != 0 {
            return Err(Error::InconsistentAuthority);
        }
        Ok(chain)
    }

    use super::super::compact_main::public_title;

    #[derive(Deserialize)]
    struct Revocation {
        version: u32,
        session_id: String,
        revoked_through: DateTime<Utc>,
    }

    struct BoundedDigest {
        digest: Sha256,
        bytes: usize,
        max: usize,
    }

    impl std::io::Write for BoundedDigest {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > self.max.saturating_sub(self.bytes) {
                return Err(std::io::Error::other("snapshot_budget"));
            }
            self.digest.update(bytes);
            self.bytes += bytes.len();
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    pub(super) fn read_snapshot(
        home: &Path,
        principal: &ActorSnapshotPrincipal,
        root: &str,
        subtree: &str,
        limits: ActorSnapshotLimits,
    ) -> Result<PublicActorSubtreeSnapshot, Error> {
        let home = Directory::open_absolute(home)?;
        let mut budget = ReadBudget::new(limits);
        // Any pending journal makes a coherent view unavailable. Stop at first
        // bounded enumeration; never call auto-recovery or mutate the journal.
        for name in [RUNTIME_TASK_TRANSACTION_DIR, SESSION_COPY_TRANSACTION_DIR] {
            if let Some(dir) = home.child(OsStr::new(name))? {
                if !dir.entries(&mut budget)?.is_empty() {
                    return Err(Error::PendingTransaction);
                }
            }
        }
        let sessions = home.child(OsStr::new("sessions"))?.ok_or(Error::NotFound)?;
        let root_dir = sessions.child(OsStr::new(root))?.ok_or(Error::NotFound)?;
        let children = root_dir.child(OsStr::new("children"))?;
        let mut sources = BTreeMap::new();
        // Prove the requester before enumerating any other children or reading
        // a foreign Root's main file. The full lineage is still verified below.
        if let ActorSnapshotAuthority::LiveActor(fence) = principal.authority() {
            selector(&fence.actor_id).map_err(|_| Error::UnauthorizedScope)?;
            let source = if fence.actor_id == root {
                read_source(&root_dir, root, root, true, &mut budget)?
            } else {
                if limits.nodes < 2 {
                    return Err(Error::BudgetExceeded);
                }
                let directory = children
                    .as_ref()
                    .ok_or(Error::UnauthorizedScope)?
                    .child(OsStr::new(&fence.actor_id))?
                    .ok_or(Error::UnauthorizedScope)?;
                read_source(&directory, &fence.actor_id, root, false, &mut budget)?
            };
            if !live_fence_matches(&source, fence) {
                return Err(Error::UnauthorizedScope);
            }
            sources.insert(fence.actor_id.clone(), source);
        }
        if !sources.contains_key(root) {
            sources.insert(
                root.to_owned(),
                read_source(&root_dir, root, root, true, &mut budget)?,
            );
        }
        if let Some(revocations) = home.child(OsStr::new(".root-revocations"))? {
            if let Some(bytes) = revocations.read(&format!("{root}.json"), 4096, &mut budget)? {
                let evidence = decode::<Revocation>(&bytes)?;
                if evidence.version != 1 || evidence.session_id != root {
                    return Err(Error::InconsistentAuthority);
                }
                if sources[root].session.created_at <= evidence.revoked_through {
                    return Err(Error::NotFound);
                }
            }
        }
        if let Some(children) = children {
            for name in children.entries(&mut budget)? {
                let id = name.to_str().ok_or(Error::InconsistentAuthority)?;
                selector(id).map_err(|_| Error::InconsistentAuthority)?;
                if id == root {
                    return Err(Error::InconsistentAuthority);
                }
                if sources.contains_key(id) {
                    continue;
                } // already proven requester
                if sources.len() >= limits.nodes {
                    return Err(Error::BudgetExceeded);
                }
                let dir = children.child(&name)?.ok_or(Error::InconsistentAuthority)?;
                let source = read_source(&dir, id, root, false, &mut budget)?;
                if sources.insert(id.to_owned(), source).is_some() {
                    return Err(Error::InconsistentAuthority);
                }
            }
        }
        // Verify every direct-parent edge and full ancestor/birth observations,
        // including nodes outside the selected subtree. Cached index is unused.
        for (id, source) in &sources {
            let chain = lineage(id, &sources)?;
            if let Some(row) = &source.row {
                let observations = chain
                    .iter()
                    .map(|s| ActorAncestorObservation {
                        actor_id: s.session.id.clone(),
                        session_created_at: s.session.created_at,
                        metadata_version: s.session.metadata_version,
                    })
                    .collect::<Vec<_>>();
                if row.actor.ancestor_observations != observations {
                    return Err(Error::StaleAuthority);
                }
            }
        }
        if !sources.contains_key(subtree) {
            return Err(Error::NotFound);
        }
        if let ActorSnapshotAuthority::LiveActor(fence) = principal.authority() {
            let requester = sources
                .get(&fence.actor_id)
                .ok_or(Error::UnauthorizedScope)?;
            if !live_fence_matches(requester, fence) {
                return Err(Error::UnauthorizedScope);
            }
            if subtree != fence.actor_id
                && !lineage(subtree, &sources)?
                    .iter()
                    .any(|s| s.session.id == fence.actor_id)
            {
                return Err(Error::UnauthorizedScope);
            }
        }
        let mut nodes = Vec::new();
        for (id, source) in &sources {
            if id != subtree
                && !lineage(id, &sources)?
                    .iter()
                    .any(|s| s.session.id == subtree)
            {
                continue;
            }
            let row = source.row.as_ref();
            let activation = row.and_then(|r| r.activation.as_ref());
            nodes.push(PublicActorSnapshotNode {
                actor_id: id.clone(),
                parent_actor_id: source.session.parent_session_id.clone(),
                root_actor_id: root.to_owned(),
                depth: source.session.spawn_depth,
                title: public_title(&source.session.title),
                role: source.session.kind,
                logical_state: row.map(|r| r.actor.state),
                placement_class: activation.and_then(|a| a.placement_ref.as_ref().map(|p| p.class)),
                revision: PublicActorRevision {
                    session_metadata_version: source.session.metadata_version,
                    actor_directory_revision: row.map(|r| r.revision),
                },
                activation: activation.map(|a| PublicActorActivation {
                    activation_id: a.activation_id.clone(),
                    attempt: a.attempt,
                    status: a.status,
                }),
            });
        }
        nodes.sort_by(|a, b| (a.depth, &a.actor_id).cmp(&(b.depth, &b.actor_id)));
        // Read this within the same lifecycle/Task barrier as the projection.
        // Legacy Roots lack the marker and cannot claim continuity. A marker
        // prepared for a failed Project change also yields no cursor until a
        // later successful publication restores matching canonical identity.
        let stream_cursor = cursor_for_root(&root_dir, &sources[root].session, &mut budget)?
            .map(|(cursor, _revision)| cursor);
        let mut snapshot = PublicActorSubtreeSnapshot {
            schema_version: ACTOR_SNAPSHOT_SCHEMA_VERSION,
            root_actor_id: root.to_owned(),
            subtree_actor_id: subtree.to_owned(),
            snapshot_id: String::new(),
            stream_cursor,
            nodes,
        };
        let stream_cursor = snapshot.stream_cursor.take();
        let cursor_growth = stream_cursor
            .as_ref()
            .map_or(0, |cursor| cursor.len().saturating_sub(2));
        // Reserve the exact 68 ASCII bytes of the final identity while hashing
        // the empty-identity view. Cursor is excluded from the view identity:
        // a private-only save can move the stream position without changing
        // the public snapshot. Reserve its serialized growth over `null` too.
        let mut writer = BoundedDigest {
            digest: Sha256::new(),
            bytes: 0,
            max: limits
                .response_bytes
                .saturating_sub(68)
                .saturating_sub(cursor_growth),
        };
        serde_json::to_writer(&mut writer, &snapshot).map_err(|_| Error::BudgetExceeded)?;
        snapshot.snapshot_id = format!("as1-{:x}", writer.digest.finalize());
        snapshot.stream_cursor = stream_cursor;
        Ok(snapshot)
    }

    #[cfg(test)]
    #[test]
    fn private_project_projection_matches_production_typed_precedence() {
        for bytes in [
            r#"{"id":"s","created_at":"2026-09-26T00:00:00Z","model":"m","metadata":{"project_id":"legacy","private":"payload"}}"#,
            r#"{"id":"s","created_at":"2026-09-26T00:00:00Z","model":"m","metadata":{"project_id":"legacy"},"runtime_metadata":{"project_id":"typed","last_run_error":"private"}}"#,
            r#"{"id":"s","created_at":"2026-09-26T00:00:00Z","model":"m","metadata":{"project_id":"legacy"},"runtime_metadata":{"project_id":null}}"#,
            r#"{"id":"s","created_at":"2026-09-26T00:00:00Z","model":"m","metadata":{"project_id":"legacy"},"runtime_metadata":{"project_id":""}}"#,
        ] {
            let source = decode::<Identity>(bytes.as_bytes()).unwrap().session();
            // Add required/default full Session fields via a real Session wire.
            let mut full = serde_json::to_value(Session::new("s", "m")).unwrap();
            for (key, value) in serde_json::from_str::<serde_json::Value>(bytes)
                .unwrap()
                .as_object()
                .unwrap()
            {
                full[key] = value.clone();
            }
            let full: Session = serde_json::from_value(full).unwrap();
            assert_eq!(source.project_id_meta(), full.project_id_meta());
        }
    }
}
