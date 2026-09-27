//! Identity observation only: neither presence nor vacancy is an acting grant.

use super::*;
use std::io::Read;

const MAIN_CAP: usize = 8 * 1024 * 1024;
const SMALL_CAP: usize = 64 * 1024;
const TOTAL_CAP: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BirthIdentity {
    pub id: String,
    pub kind: SessionKind,
    pub root_id: String,
    pub parent_id: Option<String>,
    pub depth: u32,
    pub created_at: DateTime<Utc>,
    pub project: Option<ProjectId>,
    pub authority: SessionAuthorityIdentity,
    pub actor_witnesses: bool,
    pub revocation_seen: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BirthObservation {
    PresentIdentity(BirthIdentity),
    VacantObserved,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CensusError {
    InvalidInput,
    Incomplete,
    Conflict,
    Unsupported,
    BudgetExceeded,
    Storage,
}

type Result<T> = std::result::Result<T, CensusError>;

#[derive(Clone, Copy)]
pub(super) struct Limits {
    pub(super) roots: usize,
    pub(super) probes: usize,
    pub(super) main: usize,
    pub(super) small: usize,
    pub(super) total: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            roots: 4096,
            probes: 16384,
            main: MAIN_CAP,
            small: SMALL_CAP,
            total: TOTAL_CAP,
        }
    }
}
impl Limits {
    fn supported(self) -> bool {
        let max = Self::default();
        self.roots <= max.roots
            && self.probes <= max.probes
            && self.main <= max.main
            && self.small <= max.small
            && self.total <= max.total
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub(super) struct Counts {
    pub(super) roots: usize,
    pub(super) probes: usize,
    pub(super) bytes: usize,
}

struct Collector {
    home: PathBuf,
    limits: Limits,
    counts: Counts,
    #[cfg(test)]
    hook: Option<Arc<canonical_birth_census_tests::ReadHook>>,
}

fn supported<T>(result: io::Result<T>) -> Result<T> {
    result.map_err(|_| CensusError::Unsupported)
}

impl Collector {
    fn probe(&mut self) -> Result<()> {
        self.counts.probes = self
            .counts
            .probes
            .checked_add(1)
            .ok_or(CensusError::BudgetExceeded)?;
        if self.counts.probes > self.limits.probes {
            return Err(CensusError::BudgetExceeded);
        }
        Ok(())
    }

    fn metadata(&mut self, path: &Path) -> Result<Option<std::fs::Metadata>> {
        self.probe()?;
        match std::fs::symlink_metadata(path) {
            Ok(value) => Ok(Some(value)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(_) => Err(CensusError::Storage),
        }
    }

    fn directory(&mut self, path: &Path) -> Result<bool> {
        match self.metadata(path)? {
            None => Ok(false),
            Some(meta) if meta.file_type().is_dir() => Ok(true),
            Some(_) => Err(CensusError::Unsupported),
        }
    }

    fn entries(&mut self, path: &Path) -> Result<std::fs::ReadDir> {
        self.probe()?;
        std::fs::read_dir(path).map_err(|_| CensusError::Storage)
    }

    fn next(&mut self, entries: &mut std::fs::ReadDir) -> Result<Option<std::fs::DirEntry>> {
        self.probe()?;
        entries.next().transpose().map_err(|_| CensusError::Storage)
    }

    fn read(&mut self, path: &Path, cap: usize) -> Result<Option<Vec<u8>>> {
        let Some(meta) = self.metadata(path)? else {
            return Ok(None);
        };
        if !meta.file_type().is_file() {
            return Err(CensusError::Unsupported);
        }
        self.probe()?;
        let mut file = std::fs::File::open(path).map_err(|_| CensusError::Storage)?;
        self.probe()?;
        if !file.metadata().map_err(|_| CensusError::Storage)?.is_file() {
            return Err(CensusError::Unsupported);
        }
        let mut bytes = Vec::new();
        let mut chunk = [0u8; 16 * 1024];
        loop {
            let remaining = self
                .limits
                .total
                .checked_sub(self.counts.bytes)
                .ok_or(CensusError::BudgetExceeded)?;
            let file_remaining = cap
                .checked_add(1)
                .and_then(|n| n.checked_sub(bytes.len()))
                .ok_or(CensusError::BudgetExceeded)?;
            let take = chunk.len().min(remaining).min(file_remaining);
            if take == 0 {
                return Err(CensusError::BudgetExceeded);
            }
            let count = file
                .read(&mut chunk[..take])
                .map_err(|_| CensusError::Storage)?;
            self.counts.bytes = self
                .counts
                .bytes
                .checked_add(count)
                .ok_or(CensusError::BudgetExceeded)?;
            #[cfg(test)]
            if let Some(hook) = &self.hook {
                hook.read(path, count);
            }
            if count == 0 {
                return Ok(Some(bytes));
            }
            bytes.extend_from_slice(&chunk[..count]);
            if bytes.len() > cap {
                return Err(CensusError::BudgetExceeded);
            }
        }
    }

    fn candidate(
        &mut self,
        path: &Path,
        root: &str,
        kind: SessionKind,
        found: &mut Option<(PathBuf, String, SessionKind)>,
    ) -> Result<()> {
        if self.directory(path)? {
            if found.is_some() {
                return Err(CensusError::Conflict);
            }
            *found = Some((path.to_path_buf(), root.to_owned(), kind));
        }
        Ok(())
    }

    fn locate(&mut self, id: &str) -> Result<Option<(PathBuf, String, SessionKind)>> {
        let sessions = self.home.join("sessions");
        if !self.directory(&sessions)? {
            return Ok(None);
        }
        let mut found = None;
        self.candidate(&sessions.join(id), id, SessionKind::Root, &mut found)?;
        let mut entries = self.entries(&sessions)?;
        while let Some(entry) = self.next(&mut entries)? {
            self.counts.roots = self
                .counts
                .roots
                .checked_add(1)
                .ok_or(CensusError::BudgetExceeded)?;
            if self.counts.roots > self.limits.roots {
                return Err(CensusError::BudgetExceeded);
            }
            let meta = self
                .metadata(&entry.path())?
                .ok_or(CensusError::Unsupported)?;
            if meta.file_type().is_symlink() {
                return Err(CensusError::Unsupported);
            }
            if meta.file_type().is_file() {
                continue;
            }
            if !meta.file_type().is_dir() {
                return Err(CensusError::Unsupported);
            }
            let name = entry.file_name();
            let root = name.to_str().ok_or(CensusError::Unsupported)?;
            supported(validate_session_id(root))?;
            let children = entry.path().join("children");
            if self.directory(&children)? {
                self.candidate(&children.join(id), root, SessionKind::Child, &mut found)?;
            }
        }
        Ok(found)
    }

    fn inventory(&mut self, directory: &Path, kind: SessionKind) -> Result<()> {
        let mut entries = self.entries(directory)?;
        while let Some(entry) = self.next(&mut entries)? {
            let name = entry.file_name();
            let name = name.to_str().ok_or(CensusError::Unsupported)?;
            let meta = self
                .metadata(&entry.path())?
                .ok_or(CensusError::Unsupported)?;
            let valid = match name {
                "session.json"
                | RUNTIME_SIDECAR_FILE
                | "actor-authority.json"
                | "actor-authority.initialized.json"
                | SEARCH_INDEX_REVISION_FILE
                | TOKEN_USAGE_FILE => meta.file_type().is_file(),
                "attachments" => meta.file_type().is_dir(),
                "children" => kind == SessionKind::Root && meta.file_type().is_dir(),
                root_context::ROOT_TOOL_AUTHORITY_PROOF_FILE
                | supervisor_proof::SUPERVISOR_PROOF_FILE => false,
                _ => false,
            };
            if !valid {
                return Err(CensusError::Unsupported);
            }
        }
        Ok(())
    }

    fn observe(&mut self, id: &str) -> Result<BirthObservation> {
        let revocations = self.home.join(root_lifetime::ROOT_REVOCATIONS_DIR);
        let revocation = if self.directory(&revocations)? {
            self.read(&revocations.join(format!("{id}.json")), self.limits.small)?
                .map(|bytes| supported(root_lifetime::census_revocation(&bytes, id)))
                .transpose()?
        } else {
            None
        };
        let Some((directory, root, kind)) = self.locate(id)? else {
            return if revocation.is_some() {
                Err(CensusError::Incomplete)
            } else {
                Ok(BirthObservation::VacantObserved)
            };
        };
        self.inventory(&directory, kind)?;
        let main = self.read(&directory.join("session.json"), self.limits.main)?;
        let runtime = self.read(&directory.join(RUNTIME_SIDECAR_FILE), self.limits.main)?;
        let (Some(main_bytes), Some(runtime)) = (main, runtime) else {
            return Err(CensusError::Incomplete);
        };
        supported(compact_main::validate_full_main(&main_bytes))?;
        let mut main: Session =
            serde_json::from_slice(&main_bytes).map_err(|_| CensusError::Unsupported)?;
        let mut side: Session =
            serde_json::from_slice(&runtime).map_err(|_| CensusError::Unsupported)?;
        // Project parsing must precede ActorSession's compatibility trimming.
        let project = supported(compact_main::census_project(&main_bytes))?;
        let side_project = supported(compact_main::census_project(&runtime))?;
        for session in [&mut main, &mut side] {
            if kind == SessionKind::Root && session.root_session_id.is_empty() {
                session.root_session_id = id.to_owned();
            }
            if session.id != id
                || session.kind != kind
                || session.root_session_id != root
                || session.authority_identity != SessionAuthorityIdentity::Ordinary
                || session.supervisor_management.is_some()
                || session.root_orchestration_only
            {
                return Err(CensusError::Unsupported);
            }
            bamboo_domain::ActorSession::from_session(session)
                .map_err(|_| CensusError::Unsupported)?;
        }
        if main.created_at != side.created_at
            || main.parent_session_id != side.parent_session_id
            || main.spawn_depth != side.spawn_depth
            || project != side_project
        {
            return Err(CensusError::Unsupported);
        }
        let record = self.read(&directory.join("actor-authority.json"), self.limits.small)?;
        let marker = self.read(
            &directory.join("actor-authority.initialized.json"),
            self.limits.small,
        )?;
        let actor_witnesses = match (record, marker) {
            (None, None) => false,
            (Some(record), Some(marker)) => {
                supported(actor_directory::validate_census_witnesses(
                    &record,
                    &marker,
                    &main,
                    project.as_ref(),
                ))?;
                true
            }
            _ => return Err(CensusError::Incomplete),
        };
        Ok(BirthObservation::PresentIdentity(BirthIdentity {
            id: main.id,
            kind,
            root_id: root,
            parent_id: main.parent_session_id,
            depth: main.spawn_depth,
            created_at: main.created_at,
            project,
            authority: main.authority_identity,
            actor_witnesses,
            revocation_seen: revocation.is_some(),
        }))
    }
}

impl SessionStoreV2 {
    /// No current permission/lease/lineage proof and no durable vacancy grant.
    pub(crate) async fn canonical_birth_census(&self, id: &str) -> Result<BirthObservation> {
        self.census_with_limits(id, Limits::default()).await
    }

    pub(super) async fn census_with_limits(
        &self,
        id: &str,
        limits: Limits,
    ) -> Result<BirthObservation> {
        validate_session_id(id).map_err(|_| CensusError::InvalidInput)?;
        if !limits.supported() {
            return Err(CensusError::InvalidInput);
        }
        let lifecycle = self
            .lock_session_lifecycle_shared()
            .await
            .map_err(|_| CensusError::Storage)?;
        let task = self
            .lock_runtime_task_sidecar_shared()
            .await
            .map_err(|_| CensusError::Storage)?;
        let session = self
            .acquire_session_maintenance_lock(id)
            .await
            .map_err(|_| CensusError::Storage)?;
        let guards = DefaultWriterGuards::shared(lifecycle, task, session);
        let id = id.to_owned();
        let mut collector = Collector {
            home: self.bamboo_home_dir.clone(),
            limits,
            counts: Counts::default(),
            #[cfg(test)]
            hook: self.census_read_hook.lock().unwrap().clone(),
        };
        #[cfg(test)]
        if let Some(hook) = &collector.hook {
            hook.attach(&guards);
        }
        Self::default_writer_job(&guards, move || {
            #[cfg(test)]
            if let Some(hook) = &collector.hook {
                hook.started();
            }
            let result = collector.observe(&id);
            #[cfg(test)]
            if let Some(hook) = &collector.hook {
                hook.finished(collector.counts);
            }
            Ok(result)
        })
        .await
        .map_err(|_| CensusError::Storage)?
    }
}
