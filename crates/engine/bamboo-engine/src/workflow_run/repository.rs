use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Weak};

use async_trait::async_trait;
use bamboo_domain::{WorkflowRunEvent, WorkflowRunSnapshot};
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

#[async_trait]
pub trait WorkflowRunRepository: Send + Sync {
    async fn create(
        &self,
        snapshot: &WorkflowRunSnapshot,
        event: &WorkflowRunEvent,
    ) -> io::Result<()>;
    async fn commit(
        &self,
        snapshot: &WorkflowRunSnapshot,
        event: &WorkflowRunEvent,
    ) -> io::Result<()>;
    async fn load(&self, run_id: &str) -> io::Result<Option<WorkflowRunSnapshot>>;
    async fn events_since(&self, run_id: &str, sequence: u64) -> io::Result<Vec<WorkflowRunEvent>>;
    async fn list_run_ids(&self) -> io::Result<Vec<String>>;
}

/// One directory per run: a compact atomic snapshot plus an append-only JSONL
/// event journal. Each event is synced before the corresponding snapshot is
/// replaced, and callers publish only after `commit` returns.
pub struct FileWorkflowRunRepository {
    root: PathBuf,
    locks: DashMap<String, Weak<Mutex<()>>>,
}

impl FileWorkflowRunRepository {
    pub fn new(root: PathBuf) -> io::Result<Self> {
        std::fs::create_dir_all(&root)?;
        Ok(Self {
            root,
            locks: DashMap::new(),
        })
    }

    fn validate_id(run_id: &str) -> io::Result<()> {
        if run_id.is_empty()
            || run_id.len() > 128
            || !run_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid run id",
            ));
        }
        Ok(())
    }

    fn run_dir(&self, run_id: &str) -> io::Result<PathBuf> {
        Self::validate_id(run_id)?;
        Ok(self.root.join(run_id))
    }

    fn lock(&self, run_id: &str) -> Arc<Mutex<()>> {
        self.locks.retain(|_, lock| lock.strong_count() > 0);
        match self.locks.entry(run_id.to_owned()) {
            Entry::Occupied(mut entry) => {
                if let Some(lock) = entry.get().upgrade() {
                    lock
                } else {
                    let lock = Arc::new(Mutex::new(()));
                    entry.insert(Arc::downgrade(&lock));
                    lock
                }
            }
            Entry::Vacant(entry) => {
                let lock = Arc::new(Mutex::new(()));
                entry.insert(Arc::downgrade(&lock));
                lock
            }
        }
    }

    async fn commit_locked(
        &self,
        snapshot: &WorkflowRunSnapshot,
        event: &WorkflowRunEvent,
        create: bool,
    ) -> io::Result<()> {
        if snapshot.run_id != event.run_id || snapshot.last_sequence != event.sequence {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "snapshot/event sequence mismatch",
            ));
        }
        let dir = self.run_dir(&snapshot.run_id)?;
        if create {
            match tokio::fs::create_dir(&dir).await {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                    return Err(io::Error::new(io::ErrorKind::AlreadyExists, "run exists"));
                }
                Err(error) => return Err(error),
            }
        } else if !tokio::fs::try_exists(&dir).await? {
            return Err(io::Error::new(io::ErrorKind::NotFound, "run missing"));
        }

        // Serialize both records before mutating durable state. The staged
        // snapshot is fsynced first but remains invisible until the matching
        // journal record is durable.
        let event_bytes = serde_json::to_vec(event).map_err(io::Error::other)?;
        let snapshot_bytes = serde_json::to_vec(snapshot).map_err(io::Error::other)?;
        let temporary = dir.join(format!(".snapshot-{}.tmp", event.sequence));
        let mut file = tokio::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)
            .await?;
        if let Err(error) = async {
            file.write_all(&snapshot_bytes).await?;
            file.sync_all().await
        }
        .await
        {
            let _ = tokio::fs::remove_file(&temporary).await;
            return Err(error);
        }
        drop(file);

        let journal_path = dir.join("journal.jsonl");
        let mut journal = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&journal_path)
            .await?;
        journal.write_all(&event_bytes).await?;
        journal.write_all(b"\n").await?;
        journal.sync_all().await?;
        atomic_replace(&temporary, &dir.join("snapshot.json")).await?;
        sync_directory(&dir).await?;
        Ok(())
    }

    async fn recover_snapshot(&self, run_id: &str) -> io::Result<Option<WorkflowRunSnapshot>> {
        let dir = self.run_dir(run_id)?;
        repair_journal_tail(&dir.join("journal.jsonl")).await?;
        let snapshot_path = dir.join("snapshot.json");
        let current = match tokio::fs::read(&snapshot_path).await {
            Ok(bytes) => Some(
                serde_json::from_slice::<WorkflowRunSnapshot>(&bytes).map_err(io::Error::other)?,
            ),
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(error) => return Err(error),
        };
        let journal = self.events_since(run_id, 0).await?;
        let journal_sequence = journal.last().map_or(0, |event| event.sequence);
        let snapshot_sequence = current
            .as_ref()
            .map_or(0, |snapshot| snapshot.last_sequence);
        if journal_sequence == snapshot_sequence {
            // A staged snapshot without a journal record was never committed.
            let stale = dir.join(format!(".snapshot-{}.tmp", journal_sequence + 1));
            let _ = tokio::fs::remove_file(stale).await;
            return Ok(current);
        }
        if journal_sequence < snapshot_sequence || journal_sequence != snapshot_sequence + 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "workflow journal/snapshot sequence divergence",
            ));
        }
        let temporary = dir.join(format!(".snapshot-{journal_sequence}.tmp"));
        let bytes = tokio::fs::read(&temporary).await.map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("journal is ahead but staged snapshot is missing: {error}"),
            )
        })?;
        let recovered: WorkflowRunSnapshot =
            serde_json::from_slice(&bytes).map_err(io::Error::other)?;
        if recovered.run_id != run_id || recovered.last_sequence != journal_sequence {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "staged workflow snapshot does not match journal",
            ));
        }
        atomic_replace(&temporary, &snapshot_path).await?;
        sync_directory(&dir).await?;
        Ok(Some(recovered))
    }
}

#[async_trait]
impl WorkflowRunRepository for FileWorkflowRunRepository {
    async fn create(
        &self,
        snapshot: &WorkflowRunSnapshot,
        event: &WorkflowRunEvent,
    ) -> io::Result<()> {
        let lock = self.lock(&snapshot.run_id);
        let _guard = lock.lock().await;
        match self.commit_locked(snapshot, event, true).await {
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                let current = self.recover_snapshot(&snapshot.run_id).await?;
                if current.as_ref() == Some(snapshot) {
                    Ok(())
                } else if current.is_none() {
                    let dir = self.run_dir(&snapshot.run_id)?;
                    let mut entries = tokio::fs::read_dir(&dir).await?;
                    let mut removable = true;
                    let mut paths = Vec::new();
                    while let Some(entry) = entries.next_entry().await? {
                        let path = entry.path();
                        if entry.file_name() != "journal.jsonl"
                            || entry.metadata().await?.len() != 0
                        {
                            removable = false;
                            break;
                        }
                        paths.push(path);
                    }
                    if removable {
                        for path in paths {
                            tokio::fs::remove_file(path).await?;
                        }
                        tokio::fs::remove_dir(&dir).await?;
                        self.commit_locked(snapshot, event, true).await
                    } else {
                        Err(error)
                    }
                } else {
                    Err(error)
                }
            }
            result => result,
        }
    }

    async fn commit(
        &self,
        snapshot: &WorkflowRunSnapshot,
        event: &WorkflowRunEvent,
    ) -> io::Result<()> {
        let lock = self.lock(&snapshot.run_id);
        let _guard = lock.lock().await;
        let current = self
            .recover_snapshot(&snapshot.run_id)
            .await?
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "run snapshot missing"))?;
        if current.last_sequence == event.sequence && current == *snapshot {
            return Ok(());
        }
        if current.status.is_terminal() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "terminal workflow run is immutable",
            ));
        }
        if event.sequence != current.last_sequence.saturating_add(1) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "workflow event sequence is not monotonic",
            ));
        }
        self.commit_locked(snapshot, event, false).await
    }

    async fn load(&self, run_id: &str) -> io::Result<Option<WorkflowRunSnapshot>> {
        let lock = self.lock(run_id);
        let _guard = lock.lock().await;
        self.recover_snapshot(run_id).await
    }

    async fn events_since(&self, run_id: &str, sequence: u64) -> io::Result<Vec<WorkflowRunEvent>> {
        let path = self.run_dir(run_id)?.join("journal.jsonl");
        let bytes = match tokio::fs::read(path).await {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(error),
        };
        // A process crash may leave only the final, unterminated JSON fragment.
        // Discard that tail; every newline-terminated record is committed and
        // therefore must parse and form one contiguous run-local sequence.
        let committed = match bytes.iter().rposition(|byte| *byte == b'\n') {
            Some(end) => &bytes[..=end],
            None => &[][..],
        };
        let text = std::str::from_utf8(committed)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        let mut result = Vec::new();
        for (expected, line) in (1_u64..).zip(text.lines().filter(|line| !line.trim().is_empty())) {
            let event: WorkflowRunEvent = serde_json::from_str(line)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
            if event.run_id != run_id || event.sequence != expected {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "workflow journal identity or sequence divergence",
                ));
            }
            if event.sequence > sequence {
                result.push(event);
            }
        }
        Ok(result)
    }

    async fn list_run_ids(&self) -> io::Result<Vec<String>> {
        let mut entries = tokio::fs::read_dir(&self.root).await?;
        let mut ids = Vec::new();
        while let Some(entry) = entries.next_entry().await? {
            if entry.file_type().await?.is_dir() {
                if let Some(id) = entry.file_name().to_str() {
                    if Self::validate_id(id).is_ok() {
                        ids.push(id.to_owned());
                    }
                }
            }
        }
        ids.sort();
        Ok(ids)
    }
}

#[cfg(unix)]
async fn sync_directory(path: &Path) -> io::Result<()> {
    let file = tokio::fs::File::open(path).await?;
    file.sync_all().await
}

#[cfg(not(unix))]
async fn sync_directory(_path: &Path) -> io::Result<()> {
    Ok(())
}

async fn atomic_replace(from: &Path, to: &Path) -> io::Result<()> {
    match tokio::fs::rename(from, to).await {
        Ok(()) => Ok(()),
        Err(_error) if cfg!(windows) && tokio::fs::try_exists(to).await? => {
            tokio::fs::remove_file(to).await?;
            tokio::fs::rename(from, to).await
        }
        Err(error) => Err(error),
    }
}

async fn repair_journal_tail(path: &Path) -> io::Result<()> {
    let bytes = match tokio::fs::read(path).await {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    let committed_len = bytes
        .iter()
        .rposition(|byte| *byte == b'\n')
        .map_or(0, |index| index + 1);
    if committed_len < bytes.len() {
        let file = tokio::fs::OpenOptions::new().write(true).open(path).await?;
        file.set_len(committed_len as u64).await?;
        file.sync_all().await?;
    }
    Ok(())
}
