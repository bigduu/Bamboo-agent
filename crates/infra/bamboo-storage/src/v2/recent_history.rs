//! Process-local copy of a bounded visible tail from the committed Main file.

use std::collections::VecDeque;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex};

use bamboo_domain::session::history::{
    history_page_start, is_hidden_from_ui, HistoryScope, SessionHistoryWindow,
    RECENT_HISTORY_MESSAGE_LIMIT,
};
use bamboo_domain::{Message, Session};

use super::compact_main::CompactMainAuthority;
use super::{validate_session_id, SessionStoreV2};

const MAX_SESSION_CACHE_BYTES: usize = 2 * 1024 * 1024;
const MAX_HISTORY_CACHE_BYTES: usize = 16 * 1024 * 1024;
const MAX_CACHED_SESSIONS: usize = 64;

// Each Store verifies its own canonical path, lifetime, sidecar and proof.
// All Stores share one process-local budget.
static RECENT_HISTORY: LazyLock<RecentHistoryCache> = LazyLock::new(RecentHistoryCache::default);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct MainVersion {
    volume: u64,
    file_id: [u8; 16],
    len: u64,
    modified: (i64, i64),
    changed: (i64, i64),
}

#[cfg(unix)]
async fn main_version(path: &Path) -> Option<MainVersion> {
    use std::os::unix::fs::MetadataExt;
    let metadata = tokio::fs::symlink_metadata(path).await.ok()?;
    metadata.is_file().then(|| MainVersion {
        volume: metadata.dev(),
        file_id: u128::from(metadata.ino()).to_le_bytes(),
        len: metadata.len(),
        modified: (metadata.mtime(), metadata.mtime_nsec()),
        changed: (metadata.ctime(), metadata.ctime_nsec()),
    })
}

#[cfg(windows)]
async fn main_version(path: &Path) -> Option<MainVersion> {
    use std::os::windows::fs::OpenOptionsExt;
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::{
        FileBasicInfo, FileIdInfo, GetFileInformationByHandleEx, FILE_ATTRIBUTE_REPARSE_POINT,
        FILE_BASIC_INFO, FILE_FLAG_OPEN_REPARSE_POINT, FILE_ID_INFO,
    };

    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT)
            .open(path)
            .ok()?;
        let metadata = file.metadata().ok()?;
        if !metadata.is_file() {
            return None;
        }
        let mut identity = FILE_ID_INFO::default();
        let mut times = FILE_BASIC_INFO::default();
        // The handle is live and each output buffer matches its information class.
        let (identity_ok, times_ok) = unsafe {
            (
                GetFileInformationByHandleEx(
                    file.as_raw_handle().cast(),
                    FileIdInfo,
                    (&mut identity as *mut FILE_ID_INFO).cast(),
                    size_of::<FILE_ID_INFO>() as u32,
                ),
                GetFileInformationByHandleEx(
                    file.as_raw_handle().cast(),
                    FileBasicInfo,
                    (&mut times as *mut FILE_BASIC_INFO).cast(),
                    size_of::<FILE_BASIC_INFO>() as u32,
                ),
            )
        };
        if identity_ok == 0
            || times_ok == 0
            || times.FileAttributes & FILE_ATTRIBUTE_REPARSE_POINT != 0
        {
            return None;
        }
        Some(MainVersion {
            volume: identity.VolumeSerialNumber,
            file_id: identity.FileId.Identifier,
            len: metadata.len(),
            modified: (times.LastWriteTime, 0),
            changed: (times.ChangeTime, 0),
        })
    })
    .await
    .ok()
    .flatten()
}

#[cfg(not(any(unix, windows)))]
async fn main_version(_path: &Path) -> Option<MainVersion> {
    None
}

struct BoundedBytes {
    bytes: Vec<u8>,
    limit: usize,
}

impl Write for BoundedBytes {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err(io::Error::other("history window exceeds its byte budget"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Store encoded bytes so the byte accounting includes arbitrary metadata and
/// tool results without holding a much larger deserialized Session.
#[derive(Debug)]
struct CachedHistory {
    version: MainVersion,
    authority: Box<[u8]>,
    messages: Box<[u8]>,
    total_message_count: usize,
}

impl CachedHistory {
    fn new(
        version: MainVersion,
        authority: &CompactMainAuthority,
        messages: &[impl serde::Serialize],
        total_message_count: usize,
    ) -> Option<Self> {
        let authority = serde_json::to_vec(authority).ok()?.into_boxed_slice();
        let mut encoded = BoundedBytes {
            bytes: Vec::new(),
            limit: MAX_SESSION_CACHE_BYTES.checked_sub(authority.len())?,
        };
        serde_json::to_writer(&mut encoded, messages).ok()?;
        Some(Self {
            version,
            authority,
            messages: encoded.bytes.into_boxed_slice(),
            total_message_count,
        })
    }

    fn bytes(&self) -> usize {
        self.authority.len() + self.messages.len()
    }

    fn authority_session(&self) -> io::Result<Session> {
        serde_json::from_slice::<CompactMainAuthority>(&self.authority)
            .map(CompactMainAuthority::into_snapshot_session)
            .map_err(io::Error::other)
    }

    fn window(&self, runtime: Session) -> io::Result<SessionHistoryWindow> {
        Ok(SessionHistoryWindow {
            scope: HistoryScope::from(&runtime),
            messages: serde_json::from_slice(&self.messages).map_err(io::Error::other)?,
            total_message_count: self.total_message_count,
            compression_events: runtime.compression_events,
            metadata: runtime.metadata,
        })
    }
}

#[derive(Debug, Default)]
struct CacheState {
    sessions: VecDeque<(PathBuf, Arc<CachedHistory>)>,
    bytes: usize,
}

#[derive(Debug, Default)]
struct RecentHistoryCache {
    state: Mutex<CacheState>,
}

impl RecentHistoryCache {
    fn contains(&self, path: &Path) -> bool {
        self.state
            .lock()
            .unwrap()
            .sessions
            .iter()
            .any(|(key, _)| key == path)
    }

    fn get(&self, path: &Path, version: MainVersion) -> Option<Arc<CachedHistory>> {
        let mut state = self.state.lock().unwrap();
        let index = state.sessions.iter().position(|(key, _)| key == path)?;
        let (key, cached) = state.sessions.remove(index)?;
        if cached.version != version {
            state.bytes -= cached.bytes();
            return None;
        }
        state.sessions.push_back((key, cached.clone()));
        Some(cached)
    }

    fn put(&self, path: &Path, cached: Option<CachedHistory>) {
        let mut state = self.state.lock().unwrap();
        if let Some(index) = state.sessions.iter().position(|(key, _)| key == path) {
            if let Some((_, previous)) = state.sessions.remove(index) {
                state.bytes -= previous.bytes();
            }
        }
        if let Some(cached) = cached {
            state.bytes += cached.bytes();
            state
                .sessions
                .push_back((path.to_path_buf(), Arc::new(cached)));
            while state.bytes > MAX_HISTORY_CACHE_BYTES
                || state.sessions.len() > MAX_CACHED_SESSIONS
            {
                let (_, oldest) = state.sessions.pop_front().expect("cache is over its bound");
                state.bytes -= oldest.bytes();
            }
        }
    }
}

impl SessionStoreV2 {
    /// Typical full saves already own the physical Session writer guard and
    /// have committed both canonical files. Refresh only previously read tails.
    pub(super) async fn publish_recent_history(&self, path: &Path, session: &Session) {
        if !RECENT_HISTORY.contains(path) {
            return;
        }
        let cached = match (
            main_version(path).await,
            CompactMainAuthority::from_session(session),
        ) {
            (Some(version), Ok(authority)) => {
                let visible: Vec<&Message> = session
                    .messages
                    .iter()
                    .filter(|message| !is_hidden_from_ui(message))
                    .collect();
                let start =
                    history_page_start(&visible, visible.len(), RECENT_HISTORY_MESSAGE_LIMIT);
                CachedHistory::new(version, &authority, &visible[start..], visible.len())
            }
            _ => None,
        };
        RECENT_HISTORY.put(path, cached);
    }

    pub(super) async fn read_recent_history(
        &self,
        id: &str,
    ) -> io::Result<Option<SessionHistoryWindow>> {
        validate_session_id(id)?;
        let _lifecycle = self.lock_session_lifecycle_shared().await?;
        let _task = self.lock_runtime_task_sidecar_shared().await?;
        // Unknown GET targets must not create persistent per-Session locks.
        let Some(indexed_path) = self.session_json_path(id).await? else {
            self.ensure_no_unindexed_root(id).await?;
            return Ok(None);
        };
        let _writer = self.acquire_session_maintenance_lock(id).await?;
        let Some(path) = self.session_json_path(id).await? else {
            RECENT_HISTORY.put(&indexed_path, None);
            self.ensure_no_unindexed_root(id).await?;
            return Ok(None);
        };
        let version = main_version(&path).await;
        if let Some(cached) = version.and_then(|version| RECENT_HISTORY.get(&path, version)) {
            let main = cached.authority_session()?;
            if !self.session_lifetime_is_live(&main).await? {
                RECENT_HISTORY.put(&path, None);
                return Ok(None);
            }
            if let Some(runtime) = self.read_checked_runtime_sidecar(id, &main).await? {
                // A filesystem replacement outside this Store's normal write path
                // retires the projection even when the index row did not change.
                if main_version(&path).await == version {
                    return cached.window(runtime).map(Some);
                }
            }
        }

        #[cfg(test)]
        self.recent_history_cold_reads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let (main, authority) = match Self::load_session_main_at(&path).await {
            Ok(result) => result,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                RECENT_HISTORY.put(&path, None);
                self.ensure_no_unindexed_root(id).await?;
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        let authority = authority.or_else(|| CompactMainAuthority::from_session(&main).ok());
        let Some(session) = self.finish_session_read(id, main).await? else {
            RECENT_HISTORY.put(&path, None);
            return Ok(None);
        };
        let window = SessionHistoryWindow::from_session(session, RECENT_HISTORY_MESSAGE_LIMIT);
        let cached = if let Some(version) = version {
            if main_version(&path).await == Some(version) {
                authority.and_then(|authority| {
                    CachedHistory::new(
                        version,
                        &authority,
                        &window.messages,
                        window.total_message_count,
                    )
                })
            } else {
                None
            }
        } else {
            None
        };
        RECENT_HISTORY.put(&path, cached);
        Ok(Some(window))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use bamboo_domain::storage::Storage;

    use super::*;

    #[tokio::test]
    async fn recent_history_uses_durable_saves_and_invalidates_across_stores() -> io::Result<()> {
        let home = tempfile::tempdir()?;
        let store = SessionStoreV2::new(home.path().to_path_buf()).await?;
        assert!(store
            .load_recent_session_history("absent-history")
            .await?
            .is_none());
        assert!(!store.session_write_lock_path("absent-history").exists());

        let id = "recent-history";
        let mut draft = Session::new(id, "model");
        for index in 0..2_101 {
            draft.add_message(Message::user(format!("original {index}")));
        }
        let mut hidden = Message::user("internal-only");
        hidden.metadata = Some(serde_json::json!({"hidden_from_ui": true}));
        draft.add_message(hidden);
        store.save_session(&draft).await?;

        let cold_reads = &store.recent_history_cold_reads;
        let first = store.load_recent_session_history(id).await?.unwrap();
        assert_eq!(first.total_message_count, 2_101);
        assert_eq!(first.messages.len(), RECENT_HISTORY_MESSAGE_LIMIT);
        assert_eq!(first.messages.last().unwrap().content, "original 2100");
        assert_eq!(cold_reads.load(Ordering::Relaxed), 1);
        let second = store.load_recent_session_history(id).await?.unwrap();
        assert_eq!(second.messages[0].id, first.messages[0].id);
        assert_eq!(cold_reads.load(Ordering::Relaxed), 1);

        let mut committed = store.load_session(id).await?.unwrap();
        committed.messages[2_100].compressed = true;
        committed.add_message(Message::assistant("locally committed", None));
        store.save_session(&committed).await?;
        let refreshed = store.load_recent_session_history(id).await?.unwrap();
        assert_eq!(refreshed.total_message_count, 2_102);
        assert!(refreshed.messages[98].compressed);
        assert_eq!(refreshed.messages[98].content, "original 2100");
        assert_eq!(refreshed.messages[99].content, "locally committed");
        assert_eq!(cold_reads.load(Ordering::Relaxed), 1);

        let mut runtime = store.load_runtime_control_plane(id).await?.unwrap();
        runtime
            .metadata
            .insert("goal.state".into(), "{\"status\":\"active\"}".into());
        store.save_runtime_state(&runtime).await?;
        let with_runtime = store.load_recent_session_history(id).await?.unwrap();
        assert_eq!(
            with_runtime.metadata["goal.state"],
            "{\"status\":\"active\"}"
        );
        assert_eq!(cold_reads.load(Ordering::Relaxed), 1);

        let second_store = SessionStoreV2::new(home.path().to_path_buf()).await?;
        let mut external = second_store.load_session(id).await?.unwrap();
        external.add_message(Message::assistant("externally committed", None));
        second_store.save_session(&external).await?;
        let reloaded = store.load_recent_session_history(id).await?.unwrap();
        assert_eq!(reloaded.total_message_count, 2_103);
        assert_eq!(
            reloaded.messages.last().unwrap().content,
            "externally committed"
        );
        assert_eq!(cold_reads.load(Ordering::Relaxed), 1);

        // Simulate an independent process applying Main without having access
        // to this process-local cache. The old stamp must fail its comparison.
        external.add_message(Message::assistant("new Main outside the cache", None));
        let main = store.session_json_path(id).await?.unwrap();
        super::super::durable_atomic_write(
            &main,
            &super::super::compact_main::serialize_main(&external)?,
        )
        .await?;
        let reloaded = store.load_recent_session_history(id).await?.unwrap();
        assert_eq!(reloaded.total_message_count, 2_104);
        assert_eq!(
            reloaded.messages.last().unwrap().content,
            "new Main outside the cache"
        );
        assert_eq!(cold_reads.load(Ordering::Relaxed), 2);

        let runtime_path = store.runtime_json_path(id).await?.unwrap();
        tokio::fs::write(runtime_path, b"{ incomplete runtime").await?;
        let error = match store.load_recent_session_history(id).await {
            Err(error) => error,
            Ok(_) => panic!("cached Root history must require current runtime authority"),
        };
        assert!(error
            .get_ref()
            .unwrap()
            .is::<bamboo_domain::SessionAuthorityConflict>());
        assert_eq!(cold_reads.load(Ordering::Relaxed), 2);
        Ok(())
    }

    #[test]
    fn recent_history_bounds_bytes_and_evicts_oldest_entries() {
        let version = MainVersion {
            volume: 0,
            file_id: [0; 16],
            len: 0,
            modified: (0, 0),
            changed: (0, 0),
        };
        let authority =
            CompactMainAuthority::from_session(&Session::new("cache", "model")).unwrap();
        let large = Message::user("x".repeat(MAX_SESSION_CACHE_BYTES));
        assert!(CachedHistory::new(version, &authority, &[large], 1).is_none());

        let cache = RecentHistoryCache::default();
        let capacity = MAX_HISTORY_CACHE_BYTES / MAX_SESSION_CACHE_BYTES;
        for index in 0..=capacity {
            cache.put(
                Path::new(&index.to_string()),
                Some(CachedHistory {
                    version,
                    authority: Box::new([]),
                    messages: vec![0; MAX_SESSION_CACHE_BYTES].into_boxed_slice(),
                    total_message_count: 1,
                }),
            );
        }
        assert!(!cache.contains(Path::new("0")));
        assert!(cache.contains(Path::new(&capacity.to_string())));
        let state = cache.state.lock().unwrap();
        assert_eq!(state.sessions.len(), capacity);
        assert_eq!(state.bytes, MAX_HISTORY_CACHE_BYTES);
    }
}
