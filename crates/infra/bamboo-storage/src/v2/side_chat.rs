use super::*;
use serde::de::DeserializeOwned;

pub(super) const SIDE_CHAT_FILE: &str = "side-chat.json";
/// Main-owned marker. The legacy clear path changes it with the transcript.
pub const SIDE_CHAT_CLEAR_EPOCH_KEY: &str = "side_chat_clear_epoch";
pub const SIDE_CHAT_MAX_RECORD_BYTES: usize = 2 * 1024 * 1024;

/// A separate writer lease keeps one side turn running per source even when
/// several server processes share the store. It never holds the source writer.
pub struct SideChatRunLease {
    _file: std::fs::File,
}

impl SessionStoreV2 {
    pub async fn try_lock_side_chat_run(
        &self,
        source_id: &str,
    ) -> io::Result<Option<SideChatRunLease>> {
        validate_session_id(source_id)?;
        let directory = self.bamboo_home_dir.join("side-chat-locks");
        fs::create_dir_all(&directory).await?;
        let path = directory.join(format!("{:x}", Sha256::digest(source_id.as_bytes())));
        tokio::task::spawn_blocking(move || {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(path)?;
            match file.try_lock() {
                Ok(()) => Ok(Some(SideChatRunLease { _file: file })),
                Err(std::fs::TryLockError::WouldBlock) => Ok(None),
                Err(std::fs::TryLockError::Error(error)) => Err(error),
            }
        })
        .await
        .map_err(io::Error::other)?
    }

    /// Observe the authoritative source and its attached record at the same
    /// write boundary used by transcript changes, history clear and deletion.
    /// A replacement is written atomically without rewriting source history.
    pub async fn with_side_chat<T, R>(
        &self,
        source_id: &str,
        update: impl FnOnce(&Session, Option<T>) -> io::Result<(Option<T>, R)>,
    ) -> io::Result<Option<R>>
    where
        T: DeserializeOwned + Serialize,
    {
        validate_session_id(source_id)?;
        if self
            .get_index_entry(source_id)
            .await
            .is_none_or(|entry| entry.kind != SessionKind::Root)
        {
            return Ok(None);
        }
        let lifecycle = self.lock_session_lifecycle_shared().await?;
        let task = self.lock_runtime_task_sidecar_shared().await?;
        let session_write = self.acquire_session_maintenance_lock(source_id).await?;
        let guards = DefaultWriterGuards::shared(lifecycle, task, session_write);
        let Some(source) = self.load_session_unlocked(source_id).await? else {
            return Ok(None);
        };
        if source.kind != SessionKind::Root
            || source.parent_session_id.is_some()
            || !source.authority_identity.is_ordinary()
        {
            return Ok(None);
        }
        let rel = Self::default_writer_rel_path(&source)?;
        let path = self.abs_path_from_rel(&rel).join(SIDE_CHAT_FILE);
        let record = match fs::symlink_metadata(&path).await {
            Ok(meta) if meta.is_file() && meta.len() <= SIDE_CHAT_MAX_RECORD_BYTES as u64 => {
                Some(serde_json::from_slice(&fs::read(&path).await?)?)
            }
            Ok(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Invalid side chat record",
                ))
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(error) => return Err(error),
        };
        let (write, result) = update(&source, record)?;
        if let Some(record) = write {
            let bytes = serde_json::to_vec(&record)?;
            if bytes.len() > SIDE_CHAT_MAX_RECORD_BYTES {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Side chat record is too large",
                ));
            }
            Self::default_writer_job(&guards, move || {
                durable_atomic_write_blocking(&path, &bytes, |_| Ok(()))
            })
            .await?;
        }
        Ok(Some(result))
    }
}
