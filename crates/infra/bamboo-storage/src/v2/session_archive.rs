//! Idle maintenance for the Main encoding. Both representations live at the
//! canonical path, so the index and runtime authority never need a second
//! commit to discover the winning representation.

use tokio::io::AsyncReadExt;

use super::session_version_migration::Pair;
use super::*;

pub const DEFAULT_SESSION_ARCHIVE_IDLE_DAYS: u16 = 30;
const MIN_MAIN_BYTES: usize = 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionArchiveOperation {
    Preview,
    Archive,
    AutomaticArchive,
    Restore,
}

#[derive(Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum SessionArchiveOutcome {
    Eligible {
        original_bytes: usize,
        archived_bytes: usize,
    },
    Archived {
        original_bytes: usize,
        archived_bytes: usize,
    },
    AlreadyArchived,
    Restored,
    AlreadyWritable,
    Skipped {
        reason: &'static str,
    },
}

type Outcome = SessionArchiveOutcome;

struct ArchiveSource {
    path: PathBuf,
    pair: Pair,
}

enum Inspection {
    Ready(ArchiveSource),
    Finished(Outcome),
}

fn skipped(reason: &'static str) -> Inspection {
    Inspection::Finished(Outcome::Skipped { reason })
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

async fn regular_metadata(path: &Path) -> io::Result<std::fs::Metadata> {
    let metadata = fs::symlink_metadata(path).await?;
    if !metadata.is_file() {
        return Err(invalid("Session archive source is not a regular file"));
    }
    Ok(metadata)
}

async fn bounded_read(path: &Path) -> io::Result<Vec<u8>> {
    let metadata = regular_metadata(path).await?;
    let limit = cold_main::MAX_ARCHIVE_MAIN_BYTES as u64;
    if metadata.len() > limit {
        return Err(invalid(
            "Session archive source exceeds the file size limit",
        ));
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    fs::File::open(path)
        .await?
        .take(limit + 1)
        .read_to_end(&mut bytes)
        .await?;
    if bytes.len() as u64 > limit {
        return Err(invalid(
            "Session archive source grew beyond the file size limit",
        ));
    }
    Ok(bytes)
}

impl SessionStoreV2 {
    /// Index candidates only. Admission rechecks the current files and writer
    /// lifetime, so an old in-memory index cannot authorize a replacement.
    pub async fn session_archive_candidates(&self, cutoff: DateTime<Utc>) -> Vec<String> {
        let index = self.index.read().await;
        let mut entries: Vec<_> = index
            .sessions
            .values()
            .filter(|entry| {
                entry.kind == SessionKind::Root && !entry.pinned && entry.updated_at <= cutoff
            })
            .collect();
        entries.sort_unstable_by(|a, b| a.updated_at.cmp(&b.updated_at).then(a.id.cmp(&b.id)));
        entries.into_iter().map(|entry| entry.id.clone()).collect()
    }

    /// Preview and archive use the same candidate checks and codec. Restore
    /// always targets the current archived Main under the same physical fence.
    pub async fn maintain_session_archive(
        &self,
        id: &str,
        cutoff: DateTime<Utc>,
        operation: SessionArchiveOperation,
    ) -> io::Result<Option<SessionArchiveOutcome>> {
        use SessionArchiveOperation::{Preview, Restore};
        validate_session_id(id)?;
        let guards = self.session_maintenance_guards(id).await?;
        self.ensure_no_session_version_migration_locked().await?;
        let Some(inspection) = self.inspect_archive_source(id, cutoff, operation).await? else {
            return Ok(None);
        };
        let source = match inspection {
            Inspection::Finished(outcome) => return Ok(Some(outcome)),
            Inspection::Ready(source) => source,
        };
        if operation == Restore {
            self.write_default_bytes(&source.path, source.pair.main.into_bytes(), &guards)
                .await?;
            return Ok(Some(Outcome::Restored));
        }
        drop(guards);

        let original_bytes = source.pair.main.len();
        let expected = source.pair.sha256;
        let encoded = tokio::task::spawn_blocking(move || {
            let encoded = cold_main::encode(source.pair.main.as_bytes())?;
            // Verify the exact reader before considering the archive for publication.
            if cold_main::decode(encoded.clone())? != source.pair.main.as_bytes() {
                return Err(invalid(
                    "Session archive round trip differs from its source",
                ));
            }
            Ok::<_, io::Error>(encoded)
        })
        .await??;
        let archived_bytes = encoded.len();
        if archived_bytes * 5 > original_bytes * 4 {
            return Ok(Some(Outcome::Skipped {
                reason: "insufficient_savings",
            }));
        }
        if operation == Preview {
            return Ok(Some(Outcome::Eligible {
                original_bytes,
                archived_bytes,
            }));
        }

        let guards = self.session_maintenance_guards(id).await?;
        self.ensure_no_session_version_migration_locked().await?;
        match self.inspect_archive_source(id, cutoff, operation).await? {
            Some(Inspection::Ready(current)) if current.pair.sha256 == expected => {
                self.write_default_bytes(&current.path, encoded, &guards)
                    .await?;
                Ok(Some(Outcome::Archived {
                    original_bytes,
                    archived_bytes,
                }))
            }
            Some(Inspection::Finished(Outcome::AlreadyArchived)) => {
                Ok(Some(Outcome::AlreadyArchived))
            }
            _ => Ok(Some(Outcome::Skipped {
                reason: "session_changed",
            })),
        }
    }

    async fn inspect_archive_source(
        &self,
        id: &str,
        cutoff: DateTime<Utc>,
        operation: SessionArchiveOperation,
    ) -> io::Result<Option<Inspection>> {
        let relative = Self::root_rel_path(id);
        let directory = self.abs_path_from_rel(&relative);
        for path in [&self.sessions_dir, &directory] {
            match fs::symlink_metadata(path).await {
                Ok(metadata) if metadata.is_dir() => {}
                Ok(_) => return Err(invalid("Session archive directory is not a real directory")),
                Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
                Err(error) => return Err(error),
            }
        }
        let path = directory.join("session.json");
        let metadata = match regular_metadata(&path).await {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        let archived = cold_main::file_is_archived(&mut fs::File::open(&path).await?).await?;
        use SessionArchiveOperation::{Archive, AutomaticArchive, Preview, Restore};
        match (archived, operation) {
            // Scheduled scans and previews observe the encoding without
            // reading every archived history. An explicit retry validates it.
            (true, AutomaticArchive | Preview) => {
                return Ok(Some(Inspection::Finished(Outcome::AlreadyArchived)));
            }
            (false, Restore) => {
                return Ok(self
                    .load_authoritative_root_session(id)
                    .await?
                    .map(|_| Inspection::Finished(Outcome::AlreadyWritable)));
            }
            _ => {}
        }
        if !archived {
            if metadata.len() < MIN_MAIN_BYTES as u64 {
                return Ok(Some(skipped("small_main")));
            }
            if metadata.len() > cold_main::MAX_ARCHIVE_MAIN_BYTES as u64 {
                return Ok(Some(skipped("large_main")));
            }
            // A physical restore refreshes the Main modification time while
            // preserving the conversation's displayed timestamp.
            if DateTime::<Utc>::from(metadata.modified()?) > cutoff {
                return Ok(Some(skipped("recent_main")));
            }
        }
        let stored = bounded_read(&path).await?;
        if !archived && compact_main::validate_full_main(&stored)?.is_none() {
            return Ok(Some(skipped("legacy_main")));
        }
        let main = cold_main::decode_async(stored).await?;
        let runtime = bounded_read(&directory.join(RUNTIME_SIDECAR_FILE)).await?;
        let pair = Pair::new(main, runtime)?;
        let session = self.validate_maintenance_pair(id, &relative, &pair).await?;
        if archived && operation == Archive {
            return Ok(Some(Inspection::Finished(Outcome::AlreadyArchived)));
        }
        if !archived {
            if session.pinned
                || session.updated_at.max(session.created_at) > cutoff
                || !session.authority_identity.is_ordinary()
                || !session.execution_compatibility().execution_allowed
            {
                return Ok(Some(skipped("recent_pinned_or_unsupported_session")));
            }
            // Roots with Child work use additional admission and wake paths.
            // A real empty children directory is also valid for ordinary Roots.
            match fs::symlink_metadata(directory.join("children")).await {
                Ok(metadata) if metadata.is_dir() => {
                    if fs::read_dir(directory.join("children"))
                        .await?
                        .next_entry()
                        .await?
                        .is_some()
                    {
                        return Ok(Some(skipped("has_children")));
                    }
                }
                Ok(_) => return Err(invalid("Session archive children path is not a directory")),
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
            if session.metadata.contains_key("created_by_schedule_id")
                || session.metadata.get("goal.state").is_some_and(|raw| {
                    serde_json::from_str::<serde_json::Value>(raw)
                        .ok()
                        .is_none_or(|goal| {
                            goal.get("status").and_then(|status| status.as_str())
                                != Some("complete")
                        })
                })
                || !self
                    .session_idle_blockers(&session, &relative)
                    .await?
                    .is_empty()
                || bamboo_domain::conversation_compaction_pending_rejection(&session).is_some()
            {
                return Ok(Some(skipped("pending_work")));
            }
        }
        Ok(Some(Inspection::Ready(ArchiveSource { path, pair })))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_domain::Storage;
    use chrono::TimeDelta;

    fn cutoff(now: DateTime<Utc>) -> DateTime<Utc> {
        now - TimeDelta::days(i64::from(DEFAULT_SESSION_ARCHIVE_IDLE_DAYS))
    }

    fn age_file(path: &Path, time: DateTime<Utc>) {
        std::fs::File::options()
            .write(true)
            .open(path)
            .unwrap()
            .set_modified(time.into())
            .unwrap();
    }

    async fn seed(
        store: &SessionStoreV2,
        id: &str,
        timestamp: DateTime<Utc>,
    ) -> (Session, PathBuf) {
        let mut session = Session::new(id, "kept-model");
        session.title = "Kept title".into();
        session.add_message(Message::user("可重复的历史内容\n".repeat(60_000)));
        session.created_at = timestamp;
        session.updated_at = timestamp;
        store.save_session(&session).await.unwrap();
        let main = store.sessions_dir.join(id).join("session.json");
        age_file(&main, timestamp);
        (session, main)
    }

    #[tokio::test]
    async fn preview_restore_and_continue_after_reopening_the_archived_session() {
        use SessionArchiveOperation::{Archive, Preview, Restore};

        let home = tempfile::tempdir().unwrap();
        let store = SessionStoreV2::new(home.path().into()).await.unwrap();
        let now = Utc::now();
        let boundary = cutoff(now);
        let id = "archive-round-trip";
        let (source, main) = seed(&store, id, boundary).await;
        let side = main.with_file_name(RUNTIME_SIDECAR_FILE);
        // Retain an unknown legacy field without going through Session serde.
        let mut plain = fs::read(&main).await.unwrap();
        assert_eq!(plain.pop(), Some(b'}'));
        plain.extend_from_slice(r#","reserved_field":{"string":"原始数据"}}"#.as_bytes());
        fs::write(&main, &plain).await.unwrap();
        age_file(&main, boundary);
        let runtime = fs::read(&side).await.unwrap();

        assert_eq!(store.session_archive_candidates(boundary).await, [id]);
        let Some(Outcome::Eligible {
            original_bytes,
            archived_bytes,
        }) = store
            .maintain_session_archive(id, boundary, Preview)
            .await
            .unwrap()
        else {
            panic!("30-day-old session should be eligible");
        };
        assert_eq!(original_bytes, plain.len());
        assert!(archived_bytes * 5 <= original_bytes * 4);
        assert_eq!(fs::read(&main).await.unwrap(), plain);
        assert_eq!(
            store
                .maintain_session_archive(id, boundary, Archive)
                .await
                .unwrap(),
            Some(Outcome::Archived {
                original_bytes,
                archived_bytes
            }),
        );
        let stored = fs::read(&main).await.unwrap();
        assert!(cold_main::is_archived(&stored));
        assert_eq!(stored.len(), archived_bytes);
        assert_eq!(fs::read(&side).await.unwrap(), runtime);
        drop(store);

        let reopened = SessionStoreV2::new(home.path().into()).await.unwrap();
        let session = reopened.load_session(id).await.unwrap().unwrap();
        assert_eq!(
            session.root_mode_birth_token(),
            source.root_mode_birth_token()
        );
        assert_eq!(
            serde_json::to_value(&session.messages).unwrap(),
            serde_json::to_value(&source.messages).unwrap(),
        );
        assert_eq!(session.model, source.model);
        assert_eq!(
            reopened.get_index_entry(id).await.unwrap().title,
            source.title
        );
        assert_eq!(fs::read(&main).await.unwrap(), stored);
        assert_eq!(
            reopened
                .maintain_session_archive(id, boundary, Archive)
                .await
                .unwrap(),
            Some(Outcome::AlreadyArchived),
        );
        assert_eq!(
            reopened
                .maintain_session_archive(id, boundary, Restore)
                .await
                .unwrap(),
            Some(Outcome::Restored),
        );
        assert_eq!(fs::read(&main).await.unwrap(), plain);
        assert_eq!(
            reopened
                .maintain_session_archive(id, boundary, Preview)
                .await
                .unwrap(),
            Some(Outcome::Skipped {
                reason: "recent_main"
            }),
        );

        age_file(&main, boundary);
        reopened
            .maintain_session_archive(id, boundary, Archive)
            .await
            .unwrap();
        let mut loaded = reopened.load_session(id).await.unwrap().unwrap();
        loaded.add_message(Message::user("continue from the original history"));
        reopened.save_session(&loaded).await.unwrap();
        let written = fs::read(&main).await.unwrap();
        assert!(!cold_main::is_archived(&written));
        let continued = reopened.load_session(id).await.unwrap().unwrap();
        assert_eq!(continued.id, id);
        assert_eq!(continued.messages[0].id, source.messages[0].id);
        assert_eq!(
            continued.messages[1].content,
            "continue from the original history"
        );
    }

    #[tokio::test]
    async fn archive_skips_recent_pinned_and_waiting_sessions() {
        let home = tempfile::tempdir().unwrap();
        let store = SessionStoreV2::new(home.path().into()).await.unwrap();
        let boundary = cutoff(Utc::now());
        for (id, recent, pinned, waiting, expected) in [
            ("recent", true, false, false, "recent_main"),
            (
                "pinned",
                false,
                true,
                false,
                "recent_pinned_or_unsupported_session",
            ),
            ("waiting", false, false, true, "pending_work"),
        ] {
            let time = if recent {
                boundary + TimeDelta::seconds(1)
            } else {
                boundary
            };
            let (mut session, main) = seed(&store, id, time).await;
            session.pinned = pinned;
            if waiting {
                session.set_pending_question(
                    "call".into(),
                    "tool".into(),
                    "waiting".into(),
                    vec!["answer".into()],
                    true,
                );
            }
            session.updated_at = time;
            store.save_session(&session).await.unwrap();
            age_file(&main, time);
            let before = fs::read(&main).await.unwrap();
            assert_eq!(
                store
                    .maintain_session_archive(id, boundary, SessionArchiveOperation::Archive)
                    .await
                    .unwrap(),
                Some(Outcome::Skipped { reason: expected }),
                "{id}",
            );
            assert_eq!(fs::read(&main).await.unwrap(), before);
        }
    }

    #[tokio::test]
    async fn concurrent_restores_are_idempotent_and_corruption_is_preserved() {
        let home = tempfile::tempdir().unwrap();
        let store = SessionStoreV2::new(home.path().into()).await.unwrap();
        let peer = SessionStoreV2::new(home.path().into()).await.unwrap();
        let boundary = cutoff(Utc::now());
        let id = "archived-with-peers";
        let (_, main) = seed(&store, id, boundary).await;
        let plain = fs::read(&main).await.unwrap();
        let archive = SessionArchiveOperation::Archive;
        let restore = SessionArchiveOperation::Restore;
        assert!(matches!(
            store
                .maintain_session_archive(id, boundary, archive)
                .await
                .unwrap(),
            Some(Outcome::Archived { .. })
        ));
        let (a, b) = tokio::join!(
            store.maintain_session_archive(id, boundary, restore),
            peer.maintain_session_archive(id, boundary, restore),
        );
        assert!(matches!(
            (a.unwrap(), b.unwrap()),
            (Some(Outcome::Restored), Some(Outcome::AlreadyWritable))
                | (Some(Outcome::AlreadyWritable), Some(Outcome::Restored))
        ));
        assert_eq!(fs::read(&main).await.unwrap(), plain);

        age_file(&main, boundary);
        store
            .maintain_session_archive(id, boundary, archive)
            .await
            .unwrap();
        let mut compressed = fs::read(&main).await.unwrap();
        compressed.truncate(compressed.len() - 7);
        fs::write(&main, &compressed).await.unwrap();
        assert!(peer.load_session(id).await.is_err());
        for operation in [archive, restore] {
            assert!(
                peer.maintain_session_archive(id, boundary, operation)
                    .await
                    .is_err(),
                "{operation:?} should detect corruption"
            );
        }
        assert!(peer
            .save_session(&Session::new(id, "replacement"))
            .await
            .is_err());
        assert_eq!(fs::read(&main).await.unwrap(), compressed);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn archive_survives_sigkill_at_atomic_replace() {
        use std::os::unix::process::ExitStatusExt;
        use tokio::process::Command;

        const HOME: &str = "BAMBOO_ARCHIVE_TEST_HOME";
        const AFTER: &str = "BAMBOO_ARCHIVE_TEST_AFTER_REPLACE";
        const ID: &str = "archive-process-loss";
        let phase = |after| {
            if after {
                DurableWritePhase::AfterReplace
            } else {
                DurableWritePhase::BeforeReplace
            }
        };
        if let Some(home) = std::env::var_os(HOME) {
            let home = PathBuf::from(home);
            let store = Arc::new(SessionStoreV2::new(home.clone()).await.unwrap());
            let hook = default_actor_context_tests::DefaultWriteHook::install(
                &store,
                "session.json",
                phase(std::env::var_os(AFTER).is_some_and(|value| value == "1")),
                false,
            );
            let writer = tokio::spawn(async move {
                store
                    .maintain_session_archive(
                        ID,
                        cutoff(Utc::now()),
                        SessionArchiveOperation::Archive,
                    )
                    .await
            });
            tokio::task::spawn_blocking(move || hook.wait())
                .await
                .unwrap();
            fs::write(home.join("archive-writer-ready"), b"ready")
                .await
                .unwrap();
            // The parent kills this process before releasing the filesystem job.
            let result = writer.await;
            panic!("archive writer unexpectedly returned: {result:?}");
        }

        for after in [false, true] {
            let home = tempfile::tempdir().unwrap();
            let store = SessionStoreV2::new(home.path().into()).await.unwrap();
            let (source, main) = seed(&store, ID, cutoff(Utc::now()) - TimeDelta::seconds(1)).await;
            let plain = fs::read(&main).await.unwrap();
            drop(store);

            let mut child = Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg("v2::session_archive::tests::archive_survives_sigkill_at_atomic_replace")
                .env(HOME, home.path())
                .env(AFTER, if after { "1" } else { "0" })
                .kill_on_drop(true)
                .spawn()
                .unwrap();
            tokio::time::timeout(Duration::from_secs(30), async {
                while !fs::try_exists(home.path().join("archive-writer-ready"))
                    .await
                    .unwrap()
                {
                    assert!(
                        child.try_wait().unwrap().is_none(),
                        "archive writer exited early"
                    );
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .expect("archive writer did not reach the requested publication phase");
            child.kill().await.unwrap();
            assert_eq!(child.wait().await.unwrap().signal(), Some(9));

            let reopened = SessionStoreV2::new(home.path().into()).await.unwrap();
            let loaded = reopened.load_session(ID).await.unwrap().unwrap();
            assert_eq!(
                loaded.root_mode_birth_token(),
                source.root_mode_birth_token()
            );
            assert_eq!(loaded.messages[0].id, source.messages[0].id);
            let stored = fs::read(&main).await.unwrap();
            assert_eq!(cold_main::is_archived(&stored), after);
            assert_eq!(cold_main::decode(stored).unwrap(), plain);
        }
    }
}
