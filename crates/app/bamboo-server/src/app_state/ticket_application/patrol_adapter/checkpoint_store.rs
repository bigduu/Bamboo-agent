//! Single-writer, CAS Host policy metadata. All actual file I/O lives here,
//! outside orchestration/Ticket locks. Never stores Task/lease/stop/budget facts.

#[cfg(unix)]
use bamboo_orchestration::patrol::checkpoint::{self, MAX_CHECKPOINT_BYTES};
use bamboo_orchestration::patrol::{
    Checkpoint, CheckpointPort, PortError, Scope, StoredCheckpoint,
};
use std::path::Path;

#[cfg(unix)]
mod physical {
    use super::*;
    use fs2::FileExt;
    use std::{
        ffi::CString,
        fs::{File, Metadata, OpenOptions},
        io::{Read, Write},
        os::{
            fd::{AsRawFd, FromRawFd},
            unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt},
        },
        path::{Component, PathBuf},
        sync::atomic::{AtomicU64, Ordering},
    };

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Boundary {
        BeforeRename,
        AfterRename,
        AfterDirectorySync,
    }

    type PublicationFault = Box<dyn Fn(Boundary) -> std::io::Result<()> + Send + Sync>;

    pub struct PatrolCheckpointStore {
        root: PathBuf,
        directory: File,
        writer: File,
        poisoned: bool,
        fault: Option<PublicationFault>,
    }

    impl PatrolCheckpointStore {
        pub fn open(root: &Path) -> Result<Self, PortError> {
            checked_tree(root)?;
            let directory = OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
                .open(root)
                .map_err(|_| PortError::Unavailable)?;
            let writer = open_at(
                &directory,
                "patrol-writer.lock",
                libc::O_RDWR | libc::O_CREAT,
                0o600,
            )
            .map_err(|_| PortError::Unavailable)?;
            if !plain(&writer.metadata().map_err(|_| PortError::Unavailable)?) {
                return Err(PortError::Denied);
            }
            FileExt::try_lock_exclusive(&writer).map_err(|_| PortError::Conflict)?;
            writer
                .sync_all()
                .and_then(|_| directory.sync_all())
                .map_err(|_| PortError::Unavailable)?;
            let store = Self {
                root: root.to_path_buf(),
                directory,
                writer,
                poisoned: false,
                fault: None,
            };
            store.check_root()?;
            Ok(store)
        }

        fn check_root(&self) -> Result<(), PortError> {
            if self.poisoned {
                return Err(PortError::OutcomeUnknown);
            }
            let root = std::fs::symlink_metadata(&self.root).map_err(|_| PortError::Denied)?;
            let held = self
                .directory
                .metadata()
                .map_err(|_| PortError::Unavailable)?;
            if root.file_type().is_symlink() || !root.is_dir() || !same(&root, &held) {
                return Err(PortError::Denied);
            }
            let lock = open_at(&self.directory, "patrol-writer.lock", libc::O_RDONLY, 0)
                .map_err(|_| PortError::Denied)?;
            let observed = lock.metadata().map_err(|_| PortError::Unavailable)?;
            if !plain(&observed)
                || !same(
                    &observed,
                    &self.writer.metadata().map_err(|_| PortError::Unavailable)?,
                )
            {
                return Err(PortError::Denied);
            }
            Ok(())
        }

        fn fault(&self, boundary: Boundary) -> Result<(), PortError> {
            if let Some(fault) = &self.fault {
                fault(boundary).map_err(|_| PortError::Unavailable)?;
            }
            Ok(())
        }

        fn publish(&mut self, name: &str, bytes: &[u8]) -> Result<(), PortError> {
            static SERIAL: AtomicU64 = AtomicU64::new(0);
            let temporary = format!(
                "pending-{}-{}-{}.tmp",
                std::process::id(),
                SERIAL.fetch_add(1, Ordering::Relaxed),
                name
            );
            let mut file = open_at(
                &self.directory,
                &temporary,
                libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL,
                0o600,
            )
            .map_err(|_| PortError::Unavailable)?;
            let before = (|| {
                file.write_all(bytes)
                    .and_then(|_| file.sync_all())
                    .map_err(|_| PortError::Unavailable)?;
                self.fault(Boundary::BeforeRename)?;
                self.check_root()?;
                rename_at(&self.directory, &temporary, name).map_err(|_| PortError::Unavailable)
            })();
            if let Err(error) = before {
                // This exact staging name was created by this call. Never scan
                // or clean another writer's files, including uncertain leftovers.
                unlink_at(&self.directory, &temporary);
                return Err(error);
            }
            // Any failure after rename has an uncertain publication result.
            // Stop this writer; only a fresh reopen/validated load may reconcile.
            if self.fault(Boundary::AfterRename).is_err()
                || self.directory.sync_all().is_err()
                || self.fault(Boundary::AfterDirectorySync).is_err()
                || self.check_root().is_err()
            {
                self.poisoned = true;
                return Err(PortError::OutcomeUnknown);
            }
            Ok(())
        }
    }

    impl CheckpointPort for PatrolCheckpointStore {
        fn load(&mut self, scope: &Scope) -> Result<Option<StoredCheckpoint>, PortError> {
            self.check_root()?;
            let name = format!("{}.json", checkpoint::scope_key(scope)?);
            let file = match open_at(&self.directory, &name, libc::O_RDONLY, 0) {
                Ok(file) => file,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(_) => return Err(PortError::Unavailable),
            };
            let metadata = file.metadata().map_err(|_| PortError::Unavailable)?;
            if !plain(&metadata) || metadata.len() > MAX_CHECKPOINT_BYTES as u64 {
                return Err(PortError::Unavailable);
            }
            let mut bytes = Vec::new();
            file.take(MAX_CHECKPOINT_BYTES as u64 + 1)
                .read_to_end(&mut bytes)
                .map_err(|_| PortError::Unavailable)?;
            let record = checkpoint::decode(scope, &bytes)?;
            self.check_root()?;
            Ok(Some(record))
        }

        fn save(&mut self, expected: Option<u64>, next: &Checkpoint) -> Result<u64, PortError> {
            self.check_root()?;
            let previous = self.load(&next.scope)?;
            if previous.as_ref().map(|r| r.version) != expected {
                return Err(PortError::Conflict);
            }
            if let Some(previous) = previous {
                checkpoint::validate_update(&previous.checkpoint, next)?;
            }
            let version = expected
                .unwrap_or(0)
                .checked_add(1)
                .ok_or(PortError::Unavailable)?;
            let bytes = checkpoint::encode(version, next)?;
            let name = format!("{}.json", checkpoint::scope_key(&next.scope)?);
            self.publish(&name, &bytes)?;
            Ok(version)
        }
    }

    impl Drop for PatrolCheckpointStore {
        fn drop(&mut self) {
            let _ = FileExt::unlock(&self.writer);
        }
    }

    fn same(a: &Metadata, b: &Metadata) -> bool {
        a.dev() == b.dev() && a.ino() == b.ino()
    }
    fn plain(metadata: &Metadata) -> bool {
        metadata.is_file() && metadata.nlink() == 1
    }

    fn checked_tree(path: &Path) -> Result<(), PortError> {
        if !path.is_absolute() {
            return Err(PortError::Denied);
        }
        let mut at = PathBuf::new();
        for part in path.components() {
            match part {
                Component::RootDir | Component::Normal(_) => at.push(part.as_os_str()),
                _ => return Err(PortError::Denied),
            }
            match std::fs::symlink_metadata(&at) {
                Ok(meta) if meta.is_dir() && !meta.file_type().is_symlink() => {}
                Ok(_) => return Err(PortError::Denied),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    let mut builder = std::fs::DirBuilder::new();
                    builder
                        .mode(0o700)
                        .create(&at)
                        .map_err(|_| PortError::Unavailable)?;
                    File::open(at.parent().ok_or(PortError::Denied)?)
                        .and_then(|parent| parent.sync_all())
                        .map_err(|_| PortError::Unavailable)?;
                }
                Err(_) => return Err(PortError::Unavailable),
            }
        }
        Ok(())
    }

    fn name(value: &str) -> std::io::Result<CString> {
        if value.is_empty() || value.contains('/') || value == "." || value == ".." {
            return Err(std::io::Error::other("not a single checkpoint component"));
        }
        CString::new(value).map_err(|_| std::io::Error::other("NUL in component"))
    }

    fn open_at(
        directory: &File,
        component: &str,
        flags: i32,
        mode: libc::mode_t,
    ) -> std::io::Result<File> {
        let component = name(component)?;
        // All components are single names under an owned directory descriptor.
        // No symbolic link is followed; each returned descriptor is owned once.
        let fd = unsafe {
            libc::openat(
                directory.as_raw_fd(),
                component.as_ptr(),
                flags | libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK,
                mode as libc::c_uint,
            )
        };
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(unsafe { File::from_raw_fd(fd) })
    }

    fn rename_at(directory: &File, from: &str, to: &str) -> std::io::Result<()> {
        let (from, to) = (name(from)?, name(to)?);
        if unsafe {
            libc::renameat(
                directory.as_raw_fd(),
                from.as_ptr(),
                directory.as_raw_fd(),
                to.as_ptr(),
            )
        } != 0
        {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    }

    fn unlink_at(directory: &File, component: &str) {
        if let Ok(component) = name(component) {
            let _ = unsafe { libc::unlinkat(directory.as_raw_fd(), component.as_ptr(), 0) };
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use bamboo_orchestration::patrol::{Incident, IncidentId, Phase, Reason};

        fn root() -> tempfile::TempDir {
            tempfile::tempdir().unwrap()
        }
        fn scope() -> Scope {
            Scope {
                project_id: "project".into(),
                work_id: "work".into(),
                assignment_id: "assignment".into(),
                generation: 1,
                authority_epoch: 1,
            }
        }
        fn checkpoint() -> Checkpoint {
            Checkpoint {
                scope: scope(),
                cursor: 4,
                source_revision: 4,
                last_check_ms: 100,
                next_check_ms: 110,
                incident: Some(Incident {
                    id: IncidentId {
                        scope: scope(),
                        progress_sequence: 0,
                    },
                    reason: Reason::EvidenceUnknown,
                    opened_at_ms: 100,
                    opened_cursor: 4,
                    source_revision: 4,
                    attempts: 0,
                    last_diagnostic_cursor: 4,
                    last_diagnostic_evidence_revision: None,
                    last_diagnosis: None,
                    pending_settlement: None,
                    pending_settlement_usage: None,
                    phase: Phase::Open,
                    escalation: None,
                }),
            }
        }
        fn canonical(root: &tempfile::TempDir) -> PathBuf {
            root.path().canonicalize().unwrap()
        }

        #[test]
        fn physical_cas_checkpoint_survives_reopen_and_rejects_stale_write() {
            let root = root();
            let path = canonical(&root);
            let mut writer = PatrolCheckpointStore::open(&path).unwrap();
            let first = checkpoint();
            assert_eq!(writer.save(None, &first), Ok(1));
            assert_eq!(writer.save(None, &first), Err(PortError::Conflict));
            drop(writer);
            let mut restarted = PatrolCheckpointStore::open(&path).unwrap();
            assert_eq!(
                restarted.load(&scope()).unwrap().unwrap(),
                StoredCheckpoint {
                    version: 1,
                    checkpoint: first.clone()
                }
            );
            let mut next = first;
            next.last_check_ms = 101;
            next.next_check_ms = 111;
            assert_eq!(restarted.save(Some(1), &next), Ok(2));
        }

        #[test]
        fn second_physical_writer_cannot_open_until_first_owner_exits() {
            let root = root();
            let path = canonical(&root);
            let first = PatrolCheckpointStore::open(&path).unwrap();
            assert!(matches!(
                PatrolCheckpointStore::open(&path),
                Err(PortError::Conflict)
            ));
            drop(first);
            assert!(PatrolCheckpointStore::open(&path).is_ok());
        }

        #[test]
        fn publication_failure_before_rename_retains_the_old_record() {
            let root = root();
            let path = canonical(&root);
            let mut writer = PatrolCheckpointStore::open(&path).unwrap();
            writer.save(None, &checkpoint()).unwrap();
            writer.fault = Some(Box::new(|point| {
                if point == Boundary::BeforeRename {
                    Err(std::io::Error::other("controlled pre-publication failure"))
                } else {
                    Ok(())
                }
            }));
            let mut next = checkpoint();
            next.last_check_ms = 101;
            next.next_check_ms = 111;
            assert_eq!(writer.save(Some(1), &next), Err(PortError::Unavailable));
            drop(writer);
            let mut recovered = PatrolCheckpointStore::open(&path).unwrap();
            assert_eq!(recovered.load(&scope()).unwrap().unwrap().version, 1);
        }

        #[test]
        fn rename_or_directory_ack_loss_requires_reopen_and_keeps_same_incident() {
            for boundary in [Boundary::AfterRename, Boundary::AfterDirectorySync] {
                let root = root();
                let path = canonical(&root);
                let mut writer = PatrolCheckpointStore::open(&path).unwrap();
                writer.fault = Some(Box::new(move |at| {
                    if at == boundary {
                        Err(std::io::Error::other("controlled publication ACK loss"))
                    } else {
                        Ok(())
                    }
                }));
                assert_eq!(
                    writer.save(None, &checkpoint()),
                    Err(PortError::OutcomeUnknown)
                );
                assert_eq!(writer.load(&scope()), Err(PortError::OutcomeUnknown));
                drop(writer);
                let mut recovered = PatrolCheckpointStore::open(&path).unwrap();
                let record = recovered.load(&scope()).unwrap().unwrap();
                assert_eq!(record.version, 1);
                assert_eq!(
                    record.checkpoint.incident.unwrap().id,
                    checkpoint().incident.unwrap().id
                );
                assert_eq!(
                    recovered.save(None, &checkpoint()),
                    Err(PortError::Conflict)
                );
            }
        }

        #[test]
        fn corrupt_or_future_records_are_not_reset_to_empty_or_overwritten() {
            for bytes in [
                b"{truncated".as_slice(),
                b"{\"format\":\"future\",\"schema\":2}".as_slice(),
            ] {
                let root = root();
                let path = canonical(&root);
                let mut writer = PatrolCheckpointStore::open(&path).unwrap();
                let file = path.join(format!("{}.json", checkpoint::scope_key(&scope()).unwrap()));
                std::fs::write(&file, bytes).unwrap();
                assert_eq!(writer.load(&scope()), Err(PortError::Unavailable));
                assert_eq!(
                    writer.save(None, &checkpoint()),
                    Err(PortError::Unavailable)
                );
                assert_eq!(std::fs::read(file).unwrap(), bytes);
            }
        }

        #[test]
        fn scope_hashes_isolate_projects_and_reject_copied_checkpoint_bytes() {
            let root = root();
            let path = canonical(&root);
            let mut writer = PatrolCheckpointStore::open(&path).unwrap();
            writer.save(None, &checkpoint()).unwrap();
            let mut other = scope();
            other.project_id = "other-project".into();
            assert_eq!(writer.load(&other), Ok(None));
            let original = path.join(format!("{}.json", checkpoint::scope_key(&scope()).unwrap()));
            let copied = path.join(format!("{}.json", checkpoint::scope_key(&other).unwrap()));
            std::fs::copy(original, copied).unwrap();
            assert_eq!(writer.load(&other), Err(PortError::Unavailable));
        }

        #[test]
        fn symbolic_links_hard_links_and_replaced_root_fail_closed() {
            use std::os::unix::fs::symlink;
            let root = root();
            let path = canonical(&root);
            let mut writer = PatrolCheckpointStore::open(&path).unwrap();
            let external = path.join("external");
            std::fs::write(&external, b"private").unwrap();
            let target = path.join(format!("{}.json", checkpoint::scope_key(&scope()).unwrap()));
            symlink(&external, &target).unwrap();
            assert!(writer.load(&scope()).is_err());
            std::fs::remove_file(&target).unwrap();
            std::fs::hard_link(&external, &target).unwrap();
            assert!(writer.load(&scope()).is_err());
            std::fs::remove_file(&target).unwrap();
            let moved = path.with_extension("retained");
            std::fs::rename(&path, &moved).unwrap();
            std::fs::create_dir(&path).unwrap();
            assert_eq!(writer.save(None, &checkpoint()), Err(PortError::Denied));
            drop(writer);
            std::fs::remove_dir(&path).unwrap();
            std::fs::rename(&moved, &path).unwrap();
        }

        #[test]
        fn orphan_staging_files_do_not_become_published_state() {
            let root = root();
            let path = canonical(&root);
            let mut writer = PatrolCheckpointStore::open(&path).unwrap();
            std::fs::write(
                path.join("pending-other-writer.tmp"),
                checkpoint::encode(1, &checkpoint()).unwrap(),
            )
            .unwrap();
            assert_eq!(writer.load(&scope()), Ok(None));
            writer.save(None, &checkpoint()).unwrap();
            assert!(path.join("pending-other-writer.tmp").exists());
        }
    }
}

#[cfg(unix)]
pub use physical::PatrolCheckpointStore;

#[cfg(not(unix))]
pub struct PatrolCheckpointStore;

#[cfg(not(unix))]
impl PatrolCheckpointStore {
    pub fn open(_root: &Path) -> Result<Self, PortError> {
        Err(PortError::Unavailable)
    }
}

#[cfg(not(unix))]
impl CheckpointPort for PatrolCheckpointStore {
    fn load(&mut self, _scope: &Scope) -> Result<Option<StoredCheckpoint>, PortError> {
        Err(PortError::Unavailable)
    }
    fn save(&mut self, _expected: Option<u64>, _next: &Checkpoint) -> Result<u64, PortError> {
        Err(PortError::Unavailable)
    }
}
