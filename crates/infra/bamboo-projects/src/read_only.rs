//! Existing current Project facts only. Never initializes, migrates or repairs.
use super::{ProjectId, ProjectManifest, ProjectStore, ProjectStoreResult};

impl ProjectStore {
    /// Read a current primary under the existing writer's shared lock, without
    /// creating files, consulting the index/backup, or normalizing revisions.
    /// Busy, missing, legacy and inconsistent stores require their normal owner.
    /// This snapshot grants no retained resource or execution authority.
    pub fn get_read_only(&self, project_id: &ProjectId) -> ProjectStoreResult<ProjectManifest> {
        #[cfg(unix)]
        {
            unix::read(self, project_id, || {})
        }
        #[cfg(not(unix))]
        {
            let _ = project_id;
            Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "read-only Project file identity is unsupported on this platform",
            )
            .into())
        }
    }
}

#[cfg(unix)]
mod unix {
    use super::*;
    use crate::{decode_manifest, FileLock, ProjectStoreError};
    use fs2::FileExt;
    use std::{
        ffi::CString,
        fs::{File, OpenOptions},
        io::Read,
        os::{fd::AsRawFd, unix::fs::MetadataExt},
        path::Path,
    };

    const MAX_MANIFEST_BYTES: usize = 1_048_576;
    const MAX_FLOOR_BYTES: usize = 64;

    fn invalid() -> ProjectStoreError {
        ProjectStoreError::Validation("current read-only Project proof unavailable".into())
    }

    fn name(value: &str) -> ProjectStoreResult<CString> {
        if value.is_empty() || value == "." || value == ".." || value.contains('/') {
            return Err(invalid());
        }
        CString::new(value).map_err(|_| invalid())
    }

    fn open_at(parent: &File, component: &str, directory: bool) -> ProjectStoreResult<File> {
        use std::os::fd::FromRawFd;
        let component = name(component)?;
        let flags = libc::O_RDONLY
            | libc::O_NOFOLLOW
            | libc::O_CLOEXEC
            | libc::O_NONBLOCK
            | if directory { libc::O_DIRECTORY } else { 0 };
        // SAFETY: the parent descriptor and NUL-terminated name are live. The
        // owned parent anchors resolution through replacement ancestors/links.
        let fd = unsafe { libc::openat(parent.as_raw_fd(), component.as_ptr(), flags) };
        if fd < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        // SAFETY: openat returned a new owned descriptor, transferred once.
        Ok(unsafe { File::from_raw_fd(fd) })
    }

    fn check_at(
        parent: &File,
        component: &str,
        file: &File,
        directory: bool,
    ) -> ProjectStoreResult<()> {
        let component = name(component)?;
        let mut named = std::mem::MaybeUninit::<libc::stat>::uninit();
        // SAFETY: both descriptors/names are live and fstatat initializes stat
        // on success. AT_SYMLINK_NOFOLLOW inspects the named entry itself.
        if unsafe {
            libc::fstatat(
                parent.as_raw_fd(),
                component.as_ptr(),
                named.as_mut_ptr(),
                libc::AT_SYMLINK_NOFOLLOW,
            )
        } < 0
        {
            return Err(std::io::Error::last_os_error().into());
        }
        let named = unsafe { named.assume_init() };
        let mut physical = std::mem::MaybeUninit::<libc::stat>::uninit();
        // SAFETY: file owns a live descriptor; fstat initializes stat on success.
        if unsafe { libc::fstat(file.as_raw_fd(), physical.as_mut_ptr()) } < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        let physical = unsafe { physical.assume_init() };
        let opened = file.metadata()?;
        let expected_kind = if directory {
            libc::S_IFDIR
        } else {
            libc::S_IFREG
        };
        if named.st_mode & libc::S_IFMT != expected_kind
            || (directory && !opened.is_dir())
            || (!directory && (!opened.is_file() || opened.nlink() != 1))
            || physical.st_dev != named.st_dev
            || physical.st_ino != named.st_ino
        {
            return Err(invalid());
        }
        Ok(())
    }

    fn bounded(parent: &File, component: &str, cap: usize) -> ProjectStoreResult<(File, Vec<u8>)> {
        let mut file = open_at(parent, component, false)?;
        check_at(parent, component, &file, false)?;
        if file.metadata()?.len() > cap as u64 {
            return Err(invalid());
        }
        let mut bytes = Vec::new();
        (&mut file).take(cap as u64 + 1).read_to_end(&mut bytes)?;
        if bytes.len() > cap || file.metadata()?.len() != bytes.len() as u64 {
            return Err(invalid());
        }
        check_at(parent, component, &file, false)?;
        Ok((file, bytes))
    }

    fn check_root(root: &File, path: &Path) -> ProjectStoreResult<()> {
        let opened = root.metadata()?;
        let named = std::fs::symlink_metadata(path)?;
        if !opened.is_dir()
            || !named.is_dir()
            || named.file_type().is_symlink()
            || opened.dev() != named.dev()
            || opened.ino() != named.ino()
        {
            return Err(invalid());
        }
        Ok(())
    }

    pub(super) fn read(
        store: &ProjectStore,
        id: &ProjectId,
        after_read: impl FnOnce(),
    ) -> ProjectStoreResult<ProjectManifest> {
        use std::os::unix::fs::OpenOptionsExt;
        let root = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(store.paths.data_dir())?;
        check_root(&root, store.paths.data_dir())?;
        let projects = open_at(&root, "projects", true)?;
        let home = open_at(&projects, id.as_str(), true)?;
        let lock = open_at(&home, ".project.lock", false)?;
        check_at(&home, ".project.lock", &lock, false)?;
        FileExt::try_lock_shared(&lock)?;
        let lock = FileLock(lock);
        check_at(&home, ".project.lock", &lock.0, false)?;
        let state = open_at(&home, "state", true)?;
        let (primary, bytes) = bounded(&home, "project.json", MAX_MANIFEST_BYTES)?;
        let (floor, floor_bytes) = bounded(&state, "manifest-revision", MAX_FLOOR_BYTES)?;
        let decoded = decode_manifest(&bytes, id)?;
        let revision = std::str::from_utf8(&floor_bytes)
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .filter(|v| *v > 0)
            .ok_or_else(invalid)?;
        if decoded.migrated_from.is_some() || revision != decoded.manifest.revision {
            return Err(invalid());
        }
        after_read();
        check_root(&root, store.paths.data_dir())?;
        check_at(&root, "projects", &projects, true)?;
        check_at(&projects, id.as_str(), &home, true)?;
        check_at(&home, "state", &state, true)?;
        check_at(&home, ".project.lock", &lock.0, false)?;
        check_at(&home, "project.json", &primary, false)?;
        check_at(&state, "manifest-revision", &floor, false)?;
        Ok(decoded.manifest)
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::{collections::BTreeMap, path::PathBuf};

        fn fixture() -> (tempfile::TempDir, ProjectStore, ProjectManifest) {
            let root = tempfile::tempdir().unwrap();
            let store = ProjectStore::open(root.path()).unwrap();
            let project = store.create("Read-only proof", None).unwrap();
            (root, store, project)
        }

        fn bytes(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
            fn walk(root: &Path, at: &Path, out: &mut BTreeMap<PathBuf, Vec<u8>>) {
                for entry in std::fs::read_dir(at).unwrap() {
                    let entry = entry.unwrap();
                    if entry.file_type().unwrap().is_dir() {
                        walk(root, &entry.path(), out);
                    } else if entry.file_type().unwrap().is_symlink() {
                        out.insert(
                            entry.path().strip_prefix(root).unwrap().to_owned(),
                            std::fs::read_link(entry.path())
                                .unwrap()
                                .as_os_str()
                                .as_encoded_bytes()
                                .to_vec(),
                        );
                    } else {
                        out.insert(
                            entry.path().strip_prefix(root).unwrap().to_owned(),
                            std::fs::read(entry.path()).unwrap(),
                        );
                    }
                }
            }
            let mut out = BTreeMap::new();
            walk(root, root, &mut out);
            out
        }

        #[test]
        fn current_repeated_reads_preserve_primary_floor_backup_index_and_temps() {
            let (root, store, project) = fixture();
            let home = store.paths.project_home(&project.id);
            std::fs::write(home.join(".leftover.tmp.fixture"), b"retain").unwrap();
            let before = bytes(root.path());
            for _ in 0..3 {
                assert_eq!(store.get_read_only(&project.id).unwrap(), project);
            }
            assert_eq!(bytes(root.path()), before);
        }

        #[test]
        fn missing_lock_floor_or_project_and_exclusive_writer_never_initialize() {
            let (root, store, project) = fixture();
            let lock = store.paths.project_home(&project.id).join(".project.lock");
            let writer = crate::lock_exclusive(lock.clone()).unwrap();
            let before = bytes(root.path());
            assert!(
                matches!(store.get_read_only(&project.id),Err(ProjectStoreError::Io(e)) if e.kind()==std::io::ErrorKind::WouldBlock)
            );
            assert_eq!(bytes(root.path()), before);
            drop(writer);
            for path in [lock, store.paths.manifest_revision_path(&project.id)] {
                std::fs::remove_file(&path).unwrap();
                let before = bytes(root.path());
                assert!(store.get_read_only(&project.id).is_err());
                assert_eq!(bytes(root.path()), before);
                std::fs::write(
                    &path,
                    if path.file_name().unwrap() == ".project.lock" {
                        b"".as_slice()
                    } else {
                        b"1\n".as_slice()
                    },
                )
                .unwrap();
            }
            let absent = ProjectId::parse("absent-read-only-project").unwrap();
            let before = bytes(root.path());
            assert!(store.get_read_only(&absent).is_err());
            assert_eq!(bytes(root.path()), before);
        }

        #[test]
        fn legacy_corrupt_future_large_and_revision_mismatch_are_rejected_without_recovery() {
            let (root, store, project) = fixture();
            let path = store.paths.manifest_path(&project.id);
            let original = std::fs::read(&path).unwrap();
            std::fs::write(path.with_file_name("project.json.bak"), &original).unwrap();
            for content in [
                b"{corrupt".to_vec(),
                vec![b' '; MAX_MANIFEST_BYTES + 1],
                {
                    let mut v: serde_json::Value = serde_json::from_slice(&original).unwrap();
                    v["schema_version"] = 1.into();
                    serde_json::to_vec(&v).unwrap()
                },
                {
                    let mut v: serde_json::Value = serde_json::from_slice(&original).unwrap();
                    v["schema_version"] = 999.into();
                    serde_json::to_vec(&v).unwrap()
                },
            ] {
                std::fs::write(&path, content).unwrap();
                let before = bytes(root.path());
                assert!(store.get_read_only(&project.id).is_err());
                assert_eq!(bytes(root.path()), before);
            }
            std::fs::write(&path, original).unwrap();
            for floor in ["0", "2", "invalid", &"1".repeat(MAX_FLOOR_BYTES + 1)] {
                std::fs::write(store.paths.manifest_revision_path(&project.id), floor).unwrap();
                let before = bytes(root.path());
                assert!(store.get_read_only(&project.id).is_err());
                assert_eq!(bytes(root.path()), before);
            }
        }

        #[test]
        fn named_file_and_directory_replacement_or_symlinks_never_validate_old_proof() {
            use std::os::unix::fs::symlink;
            let (root, store, project) = fixture();
            let home = store.paths.project_home(&project.id);
            let primary = home.join("project.json");
            for target in [
                primary.clone(),
                home.join(".project.lock"),
                store.paths.manifest_revision_path(&project.id),
            ] {
                let saved = target.with_extension("retained");
                assert!(read(&store, &project.id, || {
                    std::fs::rename(&target, &saved).unwrap();
                    std::fs::copy(&saved, &target).unwrap();
                })
                .is_err());
                std::fs::remove_file(&target).unwrap();
                std::fs::rename(&saved, &target).unwrap();
            }
            let saved = home.with_extension("retained");
            assert!(read(&store, &project.id, || {
                std::fs::rename(&home, &saved).unwrap();
                std::fs::create_dir(&home).unwrap();
            })
            .is_err());
            std::fs::remove_dir(&home).unwrap();
            std::fs::rename(&saved, &home).unwrap();
            let external = tempfile::tempdir().unwrap();
            std::fs::write(external.path().join("project.json"), b"private").unwrap();
            std::fs::rename(&home, &saved).unwrap();
            symlink(external.path(), &home).unwrap();
            let before = bytes(root.path());
            let outside = bytes(external.path());
            assert!(store.get_read_only(&project.id).is_err());
            assert_eq!(bytes(root.path()), before);
            assert_eq!(bytes(external.path()), outside);
        }
    }
}
