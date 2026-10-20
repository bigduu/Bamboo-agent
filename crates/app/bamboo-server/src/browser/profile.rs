//! Stable private browser profiles. HTTP and model arguments never select paths.

use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

use super::BrowserError;

const CLOSE_PENDING: &str = ".bamboo-profile-close-pending";
const LEASES: &str = ".bamboo-profile-leases";
const RESET_GATE: &str = ".bamboo-profile-reset.lock";
const RETIRED: &[u8] = b"BAMBOO_BROWSER_RETIRED_V1\n";

#[cfg(all(test, unix))]
thread_local! {
    static DIRECTORY_SYNC_FAILURE: std::cell::RefCell<Option<PathBuf>> = const {
        std::cell::RefCell::new(None)
    };
}

pub(super) struct Lease {
    pub(super) path: PathBuf,
    _lock: std::fs::File,
    // Never unlink this lock with the profile: queued openers may already hold
    // a descriptor to its inode. Open and deletion use the same stable lock.
    _lifecycle_lock: std::fs::File,
    // Shared through actual host reaping; reset holds the same inode exclusively.
    _reset_gate: std::fs::File,
}

impl Lease {
    // A forced fallback must never replay the checkpoint from before logout.
    // Keep the marker on any unconfirmed close; do not delete site data.
    pub(super) fn begin_close(&self) -> Result<(), BrowserError> {
        let marker = self.path.join(CLOSE_PENDING);
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
        }
        match options.open(&marker) {
            Ok(file) => file.sync_all().map_err(|_| unavailable()),
            // The same leased host may resume closing after caller cancellation.
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let mut options = std::fs::OpenOptions::new();
                options.read(true);
                #[cfg(unix)]
                {
                    use std::os::unix::fs::OpenOptionsExt;
                    options.custom_flags(libc::O_NOFOLLOW);
                }
                options
                    .open(marker)
                    .and_then(|file| file.sync_all())
                    .map_err(|_| unavailable())
            }
            Err(_) => Err(unavailable()),
        }?;
        sync_directory(&self.path)
    }

    pub(super) fn finish_close(&self) -> Result<(), BrowserError> {
        match std::fs::remove_file(self.path.join(CLOSE_PENDING)) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(_) => Err(unavailable()),
        }?;
        sync_directory(&self.path)
    }
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<(), BrowserError> {
    use std::os::unix::fs::OpenOptionsExt;
    #[cfg(test)]
    if DIRECTORY_SYNC_FAILURE.with(|target| target.borrow().as_deref() == Some(path)) {
        return Err(unavailable());
    }
    // A file fsync does not persist creation/removal of its directory entry.
    // Never confirm the fence (or its removal) if this barrier fails.
    std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
        .open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|_| unavailable())
}

#[cfg(not(unix))]
fn sync_directory(_: &Path) -> Result<(), BrowserError> {
    Ok(())
}

fn unavailable() -> BrowserError {
    BrowserError::Unavailable("browser profile directory unavailable".into())
}

fn private_directory(path: &Path) -> Result<(), BrowserError> {
    #[cfg(unix)]
    let result = {
        use std::os::unix::fs::DirBuilderExt;
        std::fs::DirBuilder::new().mode(0o700).create(path)
    };
    #[cfg(not(unix))]
    let result = std::fs::create_dir(path);
    match result {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(_) => return Err(unavailable()),
    }
    validate_private_directory(path)
}

fn validate_private_directory(path: &Path) -> Result<(), BrowserError> {
    let metadata = std::fs::symlink_metadata(path).map_err(|_| unavailable())?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(unavailable());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        if metadata.permissions().mode() & 0o077 != 0
            || metadata.uid() != unsafe { libc::geteuid() }
        {
            return Err(unavailable());
        }
    }
    Ok(())
}

fn root_directory(root: &Path, create: bool) -> Result<Option<PathBuf>, BrowserError> {
    let parent = root.parent().ok_or_else(unavailable)?;
    let parent = match std::fs::canonicalize(parent) {
        Ok(parent) => parent,
        Err(error) if !create && error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(unavailable()),
    };
    let root = parent.join(root.file_name().ok_or_else(unavailable)?);
    if create {
        private_directory(&root)?;
    } else {
        match std::fs::symlink_metadata(&root) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(_) => return Err(unavailable()),
            Ok(_) => validate_private_directory(&root)?,
        }
    }
    Ok(Some(root))
}

fn key(session_id: &str) -> String {
    Sha256::digest(session_id.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

pub(super) fn directory(root: &Path, session_id: &str) -> Result<PathBuf, BrowserError> {
    let root = root_directory(root, true)?.ok_or_else(unavailable)?;
    let profile = root.join(key(session_id));
    private_directory(&profile)?;
    sync_directory(&root)?;
    std::fs::canonicalize(profile).map_err(|_| unavailable())
}

fn open_lock_file(lock_path: &Path) -> Result<std::fs::File, BrowserError> {
    if let Ok(metadata) = std::fs::symlink_metadata(lock_path) {
        if !metadata.is_file() || metadata.file_type().is_symlink() {
            return Err(unavailable());
        }
    }
    let mut options = std::fs::OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    }
    let lock = options.open(lock_path).map_err(|_| unavailable())?;
    Ok(lock)
}

fn lock_file(lock_path: &Path) -> Result<std::fs::File, BrowserError> {
    let lock = open_lock_file(lock_path)?;
    fs2::FileExt::try_lock_exclusive(&lock).map_err(|_| unavailable())?;
    Ok(lock)
}

fn admission_gate(root: &Path, exclusive: bool) -> Result<std::fs::File, BrowserError> {
    let gate = open_lock_file(&root.join(RESET_GATE))?;
    if exclusive {
        fs2::FileExt::try_lock_exclusive(&gate).map_err(|_| unavailable())?;
    } else {
        fs2::FileExt::try_lock_shared(&gate).map_err(|_| unavailable())?;
    }
    gate.sync_all().map_err(|_| unavailable())?;
    sync_directory(root)?;
    sync_directory(root.parent().ok_or_else(unavailable)?)?;
    Ok(gate)
}

pub(super) fn begin_reset(root: &Path) -> Result<std::fs::File, BrowserError> {
    let root = root_directory(root, true)?.ok_or_else(unavailable)?;
    admission_gate(&root, true)
}

fn lifecycle_lock(root: &Path, session_id: &str) -> Result<std::fs::File, BrowserError> {
    lifecycle_lock_key(root, &key(session_id))
}

fn lifecycle_lock_key(root: &Path, profile_key: &str) -> Result<std::fs::File, BrowserError> {
    let locks = root.join(LEASES);
    private_directory(&locks)?;
    lock_file(&locks.join(format!("{profile_key}.lock")))
}

/// Metadata-only cleanup evidence. A missing ID must not mint a stable lock.
pub(super) fn has_evidence(root: &Path, session_id: &str) -> Result<bool, BrowserError> {
    let Some(root) = root_directory(root, false)? else {
        return Ok(false);
    };
    let path = root.join(key(session_id));
    match std::fs::symlink_metadata(&path) {
        Ok(_) => {
            validate_private_directory(&path)?;
            return Ok(true);
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => return Err(unavailable()),
    }
    let leases = root.join(LEASES);
    match std::fs::symlink_metadata(&leases) {
        Ok(_) => validate_private_directory(&leases)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(_) => return Err(unavailable()),
    }
    match std::fs::symlink_metadata(leases.join(format!("{}.lock", key(session_id)))) {
        Ok(meta) if meta.is_file() && !meta.file_type().is_symlink() => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        _ => Err(unavailable()),
    }
}

fn is_retired(lock: &std::fs::File) -> Result<bool, BrowserError> {
    use std::io::{Read, Seek, SeekFrom};
    let mut file = lock;
    file.seek(SeekFrom::Start(0)).map_err(|_| unavailable())?;
    let mut bytes = Vec::new();
    file.take((RETIRED.len() + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| unavailable())?;
    match bytes.as_slice() {
        [] => Ok(false),
        bytes if bytes == RETIRED => Ok(true),
        // Partial writes and unknown nonempty formats never reopen a profile.
        _ => Err(unavailable()),
    }
}

pub(super) fn acquire(root: &Path, session_id: &str) -> Result<Lease, BrowserError> {
    let root = root_directory(root, true)?.ok_or_else(unavailable)?;
    let reset_gate = admission_gate(&root, false)?;
    let lifecycle_lock = lifecycle_lock(&root, session_id)?;
    if is_retired(&lifecycle_lock)? {
        return Err(BrowserError::NotOpen);
    }
    let path = directory(&root, session_id)?;
    let lock = lock_file(&path.join(".bamboo-profile.lock"))?;
    match std::fs::symlink_metadata(path.join(CLOSE_PENDING)) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        _ => return Err(unavailable()),
    }
    Ok(Lease {
        path,
        _lock: lock,
        _lifecycle_lock: lifecycle_lock,
        _reset_gate: reset_gate,
    })
}

/// Purge only the derived chat directory, with no live owner. A pending-close
/// marker prevents replay, not deletion. Never create a missing profile here.
#[cfg(test)]
pub(super) fn remove(root: &Path, session_id: &str) -> Result<(), BrowserError> {
    remove_notifying(root, session_id, || {})
}

pub(super) fn remove_notifying(
    root: &Path,
    session_id: &str,
    retirement_persisted: impl FnOnce(),
) -> Result<(), BrowserError> {
    remove_key_notifying(root, &key(session_id), retirement_persisted)
}

fn valid_key(profile_key: &str) -> bool {
    profile_key.len() == 64
        && profile_key
            .bytes()
            .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

fn remove_key_notifying(
    root: &Path,
    profile_key: &str,
    retirement_persisted: impl FnOnce(),
) -> Result<(), BrowserError> {
    use std::io::{Seek, SeekFrom, Write};
    if !valid_key(profile_key) {
        return Err(unavailable());
    }
    let root = root_directory(root, true)?.ok_or_else(unavailable)?;
    let path = root.join(profile_key);
    let lifecycle_lock = lifecycle_lock_key(&root, profile_key)?;
    // Hold the original inode before observing absence or writing the marker.
    // Existing/opening owners and preopened descriptors must still contend.
    let present = match std::fs::symlink_metadata(&path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
        Err(_) => return Err(unavailable()),
        Ok(_) => {
            validate_private_directory(&path)?;
            true
        }
    };
    let mut file = &lifecycle_lock;
    file.seek(SeekFrom::Start(0)).map_err(|_| unavailable())?;
    // Never unlink, rename or clear the lease inode. A partial marker is also
    // fail-closed; authorized cleanup can complete it on the same inode.
    file.write_all(RETIRED).map_err(|_| unavailable())?;
    file.sync_all().map_err(|_| unavailable())?;
    sync_directory(&root.join(LEASES))?;
    if present {
        let _profile_lock = lock_file(&path.join(".bamboo-profile.lock"))?;
        std::fs::remove_dir_all(path).map_err(|_| unavailable())?;
    }
    // Include the containing entries if this known terminal Session never
    // opened Chromium and its profile/lease containers were just created.
    sync_directory(&root)?;
    sync_directory(root.parent().ok_or_else(unavailable)?)?;
    retirement_persisted();
    Ok(())
}

/// Greenfield reset covers actual profile/lease keys, including closed and
/// orphaned state. Never treat a filename as a Session ID and hash it twice.
/// Preserve every stable lease inode for old/preopened descriptors.
pub(super) fn purge_all(root: &Path) -> Result<(), BrowserError> {
    let Some(root) = root_directory(root, false)? else {
        return Ok(());
    };
    let mut keys = std::collections::BTreeSet::new();
    for entry in std::fs::read_dir(&root).map_err(|_| unavailable())? {
        let entry = entry.map_err(|_| unavailable())?;
        if entry.file_name() == RESET_GATE {
            let kind = entry.file_type().map_err(|_| unavailable())?;
            if !kind.is_file() || kind.is_symlink() {
                return Err(unavailable());
            }
        } else if entry.file_name() == LEASES {
            validate_private_directory(&entry.path())?;
            for lease in std::fs::read_dir(entry.path()).map_err(|_| unavailable())? {
                let lease = lease.map_err(|_| unavailable())?;
                let name = lease.file_name();
                let profile_key = name
                    .to_str()
                    .and_then(|s| s.strip_suffix(".lock"))
                    .ok_or_else(unavailable)?;
                let kind = lease.file_type().map_err(|_| unavailable())?;
                if !valid_key(profile_key) || !kind.is_file() || kind.is_symlink() {
                    return Err(unavailable());
                }
                keys.insert(profile_key.to_string());
            }
        } else {
            let name = entry.file_name();
            let profile_key = name.to_str().ok_or_else(unavailable)?;
            if !valid_key(profile_key) {
                return Err(unavailable());
            }
            validate_private_directory(&entry.path())?;
            keys.insert(profile_key.to_string());
        }
    }
    for profile_key in keys {
        remove_key_notifying(&root, &profile_key, || {})?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn profile_creation_and_reuse_require_the_parent_directory_barrier() {
        struct ClearFault;
        impl Drop for ClearFault {
            fn drop(&mut self) {
                DIRECTORY_SYNC_FAILURE.with(|target| *target.borrow_mut() = None);
            }
        }
        let temporary = tempfile::tempdir().unwrap();
        let root = std::fs::canonicalize(temporary.path())
            .unwrap()
            .join("profiles");
        private_directory(&root).unwrap();
        let sibling = root.join("synthetic-unrelated-state");
        std::fs::write(&sibling, b"preserve existing data").unwrap();
        let fault = ClearFault;
        DIRECTORY_SYNC_FAILURE.with(|target| *target.borrow_mut() = Some(root.clone()));
        let created = directory(&root, "profile-parent-barrier");
        let reused = directory(&root, "profile-parent-barrier");
        drop(fault);
        assert!(root.join(key("profile-parent-barrier")).is_dir());
        assert_eq!(std::fs::read(sibling).unwrap(), b"preserve existing data");
        assert!(
            created.is_err(),
            "profile creation returned before its parent directory barrier"
        );
        assert!(
            reused.is_err(),
            "profile reuse must confirm the parent after an earlier failed barrier"
        );
        assert!(directory(&root, "profile-parent-barrier").is_ok());
    }

    #[cfg(unix)]
    #[test]
    fn close_fence_creation_and_reuse_require_the_directory_barrier() {
        use std::os::unix::fs::PermissionsExt;
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("profiles");
        let lease = acquire(&root, "chat").unwrap();
        // Write/search permits creating and syncing the marker itself, but
        // opening its containing directory for the durability barrier fails.
        std::fs::set_permissions(&lease.path, std::fs::Permissions::from_mode(0o300)).unwrap();
        let created = lease.begin_close();
        let marker_exists = lease.path.join(CLOSE_PENDING).is_file();
        let reused = lease.begin_close();
        std::fs::set_permissions(&lease.path, std::fs::Permissions::from_mode(0o700)).unwrap();
        assert!(
            marker_exists,
            "fixture must create the marker before the barrier fails"
        );
        assert!(
            created.is_err(),
            "file fsync alone cannot confirm a durable close fence"
        );
        assert!(
            reused.is_err(),
            "an existing marker still needs its directory barrier"
        );
        lease.begin_close().unwrap();
        lease.finish_close().unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn close_fence_removal_and_absence_require_the_directory_barrier() {
        use std::os::unix::fs::PermissionsExt;
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("profiles");
        let lease = acquire(&root, "chat").unwrap();
        lease.begin_close().unwrap();
        std::fs::set_permissions(&lease.path, std::fs::Permissions::from_mode(0o300)).unwrap();
        let removed = lease.finish_close();
        let marker_absent = !lease.path.join(CLOSE_PENDING).exists();
        let absent = lease.finish_close();
        std::fs::set_permissions(&lease.path, std::fs::Permissions::from_mode(0o700)).unwrap();
        assert!(
            marker_absent,
            "fixture must unlink the marker before the barrier fails"
        );
        assert!(
            removed.is_err(),
            "unlink alone cannot confirm a durable clean close"
        );
        assert!(
            absent.is_err(),
            "retry must also confirm the directory entry is durable"
        );
        lease.finish_close().unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn profile_removal_and_absence_require_the_parent_directory_barrier() {
        use std::os::unix::fs::PermissionsExt;
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("profiles");
        let lease = acquire(&root, "chat").unwrap();
        let path = lease.path.clone();
        std::fs::write(path.join("Cookies"), b"synthetic state").unwrap();
        drop(lease);
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o300)).unwrap();
        let removed = remove(&root, "chat");
        let profile_absent = !path.exists();
        let absent = remove(&root, "chat");
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
        assert!(
            profile_absent,
            "fixture must remove the profile before the barrier fails"
        );
        assert!(
            removed.is_err(),
            "profile unlink alone cannot confirm durable removal"
        );
        assert!(
            absent.is_err(),
            "retry must still sync the parent of an absent profile"
        );
        remove(&root, "chat").unwrap();
    }

    #[test]
    fn same_chat_reuses_its_profile_and_other_chats_are_separate() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("browser-profiles");
        let a = directory(&root, "chat-a").unwrap();
        assert_eq!(a, directory(&root, "chat-a").unwrap());
        assert_ne!(a, directory(&root, "chat-b").unwrap());
        let hostile = directory(&root, "../../another-profile").unwrap();
        let canonical_root = std::fs::canonicalize(&root).unwrap();
        assert_eq!(hostile.parent(), Some(canonical_root.as_path()));
        assert_eq!(hostile.file_name().unwrap().len(), 64);
    }

    #[cfg(unix)]
    #[test]
    fn profile_root_symlink_is_rejected_without_touching_its_target() {
        let temporary = tempfile::tempdir().unwrap();
        let target = temporary.path().join("outside");
        std::fs::create_dir(&target).unwrap();
        let root = temporary.path().join("browser-profiles");
        std::os::unix::fs::symlink(&target, &root).unwrap();
        assert!(directory(&root, "chat").is_err());
        assert_eq!(std::fs::read_dir(&target).unwrap().count(), 0);
    }

    #[test]
    fn one_profile_has_one_live_owner_and_other_profiles_remain_available() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("browser-profiles");
        let owner = acquire(&root, "chat-a").unwrap();
        assert!(acquire(&root, "chat-a").is_err());
        let other = acquire(&root, "chat-b").unwrap();
        assert_ne!(owner.path, other.path);
        drop(owner);
        assert!(acquire(&root, "chat-a").is_ok());
    }
}
