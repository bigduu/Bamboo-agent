//! Stable private browser profiles. HTTP and model arguments never select paths.

use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

use super::BrowserError;

const CLOSE_PENDING: &str = ".bamboo-profile-close-pending";
const LEASES: &str = ".bamboo-profile-leases";

pub(super) struct Lease {
    pub(super) path: PathBuf,
    _lock: std::fs::File,
    // Never unlink this lock with the profile: queued openers may already hold
    // a descriptor to its inode. Open and deletion use the same stable lock.
    _lifecycle_lock: std::fs::File,
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
        }
    }

    pub(super) fn finish_close(&self) -> Result<(), BrowserError> {
        match std::fs::remove_file(self.path.join(CLOSE_PENDING)) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(_) => Err(unavailable()),
        }
    }
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
    std::fs::canonicalize(profile).map_err(|_| unavailable())
}

fn lock_file(lock_path: &Path) -> Result<std::fs::File, BrowserError> {
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
    fs2::FileExt::try_lock_exclusive(&lock).map_err(|_| unavailable())?;
    Ok(lock)
}

fn lifecycle_lock(root: &Path, session_id: &str) -> Result<std::fs::File, BrowserError> {
    let locks = root.join(LEASES);
    private_directory(&locks)?;
    lock_file(&locks.join(format!("{}.lock", key(session_id))))
}

pub(super) fn acquire(root: &Path, session_id: &str) -> Result<Lease, BrowserError> {
    let root = root_directory(root, true)?.ok_or_else(unavailable)?;
    let lifecycle_lock = lifecycle_lock(&root, session_id)?;
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
    })
}

/// Purge only the derived chat directory, with no live owner. A pending-close
/// marker prevents replay, not deletion. Never create a missing profile here.
pub(super) fn remove(root: &Path, session_id: &str) -> Result<(), BrowserError> {
    let Some(root) = root_directory(root, false)? else {
        return Ok(());
    };
    let path = root.join(key(session_id));
    let _lifecycle_lock = lifecycle_lock(&root, session_id)?;
    // Lock before even the absence check: an opener can own the lease before
    // creating the profile directory. Its startup is not an absent profile.
    match std::fs::symlink_metadata(&path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(_) => return Err(unavailable()),
        Ok(_) => validate_private_directory(&path)?,
    }
    let _profile_lock = lock_file(&path.join(".bamboo-profile.lock"))?;
    // remove_dir_all unlinks internal symlinks; it does not follow their
    // targets. The selected root itself must have passed the checks above.
    std::fs::remove_dir_all(path).map_err(|_| unavailable())
}

#[cfg(test)]
mod tests {
    use super::*;

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
