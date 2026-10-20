//! Stable private browser profiles. HTTP and model arguments never select paths.

use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

use super::BrowserError;

const CLOSE_PENDING: &str = ".bamboo-profile-close-pending";

pub(super) struct Lease {
    pub(super) path: PathBuf,
    _lock: std::fs::File,
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

pub(super) fn directory(root: &Path, session_id: &str) -> Result<PathBuf, BrowserError> {
    let parent = root.parent().ok_or_else(unavailable)?;
    let parent = std::fs::canonicalize(parent).map_err(|_| unavailable())?;
    let root = parent.join(root.file_name().ok_or_else(unavailable)?);
    private_directory(&root)?;
    let key: String = Sha256::digest(session_id.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let profile = root.join(key);
    private_directory(&profile)?;
    std::fs::canonicalize(profile).map_err(|_| unavailable())
}

pub(super) fn acquire(root: &Path, session_id: &str) -> Result<Lease, BrowserError> {
    let path = directory(root, session_id)?;
    let lock_path = path.join(".bamboo-profile.lock");
    if let Ok(metadata) = std::fs::symlink_metadata(&lock_path) {
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
    match std::fs::symlink_metadata(path.join(CLOSE_PENDING)) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        _ => return Err(unavailable()),
    }
    Ok(Lease { path, _lock: lock })
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
