//! Immutable, bounded materialized context files. These are disposable read
//! projections, never session state or a second command queue.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use fs2::FileExt;
use sha2::{Digest, Sha256};

pub const MAX_CONTEXT_SNAPSHOTS_PER_ROOT: usize = 64;
pub const MAX_CONTEXT_STATUS_BYTES: usize = 8 * 1024;
pub const MAX_CONTEXT_BRIEF_BYTES: usize = 16 * 1024;
pub const MAX_CONTEXT_MANIFEST_BYTES: usize = 8 * 1024;

/// Only these fixed filenames can be published by this interface.
pub struct ContextSnapshotFiles {
    pub manifest: Vec<u8>,
    pub status: Vec<u8>,
    pub brief: Vec<u8>,
}

pub struct PublishedContextSnapshot {
    pub directory: PathBuf,
    pub reused: bool,
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn require_directory(path: &Path) -> io::Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_dir() {
        return Err(invalid(
            "context_snapshot_unsafe_path: expected a real directory",
        ));
    }
    Ok(())
}

fn ensure_directory(path: &Path) -> io::Result<()> {
    match fs::create_dir(path) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error),
    }
    require_directory(path)
}

fn require_regular_file(path: &Path, size_limit: usize) -> io::Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_file() || metadata.len() > size_limit as u64 {
        return Err(invalid(
            "context_snapshot_corrupt: expected a bounded regular file",
        ));
    }
    Ok(())
}

fn is_revision(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn lock_root(directory: &Path) -> io::Result<File> {
    let path = directory.join(".publish.lock");
    let file = match OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&path)
    {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            require_regular_file(&path, 0)?;
            OpenOptions::new().read(true).write(true).open(&path)?
        }
        Err(error) => return Err(error),
    };
    require_regular_file(&path, 0)?;
    // One root's exporters share this advisory lock, including independent
    // processes. It does not serialize canonical session reads or other roots.
    file.lock_exclusive()?;
    Ok(file)
}

fn file_bytes(files: &ContextSnapshotFiles) -> [(&str, &[u8]); 3] {
    [
        ("status.md", &files.status),
        ("brief.md", &files.brief),
        ("manifest.json", &files.manifest),
    ]
}

fn verify_snapshot(directory: &Path, files: &ContextSnapshotFiles) -> io::Result<()> {
    require_directory(directory)?;
    if fs::read_dir(directory)?.count() != 3 {
        return Err(invalid(
            "context_snapshot_corrupt: incomplete or unexpected snapshot files",
        ));
    }
    for (name, expected) in file_bytes(files) {
        let path = directory.join(name);
        require_regular_file(&path, expected.len())?;
        if fs::read(path)? != expected {
            return Err(invalid(
                "context_snapshot_corrupt: immutable content differs",
            ));
        }
    }
    Ok(())
}

fn publish(
    home: &Path,
    root_id: &str,
    revision: &str,
    files: &ContextSnapshotFiles,
) -> io::Result<PublishedContextSnapshot> {
    if !is_revision(revision)
        || files.status.len() > MAX_CONTEXT_STATUS_BYTES
        || files.brief.len() > MAX_CONTEXT_BRIEF_BYTES
        || files.manifest.len() > MAX_CONTEXT_MANIFEST_BYTES
    {
        return Err(invalid(
            "context_snapshot_invalid: invalid revision or file budget",
        ));
    }
    require_directory(home)?;
    // Resolve the trusted configured home once (e.g. macOS /var aliases), then
    // reject symlinks in every output component below that anchor.
    let mut directory = fs::canonicalize(home)?;
    let root_hash = format!("{:x}", Sha256::digest(root_id.as_bytes()));
    for component in ["coordination", "session-context", "v1", &root_hash] {
        directory.push(component);
        ensure_directory(&directory)?;
    }
    let _lock = lock_root(&directory)?;
    let destination = directory.join(revision);
    match fs::symlink_metadata(&destination) {
        Ok(_) => {
            verify_snapshot(&destination, files)?;
            return Ok(PublishedContextSnapshot {
                directory: destination,
                reused: true,
            });
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }

    let mut count = 0;
    for entry in fs::read_dir(&directory)? {
        let entry = entry?;
        if entry.file_name() == ".publish.lock" {
            continue;
        }
        let name = entry.file_name();
        if !name.to_str().is_some_and(is_revision) || !entry.file_type()?.is_dir() {
            return Err(invalid(
                "context_snapshot_corrupt: unexpected output component or abandoned publication",
            ));
        }
        count += 1;
    }
    if count >= MAX_CONTEXT_SNAPSHOTS_PER_ROOT {
        return Err(invalid(
            "context_snapshot_quota: root snapshot limit reached; no snapshots were deleted",
        ));
    }

    let staging = directory.join(format!(".pending-{}", uuid::Uuid::new_v4()));
    fs::create_dir(&staging)?;
    let result = (|| {
        // The manifest is written last, and the entire complete directory is
        // published in one rename. Readers never receive a staging path.
        for (name, bytes) in file_bytes(files) {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(staging.join(name))?;
            file.write_all(bytes)?;
            file.sync_all()?;
        }
        fs::rename(&staging, &destination)?;
        Ok(PublishedContextSnapshot {
            directory: destination,
            reused: false,
        })
    })();
    if result.is_err() {
        // Only this call's unpublished staging is removed. Existing revisions
        // and leftovers from a crashed process are never silently reclaimed.
        let _ = fs::remove_dir_all(&staging);
    }
    result
}

/// Publish or verify one immutable snapshot without blocking the async runtime.
/// Caller authorization and safe field selection must precede this operation.
pub async fn publish_session_context_snapshot(
    home: &Path,
    root_id: &str,
    revision: &str,
    files: ContextSnapshotFiles,
) -> io::Result<PublishedContextSnapshot> {
    let home = home.to_path_buf();
    let root_id = root_id.to_string();
    let revision = revision.to_string();
    tokio::task::spawn_blocking(move || publish(&home, &root_id, &revision, &files))
        .await
        .map_err(|error| io::Error::other(format!("context snapshot publisher failed: {error}")))?
}

/// Read only the three fixed files, with caps on actual I/O. The caller must
/// independently authorize and verify manifest provenance before returning data.
pub async fn read_session_context_snapshot(
    home: &Path,
    caller_id: &str,
    revision: &str,
) -> io::Result<ContextSnapshotFiles> {
    if !is_revision(revision) {
        return Err(invalid("context_snapshot_invalid: invalid revision"));
    }
    let home = home.to_path_buf();
    let caller_id = caller_id.to_owned();
    let revision = revision.to_owned();
    tokio::task::spawn_blocking(move || retained_read(&home, &caller_id, &revision))
        .await
        .map_err(|error| io::Error::other(format!("context snapshot reader failed: {error}")))?
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn retained_read(home: &Path, caller: &str, revision: &str) -> io::Result<ContextSnapshotFiles> {
    use std::{
        ffi::CString,
        io::Read,
        os::fd::{AsRawFd, FromRawFd},
        os::unix::ffi::OsStrExt,
    };
    fn open_at(parent: &File, name: &str, directory: bool) -> io::Result<File> {
        let name = CString::new(name).map_err(|_| invalid("invalid context component"))?;
        let flags = libc::O_RDONLY
            | libc::O_CLOEXEC
            | libc::O_NOFOLLOW
            | libc::O_NONBLOCK
            | if directory { libc::O_DIRECTORY } else { 0 };
        let fd = unsafe { libc::openat(parent.as_raw_fd(), name.as_ptr(), flags) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(unsafe { File::from_raw_fd(fd) })
    }
    fn bounded(parent: &File, name: &str, limit: usize) -> io::Result<Vec<u8>> {
        let file = open_at(parent, name, false)?;
        let metadata = file.metadata()?;
        if !metadata.is_file() || metadata.len() > limit as u64 {
            return Err(invalid("context_snapshot_corrupt: file type or byte limit"));
        }
        let mut bytes = Vec::new();
        file.take(limit as u64 + 1).read_to_end(&mut bytes)?;
        if bytes.len() > limit {
            return Err(invalid(
                "context_snapshot_corrupt: actual read exceeded limit",
            ));
        }
        Ok(bytes)
    }
    // The configured data home is the trusted anchor. Every view component and
    // file below its retained fd is opened without following links/reopening paths.
    let home = fs::canonicalize(home)?;
    let name =
        CString::new(home.as_os_str().as_bytes()).map_err(|_| invalid("invalid data home"))?;
    let fd = unsafe {
        libc::open(
            name.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    let mut directory = unsafe { File::from_raw_fd(fd) };
    let caller_hash = format!("{:x}", Sha256::digest(caller.as_bytes()));
    for part in [
        "coordination",
        "session-context",
        "v1",
        &caller_hash,
        revision,
    ] {
        directory = open_at(&directory, part, true)?;
    }
    Ok(ContextSnapshotFiles {
        manifest: bounded(&directory, "manifest.json", MAX_CONTEXT_MANIFEST_BYTES)?,
        status: bounded(&directory, "status.md", MAX_CONTEXT_STATUS_BYTES)?,
        brief: bounded(&directory, "brief.md", MAX_CONTEXT_BRIEF_BYTES)?,
    })
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn retained_read(_: &Path, _: &str, _: &str) -> io::Result<ContextSnapshotFiles> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "context_snapshot_retained_read_unsupported: no path-check/reopen fallback",
    ))
}
