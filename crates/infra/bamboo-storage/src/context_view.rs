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
    read_session_context_snapshot_retaining(home, caller_id, revision, std::sync::Arc::new(()))
        .await
}

/// The canonical observer writer retains its authority through actual physical
/// reads, including cancellation of the originating async caller.
pub(crate) async fn read_session_context_snapshot_retaining<T: Send + Sync + 'static>(
    home: &Path,
    caller_id: &str,
    revision: &str,
    authority: std::sync::Arc<T>,
) -> io::Result<ContextSnapshotFiles> {
    if !is_revision(revision) {
        return Err(invalid("context_snapshot_invalid: invalid revision"));
    }
    let home = home.to_path_buf();
    let caller_id = caller_id.to_owned();
    let revision = revision.to_owned();
    tokio::task::spawn_blocking(move || {
        let _authority = authority;
        retained_read(&home, &caller_id, &revision)
    })
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

/// Runtime-owned, bounded derived JSONL bundle. No caller path or watermark is
/// accepted. The canonical writer retains its authority in the blocking job.
#[cfg(any(target_os = "linux", target_os = "macos"))]
pub(crate) fn publish_observer_change_snapshot(
    home: &Path,
    caller: &str,
    revision: &str,
    manifest: &[u8],
    shards: &[String],
) -> io::Result<PathBuf> {
    use std::{
        ffi::CString,
        io::Read,
        os::fd::{AsRawFd, FromRawFd},
    };
    fn open_at(parent: &File, name: &str, flags: i32) -> io::Result<File> {
        let name = CString::new(name).map_err(|_| invalid("invalid change component"))?;
        let fd = unsafe {
            libc::openat(
                parent.as_raw_fd(),
                name.as_ptr(),
                flags | libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK,
                0o600,
            )
        };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(unsafe { File::from_raw_fd(fd) })
    }
    fn directory(parent: &File, name: &str) -> io::Result<File> {
        let value = CString::new(name).map_err(|_| invalid("invalid change directory"))?;
        if unsafe { libc::mkdirat(parent.as_raw_fd(), value.as_ptr(), 0o700) } < 0 {
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::AlreadyExists {
                return Err(error);
            }
        }
        open_at(parent, name, libc::O_RDONLY | libc::O_DIRECTORY)
    }
    fn verify(parent: &File, name: &str, expected: &[u8]) -> io::Result<()> {
        let file = open_at(parent, name, libc::O_RDONLY)?;
        if !file.metadata()?.is_file() || file.metadata()?.len() != expected.len() as u64 {
            return Err(invalid("corrupt change file"));
        }
        let mut bytes = Vec::new();
        file.take(expected.len() as u64 + 1)
            .read_to_end(&mut bytes)?;
        if bytes != expected {
            return Err(invalid("change file commitment mismatch"));
        }
        Ok(())
    }
    if !is_revision(revision)
        || manifest.len() > 8 * 1024
        || shards.len() > 4
        || shards.iter().any(|shard| shard.len() > 4 * 1024)
    {
        return Err(invalid("derived change bundle exceeds fixed budget"));
    }
    let home = fs::canonicalize(home)?;
    let mut path = home.clone();
    let mut parent = File::open(&home)?;
    let caller_hash = format!("{:x}", Sha256::digest(caller.as_bytes()));
    for component in ["coordination", "observer-changes", "v1", &caller_hash] {
        parent = directory(&parent, component)?;
        path.push(component);
    }
    let lock = open_at(&parent, ".publish.lock", libc::O_RDWR | libc::O_CREAT)?;
    if !lock.metadata()?.is_file() || lock.metadata()?.len() != 0 {
        return Err(invalid("unsafe change publication lock"));
    }
    lock.lock_exclusive()?;
    let mut files: Vec<(String, &[u8])> = shards
        .iter()
        .enumerate()
        .map(|(index, shard)| (format!("{index}.jsonl"), shard.as_bytes()))
        .collect();
    files.push(("manifest.json".into(), manifest));
    match open_at(&parent, revision, libc::O_RDONLY | libc::O_DIRECTORY) {
        Ok(existing) => {
            for (name, bytes) in &files {
                verify(&existing, name, bytes)?;
            }
            return Ok(path.join(revision));
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    // Count the retained directory, never a path that could be swapped after
    // checking it. Abandoned publications consume quota and fail visibly.
    let duplicate = unsafe { libc::fcntl(parent.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 0) };
    if duplicate < 0 {
        return Err(io::Error::last_os_error());
    }
    let entries = unsafe { libc::fdopendir(duplicate) };
    if entries.is_null() {
        unsafe { libc::close(duplicate) };
        return Err(io::Error::last_os_error());
    }
    let mut count = 0;
    loop {
        let entry = unsafe { libc::readdir(entries) };
        if entry.is_null() {
            break;
        }
        let name = unsafe { std::ffi::CStr::from_ptr((*entry).d_name.as_ptr()) }.to_bytes();
        if name != b"." && name != b".." && name != b".publish.lock" {
            count += 1;
            if count >= MAX_CONTEXT_SNAPSHOTS_PER_ROOT {
                break;
            }
        }
    }
    unsafe { libc::closedir(entries) };
    if count >= MAX_CONTEXT_SNAPSHOTS_PER_ROOT {
        return Err(invalid("derived change snapshot quota reached"));
    }
    let staging_name = format!(".pending-{}", uuid::Uuid::new_v4());
    let staging = directory(&parent, &staging_name)?;
    let result = (|| {
        for (name, bytes) in &files {
            let mut file = open_at(
                &staging,
                name,
                libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL,
            )?;
            file.write_all(bytes)?;
            file.sync_all()?;
        }
        staging.sync_all()?;
        let from =
            CString::new(staging_name.as_str()).map_err(|_| invalid("invalid staging name"))?;
        let to = CString::new(revision).map_err(|_| invalid("invalid revision"))?;
        if unsafe {
            libc::renameat(
                parent.as_raw_fd(),
                from.as_ptr(),
                parent.as_raw_fd(),
                to.as_ptr(),
            )
        } < 0
        {
            return Err(io::Error::last_os_error());
        }
        parent.sync_all()?;
        Ok(path.join(revision))
    })();
    if result.is_err() {
        // Only this call's unpublished files are removed, relative to held FDs.
        for (name, _) in &files {
            if let Ok(name) = CString::new(name.as_str()) {
                unsafe { libc::unlinkat(staging.as_raw_fd(), name.as_ptr(), 0) };
            }
        }
        if let Ok(name) = CString::new(staging_name) {
            unsafe { libc::unlinkat(parent.as_raw_fd(), name.as_ptr(), libc::AT_REMOVEDIR) };
        }
    }
    result
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub(crate) fn publish_observer_change_snapshot(
    _: &Path,
    _: &str,
    _: &str,
    _: &[u8],
    _: &[String],
) -> io::Result<PathBuf> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "retained change publication unsupported",
    ))
}
