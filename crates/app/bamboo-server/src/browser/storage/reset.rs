//! Explicit canonical-reset cleanup of an existing, independently inspected
//! envelope. No image create/attach/detach, quota mutation or data migration.
//! The caller holds the stable exclusive reset gate through this owner.

use std::path::Path;

use super::{unavailable, BrowserError};

const JOURNAL: &str = ".bamboo-storage-reset-v1.json";

pub(in crate::browser) struct Owner {
    #[cfg(target_os = "macos")]
    launch: super::Launch,
    #[cfg(target_os = "macos")]
    journal: Journal,
}

#[cfg(target_os = "macos")]
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Journal {
    version: u8,
    generation: uuid::Uuid,
    volume_uuid: uuid::Uuid,
    image_bytes: u64,
    quota_bytes: u64,
    complete: bool,
}

impl Owner {
    pub(in crate::browser) fn prepare(profile_root: &Path) -> Result<Option<Self>, BrowserError> {
        let root = profile_root.parent().ok_or_else(|| unavailable("browser_storage_control_invalid"))?
            .join("browser-storage-v1");
        match std::fs::symlink_metadata(&root) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(_) => return Err(unavailable("browser_storage_reset_unavailable")),
            Ok(_) => {}
        }
        #[cfg(not(target_os = "macos"))]
        return Err(unavailable("browser_storage_provider_unsupported"));
        #[cfg(target_os = "macos")]
        {
            let control = profile_root.join(super::profile::shared_key());
            let launch = super::Launch::pin_existing(&control, false)?;
            super::prepare::guard_reset(&root)?;
            let receipt = inspect(&launch)?;
            if super::prepare::registered_uuid(&launch.data_root)?.is_some_and(|uuid| uuid != receipt.volume_uuid) {
                return Err(unavailable("browser_storage_reset_identity_changed"));
            }
            validate_root_names(&launch)?;
            if let Some(previous) = read_journal(&root)? {
                if previous.volume_uuid != receipt.volume_uuid {
                    return Err(unavailable("browser_storage_reset_identity_changed"));
                }
            }
            let journal = Journal { version: 1, generation: uuid::Uuid::new_v4(),
                volume_uuid: receipt.volume_uuid, image_bytes: super::IMAGE_BYTES,
                quota_bytes: 402_653_184, complete: false };
            // No canonical wipe is permitted before the pending entry and its
            // containing directory are durable, even on an explicit retry.
            write_journal(&root, &journal)?;
            Ok(Some(Self { launch, journal }))
        }
    }

    pub(in crate::browser) fn finish(&mut self) -> Result<(), BrowserError> {
        #[cfg(not(target_os = "macos"))]
        return Err(unavailable("browser_storage_provider_unsupported"));
        #[cfg(target_os = "macos")]
        {
            let root = self.launch.data_root.join("browser-storage-v1");
            let on_disk = read_journal(&root)?.ok_or_else(|| unavailable("browser_storage_reset_unavailable"))?;
            if on_disk.generation != self.journal.generation || on_disk.volume_uuid != self.journal.volume_uuid
                || on_disk.complete {
                return Err(unavailable("browser_storage_reset_identity_changed"));
            }
            let receipt = inspect(&self.launch)?;
            if receipt.volume_uuid != self.journal.volume_uuid {
                return Err(unavailable("browser_storage_reset_identity_changed"));
            }
            validate_root_names(&self.launch)?;
            verify_pins(&self.launch)?;
            // First four descriptors pin data root, image root, mount and the
            // profiles parent. Retain all seven private role directories and
            // the fixed backing; unlink only their contents, through those FDs.
            for (_, directory) in self.launch._directories.iter().skip(4) {
                erase_children(directory, receipt.mount_device, 0)?;
            }
            verify_pins(&self.launch)?;
            if inspect(&self.launch)?.volume_uuid != self.journal.volume_uuid {
                return Err(unavailable("browser_storage_reset_identity_changed"));
            }
            // Keep a bounded completion record instead of unlinking the last
            // journal. A crash during atomic replacement leaves either the
            // durable pending fence or a receipt for already-complete erasure.
            self.journal.complete = true;
            write_journal(&root, &self.journal)
        }
    }
}

#[cfg(target_os = "macos")]
pub(super) fn launch_volume_uuid(data_root: &Path) -> Result<Option<String>, BrowserError> {
    match read_journal(&data_root.join("browser-storage-v1"))? {
        None => Ok(None),
        Some(journal) if journal.complete => Ok(Some(journal.volume_uuid.to_string())),
        Some(_) => Err(unavailable("browser_storage_reset_incomplete")),
    }
}

#[cfg(target_os = "macos")]
fn read_journal(root: &Path) -> Result<Option<Journal>, BrowserError> {
    let Some(journal) = read_control_record::<Journal>(root, JOURNAL)? else { return Ok(None); };
    if journal.version != 1 || journal.generation.is_nil() || journal.volume_uuid.is_nil()
        || journal.image_bytes != super::IMAGE_BYTES || journal.quota_bytes != 402_653_184 {
        return Err(unavailable("browser_storage_reset_journal_invalid"));
    }
    Ok(Some(journal))
}

#[cfg(target_os = "macos")]
pub(super) fn read_control_record<T: serde::de::DeserializeOwned>(root: &Path, name: &str) -> Result<Option<T>, BrowserError> {
    use std::io::Read;
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
    let file = match std::fs::OpenOptions::new().read(true).custom_flags(libc::O_NOFOLLOW)
        .open(root.join(name)) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(unavailable("browser_storage_reset_journal_invalid")),
    };
    let metadata = file.metadata().map_err(|_| unavailable("browser_storage_reset_journal_invalid"))?;
    if !metadata.is_file() || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.permissions().mode() & 0o777 != 0o600 || metadata.len() > 1024 {
        return Err(unavailable("browser_storage_reset_journal_invalid"));
    }
    let mut bytes = Vec::new();
    file.take(1025).read_to_end(&mut bytes).map_err(|_| unavailable("browser_storage_reset_journal_invalid"))?;
    let journal = serde_json::from_slice(&bytes).map_err(|_| unavailable("browser_storage_reset_journal_invalid"))?;
    Ok(Some(journal))
}

#[cfg(target_os = "macos")]
fn write_journal(root: &Path, journal: &Journal) -> Result<(), BrowserError> {
    write_control_record(root, JOURNAL, journal)
}

#[cfg(target_os = "macos")]
pub(super) fn write_control_record<T: serde::Serialize>(root: &Path, name: &str, journal: &T) -> Result<(), BrowserError> {
    use std::io::Write;
    let bytes = serde_json::to_vec(journal).map_err(|_| unavailable("browser_storage_reset_journal_invalid"))?;
    if bytes.len() > 1024 { return Err(unavailable("browser_storage_reset_journal_invalid")); }
    let mut next = tempfile::NamedTempFile::new_in(root).map_err(|_| unavailable("browser_storage_reset_unavailable"))?;
    next.write_all(&bytes).and_then(|()| next.as_file().sync_all())
        .map_err(|_| unavailable("browser_storage_reset_unavailable"))?;
    next.persist(root.join(name)).map_err(|_| unavailable("browser_storage_reset_unavailable"))?;
    super::profile::sync_directory(root)
}

#[cfg(target_os = "macos")]
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct Inspection {
    kind: String,
    pub(super) volume_uuid: uuid::Uuid,
    image_bytes: u64,
    quota_bytes: u64,
    pub(super) mount_device: u64,
    image_device: u64,
    pub(super) mount_mode: Option<u32>,
    pub(super) paths_complete: Option<bool>,
    pub(super) paths_absent: Option<bool>,
}

#[cfg(target_os = "macos")]
fn inspect(launch: &super::Launch) -> Result<Inspection, BrowserError> {
    inspect_pinned(launch, "reset")
}

#[cfg(target_os = "macos")]
pub(super) fn inspect_for_launch(launch: &super::Launch) -> Result<Inspection, BrowserError> {
    inspect_pinned(launch, "launch")
}

#[cfg(target_os = "macos")]
fn inspect_pinned(launch: &super::Launch, purpose: &str) -> Result<Inspection, BrowserError> {
    use std::os::unix::fs::MetadataExt;
    let device = launch._directories[2].1.metadata().map_err(|_| unavailable("browser_storage_path_invalid"))?.dev();
    inspect_native(&launch.data_root, device, purpose)
}

#[cfg(target_os = "macos")]
pub(super) fn inspect_for_preparation(data_root: &Path, mount_device: u64) -> Result<Inspection, BrowserError> {
    inspect_native(data_root, mount_device, "preparation")
}

#[cfg(target_os = "macos")]
fn inspect_native(data_root: &Path, mount_device: u64, purpose: &str) -> Result<Inspection, BrowserError> {
    let (argument, expected_kind) = match purpose {
        "reset" => ("--storage-reset-inspect-v1", "validated_mac_storage_reset_observation_v1"),
        "launch" => ("--storage-inspect-v1", "validated_mac_storage_observation_v1"),
        "preparation" => ("--storage-prepare-inspect-v1", "validated_mac_storage_preparation_observation_v1"),
        _ => return Err(unavailable("browser_storage_inspection_failed")),
    };
    let receipt: Inspection = read_native_receipt(data_root, argument)?;
    if receipt.kind != expected_kind || receipt.volume_uuid.is_nil()
        || receipt.image_bytes != super::IMAGE_BYTES || receipt.quota_bytes != 402_653_184
        || receipt.mount_device == receipt.image_device || mount_device != receipt.mount_device
        || (purpose == "preparation" && (receipt.mount_mode.is_none() || receipt.paths_complete.is_none() || receipt.paths_absent.is_none()))
        || (purpose != "preparation" && (receipt.mount_mode.is_some() || receipt.paths_complete.is_some() || receipt.paths_absent.is_some())) {
        return Err(unavailable("browser_storage_inspection_failed"));
    }
    Ok(receipt)
}

#[cfg(target_os = "macos")]
pub(super) fn read_native_receipt<T: serde::de::DeserializeOwned>(data_root: &Path, argument: &str) -> Result<T, BrowserError> {
    use std::io::Read;
    use std::os::fd::AsRawFd;
    use std::process::{Command, Stdio};
    use std::time::{Duration, Instant};
    let script = super::super::host_script();
    let _verified_script = verify_inspector_script(&script)?;
    let node = std::env::var_os("BAMBOO_BROWSER_NODE").unwrap_or_else(|| "node".into());
    let mut command = Command::new(node);
    command.env_clear();
    for name in ["PATH", "LANG", "LC_ALL"] {
        if let Some(value) = std::env::var_os(name) { command.env(name, value); }
    }
    let mut child = command.arg(script).arg(argument).arg(data_root)
        .stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::null())
        .spawn().map_err(|_| unavailable("browser_storage_inspection_failed"))?;
    let mut output = child.stdout.take().unwrap();
    let flags = unsafe { libc::fcntl(output.as_raw_fd(), libc::F_GETFL) };
    if flags == -1 || unsafe { libc::fcntl(output.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) } == -1 {
        let _ = child.kill(); let _ = child.wait();
        return Err(unavailable("browser_storage_inspection_failed"));
    }
    let deadline = Instant::now() + Duration::from_secs(90);
    let mut bytes = Vec::new();
    let mut eof = false;
    let outcome = loop {
        let mut buffer = [0; 2048];
        match output.read(&mut buffer) {
            Ok(0) => eof = true,
            Ok(length) => bytes.extend_from_slice(&buffer[..length]),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(_) => break None,
        }
        if bytes.len() > 16_384 || Instant::now() >= deadline { break None; }
        match child.try_wait() {
            Ok(Some(status)) if eof => break Some(status),
            Ok(_) => {}
            Err(_) => break None,
        }
        std::thread::sleep(Duration::from_millis(5));
    };
    if !outcome.is_some_and(|status| status.success()) {
        // This helper performs reads only. A failed read never authorizes
        // mount, erasure, a fallback directory, or retry of a disk mutation.
        let _ = child.kill(); let _ = child.wait();
        return Err(unavailable("browser_storage_inspection_failed"));
    }
    serde_json::from_slice(&bytes).map_err(|_| unavailable("browser_storage_inspection_failed"))
}

#[cfg(target_os = "macos")]
fn verify_inspector_script(path: &Path) -> Result<std::fs::File, BrowserError> {
    use std::io::Read;
    use std::os::unix::fs::OpenOptionsExt;
    const PREFIX: &[u8] = b"#!/usr/bin/env node\n// BAMBOO_BROWSER_STORAGE_INSPECTION_V1\n";
    let mut file = std::fs::OpenOptions::new().read(true).custom_flags(libc::O_NOFOLLOW).open(path)
        .map_err(|_| unavailable("browser_storage_host_incompatible"))?;
    let metadata = file.metadata().map_err(|_| unavailable("browser_storage_host_incompatible"))?;
    if !metadata.is_file() || metadata.len() > 8 * 1024 * 1024 {
        return Err(unavailable("browser_storage_host_incompatible"));
    }
    let mut prefix = vec![0; PREFIX.len()];
    file.read_exact(&mut prefix).map_err(|_| unavailable("browser_storage_host_incompatible"))?;
    if prefix != PREFIX { return Err(unavailable("browser_storage_host_incompatible")); }
    // Compatibility only, never authentication of arbitrary user-configured
    // code. In particular, an older host may ignore inspection arguments and
    // start Chromium, so reject that cohort before executing Node at all.
    Ok(file)
}

#[cfg(target_os = "macos")]
fn validate_root_names(launch: &super::Launch) -> Result<(), BrowserError> {
    let mount = launch.data_root.join("browser-storage-v1/mounted");
    for entry in std::fs::read_dir(&mount).map_err(|_| unavailable("browser_storage_path_invalid"))? {
        let name = entry.map_err(|_| unavailable("browser_storage_path_invalid"))?.file_name();
        if !name.to_str().is_some_and(|name| matches!(name, "profiles" | "home" | "cache" | "config" | "data" | "tmp" | "downloads"
            | ".Trashes" | ".fseventsd" | ".Spotlight-V100" | ".TemporaryItems" | ".DocumentRevisions-V100")) {
            return Err(unavailable("browser_storage_unmanaged_entries"));
        }
    }
    for entry in std::fs::read_dir(mount.join("profiles")).map_err(|_| unavailable("browser_storage_path_invalid"))? {
        if entry.map_err(|_| unavailable("browser_storage_path_invalid"))?.file_name() != super::profile::shared_key().as_str() {
            return Err(unavailable("browser_storage_unmanaged_entries"));
        }
    }
    Ok(())
}

#[cfg(target_os = "macos")]
fn verify_pins(launch: &super::Launch) -> Result<(), BrowserError> {
    use std::os::unix::fs::MetadataExt;
    for (path, directory) in &launch._directories {
        let actual = std::fs::symlink_metadata(path).map_err(|_| unavailable("browser_storage_path_invalid"))?;
        let pinned = directory.metadata().map_err(|_| unavailable("browser_storage_path_invalid"))?;
        if !actual.is_dir() || actual.file_type().is_symlink() || actual.dev() != pinned.dev() || actual.ino() != pinned.ino() {
            return Err(unavailable("browser_storage_path_invalid"));
        }
    }
    Ok(())
}

#[cfg(target_os = "macos")]
fn erase_children(directory: &std::fs::File, device: u64, depth: usize) -> Result<(), BrowserError> {
    use std::ffi::CStr;
    use std::os::fd::{AsRawFd, FromRawFd};
    use std::os::unix::fs::MetadataExt;
    if depth > 256 || directory.metadata().map_err(|_| unavailable("browser_storage_reset_unavailable"))?.dev() != device {
        return Err(unavailable("browser_storage_reset_unavailable"));
    }
    struct Entries(*mut libc::DIR);
    impl Drop for Entries { fn drop(&mut self) { unsafe { libc::closedir(self.0); } } }
    // dup() would share the readdir offset with the pinned descriptor: a
    // partial failure could make a retry skip old entries. Open '.' afresh.
    let duplicate = unsafe { libc::openat(directory.as_raw_fd(), b".\0".as_ptr().cast(),
        libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC) };
    if duplicate < 0 { return Err(unavailable("browser_storage_reset_unavailable")); }
    let entries = unsafe { libc::fdopendir(duplicate) };
    if entries.is_null() {
        unsafe { libc::close(duplicate); }
        return Err(unavailable("browser_storage_reset_unavailable"));
    }
    let entries = Entries(entries);
    loop {
        unsafe { *libc::__error() = 0; }
        let entry = unsafe { libc::readdir(entries.0) };
        if entry.is_null() {
            if unsafe { *libc::__error() } != 0 { return Err(unavailable("browser_storage_reset_unavailable")); }
            break;
        }
        let name = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) };
        if name.to_bytes() == b"." || name.to_bytes() == b".." { continue; }
        let mut metadata = std::mem::MaybeUninit::<libc::stat>::uninit();
        if unsafe { libc::fstatat(directory.as_raw_fd(), name.as_ptr(), metadata.as_mut_ptr(), libc::AT_SYMLINK_NOFOLLOW) } != 0 {
            return Err(unavailable("browser_storage_reset_unavailable"));
        }
        let metadata = unsafe { metadata.assume_init() };
        if metadata.st_dev as u64 != device { return Err(unavailable("browser_storage_reset_foreign_device")); }
        let is_directory = metadata.st_mode & libc::S_IFMT == libc::S_IFDIR;
        if is_directory {
            let child = unsafe { libc::openat(directory.as_raw_fd(), name.as_ptr(), libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC) };
            if child < 0 { return Err(unavailable("browser_storage_reset_unavailable")); }
            let child = unsafe { std::fs::File::from_raw_fd(child) };
            erase_children(&child, device, depth + 1)?;
        }
        if unsafe { libc::unlinkat(directory.as_raw_fd(), name.as_ptr(), if is_directory { libc::AT_REMOVEDIR } else { 0 }) } != 0 {
            return Err(unavailable("browser_storage_reset_unavailable"));
        }
    }
    // Do not report complete if a filesystem iterator skipped an entry while
    // entries were unlinked, or an unexpected writer created something late.
    unsafe { libc::rewinddir(entries.0); }
    loop {
        unsafe { *libc::__error() = 0; }
        let entry = unsafe { libc::readdir(entries.0) };
        if entry.is_null() {
            if unsafe { *libc::__error() } != 0 { return Err(unavailable("browser_storage_reset_unavailable")); }
            break;
        }
        let name = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) };
        if name.to_bytes() != b"." && name.to_bytes() != b".." {
            return Err(unavailable("browser_storage_reset_not_empty"));
        }
    }
    directory.sync_all().map_err(|_| unavailable("browser_storage_reset_unavailable"))
}

#[cfg(all(test, target_os = "macos"))]
mod tests {
    use super::*;
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt, symlink};

    fn open_directory(path: &Path) -> std::fs::File {
        std::fs::OpenOptions::new().read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW).open(path).unwrap()
    }

    fn journal(complete: bool) -> Journal {
        Journal { version: 1, generation: uuid::Uuid::new_v4(), volume_uuid: uuid::Uuid::new_v4(),
            image_bytes: super::super::IMAGE_BYTES, quota_bytes: 402_653_184, complete }
    }

    #[test]
    fn an_old_or_linked_host_is_rejected_before_an_inspection_process_can_start() {
        let root = tempfile::tempdir().unwrap();
        let old = root.path().join("old-host.cjs");
        std::fs::write(&old, b"#!/usr/bin/env node\n// old main ignores arguments\n").unwrap();
        assert!(verify_inspector_script(&old).is_err());
        let current = root.path().join("current-host.cjs");
        std::fs::write(&current, b"#!/usr/bin/env node\n// BAMBOO_BROWSER_STORAGE_INSPECTION_V1\n").unwrap();
        assert!(verify_inspector_script(&current).is_ok());
        let linked = root.path().join("linked.cjs"); symlink(current, &linked).unwrap();
        assert!(verify_inspector_script(&linked).is_err());
    }

    #[test]
    fn pending_reset_survives_owner_loss_and_only_complete_identity_admits_launch() {
        let home = tempfile::tempdir().unwrap();
        let root = home.path().join("browser-storage-v1");
        std::fs::create_dir(&root).unwrap();
        let mut journal = journal(false);
        write_journal(&root, &journal).unwrap();
        assert!(launch_volume_uuid(home.path()).is_err());
        journal.complete = true;
        write_journal(&root, &journal).unwrap();
        assert_eq!(launch_volume_uuid(home.path()).unwrap(), Some(journal.volume_uuid.to_string()));
        assert_eq!(std::fs::read_dir(root).unwrap().count(), 1);
    }

    #[test]
    fn malformed_oversized_or_linked_reset_journal_never_admits_a_launch() {
        let home = tempfile::tempdir().unwrap();
        let root = home.path().join("browser-storage-v1");
        std::fs::create_dir(&root).unwrap();
        let marker = root.join(JOURNAL);
        write_journal(&root, &journal(false)).unwrap();
        for bytes in [vec![b'x'; 1025], b"{\"complete\":true}".to_vec()] {
            std::fs::write(&marker, bytes).unwrap();
            assert!(launch_volume_uuid(home.path()).is_err());
        }
        std::fs::remove_file(&marker).unwrap();
        let outside = home.path().join("outside-marker");
        std::fs::write(&outside, b"preserved").unwrap();
        symlink(&outside, &marker).unwrap();
        assert!(launch_volume_uuid(home.path()).is_err());
        assert_eq!(std::fs::read(outside).unwrap(), b"preserved");
    }

    #[test]
    fn reset_erasure_keeps_role_and_never_follows_a_synthetic_outside_symlink() {
        let role = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::fs::write(outside.path().join("sentinel"), b"untouched synthetic sibling").unwrap();
        std::fs::create_dir(role.path().join("nested")).unwrap();
        std::fs::write(role.path().join("nested/site-data"), b"synthetic").unwrap();
        symlink(outside.path(), role.path().join("outside-link")).unwrap();
        let directory = open_directory(role.path());
        let device = directory.metadata().unwrap().dev();
        erase_children(&directory, device, 0).unwrap();
        assert!(role.path().is_dir());
        assert_eq!(std::fs::read_dir(role.path()).unwrap().count(), 0);
        assert_eq!(std::fs::read(outside.path().join("sentinel")).unwrap(), b"untouched synthetic sibling");
        // The second traversal must use a fresh directory offset, not dup()'s
        // shared offset from the first successful/partially failed traversal.
        std::fs::write(role.path().join("second-generation"), b"synthetic retry").unwrap();
        erase_children(&directory, device, 0).unwrap();
        assert_eq!(std::fs::read_dir(role.path()).unwrap().count(), 0);
    }

    #[test]
    fn reset_erasure_refuses_an_unexpected_device_before_touching_any_file() {
        let role = tempfile::tempdir().unwrap();
        let marker = role.path().join("sentinel");
        std::fs::write(&marker, b"preserved").unwrap();
        let directory = open_directory(role.path());
        assert!(erase_children(&directory, directory.metadata().unwrap().dev() + 1, 0).is_err());
        assert_eq!(std::fs::read(marker).unwrap(), b"preserved");
    }

    #[test]
    fn reset_pin_replacement_is_refused_without_touching_either_generation() {
        let home = tempfile::tempdir().unwrap();
        let role = home.path().join("role");
        std::fs::create_dir(&role).unwrap();
        std::fs::write(role.join("sentinel"), b"original").unwrap();
        let directory = open_directory(&role);
        let mut launch = super::super::layout(&home.path().join("profiles").join(super::super::profile::shared_key())).unwrap();
        launch._directories.push((role.clone(), directory));
        let old = home.path().join("old-role");
        std::fs::rename(&role, &old).unwrap();
        std::fs::create_dir(&role).unwrap();
        std::fs::write(role.join("sentinel"), b"replacement").unwrap();
        assert!(verify_pins(&launch).is_err());
        assert_eq!(std::fs::read(old.join("sentinel")).unwrap(), b"original");
        assert_eq!(std::fs::read(role.join("sentinel")).unwrap(), b"replacement");
    }
}
