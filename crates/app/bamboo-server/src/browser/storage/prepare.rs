//! Mac first-use and clean-reboot preparation under the native account lease.
//! Pending mutations never auto-replay; no overwrite, migration, detach or sudo.
use std::ffi::OsString;
use std::path::Path;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{reset, unavailable, BrowserError, Launch, IMAGE_BYTES};

const RECORD: &str = ".bamboo-storage-preparation-v1.json";
const QUOTA_BYTES: u64 = 402_653_184;
const FREE_SPACE_FLOOR: u64 = 1_073_741_824;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Phase { Creating, Attaching, Ready }

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Journal {
    version: u8,
    generation: uuid::Uuid,
    phase: Phase,
    volume_uuid: Option<uuid::Uuid>,
    image_inode: Option<u64>,
    image_bytes: u64,
    quota_bytes: u64,
}

impl Journal {
    fn fresh() -> Self {
        Self { version: 1, generation: uuid::Uuid::new_v4(), phase: Phase::Creating,
            volume_uuid: None, image_inode: None, image_bytes: IMAGE_BYTES, quota_bytes: QUOTA_BYTES }
    }

    fn validate(&self) -> Result<(), BrowserError> {
        if self.version != 1 || self.generation.is_nil() || self.image_bytes != IMAGE_BYTES
            || self.quota_bytes != QUOTA_BYTES || self.image_inode == Some(0)
            || self.volume_uuid.is_some_and(|uuid| uuid.is_nil())
            || (self.phase == Phase::Ready && (self.volume_uuid.is_none() || self.image_inode.is_none())) {
            return Err(unavailable("browser_storage_preparation_record_invalid"));
        }
        Ok(())
    }
}

fn read_journal(root: &Path) -> Result<Option<Journal>, BrowserError> {
    let journal = reset::read_control_record::<Journal>(root, RECORD)?;
    if let Some(journal) = &journal { journal.validate()?; }
    Ok(journal)
}

pub(super) fn registered_uuid(data_root: &Path) -> Result<Option<uuid::Uuid>, BrowserError> {
    use std::os::unix::fs::MetadataExt;
    let Some(journal) = read_journal(&data_root.join("browser-storage-v1"))? else { return Ok(None); };
    if journal.phase != Phase::Ready { return Err(unavailable("browser_storage_preparation_incomplete")); }
    let image = open_image(&data_root.join("browser-storage-v1/account.dmg"), false)?;
    if image.metadata().map_err(|_| unavailable("browser_storage_backing_invalid"))?.ino()
        != journal.image_inode.unwrap() {
        return Err(unavailable("browser_storage_backing_identity_changed"));
    }
    Ok(journal.volume_uuid)
}

pub(super) fn guard_reset(root: &Path) -> Result<(), BrowserError> {
    if read_journal(root)?.is_some_and(|journal| journal.phase != Phase::Ready) {
        // A formatting/attachment process may still exist after a caller or
        // whole application disappears. Reset must not assume its exit.
        return Err(unavailable("browser_storage_preparation_incomplete"));
    }
    Ok(())
}

#[derive(Debug, PartialEq, Eq)]
enum Action { Create, InspectRegistered }

fn initial_action(journal: Option<&Journal>, image_exists: bool, root_empty: bool) -> Result<Action, BrowserError> {
    match journal {
        Some(journal) => {
            journal.validate()?;
            if journal.phase != Phase::Ready { return Err(unavailable("browser_storage_preparation_incomplete")); }
            if !image_exists { return Err(unavailable("browser_storage_registered_backing_missing")); }
            Ok(Action::InspectRegistered)
        }
        None if !image_exists && root_empty => Ok(Action::Create),
        None if image_exists => Err(unavailable("browser_storage_unregistered_backing")),
        None => Err(unavailable("browser_storage_unmanaged_entries")),
    }
}

/// Caller moves its actual account/reset lease into the blocking owner. The
/// public HTTP waiter can disappear, but this transaction keeps those locks.
pub(super) fn prepare(control: &Path) -> Result<Launch, BrowserError> {
    use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt};
    let plan = super::layout(control)?;
    let _data = open_directory(&plan.data_root, false)?;
    let _control = open_directory(control, true)?;
    if std::fs::read_dir(control).map_err(|_| unavailable("browser_storage_control_invalid"))?
        .any(|entry| entry.map_or(true, |entry| entry.file_name() != ".bamboo-profile.lock")) {
        return Err(unavailable("browser_storage_migration_required"));
    }
    let root = plan.data_root.join("browser-storage-v1");
    match std::fs::symlink_metadata(&root) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            check_space(&plan.data_root)?;
            std::fs::DirBuilder::new().mode(0o700).create(&root)
                .map_err(|_| unavailable("browser_storage_preparation_failed"))?;
            super::profile::sync_directory(&plan.data_root)?;
        }
        Err(_) => return Err(unavailable("browser_storage_path_invalid")),
        Ok(_) => {}
    }
    let _root = open_directory(&root, true)?;
    // Only read bounded records after their actual containing root is pinned.
    // Incomplete reset is not permission to initialize a fresh empty profile.
    let _ = reset::launch_volume_uuid(&plan.data_root)?;
    for entry in std::fs::read_dir(&root).map_err(|_| unavailable("browser_storage_path_invalid"))? {
        let name = entry.map_err(|_| unavailable("browser_storage_path_invalid"))?.file_name();
        if !name.to_str().is_some_and(|name| matches!(name, "account.dmg" | "mounted"
            | ".bamboo-storage-preparation-v1.json" | ".bamboo-storage-reset-v1.json")) {
            return Err(unavailable("browser_storage_unmanaged_entries"));
        }
    }
    let image = root.join("account.dmg");
    let mount = root.join("mounted");
    let image_exists = match std::fs::symlink_metadata(&image) {
        Ok(_) => true,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
        Err(_) => return Err(unavailable("browser_storage_backing_invalid")),
    };
    let journal = read_journal(&root)?;
    let root_empty = std::fs::read_dir(&root).map_err(|_| unavailable("browser_storage_path_invalid"))?
        .next().is_none();
    let action = initial_action(journal.as_ref(), image_exists, root_empty)?;
    let fresh = action == Action::Create;
    let mut journal = journal.unwrap_or_else(Journal::fresh);
    if fresh {
        check_space(&plan.data_root)?;
        reset::write_control_record(&root, RECORD, &journal)?;
        // hdiutil defaults to refusing an existing output. Never pass -ov or
        // delete a prior file, even when a previous attempt was unsuccessful.
        run_hdiutil(&create_arguments(&plan.data_root, &image)?, Duration::from_secs(60))?;
    }
    let image_file = open_image(&image, fresh)?;
    let image_metadata = image_file.metadata().map_err(|_| unavailable("browser_storage_backing_invalid"))?;
    if journal.image_inode.is_some_and(|inode| inode != image_metadata.ino()) {
        return Err(unavailable("browser_storage_backing_identity_changed"));
    }
    let backing: Backing = reset::read_native_receipt(&plan.data_root, "--storage-backing-inspect-v1")?;
    if backing.kind != "validated_mac_storage_backing_observation_v1" || backing.image_bytes != IMAGE_BYTES
        || backing.image_inode != image_metadata.ino() || backing.image_device != image_metadata.dev() {
        return Err(unavailable("browser_storage_backing_invalid"));
    }
    if fresh && backing.attached {
        return Err(unavailable("browser_storage_preparation_changed"));
    }
    if !backing.attached {
        prepare_mountpoint(&mount, image_metadata.dev())?;
        journal.image_inode = Some(image_metadata.ino());
        journal.phase = Phase::Attaching;
        reset::write_control_record(&root, RECORD, &journal)?;
        run_hdiutil(&attach_arguments(&mount, &image), Duration::from_secs(30))?;
    }
    let mounted = open_directory(&mount, false)?;
    let mounted_metadata = mounted.metadata().map_err(|_| unavailable("browser_storage_path_invalid"))?;
    if mounted_metadata.dev() == image_metadata.dev() {
        return Err(unavailable("browser_storage_provider_not_ready"));
    }
    let inspected = reset::inspect_for_preparation(&plan.data_root, mounted_metadata.dev())?;
    if journal.volume_uuid.is_some_and(|uuid| uuid != inspected.volume_uuid) {
        return Err(unavailable("browser_storage_backing_identity_changed"));
    }
    if fresh {
        if inspected.paths_absent != Some(true) {
            return Err(unavailable("browser_storage_preparation_changed"));
        }
        // Only this newly formatted, OS-bound, empty volume is initialized.
        // Never chmod an existing user directory or an already-used volume.
        if inspected.mount_mode == Some(0o755) {
            mounted.set_permissions(std::fs::Permissions::from_mode(0o700))
                .map_err(|_| unavailable("browser_storage_preparation_failed"))?;
        }
        let directories = [mount.join("profiles"), plan.profile.clone(), plan.home.clone(),
            plan.cache.clone(), plan.config.clone(), plan.data.clone(), plan.temporary.clone(), mount.join("downloads")];
        for directory in directories {
            std::fs::DirBuilder::new().mode(0o700).create(&directory)
                .map_err(|_| unavailable("browser_storage_preparation_changed"))?;
            let opened = open_directory(&directory, true)?;
            if opened.metadata().map_err(|_| unavailable("browser_storage_path_invalid"))?.dev() != mounted_metadata.dev() {
                return Err(unavailable("browser_storage_path_invalid"));
            }
            opened.sync_all().map_err(|_| unavailable("browser_storage_preparation_failed"))?;
            super::profile::sync_directory(directory.parent().unwrap())?;
        }
    } else if inspected.mount_mode != Some(0o700) || inspected.paths_complete != Some(true) {
        // A restart must retain all old roles, not synthesize replacements for
        // missing data or relax their privacy to make admission pass.
        return Err(unavailable("browser_storage_existing_layout_invalid"));
    }
    let pinned = Launch::pin_existing(control, true)?;
    if pinned._directories[2].1.metadata().map_err(|_| unavailable("browser_storage_path_invalid"))?.dev() != mounted_metadata.dev()
        || std::fs::symlink_metadata(&image).map_err(|_| unavailable("browser_storage_backing_invalid"))?.ino() != image_metadata.ino() {
        return Err(unavailable("browser_storage_backing_identity_changed"));
    }
    if action == Action::InspectRegistered && backing.attached {
        return Launch::admit(control);
    }
    // Ready means preparation completed, not that a full volume can launch.
    // Keep ready on ENOSPC so an explicit reset can inspect/reclaim that volume.
    journal.phase = Phase::Ready;
    journal.volume_uuid = Some(inspected.volume_uuid);
    journal.image_inode = Some(image_metadata.ino());
    reset::write_control_record(&root, RECORD, &journal)?;
    Launch::admit(control)
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Backing { kind: String, image_bytes: u64, image_inode: u64, image_device: u64, attached: bool }

fn open_directory(path: &Path, private: bool) -> Result<std::fs::File, BrowserError> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
    let directory = std::fs::OpenOptions::new().read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW).open(path)
        .map_err(|_| unavailable("browser_storage_path_invalid"))?;
    let metadata = directory.metadata().map_err(|_| unavailable("browser_storage_path_invalid"))?;
    let mode = metadata.permissions().mode() & 0o777;
    if metadata.uid() != unsafe { libc::geteuid() }
        || (if private { mode != 0o700 } else { mode & 0o700 != 0o700 || mode & 0o022 != 0 })
        || std::fs::canonicalize(path).ok().as_deref() != Some(path) {
        return Err(unavailable("browser_storage_path_invalid"));
    }
    Ok(directory)
}

fn open_image(path: &Path, newly_created: bool) -> Result<std::fs::File, BrowserError> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
    let image = std::fs::OpenOptions::new().read(true).custom_flags(libc::O_NOFOLLOW).open(path)
        .map_err(|_| unavailable("browser_storage_backing_invalid"))?;
    let metadata = image.metadata().map_err(|_| unavailable("browser_storage_backing_invalid"))?;
    let mode = metadata.permissions().mode() & 0o777;
    if !metadata.is_file() || metadata.len() != IMAGE_BYTES || metadata.nlink() != 1
        || !(mode == 0o600 || (newly_created && mode == 0o644)) || metadata.uid() != unsafe { libc::geteuid() }
        || std::fs::canonicalize(path).ok().as_deref() != Some(path) {
        return Err(unavailable("browser_storage_backing_invalid"));
    }
    // hdiutil helpers need not inherit the child umask. Tighten only the file
    // just created successfully in our private, previously empty directory.
    if newly_created && mode != 0o600 {
        image.set_permissions(std::fs::Permissions::from_mode(0o600))
            .map_err(|_| unavailable("browser_storage_preparation_failed"))?;
    }
    if newly_created {
        image.sync_all().map_err(|_| unavailable("browser_storage_preparation_failed"))?;
    }
    Ok(image)
}

fn prepare_mountpoint(path: &Path, image_device: u64) -> Result<(), BrowserError> {
    use std::os::unix::fs::{DirBuilderExt, MetadataExt};
    match std::fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            std::fs::DirBuilder::new().mode(0o700).create(path).map_err(|_| unavailable("browser_storage_path_invalid"))?;
            super::profile::sync_directory(path.parent().unwrap())?;
        }
        Err(_) => return Err(unavailable("browser_storage_path_invalid")),
        Ok(_) => {}
    }
    let directory = open_directory(path, true)?;
    if directory.metadata().map_err(|_| unavailable("browser_storage_path_invalid"))?.dev() != image_device
        || std::fs::read_dir(path).map_err(|_| unavailable("browser_storage_path_invalid"))?.next().is_some() {
        return Err(unavailable("browser_storage_mountpoint_busy"));
    }
    Ok(())
}

fn check_space(path: &Path) -> Result<(), BrowserError> {
    // Admission pressure floor only. The fixed backing and kernel volume
    // quota enforce storage bounds; a free-space sample is not a hard quota.
    if fs2::available_space(path).map_err(|_| unavailable("browser_storage_space_unknown"))?
        < IMAGE_BYTES + FREE_SPACE_FLOOR {
        return Err(unavailable("browser_storage_space_insufficient"));
    }
    Ok(())
}

fn create_arguments(data_root: &Path, image: &Path) -> Result<Vec<OsString>, BrowserError> {
    let data_root = data_root.to_str().ok_or_else(|| unavailable("browser_storage_control_invalid"))?;
    let hash: String = Sha256::digest(data_root.as_bytes()).iter().map(|byte| format!("{byte:02x}")).collect();
    let volume = format!("BambooBrowser_{}", &hash[..12]);
    Ok(["create", "-size", "512m", "-type", "UDIF", "-fs", "APFS", "-fsargs", "-q 402653184", "-volname"]
        .into_iter().map(OsString::from).chain([volume.into(), image.as_os_str().to_owned()]).collect())
}

fn attach_arguments(mount: &Path, image: &Path) -> Vec<OsString> {
    ["attach", "-plist", "-nobrowse", "-mountpoint"].into_iter().map(OsString::from)
        .chain([mount.as_os_str().to_owned(), image.as_os_str().to_owned()]).collect()
}

#[cfg(not(test))]
fn run_hdiutil(arguments: &[OsString], limit: Duration) -> Result<(), BrowserError> {
    use std::os::unix::process::CommandExt;
    use std::process::{Command, Stdio};
    use std::time::Instant;
    let mut command = Command::new("/usr/bin/hdiutil");
    command.args(arguments).env_clear().env("PATH", "/usr/bin:/bin:/usr/sbin:/sbin").env("LC_ALL", "C")
        .stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::null());
    // Child-local creation mask only. No parent umask or OS setting changes.
    unsafe { command.pre_exec(|| { libc::umask(0o077); Ok(()) }); }
    let mut child = command.spawn().map_err(|_| unavailable("browser_storage_preparation_failed"))?;
    let deadline = Instant::now() + limit;
    loop {
        match child.try_wait() {
            Ok(Some(status)) if status.success() => return Ok(()),
            Ok(Some(_)) => return Err(unavailable("browser_storage_preparation_failed")),
            Ok(None) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(10)),
            _ => {
                // Only this owned hdiutil handle is stopped. DiskImages may
                // still be completing an operation: the durable pending fence
                // stays, and nothing retries, unmounts or starts Chromium.
                let _ = child.kill(); let _ = child.wait();
                return Err(unavailable("browser_storage_preparation_outcome_unconfirmed"));
            }
        }
    }
}

#[cfg(test)]
thread_local! {
    static MUTATION_ATTEMPTS: std::cell::RefCell<Vec<(Vec<OsString>, Option<Phase>)>> = const {
        std::cell::RefCell::new(Vec::new())
    };
}

#[cfg(test)]
fn run_hdiutil(arguments: &[OsString], _limit: Duration) -> Result<(), BrowserError> {
    // Ordinary unit tests can never create or attach an image, even on RED.
    // The source-matched CLI is used separately for an explicitly granted
    // actual fixture. This stub is absent from production binaries.
    let phase = arguments.last().and_then(|image| Path::new(image).parent())
        .and_then(|root| read_journal(root).ok().flatten()).map(|journal| journal.phase);
    MUTATION_ATTEMPTS.with(|attempts| attempts.borrow_mut().push((arguments.to_vec(), phase)));
    Err(unavailable("browser_storage_native_mutations_disabled_in_unit_fixture"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt, symlink};

    #[test]
    fn first_use_selects_create_only_for_an_empty_unregistered_root() {
        assert_eq!(initial_action(None, false, true).unwrap(), Action::Create);
        assert!(initial_action(None, true, false).is_err());
        assert!(initial_action(None, false, false).is_err());
    }

    #[test]
    fn ready_backing_loss_never_becomes_a_fresh_create() {
        let mut journal = Journal::fresh(); journal.phase = Phase::Ready;
        journal.volume_uuid = Some(uuid::Uuid::new_v4()); journal.image_inode = Some(71);
        assert!(matches!(initial_action(Some(&journal), false, false), Err(BrowserError::Unavailable(code))
            if code == "browser_storage_registered_backing_missing"));
        assert_eq!(initial_action(Some(&journal), true, false).unwrap(), Action::InspectRegistered);
    }

    #[test]
    fn incomplete_create_and_attach_refuse_restart_and_reset_without_native_commands() {
        let home = tempfile::tempdir().unwrap();
        let root = home.path().join("browser-storage-v1");
        std::fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
        MUTATION_ATTEMPTS.with(|attempts| attempts.borrow_mut().clear());
        for phase in [Phase::Creating, Phase::Attaching] {
            let mut journal = Journal::fresh(); journal.phase = phase;
            reset::write_control_record(&root, RECORD, &journal).unwrap();
            assert!(initial_action(Some(&journal), true, false).is_err());
            assert!(registered_uuid(home.path()).is_err());
            assert!(guard_reset(&root).is_err());
        }
        assert!(MUTATION_ATTEMPTS.with(|attempts| attempts.borrow().is_empty()));
        assert!(!root.join("account.dmg").exists());
    }

    #[test]
    fn preparation_record_rejects_unknown_or_incomplete_identity() {
        let mut journal = Journal::fresh(); journal.phase = Phase::Ready;
        assert!(journal.validate().is_err());
        journal.volume_uuid = Some(uuid::Uuid::new_v4()); journal.image_inode = Some(71);
        assert!(journal.validate().is_ok());
        let mut value = serde_json::to_value(&journal).unwrap(); value["accepted"] = serde_json::json!(true);
        assert!(serde_json::from_value::<Journal>(value).is_err());
        journal.quota_bytes += 1;
        assert!(journal.validate().is_err());
    }

    #[test]
    fn native_commands_match_the_fixed_layout_without_overwrite_force_or_privilege_flags() {
        let arguments = create_arguments(Path::new("/synthetic/bamboo"), Path::new("/synthetic/bamboo/browser-storage-v1/account.dmg")).unwrap();
        let args: Vec<_> = arguments.iter().map(|value| value.to_str().unwrap()).collect();
        assert_eq!(&args[..10], ["create", "-size", "512m", "-type", "UDIF", "-fs", "APFS", "-fsargs", "-q 402653184", "-volname"]);
        let tag: String = Sha256::digest(b"/synthetic/bamboo").iter().map(|byte| format!("{byte:02x}")).collect();
        assert_eq!(args[10], format!("BambooBrowser_{}", &tag[..12]));
        assert_eq!(args[11], "/synthetic/bamboo/browser-storage-v1/account.dmg");
        assert_eq!(attach_arguments(Path::new("/synthetic/mounted"), Path::new("/synthetic/account.dmg")),
            ["attach", "-plist", "-nobrowse", "-mountpoint", "/synthetic/mounted", "/synthetic/account.dmg"].map(OsString::from));
        assert!(!args.iter().any(|value| ["-ov", "-force", "sudo"].contains(value)));
    }

    #[test]
    fn occupied_linked_or_public_mountpoint_is_not_cleaned_or_chmodded() {
        let home = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let point = home.path().join("mounted");
        std::fs::write(outside.path().join("sentinel"), b"preserved").unwrap();
        let device = std::fs::metadata(home.path()).unwrap().dev();
        symlink(outside.path(), &point).unwrap();
        assert!(prepare_mountpoint(&point, device).is_err());
        std::fs::remove_file(&point).unwrap();
        std::fs::DirBuilder::new().mode(0o700).create(&point).unwrap();
        std::fs::write(point.join("sentinel"), b"preserved").unwrap();
        assert!(prepare_mountpoint(&point, device).is_err());
        std::fs::remove_file(point.join("sentinel")).unwrap();
        std::fs::set_permissions(&point, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(prepare_mountpoint(&point, device).is_err());
        assert_eq!(std::fs::metadata(&point).unwrap().permissions().mode() & 0o777, 0o755);
        assert_eq!(std::fs::read(outside.path().join("sentinel")).unwrap(), b"preserved");
    }

    #[test]
    fn first_use_persists_pending_before_a_native_mutation_and_cannot_run_it_in_unit_tests() {
        let home = tempfile::tempdir().unwrap();
        let home = home.path().canonicalize().unwrap();
        let lease = super::super::profile::acquire_shared(&home.join("profiles")).unwrap();
        MUTATION_ATTEMPTS.with(|attempts| attempts.borrow_mut().clear());
        assert!(matches!(prepare(&lease.path), Err(BrowserError::Unavailable(code))
            if code == "browser_storage_native_mutations_disabled_in_unit_fixture"));
        let root = home.join("browser-storage-v1");
        assert_eq!(read_journal(&root).unwrap().unwrap().phase, Phase::Creating);
        MUTATION_ATTEMPTS.with(|attempts| {
            let attempts = attempts.borrow(); assert_eq!(attempts.len(), 1);
            assert_eq!(attempts[0].1, Some(Phase::Creating));
        });
        assert!(!root.join("account.dmg").exists());
        // A second opener cannot replay the failed creation, even though the
        // executor was only a test refusal and no image was ever produced.
        assert!(matches!(prepare(&lease.path), Err(BrowserError::Unavailable(code))
            if code == "browser_storage_preparation_incomplete"));
        assert_eq!(MUTATION_ATTEMPTS.with(|attempts| attempts.borrow().len()), 1);
    }
}
