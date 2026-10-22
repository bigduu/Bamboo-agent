//! Native bounded storage preparation/admission; never migrate or evict data.
//! The packaged host performs the live native quota/backing inspection before
//! loading Playwright. These pinned paths do not replace that OS inspection.

use std::path::{Path, PathBuf};

use super::BrowserError;
#[cfg(any(target_os = "macos", test))]
use super::profile;

pub(super) mod reset;
#[cfg(target_os = "macos")]
mod prepare;

#[cfg(target_os = "macos")]
const IMAGE_BYTES: u64 = 536_870_912;

pub(super) struct Launch {
    data_root: PathBuf,
    control: PathBuf,
    profile: PathBuf,
    home: PathBuf,
    cache: PathBuf,
    config: PathBuf,
    data: PathBuf,
    pub(super) temporary: PathBuf,
    // Keep the actually opened mount/path descriptors with the account lease,
    // including quarantine. No Drop implementation detaches or removes them.
    _directories: Vec<(PathBuf, std::fs::File)>,
    _image: Option<std::fs::File>,
    reset_volume_uuid: Option<String>,
}

fn unavailable(reason: &str) -> BrowserError {
    BrowserError::Unavailable(reason.into())
}

#[cfg(any(target_os = "macos", test))]
fn layout(control: &Path) -> Result<Launch, BrowserError> {
    if !control.is_absolute() || control.file_name().and_then(|name| name.to_str())
        != Some(profile::shared_key().as_str())
    {
        return Err(unavailable("browser_storage_control_invalid"));
    }
    let data_root = control
        .parent()
        .and_then(Path::parent)
        .ok_or_else(|| unavailable("browser_storage_control_invalid"))?
        .to_path_buf();
    let mount = data_root.join("browser-storage-v1").join("mounted");
    Ok(Launch {
        data_root,
        control: control.to_path_buf(),
        profile: mount.join("profiles").join(profile::shared_key()),
        home: mount.join("home"),
        cache: mount.join("cache"),
        config: mount.join("config"),
        data: mount.join("data"),
        temporary: mount.join("tmp"),
        _directories: Vec::new(),
        _image: None,
        reset_volume_uuid: None,
    })
}

impl Launch {
    #[cfg(target_os = "macos")]
    pub(super) fn prepare(control: &Path) -> Result<Self, BrowserError> {
        prepare::prepare(control)
    }

    #[cfg(not(target_os = "macos"))]
    pub(super) fn prepare(control: &Path) -> Result<Self, BrowserError> {
        Self::admit(control)
    }

    #[cfg(not(target_os = "macos"))]
    pub(super) fn admit(_control: &Path) -> Result<Self, BrowserError> {
        Err(unavailable("browser_storage_provider_unsupported"))
    }

    #[cfg(target_os = "macos")]
    pub(super) fn admit(control: &Path) -> Result<Self, BrowserError> {
        let mut launch = Self::pin_existing(control, true)?;
        let registered = prepare::registered_uuid(&launch.data_root)?;
        let reset = reset::launch_volume_uuid(&launch.data_root)?;
        let actual = reset::inspect_for_launch(&launch)?.volume_uuid;
        if registered.is_some_and(|uuid| uuid != actual)
            || reset.is_some_and(|uuid| uuid != actual.to_string()) {
            return Err(unavailable("browser_storage_backing_identity_changed"));
        }
        launch.reset_volume_uuid = Some(actual.to_string());
        Ok(launch)
    }

    #[cfg(target_os = "macos")]
    fn pin_existing(control: &Path, check_control: bool) -> Result<Self, BrowserError> {
        use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};

        let mut launch = layout(control)?;
        // Inspect names only; never read cookies or import an old database.
        if check_control {
            for entry in std::fs::read_dir(control)
                .map_err(|_| unavailable("browser_storage_control_invalid"))?
            {
                let entry = entry.map_err(|_| unavailable("browser_storage_control_invalid"))?;
                if entry.file_name() != ".bamboo-profile.lock" {
                    return Err(unavailable("browser_storage_migration_required"));
                }
            }
        }
        let root = launch.data_root.join("browser-storage-v1");
        let image = root.join("account.dmg");
        let image_file = std::fs::OpenOptions::new().read(true).custom_flags(libc::O_NOFOLLOW).open(&image)
            .map_err(|_| unavailable("browser_storage_provider_not_ready"))?;
        let image_metadata = image_file.metadata().map_err(|_| unavailable("browser_storage_backing_invalid"))?;
        if !image_metadata.is_file()
            || image_metadata.file_type().is_symlink()
            || image_metadata.len() != IMAGE_BYTES
            || image_metadata.nlink() != 1
            || image_metadata.uid() != unsafe { libc::geteuid() }
            || image_metadata.permissions().mode() & 0o777 != 0o600
            || std::fs::canonicalize(&image).ok().as_deref() != Some(image.as_path())
        {
            return Err(unavailable("browser_storage_backing_invalid"));
        }
        let mount = root.join("mounted");
        let mount_metadata = std::fs::symlink_metadata(&mount)
            .map_err(|_| unavailable("browser_storage_provider_not_ready"))?;
        if mount_metadata.dev() == image_metadata.dev() {
            return Err(unavailable("browser_storage_provider_not_ready"));
        }
        let control_metadata = std::fs::symlink_metadata(control.parent().ok_or_else(||
            unavailable("browser_storage_control_invalid"))?)
            .map_err(|_| unavailable("browser_storage_control_invalid"))?;
        if control_metadata.dev() == mount_metadata.dev() {
            return Err(unavailable("browser_storage_control_inside_quota"));
        }
        let downloads = mount.join("downloads");
        let profiles = mount.join("profiles");
        for path in [
            &launch.data_root,
            &root,
            &mount,
            &profiles,
            &launch.profile,
            &launch.home,
            &launch.cache,
            &launch.config,
            &launch.data,
            &launch.temporary,
            &downloads,
        ] {
            let directory = std::fs::OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
                .open(path)
                .map_err(|_| unavailable("browser_storage_path_invalid"))?;
            let metadata = directory
                .metadata()
                .map_err(|_| unavailable("browser_storage_path_invalid"))?;
            if metadata.uid() != unsafe { libc::geteuid() }
                || (if path == &launch.data_root {
                    let mode = metadata.permissions().mode() & 0o777;
                    mode & 0o700 != 0o700 || mode & 0o022 != 0
                } else { metadata.permissions().mode() & 0o777 != 0o700 })
                || std::fs::canonicalize(path).ok().as_deref() != Some(path.as_path())
                || (path.starts_with(&mount) && metadata.dev() != mount_metadata.dev())
            {
                return Err(unavailable("browser_storage_path_invalid"));
            }
            launch._directories.push((path.to_path_buf(), directory));
        }
        launch._image = Some(image_file);
        Ok(launch)
    }

    pub(super) fn configure(&self, command: &mut tokio::process::Command) {
        command
            .env("BAMBOO_BROWSER_STORAGE_DATA_ROOT", &self.data_root)
            .env("BAMBOO_BROWSER_PROFILE_CONTROL_DIR", &self.control)
            .env("BAMBOO_BROWSER_PROFILE_DIR", &self.profile)
            .env("HOME", &self.home)
            .env("XDG_CACHE_HOME", &self.cache)
            .env("XDG_CONFIG_HOME", &self.config)
            .env("XDG_DATA_HOME", &self.data);
        if let Some(uuid) = &self.reset_volume_uuid {
            command.env("BAMBOO_BROWSER_STORAGE_VOLUME_UUID", uuid);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stable_layout_keeps_control_outside_one_account_storage() {
        let root = tempfile::tempdir().unwrap();
        let root = root.path().canonicalize().unwrap();
        let control = root.join("browser-profiles").join(profile::shared_key());
        let one = layout(&control).unwrap();
        let two = layout(&control).unwrap();
        assert_eq!(one.profile, two.profile);
        assert!(!one.control.starts_with(root.join("browser-storage-v1")));
        assert!(one.temporary.starts_with(root.join("browser-storage-v1")));
        assert!(!root.join("browser-storage-v1").exists());
    }

    #[test]
    fn invalid_control_cannot_select_arbitrary_profile_or_create_files() {
        assert!(layout(Path::new("relative")).is_err());
        let root = tempfile::tempdir().unwrap();
        assert!(layout(&root.path().join("arbitrary")).is_err());
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn production_manager_refuses_unsafe_storage_before_native_commands_or_node() {
        let home = tempfile::tempdir().unwrap();
        let home = home.path().canonicalize().unwrap();
        let manager = super::super::BrowserManager::with_profile_root(home.join("browser-profiles"));
        // A regular sentinel cannot be a storage root. This regression must
        // never create/attach an image when ordinary unit tests are selected.
        std::fs::write(home.join("browser-storage-v1"), b"synthetic preserve").unwrap();
        let outcome = manager.open("new-production-storage-admission").await;
        assert!(matches!(outcome, Err(BrowserError::Unavailable(reason))
            if reason.starts_with("browser_storage_")));
        assert!(manager.lifecycle.owners.lock().unwrap().is_empty());
        assert_eq!(std::fs::read(home.join("browser-storage-v1")).unwrap(), b"synthetic preserve");
        manager.shutdown().await;
    }

    #[test]
    fn reset_does_not_silently_abandon_bounded_data_or_modify_its_backing() {
        let home = tempfile::tempdir().unwrap();
        let storage = home.path().join("browser-storage-v1");
        std::fs::create_dir(&storage).unwrap();
        let marker = storage.join("synthetic-backing-not-a-disk-image");
        std::fs::write(&marker, b"must remain unchanged").unwrap();
        assert!(reset::Owner::prepare(&home.path().join("browser-profiles")).is_err());
        assert_eq!(std::fs::read(marker).unwrap(), b"must remain unchanged");
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn missing_backing_refuses_before_scratch_creation() {
        let root = tempfile::tempdir().unwrap();
        let root = root.path().canonicalize().unwrap();
        let owner = profile::acquire_shared(&root.join("browser-profiles")).unwrap();
        assert!(matches!(Launch::admit(&owner.path), Err(BrowserError::Unavailable(reason))
            if reason == "browser_storage_provider_not_ready"));
        assert!(!root.join("browser-storage-v1").exists());
    }

    #[cfg(not(target_os = "macos"))]
    #[test]
    fn unqualified_platform_explicitly_refuses_without_filesystem_access() {
        assert!(matches!(Launch::admit(Path::new("/no-access-needed")),
            Err(BrowserError::Unavailable(reason)) if reason == "browser_storage_provider_unsupported"));
    }
}
