//! Platform/provider selection only. No native browser or image mutation.
use super::*;

fn platform_manager(platform: &str) -> BrowserManager {
    BrowserManager::with_profile_root_for_platform(
        PathBuf::from("synthetic-not-opened-browser-profiles"),
        platform,
    )
}

#[test]
fn macos_constructor_preserves_bounded_shared_and_clean() {
    let manager = platform_manager("macos");
    assert!(manager.lifecycle.bounded_storage);
    assert!(!manager.lifecycle.ephemeral_clean);
    for mode in [
        None,
        Some(BrowserProfileMode::Shared),
        Some(BrowserProfileMode::Clean),
    ] {
        assert_eq!(
            manager.lifecycle.new_profile_mode(mode).unwrap(),
            mode.unwrap_or_default()
        );
    }
}

fn assert_explicit_clean_only(platform: &str) {
    let manager = platform_manager(platform);
    assert!(!manager.lifecycle.bounded_storage);
    assert!(manager.lifecycle.ephemeral_clean);
    assert_eq!(
        manager
            .lifecycle
            .new_profile_mode(Some(BrowserProfileMode::Clean))
            .unwrap(),
        BrowserProfileMode::Clean
    );
    for mode in [None, Some(BrowserProfileMode::Shared)] {
        assert!(matches!(manager.lifecycle.new_profile_mode(mode),
            Err(BrowserError::Unavailable(reason)) if reason.starts_with("browser_shared_profile_unsupported")));
    }
}

#[test]
fn linux_constructor_restores_only_explicit_temporary_clean() {
    assert_explicit_clean_only("linux");
}

#[test]
fn windows_constructor_restores_only_explicit_temporary_clean() {
    assert_explicit_clean_only("windows");
}

#[test]
fn temporary_clean_command_does_not_pass_account_profile_or_native_storage_settings() {
    for platform in ["linux", "windows"] {
        let manager = platform_manager(platform);
        let mut command = Command::new("synthetic-not-executed");
        command.env("BAMBOO_BROWSER_PROFILE_DIR", "synthetic-never-import");
        command.env("BAMBOO_BROWSER_STORAGE_DATA_ROOT", "synthetic-never-open");
        restrict_host_environment(&mut command);
        manager.lifecycle.configure_profile(
            &mut command,
            Some(std::path::Path::new("ignored-account")),
        );
        let values: HashMap<_, _> = command.as_std().get_envs().collect();
        assert_eq!(
            values.get(std::ffi::OsStr::new("BAMBOO_BROWSER_EPHEMERAL_CLEAN")),
            Some(&Some(std::ffi::OsStr::new("1")))
        );
        for name in [
            "BAMBOO_BROWSER_PROFILE_DIR",
            "BAMBOO_BROWSER_STORAGE_DATA_ROOT",
            "BAMBOO_BROWSER_PROFILE_CONTROL_DIR",
            "BAMBOO_BROWSER_LAUNCH_ATTEMPT",
        ] {
            assert!(!values.contains_key(std::ffi::OsStr::new(name)));
        }
    }
}

#[test]
fn clean_readiness_cannot_claim_account_sharing_or_native_quota() {
    let generation = "a".repeat(24);
    let mut value = json!({"protocol_version":2,"host_generation":generation,
        "browser_instance_id":"b".repeat(24),"scope_count":0,"profile_modes":["clean"],
        "storage_boundary":{"provider":"ephemeral_clean_v1","persistent":false}});
    assert!(shared::valid_handshake(&value, Some(&generation), false, true));
    assert!(!shared::valid_handshake(&value, Some(&generation), true, true));
    value["profile_modes"] = json!(["shared", "clean"]);
    assert!(!shared::valid_handshake(&value, Some(&generation), false, true));
    value["profile_modes"] = json!(["clean"]);
    value["storage_boundary"]["persistent"] = json!(true);
    assert!(!shared::valid_handshake(&value, Some(&generation), false, true));
}

#[test]
fn clean_compatibility_marker_rejects_an_older_host_before_spawn() {
    let root = tempfile::tempdir().unwrap();
    let script = root.path().join("synthetic-host.cjs");
    let prior = "#!/usr/bin/env node\n// BAMBOO_BROWSER_STORAGE_INSPECTION_V1\n// BAMBOO_BROWSER_LAUNCH_ADMISSION_V1\n";
    std::fs::write(&script, prior).unwrap();
    assert!(launch_admission::verify_clean_host(&script).is_err());
    std::fs::write(
        &script,
        format!("{prior}// BAMBOO_BROWSER_EPHEMERAL_CLEAN_V1\n"),
    )
    .unwrap();
    assert!(launch_admission::verify_clean_host(&script).is_ok());
}

#[tokio::test]
async fn unsupported_shared_refuses_before_a_chat_directory_or_host_is_created() {
    let root = tempfile::tempdir().unwrap();
    for platform in ["linux", "windows"] {
        let profile = root.path().join(platform);
        let manager = BrowserManager::with_profile_root_for_platform(profile.clone(), platform);
        for mode in [None, Some(BrowserProfileMode::Shared)] {
            assert!(matches!(manager.open_with_mode("synthetic-chat", mode).await,
                Err(BrowserError::Unavailable(reason)) if reason.starts_with("browser_shared_profile_unsupported")));
        }
        assert!(!profile.exists());
        assert!(manager.lifecycle.owners.lock().unwrap().is_empty());
        manager.shutdown().await;
    }
}

#[cfg(unix)]
#[test]
fn legacy_temporary_clean_scratch_is_private_at_creation() {
    use std::os::unix::fs::PermissionsExt;
    let scratch = create_host_temporary_directory(None).unwrap();
    assert_eq!(
        scratch.path().metadata().unwrap().permissions().mode() & 0o777,
        0o700
    );
}
