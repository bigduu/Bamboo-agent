//! Deleting a chat owns and purges only its browser profile, after host exit.

use super::*;

#[tokio::test]
async fn retiring_a_closed_chat_purges_only_its_persisted_profile() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().join("profiles");
    let deleted = profile::acquire(&root, "deleted").unwrap();
    let path = deleted.path.clone();
    std::fs::create_dir(path.join("Default")).unwrap();
    std::fs::write(path.join("Default/Cookies"), b"synthetic login state").unwrap();
    let other = profile::acquire(&root, "other").unwrap();
    let other_cookie = other.path.join("Cookies");
    std::fs::write(&other_cookie, b"other synthetic state").unwrap();
    drop(deleted);
    let browser = BrowserManager::with_profile_root(root);

    browser.retire("deleted").await.unwrap();
    assert!(!path.exists(), "deleted chat retained its site data");
    assert_eq!(
        std::fs::read(other_cookie).unwrap(),
        b"other synthetic state"
    );
    browser.retire("deleted").await.unwrap();
    assert!(matches!(
        browser.open("deleted").await,
        Err(BrowserError::NotOpen)
    ));
}

#[tokio::test]
async fn retiring_a_missing_chat_does_not_create_profile_directories() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().join("profiles");
    BrowserManager::with_profile_root(root.clone())
        .retire("absent")
        .await
        .unwrap();
    assert!(!root.exists());
}

#[tokio::test]
async fn retiring_a_chat_refuses_to_remove_another_live_owners_profile() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().join("profiles");
    let owner = profile::acquire(&root, "deleted").unwrap();
    let path = owner.path.clone();
    std::fs::write(path.join("Cookies"), b"synthetic state").unwrap();
    let browser = BrowserManager::with_profile_root(root);

    assert!(browser.retire("deleted").await.is_err());
    assert_eq!(
        std::fs::read(path.join("Cookies")).unwrap(),
        b"synthetic state"
    );
    drop(owner);
    browser.retire("deleted").await.unwrap();
    assert!(!path.exists());
}

#[tokio::test]
async fn retiring_an_unconfirmed_closed_profile_purges_its_checkpoint() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().join("profiles");
    let owner = profile::acquire(&root, "deleted").unwrap();
    let path = owner.path.clone();
    std::fs::write(
        path.join(".bamboo-session-cookies-v1.json"),
        b"synthetic state",
    )
    .unwrap();
    owner.begin_close().unwrap();
    drop(owner);
    assert!(profile::acquire(&root, "deleted").is_err());

    BrowserManager::with_profile_root(root)
        .retire("deleted")
        .await
        .unwrap();
    assert!(!path.exists());
}

#[cfg(unix)]
fn closing_host(
    root: &std::path::Path,
    marker: &std::path::Path,
    status: i32,
) -> (Arc<BrowserSession>, PathBuf) {
    let lease = profile::acquire(root, "deleted").unwrap();
    let path = lease.path.clone();
    let mut command = Command::new("/bin/sh");
    command.arg("-c").arg("cat >/dev/null; test -d \"$PROFILE\" || exit 42; : > \"$MARKER\"; sleep 0.15; exit \"$STATUS\"")
        .env("PROFILE", &path).env("MARKER", marker).env("STATUS", status.to_string())
        .stdin(Stdio::piped()).stdout(Stdio::piped()).kill_on_drop(true).process_group(0);
    let session = BrowserSession::from_child(command.spawn().unwrap(), None).unwrap();
    *session.profile_lease.lock().unwrap() = Some(lease);
    (session, path)
}

#[cfg(unix)]
#[tokio::test]
async fn retiring_a_live_chat_reaps_its_host_before_purging_with_old_arcs_alive() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().join("profiles");
    let marker = temporary.path().join("profile-existed-at-eof");
    let (session, path) = closing_host(&root, &marker, 0);
    let browser = BrowserManager::with_profile_root(root);
    browser
        .sessions
        .lock()
        .await
        .insert("deleted".into(), session.clone());

    browser.retire("deleted").await.unwrap();
    assert!(marker.exists());
    assert!(session.reaped.load(Ordering::Acquire));
    assert!(session
        .child
        .lock()
        .unwrap()
        .try_wait()
        .unwrap()
        .unwrap()
        .success());
    assert!(session.profile_lease.lock().unwrap().is_none());
    assert!(!path.exists());
}

#[cfg(unix)]
#[tokio::test]
async fn retiring_a_nonzero_host_exit_still_purges_after_reaping() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().join("profiles");
    let marker = temporary.path().join("profile-existed-at-eof");
    let (session, path) = closing_host(&root, &marker, 23);
    let browser = BrowserManager::with_profile_root(root);
    browser
        .sessions
        .lock()
        .await
        .insert("deleted".into(), session.clone());

    browser.retire("deleted").await.unwrap();
    assert!(marker.exists());
    assert!(session.reaped.load(Ordering::Acquire));
    assert!(!session
        .child
        .lock()
        .unwrap()
        .try_wait()
        .unwrap()
        .unwrap()
        .success());
    assert!(!path.exists());
}

#[cfg(unix)]
#[tokio::test]
async fn cancelled_retirement_waiter_does_not_abandon_profile_deletion() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().join("profiles");
    let marker = temporary.path().join("profile-existed-at-eof");
    let (session, path) = closing_host(&root, &marker, 0);
    let browser = BrowserManager::with_profile_root(root);
    browser
        .sessions
        .lock()
        .await
        .insert("deleted".into(), session.clone());
    let waiter = tokio::spawn(async move { browser.retire("deleted").await });
    tokio::time::timeout(Duration::from_secs(3), async {
        while !marker.exists() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    tokio::time::timeout(Duration::from_secs(3), async {
        while path.exists() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("request cancellation abandoned the deleted chat's profile");
    assert!(session.reaped.load(Ordering::Acquire));
}

#[cfg(unix)]
#[tokio::test]
async fn retirement_rejects_profile_symlinks_and_does_not_follow_internal_links() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().join("profiles");
    let owner = profile::acquire(&root, "deleted").unwrap();
    let path = owner.path.clone();
    drop(owner);
    std::fs::remove_dir_all(&path).unwrap();
    let outside = temporary.path().join("outside");
    std::fs::create_dir(&outside).unwrap();
    let sentinel = outside.join("Cookies");
    std::fs::write(&sentinel, b"outside synthetic state").unwrap();
    std::os::unix::fs::symlink(&outside, &path).unwrap();
    let browser = BrowserManager::with_profile_root(root.clone());
    assert!(browser.retire("deleted").await.is_err());
    assert_eq!(
        std::fs::read(&sentinel).unwrap(),
        b"outside synthetic state"
    );
    std::fs::remove_file(&path).unwrap();
    let owner = profile::acquire(&root, "deleted").unwrap();
    std::os::unix::fs::symlink(&outside, owner.path.join("linked-site-data")).unwrap();
    drop(owner);
    browser.retire("deleted").await.unwrap();
    assert!(!path.exists());
    assert_eq!(
        std::fs::read(&sentinel).unwrap(),
        b"outside synthetic state"
    );
}

#[tokio::test]
async fn retirement_cannot_pass_an_opener_that_has_not_created_its_directory() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().join("profiles");
    let owner = profile::acquire(&root, "deleted").unwrap();
    let path = owner.path.clone();
    drop(owner);
    std::fs::remove_dir_all(&path).unwrap();
    let lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(root.join(".bamboo-profile-leases").join(format!(
            "{}.lock",
            path.file_name().unwrap().to_str().unwrap()
        )))
        .unwrap();
    fs2::FileExt::try_lock_exclusive(&lock).unwrap();
    let browser = BrowserManager::with_profile_root(root);
    assert!(browser.retire("deleted").await.is_err());
    assert!(!path.exists());
    drop(lock);
    browser.retire("deleted").await.unwrap();
    assert!(!path.exists());
}

#[cfg(unix)]
#[tokio::test]
async fn retirement_keeps_the_stable_lock_inode_and_preopened_descriptors_exclusive() {
    use std::os::unix::fs::MetadataExt;
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().join("profiles");
    let owner = profile::acquire(&root, "deleted").unwrap();
    let path = owner.path.clone();
    let stable = root.join(".bamboo-profile-leases").join(format!(
        "{}.lock",
        path.file_name().unwrap().to_str().unwrap()
    ));
    let competing = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&stable)
        .unwrap();
    let before = competing.metadata().unwrap();
    drop(owner);
    BrowserManager::with_profile_root(root.clone())
        .retire("deleted")
        .await
        .unwrap();
    assert!(!path.exists());
    let after = std::fs::metadata(&stable).unwrap();
    assert_eq!((before.dev(), before.ino()), (after.dev(), after.ino()));
    // A descriptor opened before deletion must contend with future owners of
    // this same path, rather than locking a now-unlinked obsolete inode.
    let current = profile::acquire(&root, "deleted").unwrap();
    assert!(fs2::FileExt::try_lock_exclusive(&competing).is_err());
    drop(current);
    fs2::FileExt::try_lock_exclusive(&competing).unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn retirement_io_failure_is_reported_and_can_be_retried() {
    use std::os::unix::fs::PermissionsExt;
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().join("profiles");
    let owner = profile::acquire(&root, "deleted").unwrap();
    let path = owner.path.clone();
    std::fs::write(path.join("Cookies"), b"synthetic state").unwrap();
    drop(owner);
    let browser = BrowserManager::with_profile_root(root);
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o500)).unwrap();
    let result = browser.retire("deleted").await;
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
    assert!(result.is_err());
    assert!(path.join("Cookies").exists());
    browser.retire("deleted").await.unwrap();
    assert!(!path.exists());
}
