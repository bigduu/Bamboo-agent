//! Secret-screened output recovery files for the compression runtime.
//!
//! Ordinary output is saved unchanged. Credential-like output items are omitted
//! using the existing AutoDream predicate. Filenames contain no tool arguments.
//! Actual tee writes schedule an opportunistic expiry sweep once an hour.

use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime};

use bamboo_compression::TokenCounter;
use bamboo_config::paths::bamboo_dir;
use tokio::io::AsyncWriteExt;

use crate::auto_dream_privacy::contains_secret_like_value;

const MIN_SAVINGS_BYTES: usize = 500;
const DEFAULT_RETENTION_DAYS: u64 = 30;
const SECONDS_PER_DAY: u64 = 24 * 60 * 60;
const CLEANUP_INTERVAL: Duration = Duration::from_secs(60 * 60);
const RETENTION_ENV: &str = "BAMBOO_TEE_RETENTION_DAYS";
const REDACTED_OUTPUT: &str = "[tee output omitted: credential-like content detected]\n";
static LAST_CLEANUP: Mutex<Option<Instant>> = Mutex::new(None);

/// A Read hint describes screened output, not recovery of secret bytes.
pub(crate) async fn tee_save_if_needed(
    session_id: &str,
    full_output: &str,
    compressed_output: &str,
) -> Option<String> {
    tee_save_if_needed_in(
        &bamboo_dir().join("tee"),
        session_id,
        full_output,
        compressed_output,
        retention_from_days(std::env::var(RETENTION_ENV).ok().as_deref()),
    )
    .await
}

async fn tee_save_if_needed_in(
    root: &Path,
    session_id: &str,
    full_output: &str,
    compressed_output: &str,
    retention: Duration,
) -> Option<String> {
    if full_output.len().saturating_sub(compressed_output.len()) < MIN_SAVINGS_BYTES {
        return None;
    }
    let redacted = contains_secret_like_value(full_output);
    let stored_output = if redacted {
        REDACTED_OUTPUT
    } else {
        full_output
    };
    match tee_save(root, session_id, stored_output).await {
        Ok(path) => {
            schedule_cleanup(root, retention);
            let display_path = bamboo_config::paths::path_to_display_string(&path);
            let tokens =
                bamboo_compression::TiktokenTokenCounter::default().count_text(stored_output);
            let token_display = if tokens >= 1000 {
                format!("{:.1}K", tokens as f64 / 1000.0)
            } else {
                tokens.to_string()
            };
            let screening = if redacted {
                "credential-like output was omitted"
            } else {
                "no credential-like content detected"
            };
            Some(format!(
                "[secret-screened output saved: {} ({} bytes, ~{} tokens; {}). Use Read tool to inspect details. Retention: {} days; expired files are removed on a later tee write.]",
                display_path, stored_output.len(), token_display, screening,
                retention.as_secs() / SECONDS_PER_DAY,
            ))
        }
        Err(error) => {
            tracing::warn!(error_kind = ?error.kind(), "Failed to tee-save output");
            None
        }
    }
}

async fn tee_save(root: &Path, session_id: &str, output: &str) -> std::io::Result<PathBuf> {
    let session_dir = root.join(session_id);
    tokio::fs::create_dir_all(&session_dir).await?;
    let filename = format!(
        "{}_{}.log",
        chrono::Utc::now().format("%Y%m%d_%H%M%S"),
        uuid::Uuid::new_v4()
    );
    let path = session_dir.join(filename);
    let mut options = tokio::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);
    let mut file = options.open(&path).await?;
    file.write_all(output.as_bytes()).await?;
    Ok(path)
}

fn retention_from_days(value: Option<&str>) -> Duration {
    let seconds = value
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|days| *days > 0)
        .and_then(|days| days.checked_mul(SECONDS_PER_DAY))
        .unwrap_or(DEFAULT_RETENTION_DAYS * SECONDS_PER_DAY);
    Duration::from_secs(seconds)
}

fn cleanup_due(last: Option<Instant>, now: Instant) -> bool {
    last.is_none_or(|last| now.saturating_duration_since(last) >= CLEANUP_INTERVAL)
}

fn schedule_cleanup(root: &Path, retention: Duration) {
    let now = Instant::now();
    let mut last = LAST_CLEANUP.lock().expect("tee cleanup timer lock");
    if !cleanup_due(*last, now) {
        return;
    }
    *last = Some(now);
    drop(last);
    let root = root.to_path_buf();
    tokio::spawn(async move {
        if let Err(error) = cleanup_expired(&root, retention, SystemTime::now()).await {
            tracing::warn!(error_kind = ?error.kind(), "Could not clean expired tee output");
        }
    });
}

/// Stream only the tee root's immediate Session directories. Skip symlinks and
/// unrelated entries; old command-derived names may contain secrets, so never log them.
async fn cleanup_expired(
    root: &Path,
    retention: Duration,
    now: SystemTime,
) -> std::io::Result<usize> {
    match tokio::fs::symlink_metadata(root).await {
        Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {}
        Ok(_) => return Ok(0),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(error),
    }
    let mut removed = 0;
    let mut sessions = tokio::fs::read_dir(root).await?;
    while let Some(session) = sessions.next_entry().await? {
        if !session.file_type().await?.is_dir() {
            continue;
        }
        let mut files = tokio::fs::read_dir(session.path()).await?;
        while let Some(file) = files.next_entry().await? {
            if file
                .path()
                .extension()
                .is_none_or(|extension| extension != "log")
                || !file.file_type().await?.is_file()
            {
                continue;
            }
            let metadata = match tokio::fs::symlink_metadata(file.path()).await {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error),
            };
            if !metadata.is_file() || metadata.file_type().is_symlink() {
                continue;
            }
            let expired = metadata
                .modified()
                .ok()
                .and_then(|modified| now.duration_since(modified).ok())
                .is_some_and(|age| age >= retention);
            if expired {
                match tokio::fs::remove_file(file.path()).await {
                    Ok(()) => removed += 1,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error),
                }
            }
        }
    }
    Ok(removed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_retention() -> Duration {
        retention_from_days(None)
    }

    #[test]
    fn retention_accepts_positive_days_and_defaults_for_invalid_values() {
        assert_eq!(
            retention_from_days(Some("7")),
            Duration::from_secs(7 * SECONDS_PER_DAY)
        );
        for value in [
            None,
            Some("0"),
            Some("-1"),
            Some("invalid"),
            Some("18446744073709551615"),
        ] {
            assert_eq!(
                retention_from_days(value),
                Duration::from_secs(30 * SECONDS_PER_DAY)
            );
        }
    }

    #[test]
    fn cleanup_is_due_initially_and_then_hourly() {
        let now = Instant::now();
        assert!(cleanup_due(None, now));
        assert!(!cleanup_due(Some(now), now + Duration::from_secs(3599)));
        assert!(cleanup_due(Some(now), now + CLEANUP_INTERVAL));
    }

    #[tokio::test]
    async fn small_savings_do_not_create_tee_files() {
        let root = tempfile::tempdir().unwrap();
        let note = tee_save_if_needed_in(
            root.path(),
            "small",
            "short output",
            "short out",
            default_retention(),
        )
        .await;
        assert!(note.is_none());
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn screened_output_is_readable_and_preserves_contacts_and_newlines() {
        let root = tempfile::tempdir().unwrap();
        let full = format!(
            "Contact: alice@example.com +86 13800138000\n{}\n",
            "ordinary build output\n".repeat(50)
        );
        let note = tee_save_if_needed_in(
            root.path(),
            "readable",
            &full,
            "summary",
            default_retention(),
        )
        .await
        .unwrap();
        let path = std::fs::read_dir(root.path().join("readable"))
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        assert_eq!(tokio::fs::read_to_string(&path).await.unwrap(), full);
        assert!(note.contains(&bamboo_config::paths::path_to_display_string(&path)));
        assert!(note.contains("Use Read tool"));
        assert!(note.contains("30 days"));
        assert!(note.contains("no credential-like content detected"));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
    }

    #[tokio::test]
    async fn credential_items_are_omitted_before_writing() {
        let root = tempfile::tempdir().unwrap();
        for (index, source) in [
            "api_token: synthetic-credential-value",
            "Authorization: Bearer synthetic-credential-value",
            "sk-proj-syntheticcredentialabcdefghijkl",
            "-----BEGIN PRIVATE KEY-----\nsynthetic-private-material\n-----END PRIVATE KEY-----",
        ]
        .iter()
        .enumerate()
        {
            let session = format!("secret-{index}");
            let full = format!("{}\n{source}", "ordinary output\n".repeat(50));
            let note =
                tee_save_if_needed_in(root.path(), &session, &full, "summary", default_retention())
                    .await
                    .unwrap();
            let path = std::fs::read_dir(root.path().join(session))
                .unwrap()
                .next()
                .unwrap()
                .unwrap()
                .path();
            assert_eq!(
                tokio::fs::read_to_string(&path).await.unwrap(),
                REDACTED_OUTPUT
            );
            assert!(note.contains("credential-like output was omitted"));
            assert!(!path
                .file_name()
                .unwrap()
                .to_string_lossy()
                .contains("credential"));
        }
    }

    #[tokio::test]
    async fn concurrent_writes_have_generated_distinct_names() {
        let root = tempfile::tempdir().unwrap();
        let (first, second) = tokio::join!(
            tee_save(root.path(), "same", "first output"),
            tee_save(root.path(), "same", "second output")
        );
        let first = first.unwrap();
        let second = second.unwrap();
        assert_ne!(first, second);
        assert_eq!(
            tokio::fs::read_to_string(first).await.unwrap(),
            "first output"
        );
        assert_eq!(
            tokio::fs::read_to_string(second).await.unwrap(),
            "second output"
        );
    }

    fn write_at(path: &Path, time: SystemTime) {
        let file = std::fs::File::create(path).unwrap();
        file.set_times(std::fs::FileTimes::new().set_modified(time))
            .unwrap();
    }

    #[tokio::test]
    async fn cleanup_removes_expired_logs_and_preserves_fresh_and_unrelated_entries() {
        let root = tempfile::tempdir().unwrap();
        let session = root.path().join("session");
        std::fs::create_dir(&session).unwrap();
        let now = SystemTime::now();
        let retention = default_retention();
        let old = now - retention - Duration::from_secs(1);
        write_at(&session.join("expired.log"), old);
        write_at(&session.join("fresh.log"), now);
        write_at(&session.join("unrelated.txt"), old);
        write_at(&root.path().join("root.log"), old);
        std::fs::create_dir(session.join("nested")).unwrap();
        write_at(&session.join("nested/untouched.log"), old);
        assert_eq!(
            cleanup_expired(root.path(), retention, now).await.unwrap(),
            1
        );
        assert!(!session.join("expired.log").exists());
        assert!(session.join("fresh.log").exists());
        assert!(session.join("unrelated.txt").exists());
        assert!(root.path().join("root.log").exists());
        assert!(session.join("nested/untouched.log").exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn cleanup_does_not_follow_directory_or_file_symlinks() {
        use std::os::unix::fs::symlink;
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let session = root.path().join("session");
        std::fs::create_dir(&session).unwrap();
        let now = SystemTime::now();
        write_at(
            &outside.path().join("outside.log"),
            now - default_retention() - Duration::from_secs(1),
        );
        symlink(outside.path(), root.path().join("linked-session")).unwrap();
        symlink(
            outside.path().join("outside.log"),
            session.join("linked.log"),
        )
        .unwrap();
        assert_eq!(
            cleanup_expired(root.path(), default_retention(), now)
                .await
                .unwrap(),
            0
        );
        assert!(outside.path().join("outside.log").exists());
        assert!(std::fs::symlink_metadata(session.join("linked.log"))
            .unwrap()
            .file_type()
            .is_symlink());
    }
}
