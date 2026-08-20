//! Shared persistence and fallback-rehydration helpers for external CLI
//! executors. Provider adapters keep their session-id/state-machine logic, but
//! use these helpers so transcript rendering and atomic state writes do not
//! drift between Claude Code and Codex.

use std::future::Future;
use std::io;
use std::path::Path;
use std::time::Duration;

use serde::Serialize;
use serde_json::Value;
use tokio::io::AsyncWriteExt;
use tokio::process::{Child, Command};

const ETXTBSY_RETRY_ATTEMPTS: usize = 5;
const ETXTBSY_RETRY_DELAY: Duration = Duration::from_millis(10);
#[cfg(unix)]
const ETXTBSY_RAW_OS_ERROR: i32 = 26;

/// Retry a process operation when Unix reports `ETXTBSY` ("text file busy").
///
/// Parallel test threads can briefly inherit another thread's write file
/// descriptor across `fork`, making an otherwise closed executable fail its
/// first `exec`. Production wrappers can hit the same race while being
/// atomically replaced. Only raw Unix error 26 is retryable; every other error
/// remains fail-fast. The operation is awaited inline, so a caller's timeout or
/// cancellation remains an overall bound and cannot leave a retry task behind.
pub(crate) async fn retry_on_etxtbsy<T, Operation, OperationFuture>(
    mut operation: Operation,
) -> io::Result<T>
where
    Operation: FnMut() -> OperationFuture,
    OperationFuture: Future<Output = io::Result<T>>,
{
    for attempt in 0..ETXTBSY_RETRY_ATTEMPTS {
        match operation().await {
            Ok(value) => return Ok(value),
            Err(error) if is_etxtbsy(&error) && attempt + 1 < ETXTBSY_RETRY_ATTEMPTS => {
                tokio::time::sleep(ETXTBSY_RETRY_DELAY).await;
            }
            Err(error) => return Err(error),
        }
    }
    unreachable!("the bounded retry loop always returns on its final attempt")
}

/// Build and spawn a Tokio child with the shared bounded `ETXTBSY` policy.
pub async fn spawn_with_etxtbsy_retry(
    mut build: impl FnMut() -> io::Result<Command>,
) -> io::Result<Child> {
    retry_on_etxtbsy(|| std::future::ready(build().and_then(|mut command| command.spawn()))).await
}

fn is_etxtbsy(error: &io::Error) -> bool {
    #[cfg(unix)]
    {
        error.raw_os_error() == Some(ETXTBSY_RAW_OS_ERROR)
    }
    #[cfg(not(unix))]
    {
        let _ = error;
        false
    }
}

/// Fallback history cap shared by external CLI executors: retain the newest
/// roughly 40 messages and no more than roughly 24k characters.
const HISTORY_PREAMBLE_MAX_CHARS: usize = 24_000;
const HISTORY_PREAMBLE_MAX_MESSAGES: usize = 40;

/// Render a bounded, role-tagged history preamble for a provider whose native
/// session id is unavailable.
///
/// `messages` are oldest-first and may include the current assignment as their
/// trailing user message. That duplicate is removed before rendering. Unknown
/// or malformed entries are skipped defensively.
pub fn render_history_preamble(messages: &[Value], assignment: &str) -> Option<String> {
    let mut entries: Vec<(String, String)> = messages
        .iter()
        .filter_map(|message| {
            let role = message.get("role").and_then(Value::as_str)?.to_string();
            let content = message.get("content").and_then(Value::as_str)?;
            if content.is_empty() {
                return None;
            }
            Some((role, content.to_string()))
        })
        .collect();

    if let Some((role, content)) = entries.last() {
        if role == "user" && content == assignment {
            entries.pop();
        }
    }
    if entries.is_empty() {
        return None;
    }

    let dropped_by_count = entries.len().saturating_sub(HISTORY_PREAMBLE_MAX_MESSAGES);
    if dropped_by_count > 0 {
        entries.drain(0..dropped_by_count);
    }

    let mut rendered: Vec<String> = entries
        .iter()
        .map(|(role, content)| format!("**{role}**: {content}"))
        .collect();
    let mut dropped_by_chars = 0usize;
    while rendered.len() > 1
        && rendered
            .iter()
            .map(|entry| entry.chars().count() + 2)
            .sum::<usize>()
            > HISTORY_PREAMBLE_MAX_CHARS
    {
        rendered.remove(0);
        dropped_by_chars += 1;
    }
    if let [only] = rendered.as_mut_slice() {
        if only.chars().count() > HISTORY_PREAMBLE_MAX_CHARS {
            *only = truncate_chars(only, HISTORY_PREAMBLE_MAX_CHARS);
        }
    }

    let mut out = String::from("## Prior conversation (rehydrated)\n\n");
    if dropped_by_count > 0 || dropped_by_chars > 0 {
        out.push_str(&format!(
            "_[truncated: {} earlier message(s) omitted]_\n\n",
            dropped_by_count + dropped_by_chars
        ));
    }
    out.push_str(&rendered.join("\n\n"));
    Some(out)
}

/// Build a current-turn prompt with fallback history when native resume is not
/// available. The current assignment remains in its own explicit section.
pub fn build_rehydrated_turn(messages: &[Value], assignment: &str) -> String {
    match render_history_preamble(messages, assignment) {
        Some(preamble) => format!("{preamble}\n\n## Current task\n\n{assignment}"),
        None => assignment.to_string(),
    }
}

/// Serialize JSON and replace `path` atomically using a unique temporary file
/// in the same directory. Readers see either the old complete document or the
/// new complete document, never a partially written state file.
pub async fn write_json_atomic<T: Serialize + ?Sized>(
    path: &Path,
    value: &T,
) -> Result<(), String> {
    let bytes = serde_json::to_vec_pretty(value)
        .map_err(|error| format!("serialize executor state '{}': {error}", path.display()))?;
    let directory = path
        .parent()
        .ok_or_else(|| format!("executor state path '{}' has no parent", path.display()))?;
    tokio::fs::create_dir_all(directory)
        .await
        .map_err(|error| {
            format!(
                "create executor state directory '{}': {error}",
                directory.display()
            )
        })?;
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("executor-state.json");
    let temporary = directory.join(format!(".{file_name}.tmp.{}", uuid::Uuid::new_v4()));

    let write_result = async {
        let mut file = tokio::fs::File::create(&temporary).await.map_err(|error| {
            format!(
                "create executor state temp file '{}': {error}",
                temporary.display()
            )
        })?;
        file.write_all(&bytes).await.map_err(|error| {
            format!(
                "write executor state temp file '{}': {error}",
                temporary.display()
            )
        })?;
        file.sync_all().await.map_err(|error| {
            format!(
                "sync executor state temp file '{}': {error}",
                temporary.display()
            )
        })?;
        drop(file);
        replace_path(&temporary, path).map_err(|error| {
            format!(
                "replace executor state '{}' from '{}': {error}",
                path.display(),
                temporary.display()
            )
        })
    }
    .await;

    if write_result.is_err() {
        let _ = tokio::fs::remove_file(&temporary).await;
    }
    write_result
}

fn replace_path(from: &Path, to: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        std::fs::rename(from, to)
    }
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;
        use windows_sys::Win32::Storage::FileSystem::{
            MoveFileExW, MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH,
        };

        let from = from
            .as_os_str()
            .encode_wide()
            .chain(std::iter::once(0))
            .collect::<Vec<_>>();
        let to = to
            .as_os_str()
            .encode_wide()
            .chain(std::iter::once(0))
            .collect::<Vec<_>>();
        // SAFETY: both vectors are stable NUL-terminated UTF-16 strings for
        // the call duration, and MoveFileExW does not retain their pointers.
        let result = unsafe {
            MoveFileExW(
                from.as_ptr(),
                to.as_ptr(),
                MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
            )
        };
        if result == 0 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
    #[cfg(not(any(unix, windows)))]
    {
        std::fs::rename(from, to)
    }
}

fn truncate_chars(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return text.to_string();
    }
    let head: String = text.chars().take(max_chars).collect();
    let dropped = text.chars().count() - max_chars;
    format!("{head}\n… [truncated, {dropped} more chars]")
}

#[cfg(test)]
mod tests {
    use std::future;
    use std::io;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use serde_json::{json, Value};

    use super::{
        build_rehydrated_turn, render_history_preamble, retry_on_etxtbsy, write_json_atomic,
    };
    #[cfg(unix)]
    use super::{ETXTBSY_RAW_OS_ERROR, ETXTBSY_RETRY_ATTEMPTS};

    struct DropMarker(Arc<AtomicBool>);

    impl Drop for DropMarker {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn etxtbsy_retry_recovers_after_transient_errors() {
        let attempts = AtomicUsize::new(0);
        let value = retry_on_etxtbsy(|| {
            let attempt = attempts.fetch_add(1, Ordering::SeqCst);
            async move {
                if attempt < 2 {
                    Err(io::Error::from_raw_os_error(ETXTBSY_RAW_OS_ERROR))
                } else {
                    Ok(42)
                }
            }
        })
        .await
        .unwrap();

        assert_eq!(value, 42);
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn etxtbsy_retry_returns_the_final_error_after_the_bound() {
        let attempts = AtomicUsize::new(0);
        let error = retry_on_etxtbsy(|| {
            attempts.fetch_add(1, Ordering::SeqCst);
            future::ready(Err::<(), _>(io::Error::from_raw_os_error(
                ETXTBSY_RAW_OS_ERROR,
            )))
        })
        .await
        .unwrap_err();

        assert_eq!(error.raw_os_error(), Some(ETXTBSY_RAW_OS_ERROR));
        assert_eq!(attempts.load(Ordering::SeqCst), ETXTBSY_RETRY_ATTEMPTS);
    }

    #[tokio::test]
    async fn etxtbsy_retry_fails_fast_for_other_errors() {
        let attempts = AtomicUsize::new(0);
        let error = retry_on_etxtbsy(|| {
            attempts.fetch_add(1, Ordering::SeqCst);
            future::ready(Err::<(), _>(io::Error::from(
                io::ErrorKind::PermissionDenied,
            )))
        })
        .await
        .unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn caller_timeout_cancels_the_in_flight_retry_operation() {
        let dropped = Arc::new(AtomicBool::new(false));
        let dropped_by_operation = dropped.clone();
        let result = tokio::time::timeout(
            Duration::from_millis(10),
            retry_on_etxtbsy(|| {
                let marker = DropMarker(dropped_by_operation.clone());
                async move {
                    let _marker = marker;
                    future::pending::<io::Result<()>>().await
                }
            }),
        )
        .await;

        assert!(
            result.is_err(),
            "the caller timeout should remain authoritative"
        );
        assert!(
            dropped.load(Ordering::SeqCst),
            "cancelling the retry future must drop its in-flight operation"
        );
    }

    #[test]
    fn rehydration_is_bounded_oldest_first_and_excludes_current_assignment() {
        let mut messages: Vec<Value> = (0..45)
            .map(|index| {
                json!({
                    "role": if index % 2 == 0 { "user" } else { "assistant" },
                    "content": format!("message {index}")
                })
            })
            .collect();
        messages.push(json!({"role": "user", "content": "continue"}));

        let body = build_rehydrated_turn(&messages, "continue");
        assert!(body.contains("## Prior conversation (rehydrated)"));
        assert!(body.contains("## Current task"));
        assert!(body.contains("truncated"));
        assert!(!body.contains("message 0"));
        assert!(body.contains("message 44"));
        assert_eq!(body.matches("continue").count(), 1);
    }

    #[test]
    fn malformed_or_current_only_history_has_no_preamble() {
        let messages = vec![
            json!({"role": "user", "content": 42}),
            json!({"role": "user", "content": "current"}),
        ];
        assert_eq!(render_history_preamble(&messages, "current"), None);
        assert_eq!(build_rehydrated_turn(&messages, "current"), "current");
    }

    #[tokio::test]
    async fn json_state_write_is_atomic_and_leaves_no_temp_file() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("session.json");
        write_json_atomic(&path, &json!({"thread_id": "first"}))
            .await
            .unwrap();
        write_json_atomic(&path, &json!({"thread_id": "second"}))
            .await
            .unwrap();

        let parsed: Value = serde_json::from_slice(&tokio::fs::read(&path).await.unwrap()).unwrap();
        assert_eq!(parsed["thread_id"], "second");
        assert!(!std::fs::read_dir(directory.path())
            .unwrap()
            .filter_map(Result::ok)
            .any(|entry| entry.file_name().to_string_lossy().contains(".tmp.")));
    }
}
