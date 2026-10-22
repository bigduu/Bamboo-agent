//! Blocking Workspace probes run outside the executor's async runtime thread.
//!
//! This worker performs observations only. The caller still reloads and commits
//! Session authority under its existing persistence owner after these probes.

use std::path::PathBuf;

use bamboo_agent_core::ToolError;

pub(super) async fn run<T, F>(work: F) -> Result<T, ToolError>
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    let span = tracing::Span::current();
    tokio::task::spawn_blocking(move || {
        let _entered = span.enter();
        work()
    })
        .await
        .map_err(|error| {
            super::workspace_diagnostics::record_current_error("filesystem_worker", &error);
            ToolError::Execution(format!("Workspace filesystem worker failed: {error}"))
        })
}

pub(super) enum DirectoryCheck {
    Missing(PathBuf),
    NotDirectory(PathBuf),
    Resolved(PathBuf),
}

pub(super) fn check_directory(path: PathBuf) -> std::io::Result<DirectoryCheck> {
    if !path.exists() {
        return Ok(DirectoryCheck::Missing(path));
    }
    if !path.is_dir() {
        return Ok(DirectoryCheck::NotDirectory(path));
    }
    path.canonicalize().map(DirectoryCheck::Resolved)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test(flavor = "current_thread")]
    async fn blocked_workspace_probe_leaves_runtime_available() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().to_path_buf();
        let expected = path.canonicalize().unwrap();
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let worker = tokio::spawn(run(move || {
            let _ = entered_tx.send(());
            // A bounded synthetic filesystem stall. An inline implementation
            // blocks this current-thread executor and cannot receive release.
            let released = release_rx.recv_timeout(Duration::from_secs(2)).is_ok();
            (released, check_directory(path))
        }));
        entered_rx.await.unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;
        release_tx.send(()).unwrap();
        let (released, checked) = worker.await.unwrap().unwrap();
        assert!(released, "the async owner must progress while a probe waits");
        match checked.unwrap() {
            DirectoryCheck::Resolved(path) => assert_eq!(path, expected),
            _ => panic!("expected the actual temporary directory"),
        }
    }

    #[tokio::test]
    async fn directory_probe_preserves_missing_and_file_diagnostics() {
        let temp = tempfile::tempdir().unwrap();
        let missing = temp.path().join("missing");
        let file = temp.path().join("file");
        std::fs::write(&file, "fixture").unwrap();
        let expected_missing = missing.clone();
        assert!(matches!(
            run(move || check_directory(missing)).await.unwrap().unwrap(),
            DirectoryCheck::Missing(path) if path == expected_missing
        ));
        let expected_file = file.clone();
        assert!(matches!(
            run(move || check_directory(file)).await.unwrap().unwrap(),
            DirectoryCheck::NotDirectory(path) if path == expected_file
        ));
    }
}
