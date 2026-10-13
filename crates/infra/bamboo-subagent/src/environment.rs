//! A portable, admission-time workspace contract for fixed remote actors.
//! The Host and Worker independently inspect clean Git checkouts. Neither a
//! Host-absolute path nor a process cwd is authority on the Worker machine.

use std::path::{Component, Path, PathBuf};
use std::process::Stdio;
use std::time::Duration as StdDuration;

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::AsyncReadExt;
use tokio::process::Command;

use crate::proto::RunSpec;

const MAX_TRACKED_FILES: usize = 20_000;
const MAX_TRACKED_BYTES: u64 = 512 * 1024 * 1024;
const MAX_GIT_OUTPUT: usize = 8 * 1024 * 1024;

/// Authenticated Run input, valid only for its actor and execution attempt.
/// The SHA identifies the committed Git tree; `content_sha256` additionally
/// compares actual checked-out regular file bytes across the two machines.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnvironmentLease {
    pub version: u32,
    pub actor_id: String,
    pub activation_run_id: String,
    pub execution_epoch: u64,
    pub admit_before: DateTime<Utc>,
    pub git_commit: String,
    pub content_sha256: String,
    /// `.` means repository root; all other values are slash-separated safe
    /// relative components. No Host filesystem prefix crosses the wire.
    pub workspace_relpath: String,
}

struct Snapshot {
    relpath: String,
    git_commit: String,
    content_sha256: String,
    local_workspace: PathBuf,
}

impl EnvironmentLease {
    pub async fn capture(
        workspace: &str,
        actor_id: &str,
        activation_run_id: &str,
        execution_epoch: u64,
    ) -> Result<Self, &'static str> {
        if actor_id.is_empty() || activation_run_id.is_empty() || execution_epoch == 0 {
            return Err("remote_environment_identity_missing");
        }
        let snapshot = inspect(workspace).await?;
        Ok(Self {
            version: 1,
            actor_id: actor_id.to_owned(),
            activation_run_id: activation_run_id.to_owned(),
            execution_epoch,
            admit_before: Utc::now() + Duration::minutes(2),
            git_commit: snapshot.git_commit,
            content_sha256: snapshot.content_sha256,
            workspace_relpath: snapshot.relpath,
        })
    }

    /// Validate before any provider/tool boundary and return a Worker-local
    /// canonical workspace path. A lease is an admission check, not a lock on
    /// future filesystem changes during the run.
    pub async fn validate(
        &self,
        provisioned_workspace: &str,
        run: &RunSpec,
    ) -> Result<PathBuf, &'static str> {
        if self.version != 1
            || self.actor_id
                != run
                    .logical_session
                    .as_ref()
                    .map(|id| id.session_id.as_str())
                    .unwrap_or("")
            || self.activation_run_id != run.activation_run_id.as_deref().unwrap_or("")
            || self.execution_epoch == 0
            || self.execution_epoch != run.execution_epoch
            || self.admit_before <= Utc::now()
            || self.admit_before > Utc::now() + Duration::minutes(2)
            || !valid_hash(&self.git_commit, &[40, 64])
            || !valid_hash(&self.content_sha256, &[64])
            || !valid_relpath(&self.workspace_relpath)
        {
            return Err("remote_environment_lease_invalid");
        }
        let snapshot = inspect(provisioned_workspace).await?;
        if snapshot.relpath != self.workspace_relpath
            || snapshot.git_commit != self.git_commit
            || snapshot.content_sha256 != self.content_sha256
        {
            return Err("remote_environment_snapshot_mismatch");
        }
        Ok(snapshot.local_workspace)
    }
}

fn valid_hash(value: &str, lengths: &[usize]) -> bool {
    lengths.contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn valid_relpath(value: &str) -> bool {
    value == "."
        || (!value.is_empty()
            && value.split('/').all(|part| {
                !part.is_empty() && part != "." && part != ".." && !part.contains(['\\', ':', '\0'])
            }))
}

async fn git(repo: &Path, args: &[&str]) -> Result<Vec<u8>, &'static str> {
    let mut command = Command::new("git");
    command
        .arg("-C")
        .arg(repo)
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    sanitize_git_environment(&mut command);
    let mut child = command
        .spawn()
        .map_err(|_| "remote_environment_git_unavailable")?;
    tokio::time::timeout(StdDuration::from_secs(30), async move {
        let stdout = child
            .stdout
            .take()
            .ok_or("remote_environment_git_inspection_failed")?;
        let mut limited = stdout.take((MAX_GIT_OUTPUT + 1) as u64);
        let mut bytes = Vec::new();
        limited
            .read_to_end(&mut bytes)
            .await
            .map_err(|_| "remote_environment_git_inspection_failed")?;
        if bytes.len() > MAX_GIT_OUTPUT {
            return Err("remote_environment_git_output_too_large");
        }
        if !child
            .wait()
            .await
            .map_err(|_| "remote_environment_git_inspection_failed")?
            .success()
        {
            return Err("remote_environment_git_inspection_failed");
        }
        Ok(bytes)
    })
    .await
    .map_err(|_| "remote_environment_git_inspection_timed_out")?
}

fn sanitize_git_environment(command: &mut Command) {
    // `git -C` does not override GIT_DIR, GIT_WORK_TREE, GIT_INDEX_FILE or
    // injected GIT_CONFIG_COUNT/KEY_n/VALUE_n. Inspect exactly the configured
    // checkout, independent of the Bamboo process's own Git environment.
    for (name, _) in std::env::vars_os() {
        if name.to_string_lossy().starts_with("GIT_") {
            command.env_remove(name);
        }
    }
    // Also remove explicitly injected overrides when this helper is used on
    // a Command that already had them; the dynamic pass covers all inherited
    // GIT_* names, including numbered config entries.
    for name in [
        "GIT_DIR",
        "GIT_WORK_TREE",
        "GIT_INDEX_FILE",
        "GIT_COMMON_DIR",
        "GIT_OBJECT_DIRECTORY",
        "GIT_ALTERNATE_OBJECT_DIRECTORIES",
        "GIT_CONFIG_COUNT",
        "GIT_CONFIG_PARAMETERS",
        "GIT_CONFIG_SYSTEM",
        "GIT_CONFIG_GLOBAL",
        "GIT_NAMESPACE",
        "GIT_CEILING_DIRECTORIES",
    ] {
        command.env_remove(name);
    }
    command.env("GIT_OPTIONAL_LOCKS", "0");
}

fn git_text(bytes: &[u8]) -> Result<&str, &'static str> {
    std::str::from_utf8(bytes)
        .map(|text| text.trim_end_matches(['\n', '\r']))
        .map_err(|_| "remote_environment_git_inspection_failed")
}

async fn inspect(workspace: &str) -> Result<Snapshot, &'static str> {
    if !Path::new(workspace).is_absolute() {
        return Err("remote_environment_workspace_unavailable");
    }
    let local_workspace = tokio::fs::canonicalize(workspace)
        .await
        .map_err(|_| "remote_environment_workspace_unavailable")?;
    if !tokio::fs::metadata(&local_workspace)
        .await
        .map_err(|_| "remote_environment_workspace_unavailable")?
        .is_dir()
    {
        return Err("remote_environment_workspace_unavailable");
    }
    let root_text =
        git_text(&git(&local_workspace, &["rev-parse", "--show-toplevel"]).await?)?.to_owned();
    let root = tokio::fs::canonicalize(root_text)
        .await
        .map_err(|_| "remote_environment_git_inspection_failed")?;
    let relative = local_workspace
        .strip_prefix(&root)
        .map_err(|_| "remote_environment_workspace_outside_git_root")?;
    let relpath = if relative.as_os_str().is_empty() {
        ".".to_owned()
    } else {
        let parts = relative
            .components()
            .map(|component| match component {
                Component::Normal(part) => {
                    part.to_str().ok_or("remote_environment_path_unsupported")
                }
                _ => Err("remote_environment_path_unsupported"),
            })
            .collect::<Result<Vec<_>, _>>()?;
        parts.join("/")
    };
    if !valid_relpath(&relpath) {
        return Err("remote_environment_path_unsupported");
    }
    if git_text(&git(&root, &["rev-parse", "--is-bare-repository"]).await?)? != "false"
        || git_text(&git(&root, &["rev-parse", "--is-shallow-repository"]).await?)? != "false"
        || git_text(
            &git(
                &root,
                &["config", "--default=false", "--bool", "core.sparseCheckout"],
            )
            .await?,
        )? == "true"
    {
        return Err("remote_environment_checkout_unsupported");
    }
    let commit =
        git_text(&git(&root, &["rev-parse", "--verify", "HEAD^{commit}"]).await?)?.to_owned();
    if !valid_hash(&commit, &[40, 64]) {
        return Err("remote_environment_git_inspection_failed");
    }
    let status_args = [
        "status",
        "--porcelain=v1",
        "--untracked-files=all",
        "--ignored=matching",
        "--ignore-submodules=none",
    ];
    if !git(&root, &status_args).await?.is_empty() {
        return Err("remote_environment_checkout_not_clean");
    }

    let index = git(&root, &["ls-files", "--stage", "-z"]).await?;
    let mut digest = Sha256::new();
    let mut count = 0usize;
    let mut total = 0u64;
    for record in index
        .split(|byte| *byte == 0)
        .filter(|record| !record.is_empty())
    {
        count += 1;
        if count > MAX_TRACKED_FILES {
            return Err("remote_environment_snapshot_too_large");
        }
        let separator = record
            .iter()
            .position(|byte| *byte == b'\t')
            .ok_or("remote_environment_index_invalid")?;
        let (meta, name_with_tab) = record.split_at(separator);
        let name = &name_with_tab[1..];
        let meta = std::str::from_utf8(meta).map_err(|_| "remote_environment_index_invalid")?;
        let mut fields = meta.split(' ');
        let mode = fields.next().ok_or("remote_environment_index_invalid")?;
        let _oid = fields.next().ok_or("remote_environment_index_invalid")?;
        let stage = fields.next().ok_or("remote_environment_index_invalid")?;
        if !matches!(mode, "100644" | "100755") || stage != "0" || fields.next().is_some() {
            return Err("remote_environment_index_unsupported");
        }
        let name = std::str::from_utf8(name).map_err(|_| "remote_environment_path_unsupported")?;
        if !valid_relpath(name) || name == "." {
            return Err("remote_environment_path_unsupported");
        }
        let path = root.join(name);
        if tokio::fs::canonicalize(&path)
            .await
            .map_err(|_| "remote_environment_snapshot_changed")?
            != path
        {
            return Err("remote_environment_path_unsupported");
        }
        let metadata = tokio::fs::symlink_metadata(&path)
            .await
            .map_err(|_| "remote_environment_snapshot_changed")?;
        if !metadata.file_type().is_file() {
            return Err("remote_environment_index_unsupported");
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if (metadata.permissions().mode() & 0o111 != 0) != (mode == "100755") {
                return Err("remote_environment_checkout_not_clean");
            }
        }
        total = total
            .checked_add(metadata.len())
            .ok_or("remote_environment_snapshot_too_large")?;
        if total > MAX_TRACKED_BYTES {
            return Err("remote_environment_snapshot_too_large");
        }
        digest.update((name.len() as u64).to_be_bytes());
        digest.update(name.as_bytes());
        digest.update(mode.as_bytes());
        digest.update(metadata.len().to_be_bytes());
        let mut file = tokio::fs::File::open(&path)
            .await
            .map_err(|_| "remote_environment_snapshot_changed")?;
        let mut buffer = [0u8; 64 * 1024];
        let mut read_total = 0u64;
        loop {
            let read = file
                .read(&mut buffer)
                .await
                .map_err(|_| "remote_environment_snapshot_changed")?;
            if read == 0 {
                break;
            }
            read_total = read_total
                .checked_add(read as u64)
                .filter(|count| *count <= metadata.len())
                .ok_or("remote_environment_snapshot_changed")?;
            digest.update(&buffer[..read]);
        }
        if read_total != metadata.len() {
            return Err("remote_environment_snapshot_changed");
        }
    }
    if !git(&root, &status_args).await?.is_empty() {
        return Err("remote_environment_snapshot_changed");
    }
    Ok(Snapshot {
        relpath,
        git_commit: commit,
        content_sha256: format!("{:x}", digest.finalize()),
        local_workspace,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::LogicalSessionIdentity;
    use std::process::Command as SyncCommand;

    fn git(cwd: &Path, args: &[&str]) {
        let output = SyncCommand::new("git")
            .arg("-C")
            .arg(cwd)
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn run() -> RunSpec {
        RunSpec {
            assignment: "bounded task".into(),
            logical_session: Some(LogicalSessionIdentity {
                session_id: "child".into(),
                parent_session_id: Some("root".into()),
                root_session_id: "root".into(),
                creation: None,
            }),
            project_id: None,
            reasoning_effort: None,
            permission_policy: None,
            messages: vec![],
            activation_run_id: Some("activation".into()),
            execution_epoch: 7,
            initial_session_messages: vec![],
            secrets: Default::default(),
        }
    }

    #[tokio::test]
    async fn independent_clean_checkouts_require_same_sha_bytes_and_relative_workspace() {
        let temp = tempfile::tempdir().unwrap();
        let host = temp.path().join("host");
        let worker = temp.path().join("worker");
        git(temp.path(), &["init", "-q", host.to_str().unwrap()]);
        std::fs::create_dir(host.join("src")).unwrap();
        std::fs::write(host.join("src/input.txt"), "portable\n").unwrap();
        git(&host, &["add", "src/input.txt"]);
        git(
            &host,
            &[
                "-c",
                "user.name=Test",
                "-c",
                "user.email=test@invalid",
                "commit",
                "-qm",
                "snapshot",
            ],
        );
        git(
            temp.path(),
            &[
                "clone",
                "-q",
                "--local",
                host.to_str().unwrap(),
                worker.to_str().unwrap(),
            ],
        );
        let mut polluted = Command::new("git");
        polluted
            .arg("-C")
            .arg(&worker)
            .args(["rev-parse", "--show-toplevel"])
            .env("GIT_DIR", host.join(".git"))
            .env("GIT_WORK_TREE", &host)
            .env("GIT_INDEX_FILE", host.join(".git/index"))
            .env("GIT_CONFIG_COUNT", "1")
            .env("GIT_CONFIG_KEY_0", "core.bare")
            .env("GIT_CONFIG_VALUE_0", "true");
        sanitize_git_environment(&mut polluted);
        let observed = polluted.output().await.unwrap();
        assert!(observed.status.success());
        assert_eq!(
            Path::new(git_text(&observed.stdout).unwrap())
                .canonicalize()
                .unwrap(),
            worker.canonicalize().unwrap()
        );
        let host_workspace = host.join("src").canonicalize().unwrap();
        let worker_workspace = worker.join("src").canonicalize().unwrap();
        assert_ne!(host_workspace, worker_workspace);
        let run = run();
        assert_eq!(
            EnvironmentLease::capture("relative-workspace", "child", "activation", 7)
                .await
                .unwrap_err(),
            "remote_environment_workspace_unavailable"
        );
        let lease =
            EnvironmentLease::capture(host_workspace.to_str().unwrap(), "child", "activation", 7)
                .await
                .unwrap();
        assert_eq!(lease.workspace_relpath, "src");
        assert_eq!(
            lease
                .validate(worker_workspace.to_str().unwrap(), &run)
                .await
                .unwrap(),
            worker_workspace
        );
        assert_eq!(
            lease
                .validate(worker.to_str().unwrap(), &run)
                .await
                .unwrap_err(),
            "remote_environment_snapshot_mismatch"
        );

        std::fs::write(worker.join("src/input.txt"), "different bytes\n").unwrap();
        assert_eq!(
            lease
                .validate(worker_workspace.to_str().unwrap(), &run)
                .await
                .unwrap_err(),
            "remote_environment_checkout_not_clean"
        );
        git(&worker, &["add", "src/input.txt"]);
        git(
            &worker,
            &[
                "-c",
                "user.name=Test",
                "-c",
                "user.email=test@invalid",
                "commit",
                "-qm",
                "diverge",
            ],
        );
        assert_eq!(
            lease
                .validate(worker_workspace.to_str().unwrap(), &run)
                .await
                .unwrap_err(),
            "remote_environment_snapshot_mismatch"
        );

        let mut forged = lease.clone();
        forged.workspace_relpath = "../host/src".into();
        assert_eq!(
            forged
                .validate(worker_workspace.to_str().unwrap(), &run)
                .await
                .unwrap_err(),
            "remote_environment_lease_invalid"
        );
        forged = lease.clone();
        forged.execution_epoch += 1;
        assert_eq!(
            forged
                .validate(worker_workspace.to_str().unwrap(), &run)
                .await
                .unwrap_err(),
            "remote_environment_lease_invalid"
        );
    }
}
