//! Project-scoped Git worktree lifecycle.

use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use tokio::process::Command;

pub const WORKTREE_BRANCH_PREFIX: &str = "bamboo/";
pub const WORKTREE_RETENTION: Duration = Duration::from_secs(7 * 24 * 60 * 60);

fn validate_name(name: &str) -> Result<&str, String> {
    let name = name.trim();
    if name.is_empty()
        || name.len() > 80
        || !name
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_')
    {
        return Err("worktree name must be 1-80 ASCII letters, digits, '-' or '_'".to_string());
    }
    Ok(name)
}

async fn git_output(project: &Path, args: &[&str]) -> Result<std::process::Output, String> {
    Command::new("git")
        .arg("-C")
        .arg(project)
        .args(args)
        .output()
        .await
        .map_err(|error| format!("run git: {error}"))
}

pub async fn git_project_root(workspace: &Path) -> Result<PathBuf, String> {
    // `rev-parse --show-toplevel` returns the *linked worktree* root. Runtime
    // directories must instead stay anchored at the repository's main
    // worktree, otherwise spawning from a managed checkout recursively creates
    // `<checkout>/.bamboo/worktree/...`.
    let output = git_output(workspace, &["worktree", "list", "--porcelain", "-z"]).await?;
    if !output.status.success() {
        return Err(format!(
            "workspace is not inside a Git project: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    let listing = String::from_utf8(output.stdout)
        .map_err(|_| "git project root is not valid UTF-8".to_string())?;
    let root = listing
        .split('\0')
        .find_map(|line| line.strip_prefix("worktree "))
        .filter(|root| !root.is_empty())
        .ok_or_else(|| "Git returned no main worktree".to_string())?;
    Ok(PathBuf::from(root))
}

fn ownership_marker(project_root: &Path, name: &str) -> PathBuf {
    bamboo_config::paths::project_worktree_dir(project_root)
        .join(".bamboo-owned")
        .join(name)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectWorktree {
    pub project_root: PathBuf,
    pub path: PathBuf,
    pub branch: String,
}

pub async fn create(workspace: &Path, name: &str) -> Result<ProjectWorktree, String> {
    let name = validate_name(name)?;
    let project_root = git_project_root(workspace).await?;
    bamboo_config::paths::ensure_project_runtime_dirs(&project_root)
        .map_err(|error| format!("prepare project .bamboo directory: {error}"))?;
    // Best-effort startup housekeeping. A failure to inspect stale siblings
    // must not prevent creating an unrelated fresh worktree.
    if let Err(error) = gc_orphans(&project_root, WORKTREE_RETENTION).await {
        tracing::warn!("failed to garbage-collect stale project worktrees: {error}");
    }
    let path = bamboo_config::paths::project_worktree_dir(&project_root).join(name);
    let branch = format!("{WORKTREE_BRANCH_PREFIX}{name}");
    if path.exists() {
        return Err(format!("worktree path already exists: {}", path.display()));
    }

    let branch_ref = format!("refs/heads/{branch}");
    let branch_check = git_output(
        &project_root,
        &["show-ref", "--verify", "--quiet", &branch_ref],
    )
    .await?;
    if branch_check.status.success() {
        return Err(format!("worktree branch already exists: {branch}"));
    }
    if branch_check.status.code() != Some(1) {
        return Err(format!(
            "failed to check worktree branch: {}",
            String::from_utf8_lossy(&branch_check.stderr).trim()
        ));
    }

    let path_arg = path.to_string_lossy().to_string();
    let output = git_output(
        &project_root,
        &["worktree", "add", "-b", &branch, &path_arg, "HEAD"],
    )
    .await?;
    if !output.status.success() {
        return Err(format!(
            "create Git worktree: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    let marker = ownership_marker(&project_root, name);
    if let Some(parent) = marker.parent() {
        if let Err(error) = tokio::fs::create_dir_all(parent).await {
            let _ = git_output(&project_root, &["worktree", "remove", "--force", &path_arg]).await;
            return Err(format!("create worktree ownership directory: {error}"));
        }
    }
    if let Err(error) = tokio::fs::write(&marker, branch.as_bytes()).await {
        let _ = git_output(&project_root, &["worktree", "remove", "--force", &path_arg]).await;
        return Err(format!("record worktree ownership: {error}"));
    }
    Ok(ProjectWorktree {
        project_root,
        path,
        branch,
    })
}

pub async fn remove(workspace: &Path, name: &str) -> Result<(), String> {
    let name = validate_name(name)?;
    let project_root = git_project_root(workspace).await?;
    let path = bamboo_config::paths::project_worktree_dir(&project_root).join(name);
    let marker = ownership_marker(&project_root, name);
    let expected_branch = format!("{WORKTREE_BRANCH_PREFIX}{name}");
    let marker_branch = tokio::fs::read_to_string(&marker).await.ok();
    if marker_branch.as_deref() != Some(expected_branch.as_str()) {
        return Err(format!(
            "refusing to remove an unmanaged worktree: {}",
            path.display()
        ));
    }
    if path.exists() {
        let branch = git_output(&path, &["symbolic-ref", "--quiet", "--short", "HEAD"]).await?;
        if !branch.status.success()
            || String::from_utf8_lossy(&branch.stdout).trim() != expected_branch
        {
            return Err(format!(
                "refusing to remove worktree with unexpected branch: {}",
                path.display()
            ));
        }
    }
    let path_arg = path.to_string_lossy().to_string();
    let output = git_output(&project_root, &["worktree", "remove", "--force", &path_arg]).await?;
    if !output.status.success() && path.exists() {
        return Err(format!(
            "remove Git worktree: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    let _ = tokio::fs::remove_file(marker).await;
    prune(&project_root).await
}

pub async fn prune(project_root: &Path) -> Result<(), String> {
    let output = git_output(project_root, &["worktree", "prune"]).await?;
    if output.status.success() {
        Ok(())
    } else {
        Err(format!(
            "prune Git worktrees: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ))
    }
}

/// Remove expired Bamboo-owned worktrees, then prune Git's administrative
/// records. Ownership, lease age, and the checked-out branch must all match;
/// ambiguous directories are deliberately preserved for manual recovery.
pub async fn gc_orphans(project_root: &Path, retention: Duration) -> Result<usize, String> {
    let root = bamboo_config::paths::project_worktree_dir(project_root);
    let mut entries = match tokio::fs::read_dir(&root).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(format!("read worktree directory: {error}")),
    };
    let now = SystemTime::now();
    let mut removed = 0;
    while let Ok(Some(entry)) = entries.next_entry().await {
        let name = entry.file_name().to_string_lossy().into_owned();
        if validate_name(&name).is_err() {
            continue;
        }
        let marker = ownership_marker(project_root, &name);
        let expected_branch = format!("{WORKTREE_BRANCH_PREFIX}{name}");
        if !matches!(
            tokio::fs::read_to_string(&marker).await.as_deref(),
            Ok(branch) if branch == expected_branch
        ) {
            continue;
        }
        let (Ok(metadata), Ok(marker_metadata)) =
            (entry.metadata().await, tokio::fs::metadata(&marker).await)
        else {
            continue;
        };
        let older_than_retention = |metadata: &std::fs::Metadata| {
            metadata
                .modified()
                .ok()
                .and_then(|modified| now.duration_since(modified).ok())
                .is_some_and(|age| age > retention)
        };
        let stale = metadata.is_dir()
            && older_than_retention(&metadata)
            && older_than_retention(&marker_metadata);
        if !stale {
            continue;
        }
        let path = entry.path();
        let branch = git_output(&path, &["symbolic-ref", "--quiet", "--short", "HEAD"]).await;
        if !branch.is_ok_and(|output| {
            output.status.success()
                && String::from_utf8_lossy(&output.stdout).trim() == expected_branch
        }) {
            continue;
        }
        if remove(project_root, &name).await.is_ok() {
            removed += 1;
        }
    }
    prune(project_root).await?;
    Ok(removed)
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn git(project: &Path, args: &[&str]) {
        let output = git_output(project, args).await.expect("git command");
        assert!(
            output.status.success(),
            "git failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[tokio::test]
    async fn create_collision_remove_and_prune_use_project_convention() {
        let temp = tempfile::tempdir().expect("tempdir");
        let project = temp.path().join("repo");
        tokio::fs::create_dir_all(&project).await.expect("repo");
        git(&project, &["init"]).await;
        git(&project, &["config", "user.email", "test@example.com"]).await;
        git(&project, &["config", "user.name", "Test"]).await;
        tokio::fs::write(project.join("README.md"), "test")
            .await
            .expect("readme");
        git(&project, &["add", "README.md"]).await;
        git(&project, &["commit", "-m", "initial"]).await;

        let created = create(&project, "child_1").await.expect("create");
        assert_eq!(created.branch, "bamboo/child_1");
        assert_eq!(
            created.path,
            std::fs::canonicalize(&project)
                .expect("canonical project")
                .join(".bamboo/worktree/child_1")
        );
        assert!(created.path.join(".git").is_file());
        assert!(create(&project, "child_1").await.is_err());

        remove(&project, "child_1").await.expect("remove");
        assert!(!created.path.exists());

        let stale = create(&project, "child_2").await.expect("stale create");
        assert_eq!(
            gc_orphans(&project, WORKTREE_RETENTION)
                .await
                .expect("fresh gc"),
            0
        );
        assert!(stale.path.exists(), "fresh worktree must not be reaped");
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert_eq!(gc_orphans(&project, Duration::ZERO).await.expect("gc"), 1);
        assert!(!stale.path.exists(), "expired owned worktree is reaped");

        let unowned = project.join(".bamboo/worktree/unowned");
        tokio::fs::create_dir_all(&unowned)
            .await
            .expect("unowned directory");
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert_eq!(gc_orphans(&project, Duration::ZERO).await.expect("gc"), 0);
        assert!(unowned.exists(), "unowned directory must be preserved");

        let mismatch = create(&project, "mismatch").await.expect("mismatch create");
        git(&mismatch.path, &["checkout", "--detach"]).await;
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert_eq!(gc_orphans(&project, Duration::ZERO).await.expect("gc"), 0);
        assert!(
            mismatch.path.exists(),
            "worktree on an unexpected branch must be preserved"
        );
        let mismatch_arg = mismatch.path.to_string_lossy().to_string();
        git(&project, &["worktree", "remove", "--force", &mismatch_arg]).await;
    }

    #[tokio::test]
    async fn linked_worktree_resolves_to_main_project_root() {
        let temp = tempfile::tempdir().expect("tempdir");
        let project = temp.path().join("repo");
        let linked = temp.path().join("linked");
        tokio::fs::create_dir_all(&project).await.expect("repo");
        git(&project, &["init"]).await;
        git(&project, &["config", "user.email", "test@example.com"]).await;
        git(&project, &["config", "user.name", "Test"]).await;
        tokio::fs::write(project.join("README.md"), "test")
            .await
            .expect("readme");
        git(&project, &["add", "README.md"]).await;
        git(&project, &["commit", "-m", "initial"]).await;
        let linked_arg = linked.to_string_lossy().to_string();
        git(&project, &["worktree", "add", "-b", "linked", &linked_arg]).await;

        assert_eq!(
            git_project_root(&linked).await.expect("project root"),
            std::fs::canonicalize(&project).expect("canonical project")
        );
        let child = create(&linked, "nested_child").await.expect("child");
        assert!(child.path.starts_with(
            std::fs::canonicalize(&project)
                .expect("canonical project")
                .join(".bamboo/worktree")
        ));
        assert!(!linked.join(".bamboo").exists());
    }

    #[test]
    fn rejects_names_that_can_escape_the_managed_root() {
        for name in ["", "../escape", "a/b", "space name", "."] {
            assert!(validate_name(name).is_err(), "accepted {name:?}");
        }
    }
}
