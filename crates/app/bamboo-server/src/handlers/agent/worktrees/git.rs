//! Git is the only worktree registry. This module deliberately has no runtime
//! dependencies, so its real filesystem/Git regressions can run without Cargo.
use std::fs::{self, OpenOptions};
use std::path::{Component, Path, PathBuf};
use std::process::{Command, Output};

#[derive(Debug, Default)]
pub struct Worktree {
    pub path: PathBuf,
    pub branch: Option<String>,
    pub head: String,
    pub locked: bool,
    pub prunable: bool,
    pub dirty: Option<bool>,
    pub error: Option<String>,
}

fn command(repo: &Path, args: &[&str]) -> Result<Output, String> {
    Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(args)
        .env_remove("GIT_DIR")
        .env_remove("GIT_COMMON_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .env_remove("GIT_CEILING_DIRECTORIES")
        .env("LC_ALL", "C")
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_OPTIONAL_LOCKS", "0")
        .output()
        .map_err(|e| format!("Git at {}: {e}", repo.display()))
}

fn git(repo: &Path, args: &[&str]) -> Result<String, String> {
    let output = command(repo, args)?;
    if !output.status.success() {
        return Err(format!(
            "Git at {}: {}",
            repo.display(),
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    String::from_utf8(output.stdout).map_err(|_| "Git returned a non-UTF-8 path".into())
}

pub fn repository(repo: &Path) -> Result<PathBuf, String> {
    let value = git(
        repo,
        &["rev-parse", "--path-format=absolute", "--git-common-dir"],
    )?;
    fs::canonicalize(value.strip_suffix('\n').unwrap_or(&value))
        .map_err(|e| format!("Repository metadata is unavailable: {e}"))
}

pub fn workspace_root(path: &Path) -> Result<PathBuf, String> {
    let value = git(path, &["rev-parse", "--show-toplevel"])?;
    fs::canonicalize(value.strip_suffix('\n').unwrap_or(&value)).map_err(|e| e.to_string())
}

pub fn root(path: &Path) -> Result<PathBuf, String> {
    if !path.is_absolute() || path.components().any(|c| matches!(c, Component::ParentDir)) {
        return Err("Worktree root must be an absolute existing directory without '..'".into());
    }
    let canonical = fs::canonicalize(path).map_err(|e| {
        format!(
            "Worktree root {} is unavailable (check the external volume): {e}",
            path.display()
        )
    })?;
    if !canonical.is_dir() {
        return Err("Worktree root is not a directory".into());
    }
    // Accept canonical OS aliases, but never silently resolve a selected volume
    // onto another disk through a symlink or an unmounted /Volumes directory.
    #[cfg(target_os = "macos")]
    if let Ok(relative) = path.strip_prefix("/Volumes") {
        use std::os::unix::fs::MetadataExt;
        let volume = relative
            .components()
            .next()
            .ok_or("Select a mounted volume directory")?;
        let mount = Path::new("/Volumes").join(volume.as_os_str());
        if !canonical.starts_with(&mount)
            || fs::metadata(&mount).map_err(|e| e.to_string())?.dev()
                == fs::metadata("/Volumes").map_err(|e| e.to_string())?.dev()
        {
            return Err(format!(
                "External volume {} is not mounted at the selected path",
                mount.display()
            ));
        }
    }
    fs::read_dir(&canonical)
        .map_err(|e| format!("Cannot read worktree root {}: {e}", canonical.display()))?;
    // Metadata and bare repositories have no --show-toplevel. Discover the
    // Git directory instead, and retain broken/restricted .git boundaries.
    let discovery = command(&canonical, &["rev-parse", "--absolute-git-dir"])?;
    let marker = canonical
        .ancestors()
        .any(|parent| fs::symlink_metadata(parent.join(".git")).is_ok());
    if discovery.status.success() || marker {
        return Err(
            "Worktree root must be outside an existing Git repository and its metadata".into(),
        );
    }
    let diagnostic = String::from_utf8_lossy(&discovery.stderr);
    // command() fixes the locale. Only an explicit not-a-repository result
    // establishes an outside directory; permission/config/discovery errors fail.
    if discovery.status.code() != Some(128)
        || !diagnostic.starts_with("fatal: not a git repository (or any")
    {
        return Err(format!(
            "Git could not verify worktree root {} ({}): {}",
            canonical.display(),
            discovery.status,
            diagnostic.trim()
        ));
    }
    Ok(canonical)
}

pub fn list(repo: &Path) -> Result<Vec<Worktree>, String> {
    let common = repository(repo)?;
    let listing = git(repo, &["worktree", "list", "--porcelain", "-z"])?;
    let mut result = Vec::new();
    let mut entry: Option<Worktree> = None;
    for field in listing.split('\0') {
        if let Some(path) = field.strip_prefix("worktree ") {
            if let Some(previous) = entry.take() {
                result.push(previous);
            }
            entry = Some(Worktree {
                path: path.into(),
                ..Default::default()
            });
        } else if let Some(current) = entry.as_mut() {
            if let Some(head) = field.strip_prefix("HEAD ") {
                current.head = head.into();
            }
            if let Some(branch) = field.strip_prefix("branch refs/heads/") {
                current.branch = Some(branch.into());
            }
            if field == "locked" || field.starts_with("locked ") {
                current.locked = true;
            }
            if field == "prunable" || field.starts_with("prunable ") {
                current.prunable = true;
            }
        }
    }
    if let Some(previous) = entry {
        result.push(previous);
    }
    for entry in &mut result {
        let inspect = || -> Result<(PathBuf, bool), String> {
            let canonical = fs::canonicalize(&entry.path)
                .map_err(|e| format!("{} is unavailable: {e}", entry.path.display()))?;
            if repository(&canonical)? != common || workspace_root(&canonical)? != canonical {
                return Err("Worktree path now belongs to a different repository".into());
            }
            let status = git(
                &canonical,
                &["status", "--porcelain", "-z", "--untracked-files=normal"],
            )?;
            Ok((canonical, !status.is_empty()))
        };
        match inspect() {
            Ok((path, dirty)) => {
                entry.path = path;
                entry.dirty = Some(dirty);
            }
            Err(error) => entry.error = Some(error),
        }
    }
    Ok(result)
}

fn same_identity(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        left.dev() == right.dev() && left.ino() == right.ino()
    }
    #[cfg(not(unix))]
    {
        left.is_dir() == right.is_dir()
            && left
                .created()
                .ok()
                .zip(right.created().ok())
                .is_some_and(|(left, right)| left == right)
    }
}

fn retains_identity(path: &Path, expected: &fs::Metadata) -> bool {
    fs::symlink_metadata(path)
        .is_ok_and(|actual| !actual.file_type().is_symlink() && same_identity(&actual, expected))
}

// Keep one stable metadata inode. Dropping the handle (including process
// termination) releases the OS advisory lock; unlinking would permit two
// cooperating creators to lock different inodes for the same repository.
struct CreateLock {
    path: PathBuf,
    file: fs::File,
}
impl CreateLock {
    fn acquire(path: PathBuf) -> Result<Self, String> {
        let open_error = |error: std::io::Error| {
            format!(
                "Cannot open repository creation lock ({}): {error}",
                path.display()
            )
        };
        let file = match OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
        {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let existing = fs::symlink_metadata(&path).map_err(open_error)?;
                if existing.file_type().is_symlink() || !existing.is_file() {
                    return Err(format!(
                        "Repository creation lock is not a regular file: {}",
                        path.display()
                    ));
                }
                // Never create through a replaced path or truncate old content.
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&path)
                    .map_err(open_error)?
            }
            Err(error) => return Err(open_error(error)),
        };
        let lock = Self { path, file };
        if !lock.is_current() {
            return Err("Repository creation lock changed while opening".into());
        }
        lock.file.try_lock().map_err(|error| {
            format!(
                "Repository worktree creation is busy or locking is unavailable ({}): {error}",
                lock.path.display()
            )
        })?;
        if !lock.is_current() {
            return Err("Repository creation lock changed while acquiring".into());
        }
        Ok(lock)
    }

    fn is_current(&self) -> bool {
        self.file
            .metadata()
            .is_ok_and(|owned| owned.is_file() && retains_identity(&self.path, &owned))
    }
}

pub fn create(
    repo: &Path,
    destination_root: &Path,
    name: &str,
    branch: &str,
) -> Result<PathBuf, String> {
    if name.is_empty()
        || name.len() > 80
        || !name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        return Err(
            "Worktree folder name must contain 1–80 ASCII letters, digits, '-' or '_'".into(),
        );
    }
    if branch.is_empty() || branch.starts_with('-') || branch == "HEAD" {
        return Err("Choose a valid new local branch name".into());
    }
    git(repo, &["check-ref-format", &format!("refs/heads/{branch}")])?;
    let common = repository(repo)?;
    let root = root(destination_root)?;
    let root_identity = fs::metadata(&root).map_err(|e| e.to_string())?;
    let repo_identity = fs::metadata(&common).map_err(|e| e.to_string())?;
    let lock = CreateLock::acquire(common.join("bodhi-worktree-create.lock"))?;
    let target = root.join(name);
    // create_dir reserves the exact destination across processes/repositories.
    // Existing directories (including empty ones and symlinks) are never reused.
    fs::create_dir(&target)
        .map_err(|e| format!("Cannot reserve worktree path {}: {e}", target.display()))?;
    let target_identity = fs::symlink_metadata(&target).map_err(|e| e.to_string())?;
    let create = || -> Result<(), String> {
        if self::root(destination_root)? != root
            || fs::canonicalize(&target).map_err(|e| e.to_string())? != target
            || repository(repo)? != common
            || !retains_identity(&root, &root_identity)
            || !retains_identity(&target, &target_identity)
            || !retains_identity(&common, &repo_identity)
            || !lock.is_current()
        {
            return Err("Repository or destination changed during worktree creation".into());
        }
        let target_arg = target.to_str().ok_or("Destination must be UTF-8")?;
        git(
            repo,
            &["worktree", "add", "-b", branch, "--", target_arg, "HEAD"],
        )?;
        Ok(())
    };
    if let Err(error) = create() {
        // This removes ONLY our still-empty reservation. Git or another writer's
        // files are retained. Never force-remove a checkout or delete a branch.
        if retains_identity(&target, &target_identity) {
            let _ = fs::remove_dir(&target);
        }
        return Err(format!(
            "{error}. Any files or branch created by Git were preserved."
        ));
    }
    Ok(target)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    struct Fixture(PathBuf);
    impl Fixture {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "bodhi-worktrees-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir(&path).unwrap();
            let path = path.canonicalize().unwrap();
            let repo = path.join("repo");
            fs::create_dir(&repo).unwrap();
            git(&repo, &["init", "-b", "main"]).unwrap();
            git(
                &repo,
                &[
                    "-c",
                    "user.name=Test",
                    "-c",
                    "user.email=test@example.invalid",
                    "commit",
                    "--allow-empty",
                    "-m",
                    "initial",
                ],
            )
            .unwrap();
            fs::create_dir(path.join("roots")).unwrap();
            Self(path)
        }
        fn repo(&self) -> PathBuf {
            self.0.join("repo")
        }
        fn roots(&self) -> PathBuf {
            self.0.join("roots")
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn create_and_discover_real_git_worktrees_with_dirty_and_locked_state() {
        let f = Fixture::new();
        let path = create(&f.repo(), &f.roots(), "feature", "feature/test").unwrap();
        fs::write(path.join("uncommitted.txt"), "keep").unwrap();
        git(&f.repo(), &["worktree", "lock", path.to_str().unwrap()]).unwrap();
        let entries = list(&f.repo()).unwrap();
        let entry = entries.iter().find(|e| e.path == path).unwrap();
        assert_eq!(entry.branch.as_deref(), Some("feature/test"));
        assert_eq!(entry.dirty, Some(true));
        assert!(entry.locked);
        assert_eq!(repository(&path).unwrap(), repository(&f.repo()).unwrap());
        assert_eq!(list(&path).unwrap().len(), 2);
    }

    #[test]
    fn rejects_offline_relative_traversal_and_repository_roots() {
        let f = Fixture::new();
        for root in [
            f.0.join("offline"),
            PathBuf::from("relative"),
            f.roots().join(".."),
            f.repo(),
        ] {
            assert!(
                create(&f.repo(), &root, "work", "work").is_err(),
                "{}",
                root.display()
            );
        }
        assert!(!f.0.join("offline").exists());
        for name in ["..", "../escape", "a/b", "-option", ""] {
            // Leading '-' folders are harmless path components; only branches
            // can become Git options, so check traversal names here.
            if name == "-option" {
                continue;
            }
            assert!(create(&f.repo(), &f.roots(), name, "work").is_err());
        }
        assert!(create(&f.repo(), &f.roots(), "work", "-option").is_err());
    }

    #[test]
    fn conflicts_preserve_existing_files_and_branch() {
        let f = Fixture::new();
        let target = f.roots().join("existing");
        fs::create_dir(&target).unwrap();
        fs::write(target.join("user.txt"), "keep").unwrap();
        assert!(create(&f.repo(), &f.roots(), "existing", "new").is_err());
        assert_eq!(fs::read_to_string(target.join("user.txt")).unwrap(), "keep");
        assert!(create(&f.repo(), &f.roots(), "fresh", "main").is_err());
        assert!(!f.roots().join("fresh").exists());
        assert_eq!(list(&f.repo()).unwrap().len(), 1);
    }

    #[test]
    fn concurrent_creates_admit_only_one_without_force_or_registry() {
        let f = Fixture::new();
        let (repo, root) = (f.repo(), f.roots());
        let threads: Vec<_> = (0..2)
            .map(|_| {
                let (repo, root) = (repo.clone(), root.clone());
                std::thread::spawn(move || create(&repo, &root, "same", "same"))
            })
            .collect();
        assert_eq!(
            threads
                .into_iter()
                .map(|t| t.join().unwrap())
                .filter(Result::is_ok)
                .count(),
            1
        );
        assert_eq!(list(&repo).unwrap().len(), 2);
    }

    #[cfg(unix)]
    #[test]
    fn symlink_destination_and_replaced_repository_are_preserved_and_reported() {
        let f = Fixture::new();
        let other = Fixture::new();
        std::os::unix::fs::symlink(other.repo(), f.roots().join("link")).unwrap();
        assert!(create(&f.repo(), &f.roots(), "link", "link").is_err());
        let path = create(&f.repo(), &f.roots(), "replaced", "replaced").unwrap();
        fs::rename(&path, f.roots().join("saved")).unwrap();
        std::os::unix::fs::symlink(other.repo(), &path).unwrap();
        let entries = list(&f.repo()).unwrap();
        assert!(entries
            .iter()
            .find(|e| e.branch.as_deref() == Some("replaced"))
            .unwrap()
            .error
            .is_some());
        assert!(other.repo().exists());
    }

    #[test]
    fn unavailable_checkout_is_listed_with_unknown_dirty_state() {
        let f = Fixture::new();
        let path = create(&f.repo(), &f.roots(), "offline", "offline").unwrap();
        fs::rename(&path, f.roots().join("unmounted")).unwrap();
        let entries = list(&f.repo()).unwrap();
        let entry = entries.iter().find(|e| e.path == path).unwrap();
        assert_eq!(entry.dirty, None);
        assert!(entry.error.is_some());
    }

    #[test]
    fn discovers_cli_created_detached_worktree_with_spaces_unicode_and_newline() {
        let f = Fixture::new();
        let path = f.roots().join("CLI worktree 中文\nnext\n");
        git(
            &f.repo(),
            &[
                "worktree",
                "add",
                "--detach",
                path.to_str().unwrap(),
                "HEAD",
            ],
        )
        .unwrap();
        let entries = list(&f.repo()).unwrap();
        let entry = entries.iter().find(|entry| entry.path == path).unwrap();
        assert_eq!(entry.branch, None);
        assert_eq!(entry.dirty, Some(false));
        assert!(entry.error.is_none());
    }

    #[test]
    fn rejects_git_metadata_bare_and_broken_repository_roots() {
        let f = Fixture::new();
        let bare = f.0.join("bare.git");
        fs::create_dir(&bare).unwrap();
        git(&bare, &["init", "--bare"]).unwrap();
        let broken = f.0.join("broken");
        fs::create_dir(&broken).unwrap();
        fs::create_dir(broken.join("nested")).unwrap();
        let marker = format!("gitdir: {}\n", f.0.join("missing").display());
        fs::write(broken.join(".git"), &marker).unwrap();
        for directory in [
            f.repo().join(".git"),
            f.repo().join(".git/objects"),
            bare.clone(),
            bare.join("objects"),
            broken.clone(),
            broken.join("nested"),
        ] {
            assert!(root(&directory).is_err(), "{}", directory.display());
            assert!(create(&f.repo(), &directory, "blocked", "blocked").is_err());
            assert!(!directory.join("blocked").exists());
        }
        assert_eq!(fs::read_to_string(broken.join(".git")).unwrap(), marker);
        assert_eq!(list(&f.repo()).unwrap().len(), 1);
    }

    #[cfg(unix)]
    #[test]
    fn unwritable_directory_returns_error_without_changing_permissions_or_fallback() {
        use std::os::unix::fs::PermissionsExt;
        let f = Fixture::new();
        let root = f.roots();
        let permissions = fs::metadata(&root).unwrap().permissions();
        fs::set_permissions(&root, fs::Permissions::from_mode(0o500)).unwrap();
        let result = create(&f.repo(), &root, "denied", "denied");
        let observed = fs::metadata(&root).unwrap().permissions().mode() & 0o777;
        fs::set_permissions(&root, permissions).unwrap();
        assert!(result.is_err());
        assert_eq!(observed, 0o500);
        assert!(!root.join("denied").exists());
        assert_eq!(list(&f.repo()).unwrap().len(), 1);
    }
    #[test]
    fn creation_recovers_after_lock_owner_exits_without_drop() {
        const CHILD_ROOT: &str = "BODHI_WORKTREE_TEST_EXIT_WITH_LOCK";
        if let Some(repo) = std::env::var_os(CHILD_ROOT) {
            let path = repository(Path::new(&repo))
                .unwrap()
                .join("bodhi-worktree-create.lock");
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&path)
                .unwrap();
            file.try_lock().unwrap();
            let _lock = CreateLock { path, file };
            let control = Path::new(&repo).parent().unwrap();
            fs::write(control.join("lock-owner-ready"), "ready").unwrap();
            for _ in 0..3000 {
                if control.join("lock-owner-exit").exists() {
                    // Skip Rust destructors, as in an abrupt process exit.
                    std::process::exit(0);
                }
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            std::process::exit(9);
        }
        let f = Fixture::new();
        let test_name = std::thread::current().name().unwrap().to_owned();
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", &test_name, "--test-threads=1"])
            .env(CHILD_ROOT, f.repo())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        for _ in 0..3000 {
            if f.0.join("lock-owner-ready").exists() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        let ready = f.0.join("lock-owner-ready").exists();
        let live = child.try_wait().unwrap().is_none();
        let while_live = create(&f.repo(), &f.roots(), "while_live", "while/live");
        fs::write(f.0.join("lock-owner-exit"), "exit without Drop").unwrap();
        let status = child.wait().unwrap();
        assert!(
            ready && live && status.success(),
            "Child owner probe did not settle normally"
        );
        assert!(while_live.is_err(), "A live owner must never be preempted");
        assert!(!f.roots().join("while_live").exists());
        let path = repository(&f.repo())
            .unwrap()
            .join("bodhi-worktree-create.lock");
        assert!(path.exists(), "Child must leave the lock file behind");
        let created = create(&f.repo(), &f.roots(), "after_exit", "after/exit").unwrap();
        assert!(created.join(".git").exists());
    }

    #[test]
    fn live_lock_blocks_then_reuses_existing_file_without_truncation() {
        let f = Fixture::new();
        let path = repository(&f.repo())
            .unwrap()
            .join("bodhi-worktree-create.lock");
        fs::write(&path, "preserve existing metadata content\n").unwrap();
        let identity = fs::metadata(&path).unwrap();
        let owner = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        owner.try_lock().unwrap();
        assert!(create(&f.repo(), &f.roots(), "while_busy", "while/busy").is_err());
        assert!(!f.roots().join("while_busy").exists());
        drop(owner);
        create(&f.repo(), &f.roots(), "after_close", "after/close").unwrap();
        create(&f.repo(), &f.roots(), "reuse", "reuse").unwrap();
        assert_eq!(
            fs::read_to_string(&path).unwrap(),
            "preserve existing metadata content\n"
        );
        assert!(same_identity(&identity, &fs::metadata(&path).unwrap()));
    }

    #[test]
    fn creation_lock_rejects_symlinks_and_non_regular_paths() {
        let f = Fixture::new();
        let path = repository(&f.repo())
            .unwrap()
            .join("bodhi-worktree-create.lock");
        #[cfg(unix)]
        {
            let user_file = f.0.join("preserved-content");
            fs::write(&user_file, "must remain untouched\n").unwrap();
            std::os::unix::fs::symlink(&user_file, &path).unwrap();
            assert!(create(&f.repo(), &f.roots(), "linked", "linked").is_err());
            assert_eq!(
                fs::read_to_string(&user_file).unwrap(),
                "must remain untouched\n"
            );
            assert!(fs::symlink_metadata(&path)
                .unwrap()
                .file_type()
                .is_symlink());
            fs::remove_file(&path).unwrap();
        }
        fs::create_dir(&path).unwrap();
        assert!(create(&f.repo(), &f.roots(), "directory", "directory").is_err());
        assert!(path.is_dir());
    }
}
