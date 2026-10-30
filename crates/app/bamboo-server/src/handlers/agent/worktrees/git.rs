//! Git is the only worktree registry. This module deliberately has no runtime
//! dependencies, so its real filesystem/Git regressions can run without Cargo.
use std::fs::{self, OpenOptions};
use std::io::Read;
use std::path::{Component, Path, PathBuf};
use std::process::{Command, Output};
use std::sync::mpsc;
use std::time::{Duration, Instant};

/// Hard ceiling for one Git child process. A checkout on slow or offline
/// storage (for example macOS-evicted dataless files) can block `git status`
/// inside the kernel for minutes; the inventory must never inherit that.
/// Tests run with a short ceiling so timeout regressions stay fast.
fn git_command_timeout() -> Duration {
    if cfg!(test) {
        Duration::from_millis(1500)
    } else {
        Duration::from_secs(10)
    }
}

/// Wall-clock budget for inspecting all entries of one repository during a
/// `list` call. The listing itself (`git worktree list`) is metadata-only;
/// per-entry inspection shells out repeatedly and is what can stall. Once
/// the budget is spent, remaining entries keep metadata with unknown dirty
/// state instead of blocking the inventory response.
fn list_inspect_budget() -> Duration {
    if cfg!(test) {
        Duration::from_millis(4000)
    } else {
        Duration::from_secs(60)
    }
}

/// Wall-clock budget for one complete inventory request, covering every
/// repository it scans. The HTTP response must always return, so this is the
/// bound the caller shares across repositories.
pub fn inventory_budget() -> Duration {
    list_inspect_budget()
}

/// Runs one Git child process to completion, or kills it at the deadline.
///
/// `Command::output()` waits forever: its failure model has no exit for a
/// child that never exits. Storage that stalls the kernel (dataless files,
/// dead network mounts) produces exactly that child, so every Git
/// invocation in this module funnels through this bounded wrapper instead.
/// Std-only: stdin is null so commands that read it (`mktree`) see the same
/// immediate EOF `output()` gave them instead of inheriting a long-lived
/// server stdin; one reader thread per pipe drains output (so a full pipe
/// can never deadlock the child); the parent polls `try_wait`; a missed
/// deadline escalates to `kill()` plus a reaping `wait()`, which guarantees
/// the pipe writers are gone before `recv()` runs. A killed child surfaces
/// as a normal `Err` string so callers degrade that one entry instead of
/// hanging the whole inventory.
fn bounded_output(mut command: Command, deadline_at: Instant) -> Result<Output, String> {
    let mut child = command
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| format!("Cannot run git: {e}"))?;
    let stdout_pipe = child.stdout.take();
    let stderr_pipe = child.stderr.take();
    let (stdout_tx, stdout_rx) = mpsc::channel();
    let (stderr_tx, stderr_rx) = mpsc::channel();
    let stdout_reader = std::thread::spawn(move || {
        let mut buf = Vec::new();
        if let Some(mut pipe) = stdout_pipe {
            let _ = pipe.read_to_end(&mut buf);
        }
        let _ = stdout_tx.send(buf);
    });
    let stderr_reader = std::thread::spawn(move || {
        let mut buf = Vec::new();
        if let Some(mut pipe) = stderr_pipe {
            let _ = pipe.read_to_end(&mut buf);
        }
        let _ = stderr_tx.send(buf);
    });
    let mut status = None;
    while Instant::now() < deadline_at {
        match child.try_wait() {
            Ok(exited) => {
                if exited.is_some() {
                    status = exited;
                    break;
                }
            }
            Err(_) => break,
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    if status.is_none() {
        // SIGKILL terminates the child, closing its pipe write ends, so the
        // reader threads return and recv() below can never block on a pipe
        // whose writer is gone. wait() also reaps the corpse.
        let _ = child.kill();
        let _ = child.wait();
    }
    // A descendant that inherited the pipes (a content filter, a pager) can
    // outlive the direct child and keep them open. Collect output with a
    // bounded grace period so an orphaned writer cannot reintroduce the hang
    // this wrapper exists to prevent. `join()` cannot time out, so a reader
    // that did not report within the grace period is detached instead: it owns
    // no shared state and exits on its own once the last writer closes.
    let grace = Duration::from_secs(1);
    let stdout = stdout_rx.recv_timeout(grace);
    let stderr = stderr_rx.recv_timeout(grace);
    if stdout.is_ok() {
        let _ = stdout_reader.join();
    }
    if stderr.is_ok() {
        let _ = stderr_reader.join();
    }
    let stdout = stdout.unwrap_or_default();
    let stderr = stderr.unwrap_or_default();
    match status {
        Some(status) => Ok(Output {
            status,
            stdout,
            stderr,
        }),
        None => Err(
            "git did not finish within its command timeout; the checkout may be on slow or offline storage"
                .to_string(),
        ),
    }
}

#[derive(Debug, Default)]
pub struct Worktree {
    pub path: PathBuf,
    pub branch: Option<String>,
    pub head: String,
    pub locked: bool,
    pub prunable: bool,
    pub dirty: Option<bool>,
    pub error: Option<String>,
    pub openable: bool,
    pub(super) bare: bool,
}

pub(super) fn git_command(repo: &Path) -> Command {
    let mut command = Command::new("git");
    command
        .args([
            "-c",
            "core.fsmonitor=false",
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "submodule.recurse=false",
            "-c",
            "gc.auto=0",
            "-c",
            "maintenance.auto=false",
        ])
        .arg("-C")
        .arg(repo)
        .env_remove("GIT_DIR")
        .env_remove("GIT_COMMON_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .env_remove("GIT_OBJECT_DIRECTORY")
        .env_remove("GIT_ALTERNATE_OBJECT_DIRECTORIES")
        .env_remove("GIT_QUARANTINE_PATH")
        .env_remove("GIT_CEILING_DIRECTORIES")
        .env_remove("GIT_CONFIG")
        .env_remove("GIT_CONFIG_PARAMETERS")
        .env_remove("GIT_CONFIG_COUNT")
        .env("LC_ALL", "C")
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_OPTIONAL_LOCKS", "0")
        .env("GIT_NO_LAZY_FETCH", "1")
        .env("GIT_ALLOW_PROTOCOL", "");
    command
}

fn command(repo: &Path, args: &[&str]) -> Result<Output, String> {
    command_by(repo, args, Instant::now() + git_command_timeout())
}

/// Runs one Git command, bounded by `deadline_at`. The effective ceiling is
/// also capped by the per-command ceiling, so one slow entry can never spend
/// a whole inventory budget on a single child.
fn command_by(repo: &Path, args: &[&str], deadline_at: Instant) -> Result<Output, String> {
    let ceiling = Instant::now() + git_command_timeout();
    let deadline_at = deadline_at.min(ceiling);
    let mut command = git_command(repo);
    if matches!(args.first(), Some(&"status" | &"read-tree"))
        || args.starts_with(&["worktree", "add"])
    {
        // Reading configuration names does not execute filters. Include every
        // scope and include file, but never read/log their potentially secret
        // values. Bounded too: an include file can live on stalled storage.
        let mut probe = git_command(repo);
        probe.args(["config", "--includes", "--null", "--name-only", "--list"]);
        let configured = bounded_output(probe, deadline_at)
            .map_err(|e| format!("Cannot inspect Git filter configuration: {e}"))?;
        if !configured.status.success() {
            return Err(format!(
                "Cannot inspect Git filter configuration: {}",
                String::from_utf8_lossy(&configured.stderr).trim()
            ));
        }
        let names = std::str::from_utf8(&configured.stdout)
            .map_err(|_| "Git filter names must be UTF-8")?;
        let mut drivers = std::collections::BTreeSet::new();
        for name in names.split('\0') {
            if let Some((driver, kind)) = name
                .strip_prefix("filter.")
                .and_then(|s| s.rsplit_once('.'))
            {
                if matches!(kind, "clean" | "smudge" | "process") {
                    // -c splits at the first '='. Never let a subsection name
                    // change which key is overridden.
                    if driver.contains('=') {
                        return Err("Git filter names containing '=' are unsupported".into());
                    }
                    drivers.insert(driver);
                }
            }
        }
        for driver in drivers {
            for kind in ["clean", "smudge", "process"] {
                command.args(["-c", &format!("filter.{driver}.{kind}=")]);
            }
            // A checkout needing conversion must fail explicitly, rather than
            // silently materialize raw pointers or report an inaccurate status.
            command.args(["-c", &format!("filter.{driver}.required=true")]);
        }
    }
    command.args(args);
    bounded_output(command, deadline_at).map_err(|e| format!("Git at {}: {e}", repo.display()))
}

fn git_bytes(repo: &Path, args: &[&str]) -> Result<Vec<u8>, String> {
    git_bytes_by(repo, args, Instant::now() + git_command_timeout())
}

fn git_bytes_by(repo: &Path, args: &[&str], deadline_at: Instant) -> Result<Vec<u8>, String> {
    let output = command_by(repo, args, deadline_at)?;
    if !output.status.success() {
        let diagnostic = String::from_utf8_lossy(&output.stderr);
        let guidance = if diagnostic.contains(": smudge filter ")
            || diagnostic.contains(": clean filter ")
        {
            "External Git content filters are disabled in Worktree manager. Use a trusted Git client to prepare checkouts that need conversion, then open them here. "
        } else {
            ""
        };
        return Err(format!(
            "Git at {}: {guidance}{}",
            repo.display(),
            diagnostic.trim()
        ));
    }
    Ok(output.stdout)
}

fn git(repo: &Path, args: &[&str]) -> Result<String, String> {
    git_by(repo, args, Instant::now() + git_command_timeout())
}

/// Deadline-aware variant: discovery scans many candidates under one shared
/// inventory budget, so its probes must not each restart the clock.
fn git_by(repo: &Path, args: &[&str], deadline_at: Instant) -> Result<String, String> {
    String::from_utf8(git_bytes_by(repo, args, deadline_at)?)
        .map_err(|_| "Git returned non-UTF-8 text".into())
}

/// Test-only convenience wrapper; production callers share the inventory
/// deadline through `git_path_by`.
#[cfg(test)]
fn git_path(repo: &Path, args: &[&str]) -> Result<PathBuf, String> {
    git_path_by(repo, args, Instant::now() + git_command_timeout())
}

fn git_path_by(repo: &Path, args: &[&str], deadline_at: Instant) -> Result<PathBuf, String> {
    let mut bytes = git_bytes_by(repo, args, deadline_at)?;
    if bytes.last() == Some(&b'\n') {
        bytes.pop();
    }
    path_from_bytes(&bytes)
}

fn path_from_bytes(bytes: &[u8]) -> Result<PathBuf, String> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        Ok(PathBuf::from(std::ffi::OsStr::from_bytes(bytes)))
    }
    #[cfg(not(unix))]
    {
        std::str::from_utf8(bytes)
            .map(PathBuf::from)
            .map_err(|_| "Git returned a path that cannot be represented on this platform".into())
    }
}

/// JSON paths are Unicode. Preserve a raw path internally and expose an explicit
/// escaped label for unsupported bytes, never a lossy path that could be opened.
pub fn path_label(path: &Path) -> String {
    path.to_str()
        .map(str::to_owned)
        .unwrap_or_else(|| format!("{:?}", path.as_os_str()))
}

pub fn repository(repo: &Path) -> Result<PathBuf, String> {
    repository_by(repo, Instant::now() + git_command_timeout())
}

fn repository_by(repo: &Path, deadline_at: Instant) -> Result<PathBuf, String> {
    let value = git_path_by(
        repo,
        &["rev-parse", "--path-format=absolute", "--git-common-dir"],
        deadline_at,
    )?;
    fs::canonicalize(value).map_err(|e| format!("Repository metadata is unavailable: {e}"))
}

// Observe actual resolver calls without changing process-global PATH. Each
// HTTP regression uses unique fixture paths and never resets shared state.
#[cfg(test)]
static WORKSPACE_ROOT_QUERIES: std::sync::Mutex<Vec<PathBuf>> = std::sync::Mutex::new(Vec::new());

#[cfg(test)]
pub(super) fn workspace_root_queries_for_test(path: &Path) -> usize {
    WORKSPACE_ROOT_QUERIES
        .lock()
        .unwrap()
        .iter()
        .filter(|p| p.as_path() == path)
        .count()
}

pub fn workspace_root(path: &Path) -> Result<PathBuf, String> {
    workspace_root_by(path, Instant::now() + git_command_timeout())
}

fn workspace_root_by(path: &Path, deadline_at: Instant) -> Result<PathBuf, String> {
    #[cfg(test)]
    WORKSPACE_ROOT_QUERIES
        .lock()
        .unwrap()
        .push(path.to_path_buf());
    let value = git_path_by(path, &["rev-parse", "--show-toplevel"], deadline_at)?;
    fs::canonicalize(value).map_err(|e| e.to_string())
}

#[derive(Debug, Default)]
pub struct Discovery {
    pub repositories: Vec<PathBuf>,
    pub diagnostics: Vec<(PathBuf, String)>,
}

#[derive(Clone, Copy)]
struct DiscoveryLimits {
    max_depth: usize,
    max_entries: usize,
}

/// Discover checkouts within one registered Project root without changing Git
/// or Project state. A directly bound repository subdirectory keeps its exact
/// path: discovery must not broaden that binding to its ancestor repository.
/// Each root is bounded to 12 levels and 20,000 directory entries, including
/// ignored entries. Truncation is reported rather than presented as complete.
pub fn discover_repositories(root: &Path) -> Discovery {
    discover_until(root, Instant::now() + inventory_budget())
}

/// Discovery under the caller's inventory deadline. Every Git probe inside the
/// walk inherits this deadline, so a root whose metadata is on stalled storage
/// cannot spend more than the request budget before `list_until` even runs.
pub fn discover_until(root: &Path, deadline_at: Instant) -> Discovery {
    discover_with_limits(
        root,
        DiscoveryLimits {
            max_depth: 12,
            max_entries: 20_000,
        },
        deadline_at,
    )
}

fn discover_with_limits(root: &Path, limits: DiscoveryLimits, deadline_at: Instant) -> Discovery {
    let mut result = Discovery::default();
    let canonical = match fs::canonicalize(root) {
        Ok(path) if path.is_dir() => path,
        Ok(_) => {
            result.diagnostics.push((
                root.to_path_buf(),
                "Project workspace is not a directory".into(),
            ));
            return result;
        }
        Err(error) => {
            result.diagnostics.push((
                root.to_path_buf(),
                format!("Project workspace is unavailable: {error}"),
            ));
            return result;
        }
    };
    let mut scan = RepositoryScan {
        root: &canonical,
        limits,
        remaining_entries: limits.max_entries,
        exhausted: false,
        deadline_at,
        result: &mut result,
    };
    scan.visit(&canonical, 0);
    result.repositories.sort();
    result.repositories.dedup();
    result.diagnostics.sort();
    result.diagnostics.dedup();
    result
}

struct RepositoryScan<'a> {
    root: &'a Path,
    limits: DiscoveryLimits,
    remaining_entries: usize,
    exhausted: bool,
    /// Shared inventory deadline; every Git probe in the walk is bounded by it.
    deadline_at: Instant,
    result: &'a mut Discovery,
}

impl RepositoryScan<'_> {
    fn diagnostic(&mut self, path: &Path, message: impl Into<String>) {
        self.result
            .diagnostics
            .push((path.to_path_buf(), message.into()));
    }

    /// True once the shared budget is spent. The walk then stops probing and
    /// reports the remainder as uninspected instead of overrunning the
    /// request budget one command timeout per candidate.
    fn expired(&self) -> bool {
        Instant::now() >= self.deadline_at
    }

    fn visit(&mut self, path: &Path, depth: usize) {
        if self.expired() {
            self.diagnostic(
                path,
                "Discovery budget exhausted; the remaining tree was not inspected",
            );
            return;
        }
        // Recheck each path at use time. Directory symlinks are never followed,
        // including aliases that lead back inside the same Project.
        let metadata = match fs::symlink_metadata(path) {
            Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => metadata,
            Ok(_) => return,
            Err(error) => {
                self.diagnostic(path, format!("Cannot inspect directory: {error}"));
                return;
            }
        };
        match fs::canonicalize(path) {
            Ok(canonical) if canonical == path && canonical.starts_with(self.root) => {}
            Ok(_) => {
                self.diagnostic(path, "Directory changed or escapes the Project workspace");
                return;
            }
            Err(error) => {
                self.diagnostic(path, format!("Cannot resolve directory: {error}"));
                return;
            }
        }
        let marker = match fs::symlink_metadata(path.join(".git")) {
            Ok(marker) if marker.is_dir() || marker.is_file() => true,
            Ok(_) => {
                self.diagnostic(path, "Git marker is not a regular file or directory");
                return;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
            Err(error) => {
                self.diagnostic(path, format!("Cannot inspect Git marker: {error}"));
                return;
            }
        };
        // Bare repositories have no .git marker. Recognize their metadata
        // shape without invoking Git for every ordinary directory, then stop
        // before walking objects, refs or nested linked-worktree metadata.
        let metadata_shape = path.join("HEAD").is_file()
            && path.join("objects").is_dir()
            && path.join("refs").is_dir();
        if metadata_shape {
            match git_by(
                path,
                &["rev-parse", "--is-bare-repository"],
                self.deadline_at,
            ) {
                Ok(value) if value.trim() == "true" => {
                    // Explicit bare source bindings already support worktree
                    // creation. Preserve that path without scanning metadata.
                    if depth == 0 && path.to_str().is_some() && retains_identity(path, &metadata) {
                        self.result.repositories.push(path.to_path_buf());
                    } else {
                        self.diagnostic(path, "Bare Git metadata directory was not scanned");
                    }
                    return;
                }
                Err(error) => {
                    self.diagnostic(path, error);
                    return;
                }
                Ok(_) => {
                    if git_by(
                        path,
                        &["rev-parse", "--is-inside-git-dir"],
                        self.deadline_at,
                    )
                    .is_ok_and(|value| value.trim() == "true")
                    {
                        self.diagnostic(
                            path,
                            "Git metadata directory has no working checkout; skipped",
                        );
                        return;
                    }
                }
            }
        }
        if depth == 0 || marker {
            match workspace_root_by(path, self.deadline_at) {
                Ok(checkout) if (depth == 0 && path.starts_with(&checkout)) || checkout == path => {
                    if depth > 0 {
                        // A .git file alone can point at an unrelated repository.
                        // Git's native registry must also identify this checkout
                        // before a discovered path can authorize branch creation.
                        match registered_worktree_by(path, self.deadline_at) {
                            Ok(true) => {}
                            Ok(false) => {
                                self.diagnostic(
                                    path,
                                    "Discovered directory is not a registered Git worktree",
                                );
                                return;
                            }
                            Err(error) => {
                                self.diagnostic(path, error);
                                return;
                            }
                        }
                    }
                    if path.to_str().is_some() {
                        self.result.repositories.push(path.to_path_buf());
                    } else {
                        self.diagnostic(
                            path,
                            "Repository path contains non-UTF-8 bytes; this UI cannot open it",
                        );
                        return;
                    }
                }
                Ok(_) => {
                    self.diagnostic(
                        path,
                        "Git checkout resolves outside its discovered directory",
                    );
                }
                Err(error) => {
                    let ordinary_directory = !marker
                        && !path
                            .ancestors()
                            .skip(1)
                            .any(|ancestor| fs::symlink_metadata(ancestor.join(".git")).is_ok())
                        && error.starts_with(&format!(
                            "Git at {}: fatal: not a git repository (or any",
                            path.display()
                        ));
                    if !ordinary_directory {
                        self.diagnostic(path, error);
                        if depth == 0
                            && git_by(
                                path,
                                &["rev-parse", "--is-inside-git-dir"],
                                self.deadline_at,
                            )
                            .is_ok_and(|value| value.trim() == "true")
                        {
                            return;
                        }
                    }
                }
            }
        }
        if !retains_identity(path, &metadata) {
            self.result
                .repositories
                .retain(|candidate| candidate != path);
            self.diagnostic(path, "Directory changed during repository discovery");
            return;
        }
        let entries = match fs::read_dir(path) {
            Ok(entries) => entries,
            Err(error) => {
                self.diagnostic(path, format!("Cannot read directory: {error}"));
                return;
            }
        };
        let mut directories = Vec::new();
        for entry in entries {
            if self.remaining_entries == 0 {
                self.exhausted = true;
                self.diagnostic(
                    path,
                    format!(
                        "Repository discovery truncated after {} directory entries",
                        self.limits.max_entries
                    ),
                );
                // Do not select an arbitrary read_dir prefix. Previously
                // completed directories remain useful and deterministic.
                return;
            }
            self.remaining_entries -= 1;
            match entry.and_then(|entry| Ok((entry.file_type()?, entry.path()))) {
                Ok((kind, child)) if kind.is_dir() && !ignored_discovery_directory(&child) => {
                    directories.push(child);
                }
                Ok(_) => {}
                Err(error) => {
                    self.diagnostic(path, format!("Cannot read directory entry: {error}"))
                }
            }
        }
        directories.sort();
        if depth == self.limits.max_depth && !directories.is_empty() {
            self.diagnostic(
                path,
                format!(
                    "Repository discovery truncated at depth {}",
                    self.limits.max_depth
                ),
            );
            return;
        }
        for directory in directories {
            self.visit(&directory, depth + 1);
            if self.exhausted {
                break;
            }
        }
    }
}

/// Test-only convenience wrapper; production callers must share the inventory
/// deadline through `registered_worktree_by`.
#[cfg(test)]
fn registered_worktree(path: &Path) -> Result<bool, String> {
    registered_worktree_by(path, Instant::now() + git_command_timeout())
}

fn registered_worktree_by(path: &Path, deadline_at: Instant) -> Result<bool, String> {
    let listing = git_bytes_by(
        path,
        &["worktree", "list", "--porcelain", "-z"],
        deadline_at,
    )?;
    for field in listing.split(|byte| *byte == 0) {
        if let Some(registered) = field.strip_prefix(b"worktree ") {
            let registered = path_from_bytes(registered)?;
            if fs::canonicalize(registered).is_ok_and(|registered| registered == path) {
                return Ok(true);
            }
        }
    }
    // Git lists a submodule's separate metadata directory as its main entry.
    // Accept its reverse checkout binding only from the shared metadata's own
    // config, never a candidate-controlled config.worktree, include or global.
    let common = repository_by(path, deadline_at)?;
    Ok(configured_main_worktree_by(path, &common, deadline_at)?
        .is_some_and(|configured| configured == path))
}

/// Test-only convenience wrapper; the list path threads its deadline through
/// `configured_main_worktree_by`.
#[cfg(test)]
fn configured_main_worktree(repo: &Path, common: &Path) -> Result<Option<PathBuf>, String> {
    configured_main_worktree_by(repo, common, Instant::now() + git_command_timeout())
}

fn configured_main_worktree_by(
    repo: &Path,
    common: &Path,
    deadline_at: Instant,
) -> Result<Option<PathBuf>, String> {
    let config = common.join("config");
    let config = config
        .to_str()
        .ok_or("Git metadata configuration path contains non-UTF-8 bytes")?;
    let output = command_by(
        repo,
        &[
            "config",
            "--file",
            config,
            "--no-includes",
            "--path",
            "--null",
            "--get",
            "core.worktree",
        ],
        deadline_at,
    )?;
    if output.status.code() == Some(1) {
        return Ok(None);
    }
    if !output.status.success() {
        return Err(format!(
            "Cannot verify Git checkout binding: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    let bytes = output.stdout.strip_suffix(&[0]).unwrap_or(&output.stdout);
    let configured = path_from_bytes(bytes)?;
    fs::canonicalize(common.join(configured))
        .map(Some)
        .map_err(|error| format!("Configured Git checkout is unavailable: {error}"))
}

/// Test-only convenience wrapper; `list_until` calls `main_worktree_by` with
/// the inventory deadline.
#[cfg(test)]
fn main_worktree_for_list(repo: &Path, common: &Path) -> Result<PathBuf, String> {
    main_worktree_by(repo, common, Instant::now() + git_command_timeout())
}

/// Deadline-aware variant: called from `list_until`, so its probes must stay
/// inside the inventory budget instead of restarting the clock per call.
fn main_worktree_by(repo: &Path, common: &Path, deadline_at: Instant) -> Result<PathBuf, String> {
    let verify = |checkout: PathBuf| {
        if repository_by(&checkout, deadline_at)? == common
            && workspace_root_by(&checkout, deadline_at)? == checkout
        {
            Ok(checkout)
        } else {
            Err("Configured Git main checkout belongs to a different repository".to_string())
        }
    };
    let configured = configured_main_worktree_by(repo, common, deadline_at)
        .and_then(|checkout| {
            checkout.ok_or_else(|| {
                "Native Git main entry points to metadata without a verified checkout binding"
                    .to_string()
            })
        })
        .and_then(verify);
    match configured {
        Ok(checkout) => Ok(checkout),
        Err(error) => {
            // An explicitly bound main checkout may use separate metadata with
            // no reverse config. Its own Git directory proves it is the main
            // caller; a linked worktree must never impersonate that record.
            let main_caller = git_path_by(repo, &["rev-parse", "--absolute-git-dir"], deadline_at)
                .and_then(|path| fs::canonicalize(path).map_err(|error| error.to_string()))
                .is_ok_and(|path| path == common);
            if main_caller {
                workspace_root_by(repo, deadline_at).and_then(verify)
            } else {
                Err(error)
            }
        }
    }
}

fn ignored_discovery_directory(path: &Path) -> bool {
    matches!(
        path.file_name().and_then(|name| name.to_str()),
        Some(
            ".git"
                | ".hg"
                | ".svn"
                | "node_modules"
                | "target"
                | "dist"
                | "build"
                | ".next"
                | ".nuxt"
                | ".venv"
                | "venv"
                | "__pycache__"
                | ".cache"
                | ".turbo"
                | ".parcel-cache"
                | "coverage"
        )
    )
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

/// Single-repository convenience wrapper used by tests; the inventory handler
/// shares one deadline across repositories through `list_until`.
#[cfg(test)]
pub fn list(repo: &Path) -> Result<Vec<Worktree>, String> {
    list_until(repo, Instant::now() + list_inspect_budget())
}

/// Lists one repository's worktrees, inspecting entries only until
/// `overall_deadline`. An inventory scans several repositories, so the caller
/// shares one deadline across them: the per-repository budget alone would make
/// the response unbounded in the number of repositories.
pub fn list_until(repo: &Path, overall_deadline: Instant) -> Result<Vec<Worktree>, String> {
    let deadline = overall_deadline.min(Instant::now() + list_inspect_budget());
    let common = repository(repo)?;
    let listing = git_bytes(repo, &["worktree", "list", "--porcelain", "-z"])?;
    let mut result = Vec::new();
    let mut entry: Option<Worktree> = None;
    for field in listing.split(|byte| *byte == 0) {
        if let Some(path) = field.strip_prefix(b"worktree ") {
            if let Some(previous) = entry.take() {
                result.push(previous);
            }
            entry = Some(Worktree {
                path: path_from_bytes(path)?,
                ..Default::default()
            });
        } else if let Some(current) = entry.as_mut() {
            if let Some(head) = field.strip_prefix(b"HEAD ") {
                current.head = String::from_utf8_lossy(head).into_owned();
            }
            if let Some(branch) = field.strip_prefix(b"branch refs/heads/") {
                current.branch = Some(String::from_utf8_lossy(branch).into_owned());
            }
            if field == b"locked" || field.starts_with(b"locked ") {
                current.locked = true;
            }
            if field == b"prunable" || field.starts_with(b"prunable ") {
                current.prunable = true;
            }
            if field == b"bare" {
                current.bare = true;
            }
        }
    }
    if let Some(previous) = entry {
        result.push(previous);
    }
    for entry in &mut result {
        if entry.bare {
            entry.error = Some("Bare repository metadata has no working checkout".into());
            continue;
        }
        // Native Git represents a submodule/separate-git-dir main checkout by
        // its metadata path. Never expose that path as an openable workspace.
        // Linked entries have their own registry paths and are left unchanged.
        if fs::canonicalize(&entry.path).is_ok_and(|path| path == common) {
            match main_worktree_by(repo, &common, deadline) {
                Ok(path) => entry.path = path,
                Err(error) => {
                    entry.error = Some(error);
                    continue;
                }
            }
        }
        if entry.path.to_str().is_none() {
            entry.error = Some("Worktree path contains non-UTF-8 bytes and is displayed as an escaped label. This UI cannot open it; use a Git client. No files were changed.".into());
            continue;
        }
        let mut inspect = || -> Result<(PathBuf, bool), String> {
            if Instant::now() >= deadline {
                return Err(
                    "Inspection budget exhausted; dirty state is unknown for this checkout".into(),
                );
            }
            let canonical = fs::canonicalize(&entry.path)
                .map_err(|e| format!("{} is unavailable: {e}", entry.path.display()))?;
            if repository_by(&canonical, deadline)? != common
                || workspace_root_by(&canonical, deadline)? != canonical
            {
                return Err("Worktree path now belongs to a different repository".into());
            }
            // Opening depends on checkout identity, not whether Git can report
            // its complete dirty state (for example, a disabled content filter).
            entry.openable = true;
            let index = git_bytes_by(&canonical, &["ls-files", "--stage", "-z"], deadline)?;
            let has_submodules = index
                .split(|byte| *byte == 0)
                .any(|entry| entry.starts_with(b"160000 "));
            let status = git_bytes_by(
                &canonical,
                &[
                    "status",
                    "--porcelain",
                    "-z",
                    "--untracked-files=normal",
                    "--ignore-submodules=dirty",
                ],
                deadline,
            )?;
            if status.is_empty() && has_submodules {
                return Err(
                    "Submodule working-copy changes were not inspected; dirty state is unknown"
                        .into(),
                );
            }
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
        // Register first, then read configuration in the actual new branch /
        // gitdir context. Conditional includes can differ from the source.
        git(
            repo,
            &[
                "worktree",
                "add",
                "--no-checkout",
                "-b",
                branch,
                "--",
                target_arg,
                "HEAD",
            ],
        )?;
        if repository(&target)? != common
            || workspace_root(&target)? != target
            || !retains_identity(&root, &root_identity)
            || !retains_identity(&target, &target_identity)
            || !retains_identity(&common, &repo_identity)
            || !lock.is_current()
        {
            return Err("Repository or destination changed before checkout".into());
        }
        let empty = git(&target, &["mktree"])?;
        // A native two-tree merge from the initially empty checkout preserves
        // concurrent staged/unstaged content and rejects untracked collisions.
        // Never reset, force-checkout, prune, or delete a branch on failure.
        git(
            &target,
            &[
                "read-tree",
                "-m",
                "-u",
                "--no-sparse-checkout",
                empty.trim(),
                "HEAD",
            ],
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

    fn init_discovery_repo(path: &Path) {
        fs::create_dir_all(path).unwrap();
        git(path, &["init", "-b", "main"]).unwrap();
    }

    #[test]
    fn discovery_scans_containers_and_nested_repositories_in_stable_order() {
        let f = Fixture::new();
        let container = f.roots();
        let nested = container.join("z-group/repo");
        let outer = container.join("a-outer");
        let inner = outer.join("packages/inner");
        for path in [&nested, &outer, &inner] {
            init_discovery_repo(path);
        }
        let discovery = discover_repositories(&container);
        assert_eq!(discovery.repositories, vec![outer, inner, nested]);
        assert!(discovery.diagnostics.is_empty(), "{discovery:?}");
        assert_eq!(
            discovery.repositories,
            discover_repositories(&container).repositories
        );
    }

    #[test]
    fn discovery_preserves_direct_subdirectory_binding_without_parent_expansion() {
        let f = Fixture::new();
        let subdirectory = f.repo().join("src/deep");
        fs::create_dir_all(&subdirectory).unwrap();
        let discovery = discover_repositories(&subdirectory);
        assert_eq!(discovery.repositories, vec![subdirectory]);
        assert!(discovery.diagnostics.is_empty(), "{discovery:?}");
        let empty = discover_repositories(&f.roots());
        assert!(empty.repositories.is_empty());
        assert!(empty.diagnostics.is_empty(), "{empty:?}");
    }

    #[test]
    fn discovery_accepts_linked_worktrees_and_separate_git_metadata_files() {
        let f = Fixture::new();
        let linked = create(&f.repo(), &f.roots(), "linked", "linked").unwrap();
        let submodule = f.roots().join("group/submodule");
        fs::create_dir_all(&submodule).unwrap();
        let metadata = f.0.join("submodule-metadata");
        git(
            &submodule,
            &["init", "--separate-git-dir", metadata.to_str().unwrap()],
        )
        .unwrap();
        // Separated Git metadata without a reverse checkout binding is
        // indistinguishable from an arbitrary pointer into another repository.
        let unbound = discover_repositories(&f.roots());
        assert_eq!(unbound.repositories, vec![linked.clone()]);
        assert_eq!(unbound.diagnostics.len(), 1);
        assert_eq!(
            discover_repositories(&submodule).repositories,
            vec![submodule.clone()]
        );
        let explicit_entries = list(&submodule).unwrap();
        assert_eq!(explicit_entries.len(), 1);
        assert_eq!(explicit_entries[0].path, submodule);
        assert!(explicit_entries[0].error.is_none());
        git(
            &submodule,
            &["config", "core.worktree", submodule.to_str().unwrap()],
        )
        .unwrap();
        assert!(linked.join(".git").is_file());
        assert!(submodule.join(".git").is_file());
        let discovery = discover_repositories(&f.roots());
        assert_eq!(discovery.repositories, vec![submodule, linked]);
        assert!(discovery.diagnostics.is_empty(), "{discovery:?}");
    }

    #[test]
    fn discovery_accepts_an_initialized_real_git_submodule() {
        let f = Fixture::new();
        let parent = f.roots().join("parent");
        init_discovery_repo(&parent);
        let output = git_command(&parent)
            .env("GIT_ALLOW_PROTOCOL", "file")
            .args([
                "-c",
                "protocol.file.allow=always",
                "submodule",
                "add",
                f.repo().to_str().unwrap(),
                "modules/child",
            ])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let submodule = parent.join("modules/child");
        assert!(submodule.join(".git").is_file());
        let discovery = discover_repositories(&f.roots());
        assert_eq!(discovery.repositories, vec![parent, submodule]);
        assert!(discovery.diagnostics.is_empty(), "{discovery:?}");
        let submodule = discovery.repositories[1].clone();
        let entries = list(&submodule).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].path, submodule);
        assert_eq!(entries[0].dirty, Some(false));
        assert!(entries[0].error.is_none());
        assert!(entries[0].openable);
        let linked = create(&submodule, &f.roots(), "module-linked", "module-linked").unwrap();
        let entries = list(&submodule).unwrap();
        assert_eq!(entries.len(), 2);
        assert!(entries.iter().any(|entry| entry.path == submodule));
        assert!(entries.iter().any(|entry| entry.path == linked));
        assert!(entries
            .iter()
            .all(|entry| entry.openable && entry.dirty == Some(false) && entry.error.is_none()));
        let common = repository(&submodule).unwrap();
        fs::rename(&submodule, f.0.join("saved-submodule")).unwrap();
        let partial = list(&linked).unwrap();
        assert_eq!(partial.len(), 2);
        let unavailable_main = partial.iter().find(|entry| entry.path == common).unwrap();
        assert!(!unavailable_main.openable);
        assert_eq!(unavailable_main.dirty, None);
        assert!(unavailable_main.error.is_some());
        let usable_linked = partial.iter().find(|entry| entry.path == linked).unwrap();
        assert!(usable_linked.openable);
        assert_eq!(usable_linked.dirty, Some(false));
        assert!(usable_linked.error.is_none());
    }

    #[test]
    fn bare_repository_linked_caller_keeps_native_bare_entry_unopenable() {
        let f = Fixture::new();
        let bare = f.0.join("bare.git");
        let output = git_command(&f.0)
            .env("GIT_ALLOW_PROTOCOL", "file")
            .args([
                "clone",
                "--bare",
                f.repo().to_str().unwrap(),
                bare.to_str().unwrap(),
            ])
            .output()
            .unwrap();
        assert!(output.status.success());
        let linked = create(&bare, &f.roots(), "bare-linked", "from-bare").unwrap();
        for caller in [&bare, &linked] {
            let entries = list(caller).unwrap();
            assert_eq!(entries.len(), 2);
            let main = entries.iter().find(|entry| entry.path == bare).unwrap();
            assert!(main.bare);
            assert!(!main.openable);
            assert_eq!(main.dirty, None);
            assert!(main.error.is_some());
            let checkout = entries.iter().find(|entry| entry.path == linked).unwrap();
            assert!(!checkout.bare);
            assert!(checkout.openable);
            assert_eq!(checkout.dirty, Some(false));
            assert!(checkout.error.is_none());
        }
    }

    #[test]
    fn discovery_rejects_unregistered_git_file_and_common_directory_pointers() {
        let f = Fixture::new();
        let outside = f.repo().join(".git");
        let forged_file = f.roots().join("forged-file");
        fs::create_dir(&forged_file).unwrap();
        fs::write(
            forged_file.join(".git"),
            format!("gitdir: {}\n", outside.display()),
        )
        .unwrap();
        let forged_directory = f.roots().join("forged-directory");
        init_discovery_repo(&forged_directory);
        fs::write(
            forged_directory.join(".git/commondir"),
            format!("{}\n", outside.display()),
        )
        .unwrap();
        for forged in [&forged_file, &forged_directory] {
            // --show-toplevel alone accepts both forged pointers; it does not
            // establish a native registration for the discovered checkout.
            assert_eq!(workspace_root(forged).unwrap(), *forged);
            assert_eq!(repository(forged).unwrap(), outside);
        }
        let before = git(&f.repo(), &["show-ref"]).unwrap();
        let discovery = discover_repositories(&f.roots());
        assert!(discovery.repositories.is_empty(), "{discovery:?}");
        assert_eq!(discovery.diagnostics.len(), 2, "{discovery:?}");
        assert!(discovery
            .diagnostics
            .iter()
            .all(|(_, message)| message.contains("not a registered Git worktree")));
        assert_eq!(git(&f.repo(), &["show-ref"]).unwrap(), before);
        // Explicitly registered Project roots retain their existing contract.
        assert_eq!(
            discover_repositories(&forged_file).repositories,
            vec![forged_file.clone()]
        );
        // A candidate must not manufacture its reverse binding through its
        // per-worktree configuration or an included file in shared config.
        let injected = forged_directory.join("injected-config");
        let content = format!("[core]\nworktree = {}\n", forged_directory.display());
        fs::write(&injected, &content).unwrap();
        fs::write(forged_directory.join(".git/config.worktree"), &content).unwrap();
        git(&f.repo(), &["config", "extensions.worktreeConfig", "true"]).unwrap();
        git(
            &f.repo(),
            &["config", "include.path", injected.to_str().unwrap()],
        )
        .unwrap();
        assert_eq!(workspace_root(&forged_directory).unwrap(), forged_directory);
        assert!(!registered_worktree(&forged_directory).unwrap());
        assert!(discover_repositories(&f.roots()).repositories.is_empty());
    }

    #[test]
    fn discovery_retains_broken_git_and_filesystem_diagnostics() {
        let f = Fixture::new();
        let broken_file = f.roots().join("broken-file");
        let broken_directory = f.roots().join("broken-directory");
        fs::create_dir(&broken_file).unwrap();
        fs::create_dir_all(broken_directory.join(".git")).unwrap();
        fs::write(broken_file.join(".git"), "gitdir: missing-metadata\n").unwrap();
        let discovery = discover_repositories(&f.roots());
        assert!(discovery.repositories.is_empty());
        assert_eq!(discovery.diagnostics.len(), 2, "{discovery:?}");
        assert!(discovery
            .diagnostics
            .iter()
            .all(|(_, message)| message.contains("Git at")));
        let nested = broken_directory.join("src");
        fs::create_dir(&nested).unwrap();
        assert_eq!(discover_repositories(&nested).diagnostics.len(), 1);
        let unavailable = discover_repositories(&f.0.join("missing"));
        assert_eq!(unavailable.diagnostics.len(), 1);
        assert!(unavailable.diagnostics[0].1.contains("unavailable"));
    }

    #[test]
    fn discovery_skips_dependency_output_and_bare_repository_metadata() {
        let f = Fixture::new();
        for ignored in ["node_modules", "target", "dist", "build", ".venv", ".git"] {
            init_discovery_repo(&f.roots().join(ignored).join("hidden"));
        }
        // A container with an invalid .git directory must retain that diagnostic,
        // but its metadata is still never recursively treated as source.
        let bare = f.roots().join("bare.git");
        fs::create_dir(&bare).unwrap();
        git(&bare, &["init", "--bare"]).unwrap();
        init_discovery_repo(&bare.join("objects/hidden"));
        let visible = f.roots().join(".config/visible");
        init_discovery_repo(&visible);
        let discovery = discover_repositories(&f.roots());
        assert_eq!(discovery.repositories, vec![visible]);
        assert!(discovery
            .diagnostics
            .iter()
            .any(|(path, error)| path == &bare && error.contains("metadata directory")));
        let direct_bare = discover_repositories(&bare);
        assert_eq!(direct_bare.repositories, vec![bare.clone()]);
        assert!(direct_bare.diagnostics.is_empty());
        for metadata in [bare.join("objects"), f.repo().join(".git")] {
            let discovery = discover_repositories(&metadata);
            assert!(discovery.repositories.is_empty(), "{discovery:?}");
            assert!(!discovery.diagnostics.is_empty(), "{discovery:?}");
        }
    }

    #[test]
    fn discovery_reports_depth_and_entry_limits_without_arbitrary_prefixes() {
        let f = Fixture::new();
        let shallow = f.roots().join("a-shallow");
        let deep = f.roots().join("z/deep/repo");
        init_discovery_repo(&shallow);
        init_discovery_repo(&deep);
        let discovery = discover_with_limits(
            &f.roots(),
            DiscoveryLimits {
                max_depth: 1,
                max_entries: 100,
            },
            std::time::Instant::now() + inventory_budget(),
        );
        assert_eq!(discovery.repositories, vec![shallow]);
        assert!(discovery
            .diagnostics
            .iter()
            .any(|(_, error)| error.contains("truncated at depth 1")));
        let discovery = discover_with_limits(
            &f.roots(),
            DiscoveryLimits {
                max_depth: 12,
                max_entries: 1,
            },
            std::time::Instant::now() + inventory_budget(),
        );
        assert!(discovery.repositories.is_empty());
        assert_eq!(discovery.diagnostics.len(), 1);
        assert!(discovery.diagnostics[0]
            .1
            .contains("truncated after 1 directory entries"));
    }

    #[cfg(unix)]
    #[test]
    fn discovery_does_not_follow_directory_or_git_marker_symlinks() {
        let f = Fixture::new();
        let other = Fixture::new();
        std::os::unix::fs::symlink(other.repo(), f.roots().join("outside")).unwrap();
        std::os::unix::fs::symlink(f.roots(), f.roots().join("loop")).unwrap();
        let malformed = f.roots().join("marker-link");
        fs::create_dir(&malformed).unwrap();
        std::os::unix::fs::symlink(other.repo().join(".git"), malformed.join(".git")).unwrap();
        let discovery = discover_repositories(&f.roots());
        assert!(discovery.repositories.is_empty(), "{discovery:?}");
        assert_eq!(discovery.diagnostics.len(), 1, "{discovery:?}");
        assert_eq!(discovery.diagnostics[0].0, malformed);
    }

    // APFS cannot create raw-byte directory names. Exercise the real traversal
    // on Unix filesystems that support these paths instead of a lossy fixture.
    #[cfg(all(unix, not(target_os = "macos")))]
    #[test]
    fn discovery_reports_non_utf8_repository_without_exposing_a_candidate() {
        use std::os::unix::ffi::OsStringExt;
        let f = Fixture::new();
        let raw = f
            .roots()
            .join(std::ffi::OsString::from_vec(b"non-utf8-\xff".to_vec()));
        fs::create_dir_all(&raw).unwrap();
        // Git may include the raw directory name in its initialization output.
        git_bytes(&raw, &["init", "-b", "main"]).unwrap();
        let discovery = discover_repositories(&f.roots());
        assert!(discovery.repositories.is_empty(), "{discovery:?}");
        assert_eq!(discovery.diagnostics.len(), 1, "{discovery:?}");
        assert!(discovery
            .diagnostics
            .iter()
            .any(|(path, error)| path == &raw && error.contains("non-UTF-8")));
    }

    #[test]
    fn discovery_rejects_candidate_with_checkout_redirected_outside_project() {
        let f = Fixture::new();
        let redirected = f.roots().join("redirected");
        init_discovery_repo(&redirected);
        git(
            &redirected,
            &["config", "core.worktree", f.repo().to_str().unwrap()],
        )
        .unwrap();
        let discovery = discover_repositories(&f.roots());
        assert!(discovery.repositories.is_empty(), "{discovery:?}");
        assert_eq!(discovery.diagnostics.len(), 1);
        assert!(discovery.diagnostics[0].1.contains("outside"));
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

    #[cfg(unix)]
    fn callback_script(f: &Fixture, name: &str) -> (PathBuf, PathBuf, String) {
        use std::os::unix::fs::PermissionsExt;
        let script = f.0.join(name);
        fs::write(&script, "#!/bin/sh\n: > \"$0.proof\"\nexit 1\n").unwrap();
        fs::set_permissions(&script, fs::Permissions::from_mode(0o700)).unwrap();
        let proof = PathBuf::from(format!("{}.proof", script.display()));
        let command = format!("'{}'", script.to_str().unwrap().replace('\'', "'\\''"));
        (script, proof, command)
    }

    #[cfg(unix)]
    fn commit_fixture(repo: &Path) {
        git(repo, &["add", "."]).unwrap();
        git(
            repo,
            &[
                "-c",
                "user.name=Test",
                "-c",
                "user.email=test@example.invalid",
                "-c",
                "commit.gpgsign=false",
                "commit",
                "-m",
                "tracked fixture",
            ],
        )
        .unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn inventory_never_executes_repository_fsmonitor() {
        let f = Fixture::new();
        fs::write(f.repo().join("tracked.txt"), "tracked\n").unwrap();
        commit_fixture(&f.repo());
        let (_, proof, callback) = callback_script(&f, "fsmonitor");
        git(&f.repo(), &["config", "core.fsmonitor", &callback]).unwrap();
        let inventory = list(&f.repo()).unwrap();
        assert_eq!(inventory[0].dirty, Some(false));
        assert!(!proof.exists(), "Inventory executed repository fsmonitor");
        assert_eq!(
            git(&f.repo(), &["config", "--local", "core.fsmonitor"])
                .unwrap()
                .trim(),
            callback
        );
    }

    #[cfg(unix)]
    #[test]
    fn creation_never_executes_repository_checkout_hook() {
        let f = Fixture::new();
        let (script, proof, _) = callback_script(&f, "post-checkout");
        fs::write(&script, "#!/bin/sh\n: > \"$0.proof\"\nexit 0\n").unwrap();
        git(
            &f.repo(),
            &[
                "config",
                "core.hooksPath",
                script.parent().unwrap().to_str().unwrap(),
            ],
        )
        .unwrap();
        create(&f.repo(), &f.roots(), "no_hook", "no/hook").unwrap();
        assert!(
            !proof.exists(),
            "Creation executed repository post-checkout"
        );
        assert!(script.exists());
    }

    #[cfg(unix)]
    #[test]
    fn content_filters_are_not_executed_or_silently_bypassed() {
        let mut observations = Vec::new();
        for (kind, driver, included) in [
            ("clean", "task-filter", false),
            ("smudge", "task-filter", false),
            ("process", "task-filter", false),
            ("smudge", "task=filter", false),
            ("smudge", "task-filter", true),
        ] {
            let f = Fixture::new();
            fs::write(
                f.repo().join(".gitattributes"),
                format!("*.txt filter={driver}\n"),
            )
            .unwrap();
            fs::write(f.repo().join("tracked.txt"), "tracked\n").unwrap();
            commit_fixture(&f.repo());
            let (_, proof, callback) = callback_script(&f, "filter");
            let key = format!("filter.{driver}.{kind}");
            if included {
                let config = f.0.join("included.config");
                git(
                    &f.repo(),
                    &[
                        "config",
                        "--file",
                        config.to_str().unwrap(),
                        &key,
                        &callback,
                    ],
                )
                .unwrap();
                git(
                    &f.repo(),
                    &["config", "include.path", config.to_str().unwrap()],
                )
                .unwrap();
            } else {
                git(&f.repo(), &["config", &key, &callback]).unwrap();
            }
            let explicit_error = if kind == "clean" {
                fs::write(f.repo().join("tracked.txt"), "changed\n").unwrap();
                let entries = list(&f.repo()).unwrap();
                entries[0].openable
                    && entries[0].dirty.is_none()
                    && entries[0]
                        .error
                        .as_ref()
                        .is_some_and(|e| e.contains("filter"))
            } else {
                create(&f.repo(), &f.roots(), "no_filter", "no/filter")
                    .is_err_and(|e| e.contains("filter"))
            };
            let unchanged = git(&f.repo(), &["config", &key]).unwrap().trim() == callback;
            observations.push((
                kind,
                driver,
                included,
                proof.exists(),
                explicit_error,
                unchanged,
            ));
        }
        assert!(
            observations
                .iter()
                .all(|(_, _, _, executed, explicit, unchanged)| !executed
                    && *explicit
                    && *unchanged),
            "Unsafe filter observations: {observations:?}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn nested_repository_filters_are_not_run_for_parent_inventory() {
        let f = Fixture::new();
        let child = f.repo().join("nested");
        fs::create_dir(&child).unwrap();
        git(&child, &["init", "-b", "main"]).unwrap();
        fs::write(child.join(".gitattributes"), "*.txt filter=nested\n").unwrap();
        fs::write(child.join("tracked.txt"), "tracked\n").unwrap();
        commit_fixture(&child);
        let head = git(&child, &["rev-parse", "HEAD"]).unwrap();
        fs::write(
            f.repo().join(".gitmodules"),
            "[submodule \"nested\"]\npath = nested\nurl = ../unused\n",
        )
        .unwrap();
        git(
            &f.repo(),
            &[
                "update-index",
                "--add",
                "--cacheinfo",
                &format!("160000,{},nested", head.trim()),
            ],
        )
        .unwrap();
        commit_fixture(&f.repo());
        git(&f.repo(), &["config", "submodule.nested.active", "true"]).unwrap();
        git(&f.repo(), &["config", "diff.ignoreSubmodules", "none"]).unwrap();
        let (_, proof, callback) = callback_script(&f, "nested-filter");
        git(&child, &["config", "filter.nested.clean", &callback]).unwrap();
        fs::write(child.join("tracked.txt"), "changed\n").unwrap();
        let inventory = list(&f.repo()).unwrap();
        assert!(!proof.exists(), "Inventory ran a nested repository filter");
        assert!(inventory[0].dirty.is_none());
        assert!(inventory[0]
            .error
            .as_ref()
            .is_some_and(|e| e.contains("Submodule")));
        fs::write(
            f.repo().join("untracked.txt"),
            "parent dirtiness is still known\n",
        )
        .unwrap();
        assert_eq!(list(&f.repo()).unwrap()[0].dirty, Some(true));
        assert!(!proof.exists());
    }

    #[cfg(unix)]
    #[test]
    fn creation_suppresses_filters_selected_only_in_the_new_branch() {
        let mut observations = Vec::new();
        for kind in ["smudge", "process"] {
            let f = Fixture::new();
            fs::write(f.repo().join(".gitattributes"), "*.txt filter=context\n").unwrap();
            fs::write(f.repo().join("tracked.txt"), "tracked\n").unwrap();
            commit_fixture(&f.repo());
            let (_, proof, callback) = callback_script(&f, "context-filter");
            let config = f.0.join("context.config");
            let key = format!("filter.context.{kind}");
            git(
                &f.repo(),
                &[
                    "config",
                    "--file",
                    config.to_str().unwrap(),
                    &key,
                    &callback,
                ],
            )
            .unwrap();
            git(
                &f.repo(),
                &[
                    "config",
                    "includeIf.onbranch:no/context.path",
                    config.to_str().unwrap(),
                ],
            )
            .unwrap();
            let source_names = git(
                &f.repo(),
                &["config", "--includes", "--name-only", "--list"],
            )
            .unwrap();
            assert!(!source_names.contains(&key));
            let result = create(&f.repo(), &f.roots(), "context", "no/context");
            observations.push((
                kind,
                proof.exists(),
                result.is_err_and(|error| error.contains("filter")),
                f.roots().join("context/.git").exists(),
                git(
                    &f.repo(),
                    &["show-ref", "--verify", "refs/heads/no/context"],
                )
                .is_ok(),
            ));
        }
        assert!(
            observations
                .iter()
                .all(|(_, executed, explicit, metadata, branch)| !executed
                    && *explicit
                    && *metadata
                    && *branch),
            "New-context filters: {observations:?}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn git_children_see_eof_stdin_even_when_the_parent_keeps_stdin_open() {
        // bamboo serve runs with an open stdin forever. Children that read
        // stdin (`mktree` inside create()) must see EOF immediately, not the
        // inherited server stdin, or they block until the command timeout.
        let f = Fixture::new();
        let read_end = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(f.0.join("held-open-stdin"))
            .unwrap();
        let _held = StdioHolder(read_end);
        let started = std::time::Instant::now();
        let created = create(&f.repo(), &f.roots(), "stdin-eof", "stdin-eof").unwrap();
        assert!(
            started.elapsed() < git_command_timeout(),
            "create must not wait for the command ceiling"
        );
        assert!(created.join(".git").is_file());
    }

    /// Keeps the held file descriptor open for the test body; a plain File
    /// would be dropped (and closed) before the assertion runs.
    #[cfg(unix)]
    struct StdioHolder(std::fs::File);

    #[cfg(unix)]
    #[test]
    fn initial_checkout_preserves_collisions_and_staged_and_unstaged_content() {
        let f = Fixture::new();
        fs::write(f.repo().join("tracked.txt"), "tracked\n").unwrap();
        commit_fixture(&f.repo());
        let fresh = create(&f.repo(), &f.roots(), "fresh", "fresh").unwrap();
        assert_eq!(
            fs::read_to_string(fresh.join("tracked.txt")).unwrap(),
            "tracked\n"
        );
        let empty = git(&f.repo(), &["mktree"]).unwrap();
        for mode in ["collision", "staged"] {
            let target = f.roots().join(mode);
            git(
                &f.repo(),
                &[
                    "worktree",
                    "add",
                    "--no-checkout",
                    "-b",
                    mode,
                    "--",
                    target.to_str().unwrap(),
                    "HEAD",
                ],
            )
            .unwrap();
            if mode == "collision" {
                fs::write(target.join("tracked.txt"), "preserve user collision\n").unwrap();
            } else {
                fs::write(target.join("user.txt"), "preserve staged content\n").unwrap();
                git(&target, &["add", "user.txt"]).unwrap();
                fs::write(target.join("user.txt"), "preserve unstaged content\n").unwrap();
            }
            let result = git(
                &target,
                &[
                    "read-tree",
                    "-m",
                    "-u",
                    "--no-sparse-checkout",
                    empty.trim(),
                    "HEAD",
                ],
            );
            if mode == "collision" {
                assert!(result.is_err());
                assert_eq!(
                    fs::read_to_string(target.join("tracked.txt")).unwrap(),
                    "preserve user collision\n"
                );
            } else {
                result.unwrap();
                assert_eq!(
                    git(&target, &["show", ":user.txt"]).unwrap(),
                    "preserve staged content\n"
                );
                assert_eq!(
                    fs::read_to_string(target.join("user.txt")).unwrap(),
                    "preserve unstaged content\n"
                );
                assert_eq!(
                    fs::read_to_string(target.join("tracked.txt")).unwrap(),
                    "tracked\n"
                );
            }
        }
    }

    #[cfg(unix)]
    #[test]
    fn non_utf8_cli_worktree_does_not_hide_other_inventory() {
        use std::os::unix::ffi::{OsStrExt, OsStringExt};
        let f = Fixture::new();
        let valid = create(&f.repo(), &f.roots(), "valid", "valid").unwrap();
        let registered = f.roots().join("registered");
        git(
            &f.repo(),
            &[
                "worktree",
                "add",
                "-b",
                "bytes",
                registered.to_str().unwrap(),
                "HEAD",
            ],
        )
        .unwrap();
        let metadata = git(&registered, &["rev-parse", "--absolute-git-dir"]).unwrap();
        let raw = f
            .roots()
            .join(std::ffi::OsString::from_vec(b"invalid-\xff".to_vec()));
        // APFS rejects raw-byte filenames. A Git registration imported from a
        // Unix filesystem can still contain them; Git itself emits this entry.
        let gitdir = [raw.as_os_str().as_bytes(), b"/.git\n"].concat();
        let record = Path::new(metadata.trim()).join("gitdir");
        fs::write(&record, &gitdir).unwrap();
        let native = command(&f.repo(), &["worktree", "list", "--porcelain", "-z"]).unwrap();
        assert!(native.status.success());
        assert!(native
            .stdout
            .windows(b"invalid-\xff".len())
            .any(|v| v == b"invalid-\xff"));
        let entries =
            list(&f.repo()).expect("A raw-byte Git registration must not hide other checkouts");
        assert_eq!(entries.len(), 3);
        assert_eq!(
            entries.iter().find(|e| e.path == f.repo()).unwrap().dirty,
            Some(false)
        );
        assert_eq!(
            entries.iter().find(|e| e.path == valid).unwrap().dirty,
            Some(false)
        );
        let unavailable = entries.iter().find(|e| e.path == raw).unwrap();
        assert_eq!(unavailable.branch.as_deref(), Some("bytes"));
        assert_eq!(unavailable.dirty, None);
        assert!(
            unavailable.prunable,
            "Preserve Git's actual missing-path fact"
        );
        assert!(unavailable.error.as_deref().unwrap().contains("non-UTF-8"));
        assert_eq!(fs::read(record).unwrap(), gitdir);
        assert!(registered.join(".git").is_file());
    }

    #[cfg(unix)]
    #[test]
    fn raw_index_filename_bytes_preserve_known_dirty_state() {
        use std::io::Write;
        use std::process::Stdio;
        let f = Fixture::new();
        let mut blob = git_command(&f.repo())
            .args(["hash-object", "-w", "--stdin"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        blob.stdin
            .take()
            .unwrap()
            .write_all(b"owned raw index fixture\n")
            .unwrap();
        let output = blob.wait_with_output().unwrap();
        assert!(output.status.success());
        let oid = String::from_utf8(output.stdout).unwrap();
        let record = [
            format!("100644 {}\t", oid.trim()).as_bytes(),
            b"tracked-\xff\0",
        ]
        .concat();
        let mut index = git_command(&f.repo())
            .args(["update-index", "-z", "--index-info"])
            .stdin(Stdio::piped())
            .spawn()
            .unwrap();
        index.stdin.take().unwrap().write_all(&record).unwrap();
        assert!(index.wait().unwrap().success());
        let native = command(
            &f.repo(),
            &["status", "--porcelain", "-z", "--ignore-submodules=dirty"],
        )
        .unwrap();
        assert!(native.status.success());
        assert!(native.stdout.contains(&0xff));
        let entries = list(&f.repo()).unwrap();
        assert_eq!(
            entries[0].dirty,
            Some(true),
            "Native Git reports a known staged/missing file, not unknown"
        );
        assert!(entries[0].error.is_none());
        let after = command(&f.repo(), &["ls-files", "--stage", "-z"]).unwrap();
        assert!(after.status.success());
        assert!(after.stdout.contains(&0xff));
    }

    #[cfg(unix)]
    #[test]
    fn stalled_checkout_times_out_and_keeps_other_inventory_entries() {
        let f = Fixture::new();
        let healthy = create(&f.repo(), &f.roots(), "healthy", "healthy").unwrap();
        let stalled = create(&f.repo(), &f.roots(), "stalled", "stalled").unwrap();
        // An unopening FIFO as the linked worktree's index reproduces the
        // production hang exactly: any Git that reads the index blocks in
        // the kernel, while metadata listing (`worktree list`) stays fast.
        // Dataless (macOS-evicted) working trees stall the same way.
        let admin = f.repo().join(".git").join("worktrees").join("stalled");
        fs::remove_file(admin.join("index")).unwrap();
        let _guard = FifoSentinel::create(admin.join("index"));
        let started = std::time::Instant::now();
        let entries = list(&f.repo()).expect("inventory must return despite the stalled entry");
        let elapsed = started.elapsed();
        assert!(
            elapsed < list_inspect_budget() + git_command_timeout(),
            "list took {elapsed:?}, beyond budget {:?} + ceiling {:?}",
            list_inspect_budget(),
            git_command_timeout()
        );
        assert_eq!(entries.len(), 3, "main + healthy + stalled all present");
        let hung = entries
            .iter()
            .find(|entry| entry.path == stalled)
            .expect("stalled entry kept");
        assert_eq!(
            hung.dirty, None,
            "a stalled checkout reports unknown dirty state"
        );
        assert!(
            hung.error.as_deref().unwrap().contains("timeout")
                || hung
                    .error
                    .as_deref()
                    .unwrap()
                    .contains("slow or offline storage"),
            "error explains the timeout: {:?}",
            hung.error
        );
        let fine = entries
            .iter()
            .find(|entry| entry.path == healthy)
            .expect("healthy entry present");
        assert_eq!(fine.dirty, Some(false));
        assert!(fine.error.is_none(), "{:?}", fine.error);
        let main = entries
            .iter()
            .find(|entry| entry.path == f.repo())
            .expect("main entry present");
        assert_eq!(main.dirty, Some(false));
        assert!(main.error.is_none(), "{:?}", main.error);
    }

    #[cfg(unix)]
    #[test]
    fn exhausted_budget_degrades_remaining_entries_without_git_calls() {
        let f = Fixture::new();
        let healthy = create(&f.repo(), &f.roots(), "healthy", "healthy").unwrap();
        let stalled = create(&f.repo(), &f.roots(), "stalled", "stalled").unwrap();
        let admin = f.repo().join(".git").join("worktrees").join("stalled");
        fs::remove_file(admin.join("index")).unwrap();
        let _guard = FifoSentinel::create(admin.join("index"));
        // Burn the whole budget on the first stalled entry (list order is
        // main first), then verify the remaining entries still degrade with
        // metadata instead of spawning further Git children.
        let entries = list(&f.repo()).unwrap();
        assert!(entries.iter().any(|entry| entry.path == healthy));
        assert!(entries.iter().any(|entry| entry.path == stalled));
        assert!(
            entries.iter().filter(|entry| entry.dirty.is_none()).count() >= 1,
            "at least the stalled entry has unknown dirty state: {entries:?}"
        );
    }

    /// Keeps a FIFO path present for the test body and removes it on drop so
    /// Fixture cleanup (remove_dir_all) does not block opening the pipe.
    #[cfg(unix)]
    struct FifoSentinel(PathBuf);
    #[cfg(unix)]
    impl FifoSentinel {
        #[allow(clippy::unnecessary_wraps)]
        fn create(path: PathBuf) -> Self {
            nix_mknod_fifo(&path);
            Self(path)
        }
    }
    #[cfg(unix)]
    impl Drop for FifoSentinel {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.0);
        }
    }

    #[cfg(unix)]
    fn nix_mknod_fifo(path: &Path) {
        // mkfifo(3) via libc would add a dependency; a binding-free fallback
        // is not available, so use the nix-free Command route.
        let status = Command::new("mkfifo")
            .arg(path)
            .status()
            .expect("mkfifo is available on unix hosts");
        assert!(status.success(), "mkfifo {path:?} failed");
    }

    #[cfg(unix)]
    #[test]
    fn expired_discovery_budget_stops_probing_and_reports_partial_results() {
        let f = Fixture::new();
        let container = f.roots();
        for name in ["a", "b", "c", "d"] {
            let repo = container.join(name);
            init_discovery_repo(&repo);
            // A FIFO as .git/config stalls every probe that needs config
            // (workspace_root/repository/bare detection), the shape Codex
            // reproduced against stalled metadata storage.
            fs::remove_file(repo.join(".git/config")).unwrap();
            let _guard = FifoSentinel::create(repo.join(".git/config"));
            std::mem::forget(_guard);
        }
        let started = std::time::Instant::now();
        let discovery = discover_until(
            &container,
            std::time::Instant::now() + Duration::from_millis(1500),
        );
        let elapsed = started.elapsed();
        assert!(
            elapsed < Duration::from_millis(6000),
            "discovery took {elapsed:?}; the shared deadline must apply to every probe"
        );
        assert!(
            discovery
                .diagnostics
                .iter()
                .any(|(_, message)| message.contains("budget exhausted")),
            "expiry must be reported as a diagnostic: {:?}",
            discovery.diagnostics
        );
        // Unblock teardown: the Fixture drop removes the tree, and an open FIFO
        // reader would otherwise hold the walk.
        for name in ["a", "b", "c", "d"] {
            let _ = fs::remove_file(container.join(name).join(".git/config"));
        }
    }
}
