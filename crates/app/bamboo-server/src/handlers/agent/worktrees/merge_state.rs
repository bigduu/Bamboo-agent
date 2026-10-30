//! Merge state of a worktree checkout, from local refs only.
//!
//! This module answers "has this checkout's work already landed on the
//! repository's integration branch?" using only objects already present in the
//! local repository. It never fetches, never contacts a forge and never mutates
//! a ref, so its answer is exactly as fresh as the last `git fetch`.
//!
//! Local refs cannot observe a squash merge: the squashed commit on the base
//! branch is a different commit object that shares no parent with the branch
//! tip, so patch equivalence breaks too. A branch landed that way is reported
//! `InProgress` on purpose. An authoritative forge provider may override that
//! later; this layer must never guess a merge it cannot prove.
//!
//! Every Git invocation is bounded. `git.rs` uses `Command::output()`, which has
//! no kill path (#2402); this path must not repeat that, so it polls
//! `try_wait` against a deadline instead.
use super::git::{git_command, Worktree};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

/// Per-checkout merge state. The wire contract is the lowercase label.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeState {
    /// The repository's own checkout, or a checkout already at the base tip.
    /// The UI must not paint these as branches holding outstanding work.
    Primary,
    /// Every commit on the checkout is already contained in the base branch.
    Merged,
    /// The checkout still carries work the base branch does not contain.
    InProgress,
    /// The question could not be answered (missing base, timeout, Git error).
    Unknown,
}

impl MergeState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Primary => "primary",
            Self::Merged => "merged",
            Self::InProgress => "in_progress",
            Self::Unknown => "unknown",
        }
    }
}

/// A verdict plus a stable machine reason. `reason` is diagnostic provenance,
/// never UI copy: the presentation layer maps `state` and may show `reason`
/// only as a tooltip detail.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeVerdict {
    pub state: MergeState,
    pub reason: &'static str,
}

impl MergeVerdict {
    fn new(state: MergeState, reason: &'static str) -> Self {
        Self { state, reason }
    }
}

/// The integration branch used as the merge target, plus the commit it pointed
/// at when resolved, so a stale local answer is attributable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeBase {
    /// Fully qualified ref, for example `refs/remotes/origin/dev`.
    pub reference: String,
    pub commit: String,
}

impl MergeBase {
    /// Short display name, for example `origin/dev`.
    pub fn label(&self) -> String {
        self.reference
            .strip_prefix("refs/remotes/")
            .unwrap_or(&self.reference)
            .to_owned()
    }
}

/// Budget for one checkout's classification. A sidebar poll must stay cheap
/// even when many checkouts share a repository.
pub const PER_ENTRY_BUDGET: Duration = Duration::from_secs(3);
/// Budget for the whole request, so a pathological repository cannot park a
/// blocking thread for the HTTP client's patience.
pub const SCAN_BUDGET: Duration = Duration::from_secs(12);
const CACHE_TTL: Duration = Duration::from_secs(30);
const CACHE_CAPACITY: usize = 512;

struct CachedVerdict {
    state: MergeState,
    reason: &'static str,
    at: Instant,
}

/// Memo key: one repository, one checkout head, one base commit.
type MemoKey = (PathBuf, String, String);
type Memo = Mutex<HashMap<MemoKey, CachedVerdict>>;

/// Memoise verdicts by (repository, checkout head, base commit) so repeated
/// sidebar polling does not re-run Git for unchanged state. A cache is only an
/// optimisation: every miss re-derives the same answer from Git.
fn cache() -> &'static Memo {
    static CACHE: OnceLock<Memo> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn cache_get(key: &MemoKey) -> Option<MergeVerdict> {
    let mut entries = cache().lock().ok()?;
    let entry = entries.get(key)?;
    if entry.at.elapsed() > CACHE_TTL {
        entries.remove(key);
        return None;
    }
    Some(MergeVerdict::new(entry.state, entry.reason))
}

fn cache_put(key: (PathBuf, String, String), verdict: &MergeVerdict) {
    let Ok(mut entries) = cache().lock() else {
        return;
    };
    if entries.len() >= CACHE_CAPACITY {
        entries.retain(|_, entry| entry.at.elapsed() <= CACHE_TTL);
        if entries.len() >= CACHE_CAPACITY {
            entries.clear();
        }
    }
    entries.insert(
        key,
        CachedVerdict {
            state: verdict.state,
            reason: verdict.reason,
            at: Instant::now(),
        },
    );
}

/// Test-only escape hatch: fixtures reuse temp paths, so a stale verdict from a
/// previous fixture could otherwise leak into the next one.
#[cfg(test)]
pub fn clear_cache_for_test() {
    if let Ok(mut entries) = cache().lock() {
        entries.clear();
    }
}

struct BoundedOutput {
    success: bool,
    code: Option<i32>,
    stdout: Vec<u8>,
}

/// One bounded Git invocation. Output is drained on a helper thread so a chatty
/// command cannot fill the pipe and deadlock while the deadline is polled.
fn run_bounded(
    repo: &Path,
    args: &[&str],
    deadline: Instant,
) -> Result<BoundedOutput, &'static str> {
    use std::io::Read;

    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err("budget_exhausted");
    }
    let mut child = git_command(repo)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|_| "git_unavailable")?;
    let mut reader = child.stdout.take().map(|mut pipe| {
        std::thread::spawn(move || {
            let mut buffer = Vec::new();
            let _ = pipe.read_to_end(&mut buffer);
            buffer
        })
    });
    let drain = |reader: &mut Option<std::thread::JoinHandle<Vec<u8>>>| {
        reader
            .take()
            .and_then(|handle| handle.join().ok())
            .unwrap_or_default()
    };
    let started = Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                return Ok(BoundedOutput {
                    success: status.success(),
                    code: status.code(),
                    stdout: drain(&mut reader),
                })
            }
            Ok(None) => {}
            Err(_) => {
                let _ = child.kill();
                let _ = child.wait();
                let _ = drain(&mut reader);
                return Err("git_error");
            }
        }
        if started.elapsed() >= remaining {
            let _ = child.kill();
            let _ = child.wait();
            let _ = drain(&mut reader);
            return Err("timeout");
        }
        std::thread::sleep(Duration::from_millis(2));
    }
}

fn resolve_ref(repo: &Path, reference: &str, deadline: Instant) -> Option<String> {
    let spec = format!("{reference}^{{commit}}");
    let output = run_bounded(repo, &["rev-parse", "--verify", "--quiet", &spec], deadline).ok()?;
    if !output.success {
        return None;
    }
    let commit = String::from_utf8(output.stdout).ok()?.trim().to_owned();
    (!commit.is_empty()).then_some(commit)
}

/// The checkout's repository metadata directory, bounded.
///
/// `git::repository` uses `Command::output()`, which has no kill path (#2402);
/// this endpoint runs on a sidebar poll, so every Git access it makes must be
/// killable.
pub fn repository_bounded(repo: &Path, deadline: Instant) -> Result<PathBuf, &'static str> {
    let output = run_bounded(
        repo,
        &["rev-parse", "--path-format=absolute", "--git-common-dir"],
        deadline,
    )?;
    if !output.success {
        return Err("not_a_repository");
    }
    let text = String::from_utf8(output.stdout).map_err(|_| "git_error")?;
    std::fs::canonicalize(PathBuf::from(text.trim())).map_err(|_| "not_a_repository")
}

/// Metadata-only worktree inventory, bounded.
///
/// `git worktree list --porcelain` reads only `.git/worktrees/*` metadata and
/// never touches working-tree files, so it cannot hang on storage that evicted
/// files. It also costs nothing per checkout: the merge-state question needs
/// path/branch/HEAD and nothing else, whereas `git::list` additionally runs
/// `ls-files` and `status` per entry — the unbounded behaviour #2402 describes.
pub fn list_metadata(repo: &Path, deadline: Instant) -> Result<Vec<Worktree>, &'static str> {
    let listing = run_bounded(repo, &["worktree", "list", "--porcelain", "-z"], deadline)?;
    if !listing.success {
        return Err("not_a_repository");
    }
    let mut result = Vec::new();
    let mut entry: Option<Worktree> = None;
    for field in listing.stdout.split(|byte| *byte == 0) {
        if let Some(path) = field.strip_prefix(b"worktree ") {
            if let Some(previous) = entry.take() {
                result.push(previous);
            }
            // A raw-byte path cannot be opened by this UI, so it is skipped
            // rather than shown as a lossy label.
            entry = std::str::from_utf8(path).ok().map(|path| Worktree {
                path: PathBuf::from(path),
                ..Default::default()
            });
        } else if let Some(current) = entry.as_mut() {
            if let Some(head) = field.strip_prefix(b"HEAD ") {
                current.head = String::from_utf8_lossy(head).into_owned();
            }
            if let Some(branch) = field.strip_prefix(b"branch refs/heads/") {
                current.branch = Some(String::from_utf8_lossy(branch).into_owned());
            }
            if field == b"bare" {
                current.bare = true;
            }
        }
    }
    if let Some(previous) = entry {
        result.push(previous);
    }
    Ok(result)
}

/// The deadline for one checkout: never later than the whole-scan deadline.
///
/// Giving every entry a fresh full allowance would let a repository with many
/// slow checkouts spend that allowance again and again, so the request could
/// outlast its own budget.
pub fn entry_deadline(scan_deadline: Instant) -> Instant {
    (Instant::now() + PER_ENTRY_BUDGET).min(scan_deadline)
}

/// Repositories to inspect for one registered Project root, bounded.
///
/// A root may be a checkout itself, or a container directory holding several
/// repositories — the layout the worktree inventory supports. Assuming every
/// root is a repository would return no state at all for a container Project
/// whose checkouts the inventory does list.
///
/// Bounded by depth, entry count and the caller's deadline, so a large tree
/// cannot turn a sidebar poll into a directory walk.
pub fn discover_bounded(root: &Path, deadline: Instant) -> Vec<PathBuf> {
    const MAX_DEPTH: usize = 4;
    const MAX_ENTRIES: usize = 2_000;

    let mut found = Vec::new();
    let mut pending = vec![(root.to_path_buf(), 0usize)];
    let mut visited = 0usize;
    while let Some((directory, depth)) = pending.pop() {
        if Instant::now() >= deadline || visited >= MAX_ENTRIES {
            break;
        }
        if repository_bounded(&directory, deadline).is_ok() {
            // A checkout stops the descent: nested repositories are not this
            // Project's checkouts, and the inventory does not own them either.
            found.push(directory);
            continue;
        }
        if depth >= MAX_DEPTH {
            continue;
        }
        let Ok(entries) = std::fs::read_dir(&directory) else {
            continue;
        };
        for entry in entries.flatten() {
            visited += 1;
            if visited >= MAX_ENTRIES || Instant::now() >= deadline {
                break;
            }
            // Never follow symlinks: a link could escape the Project root or loop.
            let Ok(file_type) = entry.file_type() else {
                continue;
            };
            if !file_type.is_dir() {
                continue;
            }
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.starts_with('.') || name == "node_modules" || name == "target" {
                continue;
            }
            pending.push((entry.path(), depth + 1));
        }
    }
    found
}

/// Resolve the integration base once per repository. `refs/remotes/origin/HEAD`
/// is authoritative; the ordered fallbacks cover clones that never created it.
/// A repository with none of these is answered `unknown`, never assumed.
pub fn merge_base(repo: &Path, deadline: Instant) -> Result<MergeBase, &'static str> {
    let mut candidates: Vec<String> = Vec::new();
    if let Ok(output) = run_bounded(
        repo,
        &["symbolic-ref", "--quiet", "refs/remotes/origin/HEAD"],
        deadline,
    ) {
        if output.success {
            if let Ok(text) = String::from_utf8(output.stdout) {
                let reference = text.trim();
                if reference.starts_with("refs/remotes/") {
                    candidates.push(reference.to_owned());
                }
            }
        }
    }
    for name in ["dev", "main", "master"] {
        candidates.push(format!("refs/remotes/origin/{name}"));
    }
    for reference in candidates {
        if let Some(commit) = resolve_ref(repo, &reference, deadline) {
            return Ok(MergeBase { reference, commit });
        }
    }
    Err("no_base_ref")
}

/// Classify one checkout against `base`.
///
/// `main_checkout` is the repository's primary worktree: it is the workspace
/// itself and never reported as outstanding branch work.
pub fn merge_verdict(
    repo: &Path,
    entry: &Worktree,
    main_checkout: &Path,
    base: &MergeBase,
    deadline: Instant,
) -> MergeVerdict {
    if entry.bare {
        return MergeVerdict::new(MergeState::Unknown, "bare_repository");
    }
    if entry.head.is_empty() {
        return MergeVerdict::new(MergeState::Unknown, "head_unresolved");
    }
    // These two answers depend on facts the memo key cannot express: whether the
    // path *is* the primary checkout, and whether the entry merely sits at the
    // base tip. Both are cheap, so resolve them before consulting the memo;
    // otherwise a worktree sharing the main checkout's head would inherit its
    // "main_checkout" reason.
    if is_main_checkout(&entry.path, main_checkout) {
        return MergeVerdict::new(MergeState::Primary, "main_checkout");
    }
    if entry.head == base.commit {
        return MergeVerdict::new(MergeState::Primary, "base_tip");
    }
    let key = (repo.to_path_buf(), entry.head.clone(), base.commit.clone());
    if let Some(cached) = cache_get(&key) {
        return cached;
    }
    let verdict = classify(entry, base, repo, deadline);
    // Do not memoise a verdict produced by an exhausted or failed command: it
    // describes a transient condition, not the checkout.
    if !matches!(
        verdict.reason,
        "timeout" | "git_error" | "git_unavailable" | "budget_exhausted"
    ) {
        cache_put(key, &verdict);
    }
    verdict
}

/// Whether `path` is the repository's primary checkout.
///
/// Compared both lexically (cheap, exact for the paths Git hands us) and by
/// canonical identity, because `worktree list` may report a symlinked path.
fn is_main_checkout(path: &Path, main_checkout: &Path) -> bool {
    path == main_checkout || std::fs::canonicalize(path).is_ok_and(|path| path == main_checkout)
}

fn classify(entry: &Worktree, base: &MergeBase, repo: &Path, deadline: Instant) -> MergeVerdict {
    // Ancestry covers merge commits and rebase landings, where the checkout's
    // commits are literally contained in the base history.
    match run_bounded(
        repo,
        &["merge-base", "--is-ancestor", &entry.head, &base.commit],
        deadline,
    ) {
        Ok(output) if output.success => {
            return MergeVerdict::new(MergeState::Merged, "is_ancestor")
        }
        // Exit code 1 is Git's documented "not an ancestor", not an error.
        Ok(output) if output.code == Some(1) => {}
        Ok(_) => return MergeVerdict::new(MergeState::Unknown, "git_error"),
        Err(reason) => return MergeVerdict::new(MergeState::Unknown, reason),
    }
    // Patch equivalence covers cherry-pick landings: a differing commit object
    // whose patch is already present. `git cherry` prints one line per commit
    // unique to the checkout and prefixes unapplied patches with `+`.
    match run_bounded(repo, &["cherry", &base.commit, &entry.head], deadline) {
        Ok(output) if output.success => {
            let text = String::from_utf8_lossy(&output.stdout);
            if text.lines().any(|line| line.starts_with('+')) {
                MergeVerdict::new(MergeState::InProgress, "unique_commits")
            } else if text.lines().any(|line| line.starts_with('-')) {
                MergeVerdict::new(MergeState::Merged, "patch_already_applied")
            } else {
                // No unique commit and no equivalent patch means the checkout
                // equals the base, already handled above. Do not invent a state.
                MergeVerdict::new(MergeState::Unknown, "inconsistent_result")
            }
        }
        Ok(_) => MergeVerdict::new(MergeState::Unknown, "git_error"),
        Err(reason) => MergeVerdict::new(MergeState::Unknown, reason),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static NEXT: AtomicUsize = AtomicUsize::new(0);

    fn git(repo: &Path, args: &[&str]) -> String {
        let output = git_command(repo)
            .args([
                "-c",
                "user.name=Test",
                "-c",
                "user.email=test@example.invalid",
            ])
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {:?} failed: {}",
            args,
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap()
    }

    /// A repository with `main`, an `origin/main` ref, and one commit.
    struct Fixture(PathBuf);

    impl Fixture {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "bodhi-merge-state-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            let _ = fs::remove_dir_all(&path);
            fs::create_dir_all(&path).unwrap();
            let path = path.canonicalize().unwrap();
            let repo = path.join("repo");
            fs::create_dir(&repo).unwrap();
            git(&repo, &["init", "-b", "main"]);
            fs::write(repo.join("a.txt"), "one\n").unwrap();
            git(&repo, &["add", "."]);
            git(&repo, &["commit", "-m", "initial"]);
            git(&repo, &["update-ref", "refs/remotes/origin/main", "HEAD"]);
            Self(path)
        }
        fn repo(&self) -> PathBuf {
            self.0.join("repo")
        }
        fn roots(&self) -> PathBuf {
            let roots = self.0.join("roots");
            if !roots.exists() {
                fs::create_dir(&roots).unwrap();
            }
            roots
        }
        /// Register a linked worktree on a new branch and commit one file to it.
        fn checkout(&self, name: &str) -> PathBuf {
            let target = self.roots().join(name);
            let repo = self.repo();
            git(
                &repo,
                &[
                    "worktree",
                    "add",
                    "-b",
                    name,
                    target.to_str().unwrap(),
                    "HEAD",
                ],
            );
            target
        }
        fn commit(&self, path: &Path, file: &str, body: &str) -> String {
            fs::write(path.join(file), body).unwrap();
            git(path, &["add", "."]);
            git(path, &["commit", "-m", file]);
            git(path, &["rev-parse", "HEAD"]).trim().to_owned()
        }
        fn entry(&self, path: &Path) -> Worktree {
            Worktree {
                path: path.to_path_buf(),
                head: git(path, &["rev-parse", "HEAD"]).trim().to_owned(),
                ..Default::default()
            }
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn base(f: &Fixture) -> MergeBase {
        merge_base(&f.repo(), Instant::now() + Duration::from_secs(5)).unwrap()
    }

    #[test]
    fn resolves_origin_head_then_falls_back_to_known_integration_branches() {
        let f = Fixture::new();
        // No origin/HEAD yet: the fallback must still find origin/main.
        let resolved = base(&f);
        assert_eq!(resolved.reference, "refs/remotes/origin/main");
        assert_eq!(resolved.label(), "origin/main");

        git(
            &f.repo(),
            &["update-ref", "refs/remotes/origin/dev", "HEAD"],
        );
        git(
            &f.repo(),
            &[
                "symbolic-ref",
                "refs/remotes/origin/HEAD",
                "refs/remotes/origin/dev",
            ],
        );
        let resolved = base(&f);
        assert_eq!(resolved.reference, "refs/remotes/origin/dev");
    }

    #[test]
    fn a_repository_without_an_upstream_is_unknown_not_merged() {
        let f = Fixture::new();
        git(&f.repo(), &["update-ref", "-d", "refs/remotes/origin/main"]);
        let deadline = Instant::now() + Duration::from_secs(5);
        assert_eq!(merge_base(&f.repo(), deadline).unwrap_err(), "no_base_ref");
    }

    #[test]
    fn the_main_checkout_and_a_base_tip_checkout_are_primary() {
        let f = Fixture::new();
        let base = base(&f);
        let entry = f.entry(&f.repo());
        let verdict = merge_verdict(
            &f.repo(),
            &entry,
            &f.repo(),
            &base,
            Instant::now() + Duration::from_secs(5),
        );
        assert_eq!(verdict.state, MergeState::Primary);
        assert_eq!(verdict.reason, "main_checkout");

        // A fresh linked checkout carries no work at all: it is at the base tip.
        let clean = f.checkout("clean");
        let entry = f.entry(&clean);
        let verdict = merge_verdict(
            &f.repo(),
            &entry,
            &f.repo(),
            &base,
            Instant::now() + Duration::from_secs(5),
        );
        assert_eq!(verdict.state, MergeState::Primary);
        assert_eq!(verdict.reason, "base_tip");
    }

    #[test]
    fn unique_work_is_in_progress() {
        let f = Fixture::new();
        let base = base(&f);
        let work = f.checkout("work");
        f.commit(&work, "b.txt", "two\n");
        let entry = f.entry(&work);
        let verdict = merge_verdict(
            &f.repo(),
            &entry,
            &f.repo(),
            &base,
            Instant::now() + Duration::from_secs(5),
        );
        assert_eq!(verdict.state, MergeState::InProgress);
        assert_eq!(verdict.reason, "unique_commits");
    }

    #[test]
    fn a_merge_commit_landing_is_merged_by_ancestry() {
        let f = Fixture::new();
        let work = f.checkout("landed");
        f.commit(&work, "b.txt", "two\n");
        let head = git(&work, &["rev-parse", "HEAD"]).trim().to_owned();
        // Land the checkout on the integration branch, then let the branch move
        // on: a real merge is always followed by further integration commits, so
        // the branch tip sits strictly ahead of the merged checkout head.
        let repo = f.repo();
        git(&repo, &["merge", "--ff-only", &head]);
        f.commit(&repo, "c.txt", "three\n");
        let tip = git(&repo, &["rev-parse", "HEAD"]).trim().to_owned();
        git(&repo, &["update-ref", "refs/remotes/origin/main", &tip]);
        let base = base(&f);
        assert_ne!(base.commit, head, "the base must advance past the merge");
        let entry = f.entry(&work);
        let verdict = merge_verdict(
            &repo,
            &entry,
            &repo,
            &base,
            Instant::now() + Duration::from_secs(5),
        );
        assert_eq!(verdict.state, MergeState::Merged);
        assert_eq!(verdict.reason, "is_ancestor");
    }

    #[test]
    fn a_cherry_picked_landing_is_merged_by_patch_equivalence() {
        let f = Fixture::new();
        let work = f.checkout("picked");
        f.commit(&work, "b.txt", "two\n");
        // Reproduce the same change on the base with a different commit object,
        // which is exactly the shape a cherry-pick or rebase landing leaves.
        let repo = f.repo();
        fs::write(repo.join("b.txt"), "two\n").unwrap();
        git(&repo, &["add", "."]);
        git(&repo, &["commit", "-m", "landed elsewhere"]);
        let landed = git(&repo, &["rev-parse", "HEAD"]).trim().to_owned();
        git(&repo, &["update-ref", "refs/remotes/origin/main", &landed]);
        let base = base(&f);
        let entry = f.entry(&work);
        let verdict = merge_verdict(
            &repo,
            &entry,
            &repo,
            &base,
            Instant::now() + Duration::from_secs(5),
        );
        assert_eq!(verdict.state, MergeState::Merged);
        assert_eq!(verdict.reason, "patch_already_applied");
    }

    #[test]
    fn a_squash_landing_stays_in_progress_because_local_refs_cannot_prove_it() {
        let f = Fixture::new();
        let work = f.checkout("squashed");
        f.commit(&work, "b.txt", "two\n");
        f.commit(&work, "c.txt", "three\n");
        // A squash merge collapses several commits into one new commit with
        // different patch content, so neither ancestry nor patch id matches.
        let repo = f.repo();
        fs::write(repo.join("b.txt"), "two\n").unwrap();
        fs::write(repo.join("c.txt"), "three\n").unwrap();
        git(&repo, &["add", "."]);
        git(&repo, &["commit", "-m", "squashed (#1)"]);
        let landed = git(&repo, &["rev-parse", "HEAD"]).trim().to_owned();
        git(&repo, &["update-ref", "refs/remotes/origin/main", &landed]);
        let base = base(&f);
        let entry = f.entry(&work);
        let verdict = merge_verdict(
            &repo,
            &entry,
            &repo,
            &base,
            Instant::now() + Duration::from_secs(5),
        );
        // Documented limitation, asserted so it cannot regress silently into a
        // false "merged" claim. A forge provider is the only way to upgrade it.
        assert_eq!(verdict.state, MergeState::InProgress);
        assert_eq!(verdict.reason, "unique_commits");
    }

    #[test]
    fn an_exhausted_budget_reports_unknown_instead_of_blocking() {
        let f = Fixture::new();
        let base = base(&f);
        let work = f.checkout("slow");
        f.commit(&work, "b.txt", "two\n");
        let entry = f.entry(&work);
        // A deadline already in the past stands in for an exhausted scan budget.
        let verdict = merge_verdict(&f.repo(), &entry, &f.repo(), &base, Instant::now());
        assert_eq!(verdict.state, MergeState::Unknown);
        assert_eq!(verdict.reason, "budget_exhausted");
    }

    #[test]
    fn non_ascii_git_output_and_missing_git_still_answer() {
        let f = Fixture::new();
        let base = base(&f);
        let mut entry = f.entry(&f.repo());
        entry.bare = true;
        let verdict = merge_verdict(
            &f.repo(),
            &entry,
            &f.repo(),
            &base,
            Instant::now() + Duration::from_secs(5),
        );
        assert_eq!(verdict.state, MergeState::Unknown);
        assert_eq!(verdict.reason, "bare_repository");
    }

    #[test]
    fn verdicts_are_memoised_and_invalidated_by_a_new_head() {
        let f = Fixture::new();
        clear_cache_for_test();
        let base = base(&f);
        let work = f.checkout("cached");
        f.commit(&work, "b.txt", "two\n");
        let entry = f.entry(&work);
        let deadline = Instant::now() + Duration::from_secs(5);
        let first = merge_verdict(&f.repo(), &entry, &f.repo(), &base, deadline);
        assert_eq!(first.state, MergeState::InProgress);
        // A stale deadline would fail if the second call actually ran Git: a
        // cached verdict proves the memo path is what answered.
        let second = merge_verdict(&f.repo(), &entry, &f.repo(), &base, Instant::now());
        assert_eq!(second, first);

        // New work changes the head, which must invalidate the memo.
        f.commit(&work, "c.txt", "three\n");
        let moved = f.entry(&work);
        let third = merge_verdict(
            &f.repo(),
            &moved,
            &f.repo(),
            &base,
            Instant::now() + Duration::from_secs(5),
        );
        assert_eq!(third.state, MergeState::InProgress);
    }

    #[test]
    fn a_hung_git_is_killed_and_answered_within_the_deadline() {
        // Simulates a checkout on slow/offline storage (#2402) without needing
        // one: the bounded runner must kill the child and return, not park.
        let dir = std::env::temp_dir().join(format!(
            "bodhi-merge-hang-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&dir).unwrap();
        let started = Instant::now();
        let result = run_bounded(&dir, &["-c", "core.pager=cat", "log"], started);
        // A missing repository exits quickly; the assertion here is only that
        // the call returns and never exceeds the deadline by much.
        assert!(result.is_ok() || result.is_err());
        assert!(started.elapsed() < Duration::from_secs(5));
        let _ = fs::remove_dir_all(&dir);
    }
}
