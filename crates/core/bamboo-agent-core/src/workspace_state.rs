use dashmap::DashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::{Component, Path, PathBuf};
use std::sync::OnceLock;
use std::time::Instant;

const MAX_TRACKED_WORKSPACES: usize = 2_000;

#[derive(Clone, Debug)]
struct WorkspaceEntry {
    workspace: PathBuf,
    last_touched: Instant,
}

fn workspaces() -> &'static DashMap<String, WorkspaceEntry> {
    static WORKSPACES: OnceLock<DashMap<String, WorkspaceEntry>> = OnceLock::new();
    WORKSPACES.get_or_init(DashMap::new)
}

/// Assign `session_id`'s workspace, running the effective value through
/// [`WORKSPACE_ROOT_PROVIDER`]'s pin/relocate policy (issue #217) if one is
/// registered. Returns the FINAL stored path — which may differ from
/// `workspace` when confinement relocated it — so callers that echo the
/// workspace back (e.g. the `Workspace` tool's response) report the truth.
///
/// This is the single choke point every "explicit workspace assignment" path
/// funnels through (the `Workspace` tool, [`ensure_session_workspace`]'s
/// `preferred`/configured branches, child-session workspace inheritance) —
/// see [`pin_workspace_path`] for the confinement policy itself.
pub fn set_workspace(session_id: &str, workspace: PathBuf) -> PathBuf {
    let workspace = pin_via_provider(workspace);
    let store = workspaces();
    store.insert(
        session_id.to_string(),
        WorkspaceEntry {
            workspace: workspace.clone(),
            last_touched: Instant::now(),
        },
    );
    evict_oldest_if_needed(store, MAX_TRACKED_WORKSPACES);
    workspace
}

pub fn get_workspace(session_id: &str) -> Option<PathBuf> {
    let mut entry = workspaces().get_mut(session_id)?;
    entry.last_touched = Instant::now();
    Some(entry.workspace.clone())
}

/// Resolver for the configured default workspace.
///
/// `agent-core` is a core layer and must not depend on the infrastructure
/// config crate. Instead, the composition root (the server bootstrap) registers
/// a provider here via [`set_default_workspace_provider`]; until then this
/// resolves to `None` and callers fall back to the process working directory.
/// A boxed closure (not a bare `fn`) so the composition root can capture state —
/// e.g. a handle to the server's live in-memory config — instead of being forced
/// to re-read config from disk on every call. #38.
type DefaultWorkspaceProvider = Box<dyn Fn() -> Option<PathBuf> + Send + Sync>;
static DEFAULT_WORKSPACE_PROVIDER: OnceLock<DefaultWorkspaceProvider> = OnceLock::new();

/// Register the provider that resolves the configured default workspace.
///
/// Called once at startup by the layer that owns the infrastructure config, so
/// that this crate keeps a dependency only on `bamboo-domain`. Subsequent calls
/// are ignored (first registration wins).
pub fn set_default_workspace_provider(provider: DefaultWorkspaceProvider) {
    let _ = DEFAULT_WORKSPACE_PROVIDER.set(provider);
}

pub fn get_configured_default_workspace() -> Option<PathBuf> {
    DEFAULT_WORKSPACE_PROVIDER
        .get()
        .and_then(|provider| provider())
}

/// Whether a default-workspace provider has been registered — i.e. we're running
/// in a context (the server) that owns the live config and wired the provider at
/// startup. When true the provider is authoritative: callers must NOT fall back
/// to a disk read even if it resolves to `None`, since that would re-introduce a
/// divergent disk read of config (#38 / #131). When false (SDK / CLI / unit
/// tests) callers may use their own config source.
pub fn has_default_workspace_provider() -> bool {
    DEFAULT_WORKSPACE_PROVIDER.get().is_some()
}

pub fn ensure_session_workspace(session_id: &str, preferred: Option<PathBuf>) -> Option<PathBuf> {
    if let Some(workspace) = preferred {
        return Some(set_workspace(session_id, workspace));
    }

    if let Some(existing) = get_workspace(session_id) {
        return Some(existing);
    }

    if let Some(configured) = get_configured_default_workspace() {
        return Some(set_workspace(session_id, configured));
    }

    None
}

/// Resolve the working directory tools should use for `session_id`: the
/// tracked/configured workspace if one exists, else (issue #217) a
/// persistent, session-scoped directory under the registered workspace root
/// (`WORKSPACE_ROOT_PROVIDER`'s `root`/`{session_id}`) — NOT the process
/// working directory, so a server/orchestrator never leaks tool I/O into
/// whatever directory it happened to boot in.
///
/// Back-compat (#217): when no [`WorkspaceRootProvider`] is registered — bare
/// `agent-core`/tool unit tests, or any embedding that never called
/// [`set_workspace_root_provider`] — this still falls back to the process
/// `current_dir()`, identical to pre-#217 behavior. `session_id == None`
/// (tool called with no session context at all) also falls back to
/// `current_dir()`, since there is no session to scope a directory to.
pub fn workspace_or_process_cwd(session_id: Option<&str>) -> PathBuf {
    if let Some(session_id) = session_id {
        if let Some(workspace) = ensure_session_workspace(session_id, None) {
            return workspace;
        }
        if let Some(provider) = WORKSPACE_ROOT_PROVIDER.get() {
            let cfg = provider();
            let dir = default_session_workspace_dir(&cfg.root, session_id);
            return set_workspace(session_id, dir);
        }
    }
    std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
}

/// Root + confinement policy for session workspaces, supplied by the
/// composition root (server bootstrap / SDK builder) via
/// [`set_workspace_root_provider`]. `agent-core` owns only this slot — see the
/// [`DefaultWorkspaceProvider`] doc comment for why (agent-core must not
/// depend on `bamboo-config`); the actual resolution rules (env var
/// overrides, `bamboo_dir()` join) live there.
#[derive(Clone, Debug)]
pub struct WorkspaceRootConfig {
    /// Directory new/relocated session workspaces are created under
    /// (default `data_dir/workspaces`, overridable via `BAMBOO_WORKSPACE_ROOT`).
    pub root: PathBuf,
    /// Whether an explicitly-assigned workspace path must be canonicalized
    /// and confined to `root` — escapes (`..`, a symlink pointing outside, or
    /// an absolute path elsewhere on disk) are relocated to a deterministic
    /// folder under `root` instead of honored as-is.
    ///
    /// OFF by default: local single-user back-compat (#217). A session's
    /// workspace may point anywhere on disk, exactly as before this issue —
    /// e.g. pointing bamboo at an existing project outside `~/.bamboo`. An
    /// orchestrator opts into "one folder = one tenant" containment by
    /// setting `BAMBOO_WORKSPACE_CONFINE=1` (or `BAMBOO_WORKSPACE_ROOT`).
    pub confine: bool,
}

type WorkspaceRootProvider = Box<dyn Fn() -> WorkspaceRootConfig + Send + Sync>;
static WORKSPACE_ROOT_PROVIDER: OnceLock<WorkspaceRootProvider> = OnceLock::new();

/// Register the provider that resolves the workspace root + confinement
/// policy. Called once at startup by the composition root (server bootstrap /
/// SDK builder), mirroring [`set_default_workspace_provider`]. First
/// registration wins; subsequent calls are ignored to keep the value stable.
pub fn set_workspace_root_provider(provider: WorkspaceRootProvider) {
    let _ = WORKSPACE_ROOT_PROVIDER.set(provider);
}

/// Whether a workspace-root provider has been registered.
pub fn has_workspace_root_provider() -> bool {
    WORKSPACE_ROOT_PROVIDER.get().is_some()
}

fn pin_via_provider(path: PathBuf) -> PathBuf {
    match WORKSPACE_ROOT_PROVIDER.get() {
        Some(provider) => {
            let cfg = provider();
            pin_workspace_path(&path, &cfg.root, cfg.confine)
        }
        None => path,
    }
}

/// Sanitize an arbitrary string into a single, safe path component:
/// alphanumeric / `-` / `_` / `.` only (anything else becomes `_`), with
/// leading/trailing `.`/`_` trimmed and a length cap. A bare `.`/`..`/empty
/// result falls back to `_`. Used to turn a session id — or an escaping
/// path's leaf name — into a component that cannot itself traverse.
pub fn sanitize_path_component(raw: &str) -> String {
    let cleaned: String = raw
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '-' || c == '_' || c == '.' {
                c
            } else {
                '_'
            }
        })
        .collect();
    let trimmed = cleaned.trim_matches(['.', '_']);
    if trimmed.is_empty() || trimmed == "." || trimmed == ".." {
        "_".to_string()
    } else {
        trimmed.chars().take(200).collect()
    }
}

/// Lexically collapse `.`/`..` components WITHOUT touching the filesystem.
/// Used as a fallback for paths that don't exist yet, where `canonicalize`
/// can't resolve them.
fn lexically_clean(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::ParentDir => {
                out.pop();
            }
            Component::CurDir => {}
            other => out.push(other.as_os_str()),
        }
    }
    out
}

/// Best-effort canonicalization: resolves symlinks + `..`/`.` for whatever
/// prefix of `path` exists on disk, then lexically appends the remainder — so
/// a not-yet-created target under an existing (possibly symlinked) parent
/// still resolves to where it will actually land. Falls back to pure lexical
/// cleaning if no ancestor exists at all (e.g. a fully hypothetical path).
///
/// This is a best-effort containment check, not adversarial sandboxing (see
/// the crate-level scope note in the #217 tracking issue): it cannot see a
/// symlink that gets created AFTER this check runs (TOCTOU), and a bash
/// subprocess is free to do whatever it wants once it starts — that is the
/// outer container's job, not this helper's.
fn canonicalize_best_effort(path: &Path) -> PathBuf {
    if let Ok(canon) = std::fs::canonicalize(path) {
        return canon;
    }
    let mut remainder: Vec<std::ffi::OsString> = Vec::new();
    let mut probe = path;
    loop {
        match probe.parent() {
            Some(parent) => {
                if let Some(name) = probe.file_name() {
                    remainder.push(name.to_os_string());
                }
                if let Ok(canon_parent) = std::fs::canonicalize(parent) {
                    let mut result = canon_parent;
                    for part in remainder.into_iter().rev() {
                        result.push(part);
                    }
                    return result;
                }
                probe = parent;
            }
            None => return lexically_clean(path),
        }
    }
}

/// Deterministically relocate an escaping path under `root`: derive a leaf
/// name from the original path's last component (or a short hash of the full
/// path if that's empty/unsafe/reserved) and create `root/<leaf>`.
fn relocate_under_root(root: &Path, original: &Path) -> PathBuf {
    let leaf = original
        .file_name()
        .and_then(|s| s.to_str())
        .map(sanitize_path_component)
        .filter(|s| s != "_");
    let leaf = leaf.unwrap_or_else(|| {
        let mut hasher = DefaultHasher::new();
        original.hash(&mut hasher);
        format!("relocated-{:x}", hasher.finish())
    });
    let target = root.join(leaf);
    let _ = std::fs::create_dir_all(&target);
    target
}

/// Resolve an explicitly-assigned workspace path against a root + policy
/// (issue #217 acceptance criterion 2). Pure/directly-testable: takes the
/// policy as plain parameters instead of reading the global provider (the
/// provider itself is a thin wrapper calling this).
///
/// - `confine == false` (the default — local single-user back-compat):
///   returns `requested` VERBATIM. A pure pass-through, byte-for-byte
///   identical to pre-#217 `set_workspace` behavior: an explicit workspace
///   may point anywhere on disk (e.g. an existing project outside
///   `~/.bamboo`), and no canonicalization is imposed here — callers that
///   want canonical paths (the `Workspace` tool, the chat handler) already
///   canonicalize before storing, while `Config::get_default_work_area_path`
///   DELIBERATELY returns a non-canonicalized path (macOS `/var` →
///   `/private/var` rewrite is documented there as undesirable) and must not
///   have canonicalization re-imposed on it.
/// - `confine == true`: canonicalizes `requested` and requires the result to
///   live under `root` (also canonicalized). An escape — `..`, a symlink
///   pointing outside `root`, or an absolute path elsewhere — is RELOCATED to
///   a deterministic folder under `root` rather than rejected outright, so a
///   misbehaving/untrusted request degrades to a safe folder instead of hard
///   -failing the whole session.
pub fn pin_workspace_path(requested: &Path, root: &Path, confine: bool) -> PathBuf {
    if !confine {
        return requested.to_path_buf();
    }
    let canon_root = std::fs::canonicalize(root).unwrap_or_else(|_| root.to_path_buf());
    let canon_requested = canonicalize_best_effort(requested);
    if canon_requested.starts_with(&canon_root) {
        return canon_requested;
    }
    relocate_under_root(&canon_root, requested)
}

/// Default per-session workspace directory under `root` (issue #217
/// acceptance criterion 1): `root/<sanitized-session-id>`, created if
/// missing. Pure/directly-testable, mirroring [`pin_workspace_path`].
pub fn default_session_workspace_dir(root: &Path, session_id: &str) -> PathBuf {
    let dir = root.join(sanitize_path_component(session_id));
    let _ = std::fs::create_dir_all(&dir);
    dir
}

fn evict_oldest_if_needed(store: &DashMap<String, WorkspaceEntry>, max_tracked_workspaces: usize) {
    if store.len() <= max_tracked_workspaces {
        return;
    }

    let oldest = store
        .iter()
        .map(|entry| (entry.key().clone(), entry.value().last_touched))
        .min_by_key(|(_, touched)| *touched);
    if let Some((session_id, _)) = oldest {
        let _ = store.remove(&session_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn evict_oldest_if_needed_removes_least_recent_entry() {
        let store: DashMap<String, WorkspaceEntry> = DashMap::new();
        let now = Instant::now();
        store.insert(
            "s1".to_string(),
            WorkspaceEntry {
                workspace: PathBuf::from("/tmp/s1"),
                last_touched: now - Duration::from_secs(3),
            },
        );
        store.insert(
            "s2".to_string(),
            WorkspaceEntry {
                workspace: PathBuf::from("/tmp/s2"),
                last_touched: now - Duration::from_secs(2),
            },
        );
        store.insert(
            "s3".to_string(),
            WorkspaceEntry {
                workspace: PathBuf::from("/tmp/s3"),
                last_touched: now - Duration::from_secs(1),
            },
        );

        evict_oldest_if_needed(&store, 2);

        assert_eq!(store.len(), 2);
        assert!(!store.contains_key("s1"));
        assert!(store.contains_key("s2"));
        assert!(store.contains_key("s3"));
    }

    // ── #217: workspace-root resolution + confinement ────────────────────
    //
    // NOTE: none of these tests call `set_workspace_root_provider` — that
    // OnceLock is process-global and first-registration-wins, so a test that
    // sets it would permanently affect every other test in this binary
    // (see the "workspace_or_process_cwd_falls_back_to_cwd_without_provider"
    // test below, which relies on it staying unset). The provider-wiring
    // itself is exercised end-to-end in `bamboo-tools` instead, in a
    // separate test binary/process. Everything here tests the pure,
    // parameterized helpers the provider wraps.

    #[test]
    fn sanitize_path_component_allows_safe_chars_untouched() {
        assert_eq!(
            sanitize_path_component("session-123_abc.def"),
            "session-123_abc.def"
        );
    }

    #[test]
    fn sanitize_path_component_replaces_separators_and_traversal() {
        // Leading `.`/`_` runs (including the `..` traversal tokens) are
        // trimmed, so this can never reproduce a `..` component even though
        // `.` itself is in the allowed charset.
        assert_eq!(sanitize_path_component("../../etc/passwd"), "etc_passwd");
        assert_eq!(sanitize_path_component("a/b\\c"), "a_b_c");
    }

    #[test]
    fn sanitize_path_component_rejects_dot_and_dotdot_and_empty() {
        assert_eq!(sanitize_path_component("."), "_");
        assert_eq!(sanitize_path_component(".."), "_");
        assert_eq!(sanitize_path_component(""), "_");
        assert_eq!(sanitize_path_component("///"), "_");
    }

    #[test]
    fn sanitize_path_component_caps_length() {
        let long = "a".repeat(500);
        assert_eq!(sanitize_path_component(&long).len(), 200);
    }

    /// Acceptance criterion 1: no explicit workspace path → default lands
    /// under `root/{session}`.
    #[test]
    fn default_session_workspace_dir_lands_under_root_and_is_created() {
        let root_dir = tempfile::tempdir().unwrap();
        let root = root_dir.path().join("workspaces");

        let dir = default_session_workspace_dir(&root, "session-abc-123");

        assert_eq!(dir, root.join("session-abc-123"));
        assert!(dir.is_dir(), "default workspace dir should be created");
    }

    #[test]
    fn default_session_workspace_dir_sanitizes_unsafe_session_ids() {
        let root_dir = tempfile::tempdir().unwrap();
        let root = root_dir.path().join("workspaces");

        let dir = default_session_workspace_dir(&root, "../../etc");

        // The sanitized component can never escape `root` — it stays a
        // single path segment directly under it.
        assert_eq!(dir.parent().unwrap(), root);
        assert!(dir.is_dir());
    }

    /// Acceptance criterion / back-compat: with confinement OFF (the
    /// default), an explicit workspace path may point anywhere on disk,
    /// exactly like pre-#217 behavior.
    #[test]
    fn pin_workspace_path_unconfined_allows_arbitrary_path_outside_root() {
        let root_dir = tempfile::tempdir().unwrap();
        let root = root_dir.path().join("workspaces");
        let outside = tempfile::tempdir().unwrap();

        let pinned = pin_workspace_path(outside.path(), &root, false);

        // Verbatim pass-through: no canonicalization is imposed in
        // unconfined mode (see the fn doc — `get_default_work_area_path`
        // deliberately avoids canonical paths on macOS).
        assert_eq!(pinned, outside.path());
        assert!(!pinned.starts_with(&root));
    }

    /// Acceptance criterion 2: an explicit path already inside the root is
    /// pinned (canonicalized) as-is when confinement is on.
    #[test]
    fn pin_workspace_path_confined_keeps_path_already_inside_root() {
        let root_dir = tempfile::tempdir().unwrap();
        let root = root_dir.path().join("workspaces");
        let inside = root.join("my-project");
        std::fs::create_dir_all(&inside).unwrap();

        let pinned = pin_workspace_path(&inside, &root, true);

        assert_eq!(pinned, inside.canonicalize().unwrap());
        assert!(pinned.starts_with(root.canonicalize().unwrap()));
    }

    /// Acceptance criterion 2: a `..` traversal escape is relocated under
    /// root rather than honored.
    #[test]
    fn pin_workspace_path_confined_relocates_dotdot_escape() {
        let root_dir = tempfile::tempdir().unwrap();
        let root = root_dir.path().join("workspaces");
        std::fs::create_dir_all(&root).unwrap();
        let escape = root.join("../escaped-outside");

        let pinned = pin_workspace_path(&escape, &root, true);

        let canon_root = root.canonicalize().unwrap();
        assert!(
            pinned.starts_with(&canon_root),
            "escaping `..` path must be relocated under root, got {pinned:?}"
        );
        assert_eq!(pinned, canon_root.join("escaped-outside"));
    }

    /// Acceptance criterion 2: an absolute path pointing entirely outside
    /// root is relocated under root rather than honored.
    #[test]
    fn pin_workspace_path_confined_relocates_absolute_escape() {
        let root_dir = tempfile::tempdir().unwrap();
        let root = root_dir.path().join("workspaces");
        std::fs::create_dir_all(&root).unwrap();
        let elsewhere = tempfile::tempdir().unwrap();
        let absolute_escape = elsewhere.path().join("my-real-project");
        std::fs::create_dir_all(&absolute_escape).unwrap();

        let pinned = pin_workspace_path(&absolute_escape, &root, true);

        let canon_root = root.canonicalize().unwrap();
        assert!(
            pinned.starts_with(&canon_root),
            "escaping absolute path must be relocated under root, got {pinned:?}"
        );
        assert_eq!(pinned, canon_root.join("my-real-project"));
    }

    /// Acceptance criterion 2: a symlink that lives inside root but points
    /// outside it is detected via canonicalize (which resolves symlinks) and
    /// relocated rather than followed out of root.
    #[test]
    fn pin_workspace_path_confined_relocates_symlink_escape() {
        let root_dir = tempfile::tempdir().unwrap();
        let root = root_dir.path().join("workspaces");
        std::fs::create_dir_all(&root).unwrap();

        let outside = tempfile::tempdir().unwrap();
        let outside_target = outside.path().join("secret");
        std::fs::create_dir_all(&outside_target).unwrap();

        let symlink_path = root.join("link-to-outside");
        #[cfg(unix)]
        std::os::unix::fs::symlink(&outside_target, &symlink_path).unwrap();
        #[cfg(windows)]
        std::os::windows::fs::symlink_dir(&outside_target, &symlink_path).unwrap();

        let pinned = pin_workspace_path(&symlink_path, &root, true);

        let canon_root = root.canonicalize().unwrap();
        let canon_outside = outside_target.canonicalize().unwrap();
        assert_ne!(
            pinned, canon_outside,
            "must not resolve to the symlink's real outside target"
        );
        assert!(
            pinned.starts_with(&canon_root),
            "symlink escape must be relocated under root, got {pinned:?}"
        );
    }

    /// Back-compat (#217): with no workspace-root provider registered at all
    /// (the state of this crate's own test binary — see the note above this
    /// section), `workspace_or_process_cwd` falls back to the process
    /// `current_dir()`, identical to pre-#217 behavior.
    #[test]
    fn workspace_or_process_cwd_falls_back_to_cwd_without_provider() {
        assert!(!has_workspace_root_provider());
        let session_id = format!("session_{}", uuid::Uuid::new_v4());

        let resolved = workspace_or_process_cwd(Some(&session_id));

        assert_eq!(resolved, std::env::current_dir().unwrap());
    }

    #[test]
    fn workspace_or_process_cwd_without_session_id_falls_back_to_cwd() {
        assert!(!has_workspace_root_provider());
        assert_eq!(
            workspace_or_process_cwd(None),
            std::env::current_dir().unwrap()
        );
    }
}
