use dashmap::DashMap;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, OnceLock};
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
    let workspace = resolve_workspace_path(workspace);
    publish_resolved_workspace(session_id, workspace)
}

/// Publish a candidate that already passed pure confinement and Project owner
/// validation. The exact supplied path is stored; confinement is not evaluated
/// a second time, closing the preview/check/publish TOCTOU.
pub fn publish_resolved_workspace(session_id: &str, workspace: PathBuf) -> PathBuf {
    publish_resolved_workspace_with_root(
        session_id,
        workspace,
        || WORKSPACE_ROOT_PROVIDER.get().map(|provider| provider()),
        "process_global",
    )
}

fn publish_resolved_workspace_with_root<R>(
    session_id: &str,
    workspace: PathBuf,
    root_config: R,
    source: &str,
) -> PathBuf
where
    R: FnOnce() -> Option<WorkspaceRootConfig>,
{
    if !workspace.exists() {
        if let Some(config) = root_config() {
            if let Err(error) = materialize_workspace_under_root(&workspace, &config.root) {
                tracing::warn!(
                    path = %workspace.display(),
                    workspace_root = %config.root.display(),
                    workspace_source = source,
                    %error,
                    "failed to materialize validated workspace"
                );
            }
        }
    }
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

fn materialize_workspace_under_root(workspace: &Path, root: &Path) -> std::io::Result<PathBuf> {
    let root = canonicalize_best_effort(root);
    let candidate = canonicalize_best_effort(workspace);
    if !candidate.starts_with(&root) {
        // A configured default may disappear after preview. Never recreate a
        // missing arbitrary path outside the authoritative root.
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "validated workspace is outside the authoritative workspace root",
        ));
    }
    std::fs::create_dir_all(&candidate)?;
    Ok(candidate)
}

/// Resolve the final path that [`set_workspace`] would store without mutating
/// session state. Project-aware callers use this to run cross-Project binding
/// conflict checks against the confinement-adjusted destination before the
/// workspace change becomes visible.
pub fn resolve_workspace_path(workspace: PathBuf) -> PathBuf {
    pin_via_provider(workspace)
}

/// Preview the confinement-adjusted workspace without creating the workspace
/// root or a relocated target. Validation and read-only prompt paths must use
/// this; [`set_workspace`] materializes only after authorization succeeds.
pub fn preview_workspace_path(workspace: PathBuf) -> PathBuf {
    match WORKSPACE_ROOT_PROVIDER.get() {
        Some(provider) => {
            let cfg = provider();
            preview_pin_workspace_path(&workspace, &cfg.root, cfg.confine)
        }
        None => workspace,
    }
}

pub fn get_workspace(session_id: &str) -> Option<PathBuf> {
    let mut entry = workspaces().get_mut(session_id)?;
    entry.last_touched = Instant::now();
    Some(entry.workspace.clone())
}

/// Read the tracked workspace without creating/touching any workspace state.
///
/// Project-aware validation paths use this before deciding whether a
/// candidate is allowed. Calling `ensure_session_workspace` for inspection is
/// unsafe because it can publish an unvalidated configured default.
pub fn peek_workspace(session_id: &str) -> Option<PathBuf> {
    workspaces()
        .get(session_id)
        .map(|entry| entry.workspace.clone())
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

/// Resolve the workspace a session would use without publishing it.
///
/// Precedence is explicit/persisted caller input, existing runtime state,
/// configured default, then the confinement provider's session-scoped
/// fallback. The returned path has already passed through confinement, but no
/// registry entry or directory is created.
pub fn resolve_session_workspace_candidate(
    session_id: &str,
    preferred: Option<PathBuf>,
) -> Option<PathBuf> {
    preferred
        .or_else(|| peek_workspace(session_id))
        .or_else(get_configured_default_workspace)
        .or_else(|| {
            WORKSPACE_ROOT_PROVIDER.get().map(|provider| {
                let cfg = provider();
                preview_default_session_workspace_dir(&cfg.root, session_id)
            })
        })
        .map(preview_workspace_path)
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

type SharedDefaultWorkspaceProvider = Arc<dyn Fn() -> Option<PathBuf> + Send + Sync>;
type SharedWorkspaceRootProvider = Arc<dyn Fn() -> Option<WorkspaceRootConfig> + Send + Sync>;

/// One coherent default/root provider pair.
///
/// The process-wide registration APIs remain first-wins for production and
/// embedding compatibility. A server `AppState` also retains an instance of
/// this resolver so multiple states in one test process cannot mix the first
/// state's live config with another state's session persistence.
#[derive(Clone)]
pub struct WorkspaceResolver {
    default_provider: SharedDefaultWorkspaceProvider,
    root_provider: SharedWorkspaceRootProvider,
}

impl WorkspaceResolver {
    /// Build an instance-scoped resolver from live providers.
    pub fn new<D, R>(default_provider: D, root_provider: R) -> Self
    where
        D: Fn() -> Option<PathBuf> + Send + Sync + 'static,
        R: Fn() -> WorkspaceRootConfig + Send + Sync + 'static,
    {
        Self {
            default_provider: Arc::new(default_provider),
            root_provider: Arc::new(move || Some(root_provider())),
        }
    }

    /// Dynamically delegate to the process-global first-wins providers.
    ///
    /// Registration state is intentionally read on every call, not captured
    /// here, so a resolver constructed before server bootstrap retains the
    /// historical no-provider/provider transition semantics.
    pub fn from_process_globals() -> Self {
        Self {
            default_provider: Arc::new(get_configured_default_workspace),
            root_provider: Arc::new(|| WORKSPACE_ROOT_PROVIDER.get().map(|provider| provider())),
        }
    }

    /// Return the current instance root and confinement policy.
    pub fn workspace_root_config(&self) -> Option<WorkspaceRootConfig> {
        (self.root_provider)()
    }

    /// Resolve a session workspace through this provider pair without
    /// publishing state or creating directories.
    pub fn resolve_session_workspace_candidate(
        &self,
        session_id: &str,
        preferred: Option<PathBuf>,
    ) -> Option<PathBuf> {
        preferred
            .or_else(|| peek_workspace(session_id))
            .or_else(|| (self.default_provider)())
            .or_else(|| {
                self.workspace_root_config()
                    .map(|config| preview_default_session_workspace_dir(&config.root, session_id))
            })
            .map(|path| self.preview_workspace_path(path))
    }

    /// Apply this instance's confinement policy without filesystem mutation.
    pub fn preview_workspace_path(&self, workspace: PathBuf) -> PathBuf {
        match self.workspace_root_config() {
            Some(config) => preview_pin_workspace_path(&workspace, &config.root, config.confine),
            None => workspace,
        }
    }

    /// Preview this resolver's session-scoped root fallback only.
    ///
    /// Unlike [`Self::resolve_session_workspace_candidate`], this deliberately
    /// does not consult the process-global runtime registry or a configured
    /// default. Server transaction paths use it when their own live config
    /// snapshot has authoritatively resolved no default workspace.
    pub fn preview_session_fallback(&self, session_id: &str) -> Option<PathBuf> {
        let config = self.workspace_root_config()?;
        let fallback = preview_default_session_workspace_dir(&config.root, session_id);
        Some(preview_pin_workspace_path(
            &fallback,
            &config.root,
            config.confine,
        ))
    }

    /// Materialize a trusted, already-previewed workspace without publishing
    /// it into the runtime session registry.
    ///
    /// Transactional request paths use this before reading a workspace-scoped
    /// catalog. Publication remains deferred until the corresponding session
    /// checkpoint is durable. Missing paths outside this resolver's
    /// authoritative root are rejected rather than recreated.
    pub fn materialize_resolved_workspace(&self, workspace: &Path) -> std::io::Result<PathBuf> {
        if workspace.exists() {
            return Ok(workspace.to_path_buf());
        }
        let config = self.workspace_root_config().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "workspace root is unavailable for materialization",
            )
        })?;
        materialize_workspace_under_root(workspace, &config.root)
    }

    /// Publish a previously validated candidate through this instance's root.
    ///
    /// Missing paths are materialized only when they are contained by this
    /// resolver's root. `source` is a non-secret diagnostic label supplied by
    /// the caller (for example `session_fallback`).
    pub fn publish_resolved_workspace(
        &self,
        session_id: &str,
        workspace: PathBuf,
        source: &str,
    ) -> PathBuf {
        publish_resolved_workspace_with_root(
            session_id,
            workspace,
            || self.workspace_root_config(),
            source,
        )
    }
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
                if let Some(component) = probe.components().next_back() {
                    match component {
                        Component::Normal(_) | Component::ParentDir | Component::CurDir => {
                            remainder.push(component.as_os_str().to_os_string());
                        }
                        Component::Prefix(_) | Component::RootDir => {}
                    }
                }
                if let Ok(canon_parent) = std::fs::canonicalize(parent) {
                    let mut result = canon_parent;
                    for part in remainder.into_iter().rev() {
                        result.push(part);
                    }
                    return lexically_clean(&result);
                }
                probe = parent;
            }
            None => return lexically_clean(path),
        }
    }
}

/// 64-bit FNV-1a over raw bytes: a tiny, fully-specified, version-stable hash
/// for deriving relocation directory names. Deliberately NOT `DefaultHasher`
/// (SipHash with an unspecified, rustc-version-dependent algorithm) — the
/// relocated directory name must stay identical across binary upgrades, or a
/// confined tenant's relocated workspace silently changes paths after a
/// rebuild. Not cryptographic; collision resistance here only needs to
/// separate distinct real-world workspace paths.
fn fnv1a_64(bytes: &[u8]) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = FNV_OFFSET;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

/// Deterministically relocate an escaping path under `root`.
///
/// The target MUST incorporate the full original path, not just its leaf —
/// two different escaping paths that happen to share a basename (e.g.
/// `/mnt/customer-a/project` and `/mnt/customer-b/project`) must land in
/// DISTINCT directories, or tenant isolation (the entire point of
/// confinement) collapses into cross-tenant data mixing. So the target is
/// always `root/<sanitized-leaf>-<8-hex-hash-of-full-path>` (or a pure
/// `root/relocated-<hash>` when the leaf is empty/unsafe/reserved) — a short
/// deterministic hash of `original` suffixed onto a human-readable leaf,
/// stable across calls/restarts AND binary upgrades since it's a pure,
/// fixed-algorithm (FNV-1a, not std's version-unspecified `DefaultHasher`)
/// function of the input path — a relocated tenant's directory must not
/// silently move when bamboo is rebuilt with a newer toolchain.
fn relocate_under_root(root: &Path, original: &Path) -> PathBuf {
    let target = preview_relocate_under_root(root, original);
    if let Err(err) = std::fs::create_dir_all(&target) {
        tracing::warn!(
            path = %target.display(),
            error = %err,
            "relocate_under_root: failed to create relocated workspace directory"
        );
    }
    target
}

fn preview_relocate_under_root(root: &Path, original: &Path) -> PathBuf {
    let hash = fnv1a_64(original.as_os_str().as_encoded_bytes());
    // Truncate to exactly 8 hex chars (32 bits) for the leaf-suffixed form —
    // short enough to stay a readable path component while still being
    // vanishingly unlikely to collide for distinct real-world paths.
    let short_hash = hash as u32;

    let leaf = original
        .file_name()
        .and_then(|s| s.to_str())
        .map(sanitize_path_component)
        .filter(|s| s != "_");
    let dir_name = match leaf {
        Some(leaf) => format!("{leaf}-{short_hash:08x}"),
        None => format!("relocated-{hash:x}"),
    };
    root.join(dir_name)
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
    // Best-effort: make sure `root` exists before canonicalizing it, so a
    // root that sits behind a symlink but hasn't been created yet doesn't
    // silently fall back to its non-canonical form (which would spuriously
    // fail the `starts_with` containment check below for every request).
    if let Err(err) = std::fs::create_dir_all(root) {
        tracing::warn!(
            path = %root.display(),
            error = %err,
            "pin_workspace_path: failed to create workspace root directory"
        );
    }
    let canon_root = std::fs::canonicalize(root).unwrap_or_else(|_| root.to_path_buf());
    let canon_requested = canonicalize_best_effort(requested);
    if canon_requested.starts_with(&canon_root) {
        return canon_requested;
    }
    relocate_under_root(&canon_root, requested)
}

/// Pure counterpart to [`pin_workspace_path`]. It derives the identical final
/// path while leaving both `root` and any relocation target absent.
pub fn preview_pin_workspace_path(requested: &Path, root: &Path, confine: bool) -> PathBuf {
    if !confine {
        return requested.to_path_buf();
    }
    let canon_root = canonicalize_best_effort(root);
    let canon_requested = canonicalize_best_effort(requested);
    if canon_requested.starts_with(&canon_root) {
        return canon_requested;
    }
    preview_relocate_under_root(&canon_root, requested)
}

/// Default per-session workspace directory under `root` (issue #217
/// acceptance criterion 1): `root/<sanitized-session-id>`, created if
/// missing. Pure/directly-testable, mirroring [`pin_workspace_path`].
pub fn default_session_workspace_dir(root: &Path, session_id: &str) -> PathBuf {
    let dir = preview_default_session_workspace_dir(root, session_id);
    if let Err(err) = std::fs::create_dir_all(&dir) {
        tracing::warn!(
            path = %dir.display(),
            error = %err,
            "default_session_workspace_dir: failed to create session workspace directory"
        );
    }
    dir
}

/// Pure path derivation for a session fallback workspace.
pub fn preview_default_session_workspace_dir(root: &Path, session_id: &str) -> PathBuf {
    root.join(sanitize_path_component(session_id))
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

    #[test]
    fn preview_fallback_and_confinement_relocation_do_not_create_directories() {
        let root_dir = tempfile::tempdir().unwrap();
        let root = root_dir.path().join("workspaces");
        let fallback = preview_default_session_workspace_dir(&root, "preview-session");
        assert_eq!(fallback, root.join("preview-session"));
        assert!(!root.exists());
        assert!(!fallback.exists());

        let outside = root_dir.path().join("outside/project");
        std::fs::create_dir_all(&outside).unwrap();
        let relocated = preview_pin_workspace_path(&outside, &root, true);
        assert!(relocated.starts_with(canonicalize_best_effort(&root)));
        assert!(!root.exists());
        assert!(!relocated.exists());
    }

    #[test]
    fn existing_workspace_publication_does_not_resolve_root_provider() {
        let workspace = tempfile::tempdir().unwrap();
        let root_provider_calls = std::cell::Cell::new(0);

        let published = publish_resolved_workspace_with_root(
            "existing-workspace-lazy-root",
            workspace.path().to_path_buf(),
            || {
                root_provider_calls.set(root_provider_calls.get() + 1);
                None
            },
            "test",
        );

        assert_eq!(published, workspace.path());
        assert_eq!(
            root_provider_calls.get(),
            0,
            "publishing an existing directory must not evaluate the root provider"
        );
    }

    #[test]
    fn instance_session_fallback_ignores_same_id_runtime_registry_state() {
        let state_dir = tempfile::tempdir().unwrap();
        let root = state_dir.path().join("workspaces");
        let foreign_workspace = tempfile::tempdir().unwrap();
        let session_id = "instance-fallback-ignores-runtime";
        publish_resolved_workspace(session_id, foreign_workspace.path().to_path_buf());
        let resolver = WorkspaceResolver::new(|| None, {
            let root = root.clone();
            move || WorkspaceRootConfig {
                root: root.clone(),
                confine: false,
            }
        });

        let fallback = resolver
            .preview_session_fallback(session_id)
            .expect("instance session fallback");

        assert_eq!(fallback, root.join(session_id));
        assert_ne!(fallback, foreign_workspace.path());
    }

    #[test]
    fn instance_resolver_materializes_its_own_fallback_and_confines_preferred_paths() {
        let state_dir = tempfile::tempdir().unwrap();
        let root = state_dir.path().join("workspaces");
        let resolver = WorkspaceResolver::new(|| None, {
            let root = root.clone();
            move || WorkspaceRootConfig {
                root: root.clone(),
                confine: true,
            }
        });

        let fallback = resolver
            .resolve_session_workspace_candidate("instance-fallback", None)
            .expect("instance fallback");
        assert_eq!(
            fallback,
            canonicalize_best_effort(&root).join("instance-fallback")
        );
        assert!(!fallback.exists(), "preview must remain side-effect free");
        let published = resolver.publish_resolved_workspace(
            "instance-fallback",
            fallback.clone(),
            "session_fallback",
        );
        assert_eq!(published, fallback);
        assert!(published.is_dir());

        let outside = state_dir.path().join("outside/project");
        std::fs::create_dir_all(&outside).unwrap();
        let confined = resolver
            .resolve_session_workspace_candidate("instance-confined", Some(outside.clone()))
            .expect("confined candidate");
        assert_ne!(confined, outside);
        assert!(confined.starts_with(canonicalize_best_effort(&root)));
        assert!(!confined.exists(), "confinement preview must not create");
        resolver.publish_resolved_workspace("instance-confined", confined.clone(), "request");
        assert!(confined.is_dir());
    }

    #[test]
    fn instance_resolver_can_materialize_without_publishing_runtime_state() {
        let state_dir = tempfile::tempdir().unwrap();
        let root = state_dir.path().join("workspaces");
        let resolver = WorkspaceResolver::new(|| None, {
            let root = root.clone();
            move || WorkspaceRootConfig {
                root: root.clone(),
                confine: false,
            }
        });
        let session_id = "materialize-without-publication";
        let fallback = resolver
            .preview_session_fallback(session_id)
            .expect("instance fallback");

        let materialized = resolver
            .materialize_resolved_workspace(&fallback)
            .expect("materialize trusted fallback");

        assert_eq!(materialized, canonicalize_best_effort(&fallback));
        assert!(materialized.is_dir());
        assert!(
            peek_workspace(session_id).is_none(),
            "catalog preparation must not publish a pre-commit workspace"
        );

        let outside = state_dir.path().join("outside/missing");
        let error = resolver
            .materialize_resolved_workspace(&outside)
            .expect_err("missing path outside the authority root must fail closed");
        assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
        assert!(!outside.exists());
    }

    #[test]
    fn instance_resolver_does_not_recreate_vanished_default_outside_its_root() {
        let state_dir = tempfile::tempdir().unwrap();
        let root = state_dir.path().join("workspaces");
        let vanished_default = state_dir.path().join("foreign/vanished-default");
        let resolver = WorkspaceResolver::new(
            {
                let vanished_default = vanished_default.clone();
                move || Some(vanished_default.clone())
            },
            {
                let root = root.clone();
                move || WorkspaceRootConfig {
                    root: root.clone(),
                    confine: false,
                }
            },
        );

        let candidate = resolver
            .resolve_session_workspace_candidate("vanished-default", None)
            .expect("configured default candidate");
        assert_eq!(candidate, vanished_default);
        assert!(!candidate.exists());
        let published = resolver.publish_resolved_workspace(
            "vanished-default",
            candidate.clone(),
            "configured_default",
        );

        assert_eq!(published, candidate);
        assert!(
            !candidate.exists(),
            "a missing configured default outside the instance root must not be recreated"
        );
        assert_eq!(
            get_workspace("vanished-default").as_deref(),
            Some(candidate.as_path())
        );
    }

    #[test]
    fn confinement_preview_matches_materialized_result_on_stable_filesystem() {
        let root_dir = tempfile::tempdir().unwrap();
        let root = root_dir.path().join("workspaces");
        let outside = root_dir.path().join("outside/project");
        std::fs::create_dir_all(&outside).unwrap();

        let preview = preview_pin_workspace_path(&outside, &root, true);
        assert!(!preview.exists());
        let materialized = pin_workspace_path(&outside, &root, true);

        assert_eq!(materialized, preview);
        assert!(materialized.is_dir());
    }

    #[test]
    fn confinement_preview_lexically_cleans_missing_parent_escapes() {
        let root_dir = tempfile::tempdir().unwrap();
        let root = root_dir.path().join("workspaces");
        let inside = root.join("inside");
        std::fs::create_dir_all(&inside).unwrap();
        let escaping = inside
            .join("missing")
            .join("..")
            .join("..")
            .join("..")
            .join("outside-new");

        let preview = preview_pin_workspace_path(&escaping, &root, true);
        assert!(preview.starts_with(canonicalize_best_effort(&root)));
        assert_ne!(preview, lexically_clean(&escaping));
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
        // The relocated leaf carries a hash suffix of the full original path
        // (see `relocate_under_root`), not the bare basename.
        let leaf = pinned.file_name().unwrap().to_str().unwrap();
        assert!(
            leaf.starts_with("escaped-outside-"),
            "expected a hash-suffixed leaf, got {leaf:?}"
        );
        assert_eq!(pinned.parent().unwrap(), canon_root);
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
        // The relocated leaf carries a hash suffix of the full original path
        // (see `relocate_under_root`), not the bare basename.
        let leaf = pinned.file_name().unwrap().to_str().unwrap();
        assert!(
            leaf.starts_with("my-real-project-"),
            "expected a hash-suffixed leaf, got {leaf:?}"
        );
        assert_eq!(pinned.parent().unwrap(), canon_root);
    }

    /// HIGH finding (PR #467 review): two different escaping paths that
    /// share the same basename must NOT collapse onto the same relocated
    /// directory — that would silently mix state across tenants, defeating
    /// the entire point of confinement. The relocation target must
    /// incorporate a hash of the FULL original path, not just its leaf.
    #[test]
    fn pin_workspace_path_confined_relocates_same_basename_to_distinct_dirs() {
        let root_dir = tempfile::tempdir().unwrap();
        let root = root_dir.path().join("workspaces");
        std::fs::create_dir_all(&root).unwrap();

        let tenant_a = tempfile::tempdir().unwrap();
        let tenant_b = tempfile::tempdir().unwrap();
        let path_a = tenant_a.path().join("project");
        let path_b = tenant_b.path().join("project");
        std::fs::create_dir_all(&path_a).unwrap();
        std::fs::create_dir_all(&path_b).unwrap();

        let pinned_a = pin_workspace_path(&path_a, &root, true);
        let pinned_b = pin_workspace_path(&path_b, &root, true);

        assert_ne!(
            pinned_a, pinned_b,
            "escaping paths sharing a basename must relocate to distinct directories, \
             got {pinned_a:?} and {pinned_b:?}"
        );
        let canon_root = root.canonicalize().unwrap();
        assert!(pinned_a.starts_with(&canon_root));
        assert!(pinned_b.starts_with(&canon_root));
    }

    /// HIGH finding (PR #467 review): relocation must be deterministic — the
    /// same original path pinned twice (e.g. across process restarts) lands
    /// in the SAME relocated directory, so a session's confined workspace
    /// stays stable.
    #[test]
    fn pin_workspace_path_confined_relocation_is_deterministic() {
        let root_dir = tempfile::tempdir().unwrap();
        let root = root_dir.path().join("workspaces");
        std::fs::create_dir_all(&root).unwrap();

        let elsewhere = tempfile::tempdir().unwrap();
        let escape = elsewhere.path().join("repeatable-project");
        std::fs::create_dir_all(&escape).unwrap();

        let pinned_first = pin_workspace_path(&escape, &root, true);
        let pinned_second = pin_workspace_path(&escape, &root, true);

        assert_eq!(
            pinned_first, pinned_second,
            "pinning the same original path twice must produce the same relocated target"
        );
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
