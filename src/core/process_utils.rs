use std::collections::HashMap;
#[cfg(target_os = "windows")]
use std::path::Path;
use std::path::PathBuf;
use std::sync::{LazyLock, RwLock};
use std::time::{Duration, Instant};
#[cfg(not(target_os = "windows"))]
use std::process::Stdio;
#[cfg(target_os = "windows")]
use std::sync::OnceLock;

#[cfg(not(target_os = "windows"))]
use tokio::sync::Mutex as AsyncMutex;

#[cfg(target_os = "windows")]
const CREATE_NO_WINDOW: u32 = 0x0800_0000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShellCommand {
    pub program: String,
    pub arg: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandEnvironmentSource {
    InheritedProcess,
    UnixLoginShell,
}

impl CommandEnvironmentSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::InheritedProcess => "process_env",
            Self::UnixLoginShell => "unix_login_shell",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PythonDiscoveryDiagnostics {
    pub configured: Option<String>,
    pub resolved: Option<String>,
    pub invocation: Option<String>,
    pub source: Option<String>,
    pub tried: Vec<String>,
}

impl PythonDiscoveryDiagnostics {
    fn none() -> Self {
        Self {
            configured: None,
            resolved: None,
            invocation: None,
            source: None,
            tried: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandEnvironmentDiagnostics {
    pub source: CommandEnvironmentSource,
    pub import_shell: Option<String>,
    pub import_error: Option<String>,
    pub path: Option<String>,
    pub path_entries: Option<usize>,
    pub python: PythonDiscoveryDiagnostics,
}

impl CommandEnvironmentDiagnostics {
    fn inherited_process(import_error: Option<String>) -> Self {
        Self {
            source: CommandEnvironmentSource::InheritedProcess,
            import_shell: None,
            import_error,
            path: None,
            path_entries: None,
            python: PythonDiscoveryDiagnostics::none(),
        }
    }

    fn unix_login_shell(import_shell: String) -> Self {
        Self {
            source: CommandEnvironmentSource::UnixLoginShell,
            import_shell: Some(import_shell),
            import_error: None,
            path: None,
            path_entries: None,
            python: PythonDiscoveryDiagnostics::none(),
        }
    }

    pub fn summary(&self) -> String {
        let mut parts = vec![format!("env_source={}", self.source.as_str())];
        if let Some(shell) = self.import_shell.as_deref() {
            parts.push(format!("import_shell={shell}"));
        }
        if let Some(entries) = self.path_entries {
            parts.push(format!("path_entries={entries}"));
        }
        if let Some(error) = self.import_error.as_deref() {
            parts.push(format!("import_error={error}"));
        }
        parts.join(", ")
    }
}

#[derive(Debug, Clone)]
pub struct PreparedCommandEnvironment {
    pub env: HashMap<String, String>,
    pub diagnostics: CommandEnvironmentDiagnostics,
}

impl PreparedCommandEnvironment {
    pub fn apply_to_tokio_command(&self, command: &mut tokio::process::Command) {
        for (key, value) in &self.env {
            command.env(key, value);
        }
    }
}

#[derive(Debug, Clone)]
struct ImportedCommandEnvironment {
    env: HashMap<String, String>,
    diagnostics: CommandEnvironmentDiagnostics,
}

#[cfg(not(target_os = "windows"))]
#[derive(Debug, Clone)]
struct CachedUnixShellEnvironment {
    imported: ImportedCommandEnvironment,
    expires_at: Instant,
}

#[cfg(not(target_os = "windows"))]
const UNIX_SHELL_ENV_CACHE_TTL: Duration = Duration::from_secs(60);
#[cfg(not(target_os = "windows"))]
const UNIX_SHELL_ENV_FALLBACK_TTL: Duration = Duration::from_secs(10);
#[cfg(not(target_os = "windows"))]
const UNIX_SHELL_ENV_TIMEOUT: Duration = Duration::from_secs(10);

#[cfg(not(target_os = "windows"))]
static UNIX_SHELL_ENV_CACHE: LazyLock<RwLock<Option<CachedUnixShellEnvironment>>> =
    LazyLock::new(|| RwLock::new(None));
#[cfg(not(target_os = "windows"))]
static UNIX_SHELL_ENV_REFRESH_LOCK: LazyLock<AsyncMutex<()>> =
    LazyLock::new(|| AsyncMutex::new(()));

pub async fn build_command_environment(
    overrides: &HashMap<String, String>,
) -> PreparedCommandEnvironment {
    let base = imported_command_environment().await;
    let mut env = base.env;
    env.extend(overrides.iter().map(|(key, value)| (key.clone(), value.clone())));

    let mut diagnostics = base.diagnostics;
    diagnostics.path = env.get("PATH").cloned();
    diagnostics.path_entries = diagnostics.path.as_deref().map(count_path_entries);
    diagnostics.python = resolve_python_diagnostics(&env);

    PreparedCommandEnvironment { env, diagnostics }
}

async fn imported_command_environment() -> ImportedCommandEnvironment {
    #[cfg(target_os = "windows")]
    {
        ImportedCommandEnvironment::from_process_env(None)
    }

    #[cfg(not(target_os = "windows"))]
    {
        imported_unix_shell_environment_cached().await
    }
}

impl ImportedCommandEnvironment {
    fn from_process_env(import_error: Option<String>) -> Self {
        let env = current_process_env_map();
        let mut diagnostics = CommandEnvironmentDiagnostics::inherited_process(import_error);
        diagnostics.path = env.get("PATH").cloned();
        diagnostics.path_entries = diagnostics.path.as_deref().map(count_path_entries);
        Self { env, diagnostics }
    }
}

fn current_process_env_map() -> HashMap<String, String> {
    std::env::vars().collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PythonCandidate {
    source: String,
    program: String,
    args: Vec<String>,
    path_hint: Option<PathBuf>,
}

impl PythonCandidate {
    fn configured(program: String) -> Self {
        Self {
            source: "configured".to_string(),
            program,
            args: Vec::new(),
            path_hint: None,
        }
    }

    fn command<S: Into<String>>(source: &str, program: S, args: &[&str]) -> Self {
        Self {
            source: source.to_string(),
            program: program.into(),
            args: args.iter().map(|value| value.to_string()).collect(),
            path_hint: None,
        }
    }

    fn hinted_path<P: Into<PathBuf>>(source: &str, path: P) -> Self {
        let path = path.into();
        Self {
            source: source.to_string(),
            program: path.to_string_lossy().to_string(),
            args: Vec::new(),
            path_hint: Some(path),
        }
    }

    fn display(&self) -> String {
        render_command_line(&self.program, self.args.iter().map(String::as_str))
    }
}

#[cfg(target_os = "windows")]
fn windows_executable_extensions() -> Vec<String> {
    std::env::var("PATHEXT")
        .ok()
        .map(|value| {
            value
                .split(';')
                .map(str::trim)
                .filter(|part| !part.is_empty())
                .map(|part| part.to_ascii_lowercase())
                .collect::<Vec<_>>()
        })
        .filter(|exts| !exts.is_empty())
        .unwrap_or_else(|| {
            vec![
                ".exe".to_string(),
                ".cmd".to_string(),
                ".bat".to_string(),
                ".com".to_string(),
            ]
        })
}

#[cfg(target_os = "windows")]
fn windows_path_name_candidates(name: &str) -> Vec<String> {
    let path = Path::new(name);
    let ext = path
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or_default();
    if !ext.is_empty() {
        return vec![name.to_string()];
    }

    let mut candidates = vec![name.to_string()];
    for ext in windows_executable_extensions() {
        candidates.push(format!("{name}{ext}"));
    }
    candidates
}

fn resolve_executable_from_env_path(path_env: Option<&str>, name: &str) -> Option<PathBuf> {
    let path_env = path_env?;
    let path_dirs: Vec<PathBuf> = std::env::split_paths(path_env).collect();

    #[cfg(target_os = "windows")]
    {
        for dir in &path_dirs {
            for candidate_name in windows_path_name_candidates(name) {
                let candidate = dir.join(candidate_name);
                if candidate.is_file() {
                    return Some(candidate);
                }
            }
        }
        return None;
    }

    #[cfg(not(target_os = "windows"))]
    {
        path_dirs
            .into_iter()
            .map(|dir| dir.join(name))
            .find(|candidate| candidate.is_file())
    }
}

#[cfg(target_os = "windows")]
fn windows_common_python_paths(env: &HashMap<String, String>) -> Vec<PathBuf> {
    let mut known = Vec::new();

    let local_app_data = env
        .get("LocalAppData")
        .cloned()
        .or_else(|| std::env::var("LocalAppData").ok());
    let app_data = env
        .get("AppData")
        .cloned()
        .or_else(|| std::env::var("AppData").ok());
    let user_profile = env
        .get("USERPROFILE")
        .cloned()
        .or_else(|| std::env::var("USERPROFILE").ok());

    for key in ["ProgramW6432", "ProgramFiles", "ProgramFiles(x86)"] {
        if let Some(base) = env.get(key).cloned().or_else(|| std::env::var(key).ok()) {
            let base = PathBuf::from(base);
            for version in ["Python313", "Python312", "Python311", "Python310", "Python39"] {
                known.push(base.join("Python").join(version).join("python.exe"));
            }
            known.push(base.join("Python").join("Launcher").join("py.exe"));
            known.push(base.join("Python311").join("python.exe"));
            known.push(base.join("Python312").join("python.exe"));
            known.push(base.join("Python313").join("python.exe"));
            known.push(base.join("Anaconda3").join("python.exe"));
            known.push(base.join("Miniconda3").join("python.exe"));
        }
    }

    if let Some(local_app_data) = local_app_data {
        let base = PathBuf::from(local_app_data);
        for version in ["Python313", "Python312", "Python311", "Python310", "Python39"] {
            known.push(
                base.join("Programs")
                    .join("Python")
                    .join(version)
                    .join("python.exe"),
            );
        }
        known.push(base.join("Programs").join("Python").join("Launcher").join("py.exe"));
        known.push(base.join("Microsoft").join("WindowsApps").join("python.exe"));
        known.push(base.join("Microsoft").join("WindowsApps").join("python3.exe"));
        known.push(base.join("Programs").join("Python").join("Python312").join("python.exe"));
        known.push(base.join("Programs").join("Python").join("Python311").join("python.exe"));
    }

    if let Some(app_data) = app_data {
        let roaming = PathBuf::from(app_data);
        known.push(roaming.join("Python").join("Python312").join("python.exe"));
        known.push(roaming.join("Python").join("Python311").join("python.exe"));
        known.push(roaming.join("pyenv").join("pyenv-win").join("shims").join("python.exe"));
        known.push(roaming.join("pyenv").join("pyenv-win").join("shims").join("python3.exe"));
        known.push(roaming.join("pyenv").join("pyenv-win").join("bin").join("pyenv.bat"));
    }

    if let Some(user_profile) = user_profile {
        let home = PathBuf::from(user_profile);
        known.push(home.join("AppData").join("Local").join("Programs").join("Python").join("Python312").join("python.exe"));
        known.push(home.join("AppData").join("Local").join("Programs").join("Python").join("Python311").join("python.exe"));
        known.push(home.join("miniconda3").join("python.exe"));
        known.push(home.join("anaconda3").join("python.exe"));
        known.push(home.join(".pyenv").join("pyenv-win").join("shims").join("python.exe"));
        known.push(home.join(".pyenv").join("pyenv-win").join("shims").join("python3.exe"));
    }

    known
}

fn python_candidate_sequence(
    env: &HashMap<String, String>,
    configured: Option<&str>,
) -> (Vec<PythonCandidate>, Option<String>) {
    let configured = configured
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);

    let mut candidates = Vec::new();
    if let Some(configured_path) = configured.clone() {
        candidates.push(PythonCandidate::configured(configured_path));
    }

    #[cfg(target_os = "windows")]
    {
        if let Some(virtual_env) = env.get("VIRTUAL_ENV").filter(|value| !value.trim().is_empty()) {
            candidates.push(PythonCandidate::hinted_path(
                "virtual_env",
                PathBuf::from(virtual_env).join("Scripts").join("python.exe"),
            ));
        }
        if let Some(conda_prefix) = env.get("CONDA_PREFIX").filter(|value| !value.trim().is_empty()) {
            candidates.push(PythonCandidate::hinted_path(
                "conda_env",
                PathBuf::from(conda_prefix).join("python.exe"),
            ));
        }
        candidates.push(PythonCandidate::command("launcher", "py", &["-3"]));
        candidates.push(PythonCandidate::command("path", "py", &[]));
        candidates.push(PythonCandidate::command("path", "python", &[]));
        candidates.push(PythonCandidate::command("path", "python3", &[]));
        for path in windows_common_python_paths(env) {
            candidates.push(PythonCandidate::hinted_path("common_path", path));
        }
    }

    #[cfg(not(target_os = "windows"))]
    {
        if let Some(virtual_env) = env.get("VIRTUAL_ENV").filter(|value| !value.trim().is_empty()) {
            candidates.push(PythonCandidate::hinted_path(
                "virtual_env",
                PathBuf::from(virtual_env).join("bin").join("python"),
            ));
        }
        if let Some(conda_prefix) = env.get("CONDA_PREFIX").filter(|value| !value.trim().is_empty()) {
            candidates.push(PythonCandidate::hinted_path(
                "conda_env",
                PathBuf::from(conda_prefix).join("bin").join("python"),
            ));
        }
        candidates.push(PythonCandidate::command("path", "python3", &[]));
        candidates.push(PythonCandidate::command("path", "python", &[]));
    }

    #[cfg(target_os = "macos")]
    {
        candidates.push(PythonCandidate::hinted_path(
            "common_path",
            "/opt/homebrew/bin/python3",
        ));
        candidates.push(PythonCandidate::hinted_path(
            "common_path",
            "/usr/local/bin/python3",
        ));
    }

    (candidates, configured)
}

fn resolve_python_candidate(
    path_env: Option<&str>,
    candidate: &PythonCandidate,
) -> Option<(PathBuf, String)> {
    if let Some(path_hint) = candidate.path_hint.as_ref() {
        if path_hint.is_file() {
            return Some((path_hint.clone(), candidate.display()));
        }
    }

    let resolved = if candidate.source == "configured" {
        let configured = PathBuf::from(&candidate.program);
        configured.is_file().then_some(configured)
    } else {
        resolve_executable_from_env_path(path_env, &candidate.program)
    }?;

    let invocation = if candidate.args.is_empty() {
        resolved.to_string_lossy().to_string()
    } else {
        render_command_line(
            &resolved.to_string_lossy(),
            candidate.args.iter().map(String::as_str),
        )
    };

    Some((resolved, invocation))
}

fn resolve_python_diagnostics(env: &HashMap<String, String>) -> PythonDiscoveryDiagnostics {
    let configured = env
        .get("BAMBOO_PYTHON")
        .or_else(|| env.get("PYTHON_BIN"))
        .or_else(|| env.get("PYTHON"))
        .cloned();
    let path_env = env.get("PATH").map(String::as_str);
    let (candidates, configured_copy) = python_candidate_sequence(env, configured.as_deref());

    let mut diagnostics = PythonDiscoveryDiagnostics {
        configured: configured_copy,
        resolved: None,
        invocation: None,
        source: None,
        tried: Vec::new(),
    };

    for candidate in candidates {
        diagnostics.tried.push(candidate.display());

        if let Some((resolved, invocation)) = resolve_python_candidate(path_env, &candidate) {
            diagnostics.resolved = Some(resolved.to_string_lossy().to_string());
            diagnostics.invocation = Some(invocation);
            diagnostics.source = Some(candidate.source.clone());
            break;
        }
    }

    diagnostics
}

fn count_path_entries(path: &str) -> usize {
    std::env::split_paths(path).count()
}

fn env_entry_start(line: &str) -> Option<usize> {
    let eq_index = line.find('=')?;
    if eq_index == 0 {
        return None;
    }

    let key = &line[..eq_index];
    let mut chars = key.chars();
    let first = chars.next()?;
    if !(first == '_' || first.is_ascii_alphabetic()) {
        return None;
    }
    if chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric()) {
        Some(eq_index)
    } else {
        None
    }
}

fn parse_env_output(output: &str) -> HashMap<String, String> {
    let mut env = HashMap::new();
    let mut current_key: Option<String> = None;
    let mut current_value = String::new();

    for line in output.split_terminator('\n') {
        if let Some(eq_index) = env_entry_start(line) {
            if let Some(previous_key) = current_key.replace(line[..eq_index].to_string()) {
                env.insert(previous_key, std::mem::take(&mut current_value));
            }
            current_value.push_str(&line[eq_index + 1..]);
            continue;
        }

        if current_key.is_some() {
            current_value.push('\n');
            current_value.push_str(line);
        }
    }

    if let Some(last_key) = current_key {
        env.insert(last_key, current_value);
    }

    env
}

#[cfg(not(target_os = "windows"))]
fn find_unix_shell_on_path(name: &str) -> Option<PathBuf> {
    let path_env = std::env::var_os("PATH")?;
    std::env::split_paths(&path_env)
        .map(|dir| dir.join(name))
        .find(|candidate| candidate.is_file())
}

#[cfg(not(target_os = "windows"))]
fn preferred_unix_env_import_shell() -> Option<PathBuf> {
    if let Some(configured_shell) = std::env::var_os("SHELL") {
        let configured_shell = PathBuf::from(configured_shell);
        if configured_shell.is_file() {
            return Some(configured_shell);
        }
        if let Some(file_name) = configured_shell.file_name().and_then(|value| value.to_str()) {
            if let Some(found) = find_unix_shell_on_path(file_name) {
                return Some(found);
            }
        }
    }

    let fallbacks = if cfg!(target_os = "macos") {
        ["/bin/zsh", "/bin/bash", "/bin/sh"]
    } else {
        ["/bin/bash", "/bin/zsh", "/bin/sh"]
    };

    fallbacks
        .iter()
        .map(PathBuf::from)
        .find(|candidate| candidate.is_file())
}

#[cfg(not(target_os = "windows"))]
fn read_cached_unix_shell_environment() -> Option<ImportedCommandEnvironment> {
    let guard = UNIX_SHELL_ENV_CACHE.read().ok()?;
    let cached = guard.as_ref()?;
    if Instant::now() < cached.expires_at {
        Some(cached.imported.clone())
    } else {
        None
    }
}

#[cfg(not(target_os = "windows"))]
fn write_cached_unix_shell_environment(imported: ImportedCommandEnvironment) {
    let ttl = match imported.diagnostics.source {
        CommandEnvironmentSource::UnixLoginShell => UNIX_SHELL_ENV_CACHE_TTL,
        CommandEnvironmentSource::InheritedProcess => UNIX_SHELL_ENV_FALLBACK_TTL,
    };
    let expires_at = Instant::now() + ttl;
    if let Ok(mut guard) = UNIX_SHELL_ENV_CACHE.write() {
        *guard = Some(CachedUnixShellEnvironment {
            imported,
            expires_at,
        });
    }
}

#[cfg(not(target_os = "windows"))]
async fn imported_unix_shell_environment_cached() -> ImportedCommandEnvironment {
    if let Some(cached) = read_cached_unix_shell_environment() {
        return cached;
    }

    let _refresh = UNIX_SHELL_ENV_REFRESH_LOCK.lock().await;
    if let Some(cached) = read_cached_unix_shell_environment() {
        return cached;
    }

    let imported = match import_unix_shell_environment().await {
        Ok(imported) => imported,
        Err(error) => ImportedCommandEnvironment::from_process_env(Some(error)),
    };
    write_cached_unix_shell_environment(imported.clone());
    imported
}

#[cfg(not(target_os = "windows"))]
async fn import_unix_shell_environment() -> Result<ImportedCommandEnvironment, String> {
    let shell = preferred_unix_env_import_shell()
        .ok_or_else(|| "No Unix login shell available for environment import".to_string())?;
    let shell_display = shell.to_string_lossy().to_string();

    let mut command = tokio::process::Command::new(&shell);
    hide_window_for_tokio_command(&mut command);
    command
        .arg("-lc")
        .arg("env")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    let output = tokio::time::timeout(UNIX_SHELL_ENV_TIMEOUT, command.output())
        .await
        .map_err(|_| {
            format!(
                "Timed out after {}s while importing environment from {}",
                UNIX_SHELL_ENV_TIMEOUT.as_secs(),
                shell_display
            )
        })
        .and_then(|result| {
            result.map_err(|error| {
                format!(
                    "Failed to spawn login shell {} for environment import: {}",
                    shell_display, error
                )
            })
        })?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        return Err(format!(
            "Login shell {} exited with status {} while importing environment{}",
            shell_display,
            output.status,
            if stderr.is_empty() {
                String::new()
            } else {
                format!(": {stderr}")
            }
        ));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let env = parse_env_output(&stdout);
    if env.is_empty() {
        return Err(format!(
            "Login shell {} returned no parseable environment variables",
            shell_display
        ));
    }

    let mut diagnostics = CommandEnvironmentDiagnostics::unix_login_shell(shell_display);
    diagnostics.path = env.get("PATH").cloned();
    diagnostics.path_entries = diagnostics.path.as_deref().map(count_path_entries);

    Ok(ImportedCommandEnvironment { env, diagnostics })
}

#[cfg(test)]
pub(crate) fn clear_command_environment_cache_for_tests() {
    #[cfg(not(target_os = "windows"))]
    if let Ok(mut guard) = UNIX_SHELL_ENV_CACHE.write() {
        *guard = None;
    }
}

#[cfg(test)]
pub(crate) fn prime_command_environment_cache_for_tests(
    env: HashMap<String, String>,
    diagnostics: CommandEnvironmentDiagnostics,
) {
    #[cfg(not(target_os = "windows"))]
    write_cached_unix_shell_environment(ImportedCommandEnvironment { env, diagnostics });
}

#[cfg(target_os = "windows")]
fn parse_truthy_flag(raw: &str) -> bool {
    matches!(
        raw.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

/// Render a command line for diagnostics/logging.
pub fn render_command_line<S, I>(program: &str, args: I) -> String
where
    S: AsRef<str>,
    I: IntoIterator<Item = S>,
{
    fn quote(part: &str) -> String {
        if part.is_empty()
            || part.chars().any(char::is_whitespace)
            || part.contains('"')
            || part.contains('\'')
        {
            format!("{part:?}")
        } else {
            part.to_string()
        }
    }

    let mut parts = vec![quote(program)];
    for arg in args {
        parts.push(quote(arg.as_ref()));
    }
    parts.join(" ")
}

/// Whether Windows command tracing is enabled.
///
/// Supported env vars:
/// - `BAMBOO_WINDOWS_CMD_TRACE`
/// - `BODHI_WINDOWS_CMD_TRACE`
pub fn windows_command_trace_enabled() -> bool {
    #[cfg(target_os = "windows")]
    {
        const ENV_KEYS: [&str; 2] = ["BAMBOO_WINDOWS_CMD_TRACE", "BODHI_WINDOWS_CMD_TRACE"];
        ENV_KEYS.iter().any(|key| {
            std::env::var(key)
                .map(|value| parse_truthy_flag(&value))
                .unwrap_or(false)
        })
    }

    #[cfg(not(target_os = "windows"))]
    {
        false
    }
}

/// Emit a command trace log on Windows when trace switch is enabled.
pub fn trace_windows_command<S, I>(scope: &str, program: &str, args: I)
where
    S: AsRef<str>,
    I: IntoIterator<Item = S>,
{
    #[cfg(target_os = "windows")]
    {
        if windows_command_trace_enabled() {
            let command_line = render_command_line(program, args);
            tracing::info!("[windows-cmd-trace] {}: {}", scope, command_line);
        }
    }

    #[cfg(not(target_os = "windows"))]
    {
        let _ = (scope, program, args);
    }
}

pub fn decode_process_line_lossy(bytes: &mut Vec<u8>) -> String {
    if bytes.last() == Some(&b'\n') {
        bytes.pop();
        if bytes.last() == Some(&b'\r') {
            bytes.pop();
        }
    }

    let line = String::from_utf8_lossy(bytes).into_owned();
    bytes.clear();
    line
}

#[cfg(target_os = "windows")]
fn canonicalize_for_match(path: &Path) -> String {
    path.to_string_lossy()
        .replace('/', "\\")
        .to_ascii_lowercase()
}

#[cfg(target_os = "windows")]
fn looks_like_git_bash(path: &Path) -> bool {
    let lower = canonicalize_for_match(path);
    if !lower.ends_with("\\bash.exe") {
        return false;
    }
    if lower.ends_with("\\system32\\bash.exe") {
        return false;
    }
    lower.contains("git")
}

#[cfg(target_os = "windows")]
fn first_existing<I>(paths: I) -> Option<PathBuf>
where
    I: IntoIterator<Item = PathBuf>,
{
    paths
        .into_iter()
        .find(|path| path.is_file() && looks_like_git_bash(path))
}

#[cfg(target_os = "windows")]
fn find_git_bash() -> Option<PathBuf> {
    if let Some(override_path) = std::env::var_os("BAMBOO_WINDOWS_BASH_PATH") {
        let path = PathBuf::from(override_path);
        if path.is_file() {
            return Some(path);
        }
    }

    let mut known = Vec::new();
    for key in ["ProgramW6432", "ProgramFiles", "ProgramFiles(x86)"] {
        if let Some(base) = std::env::var_os(key) {
            let base = PathBuf::from(base);
            known.push(base.join("Git").join("bin").join("bash.exe"));
            known.push(base.join("Git").join("usr").join("bin").join("bash.exe"));
        }
    }
    if let Some(local_app_data) = std::env::var_os("LocalAppData") {
        let base = PathBuf::from(local_app_data).join("Programs").join("Git");
        known.push(base.join("bin").join("bash.exe"));
        known.push(base.join("usr").join("bin").join("bash.exe"));
    }

    if let Some(path) = first_existing(known) {
        return Some(path);
    }

    let path_env = std::env::var_os("PATH")?;
    let path_candidates = std::env::split_paths(&path_env).map(|dir| dir.join("bash.exe"));
    first_existing(path_candidates)
}

pub fn preferred_bash_shell() -> ShellCommand {
    #[cfg(target_os = "windows")]
    {
        static WINDOWS_SHELL: OnceLock<ShellCommand> = OnceLock::new();
        return WINDOWS_SHELL
            .get_or_init(|| {
                if let Some(bash) = find_git_bash() {
                    ShellCommand {
                        program: bash.to_string_lossy().to_string(),
                        arg: "-lc",
                    }
                } else {
                    ShellCommand {
                        program: "cmd".to_string(),
                        arg: "/c",
                    }
                }
            })
            .clone();
    }

    #[cfg(not(target_os = "windows"))]
    {
        ShellCommand {
            program: "sh".to_string(),
            arg: "-c",
        }
    }
}

/// Configure a standard-library process command to avoid showing a console
/// window on Windows. No-op on non-Windows platforms.
pub fn hide_window_for_std_command(command: &mut std::process::Command) {
    #[cfg(target_os = "windows")]
    {
        use std::os::windows::process::CommandExt;
        command.creation_flags(CREATE_NO_WINDOW);
    }

    #[cfg(not(target_os = "windows"))]
    {
        let _ = command;
    }
}

/// Configure a Tokio process command to avoid showing a console window on
/// Windows. No-op on non-Windows platforms.
pub fn hide_window_for_tokio_command(command: &mut tokio::process::Command) {
    #[cfg(target_os = "windows")]
    {
        command.creation_flags(CREATE_NO_WINDOW);
    }

    #[cfg(not(target_os = "windows"))]
    {
        let _ = command;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_env_output_ignores_leading_noise_and_handles_multiline_values() {
        let parsed = parse_env_output(
            "warning before env\nPATH=/usr/bin:/bin\nMULTI=line1\nline2\nHOME=/Users/test\n",
        );

        assert_eq!(parsed.get("PATH").map(String::as_str), Some("/usr/bin:/bin"));
        assert_eq!(parsed.get("MULTI").map(String::as_str), Some("line1\nline2"));
        assert_eq!(parsed.get("HOME").map(String::as_str), Some("/Users/test"));
    }

    #[test]
    fn diagnostics_summary_mentions_source_and_path_entries() {
        let diagnostics = CommandEnvironmentDiagnostics {
            source: CommandEnvironmentSource::UnixLoginShell,
            import_shell: Some("/bin/zsh".to_string()),
            import_error: None,
            path: Some("/usr/bin:/bin".to_string()),
            path_entries: Some(2),
            python: PythonDiscoveryDiagnostics {
                configured: Some("python3".to_string()),
                resolved: Some("/usr/bin/python3".to_string()),
                invocation: Some("/usr/bin/python3".to_string()),
                source: Some("path".to_string()),
                tried: vec!["python3".to_string(), "python".to_string()],
            },
        };

        let summary = diagnostics.summary();
        assert!(summary.contains("env_source=unix_login_shell"));
        assert!(summary.contains("import_shell=/bin/zsh"));
        assert!(summary.contains("path_entries=2"));
    }

    #[test]
    fn test_render_command_line_simple() {
        let result = render_command_line("echo", vec!["hello", "world"]);
        assert_eq!(result, "echo hello world");
    }

    #[test]
    fn test_render_command_line_with_spaces() {
        let result = render_command_line("cmd", vec!["arg with spaces", "normal"]);
        assert_eq!(result, r#"cmd "arg with spaces" normal"#);
    }

    #[test]
    fn test_render_command_line_with_quotes() {
        let result = render_command_line("cmd", vec!["arg\"with\"quotes"]);
        assert_eq!(result, r#"cmd "arg\"with\"quotes""#);
    }

    #[test]
    fn test_render_command_line_with_single_quotes() {
        let result = render_command_line("cmd", vec!["arg'with'single"]);
        assert_eq!(result, r#"cmd "arg'with'single""#);
    }

    #[test]
    fn test_render_command_line_empty_args() {
        let result = render_command_line("program", Vec::<&str>::new());
        assert_eq!(result, "program");
    }

    #[test]
    fn test_render_command_line_empty_arg() {
        let result = render_command_line("cmd", vec![""]);
        assert_eq!(result, r#"cmd """#);
    }

    #[test]
    fn test_render_command_line_multiple_empty_args() {
        let result = render_command_line("cmd", vec!["", "valid", ""]);
        assert_eq!(result, r#"cmd "" valid """#);
    }

    #[test]
    fn test_render_command_line_program_with_spaces() {
        let result = render_command_line("my program", vec!["arg1"]);
        assert_eq!(result, r#""my program" arg1"#);
    }

    #[test]
    fn test_render_command_line_no_args() {
        let result = render_command_line("standalone", Vec::<&str>::new());
        assert_eq!(result, "standalone");
    }

    #[test]
    fn test_render_command_line_whitespace_in_arg() {
        let result = render_command_line("cmd", vec!["arg\twith\ttabs"]);
        // Tabs should trigger quoting
        assert!(result.starts_with("cmd \""));
        assert!(result.ends_with("\""));
    }

    #[test]
    fn test_render_command_line_newline_in_arg() {
        let result = render_command_line("cmd", vec!["arg\nwith\nnewline"]);
        // Newlines should trigger quoting
        assert!(result.starts_with("cmd \""));
        assert!(result.ends_with("\""));
    }

    #[test]
    fn test_render_command_line_complex() {
        let result = render_command_line(
            "my program",
            vec![
                "simple",
                "with spaces",
                "with\"quote",
                "with'apostrophe",
                "",
            ],
        );
        assert!(result.contains("my program"));
        assert!(result.contains("simple"));
        assert!(result.contains("with spaces"));
    }

    #[test]
    fn test_render_command_line_special_chars() {
        let result = render_command_line("cmd", vec!["arg$var", "arg*glob"]);
        assert_eq!(result, "cmd arg$var arg*glob");
    }

    #[test]
    fn test_render_command_line_backslash() {
        let result = render_command_line("cmd", vec![r"arg\with\backslash"]);
        assert_eq!(result, r"cmd arg\with\backslash");
    }

    #[test]
    fn test_render_command_line_unicode() {
        let result = render_command_line("cmd", vec!["unicode中文", "emoji😀"]);
        assert_eq!(result, "cmd unicode中文 emoji😀");
    }

    #[test]
    fn test_decode_process_line_lossy_strips_newline() {
        let mut bytes = b"hello\r\n".to_vec();
        let decoded = decode_process_line_lossy(&mut bytes);
        assert_eq!(decoded, "hello");
        assert!(bytes.is_empty());
    }

    #[test]
    fn test_decode_process_line_lossy_allows_invalid_utf8() {
        let mut bytes = vec![0xff, b'\n'];
        let decoded = decode_process_line_lossy(&mut bytes);
        assert_eq!(decoded, "\u{fffd}");
        assert!(bytes.is_empty());
    }

    #[test]
    fn resolve_python_diagnostics_prefers_configured_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        let configured = dir.path().join(if cfg!(target_os = "windows") {
            "python.exe"
        } else {
            "python3"
        });
        std::fs::write(&configured, b"").unwrap();

        let mut env = HashMap::new();
        env.insert(
            "BAMBOO_PYTHON".to_string(),
            configured.to_string_lossy().to_string(),
        );
        env.insert("PATH".to_string(), String::new());

        let diagnostics = resolve_python_diagnostics(&env);
        assert_eq!(diagnostics.configured, Some(configured.to_string_lossy().to_string()));
        assert_eq!(diagnostics.resolved, Some(configured.to_string_lossy().to_string()));
        assert_eq!(diagnostics.invocation, Some(configured.to_string_lossy().to_string()));
        assert_eq!(diagnostics.source, Some("configured".to_string()));
        assert!(!diagnostics.tried.is_empty());
    }

    #[test]
    fn resolve_python_diagnostics_finds_python_on_path() {
        let dir = tempfile::tempdir().unwrap();
        let python_name = if cfg!(target_os = "windows") {
            "python.exe"
        } else {
            "python3"
        };
        let python = dir.path().join(python_name);
        std::fs::write(&python, b"").unwrap();

        let mut env = HashMap::new();
        env.insert(
            "PATH".to_string(),
            std::env::join_paths([dir.path()]).unwrap().to_string_lossy().to_string(),
        );

        let diagnostics = resolve_python_diagnostics(&env);
        assert_eq!(
            diagnostics.resolved,
            Some(python.to_string_lossy().to_string())
        );
        assert_eq!(
            diagnostics.invocation,
            Some(python.to_string_lossy().to_string())
        );
        assert_eq!(diagnostics.source, Some("path".to_string()));
    }

    #[test]
    fn python_candidate_sequence_includes_default_names() {
        let env = HashMap::new();
        let (candidates, configured) = python_candidate_sequence(&env, Some("/custom/python"));
        assert_eq!(configured, Some("/custom/python".to_string()));
        assert_eq!(
            candidates.first().map(PythonCandidate::display).as_deref(),
            Some("/custom/python")
        );
        assert!(candidates.iter().any(|candidate| candidate.program == "python"));
        #[cfg(target_os = "windows")]
        {
            assert!(candidates.iter().any(|candidate| candidate.program == "py"));
            assert!(candidates.iter().any(|candidate| candidate.display() == "py -3"));
        }
        #[cfg(not(target_os = "windows"))]
        assert!(candidates.iter().any(|candidate| candidate.program == "python3"));
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn windows_path_resolution_supports_pathext_executables() {
        let dir = tempfile::tempdir().unwrap();
        let python = dir.path().join("python.exe");
        std::fs::write(&python, b"").unwrap();

        let path = std::env::join_paths([dir.path()])
            .unwrap()
            .to_string_lossy()
            .to_string();
        let resolved = resolve_executable_from_env_path(Some(&path), "python");
        assert_eq!(resolved, Some(python));
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn windows_python_candidates_include_launcher_and_common_paths() {
        let mut env = HashMap::new();
        env.insert("LocalAppData".to_string(), r"C:\Users\dev\AppData\Local".to_string());
        env.insert("AppData".to_string(), r"C:\Users\dev\AppData\Roaming".to_string());
        env.insert("USERPROFILE".to_string(), r"C:\Users\dev".to_string());
        env.insert("ProgramFiles".to_string(), r"C:\Program Files".to_string());

        let (candidates, _) = python_candidate_sequence(&env, None);
        assert!(candidates.iter().any(|candidate| candidate.display() == "py -3"));
        assert!(candidates.iter().any(|candidate| {
            candidate
                .path_hint
                .as_ref()
                .map(|path| canonicalize_for_match(path).ends_with("\\python312\\python.exe"))
                .unwrap_or(false)
        }));
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn test_looks_like_git_bash_accepts_git_paths() {
        assert!(looks_like_git_bash(Path::new(
            r"C:\Program Files\Git\bin\bash.exe"
        )));
        assert!(looks_like_git_bash(Path::new(
            r"C:\Users\dev\scoop\apps\git\current\usr\bin\bash.exe"
        )));
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn test_looks_like_git_bash_rejects_system32_bash() {
        assert!(!looks_like_git_bash(Path::new(
            r"C:\Windows\System32\bash.exe"
        )));
    }
}
