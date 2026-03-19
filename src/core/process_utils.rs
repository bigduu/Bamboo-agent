#[cfg(target_os = "windows")]
use std::path::{Path, PathBuf};
#[cfg(target_os = "windows")]
use std::sync::OnceLock;

#[cfg(target_os = "windows")]
const CREATE_NO_WINDOW: u32 = 0x0800_0000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShellCommand {
    pub program: String,
    pub arg: &'static str,
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
