#[cfg(target_os = "windows")]
const CREATE_NO_WINDOW: u32 = 0x0800_0000;

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
            log::info!("[windows-cmd-trace] {}: {}", scope, command_line);
        }
    }

    #[cfg(not(target_os = "windows"))]
    {
        let _ = (scope, program, args);
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
