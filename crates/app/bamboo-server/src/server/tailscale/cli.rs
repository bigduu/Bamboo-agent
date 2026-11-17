use std::{
    ffi::OsString,
    io::Read,
    net::SocketAddr,
    path::{Path, PathBuf},
    process::{Output, Stdio},
    sync::OnceLock,
    time::Duration,
};

use tokio::{process::Child, time::timeout};

const WORKER_FLAG: &str = "--internal-tailscale-serve";
const COMMAND_TIMEOUT: Duration = Duration::from_secs(6);
static BAMBOO_EXECUTABLE: OnceLock<PathBuf> = OnceLock::new();

pub(super) fn helper_available() -> bool {
    BAMBOO_EXECUTABLE.get().is_some()
}

pub(super) struct Cli(PathBuf);

impl Cli {
    pub(super) fn find() -> Option<Self> {
        let executable = if cfg!(windows) {
            "tailscale.exe"
        } else {
            "tailscale"
        };
        let path = std::env::var_os("PATH").unwrap_or_default();
        let mut candidates: Vec<_> = std::env::split_paths(&path)
            .filter(|path| path.is_absolute())
            .map(|dir| dir.join(executable))
            .collect();
        #[cfg(target_os = "macos")]
        {
            const APP: &str = "/Applications/Tailscale.app/Contents/MacOS/Tailscale";
            if Path::new(APP).is_file() {
                // The system launcher is a shell script without exec. Use the
                // real CLI so the worker owns the process holding Serve open.
                candidates.retain(|path| path != Path::new("/usr/local/bin/tailscale"));
                candidates.push(PathBuf::from(APP));
            }
            candidates.push(PathBuf::from("/opt/homebrew/bin/tailscale"));
        }
        #[cfg(windows)]
        if let Some(program_files) = std::env::var_os("ProgramFiles") {
            candidates.push(
                PathBuf::from(program_files)
                    .join("Tailscale")
                    .join(executable),
            );
        }
        #[cfg(all(unix, not(target_os = "macos")))]
        candidates.extend(["/usr/bin/tailscale", "/usr/local/bin/tailscale"].map(PathBuf::from));
        candidates.into_iter().find(|path| path.is_file()).map(Self)
    }

    pub(super) async fn output(&self, args: &[&str]) -> std::io::Result<Output> {
        timeout(COMMAND_TIMEOUT, command(&self.0).args(args).output())
            .await
            .map_err(std::io::Error::other)?
    }

    pub(super) fn serve(&self, port: u16, backend: SocketAddr) -> std::io::Result<ServeWorker> {
        let executable = BAMBOO_EXECUTABLE
            .get()
            .ok_or(std::io::ErrorKind::Unsupported)?;
        let mut child = command(executable);
        child
            .arg(WORKER_FLAG)
            .arg(&self.0)
            .arg(port.to_string())
            .arg(backend.port().to_string())
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(false);
        child.spawn().map(ServeWorker)
    }
}

fn command(executable: &Path) -> tokio::process::Command {
    let mut command = tokio::process::Command::new(executable);
    command.env_clear().stdin(Stdio::null()).kill_on_drop(true);
    for name in [
        "HOME",
        "PATH",
        "TMPDIR",
        "USER",
        "LOGNAME",
        "XDG_RUNTIME_DIR",
        "SystemRoot",
        "ProgramData",
        "USERPROFILE",
        "LOCALAPPDATA",
        "TEMP",
        "TMP",
    ] {
        if let Some(value) = std::env::var_os(name) {
            command.env(name, value);
        }
    }
    // The macOS app executable is also the CLI. This selects its CLI mode
    // when Bamboo is started by a GUI without a shell environment.
    #[cfg(target_os = "macos")]
    command.env("SHLVL", "1");
    #[cfg(windows)]
    command.creation_flags(0x0800_0000); // CREATE_NO_WINDOW
    command
}

pub(super) struct ServeWorker(Child);

impl ServeWorker {
    pub(super) fn running(&mut self) -> bool {
        matches!(self.0.try_wait(), Ok(None))
    }

    pub(super) async fn stop(mut self) {
        drop(self.0.stdin.take());
        // A closed pipe is also the shutdown signal after a Bamboo SIGKILL.
        let _ = timeout(COMMAND_TIMEOUT, self.0.wait()).await;
    }
}

/// The worker has no application state. Its private stdin pipe ties the
/// foreground Serve registration to the parent even after a forced shutdown.
#[doc(hidden)]
pub fn helper_entry() -> Option<i32> {
    let mut args = std::env::args_os().skip(1);
    if args.next().as_deref() != Some(WORKER_FLAG.as_ref()) {
        if let Ok(executable) = std::env::current_exe() {
            let _ = BAMBOO_EXECUTABLE.set(executable);
        }
        return None;
    }
    let parse_port = |value: Option<OsString>| -> Option<u16> {
        value?.to_str()?.parse().ok().filter(|port| *port != 0)
    };
    let (Some(cli), Some(port), Some(backend), None) = (
        args.next().map(PathBuf::from),
        parse_port(args.next()),
        parse_port(args.next()),
        args.next(),
    ) else {
        return Some(2);
    };
    if !cli.is_absolute() {
        return Some(2);
    }
    let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    else {
        return Some(1);
    };
    Some(runtime.block_on(async move {
        let (closed, parent) = tokio::sync::oneshot::channel();
        std::thread::spawn(move || {
            let _ = std::io::stdin().read(&mut [0]);
            let _ = closed.send(());
        });
        let Ok(mut child) = command(&cli)
            .args([
                "serve",
                "--bg=false",
                &format!("--https={port}"),
                &format!("http://127.0.0.1:{backend}"),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
        else {
            return 1;
        };
        tokio::select! {
            result = child.wait() => i32::from(!result.is_ok_and(|status| status.success())),
            _ = parent => i32::from(child.kill().await.is_err()),
        }
    }))
}
