//! Production-binary checks for the isolated Jiandu data-root boundary.

#![cfg(unix)]

use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::fs;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use tempfile::TempDir;

const JIANDU_DATA_DIR_ENV: &str = "BAMBOO_JIANDU_DATA_DIR";

struct ServerProcess {
    child: Child,
    log_path: PathBuf,
}

impl ServerProcess {
    fn logs(&self) -> String {
        fs::read_to_string(&self.log_path).unwrap_or_else(|error| format!("<unreadable: {error}>"))
    }
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn unused_loopback_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("reserve loopback port");
    listener.local_addr().expect("read loopback address").port()
}

fn snapshot_files(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    if !root.exists() {
        return BTreeMap::new();
    }

    let mut snapshot = BTreeMap::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(path) = pending.pop() {
        for entry in fs::read_dir(&path).expect("read snapshot directory") {
            let entry = entry.expect("read snapshot entry");
            let entry_path = entry.path();
            let file_type = entry.file_type().expect("read snapshot entry type");
            if file_type.is_dir() {
                pending.push(entry_path);
            } else if file_type.is_file() {
                let relative = entry_path
                    .strip_prefix(root)
                    .expect("snapshot entry remains under root")
                    .to_path_buf();
                snapshot.insert(relative, fs::read(entry_path).expect("read snapshot file"));
            }
        }
    }
    snapshot
}

fn snapshot_contains(snapshot: &BTreeMap<PathBuf, Vec<u8>>, needle: &[u8]) -> bool {
    snapshot.values().any(|contents| {
        contents
            .windows(needle.len())
            .any(|window| window == needle)
    })
}

fn strip_ansi_control_sequences(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();
    while let Some(character) = chars.next() {
        if character == '\u{1b}' && chars.peek() == Some(&'[') {
            chars.next();
            for control in chars.by_ref() {
                if ('@'..='~').contains(&control) {
                    break;
                }
            }
        } else {
            output.push(character);
        }
    }
    output
}

fn seed_auto_permission_mode(data_dir: &Path) {
    let permissions = serde_json::json!({
        "schema_version": 1,
        "revision": 1,
        "data": {
            "whitelist": [],
            "enabled": true,
            "session_grant_duration_secs": 1800,
            "mode": "auto",
            "ask_rules": [],
            "durable_rules": []
        }
    });
    fs::write(
        data_dir.join("permissions.json"),
        serde_json::to_vec_pretty(&permissions).expect("serialize permission fixture"),
    )
    .expect("write permission fixture");
}

fn spawn_server(
    binary: &Path,
    data_dir: &Path,
    synthetic_home: &Path,
    jiandu_root: &Path,
    port: u16,
) -> ServerProcess {
    let log_path = data_dir.join("jiandu-data-dir-server.log");
    let stdout = fs::File::create(&log_path).expect("create server log");
    let stderr = stdout.try_clone().expect("clone server log");
    let child = Command::new(binary)
        .args([
            "serve",
            "--port",
            &port.to_string(),
            "--bind",
            "127.0.0.1",
            "--data-dir",
        ])
        .arg(data_dir)
        .current_dir(data_dir)
        .env("HOME", synthetic_home)
        .env(JIANDU_DATA_DIR_ENV, jiandu_root)
        .env("RUST_LOG", "warn,bamboo.memory=info")
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .spawn()
        .expect("spawn real bamboo server");

    ServerProcess { child, log_path }
}

async fn wait_until_healthy(server: &mut ServerProcess, base_url: &str) {
    let client = reqwest::Client::builder()
        .no_proxy()
        .build()
        .expect("HTTP client");
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(status) = server.child.try_wait().expect("poll server") {
            panic!(
                "bamboo server exited before becoming healthy ({status})\n{}",
                server.logs()
            );
        }

        match client.get(format!("{base_url}/api/v1/health")).send().await {
            Ok(response) if response.status().is_success() => return,
            _ if Instant::now() < deadline => tokio::time::sleep(Duration::from_millis(100)).await,
            result => panic!(
                "bamboo health endpoint did not become ready: {result:?}\n{}",
                server.logs()
            ),
        }
    }
}

#[tokio::test]
async fn real_server_writes_only_to_the_explicit_jiandu_root() {
    let data_dir = TempDir::new().expect("isolated Bamboo data directory");
    let synthetic_home = TempDir::new().expect("synthetic default home");
    let explicit_root = TempDir::new().expect("explicit Jiandu root");
    seed_auto_permission_mode(data_dir.path());

    let synthetic_default = synthetic_home.path().join(".jiandu");
    fs::create_dir_all(&synthetic_default).expect("create synthetic default root");
    fs::write(synthetic_default.join("sentinel"), b"must-remain-untouched")
        .expect("write synthetic default sentinel");
    let default_before = snapshot_files(&synthetic_default);

    let port = unused_loopback_port();
    let base_url = format!("http://127.0.0.1:{port}");
    let mut server = spawn_server(
        Path::new(env!("CARGO_BIN_EXE_bamboo")),
        data_dir.path(),
        synthetic_home.path(),
        explicit_root.path(),
        port,
    );
    wait_until_healthy(&mut server, &base_url).await;

    let marker = "explicit-root-smoke-marker-1135";
    let response = reqwest::Client::builder()
        .no_proxy()
        .build()
        .expect("HTTP client")
        .post(format!("{base_url}/api/v1/tools/execute"))
        .json(&serde_json::json!({
            "tool_name": "memory",
            "session_id": "jiandu-root-smoke-session",
            "parameters": [
                {"name": "action", "value": "session_replace"},
                {"name": "topic", "value": "acceptance"},
                {"name": "content", "value": marker}
            ]
        }))
        .send()
        .await
        .expect("execute memory write through the real server");
    let status = response.status();
    let body = response.text().await.expect("read tool response");
    assert!(
        status.is_success(),
        "memory write returned {status}: {body}\n{}",
        server.logs()
    );

    let explicit_after = snapshot_files(explicit_root.path());
    assert!(
        snapshot_contains(&explicit_after, marker.as_bytes()),
        "memory marker was not persisted under the explicit root: {explicit_after:?}"
    );
    assert_eq!(
        snapshot_files(&synthetic_default),
        default_before,
        "the canonical-default sentinel changed despite the explicit override"
    );

    let logs = strip_ansi_control_sequences(&server.logs());
    assert!(logs.contains("selected Jiandu data root"), "{logs}");
    assert!(logs.contains("mode=\"explicit\""), "{logs}");
    assert!(
        logs.contains(&explicit_root.path().display().to_string()),
        "{logs}"
    );
}

fn assert_invalid_override_fails_before_memory_access(value: &OsStr) {
    let data_dir = TempDir::new().expect("isolated Bamboo data directory");
    let synthetic_home = TempDir::new().expect("synthetic default home");
    let synthetic_default = synthetic_home.path().join(".jiandu");
    fs::create_dir_all(&synthetic_default).expect("create synthetic default root");
    fs::write(synthetic_default.join("sentinel"), b"must-remain-untouched")
        .expect("write synthetic default sentinel");
    let default_before = snapshot_files(&synthetic_default);

    let log_path = data_dir.path().join("invalid-jiandu-root.log");
    let stdout = fs::File::create(&log_path).expect("create server log");
    let stderr = stdout.try_clone().expect("clone server log");
    let mut child = Command::new(env!("CARGO_BIN_EXE_bamboo"))
        .args([
            "serve",
            "--port",
            &unused_loopback_port().to_string(),
            "--bind",
            "127.0.0.1",
            "--data-dir",
        ])
        .arg(data_dir.path())
        .current_dir(data_dir.path())
        .env("HOME", synthetic_home.path())
        .env(JIANDU_DATA_DIR_ENV, value)
        .env("RUST_LOG", "warn,bamboo.memory=info")
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .spawn()
        .expect("spawn real bamboo server");

    let deadline = Instant::now() + Duration::from_secs(10);
    let status = loop {
        if let Some(status) = child.try_wait().expect("poll invalid server") {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!(
                "server did not reject invalid {JIANDU_DATA_DIR_ENV}\n{}",
                fs::read_to_string(&log_path).unwrap_or_default()
            );
        }
        std::thread::sleep(Duration::from_millis(50));
    };

    assert!(!status.success(), "invalid override unexpectedly started");
    let logs = fs::read_to_string(&log_path).expect("read invalid-startup log");
    assert!(logs.contains(JIANDU_DATA_DIR_ENV), "{logs}");
    assert!(logs.contains("absolute path"), "{logs}");
    assert_eq!(
        snapshot_files(&synthetic_default),
        default_before,
        "invalid override accessed the canonical-default sentinel"
    );
}

#[test]
fn real_server_rejects_empty_and_relative_overrides_before_memory_access() {
    assert_invalid_override_fails_before_memory_access(OsStr::new(""));
    assert_invalid_override_fails_before_memory_access(OsStr::new("relative/jiandu"));
}
