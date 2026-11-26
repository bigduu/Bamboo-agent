//! Clean-checkout regression for the production `bamboo serve` frontend path.

use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use tempfile::TempDir;
use tokio::time::Instant;

const STARTUP_BUDGET: Duration = Duration::from_secs(30);
const HOST_LOG_TAIL_BYTES: u64 = 16 * 1024;

#[derive(Debug, Default)]
struct HealthEvidence {
    attempts: usize,
    last_attempt_start_ms: Option<u128>,
    last_completed_ms: Option<u128>,
    last_result: Option<String>,
}

struct ServerProcess {
    child: Child,
    log_path: PathBuf,
}

impl ServerProcess {
    fn logs(&self) -> String {
        let tail = (|| -> std::io::Result<String> {
            let mut file = fs::File::open(&self.log_path)?;
            let offset = file.metadata()?.len().saturating_sub(HOST_LOG_TAIL_BYTES);
            file.seek(SeekFrom::Start(offset))?;
            let mut bytes = Vec::new();
            file.take(HOST_LOG_TAIL_BYTES).read_to_end(&mut bytes)?;
            let tail = String::from_utf8_lossy(&bytes);
            Ok(if offset == 0 {
                tail.into_owned()
            } else {
                format!("<first {offset} log bytes omitted>\n{tail}")
            })
        })();
        tail.unwrap_or_else(|error| format!("<unreadable: {error}>"))
    }

    fn startup_failure(
        &mut self,
        reason: &str,
        started: Instant,
        endpoint: &str,
        health: &HealthEvidence,
        data_dir: &Path,
        manifest: &serde_json::Value,
    ) -> String {
        let elapsed_ms = started.elapsed().as_millis();
        let process_state = match self.child.try_wait() {
            Ok(None) => "running".to_owned(),
            Ok(Some(status)) => format!("exited ({status})"),
            Err(error) => format!("unavailable: {error}"),
        };
        let version = manifest["frontend_version"].as_str().unwrap_or_default();
        let entry = manifest["entry"].as_str().unwrap_or_default();
        let staging = data_dir.join(format!(".frontend-tmp-{}", version.replace('/', "-")));
        let published = data_dir.join("frontend");
        let logs = self.logs();
        let stage = if logs.contains(&format!("Unified server running on {endpoint}")) {
            "owned listener announced"
        } else if logs.contains("Refreshed Bamboo frontend assets")
            || logs.contains("Using existing Bamboo frontend assets")
        {
            "frontend resolved; listener announcement not observed"
        } else if published.join(".frontend-manifest.json").is_file() {
            "published manifest observed; listener announcement not observed"
        } else if staging.is_dir() {
            "staging directory observed; frontend resolution not confirmed"
        } else {
            "no frontend staging, publication, or listener landmark observed"
        };
        format!(
            "{reason}\nEmbedded frontend startup sample after readiness failure: elapsed_ms={elapsed_ms} launch_pid={} state={process_state} health_url={endpoint}/api/v1/health stage={stage:?} health={health:?}\n\
             Frontend files sampled after readiness failure: staging={} staged_entry={} staged_manifest={} published_entry={} published_manifest={}\n\
             Owned Host log tail (at most {HOST_LOG_TAIL_BYTES} bytes) from {}:\n{logs}",
            self.child.id(),
            path_fact(&staging),
            path_fact(&staging.join(entry)),
            path_fact(&staging.join(".frontend-manifest.json")),
            path_fact(&published.join(entry)),
            path_fact(&published.join(".frontend-manifest.json")),
            self.log_path.display(),
        )
    }
}

fn path_fact(path: &Path) -> String {
    match fs::metadata(path) {
        Ok(metadata) if metadata.is_file() => format!("file ({} bytes)", metadata.len()),
        Ok(metadata) if metadata.is_dir() => "directory".into(),
        Ok(_) => "other".into(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => "absent".into(),
        Err(error) => format!("unavailable: {error}"),
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

fn spawn_server(binary: &Path, data_dir: &Path, port: u16) -> ServerProcess {
    let log_path = data_dir.join("frontend-packaging-server.log");
    let stdout = fs::File::create(&log_path).expect("create server log");
    let stderr = stdout.try_clone().expect("clone server log");
    let profile = data_dir.join("fixture-home");
    let temp = profile.join("temp");
    fs::create_dir_all(&temp).expect("create fixture home and temp directory");
    let mut command = Command::new(binary);
    command
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
        .env_clear()
        .env("HOME", &profile)
        .env("USERPROFILE", &profile)
        .env("XDG_CONFIG_HOME", profile.join("config"))
        .env("XDG_DATA_HOME", profile.join("data"))
        .env("XDG_CACHE_HOME", profile.join("cache"))
        .env("XDG_STATE_HOME", profile.join("state"))
        .env("APPDATA", profile.join("AppData/Roaming"))
        .env("LOCALAPPDATA", profile.join("AppData/Local"))
        .env("TMPDIR", &temp)
        .env("TMP", &temp)
        .env("TEMP", &temp)
        .env("BAMBOO_DATA_DIR", data_dir)
        .env("BAMBOO_JIANDU_DATA_DIR", data_dir.join("jiandu"))
        .env("RUST_LOG", "warn,bamboo_server::server::entrypoints=info")
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr));
    #[cfg(windows)]
    if let Some(root) = std::env::var_os("SystemRoot") {
        command.env("SystemRoot", root);
    }
    let child = command.spawn().expect("spawn real bamboo server");

    ServerProcess { child, log_path }
}

#[tokio::test]
async fn real_bamboo_binary_serves_the_required_embedded_frontend() {
    let committed_manifest: serde_json::Value = serde_json::from_slice(include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/crates/app/bamboo-server/frontend_package/frontend-manifest.json"
    )))
    .expect("parse committed embedded manifest");
    let data_dir = TempDir::new().expect("isolated Bamboo data directory");
    let port = unused_loopback_port();
    let base_url = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("HTTP client");
    let mut server = spawn_server(
        Path::new(env!("CARGO_BIN_EXE_bamboo")),
        data_dir.path(),
        port,
    );

    let started = Instant::now();
    let deadline = started + STARTUP_BUDGET;
    let endpoint = format!("{base_url}/api/v1/health");
    let mut health = HealthEvidence::default();
    let ready = tokio::time::timeout_at(deadline, async {
        loop {
            match server.child.try_wait() {
                Ok(None) => {}
                Ok(Some(status)) => {
                    return Err(format!(
                        "bamboo server exited before becoming healthy ({status})"
                    ))
                }
                Err(error) => return Err(format!("could not poll owned bamboo server: {error}")),
            }
            health.attempts += 1;
            health.last_attempt_start_ms = Some(started.elapsed().as_millis());
            let response = client.get(&endpoint).send().await;
            let completed_ms = started.elapsed().as_millis();
            let (successful, status) = match response {
                Ok(response) => (
                    response.status().is_success(),
                    format!("HTTP {}", response.status()),
                ),
                Err(error) => {
                    let mut description = format!("{error:?}");
                    if description.len() > 1024 {
                        description.truncate(description.floor_char_boundary(1024));
                        description.push_str(" (truncated)");
                    }
                    (false, description)
                }
            };
            health.last_completed_ms = Some(completed_ms);
            health.last_result = Some(status);
            if successful {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    let failure = match ready {
        Ok(Ok(())) if Instant::now() < deadline => None,
        Ok(Err(reason)) => Some(reason),
        Ok(Ok(())) | Err(_) => {
            Some("bamboo health endpoint did not become ready within 30s".into())
        }
    };
    if let Some(reason) = failure {
        panic!(
            "{}",
            server.startup_failure(
                &reason,
                started,
                &base_url,
                &health,
                data_dir.path(),
                &committed_manifest,
            )
        );
    }

    let root_response = client
        .get(format!("{base_url}/"))
        .send()
        .await
        .expect("request embedded frontend root");
    assert!(
        root_response.status().is_success(),
        "frontend root returned {}\n{}",
        root_response.status(),
        server.logs()
    );
    assert_eq!(
        root_response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(|value| value.split(';').next().unwrap_or(value)),
        Some("text/html")
    );
    let root_html = root_response.text().await.expect("read frontend root");
    assert!(
        !root_html.trim().is_empty(),
        "embedded frontend entry was empty"
    );

    let extracted_manifest_path = data_dir.path().join("frontend/.frontend-manifest.json");
    let extracted_manifest: serde_json::Value = serde_json::from_slice(
        &fs::read(&extracted_manifest_path).expect("read extracted frontend manifest"),
    )
    .expect("parse extracted frontend manifest");
    assert_eq!(extracted_manifest, committed_manifest);

    let entry = committed_manifest["entry"]
        .as_str()
        .expect("manifest entry string");
    assert!(data_dir.path().join("frontend").join(entry).is_file());
}
