//! Clean-checkout regression for the production `bamboo serve` frontend path.

use std::fs;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use tempfile::TempDir;

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

fn spawn_server(binary: &Path, data_dir: &Path, port: u16) -> ServerProcess {
    let log_path = data_dir.join("frontend-packaging-server.log");
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
        .env_remove("BAMBOO_FRONTEND_PACKAGE")
        .env("RUST_LOG", "warn")
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .spawn()
        .expect("spawn real bamboo server");

    ServerProcess { child, log_path }
}

#[tokio::test]
async fn real_bamboo_binary_serves_the_required_embedded_frontend() {
    let data_dir = TempDir::new().expect("isolated Bamboo data directory");
    let port = unused_loopback_port();
    let base_url = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::builder()
        .no_proxy()
        .build()
        .expect("HTTP client");
    let mut server = spawn_server(
        Path::new(env!("CARGO_BIN_EXE_bamboo")),
        data_dir.path(),
        port,
    );

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(status) = server.child.try_wait().expect("poll server") {
            panic!(
                "bamboo server exited before becoming healthy ({status})\n{}",
                server.logs()
            );
        }

        match client.get(format!("{base_url}/api/v1/health")).send().await {
            Ok(response) if response.status().is_success() => break,
            _ if Instant::now() < deadline => tokio::time::sleep(Duration::from_millis(100)).await,
            result => panic!(
                "bamboo health endpoint did not become ready: {result:?}\n{}",
                server.logs()
            ),
        }
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

    let committed_manifest: serde_json::Value = serde_json::from_slice(include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/crates/app/bamboo-server/frontend_package/frontend-manifest.json"
    )))
    .expect("parse committed embedded manifest");
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
