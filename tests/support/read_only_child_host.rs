//! Real CLI Host fixture for the built-in read-only Child route.

use bamboo_agent_core::storage::Storage;
use bamboo_domain::Session;
use bamboo_storage::SessionStoreV2;
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

pub struct Host {
    process: Child,
    data: PathBuf,
    base: String,
    client: reqwest::Client,
}

impl Host {
    pub async fn start(data: &Path) -> Self {
        let port = std::net::TcpListener::bind(("127.0.0.1", 0))
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let log_path = data.join("host.log");
        let log = std::fs::File::create(&log_path).unwrap();
        let process = Command::new(env!("CARGO_BIN_EXE_bamboo"))
            .args([
                "serve",
                "--bind",
                "127.0.0.1",
                "--port",
                &port.to_string(),
                "--data-dir",
            ])
            .arg(data)
            .current_dir(data)
            .env("HOME", data.join("home"))
            .env("BAMBOO_JIANDU_DATA_DIR", data.join("jiandu"))
            .env("RUST_LOG", "info")
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log))
            .spawn()
            .unwrap();
        let mut host = Self {
            process,
            data: data.into(),
            base: format!("http://127.0.0.1:{port}/api/v1"),
            client: reqwest::Client::builder()
                .no_proxy()
                .timeout(Duration::from_secs(15))
                .build()
                .unwrap(),
        };
        let listener = format!("Unified server running on http://127.0.0.1:{port}");
        tokio::time::timeout(Duration::from_secs(120), async {
            loop {
                let log = std::fs::read(&log_path).unwrap();
                let log = String::from_utf8_lossy(&log);
                assert!(
                    host.process.try_wait().unwrap().is_none(),
                    "CLI Host exited: {log}",
                );
                if log.lines().any(|line| line.ends_with(&listener))
                    && host
                        .client
                        .get(format!("{}/health", host.base))
                        .send()
                        .await
                        .is_ok_and(|response| response.status().is_success())
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("CLI Host must bind its own listener and become ready");
        host
    }

    pub async fn post(&self, route: &str, body: Value) -> reqwest::Response {
        self.client
            .post(format!("{}/{}", self.base, route))
            .json(&body)
            .send()
            .await
            .unwrap()
    }

    pub async fn session(&self, id: &str) -> Session {
        SessionStoreV2::new(self.data.clone())
            .await
            .unwrap()
            .load_session(id)
            .await
            .unwrap()
            .unwrap()
    }
}

pub fn reported_result(summary: &str) -> Value {
    let report = json!({
        "version": 1,
        "outcome": "completed",
        "summary": summary,
        "reported_evidence": [],
        "reported_verification": [],
        "proposals": [],
        "blockers": [],
        "open_decisions": [],
    });
    json!({"content": report.to_string()})
}

impl Drop for Host {
    fn drop(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
    }
}
