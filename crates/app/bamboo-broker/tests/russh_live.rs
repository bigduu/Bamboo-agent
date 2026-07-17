//! Live integration test for [`RusshDeployer`] against a real Linux sshd.
//!
//! Gated behind `BAMBOO_RUSSH_LIVE=1` (needs an sshd container; not run in normal
//! `cargo test`). Bring the target up first:
//!
//! ```sh
//! docker build -t bamboo-russh-test <fixture>   # alpine + openssh + netcat, user deploy/testpass123
//! docker run -d --name bamboo-russh -p 2222:22 bamboo-russh-test
//! BAMBOO_RUSSH_LIVE=1 cargo test -p bamboo-broker --test russh_live -- --nocapture
//! ```
//!
//! What it proves (the whole russh path end-to-end, no Linux bamboo binary
//! needed): connect + host-key TOFU + password auth + SFTP upload + chmod + the
//! reverse tunnel + worker launch. The "binary" we upload is a tiny shell script
//! that, when launched, dials the tunnel mouth (`127.0.0.1:<broker>`) and
//! announces its `--id`; the host-side dummy broker must receive that line —
//! which can only happen if the reverse tunnel bridged container → host.

use std::time::Duration;

use bamboo_broker::{AgentDeployment, Deployer, RusshAuth, RusshDeployer, UploadSpec};
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

#[tokio::test]
async fn russh_deploys_through_reverse_tunnel() {
    if std::env::var("BAMBOO_RUSSH_LIVE").is_err() {
        eprintln!("skipping: set BAMBOO_RUSSH_LIVE=1 (needs the sshd container)");
        return;
    }

    let host = env_or("RUSSH_HOST", "127.0.0.1");
    let port: u16 = env_or("RUSSH_PORT", "2222").parse().unwrap();
    let user = env_or("RUSSH_USER", "deploy");
    let pass = env_or("RUSSH_PASS", "testpass123");
    let broker_port: u16 = env_or("RUSSH_BROKER_PORT", "9600").parse().unwrap();

    // 1. Host-side dummy "broker": a listener that captures the first line.
    let listener = TcpListener::bind(("127.0.0.1", broker_port))
        .await
        .expect("bind dummy broker");
    let recv = tokio::spawn(async move {
        let (mut sock, _) = tokio::time::timeout(Duration::from_secs(20), listener.accept())
            .await
            .expect("tunnel connection within 20s")
            .expect("accept");
        let mut buf = vec![0u8; 256];
        let n = sock.read(&mut buf).await.unwrap_or(0);
        String::from_utf8_lossy(&buf[..n]).to_string()
    });

    // 2. The "binary": a probe script that dials the tunnel mouth and announces
    //    its --id. (Runs in the container; reaches the host only via the tunnel.)
    let script = format!(
        "#!/bin/sh\n\
         id=\"\"\n\
         while [ $# -gt 0 ]; do [ \"$1\" = \"--id\" ] && id=\"$2\"; shift; done\n\
         printf 'WORKER_UP id=%s\\n' \"$id\" | nc -w 3 127.0.0.1 {broker_port}\n\
         sleep 30\n"
    );
    let local = std::env::temp_dir().join("bamboo-russh-probe.sh");
    tokio::fs::write(&local, script).await.expect("write probe");

    // 3. Deploy via russh (TOFU host key, password auth, SFTP upload, tunnel).
    let deployer = RusshDeployer::new(host, port, user, RusshAuth::Password(pass)).with_upload(
        Some(UploadSpec {
            local_path: local.to_string_lossy().to_string(),
            remote_path: "/home/deploy/bamboo-probe".to_string(),
        }),
    );

    let deployment = AgentDeployment {
        id: "node-russhtest".to_string(),
        role: Some("worker".to_string()),
        broker_endpoint: format!("ws://127.0.0.1:{broker_port}"),
        token: "tok".to_string(),
        model: None,
        workspace: None,
        echo: true,
        mcp_proxy: None,
        log_path: None,
        spec_json: None,
        tls_ca_cert: None,
    };

    let handle = deployer.deploy(&deployment).await.expect("russh deploy");

    // 4. The reverse tunnel must carry the worker's announcement home.
    let got = recv.await.expect("recv task");
    assert!(
        got.contains("WORKER_UP id=node-russhtest"),
        "expected the worker to announce via the reverse tunnel, got: {got:?}"
    );

    // 5. TOFU recorded a fingerprint.
    let fp = deployer.observed_fingerprint().await;
    assert!(
        fp.as_deref()
            .map(|s| s.starts_with("SHA256:"))
            .unwrap_or(false),
        "host-key fingerprint should be recorded, got: {fp:?}"
    );

    handle.shutdown().await;
}
