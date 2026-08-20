//! Live integration test for [`RusshDeployer`] against a real Linux sshd.
//!
//! The test is explicitly ignored because it needs the hermetic Docker fixture.
//! Run it locally with `scripts/run-russh-live.sh`; the protected Linux `Test`
//! job runs that same command on every pull request.
//!
//! What it proves (the whole russh path end-to-end, no Linux bamboo binary
//! needed): connect + host-key TOFU and pin rejection + public-key auth + SFTP
//! upload + chmod + reverse tunnel + worker launch and graceful cleanup. The
//! uploaded "binary" is a tiny shell script that announces both startup and
//! SIGTERM shutdown through the tunnel.

use std::path::PathBuf;
use std::time::Duration;

use bamboo_broker::{AgentDeployment, Deployer, RusshAuth, RusshDeployer, UploadSpec};
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use tokio::sync::mpsc;

const CONTRACT_TIMEOUT: Duration = Duration::from_secs(60);

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn fixture_auth() -> RusshAuth {
    if let Some(path) = std::env::var_os("RUSSH_KEY_PATH") {
        let path = PathBuf::from(path);
        let pem = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("read RUSSH_KEY_PATH '{}': {e}", path.display()));
        return RusshAuth::PrivateKey {
            pem,
            passphrase: None,
        };
    }

    let password = std::env::var("RUSSH_PASS").expect(
        "set RUSSH_KEY_PATH (recommended) or RUSSH_PASS when running the ignored live test",
    );
    RusshAuth::Password(password)
}

async fn receive_announcements(listener: TcpListener, tx: mpsc::Sender<String>) {
    for ordinal in 1..=2 {
        let (mut socket, _) = tokio::time::timeout(Duration::from_secs(20), listener.accept())
            .await
            .unwrap_or_else(|_| panic!("announcement {ordinal} did not reach the tunnel in 20s"))
            .unwrap_or_else(|e| panic!("accept announcement {ordinal}: {e}"));
        let mut message = String::new();
        tokio::time::timeout(Duration::from_secs(5), socket.read_to_string(&mut message))
            .await
            .unwrap_or_else(|_| panic!("announcement {ordinal} did not close in 5s"))
            .unwrap_or_else(|e| panic!("read announcement {ordinal}: {e}"));
        tx.send(message)
            .await
            .unwrap_or_else(|_| panic!("announcement receiver dropped at message {ordinal}"));
    }
}

async fn run_transport_contract() {
    let host = env_or("RUSSH_HOST", "127.0.0.1");
    let port: u16 = env_or("RUSSH_PORT", "2222")
        .parse()
        .expect("RUSSH_PORT must be a valid u16");
    let user = env_or("RUSSH_USER", "deploy");
    let worker_id = env_or(
        "RUSSH_WORKER_ID",
        &format!("node-russh-live-{}", std::process::id()),
    );

    // Bind port 0 so concurrent tests and developer services cannot collide.
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("bind dummy broker");
    let broker_port = listener.local_addr().expect("dummy broker address").port();
    let (announcement_tx, mut announcement_rx) = mpsc::channel(2);
    let receiver = tokio::spawn(receive_announcements(listener, announcement_tx));

    // First contact is TOFU: authentication succeeds and the exact host key is
    // recorded for durable pinning by the caller.
    let tofu = RusshDeployer::new(&host, port, &user, fixture_auth());
    let platform = tofu.preflight().await.expect("TOFU preflight");
    assert!(
        platform.starts_with("Linux "),
        "fixture preflight must execute uname on Linux, got {platform:?}"
    );
    let fingerprint = tofu
        .observed_fingerprint()
        .await
        .expect("TOFU must record a SHA-256 host-key fingerprint");
    assert!(
        fingerprint.starts_with("SHA256:"),
        "unexpected host-key fingerprint: {fingerprint:?}"
    );

    // A changed pin must fail closed before a deployment or upload can occur.
    let wrong_pin = RusshDeployer::new(&host, port, &user, fixture_auth()).with_fingerprint(Some(
        "SHA256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".into(),
    ));
    assert!(
        wrong_pin.preflight().await.is_err(),
        "a mismatched host-key pin must be rejected"
    );
    assert_eq!(
        wrong_pin.observed_fingerprint().await.as_deref(),
        Some(fingerprint.as_str()),
        "the rejected connection must still report the actually observed key"
    );

    let temp = tempfile::tempdir().expect("create probe tempdir");
    let local_probe = temp.path().join("bamboo-russh-probe.sh");
    let probe = format!(
        "#!/bin/sh\n\
         id=\"\"\n\
         while [ $# -gt 0 ]; do\n\
           if [ \"$1\" = \"--id\" ]; then id=\"$2\"; shift; fi\n\
           shift\n\
         done\n\
         announce() {{ printf '%s id=%s\\n' \"$1\" \"$id\" | nc -N -w 3 127.0.0.1 {broker_port}; }}\n\
         on_term() {{ announce WORKER_DOWN; exit 0; }}\n\
         trap on_term TERM INT\n\
         announce WORKER_UP\n\
         while :; do sleep 1 & wait $!; done\n"
    );
    tokio::fs::write(&local_probe, probe)
        .await
        .expect("write probe");
    let remote_probe = format!("/home/deploy/bamboo-probe-{worker_id}");

    // Reconnect with the TOFU result pinned, then exercise SFTP upload, chmod,
    // reverse forwarding, and remote launch. Successful execution of the probe
    // is also the chmod assertion.
    let deployer = RusshDeployer::new(host, port, user, fixture_auth())
        .with_fingerprint(Some(fingerprint.clone()))
        .with_upload(Some(UploadSpec {
            local_path: local_probe.to_string_lossy().into_owned(),
            remote_path: remote_probe,
        }));
    let deployment = AgentDeployment {
        id: worker_id.clone(),
        role: Some("worker".to_string()),
        broker_endpoint: format!("ws://127.0.0.1:{broker_port}"),
        token: "fixture-local-token".to_string(),
        model: None,
        workspace: None,
        echo: true,
        mcp_proxy: None,
        log_path: None,
        spec_json: None,
        tls_ca_cert: None,
    };

    let handle = deployer.deploy(&deployment).await.expect("russh deploy");
    assert_eq!(
        deployer.observed_fingerprint().await.as_deref(),
        Some(fingerprint.as_str()),
        "the pinned deployment must observe the same host key"
    );

    let up = tokio::time::timeout(Duration::from_secs(20), announcement_rx.recv())
        .await
        .expect("worker startup announcement within 20s")
        .expect("startup announcement channel");
    assert_eq!(up.trim(), format!("WORKER_UP id={worker_id}"));

    // Shutdown must remain bounded, deliver SIGTERM to the remote worker, and
    // keep the reverse tunnel alive long enough for the worker's trap to report
    // that it exited. This proves launch cleanup rather than merely destroying
    // the fixture container afterward.
    let shutdown_started = tokio::time::Instant::now();
    handle.shutdown_with_timeout(Duration::from_secs(5)).await;
    assert!(
        shutdown_started.elapsed() < Duration::from_secs(4),
        "the cooperative worker must exit before the five-second hard-kill deadline"
    );
    let down = tokio::time::timeout(Duration::from_secs(10), announcement_rx.recv())
        .await
        .expect("worker shutdown announcement within 10s")
        .expect("shutdown announcement channel");
    assert_eq!(down.trim(), format!("WORKER_DOWN id={worker_id}"));

    receiver.await.expect("announcement receiver task");
}

#[tokio::test]
#[ignore = "requires the hermetic sshd fixture; run scripts/run-russh-live.sh"]
async fn russh_deploys_through_reverse_tunnel() {
    tokio::time::timeout(CONTRACT_TIMEOUT, run_transport_contract())
        .await
        .expect("real SSH/SFTP transport contract exceeded 60 seconds");
}
