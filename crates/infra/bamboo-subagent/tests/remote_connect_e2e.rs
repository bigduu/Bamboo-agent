//! End-to-end (remote-actor-plan P1, #181): a parent reaches a *resident* worker
//! it did not spawn, via `ConnectLauncher` + `Placement::Remote` + a bearer
//! token — the "cross-machine connect" path, simulated deterministically on
//! loopback.
//!
//! The `ConnectLauncher` path uses `WsServer::bind_with_token` (plaintext +
//! token: the token gates the upgrade, and a loopback link keeps the launcher
//! tests deterministic). A separate `bind_tls_real_handshake_runs_to_terminal`
//! test mints a throwaway self-signed cert and drives a REAL `wss://` handshake
//! through `bind_tls` (TLS terminate -> WS upgrade -> bearer gate -> Echo ->
//! Terminal), so the TLS server path is exercised end-to-end and not just the
//! plaintext+token path.

use std::time::Duration;

use bamboo_subagent::executor::EchoExecutor;
use bamboo_subagent::launcher::{ConnectLauncher, WorkerLauncher};
use bamboo_subagent::proto::{ChildFrame, ParentFrame, RunSpec, TerminalStatus};
use bamboo_subagent::provision::{ChildIdentity, ExecutorSpec, Placement, ProvisionSpec};
use bamboo_subagent::transport::{ChildClient, WsServer};

const TOKEN: &str = "T-remote-secret";

/// Start a resident worker on loopback that requires `TOKEN`, returning its
/// `ws://` endpoint and the serve task handle.
async fn start_resident_worker() -> (String, tokio::task::JoinHandle<()>) {
    let server = WsServer::bind_with_token(
        (std::net::Ipv4Addr::LOCALHOST, 0).into(),
        Some(TOKEN.to_string()),
    )
    .await
    .expect("bind resident worker");
    let endpoint = server.ws_endpoint();
    let handle = tokio::spawn(async move {
        let _ = server.serve(std::sync::Arc::new(EchoExecutor)).await;
    });
    (endpoint, handle)
}

/// Build a parent-side spec whose placement points at the resident worker and
/// whose scoped secrets envelope carries the worker's bearer token.
fn remote_spec(endpoint: &str, token: Option<&str>) -> ProvisionSpec {
    let mut spec = ProvisionSpec::new(
        ChildIdentity {
            child_id: "remote-c1".into(),
            parent_id: Some("p1".into()),
            project_key: None,
            role: "remote-demo".into(),
            depth: 0,
        },
        ExecutorSpec::Echo,
        std::env::temp_dir()
            .join("bamboo-remote-e2e-fabric")
            .to_string_lossy()
            .into_owned(),
    );
    spec.placement = Placement::Remote {
        endpoint: endpoint.to_string(),
    };
    spec.secrets.worker_auth_token = token.map(|t| t.to_string());
    spec
}

/// Happy path: ConnectLauncher connects (no spawn), then a real Run streams back
/// a Terminal — byte-identical to the local path after the connect.
#[tokio::test]
async fn remote_connect_launches_runs_and_terminates() {
    let (endpoint, srv) = start_resident_worker().await;

    // Parent: ConnectLauncher with the correct token in the spec envelope.
    let spec = remote_spec(&endpoint, Some(TOKEN));
    let launched = ConnectLauncher
        .launch(&spec, Duration::from_secs(5))
        .await
        .expect("ConnectLauncher should reach the resident worker");

    // The synthesized record carries the remote endpoint + identity, NO process.
    assert_eq!(launched.record.agent_id, "remote-c1");
    assert_eq!(launched.record.endpoint, endpoint);
    assert_eq!(launched.pid(), None, "a remote worker owns no local pid");

    // Now drive a real run over a fresh authenticated connection.
    let mut client = ChildClient::connect_with_auth(&launched.record.endpoint, Some(TOKEN))
        .await
        .expect("connect to the remote endpoint with the bearer");
    client
        .send(ParentFrame::Run(RunSpec {
            assignment: "remote hello".into(),
            reasoning_effort: None,
            messages: Vec::new(),
        }))
        .await
        .unwrap();

    let mut terminal = None;
    while let Some(frame) = client.next_frame().await.unwrap() {
        if let ChildFrame::Terminal { status, result, .. } = frame {
            terminal = Some((status, result));
            break;
        }
    }
    let (status, result) = terminal.expect("a Terminal must come back from the remote worker");
    assert_eq!(status, TerminalStatus::Completed);
    assert_eq!(result.as_deref(), Some("echo: remote hello"));

    // kill() is a no-op for a remote worker; it must not panic or kill the server.
    launched.kill().await;

    let _ = client.close().await;
    srv.abort();
}

/// Negative: a WRONG token makes the connectivity probe fail — the launcher
/// errors and no work is dispatched (proves the bearer gate actually gates).
#[tokio::test]
async fn remote_connect_with_wrong_token_is_rejected() {
    let (endpoint, srv) = start_resident_worker().await;

    let spec = remote_spec(&endpoint, Some("WRONG-TOKEN"));
    let result = ConnectLauncher.launch(&spec, Duration::from_secs(5)).await;
    assert!(
        result.is_err(),
        "a wrong bearer must fail the connect, not launch"
    );

    srv.abort();
}

/// Negative: a MISSING token is likewise rejected by the gated worker.
#[tokio::test]
async fn remote_connect_with_missing_token_is_rejected() {
    let (endpoint, srv) = start_resident_worker().await;

    let spec = remote_spec(&endpoint, None);
    let result = ConnectLauncher.launch(&spec, Duration::from_secs(5)).await;
    assert!(
        result.is_err(),
        "a missing bearer must fail the connect against a token-gated worker"
    );

    // Direct connect attempt (bypassing the launcher) is also rejected.
    let direct = ChildClient::connect(&endpoint).await;
    assert!(direct.is_err(), "unauthenticated direct connect must fail");

    srv.abort();
}

/// Real TLS handshake against `bind_tls`: mints a throwaway self-signed cert,
/// serves with `WsServer::bind_tls`, then connects with a raw `tokio-rustls`
/// client (no-verify) + the WS upgrade carrying the bearer, runs Echo, and
/// asserts a Terminal. This exercises the actual `wss://` server path (TLS
/// terminate -> WS upgrade -> bearer gate) — not just the plaintext+token path.
/// Skips gracefully when openssl is unavailable.
#[tokio::test]
async fn bind_tls_real_handshake_runs_to_terminal() {
    use std::sync::Arc;

    let Some((cert, key, dir)) = mint_self_signed() else {
        eprintln!("skipping bind_tls_real_handshake: openssl unavailable");
        return;
    };

    let server = WsServer::bind_tls(
        (std::net::Ipv4Addr::LOCALHOST, 0).into(),
        &cert,
        &key,
        Some(TOKEN.to_string()),
    )
    .await
    .expect("bind_tls with a valid self-signed cert");
    let endpoint = server.ws_endpoint();
    assert!(
        endpoint.starts_with("wss://"),
        "TLS endpoint must be wss://"
    );
    let addr = server.local_addr();
    let srv = tokio::spawn(async move {
        let _ = server.serve(Arc::new(EchoExecutor)).await;
    });

    // Raw tokio-rustls client that accepts the self-signed cert (test-only).
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(NoVerify))
        .with_no_client_auth();
    let connector = tokio_rustls::TlsConnector::from(Arc::new(config));
    let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    let server_name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
    let tls = connector
        .connect(server_name, tcp)
        .await
        .expect("tls handshake");

    // WS upgrade over the TLS stream, presenting the bearer in the request.
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let mut request = format!("ws://localhost:{}/", addr.port())
        .into_client_request()
        .unwrap();
    request.headers_mut().insert(
        tokio_tungstenite::tungstenite::http::header::AUTHORIZATION,
        format!("Bearer {TOKEN}").parse().unwrap(),
    );
    let (ws, _resp) = tokio_tungstenite::client_async(request, tls)
        .await
        .expect("ws upgrade over tls with bearer");

    use futures_util::{SinkExt, StreamExt};
    let (mut tx, mut rx) = ws.split();
    let run = ParentFrame::Run(RunSpec {
        assignment: "tls hello".into(),
        reasoning_effort: None,
        messages: Vec::new(),
    });
    tx.send(tokio_tungstenite::tungstenite::Message::text(run.to_text()))
        .await
        .unwrap();

    let mut got_terminal = false;
    while let Some(msg) = rx.next().await {
        if let tokio_tungstenite::tungstenite::Message::Text(t) = msg.unwrap() {
            if let Ok(ChildFrame::Terminal { status, result, .. }) =
                ChildFrame::from_text(t.as_str())
            {
                assert_eq!(status, TerminalStatus::Completed);
                assert_eq!(result.as_deref(), Some("echo: tls hello"));
                got_terminal = true;
                break;
            }
        }
    }
    assert!(got_terminal, "expected a Terminal over the wss:// link");

    srv.abort();
    drop(dir); // keep the tempdir alive until here
}

/// A rustls verifier that accepts any server cert — TEST ONLY (self-signed).
#[derive(Debug)]
struct NoVerify;
impl rustls::client::danger::ServerCertVerifier for NoVerify {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// Mint a throwaway self-signed cert + PKCS#8 key via openssl. Returns the cert
/// path, key path, and the owning tempdir (kept alive by the caller). `None` if
/// openssl is unavailable.
fn mint_self_signed() -> Option<(std::path::PathBuf, std::path::PathBuf, tempfile::TempDir)> {
    let dir = tempfile::TempDir::new().ok()?;
    let cert = dir.path().join("cert.pem");
    let key = dir.path().join("key.pem");
    let status = std::process::Command::new("openssl")
        .args([
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-keyout",
            key.to_str().unwrap(),
            "-out",
            cert.to_str().unwrap(),
            "-days",
            "1",
            "-nodes",
            "-subj",
            "/CN=localhost",
        ])
        .status()
        .ok()?;
    if status.success() {
        Some((cert, key, dir))
    } else {
        None
    }
}
