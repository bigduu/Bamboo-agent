//! Real loopback TLS regression tests using a throwaway synthetic identity.
use super::*;
use std::fs;
use std::net::SocketAddr;
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::TlsAcceptor;

struct TestIdentity {
    cert: Vec<u8>,
    root_pem: Vec<u8>,
    key: Vec<u8>,
}

fn identity() -> &'static TestIdentity {
    static IDENTITY: OnceLock<TestIdentity> = OnceLock::new();
    IDENTITY.get_or_init(|| {
        // Apple limits server certificate validity even for a custom trusted
        // root, so generate a short-lived identity instead of expiring a
        // committed fixture. The CI environments already require openssl.
        let dir = tempfile::tempdir().expect("temporary certificate directory");
        let config = dir.path().join("cert.cnf");
        let pem = dir.path().join("cert.pem");
        let cert = dir.path().join("cert.der");
        let key_pem = dir.path().join("key.pem");
        let key = dir.path().join("key.der");
        fs::write(
            &config,
            "[req]\nprompt=no\ndistinguished_name=dn\nx509_extensions=v3_req\n\
             [dn]\nCN=synthetic.invalid\n\
             [v3_req]\nbasicConstraints=critical,CA:TRUE\n\
             keyUsage=critical,digitalSignature,keyCertSign\n\
             extendedKeyUsage=serverAuth\nsubjectAltName=DNS:synthetic.invalid\n\
             subjectKeyIdentifier=hash\nauthorityKeyIdentifier=keyid:always\n",
        )
        .unwrap();
        let run = |command: &mut Command| {
            let output = command
                .output()
                .expect("openssl must be available for TLS tests");
            assert!(
                output.status.success(),
                "synthetic certificate command failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        };
        run(Command::new("openssl")
            .args([
                "req", "-x509", "-newkey", "rsa:2048", "-sha256", "-days", "1", "-nodes", "-config",
            ])
            .arg(&config)
            .args(["-extensions", "v3_req", "-keyout"])
            .arg(&key_pem)
            .arg("-out")
            .arg(&pem));
        run(Command::new("openssl")
            .args(["x509", "-in"])
            .arg(&pem)
            .args(["-outform", "DER", "-out"])
            .arg(&cert));
        run(Command::new("openssl")
            .args(["pkcs8", "-topk8", "-nocrypt", "-in"])
            .arg(&key_pem)
            .args(["-outform", "DER", "-out"])
            .arg(&key));
        TestIdentity {
            cert: fs::read(cert).unwrap(),
            root_pem: fs::read(pem).unwrap(),
            key: fs::read(key).unwrap(),
        }
    })
}

struct LocalPeer {
    addr: SocketAddr,
    accepted: Arc<AtomicUsize>,
    task: JoinHandle<()>,
}

impl Drop for LocalPeer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn peer(reset_handshake: bool) -> LocalPeer {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let accepted = Arc::new(AtomicUsize::new(0));
    let seen = accepted.clone();
    let config = ServerConfig::builder_with_provider(Arc::new(
        tokio_rustls::rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(
        vec![CertificateDer::from(identity().cert.clone())],
        PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(identity().key.clone())),
    )
    .unwrap();
    let acceptor = TlsAcceptor::from(Arc::new(config));
    let task = tokio::spawn(async move {
        while let Ok((socket, _)) = listener.accept().await {
            seen.fetch_add(1, Ordering::SeqCst);
            if reset_handshake {
                socket2::SockRef::from(&socket)
                    .set_linger(Some(Duration::ZERO))
                    .unwrap();
                drop(socket);
                continue;
            }
            let acceptor = acceptor.clone();
            tokio::spawn(async move {
                if let Ok(mut stream) = acceptor.accept(socket).await {
                    let mut request = [0u8; 2048];
                    if stream.read(&mut request).await.is_ok() {
                        let _ = stream
                            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
                            .await;
                        let _ = stream.flush().await;
                    }
                }
            });
        }
    });
    LocalPeer {
        addr,
        accepted,
        task,
    }
}

fn client(peer: &LocalPeer, trust: bool) -> reqwest::Client {
    let mut builder = reqwest::Client::builder()
        .no_proxy()
        .resolve("synthetic.invalid", peer.addr);
    if trust {
        builder = builder
            .add_root_certificate(reqwest::Certificate::from_pem(&identity().root_pem).unwrap());
    }
    builder.build().unwrap()
}

fn retry_config() -> RetryConfig {
    RetryConfig {
        max_attempts: 2,
        base_delay: Duration::from_millis(5),
        max_delay: Duration::from_millis(10),
    }
}

async fn terminal_certificate_failure(trust: bool, host: &str) {
    let peer = peer(false).await;
    let client = client(&peer, trust);
    let url = format!("https://{}:{}/", host, peer.addr.port());
    let config = retry_config();
    let attempts = AtomicUsize::new(0);
    let result = tokio::time::timeout(
        Duration::from_millis(900),
        with_upstream_retry(
            UpstreamRetry::UntilCancelled,
            send_provider_request(&config, "synthetic", || {
                attempts.fetch_add(1, Ordering::SeqCst);
                client.get(&url)
            }),
        ),
    )
    .await;
    assert!(
        result.is_ok(),
        "permanent TLS validation failure kept retrying"
    );
    assert!(result.unwrap().is_err());
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert_eq!(peer.accepted.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn untrusted_certificate_is_terminal_even_in_continuous_mode() {
    terminal_certificate_failure(false, "synthetic.invalid").await;
}

#[tokio::test]
async fn hostname_mismatch_is_terminal_even_in_continuous_mode() {
    terminal_certificate_failure(true, "127.0.0.1").await;
}

#[tokio::test]
async fn trusted_matching_certificate_still_succeeds() {
    let peer = peer(false).await;
    let client = client(&peer, true);
    let url = format!("https://synthetic.invalid:{}/", peer.addr.port());
    let config = retry_config();
    let direct = tokio::time::timeout(Duration::from_secs(3), client.get(&url).send())
        .await
        .expect("direct trusted request timed out")
        .expect("direct trusted handshake and response should succeed");
    assert_eq!(direct.status(), reqwest::StatusCode::OK);
    let response = tokio::time::timeout(
        Duration::from_secs(3),
        with_upstream_retry(
            UpstreamRetry::UntilCancelled,
            send_provider_request(&config, "synthetic", || client.get(&url)),
        ),
    )
    .await
    .expect("trusted peer should respond")
    .expect("trusted matching certificate should be accepted");
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(peer.accepted.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn opaque_handshake_failure_stops_instead_of_guessing_it_is_transient() {
    let peer = peer(true).await;
    let client = client(&peer, true);
    let url = format!("https://synthetic.invalid:{}/", peer.addr.port());
    let config = retry_config();
    let attempts = AtomicUsize::new(0);
    let result = tokio::time::timeout(
        Duration::from_millis(900),
        with_upstream_retry(
            UpstreamRetry::UntilCancelled,
            send_provider_request(&config, "synthetic", || {
                attempts.fetch_add(1, Ordering::SeqCst);
                client.get(&url)
            }),
        ),
    )
    .await;
    assert!(result.is_ok(), "opaque TLS failure must not keep retrying");
    assert!(result.unwrap().is_err());
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert_eq!(peer.accepted.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn plain_tcp_reset_before_headers_remains_retryable() {
    let peer = peer(true).await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let url = format!("http://127.0.0.1:{}/", peer.addr.port());
    let config = retry_config();
    let attempts = AtomicUsize::new(0);
    let result = tokio::time::timeout(
        Duration::from_millis(900),
        with_upstream_retry(
            UpstreamRetry::UntilCancelled,
            send_provider_request(&config, "synthetic", || {
                attempts.fetch_add(1, Ordering::SeqCst);
                client.get(&url)
            }),
        ),
    )
    .await;
    assert!(result.is_err(), "known TCP resets should remain retryable");
    assert!(attempts.load(Ordering::SeqCst) >= 2);
    assert!(peer.accepted.load(Ordering::SeqCst) >= 2);
}
