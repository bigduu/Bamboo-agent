//! Real loopback TLS regression tests using a throwaway synthetic identity.
use super::*;
use std::error::Error;
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
        let config = dir.path().join("root.cnf");
        let leaf_config = dir.path().join("leaf.cnf");
        let root = dir.path().join("root.pem");
        let root_key = dir.path().join("root-key.pem");
        let leaf = dir.path().join("leaf.pem");
        let csr = dir.path().join("leaf.csr");
        let cert = dir.path().join("cert.der");
        let key_pem = dir.path().join("key.pem");
        let key = dir.path().join("key.der");
        fs::write(
            &config,
            "[req]\nprompt=no\ndistinguished_name=dn\nx509_extensions=v3_req\n\
             [dn]\nCN=Synthetic Root\n\
             [v3_req]\nbasicConstraints=critical,CA:TRUE\n\
             keyUsage=critical,keyCertSign,cRLSign\nsubjectKeyIdentifier=hash\n",
        )
        .unwrap();
        fs::write(
            &leaf_config,
            "basicConstraints=critical,CA:FALSE\n\
             keyUsage=critical,digitalSignature,keyEncipherment\n\
             extendedKeyUsage=serverAuth\nsubjectAltName=DNS:synthetic.invalid\n",
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
            .arg(&root_key)
            .arg("-out")
            .arg(&root));
        run(Command::new("openssl")
            .args(["req", "-new", "-newkey", "rsa:2048", "-nodes", "-keyout"])
            .arg(&key_pem)
            .arg("-out")
            .arg(&csr)
            .args(["-subj", "/CN=synthetic.invalid"]));
        run(Command::new("openssl")
            .args(["x509", "-req", "-in"])
            .arg(&csr)
            .arg("-CA")
            .arg(&root)
            .arg("-CAkey")
            .arg(&root_key)
            .args(["-CAcreateserial", "-days", "1", "-sha256", "-extfile"])
            .arg(&leaf_config)
            .arg("-out")
            .arg(&leaf));
        run(Command::new("openssl")
            .args(["x509", "-in"])
            .arg(&leaf)
            .args(["-outform", "DER", "-out"])
            .arg(&cert));
        run(Command::new("openssl")
            .args(["pkcs8", "-topk8", "-nocrypt", "-in"])
            .arg(&key_pem)
            .args(["-outform", "DER", "-out"])
            .arg(&key));
        TestIdentity {
            cert: fs::read(cert).unwrap(),
            root_pem: fs::read(root).unwrap(),
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
        while let Ok((mut socket, _)) = listener.accept().await {
            seen.fetch_add(1, Ordering::SeqCst);
            if reset_handshake {
                // Make sure the request or ClientHello has reached the peer;
                // otherwise some platforms report an ordinary connect error.
                let mut first_byte = [0u8; 1];
                if socket.read_exact(&mut first_byte).await.is_err() {
                    continue;
                }
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

fn rustls_client(peer: &LocalPeer, trust: bool) -> reqwest::Client {
    let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
    let mut builder = reqwest::Client::builder()
        .no_proxy()
        .tls_backend_rustls()
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

fn assert_native_tls_error(error: LLMError) {
    let LLMError::Http(error) = error else {
        panic!("expected an HTTP transport error");
    };
    let mut source: Option<&(dyn Error + 'static)> = Some(&error);
    while let Some(cause) = source {
        if cause.downcast_ref::<native_tls::Error>().is_some() {
            return;
        }
        source = cause.source();
    }
    panic!("expected a native TLS error in the request error chain");
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
    let error = result.unwrap().expect_err("TLS failure must be returned");
    assert_native_tls_error(error);
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
    let error = result.unwrap().expect_err("TLS failure must be returned");
    assert_native_tls_error(error);
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert_eq!(peer.accepted.load(Ordering::SeqCst), 1);
}

async fn rustls_certificate_failure(trust: bool, host: &str) {
    let peer = peer(false).await;
    let client = rustls_client(&peer, trust);
    let url = format!("https://{}:{}/", host, peer.addr.port());
    let config = retry_config();
    let attempts = AtomicUsize::new(0);
    let result = tokio::time::timeout(
        Duration::from_millis(900),
        with_upstream_retry(
            UpstreamRetry::UntilCancelled,
            send_provider_request(&config, "synthetic-rustls", || {
                attempts.fetch_add(1, Ordering::SeqCst);
                client.get(&url)
            }),
        ),
    )
    .await;
    let error = result
        .expect("Rustls certificate failure kept retrying")
        .expect_err("Rustls certificate failure must be returned");
    let LLMError::Http(error) = error else {
        panic!("expected a Rustls request transport error");
    };
    let mut source: Option<&(dyn Error + 'static)> = Some(&error);
    let mut found = false;
    while let Some(cause) = source {
        if let Some(io) = cause.downcast_ref::<std::io::Error>() {
            // reqwest can wrap rustls' certificate error in an opaque IO
            // value. Its test-only display confirms the intended failure;
            // production classification never matches this text.
            if matches!(
                io.kind(),
                std::io::ErrorKind::Other | std::io::ErrorKind::InvalidData
            ) && io.to_string().contains("invalid peer certificate")
            {
                found = true;
            }
        }
        source = cause.source();
    }
    assert!(
        found,
        "expected a Rustls error inside the request error chain"
    );
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert_eq!(peer.accepted.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn rustls_untrusted_certificate_is_terminal() {
    rustls_certificate_failure(false, "synthetic.invalid").await;
}

#[tokio::test]
async fn rustls_hostname_mismatch_is_terminal() {
    rustls_certificate_failure(true, "127.0.0.1").await;
}

#[tokio::test]
async fn rustls_trusted_matching_certificate_succeeds() {
    let peer = peer(false).await;
    let client = rustls_client(&peer, true);
    let url = format!("https://synthetic.invalid:{}/", peer.addr.port());
    let config = retry_config();
    let response = tokio::time::timeout(
        Duration::from_secs(3),
        with_upstream_retry(
            UpstreamRetry::UntilCancelled,
            send_provider_request(&config, "synthetic-rustls", || client.get(&url)),
        ),
    )
    .await
    .expect("trusted Rustls peer should respond")
    .expect("trusted matching Rustls certificate should be accepted");
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(peer.accepted.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn anthropic_consumer_stops_on_untrusted_certificate() {
    let peer = peer(false).await;
    let client = client(&peer, false);
    let url = format!("https://synthetic.invalid:{}/", peer.addr.port());
    let config = retry_config();
    let attempts = AtomicUsize::new(0);
    let result = tokio::time::timeout(
        Duration::from_millis(900),
        with_upstream_retry(UpstreamRetry::UntilCancelled, async {
            let budget = InitialRequestBudget::new(&config);
            send_anthropic_with_retry(&config, &budget, || {
                attempts.fetch_add(1, Ordering::SeqCst);
                client.post(&url).body("synthetic request")
            })
            .await
        }),
    )
    .await;
    let error = match result {
        Ok(Err(error)) => error,
        Ok(Ok(_)) => panic!("untrusted certificate unexpectedly succeeded"),
        Err(_) => panic!("Anthropic consumer kept retrying a TLS validation error"),
    };
    assert_native_tls_error(error);
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert_eq!(peer.accepted.load(Ordering::SeqCst), 1);
}

struct SyntheticResolver {
    transient: bool,
    calls: Arc<AtomicUsize>,
}

impl reqwest::dns::Resolve for SyntheticResolver {
    fn resolve(&self, _: reqwest::dns::Name) -> reqwest::dns::Resolving {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let error: Box<dyn Error + Send + Sync> = if self.transient {
            Box::new(std::io::Error::from(std::io::ErrorKind::ConnectionRefused))
        } else {
            Box::new(std::fmt::Error)
        };
        Box::pin(async move { Err(error) })
    }
}

async fn synthetic_dns_failure(transient: bool) {
    let calls = Arc::new(AtomicUsize::new(0));
    let resolver = SyntheticResolver {
        transient,
        calls: calls.clone(),
    };
    let client = reqwest::Client::builder()
        .no_proxy()
        .dns_resolver(Arc::new(resolver))
        .build()
        .unwrap();
    let config = retry_config();
    let attempts = AtomicUsize::new(0);
    let result = tokio::time::timeout(
        Duration::from_millis(900),
        with_upstream_retry(
            UpstreamRetry::UntilCancelled,
            send_provider_request(&config, "synthetic-dns", || {
                attempts.fetch_add(1, Ordering::SeqCst);
                client.get("https://synthetic.invalid/")
            }),
        ),
    )
    .await;
    if transient {
        assert!(result.is_err(), "typed transient error should retry");
        assert!(attempts.load(Ordering::SeqCst) >= 2);
        assert!(calls.load(Ordering::SeqCst) >= 2);
    } else {
        let result = result.expect("unknown resolver failure kept retrying");
        let LLMError::Http(error) = result.expect_err("resolver must fail") else {
            panic!("expected resolver transport error");
        };
        assert!(error.is_connect());
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn dns_unknown_permanent_error_is_terminal() {
    synthetic_dns_failure(false).await;
}

#[tokio::test]
async fn dns_explicit_transient_error_remains_retryable() {
    synthetic_dns_failure(true).await;
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
