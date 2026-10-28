//! Real loopback TLS regression tests. The checked-in identity is test-only.
use super::*;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::TlsAcceptor;

const CERT: &[u8] = include_bytes!("tls_test_fixtures/test-only-cert.der");
const ROOT_PEM: &[u8] = include_bytes!("tls_test_fixtures/test-only-cert.pem");
const KEY: &[u8] = include_bytes!("tls_test_fixtures/test-only-key.der");

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
        vec![CertificateDer::from(CERT.to_vec())],
        PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(KEY.to_vec())),
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
        builder = builder.add_root_certificate(reqwest::Certificate::from_pem(ROOT_PEM).unwrap());
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
    assert_eq!(peer.accepted.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn connection_reset_during_handshake_remains_retryable() {
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
    assert!(
        result.is_err(),
        "temporary handshake reset must keep retrying"
    );
    assert!(attempts.load(Ordering::SeqCst) >= 2);
    assert!(peer.accepted.load(Ordering::SeqCst) >= 2);
}
