use std::{
    net::TcpListener,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use actix_web::{web, App, HttpRequest, HttpResponse};
use futures::{SinkExt, StreamExt};
use native_tls::TlsConnector;
use serde_json::{json, Value};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::Notify,
};
use tokio_native_tls::TlsConnector as TokioTlsConnector;
use tokio_tungstenite::{
    connect_async_tls_with_config,
    tungstenite::{client::IntoClientRequest, Message},
    Connector, MaybeTlsStream,
};

use super::h1::build_h1_server;
use super::tls::{build_rustls_config, test_support::gen_self_signed};
use super::WebService;
use crate::{routes::configure_routes, AppState};
use bamboo_config::TlsConfig;

#[test]
fn test_web_service_lifecycle() {
    let temp_dir = tempfile::tempdir().expect("failed to create temp dir");
    let ws = WebService::new(temp_dir.path().to_path_buf());
    assert!(!ws.is_running());
}

fn test_listener() -> (TcpListener, u16) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("ephemeral test listener");
    listener
        .set_nonblocking(true)
        .expect("listener must be nonblocking");
    let port = listener.local_addr().expect("listener address").port();
    (listener, port)
}

async fn stop_server(
    handle: actix_web::dev::ServerHandle,
    join: tokio::task::JoinHandle<std::io::Result<()>>,
) {
    handle.stop(false).await;
    join.await
        .expect("server task joins")
        .expect("server exits cleanly");
}

async fn stop_server_gracefully(
    handle: actix_web::dev::ServerHandle,
    join: tokio::task::JoinHandle<std::io::Result<()>>,
) {
    if tokio::time::timeout(Duration::from_secs(3), handle.stop(true))
        .await
        .is_err()
    {
        join.abort();
        let _ = join.await;
        panic!("graceful stop waited for the worker shutdown timeout");
    }
    join.await
        .expect("server task joins")
        .expect("server exits cleanly");
}

async fn read_http_response_head<R>(stream: &mut R) -> Vec<u8>
where
    R: AsyncRead + Unpin,
{
    let mut response = Vec::new();
    let mut chunk = [0_u8; 1024];

    tokio::time::timeout(Duration::from_secs(3), async {
        while !response.windows(4).any(|window| window == b"\r\n\r\n") {
            let read = stream
                .read(&mut chunk)
                .await
                .expect("read HTTP response head");
            assert_ne!(read, 0, "connection closed before the response head");
            response.extend_from_slice(&chunk[..read]);
        }
    })
    .await
    .expect("HTTP response head arrives before timeout");

    response
}

#[derive(Default)]
struct DrainState {
    calls: AtomicUsize,
    started: Notify,
    release: Notify,
}

async fn blocking_drain_handler(state: web::Data<DrainState>) -> HttpResponse {
    state.calls.fetch_add(1, Ordering::SeqCst);
    state.started.notify_one();
    state.release.notified().await;
    HttpResponse::Ok().body("completed-current-request")
}

#[actix_web::test]
async fn graceful_stop_closes_idle_keep_alive_connection_without_worker_timeout() {
    let (listener, port) = test_listener();
    let server = build_h1_server(
        || {
            App::new().route(
                "/idle",
                web::get().to(|| async { HttpResponse::Ok().finish() }),
            )
        },
        vec![listener],
        1,
        None,
    )
    .expect("build plaintext H1 server");
    let handle = server.handle();
    let join = tokio::spawn(server);

    let mut stream = TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("connect idle keep-alive client");
    stream
        .write_all(b"GET /idle HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive\r\n\r\n")
        .await
        .expect("send idle keep-alive request");
    let response = read_http_response_head(&mut stream).await;
    assert!(
        response.starts_with(b"HTTP/1.1 200"),
        "unexpected HTTP response: {}",
        String::from_utf8_lossy(&response)
    );

    if tokio::time::timeout(Duration::from_secs(3), handle.stop(true))
        .await
        .is_err()
    {
        join.abort();
        let _ = join.await;
        panic!("graceful stop waited for the worker shutdown timeout");
    }

    let mut trailing = [0_u8; 1];
    let read = tokio::time::timeout(Duration::from_secs(1), stream.read(&mut trailing))
        .await
        .expect("graceful drain closes the idle connection")
        .expect("read idle connection EOF");
    assert_eq!(read, 0, "idle connection remained open after graceful stop");
    join.await
        .expect("server task joins")
        .expect("server exits cleanly");
}

#[actix_web::test]
async fn graceful_stop_finishes_current_request_without_starting_buffered_request() {
    let state = Arc::new(DrainState::default());
    let state_for_factory = state.clone();
    let (listener, port) = test_listener();
    let server = build_h1_server(
        move || {
            App::new()
                .app_data(web::Data::from(state_for_factory.clone()))
                .route("/drain", web::get().to(blocking_drain_handler))
        },
        vec![listener],
        1,
        None,
    )
    .expect("build plaintext H1 server");
    let handle = server.handle();
    let join = tokio::spawn(server);

    let mut stream = TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("connect pipelined keep-alive client");
    stream
        .write_all(
            b"GET /drain HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive\r\n\r\n\
              GET /drain HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
        .await
        .expect("buffer two HTTP requests");
    tokio::time::timeout(Duration::from_secs(3), state.started.notified())
        .await
        .expect("current request starts before timeout");

    // `stop(true)` queues the server command before returning its completion
    // future. Give the worker a scheduling turn to deliver the drain signal
    // while the first request remains in flight and the second is buffered.
    let graceful_stop = handle.stop(true);
    tokio::time::sleep(Duration::from_millis(100)).await;
    state.release.notify_one();

    if tokio::time::timeout(Duration::from_secs(3), graceful_stop)
        .await
        .is_err()
    {
        join.abort();
        let _ = join.await;
        panic!("graceful stop started a buffered request or waited for worker timeout");
    }

    let mut response = Vec::new();
    tokio::time::timeout(Duration::from_secs(1), stream.read_to_end(&mut response))
        .await
        .expect("drained connection closes after current response")
        .expect("read drained HTTP response");
    assert_eq!(
        response
            .windows(b"HTTP/1.1 200".len())
            .filter(|window| *window == b"HTTP/1.1 200")
            .count(),
        1,
        "only the in-flight request may receive a response: {}",
        String::from_utf8_lossy(&response)
    );
    assert!(
        response
            .windows(b"completed-current-request".len())
            .any(|window| window == b"completed-current-request"),
        "the in-flight request must complete before the connection drains"
    );
    assert_eq!(
        state.calls.load(Ordering::SeqCst),
        1,
        "the buffered request must not enter the application service"
    );
    join.await
        .expect("server task joins")
        .expect("server exits cleanly");
}

#[actix_web::test]
async fn plaintext_server_is_http11_and_reports_insecure_context() {
    async fn transport(req: HttpRequest) -> HttpResponse {
        HttpResponse::Ok().json(json!({
            "scheme": req.connection_info().scheme(),
            "version": format!("{:?}", req.version()),
        }))
    }

    let (listener, port) = test_listener();
    let server = build_h1_server(
        || App::new().route("/transport", web::get().to(transport)),
        vec![listener],
        1,
        None,
    )
    .expect("build plaintext H1 server");
    let handle = server.handle();
    let join = tokio::spawn(server);

    let response = reqwest::get(format!("http://127.0.0.1:{port}/transport"))
        .await
        .expect("plaintext request");
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(response.version(), reqwest::Version::HTTP_11);
    let body: Value = response.json().await.expect("transport response JSON");
    assert_eq!(body["scheme"], "http");
    assert_eq!(body["version"], "HTTP/1.1");

    stop_server(handle, join).await;
}

#[actix_web::test]
async fn rustls_server_is_http11_secure_wss_and_never_negotiates_h2() {
    async fn transport(req: HttpRequest) -> HttpResponse {
        HttpResponse::Ok().json(json!({
            "scheme": req.connection_info().scheme(),
            "version": format!("{:?}", req.version()),
        }))
    }

    let fixture = tempfile::tempdir().expect("TLS fixture tempdir");
    let Some((cert_file, key_file)) =
        gen_self_signed(fixture.path()).expect("present openssl should generate the TLS fixture")
    else {
        eprintln!("skipping live TLS transport test: openssl unavailable");
        return;
    };
    let tls = TlsConfig {
        cert_file,
        key_file,
    };
    let rustls = build_rustls_config(&tls).expect("valid Rustls config");

    let mut unsafe_alpn = rustls.clone();
    unsafe_alpn.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    let (unsafe_listener, _) = test_listener();
    let unsafe_error =
        match build_h1_server(|| App::new(), vec![unsafe_listener], 1, Some(unsafe_alpn)) {
            Ok(_) => panic!("an H1 dispatcher must reject a config that advertises h2"),
            Err(error) => error,
        };
    assert_eq!(unsafe_error.kind(), std::io::ErrorKind::InvalidInput);
    assert!(unsafe_error.to_string().contains("only ALPN http/1.1"));

    let state_dir = tempfile::tempdir().expect("AppState tempdir");
    let state = web::Data::new(
        AppState::new(state_dir.path().to_path_buf())
            .await
            .expect("test AppState"),
    );
    let state_for_factory = state.clone();
    let (listener, port) = test_listener();
    let server = build_h1_server(
        move || {
            App::new()
                .app_data(state_for_factory.clone())
                .route("/__transport", web::get().to(transport))
                .configure(configure_routes)
        },
        vec![listener],
        1,
        Some(rustls),
    )
    .expect("build Rustls H1 server");
    let handle = server.handle();
    let join = tokio::spawn(server);

    // A normal HTTPS client that offers H2 and H1 must be pinned to H1, and
    // Actix must still mark the request context secure for URL/cookie logic.
    let https = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .expect("HTTPS test client");
    let response = https
        .get(format!("https://127.0.0.1:{port}/__transport"))
        .send()
        .await
        .expect("HTTPS request");
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(response.version(), reqwest::Version::HTTP_11);
    let body: Value = response.json().await.expect("transport response JSON");
    assert_eq!(body["scheme"], "https");
    assert_eq!(body["version"], "HTTP/1.1");

    // Even an H2-only client can never select H2. TLS stacks may either reject
    // the no-overlap handshake or complete it with no ALPN; both are safe.
    let h2_only = TlsConnector::builder()
        .danger_accept_invalid_certs(true)
        .request_alpns(&["h2"])
        .build()
        .expect("H2-only TLS connector");
    let tcp = TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("TCP connection for H2-only ALPN");
    if let Ok(stream) = TokioTlsConnector::from(h2_only)
        .connect("localhost", tcp)
        .await
    {
        assert_ne!(
            stream
                .get_ref()
                .negotiated_alpn()
                .expect("query negotiated ALPN")
                .as_deref(),
            Some(b"h2".as_slice()),
            "the H1-only listener must never negotiate h2"
        );
    }

    // Exercise the real `/v2/stream` WSS upgrade, application ping/pong, and
    // negotiated ALPN through the replacement Tungstenite client.
    let native = TlsConnector::builder()
        .danger_accept_invalid_certs(true)
        .request_alpns(&["h2", "http/1.1"])
        .build()
        .expect("WSS TLS connector");
    let request = format!("wss://127.0.0.1:{port}/v2/stream")
        .into_client_request()
        .expect("WSS request");
    let (mut websocket, upgrade) =
        connect_async_tls_with_config(request, None, false, Some(Connector::NativeTls(native)))
            .await
            .expect("real WSS /v2/stream upgrade");
    assert_eq!(upgrade.status().as_u16(), 101);
    match websocket.get_ref() {
        MaybeTlsStream::NativeTls(stream) => assert_eq!(
            stream
                .get_ref()
                .negotiated_alpn()
                .expect("query WSS ALPN")
                .as_deref(),
            Some(b"http/1.1".as_slice())
        ),
        stream => panic!("expected native TLS WSS stream, got {stream:?}"),
    }

    websocket
        .send(Message::Text(r#"{"type":"ping"}"#.into()))
        .await
        .expect("send WSS application ping");
    let pong = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            match websocket.next().await.expect("WSS remains open") {
                Ok(Message::Text(text)) => {
                    let value: Value = serde_json::from_str(text.as_str()).expect("JSON WSS frame");
                    if value["type"] == "pong" {
                        break value;
                    }
                }
                Ok(Message::Ping(_)) | Ok(Message::Pong(_)) => continue,
                Ok(Message::Close(frame)) => panic!("WSS closed before pong: {frame:?}"),
                Ok(other) => panic!("unexpected WSS frame before pong: {other:?}"),
                Err(error) => panic!("WSS read failed before pong: {error}"),
            }
        }
    })
    .await
    .expect("application pong arrives before timeout");
    assert_eq!(pong["type"], "pong");

    websocket.close(None).await.expect("close WSS client");

    // Keep a real TLS/H1 connection idle while the server drains. This covers
    // the Rustls listener factory independently from the plaintext drain
    // tests above; without the propagated signal, stop(true) waits for the
    // ordinary keep-alive or worker shutdown timeout.
    let idle_tls = TlsConnector::builder()
        .danger_accept_invalid_certs(true)
        .request_alpns(&["http/1.1"])
        .build()
        .expect("idle H1 TLS connector");
    let tcp = TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("TCP connection for idle H1 TLS client");
    let mut idle_tls = TokioTlsConnector::from(idle_tls)
        .connect("localhost", tcp)
        .await
        .expect("idle H1 TLS handshake");
    idle_tls
        .write_all(
            b"GET /__transport HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive\r\n\r\n",
        )
        .await
        .expect("send idle H1 TLS request");
    let response = read_http_response_head(&mut idle_tls).await;
    assert!(
        response.starts_with(b"HTTP/1.1 200"),
        "unexpected idle TLS response: {}",
        String::from_utf8_lossy(&response)
    );

    stop_server_gracefully(handle, join).await;
}
