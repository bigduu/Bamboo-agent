use std::net::TcpListener;

use actix_web::{web, App, HttpRequest, HttpResponse};
use futures::{SinkExt, StreamExt};
use native_tls::TlsConnector;
use serde_json::{json, Value};
use tokio::net::TcpStream;
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
    stop_server(handle, join).await;
}
