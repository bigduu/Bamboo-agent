//! Live HTTP round-trip for the remote-actor P2a registry (#181, Epic #181).
//!
//! Stands up a REAL bound `bamboo-server` on an ephemeral loopback port and drives
//! the REAL `bamboo_subagent::RegistryFabric` (an HTTP `Discovery` impl) against
//! its `/v1/agents` control-plane routes — end to end over `reqwest` → actix,
//! exercising the publish → resolve → discover → withdraw path the way a remote
//! worker / parent would. `test::init_service` never binds a socket, so only a
//! bound-port test like this proves the `reqwest` client + actix server actually
//! talk over the network.
//!
//! Loopback requests bypass the access middleware (`is_local_request`), so this
//! test reaches the gated handlers without a credential — the gated-vs-401
//! contract is covered separately by the `routes::tests::agents_routes_require_auth`
//! unit test, and the bearer-header wiring by the bamboo-subagent-side wiremock
//! tests. We still construct the fabric WITH a token to confirm the token path
//! does not break a real request.

use actix_web::{web, App, HttpServer};
use bamboo_server::routes::configure_routes;
use bamboo_server::AppState;
use bamboo_subagent::discovery::Discovery;
use bamboo_subagent::proto::AgentRecord;
use bamboo_subagent::RegistryFabric;
use chrono::{Duration as ChronoDuration, Utc};
use tokio::net::TcpListener;

struct TestServer {
    base_http_url: String,
    _tmp: tempfile::TempDir,
    server_handle: actix_web::dev::ServerHandle,
    _join: tokio::task::JoinHandle<std::io::Result<()>>,
}

impl TestServer {
    async fn start() -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let state = web::Data::new(AppState::new(tmp.path().to_path_buf()).await.unwrap());

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let std_listener = listener.into_std().unwrap();
        std_listener.set_nonblocking(false).unwrap();

        let state_for_factory = state.clone();
        let server = HttpServer::new(move || {
            App::new()
                .app_data(state_for_factory.clone())
                .configure(configure_routes)
        })
        .workers(1)
        .listen(std_listener)
        .unwrap()
        .run();

        let server_handle = server.handle();
        let join = tokio::spawn(server);

        TestServer {
            base_http_url: format!("http://127.0.0.1:{port}"),
            _tmp: tmp,
            server_handle,
            _join: join,
        }
    }

    async fn stop(self) {
        self.server_handle.stop(false).await;
    }
}

fn rec(id: &str, role: &str) -> AgentRecord {
    let now = Utc::now();
    AgentRecord {
        agent_id: id.into(),
        role: role.into(),
        labels: vec!["a100".into()],
        endpoint: "ws://10.0.0.9:8443".into(),
        pid: 99,
        version: "1".into(),
        started_at: now,
        // The server is the lease authority and re-stamps this; we send a value
        // anyway to prove it round-trips through the request body.
        lease_expires_at: now + ChronoDuration::seconds(60),
    }
}

#[actix_web::test]
async fn registry_fabric_publish_resolve_discover_withdraw_over_real_http() {
    let server = TestServer::start().await;

    // A token is configured but loopback bypasses auth — this just proves the
    // bearer-carrying client path works against a real server.
    let fab = RegistryFabric::with_token(&server.base_http_url, "device-token-xyz").unwrap();

    // publish (register/heartbeat).
    fab.publish(&rec("live-1", "gpu")).await.unwrap();
    fab.publish(&rec("live-2", "cpu")).await.unwrap();

    // resolve one.
    let got = fab
        .resolve("live-1")
        .await
        .unwrap()
        .expect("live-1 present");
    assert_eq!(got.agent_id, "live-1");
    assert_eq!(got.endpoint, "ws://10.0.0.9:8443");
    // Server stamped a fresh lease that is still in the future.
    assert!(got.lease_expires_at > Utc::now());

    // discover lists both.
    let all = fab.discover().await.unwrap();
    assert_eq!(all.len(), 2);

    // resolve a missing id → None.
    assert!(fab.resolve("nope").await.unwrap().is_none());

    // withdraw one; it disappears, the other stays.
    fab.withdraw("live-1").await.unwrap();
    assert!(fab.resolve("live-1").await.unwrap().is_none());
    let remaining = fab.discover().await.unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].agent_id, "live-2");

    // client gc is a no-op (server owns gc).
    assert_eq!(fab.gc().await.unwrap(), 0);

    server.stop().await;
}
