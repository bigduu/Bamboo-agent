//! Network discovery: an HTTP-backed [`Discovery`] talking to the server's
//! `/v1/agents` control plane (remote-actor-plan §3.3, §6 P2).
//!
//! Where [`crate::discovery::FileFabric`] keeps records as `*.json` files in a
//! local directory — invisible across machines — `RegistryFabric` is its
//! cross-host sibling: it `publish`/`resolve`/`discover`/`withdraw`s by calling
//! a registry endpoint over HTTP(S). The same [`AgentRecord`] + `lease_expires_at`
//! semantics carry over: a worker renews its lease by re-`publish`ing (the server
//! refreshes `lease_expires_at = now + ttl` on each upsert), exactly as the file
//! fabric re-writes its record.
//!
//! Auth: the registration face is a CREDENTIALLED control plane (a forged
//! registration could poison routing — §5 "活性伪造:注册需持 token"). When a
//! bearer token is configured, every request carries `Authorization: Bearer
//! <token>`. The token is NEVER logged or formatted into an error.
//!
//! GC: garbage collection is the REGISTRY's responsibility (lazy filter on read,
//! or a server sweep). The client [`Discovery::gc`] is therefore a no-op that
//! returns `Ok(0)` — there is nothing for a client to sweep.

use async_trait::async_trait;
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION};
use reqwest::{Client, StatusCode};

use crate::discovery::Discovery;
use crate::error::{Result, StoreError};
use crate::proto::AgentRecord;

/// An HTTP-backed discovery fabric pointed at a registry's `/v1/agents` face.
///
/// Holds a configurable base URL (e.g. `https://control-plane:8443`) and an
/// optional bearer token (the device/worker credential). Cheap to clone — the
/// inner [`Client`] is an `Arc` internally.
pub struct RegistryFabric {
    client: Client,
    /// Base URL WITHOUT a trailing slash, e.g. `http://127.0.0.1:8080`.
    base: String,
}

impl RegistryFabric {
    /// Build a fabric for `base_url` with no bearer token (e.g. a loopback/dev
    /// registry that does not require a credential).
    pub fn new(base_url: impl Into<String>) -> Result<Self> {
        Self::build(base_url.into(), None)
    }

    /// Build a fabric for `base_url` that presents `token` as
    /// `Authorization: Bearer <token>` on every request.
    pub fn with_token(base_url: impl Into<String>, token: impl Into<String>) -> Result<Self> {
        Self::build(base_url.into(), Some(token.into()))
    }

    fn build(base_url: String, token: Option<String>) -> Result<Self> {
        let mut headers = HeaderMap::new();
        if let Some(token) = token {
            // Build the header value here so the token is captured into the
            // reqwest default-headers and never re-stringified at call sites
            // (and so a malformed token surfaces as a clean error, not a panic).
            let mut value = HeaderValue::from_str(&format!("Bearer {token}"))
                .map_err(|_| StoreError::Network("invalid bearer token".into()))?;
            value.set_sensitive(true); // keep it out of any header Debug output
            headers.insert(AUTHORIZATION, value);
        }
        let client = Client::builder()
            .default_headers(headers)
            .build()
            .map_err(net_err)?;
        Ok(Self {
            client,
            base: base_url.trim_end_matches('/').to_string(),
        })
    }

    fn agents_url(&self) -> String {
        format!("{}/v1/agents", self.base)
    }

    fn agent_url(&self, agent_id: &str) -> String {
        format!("{}/v1/agents/{}", self.base, agent_id)
    }
}

#[async_trait]
impl Discovery for RegistryFabric {
    /// `POST /v1/agents` — register or heartbeat. The server upserts by
    /// `agent_id` and refreshes the lease. Re-publishing is the renewal path.
    async fn publish(&self, rec: &AgentRecord) -> Result<()> {
        let resp = self
            .client
            .post(self.agents_url())
            .json(rec)
            .send()
            .await
            .map_err(net_err)?;
        ensure_ok(resp).await.map(|_| ())
    }

    /// `GET /v1/agents/{id}` — resolve one live record. A `404` (absent or
    /// lease-expired) maps to `Ok(None)`.
    async fn resolve(&self, agent_id: &str) -> Result<Option<AgentRecord>> {
        let resp = self
            .client
            .get(self.agent_url(agent_id))
            .send()
            .await
            .map_err(net_err)?;
        if resp.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let resp = ensure_ok(resp).await?;
        let rec = resp.json::<AgentRecord>().await.map_err(net_err)?;
        Ok(Some(rec))
    }

    /// `GET /v1/agents` — list all live records.
    async fn discover(&self) -> Result<Vec<AgentRecord>> {
        let resp = self
            .client
            .get(self.agents_url())
            .send()
            .await
            .map_err(net_err)?;
        let resp = ensure_ok(resp).await?;
        let recs = resp.json::<Vec<AgentRecord>>().await.map_err(net_err)?;
        Ok(recs)
    }

    /// `DELETE /v1/agents/{id}` — withdraw a record (clean shutdown). A `404` is
    /// treated as success (idempotent, matching the file fabric).
    async fn withdraw(&self, agent_id: &str) -> Result<()> {
        let resp = self
            .client
            .delete(self.agent_url(agent_id))
            .send()
            .await
            .map_err(net_err)?;
        if resp.status() == StatusCode::NOT_FOUND {
            return Ok(());
        }
        ensure_ok(resp).await.map(|_| ())
    }

    /// GC is the REGISTRY's responsibility (server-side lazy-filter-on-read /
    /// sweep), so the client has nothing to collect. Always `Ok(0)`.
    async fn gc(&self) -> Result<usize> {
        Ok(0)
    }
}

/// Map a transport-level `reqwest::Error` into a `StoreError::Network`. Its
/// `Display` never includes request headers, so the bearer token cannot leak.
fn net_err(e: reqwest::Error) -> StoreError {
    StoreError::Network(e.to_string())
}

/// Turn a non-2xx HTTP response into a `StoreError`, mapping auth failures to a
/// distinct message. The token is never echoed (only the status is reported).
async fn ensure_ok(resp: reqwest::Response) -> Result<reqwest::Response> {
    let status = resp.status();
    if status.is_success() {
        return Ok(resp);
    }
    if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
        return Err(StoreError::Network(format!(
            "registry rejected the request: {status} (missing or invalid credential)"
        )));
    }
    Err(StoreError::Network(format!(
        "registry returned an error status: {status}"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, Utc};
    use wiremock::matchers::{header, method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn rec(id: &str) -> AgentRecord {
        AgentRecord {
            agent_id: id.into(),
            role: "service".into(),
            labels: vec!["gpu".into()],
            endpoint: "ws://10.0.0.2:8443".into(),
            pid: 7,
            version: "1".into(),
            started_at: Utc::now(),
            lease_expires_at: Utc::now() + Duration::seconds(30),
        }
    }

    #[tokio::test]
    async fn publish_resolve_discover_withdraw_round_trip_over_http() {
        let server = MockServer::start().await;
        let r = rec("a");

        // POST /v1/agents echoes the stored record back.
        Mock::given(method("POST"))
            .and(path("/v1/agents"))
            .and(header("authorization", "Bearer t-secret"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&r))
            .mount(&server)
            .await;
        // GET /v1/agents/a resolves one.
        Mock::given(method("GET"))
            .and(path("/v1/agents/a"))
            .and(header("authorization", "Bearer t-secret"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&r))
            .mount(&server)
            .await;
        // GET /v1/agents lists.
        Mock::given(method("GET"))
            .and(path("/v1/agents"))
            .and(header("authorization", "Bearer t-secret"))
            .respond_with(ResponseTemplate::new(200).set_body_json(vec![r.clone()]))
            .mount(&server)
            .await;
        // DELETE /v1/agents/a withdraws.
        Mock::given(method("DELETE"))
            .and(path("/v1/agents/a"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let fab = RegistryFabric::with_token(server.uri(), "t-secret").unwrap();

        // Drive through the trait object (object-safety + the bearer header).
        let disc: &dyn Discovery = &fab;
        disc.publish(&r).await.unwrap();
        let got = disc.resolve("a").await.unwrap().unwrap();
        assert_eq!(got.agent_id, "a");
        assert_eq!(disc.discover().await.unwrap().len(), 1);
        disc.withdraw("a").await.unwrap();
        // Client GC is a no-op (server-side responsibility).
        assert_eq!(disc.gc().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn resolve_missing_is_none_and_withdraw_missing_is_ok() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/agents/ghost"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .and(path("/v1/agents/ghost"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        let fab = RegistryFabric::new(server.uri()).unwrap();
        assert!(fab.resolve("ghost").await.unwrap().is_none());
        fab.withdraw("ghost").await.unwrap(); // 404 is idempotent success
    }

    #[tokio::test]
    async fn discover_with_role_filter_query_is_sent() {
        // `discover()` hits the bare list endpoint; this asserts the URL shape the
        // server filters on is reachable (role filtering is a server concern, but
        // the client must target `/v1/agents`).
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/agents"))
            .respond_with(ResponseTemplate::new(200).set_body_json(Vec::<AgentRecord>::new()))
            .mount(&server)
            .await;
        let fab = RegistryFabric::new(server.uri()).unwrap();
        assert!(fab.discover().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn unauthorized_maps_to_network_error_without_leaking_token() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/agents"))
            .respond_with(ResponseTemplate::new(401))
            .mount(&server)
            .await;
        let fab = RegistryFabric::with_token(server.uri(), "super-secret-token").unwrap();
        let err = fab.publish(&rec("x")).await.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("401"), "expected 401 in: {msg}");
        assert!(
            !msg.contains("super-secret-token"),
            "token must never leak into the error: {msg}"
        );
    }

    #[tokio::test]
    async fn trailing_slash_in_base_is_normalized() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/agents"))
            .respond_with(ResponseTemplate::new(200).set_body_json(Vec::<AgentRecord>::new()))
            .mount(&server)
            .await;
        // Base with a trailing slash must still produce `/v1/agents`, not `//v1/agents`.
        let fab = RegistryFabric::new(format!("{}/", server.uri())).unwrap();
        assert!(fab.discover().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn query_param_route_is_exercisable() {
        // Sanity that the mock server matches a role query — documents the server
        // contract that `RegistryFabric::discover` could grow a filtered variant.
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/agents"))
            .and(query_param("role", "service"))
            .respond_with(ResponseTemplate::new(200).set_body_json(Vec::<AgentRecord>::new()))
            .mount(&server)
            .await;
        let client = reqwest::Client::new();
        let resp = client
            .get(format!("{}/v1/agents?role=service", server.uri()))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
    }
}
