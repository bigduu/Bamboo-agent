//! Canonical, public frontend compatibility bootstrap.
//!
//! `GET /api/v1/bootstrap` is the single compatibility authority for Lotus
//! Next. It describes the currently implemented REST/realtime contract and the
//! current request's access posture without exposing configuration internals or
//! credential material. Older servers intentionally return 404; clients must
//! surface that as an incompatible server rather than probe legacy endpoints.

use actix_web::{http::header, web, HttpRequest, HttpResponse};
use serde::Serialize;

use crate::app_state::AppState;
use crate::handlers::{
    agent::ws_v2::{SUBPROTOCOL_JSON, SUBPROTOCOL_MSGPACK},
    settings::{bootstrap_access_snapshot, BootstrapAccessSnapshot},
};

const SCHEMA_VERSION: u32 = 1;
const SERVER_PRODUCT: &str = "bamboo";
const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");
const API_NAME: &str = "bamboo.agent";
const API_BASE_PATH: &str = "/api/v1";
const API_MIN_VERSION: u32 = 1;
const API_MAX_VERSION: u32 = 1;
const REALTIME_NAME: &str = "bamboo.v2";
const REALTIME_PATH: &str = "/v2/stream";
const REALTIME_MIN_VERSION: u32 = 2;
const REALTIME_MAX_VERSION: u32 = 2;
const VERIFY_PATH: &str = "/api/v1/bamboo/access/verify";
const PAIR_PATH: &str = "/v2/pair";

const CAPABILITIES: &[&str] = &[
    "auth.device_bearer.v1",
    "auth.password_cookie.v1",
    "auth.ws_device_hello.v1",
    "auth.ws_hello_ack.v1",
    "realtime.account_feed.v1",
    "realtime.agent_events.v1",
    "realtime.application_heartbeat.v1",
    "realtime.feed_cursor.v1",
    "realtime.feed_reset.v1",
    "realtime.stop_control.v1",
];

const SUBPROTOCOLS: &[BootstrapSubprotocol] = &[
    BootstrapSubprotocol {
        name: SUBPROTOCOL_JSON,
        encoding: "json",
    },
    BootstrapSubprotocol {
        name: SUBPROTOCOL_MSGPACK,
        encoding: "messagepack",
    },
];

#[derive(Debug, Serialize)]
struct BootstrapResponse {
    schema_version: u32,
    server: BootstrapServer,
    api: BootstrapApi,
    realtime: BootstrapRealtime,
    capabilities: &'static [&'static str],
    auth: BootstrapAuth,
}

#[derive(Debug, Serialize)]
struct BootstrapServer {
    product: &'static str,
    version: &'static str,
}

#[derive(Debug, Serialize)]
struct BootstrapApi {
    name: &'static str,
    canonical_base_path: &'static str,
    min_version: u32,
    max_version: u32,
}

#[derive(Debug, Serialize)]
struct BootstrapRealtime {
    name: &'static str,
    path: &'static str,
    min_version: u32,
    max_version: u32,
    subprotocols: &'static [BootstrapSubprotocol],
}

#[derive(Debug, Serialize)]
struct BootstrapSubprotocol {
    name: &'static str,
    encoding: &'static str,
}

#[derive(Debug, Serialize)]
struct BootstrapAuth {
    #[serde(flatten)]
    access: BootstrapAccessSnapshot,
    verify_path: &'static str,
    pair_path: &'static str,
}

fn response(access: BootstrapAccessSnapshot) -> BootstrapResponse {
    BootstrapResponse {
        schema_version: SCHEMA_VERSION,
        server: BootstrapServer {
            product: SERVER_PRODUCT,
            version: SERVER_VERSION,
        },
        api: BootstrapApi {
            name: API_NAME,
            canonical_base_path: API_BASE_PATH,
            min_version: API_MIN_VERSION,
            max_version: API_MAX_VERSION,
        },
        realtime: BootstrapRealtime {
            name: REALTIME_NAME,
            path: REALTIME_PATH,
            min_version: REALTIME_MIN_VERSION,
            max_version: REALTIME_MAX_VERSION,
            subprotocols: SUBPROTOCOLS,
        },
        capabilities: CAPABILITIES,
        auth: BootstrapAuth {
            access,
            verify_path: VERIFY_PATH,
            pair_path: PAIR_PATH,
        },
    }
}

/// Return the immutable frontend contract plus auth facts derived from one
/// authoritative configuration snapshot and this exact request.
pub async fn handler(state: web::Data<AppState>, req: HttpRequest) -> HttpResponse {
    let access = {
        let config = state.config.read().await;
        bootstrap_access_snapshot(&config, &req)
    };

    HttpResponse::Ok()
        .insert_header((header::CACHE_CONTROL, "no-store"))
        .insert_header((header::VARY, "Cookie, Authorization, X-Device-Id"))
        .json(response(access))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use serde_json::json;

    use super::*;

    #[test]
    fn response_shape_is_exact_stable_and_secret_free() {
        let request = actix_web::test::TestRequest::default()
            .insert_header((header::HOST, "bamboo.example.com"))
            .to_http_request();
        let config = bamboo_config::Config::default();
        let value =
            serde_json::to_value(response(bootstrap_access_snapshot(&config, &request))).unwrap();

        assert_eq!(
            value,
            json!({
                "schema_version": 1,
                "server": {
                    "product": "bamboo",
                    "version": env!("CARGO_PKG_VERSION"),
                },
                "api": {
                    "name": "bamboo.agent",
                    "canonical_base_path": "/api/v1",
                    "min_version": 1,
                    "max_version": 1,
                },
                "realtime": {
                    "name": "bamboo.v2",
                    "path": "/v2/stream",
                    "min_version": 2,
                    "max_version": 2,
                    "subprotocols": [
                        {"name": "bamboo.v2", "encoding": "json"},
                        {"name": "bamboo.v2.msgpack", "encoding": "messagepack"},
                    ],
                },
                "capabilities": [
                    "auth.device_bearer.v1",
                    "auth.password_cookie.v1",
                    "auth.ws_device_hello.v1",
                    "auth.ws_hello_ack.v1",
                    "realtime.account_feed.v1",
                    "realtime.agent_events.v1",
                    "realtime.application_heartbeat.v1",
                    "realtime.feed_cursor.v1",
                    "realtime.feed_reset.v1",
                    "realtime.stop_control.v1",
                ],
                "auth": {
                    "policy": "open",
                    "request_state": "unauthenticated",
                    "password_enabled": false,
                    "device_auth_enabled": false,
                    "verify_path": "/api/v1/bamboo/access/verify",
                    "pair_path": "/v2/pair",
                },
            })
        );

        let capabilities = value["capabilities"].as_array().unwrap();
        assert_eq!(
            capabilities
                .iter()
                .map(|capability| capability.as_str().unwrap())
                .collect::<BTreeSet<_>>()
                .len(),
            capabilities.len(),
            "capability identifiers must be unique"
        );

        // The exact response snapshot above is the strongest allowlist. Keep a
        // focused denylist too so a future nested config envelope cannot slip
        // verifier or filesystem details into this public endpoint unnoticed.
        let serialized = serde_json::to_string(&value).unwrap();
        for forbidden in [
            "password_hash",
            "password_salt",
            "token_hash",
            "token_salt",
            "device_id",
            "credential_ref",
            "app_data_dir",
            "config_path",
        ] {
            assert!(!serialized.contains(forbidden), "leaked key: {forbidden}");
        }
    }
}
