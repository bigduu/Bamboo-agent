//! Authenticated host-owner view. Root/subtree IDs are data selectors only.

use actix_web::{http::header, web, HttpRequest, HttpResponse};
use bamboo_domain::{
    ActorSnapshotError, ActorSnapshotLimits, ActorSnapshotPort, ActorSnapshotPrincipal,
    ACTOR_SNAPSHOT_SCHEMA_VERSION, ACTOR_SNAPSHOT_V2_MEDIA_TYPE, ACTOR_SNAPSHOT_V2_SCHEMA_VERSION,
};
use serde::Deserialize;

use crate::app_state::AppState;
use crate::handlers::settings::{bootstrap_access_snapshot, BootstrapRequestState};

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct Query {
    subtree_id: Option<String>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Version {
    V1,
    V2,
}

impl Version {
    fn schema(self) -> u32 {
        match self {
            Self::V1 => ACTOR_SNAPSHOT_SCHEMA_VERSION,
            Self::V2 => ACTOR_SNAPSHOT_V2_SCHEMA_VERSION,
        }
    }

    fn media_type(self) -> &'static str {
        match self {
            Self::V1 => "application/json",
            Self::V2 => ACTOR_SNAPSHOT_V2_MEDIA_TYPE,
        }
    }
}

/// Only an explicit supported vendor media type opts in. Old clients, unknown
/// media types and wildcards retain the v1 contract; q=0 never opts in.
fn negotiated_version(req: &HttpRequest) -> Version {
    let mut v1_quality = None;
    let mut v2_quality = None;
    for value in req.headers().get_all(header::ACCEPT) {
        let Ok(value) = value.to_str() else { continue };
        for range in value.split(',') {
            let mut parts = range.split(';');
            let media_type = parts.next().unwrap_or_default().trim();
            let target = if media_type.eq_ignore_ascii_case(ACTOR_SNAPSHOT_V2_MEDIA_TYPE) {
                &mut v2_quality
            } else if media_type.eq_ignore_ascii_case("application/json") {
                &mut v1_quality
            } else {
                continue;
            };
            let mut quality = Some(1.0_f32);
            for parameter in parts {
                let Some((key, value)) = parameter.trim().split_once('=') else {
                    quality = None;
                    break;
                };
                if key.trim().eq_ignore_ascii_case("q") {
                    quality = value
                        .trim()
                        .parse::<f32>()
                        .ok()
                        .filter(|q| q.is_finite() && (0.0..=1.0).contains(q));
                    if quality.is_none() {
                        break;
                    }
                } else {
                    // No alternative parameterized v2 representation exists.
                    quality = None;
                    break;
                }
            }
            if let Some(quality) = quality {
                *target = Some(target.unwrap_or(0.0_f32).max(quality));
            }
        }
    }
    if v2_quality.is_some_and(|q| q > 0.0 && q >= v1_quality.unwrap_or(0.0)) {
        Version::V2
    } else {
        Version::V1
    }
}

fn failure(error: ActorSnapshotError, version: Version) -> HttpResponse {
    use actix_web::http::StatusCode;
    let status = match error {
        ActorSnapshotError::InvalidSelector => StatusCode::BAD_REQUEST,
        ActorSnapshotError::NotFound => StatusCode::NOT_FOUND,
        ActorSnapshotError::UnauthorizedScope => StatusCode::FORBIDDEN,
        ActorSnapshotError::BudgetExceeded => StatusCode::PAYLOAD_TOO_LARGE,
        ActorSnapshotError::UnsupportedAuthority => StatusCode::NOT_IMPLEMENTED,
        ActorSnapshotError::StorageUnavailable => StatusCode::SERVICE_UNAVAILABLE,
        _ => StatusCode::CONFLICT,
    };
    HttpResponse::build(status)
        .insert_header((header::VARY, "Accept"))
        .content_type(version.media_type())
        .json(serde_json::json!({
            "schema_version": version.schema(),
            "error": crate::error::error_value(error.to_string())
        }))
}

/// The same HostOwner gate is used by snapshot REST and lazy Actor WS channels.
/// Open-policy access and Codex run tokens do not confer subtree visibility.
pub(crate) async fn host_owner_authorized(state: &web::Data<AppState>, req: &HttpRequest) -> bool {
    let codex_token = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .and_then(|h| {
            h.strip_prefix("Bearer ")
                .or_else(|| h.strip_prefix("bearer "))
        })
        .map(str::trim)
        .is_some_and(|token| token.starts_with("bcx1_"));
    if codex_token {
        return false;
    }
    let config = state.config.read().await;
    matches!(
        bootstrap_access_snapshot(&config, req).request_state,
        BootstrapRequestState::LocalBypass | BootstrapRequestState::Authenticated
    )
}

pub async fn handler(
    state: web::Data<AppState>,
    path: web::Path<String>,
    req: HttpRequest,
) -> HttpResponse {
    let version = negotiated_version(&req);
    // The existing middleware limits bcx credentials to Responses/models.
    // Retain that restriction even when this handler is mounted independently.
    if !host_owner_authorized(&state, &req).await {
        return HttpResponse::Unauthorized()
            .insert_header((header::VARY, "Accept"))
            .content_type(version.media_type())
            .json(serde_json::json!({
                "schema_version": version.schema(),
                "error": crate::error::error_value("host_authentication_required")
            }));
    }
    let query = match web::Query::<Query>::from_query(req.query_string()) {
        Ok(query) => query.into_inner(),
        Err(_) => return failure(ActorSnapshotError::InvalidSelector, version),
    };
    let root = path.into_inner();
    let subtree = query.subtree_id.as_deref().unwrap_or(&root);
    if version == Version::V2 {
        return match state
            .session_store
            .actor_subtree_snapshot_v2(
                ActorSnapshotPrincipal::host_owner(),
                &root,
                subtree,
                ActorSnapshotLimits::default(),
            )
            .await
        {
            Ok(snapshot) => snapshot_response(&req, &snapshot.snapshot_id, &snapshot, version),
            Err(error) => failure(error, version),
        };
    }
    match state
        .session_store
        .actor_subtree_snapshot(
            ActorSnapshotPrincipal::host_owner(),
            &root,
            subtree,
            ActorSnapshotLimits::default(),
        )
        .await
    {
        Ok(snapshot) => snapshot_response(&req, &snapshot.snapshot_id, &snapshot, version),
        Err(error) => failure(error, version),
    }
}

fn snapshot_response(
    req: &HttpRequest,
    snapshot_id: &str,
    snapshot: &impl serde::Serialize,
    version: Version,
) -> HttpResponse {
    let etag = format!("\"{snapshot_id}\"");
    if req
        .headers()
        .get(header::IF_NONE_MATCH)
        .and_then(|h| h.to_str().ok())
        == Some(etag.as_str())
    {
        return HttpResponse::NotModified()
            .insert_header((header::ETAG, etag))
            .insert_header((header::CACHE_CONTROL, "private, no-cache"))
            .insert_header((header::VARY, "Accept"))
            .content_type(version.media_type())
            .finish();
    }
    HttpResponse::Ok()
        .insert_header((header::ETAG, etag))
        .insert_header((header::CACHE_CONTROL, "private, no-cache"))
        .insert_header((header::VARY, "Accept"))
        .content_type(version.media_type())
        .json(snapshot)
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos", windows)))]
mod tests {
    use super::*;
    use crate::handlers::settings::issue_device_token;
    use actix_web::{http::StatusCode, test, App};
    use bamboo_config::AccessControlConfig;
    use bamboo_domain::{Session, Storage};
    use bamboo_engine::external_agents::actor_adapter::CodexRunTokenAuthority;

    #[actix_web::test]
    async fn canonical_gateway_requires_trusted_host_owner_and_keeps_codex_response_scope() {
        let _key = bamboo_config::encryption::set_test_encryption_key([0x37; 32]);
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().canonicalize().unwrap();
        let state = web::Data::new(AppState::new(home.clone()).await.unwrap());
        let root = Session::new("public-root", "model");
        let child = Session::new_child_of("public-child", &root, "model", "Child");
        state.session_store.save_session(&root).await.unwrap();
        state.session_store.save_session(&child).await.unwrap();
        // A live WS replay window is not a durable coordinate for this
        // independent REST snapshot, including when both are in one process.
        let _live_actor = state.actor_event_hub.subscribe(&child.id, None).unwrap();
        let source_path = home.join("sessions/public-root/session.json");
        let source_before = tokio::fs::read(&source_path).await.unwrap();
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(crate::agent_routes),
        )
        .await;
        let uri = "/api/v1/actors/public-root/snapshot?subtree_id=public-child";
        let local = test::TestRequest::get()
            .uri(uri)
            .peer_addr("127.0.0.1:1234".parse().unwrap())
            .insert_header((header::HOST, "localhost"));
        let response = test::call_service(&app, local.to_request()).await;
        assert_eq!(response.status(), StatusCode::OK);
        let etag = response
            .headers()
            .get(header::ETAG)
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned();
        let body: serde_json::Value = test::read_body_json(response).await;
        assert_eq!(body["nodes"].as_array().unwrap().len(), 1);
        assert!(body["stream_cursor"]
            .as_str()
            .is_some_and(|cursor| cursor.starts_with("at1-")));
        assert_eq!(body["nodes"][0]["actor_id"], child.id);
        assert_eq!(body["nodes"][0]["logical_state"], serde_json::Value::Null);
        assert_eq!(tokio::fs::read(source_path).await.unwrap(), source_before);
        assert!(!home
            .join("sessions/public-root/actor-authority.json")
            .exists());
        assert!(!home
            .join("sessions/public-root/children/public-child/actor-authority.json")
            .exists());

        let refresh = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(uri)
                .peer_addr("127.0.0.1:1234".parse().unwrap())
                .insert_header((header::HOST, "localhost"))
                .insert_header((header::IF_NONE_MATCH, etag))
                .to_request(),
        )
        .await;
        assert_eq!(refresh.status(), StatusCode::NOT_MODIFIED);
        let remote = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(uri)
                .peer_addr("203.0.113.9:1234".parse().unwrap())
                .insert_header((header::HOST, "localhost"))
                .insert_header(("x-forwarded-for", "127.0.0.1"))
                .to_request(),
        )
        .await;
        assert_eq!(remote.status(), StatusCode::UNAUTHORIZED); // Open policy is not HostOwner.

        let (device, token) = issue_device_token("snapshot-device");
        let device_id = device.device_id.clone();
        state.config.write().await.access_control = Some(AccessControlConfig {
            devices: vec![device],
            ..Default::default()
        });
        let authorized = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(uri)
                .peer_addr("203.0.113.9:1234".parse().unwrap())
                .insert_header((header::HOST, "bamboo.example.com"))
                .insert_header((header::AUTHORIZATION, format!("Bearer {token}")))
                .insert_header(("x-device-id", device_id.as_str()))
                .to_request(),
        )
        .await;
        assert_eq!(authorized.status(), StatusCode::OK);
        let invalid = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(uri)
                .peer_addr("203.0.113.9:1234".parse().unwrap())
                .insert_header((header::HOST, "bamboo.example.com"))
                .insert_header((header::AUTHORIZATION, "Bearer invalid"))
                .insert_header(("x-device-id", device_id.as_str()))
                .to_request(),
        )
        .await;
        assert_eq!(invalid.status(), StatusCode::UNAUTHORIZED);
        let codex = state.codex_run_tokens.issue(&child.id).unwrap();
        // Independently mounted handler must preserve the same restriction as
        // the canonical middleware, including lowercase and token whitespace.
        let direct = test::init_service(
            App::new()
                .app_data(state.clone())
                .route("/api/v1/actors/{root_id}/snapshot", web::get().to(handler)),
        )
        .await;
        for prefix in ["Bearer ", "bearer ", "Bearer   "] {
            let request = || {
                test::TestRequest::get()
                    .uri(uri)
                    .peer_addr("127.0.0.1:1234".parse().unwrap())
                    .insert_header((header::HOST, "localhost"))
                    .insert_header((header::AUTHORIZATION, format!("{prefix}{}", codex.token)))
                    .to_request()
            };
            let denied = test::call_service(&direct, request()).await;
            assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
            let denied_body: serde_json::Value = test::read_body_json(denied).await;
            assert_eq!(
                denied_body,
                serde_json::json!({
                    "schema_version": ACTOR_SNAPSHOT_SCHEMA_VERSION,
                    "error": crate::error::error_value("host_authentication_required")
                })
            );
            let canonical = test::call_service(&app, request()).await;
            assert_eq!(canonical.status(), StatusCode::UNAUTHORIZED);
        }
        let forged = test::call_service(
            &app,
            test::TestRequest::get()
                .uri("/api/v1/actors/public-root/snapshot?requesterId=public-root")
                .peer_addr("127.0.0.1:1234".parse().unwrap())
                .insert_header((header::HOST, "localhost"))
                .to_request(),
        )
        .await;
        assert_eq!(forged.status(), StatusCode::BAD_REQUEST);
        let forged_body: serde_json::Value = test::read_body_json(forged).await;
        assert_eq!(
            forged_body,
            serde_json::json!({
                "schema_version": ACTOR_SNAPSHOT_SCHEMA_VERSION,
                "error": crate::error::error_value("invalid_selector")
            })
        );
    }

    #[actix_web::test]
    async fn corrupt_and_unsupported_authority_return_private_typed_errors_without_writes() {
        let _key = bamboo_config::encryption::set_test_encryption_key([0x37; 32]);
        let bootstrap = tempfile::tempdir().unwrap();
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().canonicalize().unwrap();
        let mut state = AppState::new(bootstrap.path().canonicalize().unwrap())
            .await
            .unwrap();
        // Startup FTS maintenance retains its original empty store. Give the
        // real gateway a separate real store so it cannot race this witness.
        let store = std::sync::Arc::new(
            bamboo_storage::SessionStoreV2::new(home.clone())
                .await
                .unwrap(),
        );
        state.storage = store.clone();
        state.session_store = store;
        let state = web::Data::new(state);
        let root = Session::new("private-error-root", "PRIVATE-MODEL");
        state.session_store.save_session(&root).await.unwrap();
        state.session_store.flush_search_index().await;
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(crate::agent_routes),
        )
        .await;
        let request = || {
            test::TestRequest::get()
                .uri("/api/v1/actors/private-error-root/snapshot")
                .peer_addr("127.0.0.1:1234".parse().unwrap())
                .insert_header((header::HOST, "localhost"))
                .to_request()
        };
        let healthy = test::call_service(&app, request()).await;
        assert_eq!(healthy.status(), StatusCode::OK);
        let source = home.join("sessions/private-error-root/session.json");
        let runtime = home.join("sessions/private-error-root/runtime.json");
        let main_before = std::fs::read(&source).unwrap();
        let runtime_before = std::fs::read(&runtime).unwrap();
        for (path, replacement, expected_status, expected_code) in [
            (
                &runtime,
                b"{\"lease_owner\":\"PRIVATE-HOST\",\"endpoint\":\"PRIVATE-ENDPOINT\",".to_vec(),
                StatusCode::CONFLICT,
                "inconsistent_authority",
            ),
            (
                &source,
                serde_json::to_vec(&root).unwrap(), // Unsupported unframed legacy Main.
                StatusCode::NOT_IMPLEMENTED,
                "unsupported_authority",
            ),
        ] {
            std::fs::write(&source, &main_before).unwrap();
            std::fs::write(&runtime, &runtime_before).unwrap();
            std::fs::write(path, replacement).unwrap();
            let before = snapshot_durable_files(&home);
            let response = test::call_service(&app, request()).await;
            assert_eq!(response.status(), expected_status);
            assert!(response.headers().get(header::ETAG).is_none());
            let bytes = test::read_body(response).await;
            let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(
                body,
                serde_json::json!({
                    "schema_version": ACTOR_SNAPSHOT_SCHEMA_VERSION,
                    "error": crate::error::error_value(expected_code)
                })
            );
            let text = std::str::from_utf8(&bytes).unwrap();
            for private in ["PRIVATE", root.id.as_str(), home.to_str().unwrap()] {
                assert!(!text.contains(private));
            }
            state.session_store.flush_search_index().await;
            assert_eq!(snapshot_durable_files(&home), before);
        }
        std::fs::write(&source, main_before).unwrap();
        std::fs::write(&runtime, runtime_before).unwrap();
        let restored = test::call_service(&app, request()).await;
        assert_eq!(restored.status(), StatusCode::OK);
    }

    #[actix_web::test]
    async fn snapshot_v2_negotiation_is_opt_in_with_isolated_etags_and_native_none_placement() {
        use bamboo_domain::{
            ActorActivationClaim, ActorControlPlaneBinding, ActorDirectoryPort,
            ActorPlacementIntent, ChildActorRuntimeSave, ChildActorRuntimeWrite,
        };
        let _key = bamboo_config::encryption::set_test_encryption_key([0x37; 32]);
        let temp = tempfile::tempdir().unwrap();
        let state = web::Data::new(
            AppState::new(temp.path().canonicalize().unwrap())
                .await
                .unwrap(),
        );
        let root = Session::new("negotiation-root", "PRIVATE-MODEL");
        let mut child =
            Session::new_child_of("negotiation-child", &root, "PRIVATE-MODEL", "Native Child");
        state.session_store.save_session(&root).await.unwrap();
        state.session_store.save_session(&child).await.unwrap();
        state.session_store.ensure_actor(&child.id).await.unwrap();
        state
            .session_store
            .bind_control_plane(
                &child.id,
                ActorControlPlaneBinding {
                    policy_revision: None,
                    placement_intent: Some(ActorPlacementIntent::Local),
                },
            )
            .await
            .unwrap();
        let now = chrono::Utc::now();
        let activation = state
            .session_store
            .claim_activation(&ActorActivationClaim {
                actor_id: child.id.clone(),
                run_id: "PRIVATE-RUN".into(),
                lease_owner: "PRIVATE-HOST".into(),
                lease_expires_at: now + chrono::Duration::minutes(5),
                inbox_generation: 0,
                placement_ref: None,
                now,
            })
            .await
            .unwrap();
        state
            .session_store
            .start_activation(&activation.fence(), chrono::Utc::now())
            .await
            .unwrap();
        for (key, value) in [
            ("runtime.local_task.state", "running".to_string()),
            ("runtime.local_task.run_id", activation.run_id.clone()),
            (
                "runtime.local_task.child_birth",
                child.created_at.to_rfc3339(),
            ),
            (
                "runtime.local_task.observed_at",
                chrono::Utc::now().to_rfc3339(),
            ),
            ("runtime.kind", "external".into()),
            ("external.protocol", "actor".into()),
            ("external.agent_id", "local-actor".into()),
        ] {
            child.metadata.insert(key.into(), value);
        }
        let owner = ChildActorRuntimeWrite::from_session(activation.fence(), &child).unwrap();
        state
            .session_store
            .save_child_actor_runtime(ChildActorRuntimeSave {
                owner: &owner,
                session: &mut child,
                runtime_only: true,
                inherited: None,
                finalized_wait: None,
                input: None,
                publish: std::sync::Arc::new(|_| {}),
            })
            .await
            .unwrap();
        let expected = state
            .session_store
            .actor_subtree_snapshot(
                ActorSnapshotPrincipal::host_owner(),
                &root.id,
                &child.id,
                ActorSnapshotLimits::default(),
            )
            .await
            .unwrap();
        let expected_bytes = serde_json::to_vec(&expected).unwrap();
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .route("/api/v1/actors/{root_id}/snapshot", web::get().to(handler)),
        )
        .await;
        let uri = "/api/v1/actors/negotiation-root/snapshot?subtree_id=negotiation-child";
        let request = || {
            test::TestRequest::get()
                .uri(uri)
                .peer_addr("127.0.0.1:1234".parse().unwrap())
                .insert_header((header::HOST, "localhost"))
        };
        let response = test::call_service(&app, request().to_request()).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers().get(header::VARY).unwrap(), "Accept");
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "application/json"
        );
        let v1_etag = response
            .headers()
            .get(header::ETAG)
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned();
        assert_eq!(
            test::read_body(response).await.as_ref(),
            expected_bytes.as_slice()
        );
        assert!(v1_etag.starts_with("\"as1-"));
        for accept in [
            "application/json",
            "*/*",
            "application/vnd.bamboo.actor-snapshot.v3+json",
            "application/vnd.bamboo.actor-snapshot.v2+json;q=0",
            "application/vnd.bamboo.actor-snapshot.v2+json;q=0.5, application/json;q=0.9",
            "application/vnd.bamboo.actor-snapshot.v2+json;q=NaN",
        ] {
            let response = test::call_service(
                &app,
                request()
                    .insert_header((header::ACCEPT, accept))
                    .to_request(),
            )
            .await;
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(
                response.headers().get(header::ETAG).unwrap(),
                v1_etag.as_str()
            );
            assert_eq!(
                test::read_body(response).await.as_ref(),
                expected_bytes.as_slice()
            );
        }
        let accept = "application/vnd.bamboo.actor-snapshot.v2+json, application/json;q=0.9";
        let response = test::call_service(
            &app,
            request()
                .insert_header((header::ACCEPT, accept))
                .insert_header((header::IF_NONE_MATCH, v1_etag.as_str()))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers().get(header::VARY).unwrap(), "Accept");
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            ACTOR_SNAPSHOT_V2_MEDIA_TYPE
        );
        let v2_etag = response
            .headers()
            .get(header::ETAG)
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned();
        assert!(v2_etag.starts_with("\"as2-"));
        let body: serde_json::Value = test::read_body_json(response).await;
        assert_eq!(body["schema_version"], 2);
        assert_eq!(body["nodes"][0]["placement_class"], serde_json::Value::Null);
        assert_eq!(body["nodes"][0]["local_task"]["state"], "running");
        assert_eq!(
            body["nodes"][0]["local_task"]["reason"],
            serde_json::Value::Null
        );
        assert_eq!(body["nodes"][0]["activation"]["status"], "running");
        assert_eq!(
            body["stream_cursor"],
            serde_json::json!(expected.stream_cursor)
        );
        assert!((30001..=300000).contains(&body["refresh_after_ms"].as_u64().unwrap()));
        assert!(!body.to_string().contains("PRIVATE"));
        for (media, tag, content_type) in [
            ("application/json", &v1_etag, "application/json"),
            (accept, &v2_etag, ACTOR_SNAPSHOT_V2_MEDIA_TYPE),
        ] {
            let response = test::call_service(
                &app,
                request()
                    .insert_header((header::ACCEPT, media))
                    .insert_header((header::IF_NONE_MATCH, tag.as_str()))
                    .to_request(),
            )
            .await;
            assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
            assert_eq!(response.headers().get(header::ETAG).unwrap(), tag.as_str());
            assert_eq!(response.headers().get(header::VARY).unwrap(), "Accept");
            assert_eq!(
                response.headers().get(header::CONTENT_TYPE).unwrap(),
                content_type
            );
            assert!(test::read_body(response).await.is_empty());
        }
        let cross_version = test::call_service(
            &app,
            request()
                .insert_header((header::IF_NONE_MATCH, v2_etag))
                .to_request(),
        )
        .await;
        assert_eq!(cross_version.status(), StatusCode::OK);
        let invalid = test::call_service(
            &app,
            request()
                .uri("/api/v1/actors/negotiation-root/snapshot?schema_version=2")
                .insert_header((header::ACCEPT, accept))
                .to_request(),
        )
        .await;
        assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            test::read_body_json::<serde_json::Value, _>(invalid).await["schema_version"],
            2
        );
        let denied = test::call_service(
            &app,
            request()
                .peer_addr("203.0.113.9:1234".parse().unwrap())
                .insert_header((header::ACCEPT, accept))
                .to_request(),
        )
        .await;
        assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(denied.headers().get(header::VARY).unwrap(), "Accept");
    }

    fn snapshot_durable_files(
        home: &std::path::Path,
    ) -> std::collections::BTreeMap<std::path::PathBuf, Vec<u8>> {
        fn walk(
            directory: &std::path::Path,
            files: &mut std::collections::BTreeMap<std::path::PathBuf, Vec<u8>>,
        ) {
            for entry in std::fs::read_dir(directory).unwrap() {
                let entry = entry.unwrap();
                let path = entry.path();
                if entry.file_type().unwrap().is_dir() {
                    files.insert(path.clone(), vec![]);
                    walk(&path, files);
                } else {
                    assert!(entry.file_type().unwrap().is_file());
                    files.insert(path.clone(), std::fs::read(&path).unwrap());
                }
            }
        }
        let mut files = Default::default();
        walk(home, &mut files);
        files
    }
}
