//! HostOwner administration of one durable SessionInbox lifetime.

use actix_web::{http::header, web, HttpRequest, HttpResponse};
use bamboo_domain::{SessionInboxAdministrationPrincipal, SessionInboxError, SessionMessageId};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::json;

use crate::app_state::AppState;

const SCHEMA_VERSION: u32 = 1;
const DEFAULT_LIMIT: usize = 50;
const MAX_LIMIT: usize = 100;

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct InspectQuery {
    limit: Option<usize>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetryRequest {
    generation: u64,
    /// Echo the birth returned by inspection so an id reused after deletion
    /// cannot turn a stale operator action into a grant for its replacement.
    session_created_at: DateTime<Utc>,
}

fn error(status: actix_web::http::StatusCode, code: &'static str) -> HttpResponse {
    HttpResponse::build(status).json(json!({
        "schema_version": SCHEMA_VERSION,
        "error": crate::error::error_value(code),
    }))
}

fn inbox_error(error_value: SessionInboxError) -> HttpResponse {
    use actix_web::http::StatusCode;
    match error_value {
        SessionInboxError::TargetNotFound(_) => error(StatusCode::NOT_FOUND, "session_not_found"),
        SessionInboxError::InvalidClaim(_) => {
            error(StatusCode::CONFLICT, "dead_letter_unavailable_or_stale")
        }
        other => {
            tracing::error!(error = %other, "SessionInbox dead-letter administration failed");
            error(
                StatusCode::SERVICE_UNAVAILABLE,
                "dead_letter_storage_unavailable",
            )
        }
    }
}

async fn principal_for(
    state: &AppState,
    target: &str,
) -> Result<(SessionInboxAdministrationPrincipal, DateTime<Utc>), HttpResponse> {
    use actix_web::http::StatusCode;
    match state.storage.load_session(target).await {
        Ok(Some(session)) if session.id == target => Ok((
            SessionInboxAdministrationPrincipal::authenticated_host_owner_for(&session),
            session.created_at,
        )),
        Ok(None) => Err(error(StatusCode::NOT_FOUND, "session_not_found")),
        Ok(Some(_)) => Err(error(StatusCode::CONFLICT, "session_identity_conflict")),
        Err(cause) => {
            tracing::error!(session_id = %target, error = %cause, "failed to load dead-letter target");
            Err(error(
                StatusCode::SERVICE_UNAVAILABLE,
                "session_storage_unavailable",
            ))
        }
    }
}

/// `GET /api/v1/sessions/{session_id}/dead-letters?limit=50`.
/// Only bounded selectors and diagnostic codes are returned; payloads and
/// consumer identities remain private to the Inbox.
pub async fn inspect(
    state: web::Data<AppState>,
    path: web::Path<String>,
    req: HttpRequest,
) -> HttpResponse {
    use actix_web::http::StatusCode;
    if !super::actor_snapshot::host_owner_authorized(&state, &req).await {
        return error(StatusCode::UNAUTHORIZED, "host_authentication_required");
    }
    let query = match web::Query::<InspectQuery>::from_query(req.query_string()) {
        Ok(query) => query.into_inner(),
        Err(_) => return error(StatusCode::BAD_REQUEST, "invalid_dead_letter_query"),
    };
    let limit = query.limit.unwrap_or(DEFAULT_LIMIT);
    if !(1..=MAX_LIMIT).contains(&limit) {
        return error(StatusCode::BAD_REQUEST, "invalid_dead_letter_limit");
    }
    let target = path.into_inner();
    let (principal, session_created_at) = match principal_for(&state, &target).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    match state
        .session_inbox
        .inspect_dead_letters(&target, limit, &principal)
        .await
    {
        Ok(entries) => HttpResponse::Ok()
            .insert_header((header::CACHE_CONTROL, "private, no-store"))
            .json(json!({
                "schema_version": SCHEMA_VERSION,
                "session_id": target,
                "session_created_at": session_created_at,
                "dead_letters": entries.into_iter().map(|entry| json!({
                    "id": entry.id,
                    "generation": entry.generation,
                    "failure_count": entry.failure_count,
                    "last_error_code": entry.last_error_code,
                    "dead_lettered_at": entry.dead_lettered_at,
                })).collect::<Vec<_>>(),
            })),
        Err(cause) => inbox_error(cause),
    }
}

/// `POST /api/v1/sessions/{session_id}/dead-letters/{message_id}/retry`.
/// The exact id, generation and birth are mandatory. Rotation into the pending
/// queue is durable before this handler asks the activation coordinator to
/// reconcile claimable work. A wake failure leaves recovery pending.
pub async fn retry(
    state: web::Data<AppState>,
    path: web::Path<(String, String)>,
    body: web::Bytes,
    req: HttpRequest,
) -> HttpResponse {
    use actix_web::http::StatusCode;
    if !super::actor_snapshot::host_owner_authorized(&state, &req).await {
        return error(StatusCode::UNAUTHORIZED, "host_authentication_required");
    }
    if !req
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|media_type| media_type.trim().eq_ignore_ascii_case("application/json"))
    {
        return error(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "json_content_type_required",
        );
    }
    if body.len() > 4096 {
        return error(
            StatusCode::PAYLOAD_TOO_LARGE,
            "dead_letter_retry_body_too_large",
        );
    }
    let body = match serde_json::from_slice::<RetryRequest>(&body) {
        Ok(body) => body,
        Err(_) => return error(StatusCode::BAD_REQUEST, "invalid_dead_letter_retry_body"),
    };
    let (target, raw_id) = path.into_inner();
    let id = match SessionMessageId::parse(raw_id) {
        Ok(id) => id,
        Err(_) => return error(StatusCode::BAD_REQUEST, "invalid_message_id"),
    };
    if body.generation == 0 {
        return error(StatusCode::BAD_REQUEST, "invalid_dead_letter_generation");
    }
    let (principal, session_created_at) = match principal_for(&state, &target).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    if body.session_created_at != session_created_at {
        return error(StatusCode::CONFLICT, "session_lifetime_mismatch");
    }
    let now = Utc::now();
    let receipt = match state
        .session_inbox
        .retry_dead_letter(&target, &id, body.generation, now, &principal)
        .await
    {
        Ok(receipt) => receipt,
        Err(cause) => return inbox_error(cause),
    };

    // This path re-reads storage-locked claim readiness and lets the real
    // router reserve/notify a runner. A generic request_activation would skip
    // the queue's current activation policy and retry deadline.
    let handoff = match state
        .session_activation_router
        .reconcile_wake(&target, Utc::now())
        .await
    {
        Ok(result) if result.disposition.is_some() => "requested",
        Ok(_) => "pending",
        Err(cause) => {
            tracing::warn!(session_id = %target, error = %cause,
                "dead-letter retry committed; activation awaits durable wake reconciliation");
            "pending"
        }
    };
    HttpResponse::Accepted()
        .insert_header((header::CACHE_CONTROL, "private, no-store"))
        .json(json!({
            "schema_version": SCHEMA_VERSION,
            "session_id": target,
            "session_created_at": session_created_at,
            "id": receipt.id,
            "generation": receipt.generation,
            "status": "queued",
            "activation_handoff": handoff,
        }))
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
mod tests {
    use super::*;
    use actix_web::{http::StatusCode, test, App};
    use bamboo_domain::{
        Session, SessionActivationPolicy, SessionInboxConsumerId, SessionInboxFailureOutcome,
        SessionInboxFailureReport, SessionInboxLeaseRequest, SessionMessageEnvelope, Storage,
    };
    use chrono::Duration;

    #[actix_web::test]
    async fn host_owner_inspects_and_retries_only_exact_dead_letter_incarnation() {
        let _key = bamboo_config::encryption::set_test_encryption_key([0x5a; 32]);
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().canonicalize().unwrap();
        let state = web::Data::new(AppState::new(home).await.unwrap());
        let session = Session::new("dead-letter-target", "model");
        state.session_store.save_session(&session).await.unwrap();
        let envelope = SessionMessageEnvelope::user_input(&session.id, "private poison payload");
        let receipt = state
            .session_inbox
            .deliver_with_activation_intent(
                &envelope,
                SessionActivationPolicy::RespectSpecificWait,
                None,
            )
            .await
            .unwrap();

        // Keep the synthetic failure timeline in the past so the HTTP retry
        // uses its real wall clock while preserving every lease/backoff CAS.
        let mut now = Utc::now() - Duration::minutes(10);
        let mut consumer_ids = Vec::new();
        for failure_count in 1..=3 {
            let request = SessionInboxLeaseRequest {
                consumer: SessionInboxConsumerId::new(),
                now,
                duration: Duration::seconds(10),
            };
            consumer_ids.push(request.consumer.as_str().to_owned());
            let claim = state
                .session_inbox
                .claim_owned(&session.id, 1, None, &request)
                .await
                .unwrap()
                .remove(0);
            let failed_at = now + Duration::seconds(1);
            let outcome = state
                .session_inbox
                .fail_owned(
                    &session.id,
                    &claim,
                    &SessionInboxFailureReport {
                        now: failed_at,
                        error_code: "consumer_rejected".into(),
                    },
                )
                .await
                .unwrap();
            if failure_count == 3 {
                assert_eq!(
                    outcome,
                    SessionInboxFailureOutcome::DeadLettered { failure_count }
                );
            } else {
                let SessionInboxFailureOutcome::RetryScheduled { retry_after, .. } = outcome else {
                    panic!("expected bounded retry");
                };
                now = retry_after;
            }
        }

        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(crate::agent_routes),
        )
        .await;
        let listing_uri = "/api/v1/sessions/dead-letter-target/dead-letters?limit=1";
        let local_get = || {
            test::TestRequest::get()
                .uri(listing_uri)
                .peer_addr("127.0.0.1:1234".parse().unwrap())
                .insert_header((header::HOST, "localhost"))
                .to_request()
        };
        let remote = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(listing_uri)
                .peer_addr("203.0.113.9:1234".parse().unwrap())
                .insert_header((header::HOST, "localhost"))
                .insert_header(("x-forwarded-for", "127.0.0.1"))
                .to_request(),
        )
        .await;
        assert_eq!(remote.status(), StatusCode::UNAUTHORIZED);
        let listing = test::call_service(&app, local_get()).await;
        assert_eq!(listing.status(), StatusCode::OK);
        assert_eq!(
            listing.headers().get(header::CACHE_CONTROL).unwrap(),
            "private, no-store"
        );
        let listing_body = test::read_body(listing).await;
        let listing_text = String::from_utf8(listing_body.to_vec()).unwrap();
        assert!(!listing_text.contains("private poison payload"));
        for consumer_id in consumer_ids {
            assert!(!listing_text.contains(&consumer_id));
        }
        let listing: serde_json::Value = serde_json::from_str(&listing_text).unwrap();
        assert_eq!(listing["session_id"], session.id);
        assert_eq!(listing["dead_letters"].as_array().unwrap().len(), 1);
        assert_eq!(listing["dead_letters"][0]["id"], envelope.id.as_str());
        assert_eq!(listing["dead_letters"][0]["generation"], receipt.generation);
        assert_eq!(listing["dead_letters"][0]["failure_count"], 3);
        assert_eq!(
            listing["dead_letters"][0]["last_error_code"],
            "consumer_rejected"
        );
        let birth = listing["session_created_at"].clone();
        let retry_uri = format!(
            "/api/v1/sessions/{}/dead-letters/{}/retry",
            session.id,
            envelope.id.as_str()
        );
        let post = |generation: u64, birth: serde_json::Value| {
            test::TestRequest::post()
                .uri(&retry_uri)
                .peer_addr("127.0.0.1:1234".parse().unwrap())
                .insert_header((header::HOST, "localhost"))
                .set_json(json!({
                    "generation": generation,
                    "session_created_at": birth,
                }))
                .to_request()
        };
        let stale_birth = (session.created_at + Duration::seconds(1)).to_rfc3339();
        assert_eq!(
            test::call_service(&app, post(receipt.generation, json!(stale_birth)))
                .await
                .status(),
            StatusCode::CONFLICT
        );
        assert_eq!(
            test::call_service(&app, post(receipt.generation + 1, birth.clone()))
                .await
                .status(),
            StatusCode::CONFLICT
        );
        let wrong_id = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/sessions/dead-letter-target/dead-letters/other-id/retry")
                .peer_addr("127.0.0.1:1234".parse().unwrap())
                .insert_header((header::HOST, "localhost"))
                .set_json(json!({
                    "generation": receipt.generation,
                    "session_created_at": birth,
                }))
                .to_request(),
        )
        .await;
        assert_eq!(wrong_id.status(), StatusCode::CONFLICT);
        let accepted =
            test::call_service(&app, post(receipt.generation, json!(session.created_at))).await;
        assert_eq!(accepted.status(), StatusCode::ACCEPTED);
        let accepted_body: serde_json::Value = test::read_body_json(accepted).await;
        assert_eq!(accepted_body["status"], "queued");
        assert_eq!(accepted_body["id"], envelope.id.as_str());
        assert_eq!(accepted_body["generation"], receipt.generation);
        assert!(matches!(
            accepted_body["activation_handoff"].as_str(),
            Some("requested" | "pending")
        ));
        let after = test::call_service(&app, local_get()).await;
        assert_eq!(after.status(), StatusCode::OK);
        let after_body: serde_json::Value = test::read_body_json(after).await;
        assert!(after_body["dead_letters"].as_array().unwrap().is_empty());
    }

    #[actix_web::test]
    async fn dead_letter_routes_reject_untrusted_or_unbounded_requests() {
        let _key = bamboo_config::encryption::set_test_encryption_key([0x5b; 32]);
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().canonicalize().unwrap();
        let state = web::Data::new(AppState::new(home).await.unwrap());
        let session = Session::new("dead-letter-validation", "model");
        state.session_store.save_session(&session).await.unwrap();
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(crate::agent_routes),
        )
        .await;
        for suffix in ["?limit=0", "?limit=101", "?limit=1&requesterId=owner"] {
            let response = test::call_service(
                &app,
                test::TestRequest::get()
                    .uri(&format!(
                        "/api/v1/sessions/{}/dead-letters{suffix}",
                        session.id
                    ))
                    .peer_addr("127.0.0.1:1234".parse().unwrap())
                    .insert_header((header::HOST, "localhost"))
                    .to_request(),
            )
            .await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{suffix}");
        }
        let absent = test::call_service(
            &app,
            test::TestRequest::get()
                .uri("/api/v1/sessions/missing/dead-letters")
                .peer_addr("127.0.0.1:1234".parse().unwrap())
                .insert_header((header::HOST, "localhost"))
                .to_request(),
        )
        .await;
        assert_eq!(absent.status(), StatusCode::NOT_FOUND);
        let retry_uri = format!("/api/v1/sessions/{}/dead-letters/unknown/retry", session.id);
        let unauthorized = test::call_service(
            &app,
            test::TestRequest::post()
                .uri(&retry_uri)
                .peer_addr("203.0.113.9:1234".parse().unwrap())
                .insert_header((header::HOST, "localhost"))
                .set_json(json!({
                    "generation": 1,
                    "session_created_at": session.created_at,
                }))
                .to_request(),
        )
        .await;
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
        let malformed_unauthorized = test::call_service(
            &app,
            test::TestRequest::post()
                .uri(&retry_uri)
                .peer_addr("203.0.113.9:1234".parse().unwrap())
                .insert_header((header::HOST, "localhost"))
                .insert_header((header::CONTENT_TYPE, "application/json"))
                .set_payload("{".as_bytes())
                .to_request(),
        )
        .await;
        assert_eq!(malformed_unauthorized.status(), StatusCode::UNAUTHORIZED);
        let non_json = test::call_service(
            &app,
            test::TestRequest::post()
                .uri(&retry_uri)
                .peer_addr("127.0.0.1:1234".parse().unwrap())
                .insert_header((header::HOST, "localhost"))
                .insert_header((header::CONTENT_TYPE, "text/plain"))
                .set_payload(format!(
                    "{{\"generation\":1,\"session_created_at\":\"{}\"}}",
                    session.created_at.to_rfc3339()
                ))
                .to_request(),
        )
        .await;
        assert_eq!(non_json.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
        let invalid = test::call_service(
            &app,
            test::TestRequest::post()
                .uri(&retry_uri)
                .peer_addr("127.0.0.1:1234".parse().unwrap())
                .insert_header((header::HOST, "localhost"))
                .set_json(json!({
                    "generation": 0,
                    "session_created_at": session.created_at,
                }))
                .to_request(),
        )
        .await;
        assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
    }
}
