//! Owner-selected existing Roots; no model, credentials or production grants.
use actix_web::{
    http::{Method, StatusCode},
    test, web, App, HttpMessage,
};
use bamboo_domain::{
    Message, ProjectId, Session, SupervisorManagementRequest, SupervisorReference,
};
use bamboo_engine::session_app::supervisor::SupervisorSessionService;
use serde_json::{json, Value};
use std::sync::Arc;

use crate::app_state::supervisor_owner_authorization::test_support::ApprovedHostIntent;
use crate::{app_state::AppState, routes::configure_routes};

const URI: &str = "/api/v1/supervisor/default/sessions";
struct Fixture {
    home: tempfile::TempDir,
    state: web::Data<AppState>,
    supervisor: SupervisorReference,
    project: ProjectId,
}
impl Fixture {
    async fn new(exact_authorizer: bool) -> Self {
        let home = tempfile::tempdir().unwrap();
        let mut state = Box::pin(AppState::new(home.path().into())).await.unwrap();
        if exact_authorizer {
            state = state.with_supervisor_project_scope_authorizer(Arc::new(ApprovedHostIntent));
        }
        let state = web::Data::new(state);
        let supervisor = SupervisorReference::from(
            &SupervisorSessionService::new(state.storage.clone())
                .get_or_create_default("unused-model")
                .await
                .unwrap(),
        );
        let project = state
            .project_store
            .create("Selected Project", None)
            .unwrap()
            .id;
        for id in ["target-a", "target-b"] {
            let mut target = Session::new(id, format!("PRIVATE-{id}-model"));
            target.set_project_id_meta(project.to_string());
            target.set_workspace_path_meta(format!("/PRIVATE/{id}/workspace"));
            target.add_message(Message::user(format!("PRIVATE {id} original history")));
            state.storage.save_session(&target).await.unwrap();
        }
        Self {
            home,
            state,
            supervisor,
            project,
        }
    }
    async fn configure(&self) {
        SupervisorSessionService::new(self.state.storage.clone())
            .configure_project_scope(&self.supervisor, 0, [self.project.clone()].into())
            .await
            .unwrap();
    }
    fn body(&self, revision: u64, enabled: bool) -> Value {
        json!({"incarnation_id":self.supervisor.incarnation_id,
            "expected_state_revision":revision,"enabled":enabled})
    }
    async fn call(
        &self,
        method: Method,
        target: Option<&str>,
        body: Option<Value>,
    ) -> (StatusCode, Value) {
        call(self.state.clone(), method, target, body, false, false, None).await
    }
    async fn target_bytes(&self, id: &str) -> (Vec<u8>, Vec<u8>) {
        let directory = self.home.path().join("sessions").join(id);
        (
            tokio::fs::read(directory.join("session.json"))
                .await
                .unwrap(),
            tokio::fs::read(directory.join("runtime.json"))
                .await
                .unwrap(),
        )
    }
}
async fn call(
    state: web::Data<AppState>,
    method: Method,
    target: Option<&str>,
    body: Option<Value>,
    remote: bool,
    worker: bool,
    approved: Option<SupervisorManagementRequest>,
) -> (StatusCode, Value) {
    let mut factory = App::new().app_data(state);
    if let Some(intent) = approved {
        factory = factory.app_data(web::Data::new(intent));
    }
    let app = test::init_service(
        factory
            .wrap_fn(move |request, service| {
                use actix_web::dev::Service;
                if worker {
                    request
                        .extensions_mut()
                        .insert(crate::codex_run_tokens::CodexRunAuthContext {
                            session_id: "worker".into(),
                        });
                }
                service.call(request)
            })
            .configure(configure_routes),
    )
    .await;
    let uri = target.map_or_else(|| URI.to_string(), |id| format!("{URI}/{id}"));
    let mut request = test::TestRequest::default()
        .method(method)
        .uri(&uri)
        .peer_addr(
            if remote {
                "192.0.2.1:12345"
            } else {
                "127.0.0.1:12345"
            }
            .parse()
            .unwrap(),
        );
    if let Some(body) = body {
        request = request.set_json(body);
    }
    let response = test::call_service(&app, request.to_request()).await;
    let status = response.status();
    let bytes = test::read_body(response).await;
    let value = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| json!({"non_json":String::from_utf8_lossy(&bytes)}));
    (status, value)
}

#[actix_web::test]
async fn managed_sessions_owner_attach_detach_is_cas_bound_and_preserves_targets_across_restart() {
    let f = Fixture::new(false).await;
    let before = f.target_bytes("target-a").await;
    let (status, empty) = f.call(Method::GET, None, None).await;
    assert_eq!(status, StatusCode::OK, "{empty}");
    assert_eq!(empty["capabilities"]["managed_sessions_http_v1"], true);
    assert_eq!(empty["scope"]["state_revision"], 0);
    assert_eq!(empty["sessions"], json!([]));
    f.configure().await;
    let original = f.body(1, true);
    let (status, attached) = f
        .call(Method::PUT, Some("target-a"), Some(original.clone()))
        .await;
    assert_eq!(status, StatusCode::OK, "{attached}");
    assert_eq!(attached["receipt"]["state_revision"], 2);
    assert_eq!(attached["receipt"]["link_revision"], 1);
    assert_eq!(attached["receipt"]["changed"], true);
    let (status, stale) = f.call(Method::PUT, Some("target-a"), Some(original)).await;
    assert_eq!(status, StatusCode::CONFLICT, "{stale}");
    assert_eq!(stale["code"], "scope_revision_conflict");
    let (status, noop) = f
        .call(Method::PUT, Some("target-a"), Some(f.body(2, true)))
        .await;
    assert_eq!(status, StatusCode::OK, "{noop}");
    assert_eq!(noop["receipt"]["changed"], false);
    assert_eq!(noop["receipt"]["state_revision"], 2);
    let (status, observed) = f.call(Method::GET, None, None).await;
    assert_eq!(status, StatusCode::OK, "{observed}");
    assert_eq!(observed["sessions"].as_array().unwrap().len(), 1);
    assert_eq!(observed["sessions"][0]["session_id"], "target-a");
    assert_eq!(observed["sessions"][0]["authorized"], true);
    assert_eq!(observed["sessions"][0]["target_metadata_version"], 0);
    assert!(!observed.to_string().contains("PRIVATE"));
    let (status, detached) = f
        .call(Method::PUT, Some("target-a"), Some(f.body(2, false)))
        .await;
    assert_eq!(status, StatusCode::OK, "{detached}");
    assert_eq!(detached["receipt"]["state_revision"], 3);
    let (status, observed) = f.call(Method::GET, None, None).await;
    assert_eq!(status, StatusCode::OK, "{observed}");
    assert_eq!(observed["sessions"][0]["enabled"], false);
    assert_eq!(observed["sessions"][0]["authorized"], false);
    assert_eq!(f.target_bytes("target-a").await, before);
    assert!(!f.state.config.read().await.features.ticket_mutation);
    assert!(!f.state.config.read().await.features.ticket_dispatch);
    assert!(!f.home.path().join("tickets").exists());
    f.state.shutdown().await;
    let recovered = web::Data::new(Box::pin(AppState::new(f.home.path().into())).await.unwrap());
    let (status, restored) = call(
        recovered.clone(),
        Method::GET,
        None,
        None,
        false,
        false,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{restored}");
    assert_eq!(restored, observed);
    assert_eq!(f.target_bytes("target-a").await, before);
    recovered.shutdown().await;
}

#[actix_web::test]
async fn managed_sessions_denied_targets_and_invalid_requests_have_no_side_effects() {
    let f = Fixture::new(false).await;
    f.configure().await;
    let other_project = f
        .state
        .project_store
        .create("Not selected", None)
        .unwrap()
        .id;
    let mut other = Session::new("foreign-root", "unused-model");
    other.set_project_id_meta(other_project.to_string());
    f.state.storage.save_session(&other).await.unwrap();
    let parent = f
        .state
        .storage
        .load_session("target-a")
        .await
        .unwrap()
        .unwrap();
    let child = Session::new_child_of("not-a-root", &parent, "model", "Child");
    f.state.storage.save_session(&child).await.unwrap();
    let before = f.target_bytes("target-a").await;
    for (target, mut body, expected) in [
        ("foreign-root", f.body(1, true), StatusCode::FORBIDDEN),
        // The canonical Root reader never consults the index or a Child path.
        ("not-a-root", f.body(1, true), StatusCode::NOT_FOUND),
        ("missing-root", f.body(1, true), StatusCode::NOT_FOUND),
        (
            "bamboo-default-supervisor",
            f.body(1, true),
            StatusCode::FORBIDDEN,
        ),
        ("bad..target", f.body(1, true), StatusCode::BAD_REQUEST),
        ("target-a", f.body(1, true), StatusCode::FORBIDDEN),
    ] {
        if target == "target-a" {
            body["incarnation_id"] = json!(uuid::Uuid::new_v4());
        }
        let (status, response) = f.call(Method::PUT, Some(target), Some(body)).await;
        assert_eq!(status, expected, "{target}: {response}");
    }
    let mut extra = f.body(1, true);
    extra["approved"] = true.into();
    assert_eq!(
        f.call(Method::PUT, Some("target-a"), Some(extra)).await.0,
        StatusCode::BAD_REQUEST
    );
    let (_, observed) = f.call(Method::GET, None, None).await;
    assert_eq!(observed["scope"]["state_revision"], 1);
    assert_eq!(observed["sessions"], json!([]));
    assert_eq!(f.target_bytes("target-a").await, before);
    assert!(!f.home.path().join("tickets").exists());
    f.state.shutdown().await;
}

#[actix_web::test]
async fn managed_sessions_access_requires_owner_and_optional_exact_intent() {
    use bamboo_domain::SupervisorManagementMutation;
    let f = Fixture::new(true).await;
    f.configure().await;
    let before = f.target_bytes("target-a").await;
    for (remote, worker) in [(true, false), (false, true)] {
        for (method, target, body) in [
            (Method::GET, None, None),
            (Method::PUT, Some("target-a"), Some(f.body(1, true))),
        ] {
            assert_eq!(
                call(f.state.clone(), method, target, body, remote, worker, None)
                    .await
                    .0,
                StatusCode::FORBIDDEN
            );
        }
    }
    let intent = SupervisorManagementRequest {
        supervisor: f.supervisor.clone(),
        expected_state_revision: 1,
        mutation: SupervisorManagementMutation::Attach {
            target_session_id: "target-b".into(),
        },
    };
    let (status, response) = call(
        f.state.clone(),
        Method::PUT,
        Some("target-a"),
        Some(f.body(1, true)),
        false,
        false,
        Some(intent.clone()),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{response}");
    assert_eq!(response["code"], "host_owner_authorization_required");
    let (status, denied) = f
        .call(Method::PUT, Some("target-a"), Some(f.body(1, true)))
        .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{denied}");
    let (_, observed) = f.call(Method::GET, None, None).await;
    assert_eq!(observed["scope"]["state_revision"], 1);
    assert_eq!(observed["sessions"], json!([]));
    assert_eq!(f.target_bytes("target-a").await, before);
    let (status, response) = call(
        f.state.clone(),
        Method::PUT,
        Some("target-b"),
        Some(f.body(1, true)),
        false,
        false,
        Some(intent),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{response}");
    f.state.shutdown().await;
}

#[actix_web::test]
async fn managed_sessions_stale_links_need_explicit_confirmation_and_deleted_targets_can_be_revoked(
) {
    let f = Fixture::new(false).await;
    f.configure().await;
    assert_eq!(
        f.call(Method::PUT, Some("target-a"), Some(f.body(1, true)))
            .await
            .0,
        StatusCode::OK
    );
    let mut changed = f
        .state
        .storage
        .load_session("target-a")
        .await
        .unwrap()
        .unwrap();
    changed.model = "different-target-model".into();
    changed.metadata_version += 1;
    f.state.storage.save_session(&changed).await.unwrap();
    let before = f.target_bytes("target-a").await;
    let (status, observed) = f.call(Method::GET, None, None).await;
    assert_eq!(status, StatusCode::OK, "{observed}");
    assert_eq!(observed["scope"]["state_revision"], 2);
    assert_eq!(observed["sessions"][0]["enabled"], true);
    assert_eq!(observed["sessions"][0]["authorized"], false);
    assert_eq!(observed["sessions"][0]["target_metadata_version"], 0);
    assert_eq!(f.target_bytes("target-a").await, before);
    let (status, response) = f
        .call(Method::PUT, Some("target-a"), Some(f.body(2, true)))
        .await;
    assert_eq!(status, StatusCode::OK, "{response}");
    assert_eq!(response["receipt"]["link_revision"], 2);
    let (_, observed) = f.call(Method::GET, None, None).await;
    assert_eq!(observed["sessions"][0]["authorized"], true);
    assert_eq!(observed["sessions"][0]["target_metadata_version"], 1);
    f.state.storage.delete_session("target-a").await.unwrap();
    let (_, observed) = f.call(Method::GET, None, None).await;
    assert_eq!(observed["sessions"][0]["authorized"], false);
    let (status, response) = f
        .call(Method::PUT, Some("target-a"), Some(f.body(3, false)))
        .await;
    assert_eq!(status, StatusCode::OK, "{response}");
    let (_, observed) = f.call(Method::GET, None, None).await;
    assert_eq!(observed["sessions"][0]["enabled"], false);
    assert_eq!(observed["scope"]["state_revision"], 4);
    f.state.shutdown().await;
}

#[actix_web::test]
async fn managed_sessions_same_revision_race_commits_only_one_target() {
    let f = Fixture::new(false).await;
    f.configure().await;
    let (one, two) = futures::join!(
        f.call(Method::PUT, Some("target-a"), Some(f.body(1, true))),
        f.call(Method::PUT, Some("target-b"), Some(f.body(1, true))),
    );
    assert_eq!(
        [one.0, two.0]
            .iter()
            .filter(|s| **s == StatusCode::OK)
            .count(),
        1,
        "{one:?}, {two:?}"
    );
    assert_eq!(
        [one.0, two.0]
            .iter()
            .filter(|s| **s == StatusCode::CONFLICT)
            .count(),
        1,
        "{one:?}, {two:?}"
    );
    let (status, observed) = f.call(Method::GET, None, None).await;
    assert_eq!(status, StatusCode::OK, "{observed}");
    assert_eq!(observed["scope"]["state_revision"], 2);
    assert_eq!(observed["sessions"].as_array().unwrap().len(), 1);
    f.state.shutdown().await;
}

#[actix_web::test]
async fn managed_sessions_target_authority_error_stays_local_but_source_authority_failure_does_not()
{
    let f = Fixture::new(false).await;
    f.configure().await;
    assert_eq!(
        f.call(Method::PUT, Some("target-a"), Some(f.body(1, true)))
            .await
            .0,
        StatusCode::OK
    );
    let target_proof = f
        .home
        .path()
        .join("sessions/target-a/root-tool-authority.json");
    let damaged = b"malformed target authority";
    tokio::fs::write(&target_proof, damaged).await.unwrap();
    let before = f.target_bytes("target-a").await;
    let (status, observed) = f.call(Method::GET, None, None).await;
    assert_eq!(status, StatusCode::OK, "{observed}");
    assert_eq!(observed["sessions"][0]["enabled"], true);
    assert_eq!(observed["sessions"][0]["authorized"], false);
    assert_eq!(
        observed["sessions"][0]["authorization_error"],
        "target_authority_unavailable"
    );
    assert_eq!(f.target_bytes("target-a").await, before);
    assert_eq!(tokio::fs::read(&target_proof).await.unwrap(), damaged);
    let (status, response) = f
        .call(Method::PUT, Some("target-a"), Some(f.body(2, false)))
        .await;
    assert_eq!(status, StatusCode::OK, "{response}");
    let (status, observed) = f.call(Method::GET, None, None).await;
    assert_eq!(status, StatusCode::OK, "{observed}");
    assert_eq!(observed["sessions"][0]["enabled"], false);
    assert_eq!(observed["sessions"][0]["authorized"], false);
    assert!(observed["sessions"][0].get("authorization_error").is_none());
    assert_eq!(f.target_bytes("target-a").await, before);
    assert_eq!(tokio::fs::read(&target_proof).await.unwrap(), damaged);
    let source_proof = f
        .home
        .path()
        .join("sessions/bamboo-default-supervisor/supervisor-authority.json");
    tokio::fs::write(&source_proof, b"malformed source authority")
        .await
        .unwrap();
    let (status, error) = f.call(Method::GET, None, None).await;
    // Existing Supervisor proof failures are typed authority conflicts.
    assert_eq!(status, StatusCode::CONFLICT, "{error}");
    assert!(error.get("sessions").is_none());
    assert_eq!(f.target_bytes("target-a").await, before);
    f.state.shutdown().await;
}
