//! Plain HTTP scope contract. Authentication is injected only by this Rust test host.
use crate::{app_state::AppState, routes::configure_routes};
use actix_web::{
    http::{header, Method, StatusCode},
    test, web, App, HttpMessage,
};
use bamboo_domain::{ProjectId, SupervisorReference};
use bamboo_engine::session_app::supervisor::SupervisorSessionService;
use serde_json::{json, Value};

// No request header/JSON can create this test-only server extension.
#[derive(Clone, Copy)]
pub(super) enum TestOwner {
    Authenticated,
    Required,
    Unconfigured,
    Unavailable,
}
const URI: &str = "/api/v1/supervisor/default/scope";
const ORIGIN: &str = "http://127.0.0.1:64602";
struct Fixture {
    home: tempfile::TempDir,
    state: web::Data<AppState>,
    supervisor: SupervisorReference,
    projects: Vec<ProjectId>,
}
impl Fixture {
    async fn new() -> Self {
        let home = tempfile::tempdir().unwrap();
        let state = web::Data::new(Box::pin(AppState::new(home.path().into())).await.unwrap());
        let supervisor = SupervisorReference::from(
            &SupervisorSessionService::new(state.storage.clone())
                .get_or_create_default("unused-model")
                .await
                .unwrap(),
        );
        let projects = ["Web fixture A", "Web fixture B"]
            .map(|name| state.project_store.create(name, None).unwrap().id)
            .to_vec();
        Self {
            home,
            state,
            supervisor,
            projects,
        }
    }
    fn body(&self, revision: u64) -> Value {
        json!({"incarnation_id":self.supervisor.incarnation_id,"expected_state_revision":revision,"allowed_projects":self.projects})
    }
    async fn call(
        &self,
        method: Method,
        body: Option<Value>,
        owner: Option<TestOwner>,
        origin: Option<&str>,
    ) -> (StatusCode, Value) {
        let app = test::init_service(
            App::new()
                .app_data(self.state.clone())
                .configure(configure_routes),
        )
        .await;
        let mut request = test::TestRequest::default()
            .method(method)
            .uri(URI)
            .peer_addr("127.0.0.1:43210".parse().unwrap())
            .insert_header((header::HOST, "127.0.0.1:64602"));
        if let Some(origin) = origin {
            request = request.insert_header((header::ORIGIN, origin));
        }
        if let Some(body) = body {
            request = request.set_json(body);
        }
        let request = request.to_request();
        if let Some(owner) = owner {
            request.extensions_mut().insert(owner);
        }
        let response = test::call_service(&app, request).await;
        let status = response.status();
        let bytes = test::read_body(response).await;
        let value = serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| json!({"non_json":String::from_utf8_lossy(&bytes)}));
        (status, value)
    }
    async fn unchanged(&self) {
        let scope = SupervisorSessionService::new(self.state.storage.clone())
            .inspect_scope(&self.supervisor)
            .await
            .unwrap();
        assert_eq!(scope.state_revision, 0);
        assert!(scope.allowed_projects.is_empty());
        let authority = self
            .state
            .storage
            .load_root_authority(&self.supervisor.session_id)
            .await
            .unwrap()
            .unwrap();
        assert!(authority.messages.is_empty());
        assert!(!self.state.config.read().await.features.ticket_mutation);
        assert!(!self.state.config.read().await.features.ticket_dispatch);
        assert!(!self.home.path().join("tickets").exists());
    }
}
#[actix_web::test]
async fn plain_web_authenticated_owner_can_use_one_put_and_scope_survives_restart() {
    let f = Fixture::new().await;
    let (status, read) = f
        .call(Method::GET, None, Some(TestOwner::Authenticated), None)
        .await;
    assert_eq!(status, StatusCode::OK, "{read}");
    assert_eq!(read["capabilities"]["project_scope_http_write_v1"], true);
    assert_eq!(read["owner_authorization"]["kind"], "authenticated_owner");
    assert_eq!(read["owner_authorization"]["available"], true);
    let (status, reply) = f
        .call(
            Method::PUT,
            Some(f.body(0)),
            Some(TestOwner::Authenticated),
            Some(ORIGIN),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{reply}");
    assert_eq!(reply["receipt"]["state_revision"], 1);
    assert_eq!(reply["receipt"]["changed"], true);
    let (status, replay) = f
        .call(
            Method::PUT,
            Some(f.body(0)),
            Some(TestOwner::Authenticated),
            Some(ORIGIN),
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "{replay}");
    assert_eq!(replay["code"], "scope_revision_conflict");
    f.state.shutdown().await;
    let recovered = Box::pin(AppState::new(f.home.path().into())).await.unwrap();
    let scope = SupervisorSessionService::new(recovered.storage.clone())
        .inspect_scope(&f.supervisor)
        .await
        .unwrap();
    assert_eq!(scope.state_revision, 1);
    assert_eq!(scope.allowed_projects, f.projects.into_iter().collect());
    assert!(!recovered.config.read().await.features.ticket_mutation);
    recovered.shutdown().await;
}
#[actix_web::test]
async fn plain_web_local_or_open_access_never_becomes_owner() {
    let f = Fixture::new().await;
    let (status, read) = f.call(Method::GET, None, None, None).await;
    assert_eq!(status, StatusCode::OK, "{read}");
    assert_eq!(read["owner_authorization"]["available"], false);
    assert_eq!(
        read["owner_authorization"]["code"],
        "owner_authentication_unconfigured"
    );
    let (status, reply) = f
        .call(Method::PUT, Some(f.body(0)), None, Some(ORIGIN))
        .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{reply}");
    assert_eq!(reply["code"], "owner_authentication_unconfigured");
    f.unchanged().await;
    f.state.shutdown().await;
}
#[actix_web::test]
async fn plain_web_missing_expired_or_unavailable_owner_has_no_side_effects() {
    let f = Fixture::new().await;
    for (owner, expected, code) in [
        (
            TestOwner::Required,
            StatusCode::UNAUTHORIZED,
            "owner_authentication_required",
        ),
        (
            TestOwner::Unconfigured,
            StatusCode::SERVICE_UNAVAILABLE,
            "owner_authentication_unconfigured",
        ),
        (
            TestOwner::Unavailable,
            StatusCode::SERVICE_UNAVAILABLE,
            "owner_authentication_unavailable",
        ),
    ] {
        let (status, reply) = f
            .call(Method::PUT, Some(f.body(0)), Some(owner), Some(ORIGIN))
            .await;
        assert_eq!(status, expected, "{reply}");
        assert_eq!(reply["code"], code);
        f.unchanged().await;
    }
    f.state.shutdown().await;
}
#[actix_web::test]
async fn plain_web_wrong_missing_or_credentialed_origin_is_denied_before_cas() {
    let f = Fixture::new().await;
    for origin in [
        None,
        Some("null"),
        Some("http://127.0.0.1:64603"),
        Some("https://attacker.example"),
        Some("http://user@127.0.0.1:64602"),
        Some("http://127.0.0.1:64602/path"),
        Some("http://127.0.0.1:64602?query"),
    ] {
        let (status, reply) = f
            .call(
                Method::PUT,
                Some(f.body(0)),
                Some(TestOwner::Authenticated),
                origin,
            )
            .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{origin:?}: {reply}");
        assert_eq!(reply["code"], "owner_request_origin_invalid");
        f.unchanged().await;
    }
    f.state.shutdown().await;
}
#[actix_web::test]
async fn plain_web_identity_and_project_rules_are_bamboo_owned() {
    let f = Fixture::new().await;
    let mut identity = f.body(0);
    identity["incarnation_id"] = json!(uuid::Uuid::new_v4());
    let (status, _) = f
        .call(
            Method::PUT,
            Some(identity),
            Some(TestOwner::Authenticated),
            Some(ORIGIN),
        )
        .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    f.unchanged().await;
    let first = f.state.project_store.get(&f.projects[0]).unwrap();
    f.state
        .project_store
        .archive(&first.id, first.revision)
        .unwrap();
    let (status, reply) = f
        .call(
            Method::PUT,
            Some(f.body(0)),
            Some(TestOwner::Authenticated),
            Some(ORIGIN),
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "{reply}");
    assert_eq!(reply["code"], "project_archived");
    f.unchanged().await;
    f.state.shutdown().await;
}
#[actix_web::test]
async fn plain_web_frontend_approved_claim_is_not_an_authentication_protocol() {
    let f = Fixture::new().await;
    let mut body = f.body(0);
    body["approved"] = json!(true);
    let (status, _) = f
        .call(
            Method::PUT,
            Some(body),
            Some(TestOwner::Authenticated),
            Some(ORIGIN),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    f.unchanged().await;
    f.state.shutdown().await;
}
#[actix_web::test]
async fn plain_web_read_does_not_require_or_bootstrap_a_native_channel() {
    let f = Fixture::new().await;
    let (status, read) = f
        .call(Method::GET, None, Some(TestOwner::Required), None)
        .await;
    assert_eq!(status, StatusCode::OK, "{read}");
    assert_eq!(read["capabilities"]["project_scope_http_write_v1"], true);
    assert_eq!(read["owner_authorization"]["available"], false);
    assert_eq!(
        read["owner_authorization"]["code"],
        "owner_authentication_required"
    );
    f.unchanged().await;
    f.state.shutdown().await;
}
