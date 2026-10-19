//! Plain HTTP scope contract using the existing request access mode.
//! No test principal, credentials, native channel or extra Origin gate.
use crate::{app_state::AppState, routes::configure_routes};
use actix_web::{
    http::{Method, StatusCode},
    test, web, App,
};
use bamboo_domain::{ProjectId, SupervisorReference};
use bamboo_engine::session_app::supervisor::SupervisorSessionService;
use serde_json::{json, Value};

const URI: &str = "/api/v1/supervisor/default/scope";
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
    async fn call(&self, method: Method, body: Option<Value>) -> (StatusCode, Value) {
        let app = test::init_service(
            App::new()
                .app_data(self.state.clone())
                .configure(configure_routes),
        )
        .await;
        let mut request = test::TestRequest::default()
            .method(method)
            .uri(URI)
            .peer_addr("127.0.0.1:43210".parse().unwrap());
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
async fn plain_web_default_path_retains_identity_and_project_validation() {
    let f = Fixture::new().await;
    let mut wrong_identity = f.body(0);
    wrong_identity["incarnation_id"] = json!(uuid::Uuid::new_v4());
    assert_eq!(
        f.call(Method::PUT, Some(wrong_identity)).await.0,
        StatusCode::FORBIDDEN
    );
    let mut unknown = f.body(0);
    unknown["allowed_projects"] = json!(["missing-project"]);
    let (status, reply) = f.call(Method::PUT, Some(unknown)).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{reply}");
    assert_eq!(reply["code"], "project_unavailable");
    let first = f.state.project_store.get(&f.projects[0]).unwrap();
    f.state
        .project_store
        .archive(&first.id, first.revision)
        .unwrap();
    let (status, reply) = f.call(Method::PUT, Some(f.body(0))).await;
    assert_eq!(status, StatusCode::CONFLICT, "{reply}");
    assert_eq!(reply["code"], "project_archived");
    f.unchanged().await;
    f.state.shutdown().await;
}
#[actix_web::test]
async fn plain_web_default_path_preserves_the_exact_scope_request_schema() {
    let f = Fixture::new().await;
    let mut body = f.body(0);
    body["approved"] = json!(true);
    assert_eq!(
        f.call(Method::PUT, Some(body)).await.0,
        StatusCode::BAD_REQUEST
    );
    f.unchanged().await;
    f.state.shutdown().await;
}
#[actix_web::test]
async fn existing_access_http_read_has_no_scope_login_prerequisite() {
    let f = Fixture::new().await;
    let (status, read) = f.call(Method::GET, None).await;
    assert_eq!(status, StatusCode::OK, "{read}");
    assert_eq!(read["owner_authorization"]["kind"], "existing_access");
    assert_eq!(read["owner_authorization"]["available"], true);
    assert!(read["owner_authorization"]["code"].is_null());
    assert_eq!(read["capabilities"]["project_scope_http_write_v1"], true);
    f.unchanged().await;
    f.state.shutdown().await;
}

#[actix_web::test]
async fn existing_access_http_local_put_needs_no_new_login_and_survives_restart() {
    let f = Fixture::new().await;
    let (status, reply) = f.call(Method::PUT, Some(f.body(0))).await;
    assert_eq!(status, StatusCode::OK, "{reply}");
    assert_eq!(reply["receipt"]["state_revision"], 1);
    assert_eq!(reply["receipt"]["changed"], true);
    let (status, stale) = f.call(Method::PUT, Some(f.body(0))).await;
    assert_eq!(status, StatusCode::CONFLICT, "{stale}");
    assert_eq!(stale["code"], "scope_revision_conflict");
    assert!(!f.state.config.read().await.features.ticket_mutation);
    assert!(!f.state.config.read().await.features.ticket_dispatch);
    assert!(!f.home.path().join("tickets").exists());
    f.state.shutdown().await;
    let recovered = Box::pin(AppState::new(f.home.path().into())).await.unwrap();
    let scope = SupervisorSessionService::new(recovered.storage.clone())
        .inspect_scope(&f.supervisor)
        .await
        .unwrap();
    assert_eq!(scope.state_revision, 1);
    assert_eq!(scope.allowed_projects, f.projects.into_iter().collect());
    let authority = recovered
        .storage
        .load_root_authority(&f.supervisor.session_id)
        .await
        .unwrap()
        .unwrap();
    assert!(authority.messages.is_empty());
    assert!(!recovered.config.read().await.features.ticket_mutation);
    assert!(!recovered.config.read().await.features.ticket_dispatch);
    assert!(!f.home.path().join("tickets").exists());
    recovered.shutdown().await;
}
