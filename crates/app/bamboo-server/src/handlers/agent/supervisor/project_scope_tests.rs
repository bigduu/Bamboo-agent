//! Owner HTTP contract. No user data directory, model, or production grants.
use actix_web::{
    http::{Method, StatusCode},
    test, web, App, HttpMessage,
};
use bamboo_domain::{ProjectId, Session, SupervisorReference};
use bamboo_engine::session_app::supervisor::SupervisorSessionService;
use serde_json::{json, Value};

use crate::app_state::supervisor_owner_authorization::test_support::{
    intent as approved_intent, ApprovedHostIntent,
};
use crate::{app_state::AppState, routes::configure_routes};
use std::sync::Arc;

const URI: &str = "/api/v1/supervisor/default/scope";

struct Fixture {
    home: tempfile::TempDir,
    state: web::Data<AppState>,
    reference: SupervisorReference,
    project: ProjectId,
}

impl Fixture {
    async fn new() -> Self {
        let home = tempfile::tempdir().unwrap();
        let state = web::Data::new(
            Box::pin(AppState::new(home.path().into()))
                .await
                .unwrap()
                .with_supervisor_project_scope_authorizer(Arc::new(ApprovedHostIntent)),
        );
        let reference = SupervisorReference::from(
            &SupervisorSessionService::new(state.storage.clone())
                .get_or_create_default("unconfigured-fixture-model")
                .await
                .unwrap(),
        );
        let project = state
            .project_store
            .create("Explicit Project", None)
            .unwrap()
            .id;
        Self {
            home,
            state,
            reference,
            project,
        }
    }

    fn update(&self, revision: u64, projects: Vec<ProjectId>) -> Value {
        json!({"incarnation_id":self.reference.incarnation_id,
            "expected_state_revision":revision,"allowed_projects":projects})
    }

    async fn call(&self, method: Method, body: Option<Value>) -> (StatusCode, Value) {
        let mut factory = App::new().app_data(self.state.clone());
        if let Some(intent) = body.as_ref().and_then(approved_intent) {
            factory = factory.app_data(web::Data::new(intent));
        }
        let app = test::init_service(factory.configure(configure_routes)).await;
        let request = test::TestRequest::default()
            .method(method)
            .uri(URI)
            .peer_addr("127.0.0.1:12345".parse().unwrap());
        let request = if let Some(body) = body {
            request.set_json(body)
        } else {
            request
        };
        let response = test::call_service(&app, request.to_request()).await;
        let status = response.status();
        let bytes = test::read_body(response).await;
        let value = serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| json!({"non_json_body":String::from_utf8_lossy(&bytes)}));
        (status, value)
    }
}

#[actix_web::test]
async fn owner_scope_cas_is_explicit_restartable_and_does_not_initialize_work() {
    let f = Fixture::new().await;
    let (status, observed) = f.call(Method::GET, None).await;
    assert_eq!(status, StatusCode::OK, "{observed}");
    assert_eq!(observed["scope"]["state_revision"], 0);
    assert_eq!(observed["scope"]["allowed_projects"], json!([]));
    assert_eq!(observed["code"], "project_scope_uninitialized");
    let request = f.update(0, vec![f.project.clone()]);
    let (status, configured) = f.call(Method::PUT, Some(request.clone())).await;
    assert_eq!(status, StatusCode::OK, "{configured}");
    assert_eq!(configured["receipt"]["state_revision"], 1);
    assert_eq!(configured["receipt"]["changed"], true);
    let (status, stale) = f.call(Method::PUT, Some(request)).await;
    assert_eq!(status, StatusCode::CONFLICT, "{stale}");
    assert_eq!(stale["code"], "scope_revision_conflict");
    let (status, noop) = f
        .call(Method::PUT, Some(f.update(1, vec![f.project.clone()])))
        .await;
    assert_eq!(status, StatusCode::OK, "{noop}");
    assert_eq!(noop["receipt"]["changed"], false);
    assert_eq!(noop["receipt"]["state_revision"], 1);
    let Fixture {
        home,
        state,
        reference,
        project,
    } = f;
    state.shutdown().await;
    drop(state);
    let recovered = Box::pin(AppState::new(home.path().into())).await.unwrap();
    let scope = SupervisorSessionService::new(recovered.storage.clone())
        .inspect_scope(&reference)
        .await
        .unwrap();
    assert_eq!(scope.state_revision, 1);
    assert_eq!(scope.allowed_projects, [project].into());
    let authority = recovered
        .storage
        .load_root_authority(&reference.session_id)
        .await
        .unwrap()
        .unwrap();
    assert!(!authority.root_orchestration_only_enabled());
    assert!(authority.messages.is_empty());
    assert!(!recovered.config.read().await.features.ticket_mutation);
    assert!(!recovered.config.read().await.features.ticket_dispatch);
    assert!(!home.path().join("tickets").exists());
    recovered.shutdown().await;
}

#[actix_web::test]
async fn owner_scope_rejects_remote_and_worker_without_creating_supervisor() {
    let home = tempfile::tempdir().unwrap();
    let state = web::Data::new(Box::pin(AppState::new(home.path().into())).await.unwrap());
    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .configure(configure_routes),
    )
    .await;
    for method in [Method::GET, Method::PUT] {
        let request = test::TestRequest::default().method(method).uri(URI)
            .peer_addr("192.0.2.1:12345".parse().unwrap())
            .set_json(json!({"incarnation_id":uuid::Uuid::new_v4(),"expected_state_revision":0,"allowed_projects":[]}));
        let response = test::call_service(&app, request.to_request()).await;
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }
    // Install the same verified Worker auth extension used by owner fencing.
    let worker_app = test::init_service(
        App::new()
            .app_data(state.clone())
            .wrap_fn(|req, service| {
                use actix_web::dev::Service;
                req.extensions_mut()
                    .insert(crate::codex_run_tokens::CodexRunAuthContext {
                        session_id: "worker".into(),
                    });
                service.call(req)
            })
            .configure(configure_routes),
    )
    .await;
    for method in [Method::GET, Method::PUT] {
        let request = test::TestRequest::default().method(method).uri(URI)
            .peer_addr("127.0.0.1:12345".parse().unwrap())
            .set_json(json!({"incarnation_id":uuid::Uuid::new_v4(),"expected_state_revision":0,"allowed_projects":[]}));
        let response = test::call_service(&worker_app, request.to_request()).await;
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }
    assert!(state
        .storage
        .load_root_authority(bamboo_domain::DEFAULT_SUPERVISOR_SESSION_ID)
        .await
        .unwrap()
        .is_none());
    state.shutdown().await;
}

#[actix_web::test]
async fn owner_scope_rejects_unknown_archived_or_oversized_projects_and_stale_identity() {
    let f = Fixture::new().await;
    let archived = f
        .state
        .project_store
        .create("Archived Project", None)
        .unwrap();
    f.state
        .project_store
        .archive(&archived.id, archived.revision)
        .unwrap();
    for (body, expected, code) in [
        (
            f.update(0, vec!["absent-project".parse().unwrap()]),
            StatusCode::NOT_FOUND,
            "project_unavailable",
        ),
        (
            f.update(0, vec![archived.id]),
            StatusCode::CONFLICT,
            "project_archived",
        ),
        (
            f.update(
                0,
                (0..65).map(|i| format!("p-{i}").parse().unwrap()).collect(),
            ),
            StatusCode::BAD_REQUEST,
            "project_scope_invalid",
        ),
        (
            json!({"incarnation_id":uuid::Uuid::new_v4(),"expected_state_revision":0,"allowed_projects":[]}),
            StatusCode::FORBIDDEN,
            "supervisor_identity_mismatch",
        ),
    ] {
        let (status, error) = f.call(Method::PUT, Some(body)).await;
        assert_eq!(status, expected, "{error}");
        assert_eq!(error["code"], code);
        let observed = SupervisorSessionService::new(f.state.storage.clone())
            .inspect_scope(&f.reference)
            .await
            .unwrap();
        assert_eq!(observed.state_revision, 0);
        assert!(observed.allowed_projects.is_empty());
    }
    let mut extra = f.update(0, vec![f.project.clone()]);
    extra["attach_session_id"] = json!("historical-root");
    assert_eq!(
        f.call(Method::PUT, Some(extra)).await.0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(f.call(Method::PUT, Some(json!({"incarnation_id":f.reference.incarnation_id,"expected_state_revision":0,"allowed_projects":["../bad"]}))).await.0, StatusCode::BAD_REQUEST);
    assert_eq!(
        SupervisorSessionService::new(f.state.storage.clone())
            .inspect_scope(&f.reference)
            .await
            .unwrap()
            .state_revision,
        0
    );
    f.state.shutdown().await;
}

#[actix_web::test]
async fn owner_scope_revoke_and_regrant_never_attach_or_revive_existing_root() {
    let f = Fixture::new().await;
    let mut root = Session::new("preexisting-unrelated-root", "private-target-model");
    root.set_project_id_meta(f.project.to_string());
    root.add_message(bamboo_domain::Message::user("private target history"));
    f.state.storage.save_session(&root).await.unwrap();
    let before = serde_json::to_value(
        f.state
            .storage
            .load_session(&root.id)
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        f.call(Method::PUT, Some(f.update(0, vec![f.project.clone()])))
            .await
            .0,
        StatusCode::OK
    );
    let service = SupervisorSessionService::new(f.state.storage.clone());
    assert!(
        !service
            .inspect_link(&f.reference, &root.id)
            .await
            .unwrap()
            .authorized
    );
    service.attach(&f.reference, 1, &root.id).await.unwrap(); // Test-only explicit Host attach.
    assert_eq!(
        f.call(Method::PUT, Some(f.update(2, vec![]))).await.0,
        StatusCode::OK
    );
    assert_eq!(
        f.call(Method::PUT, Some(f.update(3, vec![f.project.clone()])))
            .await
            .0,
        StatusCode::OK
    );
    let link = service.inspect_link(&f.reference, &root.id).await.unwrap();
    assert!(!link.authorized);
    assert!(!link.link.unwrap().enabled);
    assert_eq!(
        serde_json::to_value(
            f.state
                .storage
                .load_session(&root.id)
                .await
                .unwrap()
                .unwrap()
        )
        .unwrap(),
        before
    );
    assert!(!f.home.path().join("tickets").exists());
    f.state.shutdown().await;
}

#[actix_web::test]
async fn owner_scope_same_revision_race_commits_only_one_desired_set() {
    let f = Fixture::new().await;
    let second = f
        .state
        .project_store
        .create("Second Project", None)
        .unwrap()
        .id;
    let (one, two) = futures::join!(
        f.call(Method::PUT, Some(f.update(0, vec![f.project.clone()]))),
        f.call(Method::PUT, Some(f.update(0, vec![second.clone()]))),
    );
    assert_eq!(
        [one.0, two.0]
            .iter()
            .filter(|s| **s == StatusCode::OK)
            .count(),
        1
    );
    assert_eq!(
        [one.0, two.0]
            .iter()
            .filter(|s| **s == StatusCode::CONFLICT)
            .count(),
        1
    );
    let observed = SupervisorSessionService::new(f.state.storage.clone())
        .inspect_scope(&f.reference)
        .await
        .unwrap();
    assert_eq!(observed.state_revision, 1);
    assert!(
        observed.allowed_projects == [f.project.clone()].into()
            || observed.allowed_projects == [second].into()
    );
    f.state.shutdown().await;
}

#[actix_web::test]
async fn owner_scope_get_missing_or_ordinary_default_never_promotes_identity() {
    let home = tempfile::tempdir().unwrap();
    let state = web::Data::new(Box::pin(AppState::new(home.path().into())).await.unwrap());
    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .configure(configure_routes),
    )
    .await;
    let request = || {
        test::TestRequest::get()
            .uri(URI)
            .peer_addr("127.0.0.1:12345".parse().unwrap())
            .to_request()
    };
    let response = test::call_service(&app, request()).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let body: Value = test::read_body_json(response).await;
    assert_eq!(body["code"], "supervisor_missing");
    assert!(state
        .storage
        .load_root_authority(bamboo_domain::DEFAULT_SUPERVISOR_SESSION_ID)
        .await
        .unwrap()
        .is_none());
    let ordinary = Session::new(
        bamboo_domain::DEFAULT_SUPERVISOR_SESSION_ID,
        "ordinary-private-model",
    );
    state.storage.save_session(&ordinary).await.unwrap();
    let response = test::call_service(&app, request()).await;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let body: Value = test::read_body_json(response).await;
    assert_eq!(body["code"], "supervisor_identity_mismatch");
    assert_eq!(
        serde_json::to_value(
            state
                .storage
                .load_session(&ordinary.id)
                .await
                .unwrap()
                .unwrap()
        )
        .unwrap(),
        serde_json::to_value(&ordinary).unwrap()
    );
    state.shutdown().await;
}

async fn model_scope(f: &Fixture) -> Value {
    use bamboo_agent_core::tools::{
        ExecutingSupervisorObservation, FunctionCall, ToolCall, ToolExecutionContext, ToolOutcome,
    };
    let executing = f
        .state
        .storage
        .load_root_authority(&f.reference.session_id)
        .await
        .unwrap()
        .unwrap();
    let call = ToolCall {
        id: "scope-diagnostic".into(),
        tool_type: "function".into(),
        function: FunctionCall {
            name: "session_control".into(),
            arguments: json!({"action":"scope"}).to_string(),
        },
    };
    let mut ctx = ToolExecutionContext::none(&call.id);
    ctx.session_id = Some(&executing.id);
    ctx.root_session_id = Some(&executing.id);
    let ctx = ctx.with_executing_supervisor(
        ExecutingSupervisorObservation::capture_from_executing_session(&executing),
    );
    let outcome = f
        .state
        .tools_for(crate::tools::ToolSurface::Root)
        .execute_with_context_outcome(&call, ctx)
        .await
        .unwrap();
    let ToolOutcome::Completed(result) = outcome else {
        panic!("scope is a bounded observation")
    };
    assert!(result.success, "{}", result.result);
    serde_json::from_str(&result.result).unwrap()
}

#[actix_web::test]
async fn model_scope_reports_owner_action_without_grant_or_work_readiness() {
    let f = Fixture::new().await;
    let scope = model_scope(&f).await;
    assert_eq!(scope["code"], "project_scope_uninitialized");
    assert_eq!(scope["owner_action"]["model_callable"], false);
    assert_eq!(scope["owner_action"]["path"], URI);
    assert_eq!(scope["owner_action"]["requires"], "authenticated_owner");
    assert_eq!(scope["work_scope"]["availability"], "not_observed");
    assert_eq!(scope["work_scope"]["status_path"], "/api/v1/tickets/scope");
    let observed = SupervisorSessionService::new(f.state.storage.clone())
        .inspect_scope(&f.reference)
        .await
        .unwrap();
    assert!(observed.allowed_projects.is_empty());
    assert_eq!(observed.state_revision, 0);
    assert!(!f.home.path().join("tickets").exists());
    f.state.shutdown().await;
}

#[actix_web::test]
async fn ultra_owner_scope_and_model_observation_use_management_revision_not_work_binding() {
    use bamboo_domain::{RootModeOperationAction, RootModeOperationRequest};
    let f = Fixture::new().await;
    let canonical = f
        .state
        .storage
        .load_root_authority(&f.reference.session_id)
        .await
        .unwrap()
        .unwrap();
    f.state
        .storage
        .root_mode_operation(&RootModeOperationRequest {
            session_id: canonical.id.clone(),
            operation_id: format!(
                "{}:{}",
                canonical.root_mode_transition_epoch,
                uuid::Uuid::new_v4()
            ),
            birth_token: canonical.root_mode_birth_token(),
            expected_epoch: canonical.root_mode_transition_epoch,
            requested_enabled: true,
            action: RootModeOperationAction::Select,
        })
        .await
        .unwrap();
    let (status, configured) = f
        .call(Method::PUT, Some(f.update(0, vec![f.project.clone()])))
        .await;
    assert_eq!(status, StatusCode::OK, "{configured}");
    // Management revision advances independently of the unchanged Work binding.
    assert_eq!(
        f.call(Method::PUT, Some(f.update(1, vec![]))).await.0,
        StatusCode::OK
    );
    let (_, http) = f.call(Method::GET, None).await;
    let model = model_scope(&f).await;
    assert_eq!(http["scope"]["supervisor"], model["supervisor"]);
    assert_eq!(http["scope"]["state_revision"], model["state_revision"]);
    assert_eq!(http["scope"]["allowed_projects"], model["allowed_projects"]);
    assert_eq!(model["state_revision"], 2);
    assert_eq!(model["work_scope"]["availability"], "not_observed");
    let canonical = f
        .state
        .storage
        .load_root_authority(&f.reference.session_id)
        .await
        .unwrap()
        .unwrap();
    assert!(canonical.root_orchestration_only_enabled());
    assert_eq!(canonical.root_tool_authority_revision, 1);
    assert!(!f.home.path().join("tickets").exists());
    f.state.shutdown().await;
}

#[actix_web::test]
async fn owner_scope_damaged_authority_fails_closed_without_repair() {
    let f = Fixture::new().await;
    let runtime = f
        .state
        .session_store
        .sessions_root_dir()
        .join(&f.reference.session_id)
        .join("runtime.json");
    assert!(runtime.is_file());
    let damaged = b"{intentionally-invalid-fixture-runtime";
    std::fs::write(&runtime, damaged).unwrap();
    for (method, body) in [
        (Method::GET, None),
        (Method::PUT, Some(f.update(0, vec![f.project.clone()]))),
    ] {
        let (status, error) = f.call(method, body).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{error}");
        assert_eq!(error["code"], "scope_authority_invalid");
        assert_eq!(std::fs::read(&runtime).unwrap(), damaged);
    }
    assert!(!f.home.path().join("tickets").exists());
    f.state.shutdown().await;
    assert_eq!(std::fs::read(&runtime).unwrap(), damaged);
}

#[actix_web::test]
async fn review_loopback_token_omission_cannot_self_grant_project_scope() {
    let f = Fixture::new().await;
    let app = test::init_service(
        App::new()
            .app_data(f.state.clone())
            .configure(configure_routes),
    )
    .await;
    for forged in [false, true] {
        let request = test::TestRequest::put()
            .uri(URI)
            .peer_addr("127.0.0.1:12345".parse().unwrap())
            .set_json(f.update(0, vec![f.project.clone()]));
        let request = if forged {
            request
                .insert_header(("X-Bamboo-Owner", "true"))
                .insert_header(("Authorization", "Bearer bd1_worker_selected_value"))
        } else {
            request
        };
        let response = test::call_service(&app, request.to_request()).await;
        assert_eq!(
            response.status(),
            StatusCode::FORBIDDEN,
            "an omitted run token or caller-selected header is not owner approval"
        );
        let error: Value = test::read_body_json(response).await;
        assert_eq!(error["code"], "host_owner_authorization_required");
        let observed = SupervisorSessionService::new(f.state.storage.clone())
            .inspect_scope(&f.reference)
            .await
            .unwrap();
        assert_eq!(observed.state_revision, 0);
        assert!(observed.allowed_projects.is_empty());
    }
    f.state.shutdown().await;
}

#[actix_web::test]
async fn review_put_before_bootstrap_reports_missing_without_creating_identity() {
    let home = tempfile::tempdir().unwrap();
    let state = web::Data::new(Box::pin(AppState::new(home.path().into())).await.unwrap());
    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .configure(configure_routes),
    )
    .await;
    let response=test::call_service(&app,test::TestRequest::put().uri(URI)
        .peer_addr("127.0.0.1:12345".parse().unwrap())
        .set_json(json!({"incarnation_id":uuid::Uuid::new_v4(),"expected_state_revision":0,"allowed_projects":[]})).to_request()).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let error: Value = test::read_body_json(response).await;
    assert_eq!(error["code"], "supervisor_missing");
    assert!(state
        .storage
        .load_root_authority(bamboo_domain::DEFAULT_SUPERVISOR_SESSION_ID)
        .await
        .unwrap()
        .is_none());
    state.shutdown().await;
}

#[actix_web::test]
async fn default_host_without_owner_verifier_never_accepts_scope_grant() {
    let home = tempfile::tempdir().unwrap();
    let state = web::Data::new(Box::pin(AppState::new(home.path().into())).await.unwrap());
    let service = SupervisorSessionService::new(state.storage.clone());
    let reference =
        SupervisorReference::from(&service.get_or_create_default("no-model").await.unwrap());
    let project = state
        .project_store
        .create("Unapproved Project", None)
        .unwrap();
    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .configure(configure_routes),
    )
    .await;
    let response = test::call_service(
        &app,
        test::TestRequest::get()
            .uri(URI)
            .peer_addr("127.0.0.1:12345".parse().unwrap())
            .to_request(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let observed: Value = test::read_body_json(response).await;
    assert_eq!(observed["owner_authorization"]["available"], false);
    assert_eq!(observed["owner_authorization"]["http_registration"], false);
    let response=test::call_service(&app,test::TestRequest::put().uri(URI).peer_addr("127.0.0.1:12345".parse().unwrap())
        .insert_header(("X-Bamboo-Owner","true"))
        .set_json(json!({"incarnation_id":reference.incarnation_id,"expected_state_revision":0,"allowed_projects":[project.id]})).to_request()).await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let error: Value = test::read_body_json(response).await;
    assert_eq!(error["code"], "owner_authentication_unconfigured");
    let scope = service.inspect_scope(&reference).await.unwrap();
    assert_eq!(scope.state_revision, 0);
    assert!(scope.allowed_projects.is_empty());
    assert!(!state.config.read().await.features.ticket_mutation);
    state.shutdown().await;
}

#[actix_web::test]
async fn host_consent_is_bound_to_the_exact_scope_intent() {
    let f = Fixture::new().await;
    let other = f
        .state
        .project_store
        .create("Different Project", None)
        .unwrap()
        .id;
    let approved = f.update(0, vec![f.project.clone()]);
    for changed in [
        f.update(0, vec![other]),
        f.update(1, vec![f.project.clone()]),
    ] {
        let app = test::init_service(
            App::new()
                .app_data(f.state.clone())
                .app_data(web::Data::new(approved_intent(&approved).unwrap()))
                .configure(configure_routes),
        )
        .await;
        let response = test::call_service(
            &app,
            test::TestRequest::put()
                .uri(URI)
                .peer_addr("127.0.0.1:12345".parse().unwrap())
                .set_json(changed)
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let error: Value = test::read_body_json(response).await;
        assert_eq!(error["code"], "host_owner_authorization_required");
        let scope = SupervisorSessionService::new(f.state.storage.clone())
            .inspect_scope(&f.reference)
            .await
            .unwrap();
        assert_eq!(scope.state_revision, 0);
        assert!(scope.allowed_projects.is_empty());
    }
    f.state.shutdown().await;
}
