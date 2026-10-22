//! Isolated temporary canonical storage; mock user decisions never leave tests.
use super::{
    adapter::{Adapter, Connection},
    canonical::CanonicalScope,
    confirmation::{self, ConfirmationRequest, Decision},
    provider::ConfirmationProvider,
    wire::AuthorityProfile,
};
use crate::app_state::AppState;
use actix_web::web;
use async_trait::async_trait;
use bamboo_domain::{
    ProjectId, SupervisorManagementMutation, SupervisorManagementRequest, SupervisorReference,
    DEFAULT_SUPERVISOR_SESSION_ID,
};
use bamboo_engine::session_app::supervisor::SupervisorSessionService;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use uuid::Uuid;

struct Mock {
    state: web::Data<AppState>,
    action: u8,
    calls: Arc<AtomicUsize>,
}
#[async_trait]
impl ConfirmationProvider for Mock {
    fn profile(&self) -> AuthorityProfile {
        AuthorityProfile::new((self.action == 0).then_some("native_confirmation_not_enabled"))
    }
    async fn confirm(&self, request: &ConfirmationRequest) -> Decision {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.action == 2 {
            SupervisorSessionService::new(self.state.storage.clone())
                .configure_project_scope(
                    &request.intent.supervisor,
                    request.intent.expected_state_revision,
                    Default::default(),
                )
                .await
                .unwrap();
            // An actual competing change must advance the management revision.
            let project = self
                .state
                .project_store
                .create("Competing Project", None)
                .unwrap()
                .id;
            SupervisorSessionService::new(self.state.storage.clone())
                .configure_project_scope(
                    &request.intent.supervisor,
                    request.intent.expected_state_revision,
                    [project].into(),
                )
                .await
                .unwrap();
        }
        if self.action == 3 {
            let SupervisorManagementMutation::ConfigureProjectScope { allowed_projects } =
                &request.intent.mutation
            else {
                panic!()
            };
            let id = allowed_projects.iter().next().unwrap();
            let project = self.state.project_store.get(id).unwrap();
            self.state
                .project_store
                .archive(id, project.revision)
                .unwrap();
        }
        confirmation::mock_approval(request)
    }
}
fn intent(
    reference: SupervisorReference,
    projects: impl IntoIterator<Item = ProjectId>,
) -> SupervisorManagementRequest {
    SupervisorManagementRequest {
        supervisor: reference,
        expected_state_revision: 0,
        mutation: SupervisorManagementMutation::ConfigureProjectScope {
            allowed_projects: projects.into_iter().collect(),
        },
    }
}
async fn state() -> (tempfile::TempDir, web::Data<AppState>) {
    let home = tempfile::tempdir().unwrap();
    let state = web::Data::new(Box::pin(AppState::new(home.path().into())).await.unwrap());
    (home, state)
}
fn make_adapter(
    state: web::Data<AppState>,
    action: u8,
) -> (Adapter<CanonicalScope, Mock>, Uuid, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let launch = Uuid::new_v4();
    let mut adapter = Adapter::new(
        launch,
        CanonicalScope(state.clone()),
        Mock {
            state,
            action,
            calls: calls.clone(),
        },
    )
    .unwrap();
    adapter.acknowledge(launch).unwrap();
    (adapter, launch, calls)
}
#[actix_web::test]
async fn absent_native_scope_never_bootstraps_or_prompts() {
    let (_home, state) = state().await;
    let (mut adapter, launch, calls) = make_adapter(state.clone(), 1);
    let request = intent(
        SupervisorReference {
            session_id: DEFAULT_SUPERVISOR_SESSION_ID.into(),
            incarnation_id: Uuid::new_v4(),
        },
        [],
    );
    let result = adapter
        .apply(launch, Uuid::new_v4(), request, false, &Connection::new())
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(&result.outcome).unwrap()["code"],
        "supervisor_missing"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(state
        .storage
        .load_root_authority(DEFAULT_SUPERVISOR_SESSION_ID)
        .await
        .unwrap()
        .is_none());
    state.shutdown().await;
}
#[actix_web::test]
async fn mock_confirmed_scope_uses_real_cas_with_no_http_grant_and_no_work_initialization() {
    let (home, state) = state().await;
    let service = SupervisorSessionService::new(state.storage.clone());
    let reference = SupervisorReference::from(
        &service
            .get_or_create_default("unconfigured-fixture-model")
            .await
            .unwrap(),
    );
    let project = state
        .project_store
        .create("Explicit fixture Project", None)
        .unwrap()
        .id;
    let request = intent(reference.clone(), [project.clone()]);
    let (mut adapter, launch, calls) = make_adapter(state.clone(), 1);
    let result = adapter
        .apply(launch, Uuid::new_v4(), request, false, &Connection::new())
        .await
        .unwrap();
    let value = serde_json::to_value(&result.outcome).unwrap();
    assert_eq!(value["kind"], "committed");
    assert_eq!(value["receipt"]["state_revision"], 1);
    adapter.flushed(result).unwrap();
    let scope = service.inspect_scope(&reference).await.unwrap();
    assert_eq!(scope.allowed_projects, [project].into());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(state.supervisor_project_scope_owner_authorizer.is_none());
    assert!(!state.config.read().await.features.ticket_mutation);
    assert!(!state.config.read().await.features.ticket_dispatch);
    assert!(!home.path().join("tickets").exists());
    state.shutdown().await;
    drop(service);
    drop(adapter);
    drop(state);
    let recovered = web::Data::new(Box::pin(AppState::new(home.path().into())).await.unwrap());
    let service = SupervisorSessionService::new(recovered.storage.clone());
    assert_eq!(service.inspect_scope(&reference).await.unwrap(), scope);
    let (mut restarted, new_launch, new_calls) = make_adapter(recovered.clone(), 0);
    let mut replacement = intent(reference, []);
    replacement.expected_state_revision = 1;
    let result = restarted
        .apply(
            new_launch,
            Uuid::new_v4(),
            replacement,
            false,
            &Connection::new(),
        )
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(&result.outcome).unwrap()["code"],
        "native_confirmation_not_enabled"
    );
    assert_eq!(new_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        service.inspect_scope(&scope.supervisor).await.unwrap(),
        scope
    );
    recovered.shutdown().await;
}
#[actix_web::test]
async fn real_competing_revision_after_confirmation_is_not_silently_retried() {
    let (_home, state) = state().await;
    let service = SupervisorSessionService::new(state.storage.clone());
    let reference = SupervisorReference::from(
        &service
            .get_or_create_default("unconfigured-fixture-model")
            .await
            .unwrap(),
    );
    let project = state
        .project_store
        .create("Reviewed Project", None)
        .unwrap()
        .id;
    let (mut adapter, launch, calls) = make_adapter(state.clone(), 2);
    let result = adapter
        .apply(
            launch,
            Uuid::new_v4(),
            intent(reference.clone(), [project.clone()]),
            false,
            &Connection::new(),
        )
        .await
        .unwrap();
    let value = serde_json::to_value(result.outcome).unwrap();
    assert_eq!(value["code"], "scope_revision_conflict");
    assert_eq!(value["status_code"], 409);
    let scope = service.inspect_scope(&reference).await.unwrap();
    assert_eq!(scope.state_revision, 1);
    assert!(!scope.allowed_projects.contains(&project));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    state.shutdown().await;
}
#[actix_web::test]
async fn project_archived_while_native_confirmation_pending_is_revalidated_before_cas() {
    let (_home, state) = state().await;
    let service = SupervisorSessionService::new(state.storage.clone());
    let reference = SupervisorReference::from(
        &service
            .get_or_create_default("unconfigured-fixture-model")
            .await
            .unwrap(),
    );
    let project = state
        .project_store
        .create("Reviewed Project", None)
        .unwrap()
        .id;
    let (mut adapter, launch, calls) = make_adapter(state.clone(), 3);
    let result = adapter
        .apply(
            launch,
            Uuid::new_v4(),
            intent(reference.clone(), [project]),
            false,
            &Connection::new(),
        )
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(result.outcome).unwrap()["code"],
        "project_archived"
    );
    assert_eq!(
        service
            .inspect_scope(&reference)
            .await
            .unwrap()
            .state_revision,
        0
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    state.shutdown().await;
}
