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
        let memory = bamboo_memory::memory_store::MemoryStore::new(home.path().join("jiandu"));
        let mut state = Box::pin(AppState::new_with_memory_store(home.path().into(), memory))
            .await
            .unwrap();
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
    assert_eq!(empty["sessions"].as_array().unwrap().len(), 2);
    assert!(empty["legacy_links"].as_object().unwrap().is_empty());
    assert_eq!(empty["management_access"]["kind"], "current_user");
    assert_eq!(
        empty["management_access"]["requires_session_attachment"],
        false
    );
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
    assert_eq!(observed["sessions"].as_array().unwrap().len(), 2);
    assert_eq!(observed["legacy_links"].as_object().unwrap().len(), 1);
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
    assert_eq!(observed["sessions"][0]["enabled"], true);
    assert_eq!(observed["sessions"][0]["authorized"], true);
    assert_eq!(observed["legacy_links"]["target-a"]["enabled"], false);
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
    let mut child = Session::new_child_of("not-a-root", &parent, "model", "Child");
    // new_child_of captures birth/ancestry, not the parent's Project. This
    // synthetic Child must have the same valid canonical Project before the
    // independent legacy-Root admission negative is exercised.
    child.set_project_id_meta(parent.project_id_meta().unwrap());
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
    let (status, observed) = f.call(Method::GET, None, None).await;
    assert_eq!(status, StatusCode::OK, "{observed}");
    assert_eq!(observed["scope"]["state_revision"], 1);
    assert!(observed["sessions"]
        .as_array()
        .unwrap()
        .iter()
        .any(|target| target["session_id"] == "target-a"));
    assert!(observed["legacy_links"].as_object().unwrap().is_empty());
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
    let (status, observed) = f.call(Method::GET, None, None).await;
    assert_eq!(status, StatusCode::OK, "{observed}");
    assert_eq!(observed["scope"]["state_revision"], 1);
    assert!(observed["sessions"]
        .as_array()
        .unwrap()
        .iter()
        .any(|target| target["session_id"] == "target-a"));
    assert!(observed["legacy_links"].as_object().unwrap().is_empty());
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
    assert_eq!(observed["sessions"][0]["authorized"], true);
    assert_eq!(observed["sessions"][0]["target_metadata_version"], 1);
    assert_eq!(
        observed["legacy_links"]["target-a"]["target_metadata_version"],
        0
    );
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
    assert!(observed["sessions"]
        .as_array()
        .unwrap()
        .iter()
        .all(|target| target["session_id"] != "target-a"));
    assert_eq!(observed["legacy_links"]["target-a"]["enabled"], true);
    let (status, response) = f
        .call(Method::PUT, Some("target-a"), Some(f.body(3, false)))
        .await;
    assert_eq!(status, StatusCode::OK, "{response}");
    let (_, observed) = f.call(Method::GET, None, None).await;
    assert_eq!(observed["legacy_links"]["target-a"]["enabled"], false);
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
    assert_eq!(observed["sessions"].as_array().unwrap().len(), 2);
    assert_eq!(observed["legacy_links"].as_object().unwrap().len(), 1);
    f.state.shutdown().await;
}

#[actix_web::test]
async fn managed_sessions_corrupt_canonical_target_and_source_fail_closed_without_repair() {
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
    assert_eq!(status, StatusCode::CONFLICT, "{observed}");
    assert!(
        observed.get("sessions").is_none(),
        "unverified target metadata cannot authorize discovery"
    );
    assert_eq!(f.target_bytes("target-a").await, before);
    assert_eq!(tokio::fs::read(&target_proof).await.unwrap(), damaged);
    let (status, response) = f
        .call(Method::PUT, Some("target-a"), Some(f.body(2, false)))
        .await;
    assert_eq!(status, StatusCode::OK, "{response}");
    let (status, observed) = f.call(Method::GET, None, None).await;
    assert_eq!(
        status,
        StatusCode::CONFLICT,
        "legacy detach cannot repair canonical target authority: {observed}"
    );
    assert!(observed.get("sessions").is_none());
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

/// These requests use a temporary AppState and synthetic Sessions only.  The
/// stale selector is data, not authorization, and must never select a new birth.
#[actix_web::test]
async fn managed_history_binds_snapshot_to_observed_target_and_keeps_legacy_history() {
    let f = Fixture::new(false).await;
    let (status, directory) = f.call(Method::GET, None, None).await;
    assert_eq!(status, StatusCode::OK, "{directory}");
    assert_eq!(
        directory["capabilities"]["current_user_bound_history_v1"],
        true
    );
    let old_target = directory["sessions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["session_id"] == "target-a")
        .unwrap()["current_user_target"]
        .clone();
    let binding = json!({"supervisor": f.supervisor, "target": old_target});
    let (status, initial) = f
        .call(
            Method::POST,
            Some("target-a/history"),
            Some(binding.clone()),
        )
        .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "bound history must exist: {initial}"
    );
    assert_eq!(initial["current_user_target"], old_target);
    assert_eq!(
        initial["messages"][0]["content"],
        "PRIVATE target-a original history"
    );
    let cursor = initial["messages"][0]["id"].as_str().unwrap().to_string();

    // A normal append in the same birth is still the same target, including
    // when a client requests the existing cursor/projection semantics.
    let mut original = f
        .state
        .storage
        .load_session("target-a")
        .await
        .unwrap()
        .unwrap();
    // The pre-existing quiet cache has the same birth, but misses the newly
    // persisted history. Identity equality is not a freshness proof.
    f.state.sessions.insert(
        "target-a".into(),
        Arc::new(bamboo_engine::SessionSnapshot::new(original.clone())),
    );
    original.add_message(Message::user("synthetic same-birth append"));
    f.state.storage.save_session(&original).await.unwrap();
    let mut delta_request = binding.clone();
    delta_request["query"] = json!({"since_message_id": cursor, "projection": "messages"});
    let (status, delta) = f
        .call(Method::POST, Some("target-a/history"), Some(delta_request))
        .await;
    assert_eq!(status, StatusCode::OK, "{delta}");
    assert_eq!(delta["is_delta"], true);
    assert_eq!(delta["messages"].as_array().unwrap().len(), 1);
    assert_eq!(
        delta["messages"][0]["content"],
        "synthetic same-birth append"
    );

    assert_eq!(delta["history_snapshot_source"], "durable");
    assert!(delta.get("bound_token_usage").is_some());

    // Same-identity edits and truncations must also beat the old cached copy.
    original.messages[1].content = "synthetic edited".into();
    f.state.storage.save_session(&original).await.unwrap();
    let (status, edited) = f
        .call(
            Method::POST,
            Some("target-a/history"),
            Some(binding.clone()),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{edited}");
    assert_eq!(edited["messages"][1]["content"], "synthetic edited");
    original.messages.truncate(1);
    original.token_usage = Some(bamboo_domain::TokenBudgetUsage {
        system_tokens: 2,
        summary_tokens: 0,
        window_tokens: 3,
        total_tokens: 5,
        max_context_tokens: 100,
        budget_limit: 100,
        truncation_occurred: false,
        segments_removed: 0,
        prompt_cached_tool_outputs: 0,
        prompt_cached_tool_tokens_saved: 0,
        thinking_tokens: 0,
        cache_read_input_tokens: 0,
        provider_prompt_usage: None,
    });
    f.state.storage.save_session(&original).await.unwrap();
    let (status, shortened) = f
        .call(
            Method::POST,
            Some("target-a/history"),
            Some(binding.clone()),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{shortened}");
    assert_eq!(shortened["messages"].as_array().unwrap().len(), 1);
    assert_eq!(shortened["bound_token_usage"]["total_tokens"], 5);

    // Actual supported replacement in the isolated temporary store: the old
    // selector cannot retrieve the new birth's transcript.
    assert!(f.state.storage.delete_session("target-a").await.unwrap());
    let mut replacement = f
        .state
        .storage
        .recreate_root_session("target-a", "model")
        .await
        .unwrap();
    replacement.set_project_id_meta(f.project.to_string());
    // Recreated Roots use the same next-revision CAS for Project changes.
    replacement.metadata_version = replacement.metadata_version.checked_add(1).unwrap();
    replacement.add_message(Message::user("PRIVATE replacement history"));
    f.state.storage.save_session(&replacement).await.unwrap();
    let (status, stale) = f
        .call(Method::POST, Some("target-a/history"), Some(binding))
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "{stale}");
    assert_eq!(stale["code"], "target_observation_stale");
    assert!(!stale.to_string().contains("PRIVATE replacement history"));
    assert!(stale.get("messages").is_none());

    // Legacy cache publication is independent of this bound endpoint.
    let _ = f.state.sessions.remove("target-a");
    // Legacy GET remains available with its existing, weaker ID-only contract.
    let app = test::init_service(
        App::new()
            .app_data(f.state.clone())
            .configure(configure_routes),
    )
    .await;
    let request = test::TestRequest::get()
        .uri("/api/v1/history/target-a")
        .peer_addr("127.0.0.1:12345".parse().unwrap())
        .to_request();
    let response = test::call_service(&app, request).await;
    assert_eq!(response.status(), StatusCode::OK);
    let legacy: Value = test::read_body_json(response).await;
    assert_eq!(
        legacy["messages"][0]["content"],
        "PRIVATE replacement history"
    );
    assert!(legacy.get("current_user_target").is_none());
    f.state.shutdown().await;
}

#[actix_web::test]
async fn managed_history_requires_owner_and_matching_supervisor_and_path() {
    let f = Fixture::new(false).await;
    let (_, directory) = f.call(Method::GET, None, None).await;
    let target = directory["sessions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["session_id"] == "target-a")
        .unwrap()["current_user_target"]
        .clone();
    let binding = json!({"supervisor": f.supervisor, "target": target});
    for (remote, worker) in [(true, false), (false, true)] {
        let (status, denied) = call(
            f.state.clone(),
            Method::POST,
            Some("target-a/history"),
            Some(binding.clone()),
            remote,
            worker,
            None,
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{denied}");
        assert!(denied.get("messages").is_none());
    }
    let (status, mismatch) = f
        .call(
            Method::POST,
            Some("target-b/history"),
            Some(binding.clone()),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{mismatch}");
    let mut stale_supervisor = binding;
    stale_supervisor["supervisor"]["incarnation_id"] = json!(uuid::Uuid::new_v4());
    let (status, stale) = f
        .call(
            Method::POST,
            Some("target-a/history"),
            Some(stale_supervisor),
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "{stale}");
    assert!(stale.get("messages").is_none());
    f.state.shutdown().await;
}

#[actix_web::test]
async fn managed_history_uses_durable_snapshot_even_with_a_same_birth_cache() {
    let f = Fixture::new(false).await;
    let (_, directory) = f.call(Method::GET, None, None).await;
    let target = directory["sessions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["session_id"] == "target-a")
        .unwrap()["current_user_target"]
        .clone();
    let binding = json!({"supervisor": f.supervisor, "target": target});
    let mut cached = f
        .state
        .storage
        .load_session("target-a")
        .await
        .unwrap()
        .unwrap();
    cached.add_message(Message::user("synthetic cache-only same-birth message"));
    f.state.sessions.insert(
        "target-a".into(),
        Arc::new(bamboo_engine::SessionSnapshot::new(cached)),
    );
    let (status, matching) = f
        .call(
            Method::POST,
            Some("target-a/history"),
            Some(binding.clone()),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{matching}");
    assert_eq!(matching["history_snapshot_source"], "durable");
    assert!(matching.get("bound_token_usage").is_some());
    assert!(!matching
        .to_string()
        .contains("synthetic cache-only same-birth message"));

    // The live runner owns its local mutable Session; like the legacy handler,
    // bound history chooses the persisted snapshot rather than a lagging cache.
    f.state
        .agent_runners
        .write()
        .await
        .insert("target-a".into(), bamboo_engine::AgentRunner::new());
    let (status, active) = f
        .call(
            Method::POST,
            Some("target-a/history"),
            Some(binding.clone()),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{active}");
    assert_eq!(active["history_snapshot_source"], "durable");
    assert!(!active
        .to_string()
        .contains("synthetic cache-only same-birth message"));
    f.state.agent_runners.write().await.remove("target-a");

    let mut other = Session::new("target-a", "model");
    other.set_project_id_meta(f.project.to_string());
    other.add_message(Message::user("PRIVATE wrong-birth cache"));
    f.state.sessions.insert(
        "target-a".into(),
        Arc::new(bamboo_engine::SessionSnapshot::new(other)),
    );
    let (status, safe) = f
        .call(Method::POST, Some("target-a/history"), Some(binding))
        .await;
    assert_eq!(status, StatusCode::OK, "{safe}");
    assert_eq!(safe["history_snapshot_source"], "durable");
    assert!(!safe.to_string().contains("PRIVATE wrong-birth cache"));
    assert!(safe
        .to_string()
        .contains("PRIVATE target-a original history"));
    f.state.shutdown().await;
}

#[actix_web::test]
async fn managed_history_binds_ordinary_child_and_its_ancestry() {
    let f = Fixture::new(false).await;
    let parent = f
        .state
        .storage
        .load_session("target-a")
        .await
        .unwrap()
        .unwrap();
    let mut child = Session::new_child_of("child-a", &parent, "model", "Synthetic child");
    child.set_project_id_meta(f.project.to_string());
    child.add_message(Message::user("synthetic child history"));
    f.state.storage.save_session(&child).await.unwrap();
    let (_, directory) = f.call(Method::GET, None, None).await;
    let target = directory["sessions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["session_id"] == "child-a")
        .unwrap()["current_user_target"]
        .clone();
    assert_eq!(target["kind"], "child");
    assert_eq!(target["parent_session_id"], "target-a");
    let binding = json!({"supervisor": f.supervisor, "target": target});
    let (status, history) = f
        .call(Method::POST, Some("child-a/history"), Some(binding))
        .await;
    assert_eq!(status, StatusCode::OK, "{history}");
    assert_eq!(history["current_user_target"], target);
    assert_eq!(history["messages"][0]["content"], "synthetic child history");
    f.state.shutdown().await;
}

// Synthetic state only: verify the readback evidence comes from the selected
// durable Session and that the Host activity probe does not wait under storage.
#[actix_web::test]
async fn managed_clear_readback_binds_full_count_pending_and_local_activity() {
    let f = Fixture::new(false).await;
    let (_, directory) = f.call(Method::GET, None, None).await;
    let target = directory["sessions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["session_id"] == "target-a")
        .unwrap()["current_user_target"]
        .clone();
    let ordinary = json!({"supervisor": f.supervisor, "target": target});
    let mut request = ordinary.clone();
    request["clear_readback"] = json!(true);
    let (_, default_history) = f
        .call(Method::POST, Some("target-a/history"), Some(ordinary))
        .await;
    assert!(default_history.get("bound_clear_evidence").is_none());

    let mut session = f
        .state
        .storage
        .load_session("target-a")
        .await
        .unwrap()
        .unwrap();
    session.messages.clear();
    f.state.storage.save_session(&session).await.unwrap();
    let (status, empty) = f
        .call(
            Method::POST,
            Some("target-a/history"),
            Some(request.clone()),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{empty}");
    assert_eq!(
        empty["bound_clear_evidence"],
        json!({
            "full_message_count": 0, "execution_active": false,
            "has_pending_question": false, "has_pending_approval": false
        })
    );
    assert_eq!(empty["bound_token_usage"], Value::Null);

    let mut hidden = Message::user("synthetic hidden runtime record");
    hidden.metadata = Some(json!({"hidden_from_ui": true}));
    session.add_message(hidden);
    f.state.storage.save_session(&session).await.unwrap();
    let (_, hidden_history) = f
        .call(
            Method::POST,
            Some("target-a/history"),
            Some(request.clone()),
        )
        .await;
    assert_eq!(hidden_history["messages"].as_array().unwrap().len(), 0);
    assert_eq!(
        hidden_history["bound_clear_evidence"]["full_message_count"],
        1
    );

    session.messages.clear();
    session.set_pending_question(
        "q".into(),
        "synthetic".into(),
        "synthetic".into(),
        vec![],
        false,
    );
    f.state.storage.save_session(&session).await.unwrap();
    let (_, pending) = f
        .call(
            Method::POST,
            Some("target-a/history"),
            Some(request.clone()),
        )
        .await;
    assert_eq!(
        pending["bound_clear_evidence"]["has_pending_question"],
        true
    );
    session.clear_pending_question();
    let mut runtime = bamboo_domain::AgentRuntimeState::default();
    runtime.plan_mode = Some(bamboo_domain::PlanModeState {
        entered_at: chrono::Utc::now(),
        pre_permission_mode: "default".into(),
        plan_file_path: None,
        status: bamboo_domain::PlanModeStatus::AwaitingApproval,
    });
    session.agent_runtime_state = Some(runtime);
    f.state.storage.save_session(&session).await.unwrap();
    let (_, approval) = f
        .call(
            Method::POST,
            Some("target-a/history"),
            Some(request.clone()),
        )
        .await;
    assert_eq!(
        approval["bound_clear_evidence"]["has_pending_approval"],
        true
    );

    f.state
        .agent_runners
        .write()
        .await
        .insert("target-a".into(), bamboo_engine::AgentRunner::new());
    let (_, active) = f
        .call(
            Method::POST,
            Some("target-a/history"),
            Some(request.clone()),
        )
        .await;
    assert_eq!(active["bound_clear_evidence"]["execution_active"], true);
    f.state.agent_runners.write().await.remove("target-a");
    let startup =
        crate::handlers::agent::events::begin_execute_startup(f.state.get_ref(), "target-a");
    let (_, starting) = f
        .call(
            Method::POST,
            Some("target-a/history"),
            Some(request.clone()),
        )
        .await;
    assert_eq!(starting["bound_clear_evidence"]["execution_active"], true);
    drop(startup);

    let held = f.state.agent_runners.write().await;
    let (status, busy) = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        f.call(
            Method::POST,
            Some("target-a/history"),
            Some(request.clone()),
        ),
    )
    .await
    .expect("no registry wait under storage guard");
    drop(held);
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{busy}");

    let startups = f.state.execute_startups.clone();
    let (locked_tx, locked_rx) = std::sync::mpsc::sync_channel(0);
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
    let holder = std::thread::spawn(move || {
        let _guard = startups.lock().unwrap();
        locked_tx.send(()).unwrap();
        release_rx.recv().unwrap();
    });
    locked_rx.recv().unwrap();
    let (status, busy) = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        f.call(
            Method::POST,
            Some("target-a/history"),
            Some(request.clone()),
        ),
    )
    .await
    .expect("no startup registry wait under storage guard");
    release_tx.send(()).unwrap();
    holder.join().unwrap();
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{busy}");

    let mut stale = request.clone();
    stale["target"]["authority_sha256"] = json!("f".repeat(64));
    let (status, _) = f
        .call(Method::POST, Some("target-a/history"), Some(stale))
        .await;
    assert_eq!(status, StatusCode::CONFLICT);

    request["query"] = json!({"projection":"messages"});
    let (status, _) = f
        .call(Method::POST, Some("target-a/history"), Some(request))
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    f.state.shutdown().await;
}

// Exercise the real C1 write and the opt-in same-identity readback through
// production routes, using only a temporary store and synthetic Session.
#[actix_web::test]
async fn managed_clear_readback_follows_acknowledged_c1() {
    let f = Fixture::new(false).await;
    // Retain the selector actually held by the UI when it issued C1.
    let (_, directory) = f.call(Method::GET, None, None).await;
    let target = directory["sessions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["session_id"] == "target-a")
        .unwrap()["current_user_target"]
        .clone();
    let app = test::init_service(
        App::new()
            .app_data(f.state.clone())
            .configure(configure_routes),
    )
    .await;
    let request = test::TestRequest::post()
        .uri("/api/v1/sessions/target-a/clear")
        .peer_addr("127.0.0.1:12345".parse().unwrap())
        .to_request();
    let response = test::call_service(&app, request).await;
    assert_eq!(response.status(), StatusCode::OK);
    let receipt: Value = test::read_body_json(response).await;
    assert_eq!(receipt["success"], true);
    assert_eq!(receipt["session_id"], "target-a");
    let (_, after_directory) = f.call(Method::GET, None, None).await;
    let after_target = &after_directory["sessions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["session_id"] == "target-a")
        .unwrap()["current_user_target"];
    assert_eq!(
        after_target, &target,
        "ordinary C1 must retain its observed selector"
    );
    let request = json!({"supervisor": f.supervisor, "target": target, "clear_readback": true});
    let (status, history) = f
        .call(Method::POST, Some("target-a/history"), Some(request))
        .await;
    assert_eq!(status, StatusCode::OK, "{history}");
    assert_eq!(
        history["bound_clear_evidence"],
        json!({
            "full_message_count": 0, "execution_active": false,
            "has_pending_question": false, "has_pending_approval": false
        })
    );
    assert_eq!(history["bound_token_usage"], Value::Null);
    assert_eq!(history["is_delta"], false);
    assert_eq!(history["truncated"], false);
    f.state.shutdown().await;
}

// C1 retains these permission replay markers. An empty visible transcript and
// an idle local runner are therefore insufficient clear-confirmation evidence.
#[actix_web::test]
async fn managed_clear_readback_refuses_permission_replay_or_uncertain_save() {
    for marker in [
        "permission.reexecute_tool_call_id",
        "permission.reexecute_request_generation",
        "permission.replay_save_recovery_required",
    ] {
        let f = Fixture::new(false).await;
        let mut session = f
            .state
            .storage
            .load_session("target-a")
            .await
            .unwrap()
            .unwrap();
        session.metadata.insert(marker.into(), "synthetic".into());
        f.state.storage.save_session(&session).await.unwrap();
        let (_, directory) = f.call(Method::GET, None, None).await;
        let target = directory["sessions"]
            .as_array()
            .unwrap()
            .iter()
            .find(|row| row["session_id"] == "target-a")
            .unwrap()["current_user_target"]
            .clone();
        let app = test::init_service(
            App::new()
                .app_data(f.state.clone())
                .configure(configure_routes),
        )
        .await;
        let clear = test::TestRequest::post()
            .uri("/api/v1/sessions/target-a/clear")
            .peer_addr("127.0.0.1:12345".parse().unwrap())
            .to_request();
        let response = test::call_service(&app, clear).await;
        assert_eq!(response.status(), StatusCode::OK, "{marker}");
        let receipt: Value = test::read_body_json(response).await;
        assert_eq!(receipt["success"], true, "{marker}");
        let stored = f
            .state
            .storage
            .load_session("target-a")
            .await
            .unwrap()
            .unwrap();
        assert!(stored.messages.is_empty(), "{marker}");
        assert!(stored.pending_question.is_none(), "{marker}");
        assert!(stored.token_usage.is_none(), "{marker}");
        assert!(stored.metadata.contains_key(marker), "{marker}");
        assert!(
            !f.state.agent_runners.read().await.contains_key("target-a"),
            "{marker}"
        );
        let body = json!({"supervisor": f.supervisor, "target": target, "clear_readback": true});
        let (status, history) = f
            .call(Method::POST, Some("target-a/history"), Some(body.clone()))
            .await;
        if marker == "permission.replay_save_recovery_required" {
            assert_eq!(status, StatusCode::LOCKED, "{marker}: {history}");
            assert_eq!(history["code"], "clear_readback_unsettled");
            assert!(history.get("bound_clear_evidence").is_none());
        } else {
            assert_eq!(status, StatusCode::OK, "{marker}: {history}");
            assert_eq!(
                history["bound_clear_evidence"]["has_pending_approval"], true,
                "{marker}"
            );
        }
        let mut ordinary = body;
        ordinary.as_object_mut().unwrap().remove("clear_readback");
        let (status, ordinary_history) = f
            .call(Method::POST, Some("target-a/history"), Some(ordinary))
            .await;
        assert_eq!(status, StatusCode::OK, "ordinary history: {marker}");
        assert!(ordinary_history.get("bound_clear_evidence").is_none());
        f.state.shutdown().await;
    }
}
