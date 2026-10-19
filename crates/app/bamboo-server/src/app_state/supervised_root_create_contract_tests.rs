use super::*;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use bamboo_agent_core::tools::{ExecutingSupervisorObservation, Tool, ToolCtx, ToolOutcome};
use bamboo_domain::{
    SessionActivationError, SessionActivationPort, SessionInboxLimits, SessionInboxPort,
};
use bamboo_engine::session_app::supervisor::SupervisorSessionService;
use bamboo_storage::{FileSessionInbox, LockedSessionStore, SessionStoreV2};

#[derive(Default)]
struct Activation {
    fail: AtomicBool,
    pause: AtomicBool,
    calls: AtomicUsize,
    arrived: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

#[async_trait]
impl SessionActivationPort for Activation {
    async fn request_activation(
        &self,
        _target: &str,
        _generation: u64,
    ) -> Result<SessionActivationDisposition, SessionActivationError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.pause.swap(false, Ordering::SeqCst) {
            self.arrived.notify_one();
            self.release.notified().await;
        }
        if self.fail.load(Ordering::SeqCst) {
            Err(SessionActivationError::Internal(
                "fixture activation unavailable".into(),
            ))
        } else {
            Ok(SessionActivationDisposition::ActivationReserved)
        }
    }
}

struct Fixture {
    home: tempfile::TempDir,
    creator: SupervisedRootCreator,
    inbox: Arc<FileSessionInbox>,
    activation: Arc<Activation>,
    supervisor: SupervisorReference,
    request: SupervisedRootCreateRequest,
}

impl Fixture {
    async fn new() -> Self {
        let home = tempfile::tempdir().unwrap();
        let (creator, inbox, activation) = Self::open(home.path()).await;
        let workspace = home.path().join("workspaces").join("worker");
        std::fs::create_dir_all(&workspace).unwrap();
        let project = creator
            .projects
            .create_with_project_path(
                "Explicit Project",
                None,
                workspace.to_string_lossy(),
                Vec::new(),
            )
            .unwrap();
        let service = SupervisorSessionService::new(creator.store.clone());
        let supervisor = SupervisorReference::from(
            &service
                .get_or_create_default("private-model")
                .await
                .unwrap(),
        );
        service
            .configure_project_scope(&supervisor, 0, [project.id.clone()].into())
            .await
            .unwrap();
        let request = SupervisedRootCreateRequest {
            operation_id: uuid::Uuid::new_v4(),
            expected_state_revision: 1,
            project_id: project.id,
            workspace_path: workspace.to_string_lossy().into_owned(),
            message: "Inspect only this explicit assignment".into(),
            title: Some("Independent task".into()),
        };
        Self {
            home,
            creator,
            inbox,
            activation,
            supervisor,
            request,
        }
    }

    async fn open(
        home: &std::path::Path,
    ) -> (
        SupervisedRootCreator,
        Arc<FileSessionInbox>,
        Arc<Activation>,
    ) {
        let store = Arc::new(SessionStoreV2::new(home.to_path_buf()).await.unwrap());
        let projects = Arc::new(bamboo_projects::ProjectStore::open(home).unwrap());
        let root = home.join("workspaces");
        let workspace = bamboo_agent_core::workspace_state::WorkspaceResolver::new(
            || None,
            move || bamboo_agent_core::workspace_state::WorkspaceRootConfig {
                root: root.clone(),
                confine: true,
            },
        );
        let project_context = Arc::new(
            bamboo_engine::project_context::ProjectContextResolver::new_with_workspace_resolver(
                Arc::new(crate::project_context::ProjectStoreContextSource::new(
                    projects.clone(),
                )),
                workspace.clone(),
            ),
        );
        let cache: bamboo_engine::SessionCache = Arc::new(Default::default());
        let repo = SessionRepository::new(
            cache,
            store.clone(),
            Arc::new(LockedSessionStore::new(store.clone())),
        )
        .with_root_actor_directory(store.clone());
        let inbox = Arc::new(FileSessionInbox::new(
            store.clone(),
            SessionInboxLimits::default(),
        ));
        let activation = Arc::new(Activation::default());
        let messenger = Arc::new(SessionMessenger::new(
            store.clone(),
            inbox.clone(),
            activation.clone(),
        ));
        let creator = SupervisedRootCreator {
            store,
            operations: Arc::new(SessionCreateOperationStore::new(home)),
            projects,
            workspace,
            project_context,
            config: Arc::new(RwLock::new(bamboo_llm::Config::default())),
            repo,
            messenger,
            account: bamboo_engine::events::AccountEventSink::new(home.join("events")).unwrap(),
        };
        (creator, inbox, activation)
    }

    async fn create(&self, request: SupervisedRootCreateRequest) -> ToolResult {
        self.creator
            .create(&self.supervisor, request)
            .await
            .unwrap()
    }

    async fn count(&self) -> usize {
        self.creator.store.list_index_entries().await.len()
    }

    fn digest(&self) -> String {
        key_digest(&format!(
            "supervised-root:v1\0{}\0{}\0{}",
            self.supervisor.session_id, self.supervisor.incarnation_id, self.request.operation_id
        ))
    }

    async fn reserve(&self) -> (SessionCreateOperationRecord, Session) {
        let digest = self.digest();
        let record = SessionCreateOperationRecord::pending(
            digest.clone(),
            payload_fingerprint(&(&self.supervisor, &self.request)).unwrap(),
            uuid::Uuid::parse_str(&digest[..32]).unwrap().to_string(),
        );
        self.creator.operations.save(&record).await.unwrap();
        let mut candidate = self
            .creator
            .build_candidate(
                &record,
                &self.request,
                std::path::Path::new(&self.request.workspace_path),
            )
            .await;
        self.creator
            .project_context
            .refresh_session_prompt_read_only(&mut candidate)
            .await
            .unwrap();
        (record, candidate)
    }
}

fn value(result: ToolResult) -> Value {
    serde_json::from_str(&result.result).unwrap()
}

fn accepted(result: ToolResult) -> Value {
    assert!(result.success, "{}", result.result);
    let result = value(result);
    assert_eq!(result["admitted"], true);
    result
}

#[tokio::test]
async fn concurrent_creation_and_restart_replay_preserve_one_root_link_and_first_input() {
    let f = Fixture::new().await;
    let (one, two) = tokio::join!(f.create(f.request.clone()), f.create(f.request.clone()));
    let one = accepted(one);
    let two = accepted(two);
    assert_eq!(one["target_session_id"], two["target_session_id"]);
    assert_eq!(one["receipt_id"], two["receipt_id"]);
    assert_eq!(f.count().await, 2);
    assert_eq!(f.activation.calls.load(Ordering::SeqCst), 1);
    let (reopened, inbox, activation) = Fixture::open(f.home.path()).await;
    let replay = accepted(
        reopened
            .create(&f.supervisor, f.request.clone())
            .await
            .unwrap(),
    );
    assert_eq!(replay["target_session_id"], one["target_session_id"]);
    assert_eq!(replay["receipt_id"], one["receipt_id"]);
    assert_eq!(replay["replayed"], true);
    assert_eq!(activation.calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        inbox
            .inspect(one["target_session_id"].as_str().unwrap())
            .await
            .unwrap()
            .generation,
        1
    );
}

#[tokio::test]
async fn changed_payload_conflicts_without_a_second_root_or_instruction() {
    let f = Fixture::new().await;
    let first = accepted(f.create(f.request.clone()).await);
    let mut changed = f.request.clone();
    changed.message = "A different instruction is a new operation".into();
    let conflict = f.create(changed).await;
    assert!(!conflict.success);
    assert_eq!(value(conflict)["code"], "idempotency_conflict");
    assert_eq!(f.count().await, 2);
    assert_eq!(f.activation.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        f.inbox
            .inspect(first["target_session_id"].as_str().unwrap())
            .await
            .unwrap()
            .generation,
        1
    );
}

#[tokio::test]
async fn invalid_project_workspace_and_stale_scope_have_no_root_or_link_side_effects() {
    let f = Fixture::new().await;
    let before = f.count().await;
    let service = SupervisorSessionService::new(f.creator.store.clone());
    for case in [
        "foreign_project",
        "foreign_workspace",
        "missing_directory",
        "unbound_directory",
        "stale_scope",
    ] {
        let mut request = f.request.clone();
        match case {
            "foreign_project" => request.project_id = "outside-scope".parse().unwrap(),
            "foreign_workspace" => {
                let path = f.home.path().join("workspaces/foreign");
                std::fs::create_dir_all(&path).unwrap();
                f.creator
                    .projects
                    .create_with_project_path(
                        "Foreign owner",
                        None,
                        path.to_string_lossy(),
                        Vec::new(),
                    )
                    .unwrap();
                request.workspace_path = path.to_string_lossy().into_owned();
            }
            "missing_directory" => request.workspace_path.push_str("/missing"),
            "unbound_directory" => {
                let path = f.home.path().join("workspaces/unbound");
                std::fs::create_dir_all(&path).unwrap();
                request.workspace_path = path.to_string_lossy().into_owned();
            }
            "stale_scope" => request.expected_state_revision = 0,
            _ => unreachable!(),
        }
        let result = f.create(request).await;
        assert!(!result.success, "{case}: {}", result.result);
        let expected = match case {
            "foreign_project" => "scope_denied",
            "foreign_workspace" => "project_workspace_conflict",
            "missing_directory" => "workspace_invalid",
            "unbound_directory" => "project_workspace_unbound",
            "stale_scope" => "scope_revision_conflict",
            _ => unreachable!(),
        };
        assert_eq!(value(result)["code"], expected, "{case}");
        assert_eq!(f.count().await, before, "{case}");
        assert_eq!(
            service
                .inspect_scope(&f.supervisor)
                .await
                .unwrap()
                .state_revision,
            1,
            "{case}"
        );
        assert_eq!(f.activation.calls.load(Ordering::SeqCst), 0, "{case}");
    }
}

#[tokio::test]
async fn recovered_root_before_link_and_link_before_input_use_the_reserved_lifetime_once() {
    for linked in [false, true] {
        let f = Fixture::new().await;
        let (record, candidate) = f.reserve().await;
        if linked {
            f.creator
                .store
                .create_and_attach_supervised_root(&f.supervisor, 1, &candidate)
                .await
                .unwrap();
        } else {
            // Simulate a crash after the existing canonical Root save but
            // before management publication. The reservation preceded it.
            f.creator.store.save_session(&candidate).await.unwrap();
        }
        let result = accepted(f.create(f.request.clone()).await);
        assert_eq!(result["target_session_id"], record.session_id);
        assert_eq!(result["link_revision"], 1);
        assert_eq!(result["generation"], 1);
        assert_eq!(f.count().await, 2);
        let canonical = f
            .creator
            .store
            .load_session(&record.session_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(canonical.created_at, candidate.created_at);
        assert_eq!(canonical.model, candidate.model);
        assert_eq!(f.activation.calls.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn activation_failure_reopen_retry_preserves_the_durable_first_instruction() {
    let f = Fixture::new().await;
    f.activation.fail.store(true, Ordering::SeqCst);
    let pending = accepted(f.create(f.request.clone()).await);
    assert_eq!(pending["activation"], "activation_pending");
    let (reopened, inbox, _) = Fixture::open(f.home.path()).await;
    let recovered = accepted(
        reopened
            .create(&f.supervisor, f.request.clone())
            .await
            .unwrap(),
    );
    assert_eq!(recovered["target_session_id"], pending["target_session_id"]);
    assert_eq!(recovered["receipt_id"], pending["receipt_id"]);
    assert_eq!(recovered["generation"], pending["generation"]);
    assert_eq!(
        inbox
            .inspect(recovered["target_session_id"].as_str().unwrap())
            .await
            .unwrap()
            .generation,
        1
    );
}

#[tokio::test]
async fn caller_cancellation_does_not_drop_the_durable_creation_drive() {
    let f = Fixture::new().await;
    f.activation.pause.store(true, Ordering::SeqCst);
    let creator = f.creator.clone();
    let supervisor = f.supervisor.clone();
    let request = f.request.clone();
    let caller = tokio::spawn(async move { creator.create(&supervisor, request).await });
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        f.activation.arrived.notified(),
    )
    .await
    .unwrap();
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    f.activation.release.notify_one();
    let result = accepted(f.create(f.request.clone()).await);
    assert_eq!(result["generation"], 1);
    assert_eq!(f.count().await, 2);
    assert_eq!(f.activation.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn revoked_or_replaced_links_and_deleted_targets_never_recover_creation_authority() {
    for case in ["detach", "reattach", "delete", "scope", "incarnation"] {
        let f = Fixture::new().await;
        let first = accepted(f.create(f.request.clone()).await);
        let id = first["target_session_id"].as_str().unwrap();
        let service = SupervisorSessionService::new(f.creator.store.clone());
        match case {
            "detach" | "reattach" => {
                service.detach(&f.supervisor, 2, id).await.unwrap();
                if case == "reattach" {
                    service.attach(&f.supervisor, 3, id).await.unwrap();
                }
            }
            "delete" => {
                f.creator.store.delete_session(id).await.unwrap();
            }
            "scope" => {
                service
                    .configure_project_scope(&f.supervisor, 2, Default::default())
                    .await
                    .unwrap();
            }
            "incarnation" => {
                f.creator
                    .store
                    .delete_session(&f.supervisor.session_id)
                    .await
                    .unwrap();
                service.get_or_create_default("replacement").await.unwrap();
            }
            _ => unreachable!(),
        }
        let count = f.count().await;
        let replay = f.create(f.request.clone()).await;
        assert!(!replay.success, "{case}: {}", replay.result);
        assert_eq!(f.count().await, count, "{case}");
        assert_eq!(f.activation.calls.load(Ordering::SeqCst), 1, "{case}");
        if case == "delete" {
            assert!(f.creator.store.load_session(id).await.unwrap().is_none());
        }
    }
}

#[tokio::test]
async fn ordinary_root_child_and_stale_executing_identity_are_rejected_before_writes() {
    let f = Fixture::new().await;
    let tool = bamboo_server_tools::SessionControlTool::new(f.creator.messenger.clone())
        .with_root_creator(Arc::new(f.creator.clone()));
    for case in [
        "ordinary",
        "ultra",
        "child",
        "stale_incarnation",
        "other_caller",
        "plan",
    ] {
        let canonical = f
            .creator
            .store
            .load_session(&f.supervisor.session_id)
            .await
            .unwrap()
            .unwrap();
        let mut executing = canonical.clone();
        let mut ctx = ToolCtx::none("rejected-create");
        ctx.session_id = Some(canonical.id.clone().into());
        match case {
            "ordinary" | "ultra" | "child" => {
                executing.authority_identity = bamboo_domain::SessionAuthorityIdentity::Ordinary;
                if case == "child" {
                    executing.kind = bamboo_domain::SessionKind::Child;
                }
                executing.root_orchestration_only = case == "ultra";
            }
            "stale_incarnation" => {
                executing.authority_identity = bamboo_domain::SessionAuthorityIdentity::Supervisor {
                    incarnation_id: uuid::Uuid::new_v4(),
                }
            }
            "other_caller" => ctx.session_id = Some("unrelated-caller".into()),
            "plan" => ctx.plan_read_only = true,
            _ => unreachable!(),
        }
        ctx.executing_supervisor =
            ExecutingSupervisorObservation::capture_from_executing_session(&executing);
        let args = json!({"action":"create", "request":f.request});
        let result = tool.invoke(args, ctx).await;
        match result {
            Err(_) => {}
            Ok(ToolOutcome::Completed(result)) => assert!(!result.success, "{case}"),
            Ok(_) => panic!("unauthorized create must not run: {case}"),
        }
        assert_eq!(f.count().await, 1, "{case}");
        assert_eq!(f.activation.calls.load(Ordering::SeqCst), 0, "{case}");
    }
}

#[tokio::test]
async fn damaged_receipt_cannot_attach_a_historical_root_and_expiry_cannot_allocate_another() {
    let f = Fixture::new().await;
    let (mut record, _) = f.reserve().await;
    let mut historical = Session::new("unrelated-root", "original-model");
    historical.set_project_id_meta(f.request.project_id.to_string());
    historical.set_workspace_path_meta(&f.request.workspace_path);
    f.creator.store.save_session(&historical).await.unwrap();
    record.session_id = historical.id.clone();
    record.created_at = historical.created_at;
    f.creator.operations.save(&record).await.unwrap();
    let blocked = f.create(f.request.clone()).await;
    assert!(!blocked.success);
    assert_eq!(value(blocked)["code"], "operation_identity_conflict");
    assert!(
        !f.creator
            .store
            .inspect_supervisor_link(&f.supervisor, &historical.id)
            .await
            .unwrap()
            .authorized
    );
    assert_eq!(f.activation.calls.load(Ordering::SeqCst), 0);

    let fresh = Fixture::new().await;
    let original = accepted(fresh.create(fresh.request.clone()).await);
    let mut record = fresh
        .creator
        .operations
        .load_for_status(&fresh.digest())
        .await
        .unwrap()
        .unwrap();
    record.expires_at = Some(chrono::Utc::now() - chrono::Duration::seconds(1));
    fresh.creator.operations.save(&record).await.unwrap();
    let expired = fresh.create(fresh.request.clone()).await;
    assert!(!expired.success);
    assert_eq!(fresh.count().await, 2);
    assert!(fresh
        .creator
        .store
        .load_session(original["target_session_id"].as_str().unwrap())
        .await
        .unwrap()
        .is_some());
    assert_eq!(fresh.activation.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn scope_observation_reports_uninitialized_scope_without_granting_or_creating() {
    let f = Fixture::new().await;
    let scoped = value(f.creator.scope(&f.supervisor).await.unwrap());
    assert_eq!(scoped["allowed_projects"], json!([f.request.project_id]));
    assert_eq!(scoped["code"], "project_scope_configured");
    SupervisorSessionService::new(f.creator.store.clone())
        .configure_project_scope(&f.supervisor, 1, Default::default())
        .await
        .unwrap();
    let uninitialized = value(f.creator.scope(&f.supervisor).await.unwrap());
    assert_eq!(uninitialized["project_scope_configured"], false);
    assert_eq!(uninitialized["code"], "project_scope_uninitialized");
    assert_eq!(uninitialized["allowed_projects"], json!([]));
    assert_eq!(f.count().await, 1);
    assert_eq!(f.activation.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn canonical_creation_rechecks_revocation_and_scope_cas_before_root_publication() {
    for regrant in [false, true] {
        let f = Fixture::new().await;
        let (_, candidate) = f.reserve().await;
        let service = SupervisorSessionService::new(f.creator.store.clone());
        service
            .configure_project_scope(&f.supervisor, 1, Default::default())
            .await
            .unwrap();
        if regrant {
            service
                .configure_project_scope(&f.supervisor, 2, [f.request.project_id.clone()].into())
                .await
                .unwrap();
        }
        assert!(f
            .creator
            .store
            .create_and_attach_supervised_root(&f.supervisor, 1, &candidate)
            .await
            .is_err());
        assert!(f
            .creator
            .store
            .load_root_authority(&candidate.id)
            .await
            .unwrap()
            .is_none());
        assert_eq!(f.count().await, 1);
        assert_eq!(f.activation.calls.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn terminal_failed_creation_receipt_cannot_start_a_root() {
    let f = Fixture::new().await;
    let (mut record, _) = f.reserve().await;
    record.mark_failed(
        super::super::session_create_operations::StoredOperationError {
            code: "fixture_terminal".into(),
            message: "Host-recorded terminal failure".into(),
            http_status: 409,
        },
    );
    f.creator.operations.save(&record).await.unwrap();
    let result = f.create(f.request.clone()).await;
    assert!(!result.success);
    assert_eq!(value(result)["code"], "creation_failed");
    assert_eq!(f.count().await, 1);
    assert_eq!(f.activation.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn incomplete_or_corrupt_canonical_creation_requires_host_repair_without_overwrite() {
    for damaged in ["missing", "corrupt"] {
        let f = Fixture::new().await;
        let (record, candidate) = f.reserve().await;
        f.creator.store.save_session(&candidate).await.unwrap();
        let directory = f.creator.store.sessions_root_dir().join(&record.session_id);
        let runtime = directory.join("runtime.json");
        let original_runtime = std::fs::read(&runtime).unwrap();
        let original_main = std::fs::read(directory.join("session.json")).unwrap();
        if damaged == "missing" {
            std::fs::remove_file(&runtime).unwrap();
        } else {
            std::fs::write(&runtime, b"invalid runtime authority").unwrap();
        }
        let blocked = f.create(f.request.clone()).await;
        assert!(!blocked.success, "{damaged}");
        assert_eq!(f.count().await, 2);
        assert_eq!(
            std::fs::read(directory.join("session.json")).unwrap(),
            original_main
        );
        assert_eq!(f.activation.calls.load(Ordering::SeqCst), 0);
        assert!(!SupervisorSessionService::new(f.creator.store.clone())
            .inspect_scope(&f.supervisor)
            .await
            .unwrap()
            .allowed_projects
            .is_empty());
        // Only the fixture's Host restores its known exact canonical bytes.
        // The model creation path neither guesses authority nor repairs it.
        std::fs::write(&runtime, original_runtime).unwrap();
        let repaired = accepted(f.create(f.request.clone()).await);
        assert_eq!(repaired["target_session_id"], record.session_id);
        assert_eq!(repaired["generation"], 1);
        assert_eq!(f.count().await, 2);
    }
}

fn native_git(path: &std::path::Path, args: &[&str]) -> String {
    let output = std::process::Command::new("git")
        .args(args)
        .current_dir(path)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
}

fn v2_context(session: &Session) -> ToolCtx {
    let mut ctx = ToolCtx::none("v2-native-spawn");
    ctx.session_id = Some(session.id.clone().into());
    ctx.executing_supervisor =
        ExecutingSupervisorObservation::capture_from_executing_session(session);
    ctx
}

#[tokio::test]
async fn versioned_spawn_uses_an_explicit_independent_checkout_and_recovers_first_task_on_reopen() {
    use bamboo_server_tools::session_control::versioned::SpawnSessionTool;
    let f = Fixture::new().await;
    let main = f.home.path().join("workspaces").join("source-checkout");
    std::fs::create_dir_all(&main).unwrap();
    native_git(&main, &["init", "--initial-branch=fixture"]);
    native_git(
        &main,
        &[
            "-c",
            "user.name=Fixture",
            "-c",
            "user.email=fixture@example.invalid",
            "-c",
            "core.hooksPath=/dev/null",
            "commit",
            "--allow-empty",
            "-m",
            "isolated fixture",
        ],
    );
    native_git(
        &main,
        &["worktree", "add", "--detach", &f.request.workspace_path],
    );
    let worker = std::path::Path::new(&f.request.workspace_path);
    assert_ne!(
        native_git(&main, &["rev-parse", "--show-toplevel"]),
        native_git(worker, &["rev-parse", "--show-toplevel"])
    );
    assert_eq!(
        native_git(&main, &["rev-parse", "HEAD"]),
        native_git(worker, &["rev-parse", "HEAD"])
    );
    let session = f
        .creator
        .store
        .load_session(&f.supervisor.session_id)
        .await
        .unwrap()
        .unwrap();
    let original = serde_json::to_value(&session).unwrap();
    let tool = SpawnSessionTool::new(
        Arc::new(f.creator.clone()),
        f.creator.store.clone(),
        f.creator.config.clone(),
        Arc::new(bamboo_tools::BuiltinToolExecutor::with_registry(bamboo_tools::ToolRegistry::new())),
    );
    f.activation.fail.store(true, Ordering::SeqCst);
    let ToolOutcome::Completed(result) = tool
        .invoke(
            serde_json::to_value(&f.request).unwrap(),
            v2_context(&session),
        )
        .await
        .unwrap()
    else {
        panic!("bounded Root receipt")
    };
    let one = accepted(result);
    assert_eq!(one["activation"], "activation_pending");
    let id = one["target_session_id"].as_str().unwrap();
    let root = f.creator.store.load_session(id).await.unwrap().unwrap();
    assert_eq!(root.kind, bamboo_domain::SessionKind::Root);
    assert!(root.parent_session_id.is_none());
    assert_eq!(root.session_version, Some(2));
    assert_eq!(
        root.workspace_path_meta().as_deref(),
        Some(f.request.workspace_path.as_str())
    );
    let after = serde_json::to_value(
        f.creator
            .store
            .load_session(&session.id)
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    for key in [
        "messages",
        "model",
        "authority_identity",
        "session_version",
        "root_orchestration_only",
    ] {
        assert_eq!(
            after[key], original[key],
            "Supervisor {key} must not be inherited or overwritten"
        );
    }
    let (reopened, inbox, activation) = Fixture::open(f.home.path()).await;
    let replay_tool = SpawnSessionTool::new(
        Arc::new(reopened.clone()),
        reopened.store.clone(),
        reopened.config.clone(),
        Arc::new(bamboo_tools::BuiltinToolExecutor::with_registry(bamboo_tools::ToolRegistry::new())),
    );
    let ToolOutcome::Completed(result) = replay_tool
        .invoke(
            serde_json::to_value(&f.request).unwrap(),
            v2_context(&session),
        )
        .await
        .unwrap()
    else {
        panic!("bounded recovery receipt")
    };
    let recovered = accepted(result);
    assert_eq!(recovered["target_session_id"], one["target_session_id"]);
    assert_eq!(recovered["receipt_id"], one["receipt_id"]);
    assert_eq!(recovered["generation"], one["generation"]);
    assert_eq!(inbox.inspect(id).await.unwrap().generation, 1);
    assert_eq!(activation.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn versioned_spawn_disabled_stale_and_invalid_scope_refusals_have_no_side_effects() {
    use bamboo_server_tools::session_control::versioned::SpawnSessionTool;
    let f = Fixture::new().await;
    let canonical = f
        .creator
        .store
        .load_session(&f.supervisor.session_id)
        .await
        .unwrap()
        .unwrap();
    let original = serde_json::to_value(&canonical).unwrap();
    let tool = SpawnSessionTool::new(
        Arc::new(f.creator.clone()),
        f.creator.store.clone(),
        f.creator.config.clone(),
        Arc::new(bamboo_tools::BuiltinToolExecutor::with_registry(bamboo_tools::ToolRegistry::new())),
    );
    for case in [
        "ordinary",
        "child",
        "stale",
        "plan",
        "disabled",
        "foreign_project",
        "unbound_workspace",
        "scope_cas",
    ] {
        let mut ctx = v2_context(&canonical);
        let mut request = f.request.clone();
        match case {
            "ordinary" | "child" => ctx.executing_supervisor = None,
            "stale" => {
                let mut stale = canonical.clone();
                stale.authority_identity = bamboo_domain::SessionAuthorityIdentity::Supervisor {
                    incarnation_id: uuid::Uuid::new_v4(),
                };
                ctx = v2_context(&stale);
            }
            "plan" => ctx.plan_read_only = true,
            "disabled" => {
                f.creator.config.write().await.tools.disabled = vec!["default::spawnSession".into()]
            }
            "foreign_project" => request.project_id = "foreign-project".parse().unwrap(),
            "unbound_workspace" => {
                request.workspace_path = f
                    .home
                    .path()
                    .join("workspaces/unbound")
                    .to_string_lossy()
                    .into_owned()
            }
            "scope_cas" => request.expected_state_revision = 0,
            _ => unreachable!(),
        }
        let outcome = tool
            .invoke(serde_json::to_value(request).unwrap(), ctx)
            .await;
        assert!(
            match outcome {
                Err(_) => true,
                Ok(ToolOutcome::Completed(r)) => !r.success,
                _ => false,
            },
            "{case}"
        );
        f.creator.config.write().await.tools.disabled.clear();
        assert_eq!(f.count().await, 1, "{case}");
        assert_eq!(f.activation.calls.load(Ordering::SeqCst), 0, "{case}");
        assert_eq!(
            serde_json::to_value(
                f.creator
                    .store
                    .load_session(&canonical.id)
                    .await
                    .unwrap()
                    .unwrap()
            )
            .unwrap(),
            original,
            "{case}"
        );
    }
}

#[tokio::test]
async fn versioned_spawn_disabled_exact_shadow_does_not_block_framework_creation_or_approval_recheck() {
    use bamboo_agent_core::tools::{FunctionCall, ToolCall, ToolExecutionContext, ToolExecutor};
    use bamboo_server_tools::session_control::versioned::{SpawnSessionTool, VersionedRootTools};

    struct ExactShadow(Arc<AtomicUsize>);
    #[async_trait]
    impl Tool for ExactShadow {
        fn name(&self) -> &str { "vendor::spawn_session" }
        fn description(&self) -> &str { "independent exact registration" }
        fn parameters_schema(&self) -> serde_json::Value { json!({"type":"object"}) }
        async fn invoke(&self, _: serde_json::Value, _: ToolCtx) -> Result<ToolOutcome, bamboo_agent_core::tools::ToolError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(ToolOutcome::Completed(ToolResult::text(true, "exact shadow")))
        }
    }

    let f = Fixture::new().await;
    let session = f.creator.store.load_session(&f.supervisor.session_id).await.unwrap().unwrap();
    let shadow_calls = Arc::new(AtomicUsize::new(0));
    let registry = bamboo_tools::ToolRegistry::new();
    registry.register(ExactShadow(shadow_calls.clone())).unwrap();
    let legacy: Arc<dyn ToolExecutor> = Arc::new(bamboo_tools::BuiltinToolExecutor::with_registry(registry));
    let spawn = Arc::new(SpawnSessionTool::new(Arc::new(f.creator.clone()), f.creator.store.clone(), f.creator.config.clone(), legacy.clone()));
    let installed = VersionedRootTools::new(legacy, spawn, f.creator.store.clone());
    let tools = installed.for_session(&session).unwrap();
    let context = || ToolExecutionContext {
        session_id: Some(&session.id),
        root_session_id: Some(&session.id),
        executing_supervisor: ExecutingSupervisorObservation::capture_from_executing_session(&session),
        ..ToolExecutionContext::none("disabled-owner-recheck")
    };
    let call = ToolCall {
        id: "disabled-owner-recheck".into(),
        tool_type: "function".into(),
        function: FunctionCall {
            name: "default::spawnSession".into(),
            arguments: serde_json::to_string(&f.request).unwrap(),
        },
    };
    f.creator.config.write().await.tools.disabled = vec!["vendor::spawn_session".into()];
    assert_eq!(bamboo_domain::resolve_session_tool_reference(Some(2), "vendor::spawn_session", |name| tools.owns_exact_tool(name)).as_deref(), Some("vendor::spawn_session"));
    assert!(tools.check_permissions_for_exact(&call, "spawn_session", &context()).await.unwrap().is_none());
    assert_eq!(f.count().await, 1, "permission recheck must not create a Root");
    let result = tools.execute_exact_with_context_outcome(&call, "spawn_session", context()).await.unwrap().into_tool_result();
    let receipt = accepted(result);
    let target = receipt["target_session_id"].as_str().unwrap();
    assert_eq!(f.creator.store.load_session(target).await.unwrap().unwrap().kind, bamboo_domain::SessionKind::Root);
    assert_eq!(f.inbox.inspect(target).await.unwrap().generation, 1);
    assert_eq!(shadow_calls.load(Ordering::SeqCst), 0);
    for disabled in ["spawn_session", "spawnSession", "default::spawnSession", "other::spawn_session"] {
        f.creator.config.write().await.tools.disabled = vec![disabled.into()];
        let error = tools.check_permissions_for_exact(&call, "spawn_session", &context()).await.err().expect("unshadowed disabled Root must still refuse");
        assert!(error.to_string().contains("disabled"), "{disabled}: {error}");
        assert_eq!(f.count().await, 2);
        assert_eq!(f.inbox.inspect(target).await.unwrap().generation, 1);
        assert_eq!(shadow_calls.load(Ordering::SeqCst), 0);
    }
}
