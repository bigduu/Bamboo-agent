#![cfg(unix)]
use super::*;
use async_trait::async_trait;
use bamboo_agent_core::tools::{FunctionCall, ToolCall, ToolSchema};
use bamboo_domain::{Message, WorkspaceBinding};
use bamboo_llm::{Config, LLMChunk, LLMError, LLMProvider, LLMStream, ProviderRegistry};
use bamboo_orchestration::patrol::{program::ProgramPatrol, Clock, Policy, Scope};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

const SEED: &str = "BAMBOO_2499_STATUS_SEED";
const WAIT: Duration = Duration::from_secs(20);
struct Provider {
    calls: AtomicUsize,
    limit: usize,
}
#[async_trait]
impl LLMProvider for Provider {
    async fn chat_stream(
        &self,
        _messages: &[Message],
        tools: &[ToolSchema],
        _max: Option<u32>,
        model: &str,
    ) -> std::result::Result<LLMStream, LLMError> {
        assert_eq!(model, "patrol-status-local");
        assert!(tools.iter().any(|tool| tool.function.name == "Task"));
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        assert!(call < self.limit, "status must not activate a provider");
        let chunk = if call == 0 {
            LLMChunk::ToolCalls(vec![ToolCall{id:"bounded-status-task".into(),tool_type:"function".into(),function:FunctionCall{name:"Task".into(),
            arguments:json!({"tasks":[{"id":"result","content":"Produce a bounded result","status":"completed"}]}).to_string()}}])
        } else {
            LLMChunk::Token("A bounded result for owner review.".into())
        };
        Ok(Box::pin(futures::stream::iter([
            Ok(chunk),
            Ok(LLMChunk::Done),
        ])))
    }
}
async fn host(root: &Path, limit: usize) -> (web::Data<AppState>, Arc<Provider>) {
    let provider = Arc::new(Provider {
        calls: AtomicUsize::new(0),
        limit,
    });
    let mut config:Config=serde_json::from_value(json!({"provider":"openai","features":{"ticket_mutation":true,"ticket_dispatch":true},
        "providers":{"openai":{"api_key":"unused-controlled-fixture","model":"patrol-status-local"}},"run_budget":{"max_rounds":4},"subagents":{"max_concurrent":1}})).unwrap();
    config
        .memory_mut()
        .get_or_insert_with(Default::default)
        .auto_dream_enabled = false;
    let state = web::Data::new(
        AppState::new_with_provider_and_memory_store(
            root.into(),
            config,
            provider.clone(),
            bamboo_memory::memory_store::MemoryStore::new(root.join("jiandu")),
        )
        .await
        .unwrap(),
    );
    state.provider_registry.replace_with(ProviderRegistry::new(
        HashMap::from([("openai".into(), provider.clone() as Arc<dyn LLMProvider>)]),
        "openai".into(),
    ));
    (state, provider)
}
fn owner() -> Principal {
    Principal::User {
        user_id: "host-owner".into(),
    }
}
struct At(u64);
impl Clock for At {
    fn now_ms(&self) -> u64 {
        self.0
    }
}
fn now() -> u64 {
    chrono::Utc::now().timestamp_millis().try_into().unwrap()
}
fn git(repo: &Path, args: &[&str]) -> String {
    let output = std::process::Command::new("git")
        .args([
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "commit.gpgsign=false",
            "-C",
        ])
        .arg(repo)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().into()
}
fn workspace(root: &Path) -> ExecutionWorkspace {
    let repo = root.join("repo");
    std::fs::create_dir(&repo).unwrap();
    git(&repo, &["init", "-b", "fixture"]);
    std::fs::write(repo.join("fixture"), "status native acceptance\n").unwrap();
    git(&repo, &["add", "fixture"]);
    git(
        &repo,
        &[
            "-c",
            "user.name=Fixture",
            "-c",
            "user.email=fixture@example.invalid",
            "commit",
            "-m",
            "fixture",
        ],
    );
    let base = git(&repo, &["rev-parse", "HEAD"]);
    let tree = root.join("worktree");
    git(
        &repo,
        &[
            "worktree",
            "add",
            "-b",
            "work",
            tree.to_str().unwrap(),
            &base,
        ],
    );
    let tree = tree.canonicalize().unwrap().to_string_lossy().into_owned();
    ExecutionWorkspace {
        repo: repo.canonicalize().unwrap().to_string_lossy().into_owned(),
        base_commit: base,
        branch: "work".into(),
        worktree: tree.clone(),
        write_roots: vec![tree.clone()],
        claims: BTreeSet::from([format!("worktree:{tree}")]),
    }
}
fn files(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn walk(root: &Path, output: &mut BTreeMap<PathBuf, Vec<u8>>) {
        for entry in std::fs::read_dir(root).unwrap() {
            let entry = entry.unwrap();
            let kind = entry.file_type().unwrap();
            if kind.is_dir() {
                walk(&entry.path(), output)
            } else if kind.is_file() {
                output.insert(entry.path(), std::fs::read(entry.path()).unwrap());
            }
        }
    }
    let mut output = BTreeMap::new();
    for name in ["sessions", "tickets", "orchestration"] {
        let path = root.join(name);
        if path.exists() {
            walk(&path, &mut output);
        }
    }
    output
}
fn body(scope: &Scope) -> Value {
    json!({"project_id":scope.project_id,"work_id":scope.work_id,"assignment_id":scope.assignment_id})
}
macro_rules! query {
    ($app:expr,$body:expr,$status:expr) => {{
        let response = actix_web::test::call_service(
            &$app,
            actix_web::test::TestRequest::post()
                .uri("/api/v1/tickets/patrol/status")
                .peer_addr("127.0.0.1:43210".parse().unwrap())
                .set_json($body)
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), $status);
        actix_web::test::read_body_json::<Value, _>(response).await
    }};
}
async fn seed(root: &Path) {
    let execution = workspace(root);
    let (state, provider) = host(root, 2).await;
    let project = state.project_store.create("Status", None).unwrap();
    let project = state
        .project_store
        .bind_workspace(
            &project.id,
            project.revision,
            WorkspaceBinding {
                path: execution.repo.clone(),
                label: None,
                git_common_dir: None,
            },
        )
        .unwrap();
    let (service, authority) = state.tickets.authority(owner()).await.unwrap();
    let contract = Contract {
        title: "Bounded status result".into(),
        objective: "Produce a bounded result".into(),
        constraints: vec![],
        acceptance: vec!["Owner review".into()],
        user_acceptance_required: true,
        allowed_tools: BTreeSet::from(["Task".into()]),
    };
    let create = service
        .prepare_command(
            &authority,
            "create",
            vec![
                Operation::Create {
                    temp_id: "work".into(),
                    kind: TicketKind::Work,
                    parent: None,
                    contract: contract.clone(),
                    depends_on: BTreeSet::new(),
                },
                Operation::AssociateProject {
                    ticket_id: "work".into(),
                    project_id: project.id.clone(),
                },
            ],
        )
        .unwrap();
    let work = state.tickets.update(owner(), &create).await.unwrap().ids["work"].clone();
    let ready = service
        .prepare_command(
            &authority,
            "ready",
            vec![
                Operation::SetProjectContext {
                    work_id: work.clone(),
                    context: ProjectContextSnapshot {
                        project_id: project.id.clone(),
                        project_revision: project.revision,
                        resource_revision: project.resource_revision,
                        workspace_binding: project.workspace_bindings[0].clone(),
                        contract_revision: 1,
                        contract,
                        sources: BTreeMap::new(),
                    },
                },
                Operation::Ready {
                    work_id: work.clone(),
                },
            ],
        )
        .unwrap();
    state.tickets.update(owner(), &ready).await.unwrap();
    let start = service
        .prepare_command(
            &authority,
            "start",
            vec![Operation::Start {
                work_id: work.clone(),
                temp_id: "attempt".into(),
                workspace: Some(execution),
            }],
        )
        .unwrap();
    assert_eq!(
        state.tickets.dispatch(owner(), &start).await.unwrap()["errors"],
        json!([])
    );
    let snapshot = tokio::time::timeout(WAIT, async {
        loop {
            let snapshot = service.published().unwrap().1;
            if snapshot.tickets[&work].state == WorkState::Submitted {
                let submitted = &snapshot.submissions
                    [snapshot.tickets[&work].current_submission.as_ref().unwrap()];
                let receipt = &submitted.runtime;
                let done = state
                    .agent_runners
                    .read()
                    .await
                    .get(&receipt.session_id)
                    .is_some_and(|runner| {
                        runner.run_id == receipt.run_id
                            && matches!(
                                runner.status,
                                bamboo_engine::execution::AgentStatus::Completed
                            )
                            && runner.completed_at.is_some()
                    });
                if snapshot.assignments[&submitted.assignment_id].process_stopped
                    && done
                    && state
                        .session_activation_router
                        .current_run_id(&receipt.session_id)
                        .await
                        .is_none()
                {
                    break snapshot;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("real native submitted/stopped/finalized run");
    let submitted =
        &snapshot.submissions[snapshot.tickets[&work].current_submission.as_ref().unwrap()];
    let scope = state
        .tickets
        .patrol_scope(project.id.as_str(), &work, &submitted.assignment_id)
        .await
        .unwrap();
    exercise(&state, &provider, root, &scope, true).await;
    std::fs::write(
        root.join("status-seed.json"),
        serde_json::to_vec(&scope).unwrap(),
    )
    .unwrap();
    state.shutdown().await;
}
async fn exercise(
    state: &web::Data<AppState>,
    provider: &Provider,
    root: &Path,
    scope: &Scope,
    seed: bool,
) {
    let app = actix_web::test::init_service(
        actix_web::App::new()
            .app_data(state.clone())
            .configure(crate::routes::configure_routes),
    )
    .await;
    let request = body(scope);
    let calls = provider.calls.load(Ordering::SeqCst);
    let service = state.tickets.service().unwrap();
    let before = files(root);
    let ticket = service.published().unwrap();
    if seed {
        assert_eq!(
            query!(app, &request, StatusCode::OK)["state"],
            "not_checked"
        );
        assert_eq!(
            files(root),
            before,
            "missing status read must not initialize journal/lock files"
        );
        let mut journal = state.tickets.open_patrol_checkpoints(scope).await.unwrap();
        let busy = files(root);
        assert_eq!(query!(app, &request, StatusCode::OK)["state"], "busy");
        assert_eq!(files(root), busy);
        let mut observation = state.tickets.patrol_observation(scope).await.unwrap();
        ProgramPatrol::new(Policy::default())
            .unwrap()
            .tick(scope, &At(now()), &mut journal, &mut observation)
            .unwrap();
        drop(journal);
    }
    let baseline = files(root);
    let first = query!(app, &request, StatusCode::OK);
    assert!(matches!(
        first["state"].as_str(),
        Some("available" | "stale")
    ));
    assert_eq!(first["scope"], serde_json::to_value(scope).unwrap());
    assert_eq!(first["checkpoint"]["version"], 1);
    assert_eq!(
        first["checkpoint"]["incident"]["reason"],
        "evidence_unknown"
    );
    assert!(first.get("healthy").is_none());
    for _ in 0..3 {
        assert_eq!(query!(app, &request, StatusCode::OK), first);
    }
    for field in ["project_id", "work_id", "assignment_id"] {
        let mut wrong = request.clone();
        wrong[field] = json!("foreign");
        let _ = query!(app, &wrong, StatusCode::FORBIDDEN);
    }
    assert!(matches!(
        state
            .tickets
            .patrol_status(
                Principal::User {
                    user_id: "foreign-owner".into()
                },
                &scope.project_id,
                &scope.work_id,
                &scope.assignment_id
            )
            .await,
        Err(Error::ScopeDenied(_))
    ));
    assert_eq!(
        files(root),
        baseline,
        "reads cannot touch Ticket/Actor/Inbox/journal bytes"
    );
    assert_eq!(service.published().unwrap().0, ticket.0);
    assert_eq!(provider.calls.load(Ordering::SeqCst), calls);
}

#[test]
fn owner_http_status_repeats_and_cold_reopens_without_writes_or_model_calls() {
    if let Some(root) = std::env::var_os(SEED) {
        actix_web::rt::System::new().block_on(seed(Path::new(&root)));
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let root = root.path().canonicalize().unwrap();
    let home = root.join("home");
    let temp = root.join("tmp");
    std::fs::create_dir(&home).unwrap();
    std::fs::create_dir(&temp).unwrap();
    let case = format!(
        "{}::owner_http_status_repeats_and_cold_reopens_without_writes_or_model_calls",
        module_path!().strip_prefix("bamboo_server::").unwrap()
    );
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", &case, "--nocapture", "--test-threads=1"])
        .env_clear()
        .env("PATH", "/usr/bin:/bin:/usr/sbin:/sbin")
        .env("HOME", &home)
        .env("XDG_CONFIG_HOME", home.join("config"))
        .env("XDG_DATA_HOME", home.join("data"))
        .env("XDG_CACHE_HOME", home.join("cache"))
        .env("TMPDIR", &temp)
        .env("TMP", &temp)
        .env("TEMP", &temp)
        .env("RUST_MIN_STACK", "8388608")
        .env(SEED, &root)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let scope: Scope =
        serde_json::from_slice(&std::fs::read(root.join("status-seed.json")).unwrap()).unwrap();
    actix_web::rt::System::new().block_on(async {
        let (state, provider) = host(&root, 0).await;
        exercise(&state, &provider, &root, &scope, false).await;
        state.shutdown().await;
    });
}

#[actix_web::test]
async fn status_rejects_worker_remote_unauthenticated_and_malformed_selectors() {
    let root = tempfile::tempdir().unwrap();
    let (state, provider) = host(root.path(), 0).await;
    let app = actix_web::test::init_service(
        actix_web::App::new()
            .app_data(state.clone())
            .configure(crate::routes::configure_routes),
    )
    .await;
    let body = json!({"project_id":"project","work_id":"work","assignment_id":"assignment"});
    let before = files(root.path());
    let request = actix_web::test::TestRequest::post()
        .uri("/api/v1/tickets/patrol/status")
        .peer_addr("203.0.113.1:43210".parse().unwrap())
        .set_json(&body)
        .to_request();
    assert_eq!(
        actix_web::test::call_service(&app, request).await.status(),
        StatusCode::FORBIDDEN
    );
    let request = actix_web::test::TestRequest::post()
        .uri("/api/v1/tickets/patrol/status")
        .peer_addr("127.0.0.1:43210".parse().unwrap())
        .set_json(&body)
        .to_request();
    request
        .extensions_mut()
        .insert(crate::codex_run_tokens::CodexRunAuthContext {
            session_id: "worker".into(),
        });
    assert_eq!(
        actix_web::test::call_service(&app, request).await.status(),
        StatusCode::FORBIDDEN
    );
    for field in ["generation", "authority_epoch", "path", "principal"] {
        let mut malformed = body.clone();
        malformed[field] = json!(1);
        let response = actix_web::test::call_service(
            &app,
            actix_web::test::TestRequest::post()
                .uri("/api/v1/tickets/patrol/status")
                .peer_addr("127.0.0.1:43210".parse().unwrap())
                .set_json(malformed)
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
    assert_eq!(files(root.path()), before);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
    state.shutdown().await;
}
