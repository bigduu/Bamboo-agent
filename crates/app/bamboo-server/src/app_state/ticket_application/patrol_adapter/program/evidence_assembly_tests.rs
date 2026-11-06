//! Real default Host/Store/Inbox path, with a private failure at actual ACK save.
use super::*;
use async_trait::async_trait;
use bamboo_agent_core::tools::{FunctionCall, ToolCall, ToolSchema};
use bamboo_domain::{Message, ProjectId, SessionMessageEnvelope, WorkspaceBinding};
use bamboo_llm::{Config, LLMChunk, LLMError, LLMProvider, LLMStream, ProviderRegistry};
use bamboo_orchestration::patrol::Status;
use bamboo_tickets::{Contract, Operation, ProjectContextSnapshot, WorkState};
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

const SEED: &str = "BAMBOO_2486_EVIDENCE_SEED";
const WAIT: Duration = Duration::from_secs(20);

pub(super) fn before_ack(store: &mut PatrolCheckpointStore) {
    if std::env::var_os(SEED).is_some() {
        store.reject_next_ack();
    }
}

struct Provider {
    calls: AtomicUsize,
    limit: usize,
    gate: Option<Arc<tokio::sync::Semaphore>>,
    write_path: Option<PathBuf>,
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
        assert_eq!(model, "patrol-evidence-local");
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        assert!(call < self.limit, "observer cannot activate provider");
        if self.write_path.is_none() || call >= 2 {
            if let Some(gate) = &self.gate {
                gate.acquire()
                    .await
                    .expect("controlled provider gate")
                    .forget();
            }
        }
        assert!(tools.iter().any(|tool| tool.function.name == "Task"));
        let chunk = if call == 0 {
            LLMChunk::ToolCalls(vec![ToolCall { id:"evidence-task".into(),tool_type:"function".into(),function:FunctionCall {
            name:"Task".into(),arguments:json!({"tasks":[{"id":"bounded-result","content":"Produce one bounded result","status":"completed"}]}).to_string()
        }}])
        } else if let Some(write_path) = self.write_path.as_ref().filter(|_| call == 1) {
            assert!(tools.iter().any(|tool| tool.function.name == "Write"));
            LLMChunk::ToolCalls(vec![ToolCall {
                id: "evidence-write".into(),
                tool_type: "function".into(),
                function: FunctionCall {
                    name: "Write".into(),
                    arguments:
                        json!({"file_path": write_path, "content": "verified native result"})
                            .to_string(),
                },
            }])
        } else {
            LLMChunk::Token("The bounded native result awaits owner review.".into())
        };
        Ok(Box::pin(futures::stream::iter([
            Ok(chunk),
            Ok(LLMChunk::Done),
        ])))
    }
}

async fn host(root: &Path, limit: usize) -> (crate::AppState, Arc<Provider>) {
    host_with_gate(root, limit, None, None).await
}
async fn host_with_gate(
    root: &Path,
    limit: usize,
    gate: Option<Arc<tokio::sync::Semaphore>>,
    write_path: Option<PathBuf>,
) -> (crate::AppState, Arc<Provider>) {
    let provider = Arc::new(Provider {
        calls: AtomicUsize::new(0),
        limit,
        gate,
        write_path,
    });
    let mut config:Config=serde_json::from_value(json!({"provider":"openai","features":{"ticket_mutation":true,"ticket_dispatch":true},
        "providers":{"openai":{"api_key":"unused-controlled-fixture","model":"patrol-evidence-local"}},"run_budget":{"max_rounds":4},"subagents":{"max_concurrent":1}})).unwrap();
    config
        .memory_mut()
        .get_or_insert_with(Default::default)
        .auto_dream_enabled = false;
    let state = crate::AppState::new_with_provider_and_memory_store(
        root.into(),
        config,
        provider.clone(),
        bamboo_memory::memory_store::MemoryStore::new(root.join("jiandu")),
    )
    .await
    .unwrap();
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
fn now() -> u64 {
    chrono::Utc::now().timestamp_millis().try_into().unwrap()
}
fn git(path: &Path, args: &[&str]) -> String {
    let output = std::process::Command::new("git")
        .args([
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "commit.gpgsign=false",
            "-C",
        ])
        .arg(path)
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
fn workspace(root: &Path) -> bamboo_tickets::ExecutionWorkspace {
    let repo = root.join("repo");
    std::fs::create_dir(&repo).unwrap();
    git(&repo, &["init", "-b", "fixture"]);
    std::fs::write(repo.join("fixture"), "native evidence\n").unwrap();
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
    let worktree = root.join("worktree");
    git(
        &repo,
        &[
            "worktree",
            "add",
            "-b",
            "work",
            worktree.to_str().unwrap(),
            &base,
        ],
    );
    let worktree = worktree
        .canonicalize()
        .unwrap()
        .to_string_lossy()
        .into_owned();
    bamboo_tickets::ExecutionWorkspace {
        repo: repo.canonicalize().unwrap().to_string_lossy().into_owned(),
        base_commit: base,
        branch: "work".into(),
        worktree: worktree.clone(),
        write_roots: vec![worktree.clone()],
        claims: BTreeSet::from([format!("worktree:{worktree}")]),
    }
}
fn policy(
    snapshot: &bamboo_tickets::Snapshot,
    scope: &Scope,
) -> bamboo_orchestration::ReadyWorkPolicy {
    let work = &snapshot.tickets[&scope.work_id];
    let assignment = &snapshot.assignments[&scope.assignment_id];
    bamboo_orchestration::ReadyWorkPolicy {
        project_id: snapshot.project.links[&work.id].clone(),
        capacity: 1,
        max_rounds: 4,
        lifetime: Duration::from_secs(120),
        grants: vec![bamboo_orchestration::ReadyWorkGrant {
            work_id: work.id.clone(),
            contract_revision: work.contract_revision,
            context_hash: snapshot.project.work_contexts[&work.id].clone(),
            workspace: assignment.workspace.clone().unwrap(),
        }],
    }
}
fn journal_file(root: &Path, snapshot: &bamboo_tickets::Snapshot, scope: &Scope) -> PathBuf {
    root.join("orchestration/patrol")
        .join(bamboo_tickets::content_hash(
            &bamboo_tickets::canonical_bytes(&snapshot.binding).unwrap(),
        ))
        .join(format!(
            "{}.json",
            bamboo_orchestration::patrol::checkpoint::scope_key(scope).unwrap()
        ))
}
fn notice(root: &Path, target: &str) -> SessionMessageEnvelope {
    let values=std::fs::read_dir(root.join("sessions").join(target).join("inbox/new")).unwrap().filter_map(|entry| {
        let transport:bamboo_subagent::InboxMessage=serde_json::from_slice(&std::fs::read(entry.ok()?.path()).ok()?).ok()?;
        let envelope:SessionMessageEnvelope=serde_json::from_value(transport.body).ok()?;
        matches!(&envelope.source,bamboo_domain::SessionMessageSource::Runtime{subsystem} if subsystem=="ticket_patrol").then_some(envelope)
    }).collect::<Vec<_>>();
    assert_eq!(values.len(), 1);
    values.into_iter().next().unwrap()
}
fn actor_bytes(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn walk(path: &Path, output: &mut BTreeMap<PathBuf, Vec<u8>>) {
        for entry in std::fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            let kind = entry.file_type().unwrap();
            if entry.file_name() == ".git" {
                continue;
            }
            if kind.is_dir() {
                walk(&entry.path(), output)
            } else if entry.file_name() == "actor-authority.json"
                || entry.file_name() == "actor-authority.initialized.json"
            {
                output.insert(entry.path(), std::fs::read(entry.path()).unwrap());
            }
        }
    }
    let mut output = BTreeMap::new();
    walk(root, &mut output);
    output
}
async fn ask(state: &crate::AppState, work: &str, id: &str) {
    let (service, authority) = state.tickets.authority(owner()).await.unwrap();
    let command = service
        .prepare_command(
            &authority,
            id,
            vec![Operation::Ask {
                work_id: work.into(),
                temp_id: "question".into(),
                prompt: format!("Owner review checkpoint {id}?"),
                action: None,
            }],
        )
        .unwrap();
    state.tickets.update(owner(), &command).await.unwrap();
}

fn assert_source_negatives(
    service: &bamboo_tickets::TicketService,
    authority: &bamboo_tickets::Authority,
    source: &NoticeSource,
    escalation: &bamboo_orchestration::patrol::Escalation,
) {
    use super::super::notice_evidence;
    let good = notice_evidence::read(service, authority, source, escalation).unwrap();
    assert_eq!(good["historical"], true);
    assert_eq!(good["assignment"]["process_stopped"], true);
    assert_eq!(good["references"].as_array().unwrap().len(), 2);
    assert_eq!(
        good["workspace_declaration"]["sha256"]
            .as_str()
            .unwrap()
            .len(),
        64
    );
    assert!(serde_json::to_vec(&good).unwrap().len() <= notice_evidence::MAX_EVIDENCE_BYTES);
    for commit in ["f".repeat(64), service.published().unwrap().0] {
        let mut wrong = source.clone();
        wrong.commit = commit;
        assert!(
            notice_evidence::read(service, authority, &wrong, escalation).is_err(),
            "same counters cannot authorize another commit"
        );
    }
    for kind in ["generation", "project", "revision"] {
        let mut wrong = source.clone();
        let mut event = escalation.clone();
        match kind {
            "generation" => wrong.operation.incident.scope.generation += 1,
            "project" => wrong.operation.incident.scope.project_id = "foreign-project".into(),
            _ => wrong.execution_facts_revision += 1,
        }
        event.operation = wrong.operation.clone();
        event.source_revision = wrong.execution_facts_revision;
        assert!(notice_evidence::read(service, authority, &wrong, &event).is_err());
    }
    let current = service.published().unwrap().1;
    let snapshot = service.fixed_snapshot(&source.commit).unwrap();
    let scope = &source.operation.incident.scope;
    let mut bad = snapshot.clone();
    let assignment = bad.assignments.get_mut(&scope.assignment_id).unwrap();
    assignment.runtime.as_mut().unwrap().run_id = uuid::Uuid::new_v4().to_string();
    bad.intents
        .get_mut(&assignment.dispatch_key)
        .unwrap()
        .receipt = assignment.runtime.clone();
    assert!(
        notice_evidence::project(source, escalation, &current, &bad).is_err(),
        "foreign receipt cannot become historical evidence"
    );
    let mut oversized = snapshot;
    oversized
        .assignments
        .get_mut(&scope.assignment_id)
        .unwrap()
        .plan
        .steps
        .push(bamboo_tickets::LocalStep {
            id: "large".into(),
            parent: None,
            title: "x".repeat(65_536),
            completed: false,
            status: None,
        });
    assert_eq!(
        notice_evidence::project(source, escalation, &current, &oversized),
        Err(PortError::Unavailable)
    );
}

async fn start_work(
    state: &crate::AppState,
    execution: bamboo_tickets::ExecutionWorkspace,
    allowed_tools: BTreeSet<String>,
) -> (String, ProjectId) {
    let project = state.project_store.create("Evidence", None).unwrap();
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
        title: "Bounded result".into(),
        objective: "Produce one bounded result".into(),
        constraints: vec![],
        acceptance: vec!["Owner review".into()],
        user_acceptance_required: true,
        allowed_tools,
    };
    let create = service
        .prepare_command(
            &authority,
            "create",
            vec![
                Operation::Create {
                    temp_id: "work".into(),
                    kind: bamboo_tickets::TicketKind::Work,
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
    (work, project.id)
}

#[tokio::test]
async fn ready_semantic_patrol_settles_native_workspace_progress() {
    Box::pin(ready_semantic_fixture(SemanticCase::Known)).await;
}

#[tokio::test]
async fn ready_semantic_patrol_keeps_unknown_charge_if_project_changes_before_send() {
    Box::pin(ready_semantic_fixture(SemanticCase::ProjectChanged)).await;
}

#[tokio::test]
async fn ready_semantic_patrol_retries_late_usage_against_the_original_receipt() {
    Box::pin(ready_semantic_fixture(SemanticCase::LateUsage)).await;
}

#[tokio::test]
async fn ready_semantic_patrol_uses_the_configured_provider_without_a_local_profile() {
    Box::pin(ready_semantic_fixture(SemanticCase::Registered)).await;
}

#[tokio::test]
async fn ready_semantic_patrol_keeps_the_reserved_charge_when_the_provider_omits_usage() {
    Box::pin(ready_semantic_fixture(SemanticCase::RegisteredMissingUsage)).await;
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SemanticCase {
    Known,
    ProjectChanged,
    LateUsage,
    Registered,
    RegisteredMissingUsage,
}

async fn ready_semantic_fixture(case: SemanticCase) {
    use bamboo_orchestration::{
        patrol::ObservationPort, ReadyDispatch, ReadyWorkHost, ReadyWorkScheduler,
    };
    use bamboo_tickets::{
        Authority, Command, DispatchSpec, ExecutionTokenUsage, FaultPoint, TicketService,
        WorkTokenLimits,
    };
    use wiremock::{
        matchers::{method, path},
        Mock, MockServer, Request, ResponseTemplate,
    };

    struct Host<'a>(&'a TicketApplication);
    #[async_trait]
    impl ReadyWorkHost for Host<'_> {
        async fn authority(&self) -> Result<(Arc<TicketService>, Authority)> {
            ReadyWorkHost::authority(self.0).await
        }
        async fn check_budget(&self, max_rounds: u32) -> Result<()> {
            self.0.check_budget(max_rounds).await
        }
        async fn reconcile_program_patrol(
            &self,
            selection: &ProgramSelection<'_>,
            fence: &ReadyAdmissionFence,
        ) -> Result<()> {
            Box::pin(self.0.reconcile_ready_patrol_with_policy(
                selection,
                fence,
                Policy {
                    // Exercise the production pass without a ten-minute wait
                    // after the actual native Write and its success receipt.
                    progress_timeout_ms: 1,
                    check_interval_ms: 1,
                    ..Policy::default()
                },
            ))
            .await
        }
        async fn preflight(&self, command: &Command) -> Result<()> {
            self.0.preflight(command).await
        }
        async fn ensure(
            &self,
            key: &str,
            spec: &DispatchSpec,
            rounds: u32,
        ) -> Result<ReadyDispatch> {
            ReadyWorkHost::ensure(self.0, key, spec, rounds).await
        }
        async fn dispatch(&self, command: &Command, rounds: u32) -> Result<ReadyDispatch> {
            ReadyWorkHost::dispatch(self.0, command, rounds).await
        }
    }

    let root = tempfile::tempdir().unwrap();
    let data = root.path().canonicalize().unwrap();
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let result_path = data.join("worktree/native-result.txt");
    let (state, agent) = Box::pin(host_with_gate(
        &data,
        3,
        Some(gate.clone()),
        Some(result_path.clone()),
    ))
    .await;
    let (work, project) = Box::pin(start_work(
        &state,
        workspace(&data),
        BTreeSet::from(["Task".into(), "Write".into()]),
    ))
    .await;
    tokio::time::timeout(WAIT, async {
        while agent.calls.load(Ordering::SeqCst) != 3 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        std::fs::read_to_string(&result_path).unwrap(),
        "verified native result",
    );

    let (service, owner) = state.tickets.authority(owner()).await.unwrap();
    service
        .configure_work_token_budget(
            &owner,
            0,
            WorkTokenLimits {
                work_tokens: BTreeMap::from([(work.clone(), 8192)]),
                utc_day_tokens: 8192,
                total_tokens: 8192,
            },
        )
        .unwrap();
    let (_, runtime) = state.tickets.authority(Principal::Runtime).await.unwrap();
    let snapshot = service.published().unwrap().1;
    let assignment = snapshot.tickets[&work].active_assignment.as_ref().unwrap();
    let scope = state
        .tickets
        .patrol_scope(project.as_str(), &work, assignment)
        .await
        .unwrap();
    let observed = state
        .tickets
        .patrol_observation(&scope)
        .await
        .unwrap()
        .observation(&scope, 0)
        .unwrap();
    assert!(observed.progress.is_some());
    assert!(!observed.repeated_errors);

    let provider = MockServer::start().await;
    let registered = matches!(
        case,
        SemanticCase::Registered | SemanticCase::RegisteredMissingUsage | SemanticCase::LateUsage
    );
    for (route, body) in [
        (
            "/v1/models",
            json!({"data":[{"id":"gpt-oss-20b","owned_by":"llamacpp","meta":{"n_vocab":201088,"n_embd":2880,"n_params":20914790400u64}}]}),
        ),
        (
            "/props",
            json!({"model_alias":"gpt-oss-20b","default_generation_settings":{"n_ctx":8192}}),
        ),
    ].into_iter().filter(|_| !registered) {
        Mock::given(method("GET"))
            .and(path(route))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(&provider)
            .await;
    }
    let projects = state.project_store.clone();
    let change_project = case == SemanticCase::ProjectChanged;
    if !registered {
        Mock::given(method("POST"))
            .and(path("/tokenize"))
            .respond_with(move |_: &Request| {
                if change_project {
                    let revision = projects.get_read_only(&project).unwrap().revision;
                    projects
                        .update(&project, revision, |manifest| {
                            manifest.name = "Changed while preparing the model call".into();
                            Ok(())
                        })
                        .unwrap();
                }
                ResponseTemplate::new(200)
                    .set_body_json(json!({"tokens":[200006,173781,200005,17196,200008,200002]}))
            })
            .expect(1)
            .mount(&provider)
            .await;
    }

    let generated = bamboo_llm::protocol::llama_cpp::GptOssPrompt::encode(
        "",
        r#"{"advice":"observe","evidence":[],"uncertain":true,"note":"Controlled result."}"#,
    )
    .unwrap();
    let ids = generated.tokens();
    let body = ids.iter().rposition(|&id| id == 200008).unwrap() + 1;
    let end = ids.iter().rposition(|&id| id == 200007).unwrap();
    let mut response = vec![200005, 17196, 200008];
    response.extend_from_slice(&ids[body..end]);
    response.push(200002);
    let response_tokens = response.len() as u64;
    let ledger_at_send = service.clone();
    let runtime_at_send = runtime.clone();
    let failed_publications = Arc::new(AtomicUsize::new(0));
    let failure_probe = failed_publications.clone();
    Mock::given(method("POST"))
        .and(path(if registered {
            "/chat/completions"
        } else {
            "/completion"
        }))
        .respond_with(move |req: &Request| {
            let ledger = ledger_at_send
                .work_token_budget(&runtime_at_send)
                .unwrap()
                .unwrap();
            assert_eq!(ledger.total_charged, 4224);
            assert_eq!(ledger.reservations.len(), 1);
            let original = ledger.reservations.values().next().unwrap();
            assert!(original.usage.is_none());
            if case == SemanticCase::LateUsage {
                // Record the conservative outcome while the reply is in flight.
                ledger_at_send
                    .settle_work_tokens(
                        &runtime_at_send,
                        &original.request.operation_id,
                        &original.request.request_hash,
                        &original.receipt,
                        ExecutionTokenUsage::Unknown,
                    )
                    .unwrap();
                let failure_probe = failure_probe.clone();
                ledger_at_send.set_publication_fault(Some(Arc::new(move |point| {
                    if point == FaultPoint::BeforeHeadRename {
                        failure_probe.fetch_add(1, Ordering::SeqCst);
                        return Err(std::io::Error::other("controlled settlement write failure"));
                    }
                    Ok(())
                })));
            }
            let body: Value = serde_json::from_slice(&req.body).unwrap();
            if registered {
                assert_eq!(body["model"], "registered-patrol-model");
                assert_eq!(body["max_tokens"], 128);
                assert_eq!(body["messages"][0]["role"], "system");
                assert_eq!(body["messages"][1]["role"], "user");
                assert!(
                    body["tools"].is_null() || body["tools"].as_array().is_some_and(Vec::is_empty)
                );
                if case == SemanticCase::RegisteredMissingUsage {
                    return ResponseTemplate::new(200).set_body_raw(
                        "data: {\"choices\":[{\"delta\":{\"content\":\"{}\"}}]}\n\ndata: [DONE]\n\n",
                        "text/event-stream",
                    );
                }
                return ResponseTemplate::new(200).set_body_raw(
                    concat!(
                        "data: {\"choices\":[{\"delta\":{\"content\":\"{}\"}}]}\n\n",
                        "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":40,",
                        "\"completion_tokens\":4,\"total_tokens\":44}}\n\n",
                        "data: [DONE]\n\n",
                    ),
                    "text/event-stream",
                );
            }
            let prompt = body["prompt"].as_array().unwrap();
            assert!(prompt.iter().all(Value::is_u64));
            ResponseTemplate::new(200).set_body_json(json!({
                "model":"gpt-oss-20b","stop":true,"stop_type":"eos","truncated":false,
                "tokens_evaluated":prompt.len(),"tokens_predicted":response.len(),"tokens":response
            }))
        })
        .expect(u64::from(!change_project))
        .mount(&provider)
        .await;
    let mut selection = json!({
        "provider": "patrol-local", "model": "gpt-oss-20b", "protocol": "llama_cpp_raw_v1",
        "endpoint": provider.address().to_string(), "max_input_tokens":4096,"max_output_tokens":128,
        "max_evidence_bytes":16384,"max_evidence_refs":8
    });
    if registered {
        selection["provider"] = json!("patrol-existing");
        selection["model"] = json!("registered-patrol-model");
        selection["protocol"] = json!("registered_provider_v1");
        selection.as_object_mut().unwrap().remove("endpoint");
        let config = {
            let mut config = state.config.write().await;
            config.provider_instances.insert(
                "patrol-existing".into(),
                serde_json::from_value(json!({
                    "provider_type":"openai", "enabled":true, "api_key":"controlled-registry-key",
                    "base_url":provider.uri(), "model":"registered-patrol-model",
                    "runtime_models":["registered-patrol-model"]
                }))
                .unwrap(),
            );
            config.extra.insert(
                "model_limits".into(),
                json!([{
                    "model_pattern":"registered-patrol-model", "max_context_tokens":4096,
                    "max_output_tokens":128, "safety_margin":0
                }]),
            );
            config.clone()
        };
        state.provider_registry.replace_with(
            ProviderRegistry::from_config(&config, data.clone())
                .await
                .unwrap(),
        );
    }
    state.config.write().await.patrol_model = Some(serde_json::from_value(selection).unwrap());
    let host = Host(&state.tickets);
    let scheduler = ReadyWorkScheduler::default();
    let granted = policy(&snapshot, &scope);
    let mut stale = granted.clone();
    stale.grants[0].context_hash = "0".repeat(64);
    scheduler.install(&host, stale).await.unwrap();
    Box::pin(scheduler.reconcile(&host)).await.unwrap();
    assert!(provider.received_requests().await.unwrap().is_empty());
    assert!(service
        .work_token_budget(&runtime)
        .unwrap()
        .unwrap()
        .reservations
        .is_empty());
    scheduler.revoke().await;

    scheduler.install(&host, granted).await.unwrap();
    let mut ledger = tokio::time::timeout(WAIT, async {
        loop {
            Box::pin(scheduler.reconcile(&host)).await.unwrap();
            let ledger = service.work_token_budget(&runtime).unwrap().unwrap();
            let ready = match case {
                SemanticCase::LateUsage => state.tickets.patrol_calls.needs_accounting(),
                _ => ledger
                    .reservations
                    .values()
                    .any(|record| record.usage.is_some()),
            };
            if ready {
                break ledger;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    if case == SemanticCase::LateUsage {
        assert_eq!(ledger.total_charged, 4224);
        assert_eq!(ledger.reservations.len(), 1);
        assert!(failed_publications.load(Ordering::SeqCst) > 0);
        let original = ledger.reservations.values().next().unwrap().clone();
        assert_eq!(original.usage, Some(ExecutionTokenUsage::Unknown));
        // The next Ready pass can finish accounting after model selection is
        // removed, using the exact original receipt and no provider request.
        state.config.write().await.patrol_model = None;
        service.set_publication_fault(None);
        Box::pin(scheduler.reconcile(&host)).await.unwrap();
        assert!(!state.tickets.patrol_calls.needs_accounting());
        ledger = service.work_token_budget(&runtime).unwrap().unwrap();
        assert_eq!(ledger.reservations.len(), 1);
        let reconciled = ledger.reservations.values().next().unwrap();
        assert_eq!(reconciled.receipt, original.receipt);
        assert_eq!(reconciled.request, original.request);
    }
    scheduler.revoke().await;
    assert_eq!(ledger.reservations.len(), 1);
    let receipt = ledger.reservations.values().next().unwrap();
    assert_eq!(receipt.request.work_id, work);
    let requests = provider.received_requests().await.unwrap();
    if change_project {
        assert_eq!(requests.len(), 3);
        assert_eq!(ledger.total_charged, 4224);
        assert_eq!(receipt.usage, Some(ExecutionTokenUsage::Unknown));
    } else if case == SemanticCase::RegisteredMissingUsage {
        assert_eq!(requests.len(), 1);
        assert_eq!(ledger.total_charged, 4224);
        assert_eq!(receipt.usage, Some(ExecutionTokenUsage::Unknown));
    } else if registered {
        assert_eq!(requests.len(), 1);
        assert_eq!(ledger.total_charged, 44);
        assert_eq!(
            receipt.usage,
            Some(ExecutionTokenUsage::Known {
                input_tokens: 40,
                output_tokens: 4,
            })
        );
    } else {
        assert_eq!(requests.len(), 4);
        let body: Value = serde_json::from_slice(&requests[3].body).unwrap();
        let prompt_tokens = body["prompt"].as_array().unwrap().len() as u64;
        assert_eq!(ledger.total_charged, prompt_tokens + response_tokens);
        assert_eq!(
            receipt.usage,
            Some(ExecutionTokenUsage::Known {
                input_tokens: prompt_tokens,
                output_tokens: response_tokens,
            })
        );
    }
    assert_eq!(agent.calls.load(Ordering::SeqCst), 3);
    gate.add_permits(1);
    state.shutdown().await;
}

async fn seed(root: &Path) {
    let execution = workspace(root);
    let (state, provider) = host(root, 2).await;
    let (work, project) = start_work(&state, execution, BTreeSet::from(["Task".into()])).await;
    let (service, _) = state.tickets.authority(owner()).await.unwrap();
    let snapshot = tokio::time::timeout(WAIT, async {
        loop {
            let snapshot = service.published().unwrap().1;
            if snapshot.tickets[&work].state == WorkState::Submitted {
                let submission = &snapshot.submissions
                    [snapshot.tickets[&work].current_submission.as_ref().unwrap()];
                let receipt = &submission.runtime;
                let runner = state
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
                    });
                if snapshot.assignments[&submission.assignment_id].process_stopped
                    && runner
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
    .expect("real stopped native submission");
    let assignment = snapshot.tickets[&work]
        .current_submission
        .as_ref()
        .map(|id| snapshot.submissions[id].assignment_id.clone())
        .unwrap();
    let scope = state
        .tickets
        .patrol_scope(project.as_str(), &work, &assignment)
        .await
        .unwrap();
    let mut journal = state.tickets.open_patrol_checkpoints(&scope).await.unwrap();
    let mut observation = state.tickets.patrol_observation(&scope).await.unwrap();
    assert_eq!(
        ProgramPatrol::new(Policy::default())
            .unwrap()
            .tick(&scope, &SystemClock(now()), &mut journal, &mut observation)
            .unwrap()
            .status,
        Status::Incident
    );
    drop(journal);
    ask(&state, &work, "first-source").await;
    let source_head = service.published().unwrap().0;
    let actors = actor_bytes(root);
    assert!(!actors.is_empty());
    let initial = state
        .session_inbox
        .inspect(&snapshot.binding.supervisor_session_id)
        .await
        .unwrap();
    state
        .tickets
        .install_ready_work_policy(policy(&snapshot, &scope))
        .await
        .unwrap();
    tokio::time::timeout(WAIT, async {
        loop {
            if let Ok(mut journal) = state.tickets.open_patrol_checkpoints(&scope).await {
                let stored = journal.load(&scope).unwrap().unwrap();
                if stored
                    .checkpoint
                    .incident
                    .as_ref()
                    .unwrap()
                    .escalation
                    .as_ref()
                    .is_some_and(|e| e.deliveries == 1 && !e.queued)
                    && state
                        .session_inbox
                        .inspect(&snapshot.binding.supervisor_session_id)
                        .await
                        .unwrap()
                        .pending
                        == initial.pending + 1
                {
                    break;
                }
            }
            state.tickets.wake_ready_work();
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("real stage succeeded and actual ACK publication failed");
    let first = notice(root, &snapshot.binding.supervisor_session_id);
    let file = journal_file(root, &snapshot, &scope);
    let record: Value = serde_json::from_slice(&std::fs::read(&file).unwrap()).unwrap();
    assert_eq!(record["schema"], 2);
    assert_eq!(record["source"]["commit"], source_head);
    let before = state
        .session_inbox
        .inspect(&snapshot.binding.supervisor_session_id)
        .await
        .unwrap();
    assert_eq!(before.activation_generation, initial.activation_generation);
    assert_eq!(before.interrupt_generation, initial.interrupt_generation);
    ask(&state, &work, "later-source").await;
    assert_ne!(service.published().unwrap().0, source_head);
    let changed = service.published().unwrap().0;
    let source: NoticeSource = serde_json::from_value(record["source"].clone()).unwrap();
    let event: bamboo_orchestration::patrol::Escalation =
        serde_json::from_value(record["record"]["checkpoint"]["incident"]["escalation"].clone())
            .unwrap();
    let (_, runtime) = state.tickets.authority(Principal::Runtime).await.unwrap();
    assert_source_negatives(&service, &runtime, &source, &event);
    // The production helper runs again after a canonical source change. Its
    // exact existing receipt proves it did not re-render current facts.
    let _ = state.tickets.reconcile_ready_work().await;
    assert_eq!(notice(root, &snapshot.binding.supervisor_session_id), first);
    assert_eq!(
        state
            .session_inbox
            .inspect(&snapshot.binding.supervisor_session_id)
            .await
            .unwrap(),
        before
    );
    assert_eq!(service.published().unwrap().0, changed);
    assert_eq!(actor_bytes(root), actors);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
    std::fs::write(root.join("2486-seed.json"),serde_json::to_vec(&json!({"scope":scope,"notice":first,"actors":actors.keys().map(|p|p.to_string_lossy()).collect::<Vec<_>>()})).unwrap()).unwrap();
    state.tickets.revoke_ready_work_policy().await;
    state.shutdown().await;
}

async fn reopened(root: &Path) {
    let seed: Value =
        serde_json::from_slice(&std::fs::read(root.join("2486-seed.json")).unwrap()).unwrap();
    let scope: Scope = serde_json::from_value(seed["scope"].clone()).unwrap();
    let original: SessionMessageEnvelope = serde_json::from_value(seed["notice"].clone()).unwrap();
    let (state, provider) = host(root, 0).await;
    let (service, _) = state.tickets.authority(owner()).await.unwrap();
    let (head, snapshot) = service.published().unwrap();
    let before = state
        .session_inbox
        .inspect(&snapshot.binding.supervisor_session_id)
        .await
        .unwrap();
    let actors = actor_bytes(root);
    let file = journal_file(root, &snapshot, &scope);
    let original_record: Value = serde_json::from_slice(&std::fs::read(&file).unwrap()).unwrap();
    state
        .tickets
        .install_ready_work_policy(policy(&snapshot, &scope))
        .await
        .unwrap();
    tokio::time::timeout(WAIT, async {
        loop {
            if let Ok(mut journal) = state.tickets.open_patrol_checkpoints(&scope).await {
                if journal
                    .load(&scope)
                    .unwrap()
                    .unwrap()
                    .checkpoint
                    .incident
                    .unwrap()
                    .escalation
                    .unwrap()
                    .queued
                {
                    break;
                }
            }
            state.tickets.wake_ready_work();
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("cold Host reconciles the original actual receipt");
    assert_eq!(
        notice(root, &snapshot.binding.supervisor_session_id),
        original
    );
    assert!(state
        .session_inbox
        .existing_staged_delivery(&original)
        .await
        .unwrap()
        .is_some());
    assert_eq!(
        state
            .session_inbox
            .inspect(&snapshot.binding.supervisor_session_id)
            .await
            .unwrap(),
        before
    );
    let record: Value = serde_json::from_slice(&std::fs::read(file).unwrap()).unwrap();
    assert_eq!(record["source"], original_record["source"]);
    assert_eq!(
        record["record"]["checkpoint"]["incident"]["escalation"]["deliveries"],
        1
    );
    assert_eq!(service.published().unwrap().0, head);
    assert_eq!(actor_bytes(root), actors);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
    state.tickets.revoke_ready_work_policy().await;
    state.shutdown().await;
}

#[test]
fn native_staged_evidence_survives_failed_ack_source_change_and_cold_reopen() {
    if let Some(root) = std::env::var_os(SEED) {
        actix_web::rt::System::new().block_on(seed(Path::new(&root)));
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let root = root.path().canonicalize().unwrap();
    let case = format!(
        "{}::native_staged_evidence_survives_failed_ack_source_change_and_cold_reopen",
        module_path!().strip_prefix("bamboo_server::").unwrap()
    );
    let fixture_home = root.join("seed-home");
    let fixture_temp = root.join("seed-tmp");
    for directory in [
        fixture_home.clone(),
        fixture_home.join("config"),
        fixture_home.join("data"),
        fixture_home.join("cache"),
        fixture_temp.clone(),
    ] {
        std::fs::create_dir_all(directory).unwrap();
    }
    // This native child receives no parent credentials, workflow channels or
    // ambient user configuration. Git uses only the standard Unix tool path.
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", &case, "--nocapture", "--test-threads=1"])
        .env_clear()
        .env("PATH", "/usr/bin:/bin:/usr/sbin:/sbin")
        .env("HOME", &fixture_home)
        .env("XDG_CONFIG_HOME", fixture_home.join("config"))
        .env("XDG_DATA_HOME", fixture_home.join("data"))
        .env("XDG_CACHE_HOME", fixture_home.join("cache"))
        .env("TMPDIR", &fixture_temp)
        .env("TMP", &fixture_temp)
        .env("TEMP", &fixture_temp)
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
    actix_web::rt::System::new().block_on(reopened(&root));
}

#[tokio::test]
async fn live_start_disappearing_at_final_host_read_is_a_conflict_without_ticket_writes() {
    use crate::app_state::ticket_application::patrol_adapter::runtime_observation;
    use bamboo_orchestration::patrol::ObservationPort;
    let root = tempfile::tempdir().unwrap();
    let root = root.path().canonicalize().unwrap();
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let execution = workspace(&root);
    let (state, provider) = host_with_gate(&root, 1, Some(gate.clone()), None).await;
    let (work, project) = start_work(&state, execution, BTreeSet::from(["Task".into()])).await;
    tokio::time::timeout(WAIT, async {
        while provider.calls.load(Ordering::SeqCst) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let (service, authority) = state.tickets.authority(Principal::Runtime).await.unwrap();
    let before = service.published().unwrap();
    let assignment = before.1.tickets[&work].active_assignment.as_ref().unwrap();
    let scope = state
        .tickets
        .patrol_scope(project.as_str(), &work, assignment)
        .await
        .unwrap();
    let receipt = before.1.assignments[assignment].runtime.as_ref().unwrap();
    let child = state
        .storage
        .load_runtime_control_plane(&receipt.session_id)
        .await
        .unwrap()
        .unwrap();
    assert!(state
        .spawn_scheduler
        .native_task_start(&child.id, &receipt.run_id, child.created_at)
        .is_some());
    let selection = ProgramSelection {
        project_id: &project,
        work_ids: std::slice::from_ref(&work),
    };
    let prepared =
        runtime_observation::prepare_selection(&state.tickets, &service, &authority, &selection)
            .await
            .unwrap();
    let runtime = &prepared[&scope];
    assert!(runtime
        .read_observation(&service, &authority, &scope, &Policy::default(), None)
        .unwrap()
        .observation(&scope, 0)
        .unwrap()
        .started_at_ms
        .is_some());
    let wait_for_renewal = |after: Option<u64>| {
        let state = &state;
        let child = &child;
        async move {
            tokio::time::timeout(WAIT, async {
                loop {
                    let at = state
                        .spawn_scheduler
                        .native_task_liveness(&child.id, &receipt.run_id, child.created_at)
                        .and_then(|live| live.heartbeat_at_ms());
                    if let Some(at) = at.filter(|at| after.is_none_or(|old| *at > old)) {
                        break at;
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            })
            .await
            .expect("the held native Task renews its real Actor activation")
        }
    };
    let first_heartbeat = wait_for_renewal(None).await;
    let advanced = runtime_observation::prepare_with_probe(
        &state.tickets,
        &service,
        &authority,
        &scope,
        async {
            // Advance the actual renewal after initial Host collection, while
            // the same execution instance and the same Ticket commit remain.
            wait_for_renewal(Some(first_heartbeat)).await;
        },
    )
    .await
    .unwrap()
    .unwrap();
    for prepared in [runtime, &advanced] {
        let observed = prepared
            .read_observation(&service, &authority, &scope, &Policy::default(), None)
            .unwrap()
            .observation(&scope, 0)
            .unwrap();
        assert!(observed
            .heartbeat_at_ms
            .is_some_and(|at| at > first_heartbeat));
        assert!(observed.progress.is_none());
    }

    // The Ready pass has one fixed clock. A later real heartbeat must not
    // break an otherwise valid ProgramPatrol tick at the earlier cutoff.
    let policy = Policy::default();
    let program = ProgramPatrol::new(policy.clone()).unwrap();
    let clock = SystemClock(first_heartbeat);
    let mut journal = PatrolCheckpointStore::open(&root.join("heartbeat-clock-control")).unwrap();
    let mut unbounded = runtime
        .read_observation(&service, &authority, &scope, &policy, None)
        .unwrap();
    assert!(matches!(
        program.tick(&scope, &clock, &mut journal, &mut unbounded),
        Err(bamboo_orchestration::patrol::Error::InvalidObservation)
    ));
    let mut bounded = runtime
        .read_observation(&service, &authority, &scope, &policy, Some(first_heartbeat))
        .unwrap();
    assert_eq!(
        program
            .tick(&scope, &clock, &mut journal, &mut bounded)
            .unwrap()
            .status,
        Status::Incident
    );
    assert_eq!(
        journal
            .load(&scope)
            .unwrap()
            .unwrap()
            .checkpoint
            .incident
            .unwrap()
            .reason,
        bamboo_orchestration::patrol::Reason::EvidenceUnknown
    );

    let (entered, enter) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    let read = runtime_observation::prepare_with_probe(
        &state.tickets,
        &service,
        &authority,
        &scope,
        async {
            entered.send(()).unwrap();
            released.await.unwrap();
        },
    );
    tokio::pin!(read);
    tokio::select! { biased;
        _ = &mut read => panic!("the reader must remain at its final proof barrier"),
        result = enter => result.unwrap(),
    }
    let shutdown = state.external_child_runner.shutdown();
    tokio::pin!(shutdown);
    // Current-thread runtime: close the real native source but do not yield to
    // its terminal writer. The same read/Ticket commit must detect that loss.
    assert!(futures::poll!(&mut shutdown).is_pending());
    assert!(state
        .spawn_scheduler
        .native_task_start(&child.id, &receipt.run_id, child.created_at)
        .is_none());
    assert_eq!(service.published().unwrap().0, before.0);
    // This is the same reader the program factory uses after collecting its
    // batch. The previously valid sample cannot survive a lost live source.
    let observed = runtime
        .read_observation(&service, &authority, &scope, &Policy::default(), None)
        .unwrap()
        .observation(&scope, 0)
        .unwrap();
    assert_eq!(observed.started_at_ms, None);
    assert_eq!(observed.heartbeat_at_ms, None);
    assert!(observed.progress.is_none());
    release.send(()).unwrap();
    assert!(matches!(
        futures::poll!(&mut read),
        std::task::Poll::Ready(Err(Error::RevisionConflict))
    ));
    assert_eq!(service.published().unwrap().0, before.0);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    eprintln!(
        "2525 heartbeat advanced and vanished at shutdown: unchanged commit={}, native provider calls=1",
        before.0
    );
    gate.add_permits(1);
    tokio::time::timeout(WAIT, &mut shutdown)
        .await
        .unwrap()
        .unwrap();
    state.shutdown().await;
}

#[tokio::test]
async fn explicit_work_review_uses_fast_and_reconciles_the_original_work_receipt() {
    Box::pin(work_review_fixture(false)).await;
}

#[tokio::test]
async fn reasoning_only_work_review_reports_inconclusive_and_preserves_known_usage() {
    Box::pin(work_review_fixture(true)).await;
}

async fn work_review_fixture(reasoning_only: bool) {
    use bamboo_agent_core::tools::{ExecutingSupervisorObservation, ToolOutcome};
    use bamboo_agent_core::ToolExecutionContext;
    use bamboo_domain::{SessionCallerReference, TokenUsage, DEFAULT_SUPERVISOR_SESSION_ID};
    use bamboo_orchestration::work_review::{settle_review, WorkReviewEvidence, WorkReviewUsage};
    use bamboo_tickets::{
        Authority, ExecutionBudgetPurpose, ExecutionTokenUsage, FaultPoint, WorkTokenLimits,
    };
    use wiremock::{
        matchers::{method, path},
        Mock, MockServer, Request, ResponseTemplate,
    };

    let root = tempfile::tempdir().unwrap();
    let data = root.path().canonicalize().unwrap();
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let (state, agent) = Box::pin(host_with_gate(
        &data,
        3,
        Some(gate.clone()),
        Some(data.join("worktree/native-result.txt")),
    ))
    .await;
    let (work, project) = Box::pin(start_work(
        &state,
        workspace(&data),
        BTreeSet::from(["Task".into(), "Write".into()]),
    ))
    .await;
    tokio::time::timeout(WAIT, async {
        while agent.calls.load(Ordering::SeqCst) != 3 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();

    let (service, owner) = state.tickets.authority(owner()).await.unwrap();
    let (_, runtime) = state.tickets.authority(Principal::Runtime).await.unwrap();
    let snapshot = service.published().unwrap().1;
    let assignment_id = snapshot.tickets[&work].active_assignment.as_ref().unwrap();
    let assignment = &snapshot.assignments[assignment_id];
    let receipt = assignment.runtime.as_ref().unwrap();
    let worker = Authority::from_verified_host(
        snapshot.binding.clone(),
        Principal::Worker {
            assignment_id: assignment.id.clone(),
            generation: assignment.generation,
            run_id: receipt.run_id.clone(),
            session_id: receipt.session_id.clone(),
        },
    );
    let packet = service
        .child_context_packet(&worker, &assignment.id, 65536)
        .unwrap();
    let ask = service
        .prepare_command(
            &worker,
            "review-fixture-yield",
            vec![
                Operation::Ask {
                    work_id: work.clone(),
                    temp_id: "question".into(),
                    prompt: "Which input should the Worker use?".into(),
                    action: None,
                },
                Operation::YieldForInput {
                    assignment_id: assignment.id.clone(),
                    request_id: "question".into(),
                    packet_hash: bamboo_tickets::content_hash(
                        &bamboo_tickets::canonical_bytes(&packet).unwrap(),
                    ),
                },
            ],
        )
        .unwrap();
    let question = service.execute(&worker, &ask).unwrap().ids["question"].clone();

    let supervisor = state
        .storage
        .load_root_authority(DEFAULT_SUPERVISOR_SESSION_ID)
        .await
        .unwrap()
        .unwrap();
    let review = async |operation_id: uuid::Uuid| {
        let call = ToolCall {
            id: operation_id.to_string(),
            tool_type: "function".into(),
            function: FunctionCall {
                name: "work_review".into(),
                arguments: json!({
                    "project_id":project, "work_id":work, "assignment_id":assignment_id,
                    "operation_id":operation_id
                })
                .to_string(),
            },
        };
        let mut ctx = ToolExecutionContext::none(&call.id);
        ctx.session_id = Some(&supervisor.id);
        ctx.root_session_id = Some(&supervisor.id);
        ctx = ctx.with_executing_supervisor(
            ExecutingSupervisorObservation::capture_from_executing_session(&supervisor),
        );
        let tools = state.tools_for(crate::tools::ToolSurface::Root);
        let result = bamboo_agent_core::tools::with_executing_session_caller(
            Some(Arc::new(SessionCallerReference::from(&supervisor))),
            tools.execute_with_context_outcome(&call, ctx),
        )
        .await
        .unwrap();
        let ToolOutcome::Completed(result) = result else {
            panic!("work_review must return the result and receipt");
        };
        assert!(result.success, "{}", result.result);
        serde_json::from_str::<Value>(&result.result).unwrap()
    };
    let unavailable = review(uuid::Uuid::new_v4()).await;
    assert_eq!(unavailable["status"], "model_unavailable");
    assert_eq!(unavailable["evidence"]["waiting"]["request_id"], question);
    assert_eq!(unavailable["evidence"]["session"]["run_id"], receipt.run_id);
    assert!(service.work_token_budget(&runtime).unwrap().is_none());

    service
        .configure_work_token_budget(
            &owner,
            0,
            WorkTokenLimits {
                work_tokens: BTreeMap::from([(work.clone(), 16384)]),
                utc_day_tokens: 16384,
                total_tokens: 16384,
            },
        )
        .unwrap();
    let provider = MockServer::start().await;
    let ledger_at_send = service.clone();
    let runtime_at_send = runtime.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let sent = calls.clone();
    let expected_question = question.clone();
    Mock::given(method("POST")).and(path("/chat/completions")).respond_with(move |request: &Request| {
        let call = sent.fetch_add(1, Ordering::SeqCst);
        let budget = ledger_at_send.work_token_budget(&runtime_at_send).unwrap().unwrap();
        let pending: Vec<_> = budget.reservations.values().filter(|record| record.usage.is_none()).collect();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].request.purpose, ExecutionBudgetPurpose::SupervisorDecision);
        let expected_charge = match call {
            0 => 4224,
            1 => 44 + 8192 + 4096,
            _ => 89 + 8192 + 4096,
        };
        assert_eq!(budget.total_charged, expected_charge);
        let body: Value = serde_json::from_slice(&request.body).unwrap();
        assert_eq!(body["model"], "review-fixture-model");
        assert_eq!(body["max_completion_tokens"], if call == 0 { 128 } else { 4096 });
        assert!(body.get("max_tokens").is_none());
        assert_eq!(body["reasoning_effort"], if call == 0 { "max" } else { "none" });
        let evidence: Value = serde_json::from_str(body["messages"][1]["content"].as_str().unwrap()).unwrap();
        assert_eq!(evidence["waiting"]["request_id"], expected_question);
        let review = if reasoning_only && call == 0 {
            json!({"choices":[{"delta":{"reasoning_content":"Private controlled reasoning must never appear in diagnostics."},"finish_reason":"length"}]})
        } else {
            json!({"choices":[{"delta":{"content":"{\"summary\":\"Worker is waiting for input.\",\"suggested_intervention\":\"Review the recorded question.\"}"}}]})
        };
        let usage = match call {
            0 => "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":40,\"completion_tokens\":4,\"total_tokens\":44}}\n\n",
            1 => "",
            _ => {
                ledger_at_send.set_publication_fault(Some(Arc::new(|point| {
                    if point == FaultPoint::BeforeHeadRename {
                        return Err(std::io::Error::other("controlled Work Review settlement failure"));
                    }
                    Ok(())
                })));
                "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":8193,\"completion_tokens\":4,\"total_tokens\":8197}}\n\n"
            }
        };
        ResponseTemplate::new(200).set_body_raw(
            format!("data: {review}\n\n{usage}data: [DONE]\n\n"), "text/event-stream",
        )
    }).expect(3).mount(&provider).await;

    let config = {
        let mut config = state.config.write().await;
        config.provider_instances.insert("work-review-existing".into(), serde_json::from_value(json!({
            "provider_type":"openai", "enabled":true, "api_key":"controlled-registry-key",
            "base_url":provider.uri(), "model":"review-fixture-model", "runtime_models":["review-fixture-model"]
        })).unwrap());
        config.extra.insert(
            "model_limits".into(),
            json!([{
                "model_pattern":"review-fixture-model", "max_context_tokens":4096,
                "max_output_tokens":128, "safety_margin":0
            }]),
        );
        config.defaults = Some(
            serde_json::from_value(json!({
                "chat":{"provider":"openai","model":"patrol-evidence-local"},
                "fast":{"provider":"work-review-existing","model":"review-fixture-model","reasoning_effort":"max"}
            }))
            .unwrap(),
        );
        config.clone()
    };
    state.provider_registry.replace_with(
        ProviderRegistry::from_config(&config, data.clone())
            .await
            .unwrap(),
    );
    let known_id = uuid::Uuid::new_v4();
    let known = review(known_id).await;
    assert_eq!(known["status"], "recorded");
    assert_eq!(
        serde_json::from_value::<ExecutionTokenUsage>(known["accounting"]["usage"].clone())
            .unwrap(),
        ExecutionTokenUsage::Known {
            input_tokens: 40,
            output_tokens: 4
        }
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        service
            .work_token_budget(&runtime)
            .unwrap()
            .unwrap()
            .total_charged,
        44
    );
    if reasoning_only {
        assert!(known["advisory"].is_null());
        assert_eq!(known["inconclusive"]["reason"], "reasoning_only");
        assert_eq!(known["inconclusive"]["finish_reason"], "output_limit");
        assert!(!known.to_string().contains("Private controlled reasoning"));
    } else {
        assert_eq!(known["advisory"]["summary"], "Worker is waiting for input.");
    }
    let replay = review(known_id).await;
    assert_eq!(replay["status"], "existing_receipt");
    assert_eq!(
        replay["accounting"]["receipt"],
        known["accounting"]["receipt"]
    );

    let mut successor =
        WorkReviewEvidence::capture(&service, &runtime, project.as_str(), &work, assignment_id)
            .unwrap();
    successor.scope.generation += 1;
    assert!(matches!(
        successor.recorded_reservation(&service, &runtime, &known_id.to_string()),
        Err(bamboo_tickets::Error::IdempotencyConflict)
    ));

    // A configured Fast model without a global Model Limits row uses the
    // per-call Work Review ceilings and the same registered provider.
    {
        let mut config = state.config.write().await;
        config.extra.remove("model_limits");
        config
            .defaults
            .as_mut()
            .unwrap()
            .fast
            .as_mut()
            .unwrap()
            .reasoning_effort = Some(bamboo_domain::ReasoningEffort::Disabled);
    }
    let after_policy_change = review(known_id).await;
    assert_eq!(after_policy_change["status"], "existing_receipt");
    assert_eq!(after_policy_change["accounting"], known["accounting"]);
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "historical receipt precedes new planning"
    );
    let unknown = review(uuid::Uuid::new_v4()).await;
    let original: bamboo_tickets::WorkTokenReservation =
        serde_json::from_value(unknown["accounting"].clone()).unwrap();
    assert_eq!(original.usage, Some(ExecutionTokenUsage::Unknown));
    assert!(unknown["advisory"].is_null());
    let (reconciled, _) = settle_review(
        &service,
        &runtime,
        &original,
        WorkReviewUsage::Observed(TokenUsage {
            prompt_tokens: 41,
            completion_tokens: 4,
            total_tokens: 45,
        }),
        None,
    )
    .unwrap();
    assert_eq!(reconciled.receipt, original.receipt);
    let current = service.published().unwrap().1;
    assert_eq!(current.execution_budget.as_ref().unwrap().total_charged, 89);
    assert_eq!(
        current
            .execution_budget
            .as_ref()
            .unwrap()
            .reservations
            .len(),
        2
    );
    assert!(current.tickets[&work].accepted_submission.is_none());
    let over_cap_id = uuid::Uuid::new_v4();
    let pending = review(over_cap_id).await;
    assert_eq!(pending["status"], "accounting_pending");
    service.set_publication_fault(None);
    let recovered = review(over_cap_id).await;
    assert_eq!(recovered["status"], "existing_receipt");
    assert_eq!(
        recovered["accounting"]["receipt"],
        pending["accounting"]["receipt"]
    );
    assert_eq!(
        recovered["accounting"]["over_cap_usage"]["input_tokens"],
        8193
    );
    let denied = review(uuid::Uuid::new_v4()).await;
    assert_eq!(denied["status"], "budget_blocked");
    assert_eq!(denied["reason"], "accounting");

    let revision = state
        .project_store
        .get_read_only(&project)
        .unwrap()
        .revision;
    state.project_store.archive(&project, revision).unwrap();
    let stale = review(uuid::Uuid::new_v4()).await;
    assert_eq!(stale["status"], "source_unavailable");
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    gate.add_permits(1);
    state.shutdown().await;
}
