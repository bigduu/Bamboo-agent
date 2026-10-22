//! Ordinary Native Workspace handoff contract, UNRUN.
//! The completion tap runs after the real ordinary Tool future returns, under
//! per_call's real Root writer. Keep it OUTSIDE any future Source read gate:
//! the Host setter must never wait for a gate held by this completed call.
#![cfg(unix)]

use super::*;
use bamboo_agent_core::tools::Tool;
use bamboo_domain::ActorDirectoryPort;
use bamboo_server_tools::project_tools::WorkspaceHandoffHook;
use std::sync::OnceLock;

const SKILL: &str = "aaa-handoff-proof";
const READ: &str = "handoff-completed-read";
const CWD_B: &str = "handoff-current-cwd";
const CWD_A: &str = "handoff-returned-cwd";
const OLD_B: &str = "handoff-retained-reader-after-b";
const OLD_A: &str = "handoff-retained-reader-after-a";

#[derive(Clone, Copy, PartialEq, Eq)]
enum Case {
    Switch,
    RoundTrip,
    FailedSet,
    TitleAfterSet,
}

#[derive(Default, Debug)]
struct Observed {
    calls: BTreeMap<String, usize>,
    commits: Vec<(bool, Option<String>, u64)>,
    cwd: BTreeMap<String, PathBuf>,
    rounds: Vec<(Option<String>, Option<PathBuf>)>,
    responses: BTreeMap<String, (bool, String)>,
    title_patches: Vec<(u64, u64)>,
}
struct Probe {
    case: Case,
    state: Mutex<Option<web::Data<AppState>>>,
    a: PathBuf,
    b: PathBuf,
    retained: Mutex<Option<Arc<SkillsReadTool>>>,
    observed: Mutex<Observed>,
    title_patch: Mutex<Option<tokio::sync::mpsc::UnboundedSender<TitlePatch>>>,
}
type TitlePatch = (String, tokio::sync::oneshot::Sender<StatusCode>);
fn probes() -> &'static Mutex<BTreeMap<String, Arc<Probe>>> {
    static PROBES: OnceLock<Mutex<BTreeMap<String, Arc<Probe>>>> = OnceLock::new();
    PROBES.get_or_init(Mutex::default)
}
fn probe_for(id: &str) -> Option<Arc<Probe>> {
    probes().lock().unwrap().get(id).cloned()
}

/// Retain the actual installed Reader, not merely an old cursor or executor.
pub(super) fn observe_reader(run: &Arc<NativeRun>, reader: &Arc<SkillsReadTool>) {
    if let Some(probe) = probe_for(&run.policy.session_id) {
        let mut retained = probe.retained.lock().unwrap();
        if retained.is_none() {
            *retained = Some(reader.clone());
        }
    }
}
/// Only invoked inside the real private dispatch and current Q scopes.
pub(super) async fn retained_reader_call(
    tools: &Arc<dyn ToolExecutor>,
    call: &ToolCall,
    name: &str,
    ctx: ToolExecutionContext<'_>,
) -> Option<Result<ToolOutcome, ToolError>> {
    if name != "skills_read" || !matches!(call.id.as_str(), OLD_B | OLD_A) {
        return None;
    }
    let probe = probe_for(ctx.session_id?)?;
    let reader = probe
        .retained
        .lock()
        .unwrap()
        .clone()
        .expect("installed Reader tap");
    assert!(
        NATIVE_PHASE
            .try_with(
                |phase| phase.purpose == Purpose::Dispatch && phase.call_id.as_ref() == call.id
            )
            .unwrap_or(false),
        "retained Reader requires genuine dispatch, not a forged public context"
    );
    match tools.check_permissions_for_exact(call, name, &ctx).await {
        Ok(Some(outcome)) => return Some(Ok(outcome)),
        Err(error) => return Some(Err(error)),
        Ok(None) => {}
    }
    let args = serde_json::from_str(&call.function.arguments).unwrap();
    Some(reader.invoke(args, ctx.to_tool_ctx()).await)
}

/// Actual Read/exec future has returned; per_call's Root writer remains scoped.
/// This controlled Host operation never advertises Workspace to the provider.
pub(super) async fn after_completed_call(
    executor: &NativeExecutor,
    call: &ToolCall,
    name: &str,
    ctx: ToolExecutionContext<'_>,
    success: bool,
) {
    let Some(probe) = probe_for(&executor.run.policy.session_id) else {
        return;
    };
    if !matches!(call.id.as_str(), READ | CWD_B | CWD_A) {
        return;
    }
    assert!(
        success,
        "actual ordinary {name} must finish before Host Workspace"
    );
    {
        let mut facts = probe.observed.lock().unwrap();
        *facts.calls.entry(call.id.clone()).or_default() += 1;
        facts.cwd.insert(
            call.id.clone(),
            bamboo_agent_core::workspace_state::workspace_or_process_cwd(ctx.session_id),
        );
    }
    let target = match call.id.as_str() {
        READ if probe.case == Case::FailedSet => probe.b.join("not-created"),
        READ => probe.b.clone(),
        CWD_B if probe.case == Case::RoundTrip => probe.a.clone(),
        _ => return,
    };
    assert!(
        matches!(
            bamboo_agent_core::tools::context::root_actor_tool_writer(),
            Some(Some(_))
        ),
        "Host setter must use the actual completed dispatch's Root writer"
    );
    assert!(
        !ctx.bypass_permissions && !ctx.auto_approve_permissions,
        "the ordinary fixture must retain its real permission gate"
    );
    let state = probe.state.lock().unwrap().as_ref().unwrap().clone();
    let host = ToolCall {
        id: format!("host-workspace-after-{}", call.id),
        tool_type: "function".into(),
        function: FunctionCall {
            name: "Workspace".into(),
            arguments: json!({"path":target}).to_string(),
        },
    };
    // This is a distinct Host call. Reusing the completed Read/exec's parsed
    // arguments would silently turn Workspace(set) into Workspace(get).
    let context = ToolExecutionContext {
        tool_call_id: &host.id,
        pre_parsed_args: None,
        ..ctx
    };
    let result = state
        .tools_for(ToolSurface::Root)
        .execute_exact_with_context_outcome(&host, "Workspace", context)
        .await
        .expect("real authorized Host Workspace setter")
        .into_tool_result();
    assert_eq!(
        result.success,
        probe.case != Case::FailedSet,
        "Host Workspace fixture setup: {}",
        result.result
    );
    let durable = state
        .storage
        .load_session(&executor.run.policy.session_id)
        .await
        .unwrap()
        .unwrap();
    let expected = if probe.case == Case::FailedSet {
        &probe.a
    } else {
        &target
    };
    assert_eq!(
        workspace_identity(&durable).unwrap().as_ref(),
        Some(expected),
        "the real Workspace setter, not a getter or a raw metadata substitute"
    );
    assert_eq!(
        durable.project_id_meta(),
        executor.run.policy.project,
        "ordinary Workspace handoff must preserve the assigned Project"
    );
    probe.observed.lock().unwrap().commits.push((
        result.success,
        durable.workspace_path_meta(),
        durable.metadata_version,
    ));
    if probe.case == Case::TitleAfterSet && call.id == READ {
        let requests = probe.title_patch.lock().unwrap().as_ref().unwrap().clone();
        let (reply, result) = tokio::sync::oneshot::channel();
        assert!(requests.send((durable.id.clone(), reply)).is_ok());
        assert_eq!(
            result.await.unwrap(),
            StatusCode::OK,
            "actual title PATCH after the Host commit"
        );
        let titled = state
            .storage
            .load_session(&durable.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(titled.metadata_version, durable.metadata_version + 1);
        assert_eq!(titled.title_version, durable.title_version + 1);
        assert_eq!(titled.workspace_path_meta(), durable.workspace_path_meta());
        assert_eq!(titled.project_id_meta(), durable.project_id_meta());
        probe
            .observed
            .lock()
            .unwrap()
            .title_patches
            .push((titled.metadata_version, titled.title_version));
    }
}

#[derive(Clone, Copy)]
enum Step {
    List(&'static str, &'static str),
    Fresh(&'static str, &'static str),
    Read,
    Retained(&'static str, bool),
    StaleCursor(&'static str),
    Cwd(&'static str, &'static str),
}
impl Step {
    fn id(self) -> &'static str {
        match self {
            Self::List(id, _)
            | Self::Fresh(id, _)
            | Self::Retained(id, _)
            | Self::Cwd(id, _)
            | Self::StaleCursor(id) => id,
            Self::Read => READ,
        }
    }
    fn tool(self) -> &'static str {
        match self {
            Self::List(..) => "skills_list",
            Self::Fresh(..) | Self::Retained(..) | Self::StaleCursor(..) => "skills_read",
            Self::Read => "Read",
            Self::Cwd(..) => "exec_command",
        }
    }
}
#[derive(Default)]
struct ProviderState {
    next: usize,
    pending: Option<(Step, String)>,
    packages: BTreeMap<&'static str, String>,
    cursor: Option<String>,
    list_cursor: Option<String>,
    attempts: usize,
    finished: usize,
}
struct HandoffProvider {
    probe: Arc<Probe>,
    plan: Vec<Step>,
    current: Mutex<ProviderState>,
}
impl HandoffProvider {
    fn new(probe: Arc<Probe>) -> Self {
        let scope = if probe.case == Case::FailedSet {
            "A"
        } else {
            "B"
        };
        let mut plan = vec![
            Step::List("handoff-list-a", "A"),
            Step::Fresh("handoff-warm-a", "A"),
            Step::Read,
            Step::Retained(OLD_B, probe.case == Case::FailedSet),
            Step::List("handoff-list-current", scope),
            Step::Fresh("handoff-read-current", scope),
            Step::Cwd(CWD_B, scope),
        ];
        if probe.case == Case::RoundTrip {
            plan.extend([
                Step::Retained(OLD_A, false),
                Step::List("handoff-list-returned-a", "A"),
                Step::Fresh("handoff-read-returned-a", "A"),
                Step::StaleCursor("handoff-old-cursor-on-fresh-returned-reader"),
                Step::Cwd(CWD_A, "A"),
            ]);
        }
        Self {
            probe,
            plan,
            current: Mutex::default(),
        }
    }
}
#[async_trait]
impl LLMProvider for HandoffProvider {
    async fn chat_stream(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        _: Option<u32>,
        _: &str,
    ) -> Result<LLMStream, LLMError> {
        assert!(
            !tools.iter().any(|tool| tool.function.name == "Workspace"),
            "Workspace remains Host-only"
        );
        let state = self.probe.state.lock().unwrap().as_ref().unwrap().clone();
        let id = probes()
            .lock()
            .unwrap()
            .iter()
            .find(|(_, value)| Arc::ptr_eq(value, &self.probe))
            .map(|(id, _)| id.clone())
            .unwrap();
        let durable = state.storage.load_session(&id).await.unwrap().unwrap();
        let expected = self
            .probe
            .observed
            .lock()
            .unwrap()
            .commits
            .last()
            .and_then(|(_, workspace, _)| workspace.clone())
            .map(PathBuf::from)
            .unwrap_or_else(|| self.probe.a.clone());
        assert_eq!(
            workspace_identity(&durable).unwrap(),
            Some(expected.clone()),
            "the next round must retain the actual authorized Workspace commit"
        );
        assert_eq!(
            bamboo_agent_core::workspace_state::peek_workspace(&id),
            Some(expected),
            "round prompt refresh must not republish the stale workspace"
        );
        self.probe.observed.lock().unwrap().rounds.push((
            durable.workspace_path_meta(),
            bamboo_agent_core::workspace_state::peek_workspace(&id),
        ));
        let mut current = self.current.lock().unwrap();
        if let Some((step, id)) = current.pending.take() {
            let result = messages
                .iter()
                .rev()
                .find(|message| message.tool_call_id.as_deref() == Some(id.as_str()))
                .expect("actual prior Tool result reaches the next provider round");
            let success = result.tool_success == Some(true);
            self.probe
                .observed
                .lock()
                .unwrap()
                .responses
                .insert(step.id().into(), (success, result.content.clone()));
            match step {
                Step::Retained(_, expected) => {
                    assert_eq!(
                        success, expected,
                        "retained actual Reader: {}",
                        result.content
                    );
                    assert!(
                        !result.content.contains("outside its actual dispatch"),
                        "forged dispatch must not make the old Reader negative pass"
                    );
                    if !expected {
                        assert!(
                            result.content.contains("Source")
                                || result.content.contains("source")
                                || result.content.contains("no longer active"),
                            "the original Source must be revoked: {}",
                            result.content
                        );
                    }
                }
                Step::StaleCursor(..) => {
                    assert!(
                        !success,
                        "an earlier Source cursor must fail on the fresh returned Reader"
                    );
                    assert!(
                        !result.content.contains("outside its actual dispatch"),
                        "the cursor negative must use the actual fresh Reader dispatch"
                    );
                }
                Step::List(_, scope) => {
                    assert!(success, "fresh {scope} catalog: {}", result.content);
                    let page: Value = serde_json::from_str(&result.content).unwrap();
                    if let Some(entry) = page["skills"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .find(|entry| entry["name"] == SKILL)
                    {
                        current
                            .packages
                            .insert(scope, entry["package"].as_str().unwrap().into());
                        current.list_cursor = None;
                    } else {
                        current.list_cursor = Some(
                            page["next_cursor"]
                                .as_str()
                                .expect("the actual scoped Skill must be advertised")
                                .into(),
                        );
                        current.next -= 1;
                    }
                }
                Step::Fresh(id, scope) => {
                    assert!(success, "fresh {scope} Reader: {}", result.content);
                    let page: Value = serde_json::from_str(&result.content).unwrap();
                    assert!(page["contents"]
                        .as_str()
                        .unwrap()
                        .contains(&format!("SCOPED_{scope}")));
                    if id == "handoff-warm-a" {
                        current.cursor = Some(
                            page["next_cursor"]
                                .as_str()
                                .expect("the actual initial Reader must have a warm page")
                                .into(),
                        );
                    }
                }
                Step::Read => assert!(
                    success && result.content.contains("COMPLETED_ONCE_IN_A"),
                    "actual successful ordinary Read: {}",
                    result.content
                ),
                Step::Cwd(_, scope) => {
                    assert!(success, "actual cwd observation: {}", result.content);
                    let page: Value = serde_json::from_str(&result.content).unwrap();
                    assert_eq!(page["exit_code"], 0);
                    let cwd = PathBuf::from(page["output"].as_str().unwrap().trim())
                        .canonicalize()
                        .unwrap();
                    assert_eq!(
                        &cwd,
                        if scope == "A" {
                            &self.probe.a
                        } else {
                            &self.probe.b
                        },
                        "actual ordinary command cwd across the next round"
                    );
                }
            }
        }
        if current.next == self.plan.len() {
            current.finished += 1;
            return Ok(Box::pin(futures::stream::iter([
                Ok(LLMChunk::Token(
                    "Controlled workspace handoff complete".into(),
                )),
                Ok(LLMChunk::Done),
            ])));
        }
        let step = self.plan[current.next];
        current.next += 1;
        current.attempts += 1;
        let id = match step {
            Step::List(..) => format!("{}-{}", step.id(), current.attempts),
            _ => step.id().into(),
        };
        let args = match step {
            Step::List(..) => json!({"limit":20,"cursor":current.list_cursor}),
            Step::Fresh(_, scope) => json!({"package":current.packages[scope]}),
            Step::Read => json!({"file_path":self.probe.a.join("completed-once.txt")}),
            Step::Retained(..) | Step::StaleCursor(..) => {
                json!({"package":current.packages["A"],"cursor":current.cursor})
            }
            Step::Cwd(..) => json!({"cmd":"pwd","yield_time_ms":10000,"max_output_tokens":1024}),
        };
        assert!(
            tools.iter().any(|tool| tool.function.name == step.tool()),
            "current ordinary callable set must include {}",
            step.tool()
        );
        current.pending = Some((step, id.clone()));
        Ok(Box::pin(futures::stream::iter([
            Ok(LLMChunk::ToolCalls(vec![ToolCall {
                id,
                tool_type: "function".into(),
                function: FunctionCall {
                    name: step.tool().into(),
                    arguments: args.to_string(),
                },
            }])),
            Ok(LLMChunk::Done),
        ])))
    }
}

struct Registration(String);
impl Drop for Registration {
    fn drop(&mut self) {
        if let Some(probe) = probes().lock().unwrap().remove(&self.0) {
            probe.retained.lock().unwrap().take();
            probe.state.lock().unwrap().take();
            probe.title_patch.lock().unwrap().take();
        }
    }
}
async fn run_case(case: Case) {
    let _proof = HTTP_PROOF.lock().await;
    let home = tempfile::tempdir().unwrap();
    let root = home.path().canonicalize().unwrap();
    let a = root.join("workspace-a");
    let b = root.join("workspace-b");
    for (workspace, scope) in [(&a, "A"), (&b, "B")] {
        std::fs::create_dir_all(workspace).unwrap();
        write_skill(&workspace.join(".bamboo"), SKILL, true);
        let path = workspace
            .join(".bamboo/skills")
            .join(SKILL)
            .join("SKILL.md");
        let body = std::fs::read_to_string(&path).unwrap();
        std::fs::write(&path, body.replace("quoted", &format!("SCOPED_{scope}"))).unwrap();
    }
    std::fs::write(a.join("completed-once.txt"), "COMPLETED_ONCE_IN_A").unwrap();
    let probe = Arc::new(Probe {
        case,
        state: Mutex::default(),
        a: a.clone(),
        b: b.clone(),
        retained: Mutex::default(),
        observed: Mutex::default(),
        title_patch: Mutex::default(),
    });
    let provider = Arc::new(HandoffProvider::new(probe.clone()));
    let state = state(&root, provider.clone()).await;
    // Actix services are local. Keep the real HTTP handler on this test's
    // local runtime; only Send channels cross the actual ToolExecutor future.
    let patch_worker = if case == Case::TitleAfterSet {
        let (requests, mut incoming) = tokio::sync::mpsc::unbounded_channel::<TitlePatch>();
        *probe.title_patch.lock().unwrap() = Some(requests);
        let state = state.clone();
        Some(actix_web::rt::spawn(async move {
            let app = test::init_service(
                App::new()
                    .app_data(state)
                    .configure(crate::configure_routes),
            )
            .await;
            while let Some((id, reply)) = incoming.recv().await {
                let request = test::TestRequest::patch()
                    .uri(&format!("/api/v1/sessions/{id}"))
                    .peer_addr("127.0.0.1:5700".parse().unwrap())
                    .set_json(json!({"title":"Controlled title after Workspace commit"}))
                    .to_request();
                let response = test::call_service(&app, request).await;
                let _ = reply.send(response.status());
            }
        }))
    } else {
        None
    };
    let project = state
        .project_store
        .create_with_project_path(
            "Native workspace handoff",
            None,
            a.to_string_lossy(),
            vec![bamboo_domain::WorkspaceBinding {
                path: b.to_string_lossy().into_owned(),
                label: None,
                git_common_dir: None,
            }],
        )
        .unwrap();
    let id = format!("native-workspace-handoff-{}", uuid::Uuid::new_v4());
    let mut session = Session::new(&id, "test-model");
    session.title_generated = true;
    session.set_project_id_meta(project.id.to_string());
    session.set_workspace_path_meta(a.to_string_lossy());
    session.metadata.insert(
        bamboo_engine::project_context::WORKSPACE_SOURCE_METADATA_KEY.into(),
        "explicit".into(),
    );
    state.storage.save_session(&session).await.unwrap();
    state
        .workspace_resolver
        .publish_resolved_workspace(&id, a.clone(), "test_initial");
    *probe.state.lock().unwrap() = Some(state.clone());
    probes().lock().unwrap().insert(id.clone(), probe.clone());
    let _registration = Registration(id.clone());
    let (status, input) = http(
        &state,
        "/api/v1/chat",
        json!({"session_id":id,"message":"Inspect the scoped workspace proof"}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{input}");
    let (status, response) = http(&state, &format!("/api/v1/execute/{id}"), json!({})).await;
    assert_eq!(status, StatusCode::ACCEPTED, "{response}");
    let terminal = done(&state, &id).await;
    let router = state.agent.activation_router().unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while router.current_run_id(&id).await.is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("actual terminal Root must release before fixture cleanup");
    let durable = state.storage.load_session(&id).await.unwrap().unwrap();
    let facts = probe.observed.lock().unwrap();
    let (plan_consumed, diagnostic) = {
        let current = provider.current.lock().unwrap();
        let cwd_a = facts.responses.get(CWD_A);
        let cwd_a_envelope =
            cwd_a.and_then(|(_, result)| serde_json::from_str::<Value>(result).ok());
        let durable_cwd_a = durable
            .messages
            .iter()
            .rev()
            .find(|message| message.tool_call_id.as_deref() == Some(CWD_A));
        let safe = bamboo_server_tools::project_tools::workspace_diagnostics::safe_identifier;
        let diagnostic = json!({
            "plan_next":current.next,"plan_len":provider.plan.len(),
            "pending_id":current.pending.as_ref().map(|(_, id)| safe(id)),
            "final_replies":current.finished,"tool_requests":current.attempts,
            "completion_reason":durable.metadata.get("runtime.completion_reason").map(|value| safe(value)),
            "pending_question_present":durable.pending_question.is_some(),
            "pending_approval_field_available":false,
            "last_tool_result_id":durable.messages.iter().rev()
                .find_map(|message| message.tool_call_id.as_deref()).map(safe),
            "cwd_a_result_present":cwd_a.is_some(),
            "cwd_a_success":cwd_a.map(|(success, _)| *success),
            "cwd_a_status":cwd_a_envelope.as_ref().and_then(|value| value["status"].as_str()).map(safe),
            "cwd_a_exit_code":cwd_a_envelope.as_ref().and_then(|value| value["exit_code"].as_i64()),
            "cwd_a_durable_requested":durable.messages.iter().any(|message|
                message.tool_calls.as_ref().is_some_and(|calls| calls.iter().any(|call| call.id == CWD_A))),
            "cwd_a_durable_result_present":durable_cwd_a.is_some(),
            "cwd_a_durable_success":durable_cwd_a.and_then(|message| message.tool_success),
            "old_a_result_present":facts.responses.contains_key(OLD_A),
            "returned_reader_result_present":facts.responses.contains_key("handoff-read-returned-a"),
            "stale_cursor_result_present":facts.responses.contains_key("handoff-old-cursor-on-fresh-returned-reader"),
        });
        (
            current.next == provider.plan.len()
                && current.pending.is_none()
                && current.finished == 1,
            diagnostic,
        )
    };
    assert!(
        matches!(terminal, AgentStatus::Completed),
        "ordinary authorized handoff must complete: {terminal:?}; {facts:?}"
    );
    assert!(
        probe.retained.lock().unwrap().is_some(),
        "actual installed Reader retained"
    );
    assert_eq!(
        facts.calls.get(READ),
        Some(&1),
        "a completed Tool must not be replayed"
    );
    assert_eq!(
        durable
            .messages
            .iter()
            .filter(|message| message.tool_call_id.as_deref() == Some(READ))
            .count(),
        1
    );
    assert_eq!(
        durable
            .messages
            .iter()
            .filter(|message| message.id == input["message_id"].as_str().unwrap())
            .count(),
        1
    );
    assert_eq!(
        facts.commits.len(),
        if case == Case::RoundTrip { 2 } else { 1 }
    );
    let expected = if matches!(case, Case::Switch | Case::TitleAfterSet) {
        &b
    } else {
        &a
    };
    assert_eq!(
        durable.workspace_path_meta().as_deref(),
        Some(expected.to_str().unwrap())
    );
    assert_eq!(
        bamboo_agent_core::workspace_state::peek_workspace(&id),
        Some(expected.clone())
    );
    assert_eq!(facts.calls.get(CWD_B), Some(&1));
    if case == Case::RoundTrip {
        assert_eq!(facts.calls.get(CWD_A), Some(&1), "{diagnostic}");
    }
    assert!(
        plan_consumed,
        "the complete finite provider plan must be consumed: {diagnostic}"
    );
    if case == Case::TitleAfterSet {
        assert_eq!(
            facts.title_patches.len(),
            1,
            "one actual title PATCH must precede the next round"
        );
        assert_eq!(durable.title, "Controlled title after Workspace commit");
        assert_eq!(
            facts.responses[OLD_B].0, false,
            "title succession cannot renew the old Reader"
        );
    }
    drop(facts);
    probe.title_patch.lock().unwrap().take();
    if let Some(worker) = patch_worker {
        worker.await.unwrap();
    }
}

// Only harmless pwd in disposable Unix homes; never replay a user's exec.
#[actix_web::test]
async fn native_workspace_handoff_rebinds_fresh_reader_and_next_round_scope() {
    run_case(Case::Switch).await;
}
#[actix_web::test]
async fn native_workspace_handoff_round_trip_does_not_revive_retained_reader() {
    run_case(Case::RoundTrip).await;
}
#[actix_web::test]
async fn native_workspace_handoff_failed_set_preserves_current_source() {
    run_case(Case::FailedSet).await;
}
#[actix_web::test]
async fn native_workspace_handoff_title_patch_suffix_keeps_scope_and_old_reader_revoked() {
    run_case(Case::TitleAfterSet).await;
}

// These two owner/gate contracts share the existing real Server fixture but
// make no HTTP/provider call and never spawn an agent execution.
async fn controlled_source_fixture() -> (
    tempfile::TempDir,
    web::Data<AppState>,
    Arc<SkillsProvider>,
    Session,
    PathBuf,
) {
    let home = tempfile::tempdir().unwrap();
    let root = home.path().canonicalize().unwrap();
    let a = root.join("workspace-a");
    let b = root.join("workspace-b");
    std::fs::create_dir(&a).unwrap();
    std::fs::create_dir(&b).unwrap();
    let provider = provider(&root, 4096, false);
    let state = state(&root, provider.clone()).await;
    let mut session = Session::new(
        format!("controlled-handoff-{}", uuid::Uuid::new_v4()),
        "test-model",
    );
    session.title_generated = true;
    session.set_workspace_path_meta(a.to_string_lossy());
    session.metadata.insert(
        bamboo_engine::project_context::WORKSPACE_SOURCE_METADATA_KEY.into(),
        "explicit".into(),
    );
    state.storage.save_session(&session).await.unwrap();
    (home, state, provider, session, b)
}

async fn reserve_owned_source(
    state: &web::Data<AppState>,
    session: &Session,
) -> SessionExecutionReservation {
    let sender = state.get_session_event_sender(&session.id).await;
    let bamboo_engine::execution::SessionExecutionReserveOutcome::Reserved(mut reservation) =
        bamboo_engine::execution::reserve_session_execution(
            &state.agent,
            &state.agent_runners,
            &state.session_event_senders,
            &session.id,
            &sender,
        )
        .await
    else {
        panic!("fresh genuine Root reservation")
    };
    reservation
        .bind_root_actor(&state.agent, session)
        .await
        .unwrap();
    assert!(reservation.root_actor_writer().is_some());
    reservation
}

#[actix_web::test]
async fn native_workspace_handoff_drop_after_revocation_keeps_source_unavailable() {
    let _proof = HTTP_PROOF.lock().await;
    for persist_before_drop in [false, true] {
        let (_home, state, provider, session, b) = controlled_source_fixture().await;
        let reservation = reserve_owned_source(&state, &session).await;
        bindings()
            .lock()
            .unwrap()
            .insert(session.id.clone(), std::sync::Weak::new());
        let binding = bind_execution(
            state.clone(),
            &session,
            &reservation,
            state.tools_for(ToolSurface::Root),
        )
        .expect("actual registered Native Source");
        let executor = bindings()
            .lock()
            .unwrap()
            .remove(&session.id)
            .unwrap()
            .upgrade()
            .unwrap();
        assert!(executor.run.source.binding().is_ok());
        let mut prepared = state.workspace_handoffs.prepare(&session.id).await.unwrap();
        let session_guard = state
            .session_repo
            .persistence()
            .acquire_lock(&session.id)
            .await;
        let before = state
            .storage
            .load_session(&session.id)
            .await
            .unwrap()
            .unwrap();
        let mut candidate = before.clone();
        candidate.set_workspace_path_meta(b.to_string_lossy());
        candidate.metadata_version = before.metadata_version.checked_add(1).unwrap();
        candidate.updated_at = chrono::Utc::now();
        prepared.before_write(&before, &candidate).await.unwrap();
        assert!(
            executor.run.source.has_no_publication(),
            "revocation precedes the real save"
        );
        if persist_before_drop {
            state.storage.save_session(&candidate).await.unwrap();
        }
        // Model cancellation before save or lost confirmation after actual
        // save. Neither path calls committed, so neither can renew a Source.
        drop(prepared);
        drop(session_guard);
        assert!(
            executor.run.source.has_no_publication(),
            "Drop must not restore a Reader or receipt"
        );
        assert!(executor.run.source.binding().is_err());
        let _released = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            executor.run.source.read(),
        )
        .await
        .expect("Source write boundary releases even on uncertain completion");
        let actual = state
            .storage
            .load_session(&session.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            actual.workspace_path_meta(),
            if persist_before_drop {
                candidate.workspace_path_meta()
            } else {
                before.workspace_path_meta()
            }
        );
        assert_eq!(
            executor.run.epoch.load(Ordering::Acquire),
            0,
            "Source changes do not advance Q"
        );
        executor.run.revoke();
        state.workspace_handoffs.unregister(&executor.run);
        reservation.abandon().await;
        drop(binding);
        assert!(provider.trace.lock().unwrap().requests.is_empty());
    }
}

#[actix_web::test]
async fn native_workspace_handoff_pending_without_run_fences_new_bind() {
    let _proof = HTTP_PROOF.lock().await;
    let (_home, state, provider, session, b) = controlled_source_fixture().await;
    let prepared = state.workspace_handoffs.prepare(&session.id).await.unwrap();
    let reservation = reserve_owned_source(&state, &session).await;
    let error = bind_execution(
        state.clone(),
        &session,
        &reservation,
        state.tools_for(ToolSurface::Root),
    )
    .err()
    .expect("a preparation with no run must fence the subsequent genuine Native bind");
    assert!(
        error.to_string().contains("pending Workspace transaction"),
        "{error}"
    );
    drop(prepared);
    bindings()
        .lock()
        .unwrap()
        .insert(session.id.clone(), std::sync::Weak::new());
    let binding = bind_execution(
        state.clone(),
        &session,
        &reservation,
        state.tools_for(ToolSurface::Root),
    )
    .expect("the same genuine owner can bind after the no-run preparation has ended");
    let executor = bindings()
        .lock()
        .unwrap()
        .remove(&session.id)
        .unwrap()
        .upgrade()
        .unwrap();
    let call = ToolCall {
        id: "controlled-host-set-before-first-q".into(),
        tool_type: "function".into(),
        function: FunctionCall {
            name: "Workspace".into(),
            arguments: json!({"path":b}).to_string(),
        },
    };
    let result = state
        .tools_for(ToolSurface::Root)
        .execute_exact_with_context_outcome(
            &call,
            "Workspace",
            ToolExecutionContext {
                session_id: Some(&session.id),
                ..ToolExecutionContext::none(&call.id)
            },
        )
        .await
        .unwrap()
        .into_tool_result();
    assert!(
        result.success,
        "actual Host commit before first Q: {}",
        result.result
    );
    let mut live = state
        .storage
        .load_session(&session.id)
        .await
        .unwrap()
        .unwrap();
    let host = NativeHost {
        run: executor.run.clone(),
    };
    host.refresh_runtime_scope(&mut live, "controlled-first-native-execution")
        .await
        .unwrap();
    {
        let current = executor.run.current.read().unwrap();
        assert_eq!(
            current.execution_id.as_deref(),
            Some("controlled-first-native-execution")
        );
        assert!(current.current_input_id.is_none());
        assert_eq!(current.epoch, 0);
        assert_eq!(
            executor.run.epoch.load(Ordering::Acquire),
            0,
            "private refresh creates no Q"
        );
    }
    assert!(
        host.refresh_runtime_scope(&mut live, "different-execution")
            .await
            .is_err(),
        "first private refresh binds exactly one execution id"
    );
    host.finish(&session.id, "controlled-first-native-execution");
    reservation.abandon().await;
    drop(binding);
    assert!(provider.trace.lock().unwrap().requests.is_empty());
}

#[actix_web::test]
async fn native_workspace_handoff_idle_expired_actor_observes_both_switches_before_reopen() {
    let _proof = HTTP_PROOF.lock().await;
    let (_home, state, provider, session, b) = controlled_source_fixture().await;
    let past = chrono::Utc::now() - chrono::Duration::minutes(1);
    let expired = state
        .session_store
        .claim_activation(&bamboo_domain::ActorActivationClaim {
            actor_id: session.id.clone(),
            run_id: "controlled-expired-run".into(),
            lease_owner: "controlled-expired-host".into(),
            now: past,
            lease_expires_at: past + chrono::Duration::seconds(15),
            inbox_generation: 0,
            placement_ref: None,
        })
        .await
        .unwrap();
    assert!(state.agent_runners.read().await.get(&session.id).is_none());
    let actor = state
        .session_store
        .inspect_actor(&session.id)
        .await
        .unwrap();
    assert_eq!(actor.actor.state, bamboo_domain::ActorLogicalState::Active);
    assert!(expired.lease_expires_at < chrono::Utc::now());
    let a = session.workspace_path_meta().unwrap();
    for (offset, workspace) in [b.to_string_lossy().into_owned(), a]
        .into_iter()
        .enumerate()
    {
        let call = ToolCall {
            id: format!("controlled-idle-workspace-{offset}"),
            tool_type: "function".into(),
            function: FunctionCall {
                name: "Workspace".into(),
                arguments: json!({"path":workspace}).to_string(),
            },
        };
        let result = state
            .tools_for(ToolSurface::Root)
            .execute_exact_with_context_outcome(
                &call,
                "Workspace",
                ToolExecutionContext {
                    session_id: Some(&session.id),
                    ..ToolExecutionContext::none(&call.id)
                },
            )
            .await
            .unwrap()
            .into_tool_result();
        assert!(result.success, "actual idle Host setter: {}", result.result);
    }
    let current = state
        .storage
        .load_session(&session.id)
        .await
        .unwrap()
        .unwrap();
    // Do not inspect between the two writes: inspect itself observes a
    // revision and would repair the very production omission being tested.
    let actor = state.session_store.ensure_actor(&session.id).await.unwrap();
    assert_eq!(current.metadata_version, session.metadata_version + 2);
    assert_eq!(
        actor.actor.observed_metadata_version,
        current.metadata_version
    );
    assert_eq!(
        actor.activation.unwrap().fence(),
        expired.fence(),
        "idle observation does not replace a lease"
    );
    let reservation = reserve_owned_source(&state, &current).await;
    assert_ne!(
        reservation.root_actor_writer().unwrap().fence,
        expired.fence()
    );
    let binding = bind_execution(
        state.clone(),
        &current,
        &reservation,
        state.tools_for(ToolSurface::Root),
    )
    .expect("the canonical Actor can reopen after both legitimate idle Workspace commits");
    reservation.abandon().await;
    drop(binding);
    assert!(provider.trace.lock().unwrap().requests.is_empty());
}

// Exercise the existing referenced HTTP Inbox adapter, not a fabricated
// SkillsHost. Native Main above still never advertises provider Workspace.
const LEGACY_READ: &str = "legacy-handoff-completed-read";
const LEGACY_TRIGGER: &str = "legacy-handoff-second-read";
const LEGACY_SET: &str = "legacy-handoff-workspace";
const LEGACY_CWD: &str = "legacy-handoff-next-cwd";

#[derive(Default)]
struct LegacyTrace {
    requests: usize,
    read_completed: usize,
    read_consumed: usize,
    trigger_completed: usize,
    trigger_consumed: usize,
    set_attempts: usize,
    set_completed: usize,
    set_errors: usize,
    cwd_completed: usize,
    later_provider_requests: usize,
}
struct LegacyProbe {
    id: String,
    a: PathBuf,
    b: PathBuf,
    uncertain: bool,
    state: Mutex<Option<web::Data<AppState>>>,
    trace: Mutex<LegacyTrace>,
}
struct LegacyProvider(Arc<LegacyProbe>);

/// A test observation around the actual Root executor. Every registration,
/// exact owner, permission and scheduling decision remains the inner owner's.
/// The Host hook runs after a distinct second Read has completed, inside
/// per_call's genuine Root capability scope. The first Read is already durable
/// before any cancellation can occur; Workspace never becomes a model function.
struct LegacyAfterRead {
    inner: Arc<dyn ToolExecutor>,
    probe: Arc<LegacyProbe>,
}
impl LegacyAfterRead {
    async fn completed_read(
        &self,
        call: &ToolCall,
        name: &str,
        ctx: ToolExecutionContext<'_>,
        outcome: Result<ToolOutcome, ToolError>,
    ) -> Result<ToolOutcome, ToolError> {
        if name != "Read" || !matches!(call.id.as_str(), LEGACY_READ | LEGACY_TRIGGER) {
            return outcome;
        }
        assert!(
            matches!(&outcome, Ok(ToolOutcome::Completed(result)) if result.success),
            "the actual legacy Read must succeed before the Host setter"
        );
        if call.id == LEGACY_READ {
            self.probe.trace.lock().unwrap().read_completed += 1;
            return outcome;
        }
        let owner = bamboo_agent_core::tools::context::root_actor_tool_writer()
            .flatten()
            .expect("actual per_call Root writer remains scoped after the completed Read");
        assert_eq!(owner.fence.actor_id, self.probe.id);
        assert_eq!(ctx.session_id, Some(self.probe.id.as_str()));
        assert!(!ctx.bypass_permissions && !ctx.auto_approve_permissions);
        let state = self.probe.state.lock().unwrap().as_ref().unwrap().clone();
        let before = state
            .storage
            .load_session(&self.probe.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            before
                .messages
                .iter()
                .filter(|message| {
                    message.tool_call_id.as_deref() == Some(LEGACY_READ)
                        && message.tool_success == Some(true)
                })
                .count(),
            1,
            "the original successful Read must be durable before the separate Host trigger"
        );
        {
            let mut trace = self.probe.trace.lock().unwrap();
            trace.trigger_completed += 1;
            trace.set_attempts += 1;
        }
        let original_cancel = {
            let runners = state.agent_runners.read().await;
            let runner = runners.get(&self.probe.id).unwrap();
            assert_eq!(runner.run_id, owner.fence.run_id);
            assert!(!runner.cancel_token.is_cancelled());
            runner.cancel_token.clone()
        };
        let host = ToolCall {
            id: LEGACY_SET.into(),
            tool_type: "function".into(),
            function: FunctionCall {
                name: "Workspace".into(),
                arguments: json!({"path":self.probe.b}).to_string(),
            },
        };
        let result = self
            .inner
            .execute_exact_with_context_outcome(
                &host,
                "Workspace",
                ToolExecutionContext {
                    tool_call_id: &host.id,
                    pre_parsed_args: None,
                    ..ctx
                },
            )
            .await;
        match result {
            Ok(ToolOutcome::Completed(result)) => {
                assert!(
                    result.success && !self.probe.uncertain,
                    "the controlled Host setter must confirm the actual commit"
                );
                let actual = state
                    .storage
                    .load_session(&self.probe.id)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    actual.workspace_path_meta().as_deref(),
                    self.probe.b.to_str()
                );
                assert_eq!(actual.project_id_meta(), before.project_id_meta());
                assert_eq!(actual.created_at, before.created_at);
                assert_eq!(actual.metadata_version, before.metadata_version + 1);
                self.probe.trace.lock().unwrap().set_completed += 1;
            }
            Err(error) => {
                assert!(
                    self.probe.uncertain
                        && error
                            .to_string()
                            .contains("Controlled loss of legacy Workspace commit confirmation"),
                    "only the deliberate post-save confirmation loss belongs to this case"
                );
                // No await after an unknown completion: the outer biased
                // cancel select may drop this future at its next suspension.
                // Canonical B is checked after the real terminal boundary.
                assert!(
                    original_cancel.is_cancelled(),
                    "the uncertain Host result must cancel its original genuine owner"
                );
                self.probe.trace.lock().unwrap().set_errors += 1;
            }
            _ => panic!("the real Host setter must have a completed or error outcome"),
        }
        // Preserve the actual trigger Read outcome verbatim. Unknown Host
        // completion may cancel this batch before the trigger checkpoints;
        // the original first Read was already durable in the prior round.
        outcome
    }
}
#[async_trait]
impl ToolExecutor for LegacyAfterRead {
    fn for_session(&self, session: &Session) -> Option<Arc<dyn ToolExecutor>> {
        self.inner.for_session(session).map(|inner| {
            Arc::new(Self {
                inner,
                probe: self.probe.clone(),
            }) as Arc<dyn ToolExecutor>
        })
    }
    fn resolve_reference_for_session(&self, session: &Session, reference: &str) -> Option<String> {
        self.inner.resolve_reference_for_session(session, reference)
    }
    async fn execute(&self, call: &ToolCall) -> Result<ToolResult, ToolError> {
        self.inner.execute(call).await
    }
    async fn execute_with_context(
        &self,
        call: &ToolCall,
        ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolResult, ToolError> {
        self.inner.execute_with_context(call, ctx).await
    }
    async fn execute_with_context_outcome(
        &self,
        call: &ToolCall,
        ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolOutcome, ToolError> {
        self.inner.execute_with_context_outcome(call, ctx).await
    }
    async fn execute_exact_with_context_outcome(
        &self,
        call: &ToolCall,
        name: &str,
        ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolOutcome, ToolError> {
        let outcome = self
            .inner
            .execute_exact_with_context_outcome(call, name, ctx)
            .await;
        self.completed_read(call, name, ctx, outcome).await
    }
    async fn check_permissions_for(
        &self,
        call: &ToolCall,
        ctx: &ToolExecutionContext<'_>,
    ) -> Result<Option<ToolOutcome>, ToolError> {
        self.inner.check_permissions_for(call, ctx).await
    }
    async fn check_permissions_for_exact(
        &self,
        call: &ToolCall,
        name: &str,
        ctx: &ToolExecutionContext<'_>,
    ) -> Result<Option<ToolOutcome>, ToolError> {
        self.inner
            .check_permissions_for_exact(call, name, ctx)
            .await
    }
    async fn check_permissions_for_resolved(
        &self,
        call: &ToolCall,
        name: &str,
        args: &Value,
        ctx: &ToolExecutionContext<'_>,
    ) -> Result<Option<ToolOutcome>, ToolError> {
        self.inner
            .check_permissions_for_resolved(call, name, args, ctx)
            .await
    }
    fn list_tools(&self) -> Vec<ToolSchema> {
        self.inner.list_tools()
    }
    fn owns_exact_tool(&self, name: &str) -> bool {
        self.inner.owns_exact_tool(name)
    }
    fn exact_tool_owner(&self, name: &str) -> Option<&dyn ToolExecutor> {
        self.inner.exact_tool_owner(name)
    }
    fn tool_guidance(&self) -> Option<String> {
        self.inner.tool_guidance()
    }
    fn tool_mutability(&self, name: &str) -> ToolMutability {
        self.inner.tool_mutability(name)
    }
    fn call_mutability(&self, call: &ToolCall) -> ToolMutability {
        self.inner.call_mutability(call)
    }
    fn tool_concurrency_safe(&self, name: &str) -> bool {
        self.inner.tool_concurrency_safe(name)
    }
    fn call_concurrency_safe(&self, call: &ToolCall) -> bool {
        self.inner.call_concurrency_safe(call)
    }
    fn call_parallel_classification(&self, call: &ToolCall) -> (ToolMutability, bool) {
        self.inner.call_parallel_classification(call)
    }
}

struct LegacyRegistration(Arc<LegacyProbe>);
impl Drop for LegacyRegistration {
    fn drop(&mut self) {
        self.0.state.lock().unwrap().take();
    }
}

#[async_trait]
impl LLMProvider for LegacyProvider {
    async fn chat_stream(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        _: Option<u32>,
        _: &str,
    ) -> Result<LLMStream, LLMError> {
        let probe = &self.0;
        assert!(
            !tools.iter().any(|tool| tool.function.name == "Workspace"),
            "legacy Main must retain the same Host-only Workspace catalog boundary"
        );
        let state = probe.state.lock().unwrap().as_ref().unwrap().clone();
        let actual = state
            .storage
            .load_session(&probe.id)
            .await
            .unwrap()
            .unwrap();
        assert!(
            state.workspace_handoffs.prepare(&probe.id).await.is_err(),
            "a provider callback has no actual per_call Root proof"
        );
        let mut trace = probe.trace.lock().unwrap();
        let round = trace.requests;
        trace.requests += 1;
        let prior = |id: &str| {
            messages
                .iter()
                .rev()
                .find(|message| message.tool_call_id.as_deref() == Some(id))
                .expect("actual legacy Tool result must reach its next round")
        };
        let (id, name, arguments) = match round {
            0 => (
                LEGACY_READ,
                "Read",
                json!({"file_path":probe.a.join("completed-once.txt")}),
            ),
            1 => {
                let read = prior(LEGACY_READ);
                assert_eq!(read.tool_success, Some(true), "{}", read.content);
                assert!(read.content.contains("LEGACY_COMPLETED_ONCE_IN_A"));
                trace.read_consumed += 1;
                // The next real tool-call handler checkpoints this prior
                // result before dispatching the distinct trigger. Its Host
                // hook below verifies canonical durable success before set;
                // do not assume an asynchronous event writer won this race.
                (
                    LEGACY_TRIGGER,
                    "Read",
                    json!({"file_path":probe.a.join("handoff-trigger.txt")}),
                )
            }
            _ if probe.uncertain => {
                trace.later_provider_requests += 1;
                return Ok(Box::pin(futures::stream::iter([
                    Ok(LLMChunk::Token("Unexpected later provider request".into())),
                    Ok(LLMChunk::Done),
                ])));
            }
            2 => {
                let trigger = prior(LEGACY_TRIGGER);
                assert_eq!(trigger.tool_success, Some(true), "{}", trigger.content);
                assert!(trigger.content.contains("LEGACY_HANDOFF_TRIGGER_IN_A"));
                trace.trigger_consumed += 1;
                assert_eq!(
                    trace.set_completed, 1,
                    "the actual Host hook must commit before this round"
                );
                assert_eq!(actual.workspace_path_meta().as_deref(), probe.b.to_str());
                assert_eq!(
                    bamboo_agent_core::workspace_state::peek_workspace(&probe.id),
                    Some(probe.b.clone())
                );
                let workspace_sections: Vec<_> = messages
                    .iter()
                    .filter(|message| message.role == Role::System)
                    .flat_map(|message| {
                        message
                            .content
                            .split("<!-- BAMBOO_WORKSPACE_CONTEXT_START -->")
                            .skip(1)
                    })
                    .map(|section| {
                        section
                            .split("<!-- BAMBOO_WORKSPACE_CONTEXT_END -->")
                            .next()
                            .unwrap()
                    })
                    .collect();
                assert!(
                    !workspace_sections.is_empty(),
                    "real next prompt must carry workspace context"
                );
                assert!(workspace_sections.iter().all(|section|
                    section.contains(probe.b.to_str().unwrap()) && !section.contains(probe.a.to_str().unwrap())),
                    "only the workspace section, not Project's retained primary path, must switch to B");
                (
                    LEGACY_CWD,
                    "exec_command",
                    json!({"cmd":"pwd","yield_time_ms":10000,"max_output_tokens":1024}),
                )
            }
            3 => {
                let cwd = prior(LEGACY_CWD);
                assert_eq!(cwd.tool_success, Some(true), "{}", cwd.content);
                let result: Value = serde_json::from_str(&cwd.content).unwrap();
                assert_eq!(result["exit_code"], 0);
                assert_eq!(
                    PathBuf::from(result["output"].as_str().unwrap().trim())
                        .canonicalize()
                        .unwrap(),
                    probe.b
                );
                trace.cwd_completed += 1;
                return Ok(Box::pin(futures::stream::iter([
                    Ok(LLMChunk::Token("Controlled legacy handoff complete".into())),
                    Ok(LLMChunk::Done),
                ])));
            }
            _ => panic!("bounded legacy fixture exceeded its provider plan"),
        };
        assert!(
            tools.iter().any(|tool| tool.function.name == name),
            "actual legacy callable {name}"
        );
        Ok(Box::pin(futures::stream::iter([
            Ok(LLMChunk::ToolCalls(vec![ToolCall {
                id: id.into(),
                tool_type: "function".into(),
                function: FunctionCall {
                    name: name.into(),
                    arguments: arguments.to_string(),
                },
            }])),
            Ok(LLMChunk::Done),
        ])))
    }
}

async fn legacy_case(uncertain: bool) {
    let _proof = HTTP_PROOF.lock().await;
    let home = tempfile::tempdir().unwrap();
    let root = home.path().canonicalize().unwrap();
    let a = root.join("legacy-a");
    let b = root.join("legacy-b");
    std::fs::create_dir(&a).unwrap();
    std::fs::create_dir(&b).unwrap();
    std::fs::write(a.join("completed-once.txt"), "LEGACY_COMPLETED_ONCE_IN_A").unwrap();
    std::fs::write(a.join("handoff-trigger.txt"), "LEGACY_HANDOFF_TRIGGER_IN_A").unwrap();
    let id = format!("legacy-workspace-handoff-{}", uuid::Uuid::new_v4());
    let probe = Arc::new(LegacyProbe {
        id: id.clone(),
        a: a.clone(),
        b: b.clone(),
        uncertain,
        state: Mutex::default(),
        trace: Mutex::default(),
    });
    let _registration = LegacyRegistration(probe.clone());
    let provider = Arc::new(LegacyProvider(probe.clone()));
    let mut config = bamboo_llm::Config::from_data_dir(Some(root.clone()));
    config.provider = "openai".into();
    config.providers_mut().openai = Some(bamboo_config::OpenAIConfig {
        model: Some("test-model".into()),
        ..Default::default()
    });
    let mut actual_state =
        AppState::new_with_provider_for_legacy_ingress_test(root.clone(), config, provider.clone())
            .await
            .unwrap();
    actual_state
        .provider_registry
        .replace_with(ProviderRegistry::new(
            std::collections::HashMap::from([("openai".into(), provider as Arc<dyn LLMProvider>)]),
            "openai".into(),
        ));
    actual_state.provider_router = Arc::new(ProviderModelRouter::new(
        actual_state.provider_registry.clone(),
    ));
    let root_tools = actual_state.tool_factory.get(ToolSurface::Root);
    actual_state.tool_factory = crate::tools::ToolSurfaceFactory::new(
        actual_state.tool_factory.get(ToolSurface::Base),
        actual_state.tool_factory.get(ToolSurface::WithTask),
        Arc::new(LegacyAfterRead {
            inner: root_tools,
            probe: probe.clone(),
        }),
    );
    let state = web::Data::new(actual_state);
    *probe.state.lock().unwrap() = Some(state.clone());
    let project = state
        .project_store
        .create_with_project_path(
            "Controlled legacy Workspace handoff",
            None,
            a.to_string_lossy(),
            vec![bamboo_domain::WorkspaceBinding {
                path: b.to_string_lossy().into_owned(),
                label: None,
                git_common_dir: None,
            }],
        )
        .unwrap();
    let mut session = Session::new(&id, "test-model");
    session.title_generated = true;
    session.set_project_id_meta(project.id.to_string());
    session.set_workspace_path_meta(a.to_string_lossy());
    session.metadata.insert(
        bamboo_engine::project_context::WORKSPACE_SOURCE_METADATA_KEY.into(),
        "explicit".into(),
    );
    state.storage.save_session(&session).await.unwrap();
    state
        .workspace_resolver
        .publish_resolved_workspace(&id, a, "controlled_legacy_initial");
    let message_id = format!("legacy-input-{}", uuid::Uuid::new_v4());
    let (status, response) = http(&state, "/api/v1/chat", json!({
        "session_id":id,"message":"Inspect the controlled legacy workspace proof","message_id":message_id
    })).await;
    assert_eq!(status, StatusCode::CREATED, "{response}");
    let queued = state.storage.load_session(&id).await.unwrap().unwrap();
    assert_eq!(
        queued.metadata.get("chat.queued_ingress.v1"),
        Some(&message_id),
        "the actual referenced HTTP ingress must select the legacy adapter"
    );
    if uncertain {
        state
            .workspace_handoffs
            .lose_legacy_commit_confirmation(&id);
    }
    let (status, response) = http(&state, &format!("/api/v1/execute/{id}"), json!({})).await;
    assert_eq!(status, StatusCode::ACCEPTED, "{response}");
    let terminal = done(&state, &id).await;
    let router = state.agent.activation_router().unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while router.current_run_id(&id).await.is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("actual legacy owner must release before cleanup");
    let durable = state.storage.load_session(&id).await.unwrap().unwrap();
    assert_eq!(durable.project_id_meta(), session.project_id_meta());
    assert_eq!(
        durable.workspace_path_meta().as_deref(),
        b.to_str(),
        "real save precedes the controlled lost confirmation"
    );
    assert_eq!(
        durable
            .messages
            .iter()
            .filter(|message| message.tool_call_id.as_deref() == Some(LEGACY_READ))
            .count(),
        1,
        "the already successful read must never replay"
    );
    let primary_result = durable
        .messages
        .iter()
        .find(|message| message.tool_call_id.as_deref() == Some(LEGACY_READ))
        .unwrap();
    assert_eq!(primary_result.tool_success, Some(true));
    assert!(primary_result
        .content
        .contains("LEGACY_COMPLETED_ONCE_IN_A"));
    let cancelled = state
        .agent_runners
        .read()
        .await
        .get(&id)
        .unwrap()
        .cancel_token
        .is_cancelled();
    let trace = probe.trace.lock().unwrap();
    assert_eq!(trace.read_completed, 1);
    assert_eq!(trace.read_consumed, 1);
    assert_eq!(trace.trigger_completed, 1);
    assert_eq!(
        trace.set_attempts, 1,
        "one Host setter after the separate real trigger Read"
    );
    assert_eq!(trace.later_provider_requests, 0);
    if uncertain {
        assert!(!state.workspace_handoffs.legacy_commit_confirmation_pending(&id),
            "the failpoint must fire in committed() after the exact real save, not an unrelated failure");
        assert!(
            matches!(terminal, AgentStatus::Cancelled | AgentStatus::Error(_)),
            "{terminal:?}"
        );
        assert_eq!(
            trace.requests, 2,
            "unknown completion cancels before a later provider round"
        );
        assert_eq!(trace.trigger_consumed, 0);
        assert_eq!(trace.set_errors, 1);
        assert_eq!(trace.set_completed, 0);
        assert_eq!(trace.cwd_completed, 0);
        assert!(cancelled);
    } else {
        assert!(matches!(terminal, AgentStatus::Completed), "{terminal:?}");
        assert_eq!(trace.requests, 4);
        assert_eq!(trace.trigger_consumed, 1);
        assert_eq!(trace.set_completed, 1);
        assert_eq!(trace.set_errors, 0);
        assert_eq!(trace.cwd_completed, 1);
        assert_eq!(
            durable
                .messages
                .iter()
                .filter(|message| {
                    message.tool_call_id.as_deref() == Some(LEGACY_TRIGGER)
                        && message.tool_success == Some(true)
                })
                .count(),
            1,
            "the successful handoff must retain the separate real trigger Read exactly once"
        );
    }
}

#[actix_web::test]
async fn legacy_workspace_handoff_real_dispatch_keeps_next_prompt_and_exec_in_b() {
    legacy_case(false).await;
}

#[actix_web::test]
async fn legacy_workspace_handoff_unknown_commit_cancels_before_later_provider() {
    legacy_case(true).await;
}

#[actix_web::test]
async fn legacy_workspace_handoff_missing_or_wrong_dispatch_proof_has_zero_mutation() {
    let _proof = HTTP_PROOF.lock().await;
    let (_home, state, provider, session, b) = controlled_source_fixture().await;
    state.workspace_resolver.publish_resolved_workspace(
        &session.id,
        PathBuf::from(session.workspace_path_meta().unwrap()),
        "controlled_legacy_initial",
    );
    let reservation = reserve_owned_source(&state, &session).await;
    // Deliberately no Native bind: this is the real non-Native runner owner.
    // Warm through the same legitimate repository reader as Workspace's
    // session_project preflight. Filling an empty cache is not a state write;
    // retain the complete immutable A snapshot for every rejected proof.
    let before = state
        .session_repo
        .try_load(&session.id)
        .await
        .unwrap()
        .unwrap();
    let cached = state
        .session_repo
        .cache()
        .get(&session.id)
        .map(|entry| entry.read().clone());
    assert!(
        cached.is_some(),
        "legal repository warm must publish the baseline"
    );
    let workspace = bamboo_agent_core::workspace_state::peek_workspace(&session.id);
    let mut wrong_run = reservation.root_actor_writer().unwrap();
    wrong_run.fence.run_id = "different-run".into();
    let mut wrong_birth = reservation.root_actor_writer().unwrap();
    wrong_birth.created_at = wrong_birth.created_at - chrono::Duration::seconds(1);
    let tools = state.tools_for(ToolSurface::Root);
    for (ordinal, proof) in [
        None,
        Some(None),
        Some(Some(wrong_run)),
        Some(Some(wrong_birth)),
    ]
    .into_iter()
    .enumerate()
    {
        let call = ToolCall {
            id: format!("legacy-unproven-workspace-{ordinal}"),
            tool_type: "function".into(),
            function: FunctionCall {
                name: "Workspace".into(),
                arguments: json!({"path":b}).to_string(),
            },
        };
        let dispatch = tools.execute_exact_with_context_outcome(
            &call,
            "Workspace",
            ToolExecutionContext {
                session_id: Some(&session.id),
                ..ToolExecutionContext::none(&call.id)
            },
        );
        let result = match proof {
            None => dispatch.await,
            Some(owner) => {
                bamboo_agent_core::tools::context::with_root_actor_tool_writer(owner, dispatch)
                    .await
            }
        };
        assert!(
            result.is_err(),
            "unproven dispatch must reject before mutation"
        );
        let after = state
            .storage
            .load_session(&session.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::to_value(&before).unwrap(),
            serde_json::to_value(&after).unwrap()
        );
        assert_eq!(
            cached
                .as_ref()
                .map(|session| serde_json::to_value(session).unwrap()),
            state
                .session_repo
                .cache()
                .get(&session.id)
                .map(|entry| serde_json::to_value(entry.read().clone()).unwrap())
        );
        assert_eq!(
            bamboo_agent_core::workspace_state::peek_workspace(&session.id),
            workspace
        );
        assert!(
            !reservation.cancel_token().is_cancelled(),
            "a rejected preparation cannot cancel its valid owner"
        );
    }
    reservation.abandon().await;
    assert!(provider.trace.lock().unwrap().requests.is_empty());
}
