//! Original #1048 composition, now wired as a tests-first acceptance target.
//! The current-user default resolver is an explicit production dependency;
//! its absence must produce a real failure rather than manual scope grants.
//! Planner API was checked against accepted a64a852810b7104954e9fd1f60230df09b48eef2.
//! Supervisor's delegation-only policy is independently owned by #2334/#2335;
//! default-all resolution/legacy-scope migration remains an unaccepted dependency.
//! No runtime PASS is claimed by its presence. Store, Inbox, tools and native
//! execution below are production implementations; only model replies are held.
#![cfg(any(target_os = "linux", target_os = "macos"))]

use super::AppState;
use async_trait::async_trait;
use bamboo_agent_core::tools::{
    FunctionCall, Tool, ToolCall, ToolCtx, ToolError, ToolOutcome, ToolResult, ToolSchema,
};
use bamboo_domain::{
    Message, ObserverChangeKind, ObserverViewChange, PendingQuestion, PendingQuestionSource,
    ProviderModelRef, Session, SessionActivationPolicy, SessionInboxPort, SessionMessageEnvelope,
    SessionPermissionMode, SupervisorReference,
};
use bamboo_engine::session_app::child_session::{
    create_child_action, ChildSessionPort, CreateChildInput,
};
use bamboo_engine::session_app::supervisor::SupervisorSessionService;
use bamboo_llm::{Config, LLMChunk, LLMError, LLMProvider, LLMStream, ProviderRegistry};
use serde_json::{json, Value};
use std::{
    collections::{BTreeSet, HashMap},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::sync::Notify;
use uuid::Uuid;

const PROVIDER: &str = "epic-chain-provider";
const PARENT: &str = "epic-supervisor";
const TRACKER: &str = "epic-tracker";
const PLANNER: &str = "epic-planner";
const WORKER: &str = "epic-original-worker";
const BLOCKED_MODEL: &str = "epic-original-blocked-worker";
const QUESTION_TOOL: &str = "epic_fixture_ask_user";
const OBSERVER: &str = "epic-fixed-tracker";
const ROOT_A: &str = "epic-existing-a";
const ROOT_B: &str = "epic-existing-b";
const CONTROL_ROOT: &str = "epic-negative-control";
const PRIVATE_CONTROL: &str = "UNASSIGNED_CURRENT_USER_ROOT_PRIVATE_HISTORY";
const TARGET_GRANT_REJECTION: &str = "context_view: Observer has no current view grant";
const USER: &str = "EPIC_USER: plan, then execute exactly one authorized followup on Root B";
const COLD: &str = "EPIC_COLD: reread the same plan and retry the identical followup";
const FOLLOWUP: &str =
    "EPIC_AUTHORIZED_FOLLOWUP: apply the selected bounded plan to this original Root";
const OPERATION: &str = "8ce86015-a819-4e67-b688-ddd30d6ba240";
const BLOCKER: &str = "EPIC_BLOCKER: Root A awaits its existing owner decision";
const DURING_PLAN: &str = "EPIC_SECOND_PROJECT_OBSERVED_DURING_PLAN";
const DONE: &str = "EPIC_CHAIN_FINISHED";
const COLD_DONE: &str = "EPIC_COLD_FINISHED";
const APPENDIX: &str = "EXCLUDED_EPIC_APPENDIX";
const PRIVATE_A: &str = "ORIGINAL_ROOT_A_PRIVATE_HISTORY";
const PRIVATE_B: &str = "ORIGINAL_ROOT_B_PRIVATE_HISTORY";
const WAIT: Duration = Duration::from_secs(90);

fn call(name: &str, id: &str, arguments: Value) -> LLMChunk {
    LLMChunk::ToolCalls(vec![ToolCall {
        id: id.into(),
        tool_type: "function".into(),
        function: FunctionCall {
            name: name.into(),
            arguments: arguments.to_string(),
        },
    }])
}
fn tool_json(messages: &[Message], id: &str) -> Option<Value> {
    let message = messages
        .iter()
        .rev()
        .find(|m| m.tool_call_id.as_deref() == Some(id))?;
    assert_ne!(
        message.tool_success,
        Some(false),
        "{id}: {}",
        message.content
    );
    let mut result: Value = serde_json::from_str(&message.content).unwrap();
    if let Some(inner) = result.get("result").and_then(Value::as_str) {
        result = serde_json::from_str(inner).unwrap();
    }
    Some(result)
}
fn tool_arguments(messages: &[Message], id: &str) -> Value {
    let call = messages
        .iter()
        .flat_map(|message| message.tool_calls.iter().flatten())
        .find(|call| call.id == id)
        .expect("canonical recorded tool call");
    serde_json::from_str(&call.function.arguments).unwrap()
}
fn artifact(messages: &[Message]) -> Option<Value> {
    messages.iter().rev().find_map(|message| {
        let (_, tail) = message.content.split_once("Child typed result:\n")?;
        let result: Value = serde_json::from_str(tail.lines().next()?).ok()?;
        (result["view"] == "planner_artifact").then_some(result)
    })
}
fn recent_messages(messages: &[Message]) -> Vec<Value> {
    messages
        .iter()
        .rev()
        .take(12)
        .rev()
        .map(|message| {
            json!({"id":message.id,"role":message.role,
                "call_id":message.tool_call_id,"success":message.tool_success,
                "compressed":message.compressed,
                "markers":[BLOCKER,DURING_PLAN,DONE,USER,COLD,"EPIC_ROOT_B_PERSISTED_STATUS_OBSERVED"]
                    .into_iter().filter(|marker|message.content.contains(*marker)).collect::<Vec<_>>(),
                "preview":message.content.chars().take(180).collect::<String>(),
                "metadata":serde_json::to_string(&message.metadata).unwrap().chars().take(256).collect::<String>(),
                "calls":message.tool_calls.iter().flatten().take(3)
                    .map(|call|json!({"id":call.id,"name":call.function.name})).collect::<Vec<_>>()})
        })
        .collect()
}

struct Probe {
    workspace: PathBuf,
    requests: AtomicUsize,
    worker_calls: AtomicUsize,
    blocked_calls: AtomicUsize,
    planner_calls: AtomicUsize,
    tracker_calls: AtomicUsize,
    plan_started: AtomicBool,
    tracker_entered: Notify,
    tracker_release: Notify,
    planner_entered: Notify,
    planner_release: Notify,
    observed: Mutex<BTreeSet<String>>,
    control_revision: Mutex<Option<String>>,
    followup_target: Mutex<Option<Value>>,
    parent_requests: Mutex<Vec<(Vec<Message>, Value)>>,
    reported_changes: Mutex<Vec<ObserverViewChange>>,
    denied_control_exports: Mutex<BTreeSet<String>>,
    denied_control_reads: Mutex<BTreeSet<String>>,
}
impl Probe {
    fn new(workspace: PathBuf) -> Self {
        Self {
            workspace,
            requests: AtomicUsize::new(0),
            worker_calls: AtomicUsize::new(0),
            blocked_calls: AtomicUsize::new(0),
            planner_calls: AtomicUsize::new(0),
            tracker_calls: AtomicUsize::new(0),
            plan_started: AtomicBool::new(false),
            tracker_entered: Notify::new(),
            tracker_release: Notify::new(),
            planner_entered: Notify::new(),
            planner_release: Notify::new(),
            observed: Mutex::new(BTreeSet::new()),
            control_revision: Mutex::new(None),
            followup_target: Mutex::new(None),
            parent_requests: Mutex::new(Vec::new()),
            reported_changes: Mutex::new(Vec::new()),
            denied_control_exports: Mutex::new(BTreeSet::new()),
            denied_control_reads: Mutex::new(BTreeSet::new()),
        }
    }
    fn parent(&self, messages: &[Message]) -> LLMChunk {
        assert!(!messages.iter().any(|m| m.content.contains(APPENDIX)));
        assert!(!messages
            .iter()
            .any(|m| m.content.contains(PRIVATE_A) || m.content.contains(PRIVATE_B)));
        // Real native calls must be rejected by the Supervisor role, even
        // though its isolated parent permission mode is permissive. The same
        // source.txt must later be readable by its ordinary read-only Planner.
        for (id, name, args) in [
            (
                "epic-supervisor-denied-read",
                "Read",
                json!({"file_path":self.workspace.join("source.txt")}),
            ),
            (
                "epic-supervisor-denied-write",
                "Write",
                json!({"file_path":self.workspace.join("supervisor-must-not-write.txt"),"content":"forbidden"}),
            ),
            (
                "epic-supervisor-denied-bash",
                "Bash",
                json!({"command":format!("printf forbidden > '{}'",self.workspace.join("supervisor-must-not-run.txt").display())}),
            ),
        ] {
            let Some(result) = messages
                .iter()
                .find(|message| message.tool_call_id.as_deref() == Some(id))
            else {
                return call(name, id, args);
            };
            assert_eq!(
                result.tool_success,
                Some(false),
                "Supervisor must delegate {name}: {}",
                result.content
            );
        }
        assert!(!self
            .workspace
            .join("supervisor-must-not-write.txt")
            .exists());
        assert!(!self.workspace.join("supervisor-must-not-run.txt").exists());
        // DEFAULT_CURRENT_USER_RESOLVER: sole catalog contract consumption
        // point. The existing native business-view surface must see both
        // current-user Projects without configure/attach/UI clicks. This is
        // expected to remain RED until the owning resolver migration ships;
        // no fallback grant, guessed API or hand-written catalog is supplied.
        let Some(directory) = tool_json(messages, "epic-default-current-user-catalog") else {
            return call(
                "session_history",
                "epic-default-current-user-catalog",
                json!({"action":"list","query":"epic-existing-","limit":8}),
            );
        };
        let entries = directory["sessions"]
            .as_array()
            .expect("canonical bounded current-user catalog");
        let targets: BTreeSet<_> = entries
            .iter()
            .map(|entry| entry["session_id"].as_str().unwrap())
            .collect();
        let projects: BTreeSet<_> = entries
            .iter()
            .map(|entry| entry["project_id"].as_str().unwrap())
            .collect();
        assert_eq!(targets, BTreeSet::from([ROOT_A, ROOT_B]));
        assert_eq!(
            projects.len(),
            2,
            "default catalog spans both current-user Projects"
        );
        let followup_target = entries
            .iter()
            .find(|entry| entry["session_id"] == ROOT_B)
            .unwrap()
            .get("current_user_target")
            .filter(|target| target.is_object())
            .expect("canonical discovery returns the original worker selection")
            .clone();
        assert_eq!(followup_target["session_id"], ROOT_B);
        // This observed DTO selects a lifetime; it grants no permission. Keep
        // it unchanged on the stable-operation retry after a cold restart.
        *self.followup_target.lock().unwrap() = Some(followup_target.clone());
        // This third Root shares Project A and is visible to the Supervisor
        // by default, but deliberately lies outside the Tracker's two targets.
        let Some(control_catalog) = tool_json(messages, "epic-control-catalog") else {
            return call(
                "session_history",
                "epic-control-catalog",
                json!({"action":"list","query":CONTROL_ROOT,"limit":4}),
            );
        };
        let controls = control_catalog["sessions"].as_array().unwrap();
        assert_eq!(controls.len(), 1);
        assert_eq!(controls[0]["session_id"], CONTROL_ROOT);
        let project_a = entries
            .iter()
            .find(|entry| entry["session_id"] == ROOT_A)
            .unwrap()["project_id"]
            .clone();
        assert_eq!(controls[0]["project_id"], project_a);
        let Some(control) = tool_json(messages, "epic-control-inspect") else {
            return call(
                "session_history",
                "epic-control-inspect",
                json!({"action":"inspect","session_id":CONTROL_ROOT,"max_chars":800}),
            );
        };
        assert_eq!(control["session_id"], CONTROL_ROOT);
        assert_eq!(control["project_id"], project_a);
        assert_eq!(control["status_is_live"], false);
        assert!(!control.to_string().contains(PRIVATE_CONTROL));
        let Some(export) = tool_json(messages, "epic-control-export") else {
            return call(
                "session_history",
                "epic-control-export",
                json!({"action":"export_context","session_id":CONTROL_ROOT}),
            );
        };
        let revision = export["revision"]
            .as_str()
            .expect("Supervisor actually published the control view");
        *self.control_revision.lock().unwrap() = Some(revision.to_string());
        assert!(!export.to_string().contains(PRIVATE_CONTROL));
        let cold = messages.iter().any(|m| m.content == COLD);
        if let Some(reference) = artifact(messages) {
            assert_eq!(reference["available"], true);
            assert_eq!(reference["execution_approved"], false);
            let prefix = if cold { "epic-cold" } else { "epic-plan" };
            let index_id = format!("{prefix}-index");
            let section_id = format!("{prefix}-section");
            let followup_id = format!("{prefix}-followup");
            let read = reference["read"].clone();
            let Some(index) = tool_json(messages, &index_id) else {
                return call("Plan", &index_id, read);
            };
            let Some(section) = tool_json(messages, &section_id) else {
                let section = index["sections"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|section| section["heading"] == "Chosen action")
                    .unwrap()["id"]
                    .clone();
                let mut read = read;
                read["section_id"] = section;
                read["max_bytes"] = json!(512);
                return call("Plan", &section_id, read);
            };
            assert!(section["text"]
                .as_str()
                .unwrap()
                .contains("EPIC_SELECTED_ACTION"));
            if tool_json(messages, &followup_id).is_none() {
                return call(
                    "session_control",
                    &followup_id,
                    json!({"action":"followup",
                    "target_session_id":ROOT_B,"target_ref":followup_target,
                    "operation_id":OPERATION,"message":FOLLOWUP}),
                );
            }
            return LLMChunk::Token(if cold { COLD_DONE } else { DONE }.into());
        }
        if messages.iter().any(|m| m.content.contains(BLOCKER)) {
            if !self.plan_started.swap(true, Ordering::SeqCst) {
                let user = messages
                    .iter()
                    .find(|m| m.content == USER)
                    .expect("canonical accepted user input");
                return call(
                    "Plan",
                    "epic-create-plan",
                    json!({
                    "task":"Read source.txt and plan the explicitly authorized bounded Root B followup. Do not implement.",
                    "context_packet":{"version":1,"objective":"Return a bounded plan for the original Root B",
                        "constraints":["Preserve both original Roots and Root A pending question"],
                        "acceptance":["Return a versioned plan with a selected action section"],
                        "non_goals":["No implementation or approval by the planner"],
                        "necessary_user_instructions":[],"recorded_decisions":[BLOCKER],
                        "source_user_message_ids":[user.id],"background_message_ids":[]}}),
                );
            }
            return LLMChunk::Token(DURING_PLAN.into());
        }
        LLMChunk::Token("EPIC_IDLE_PARENT_FINISHED_WITH_ACTIVE_TRACKER".into())
    }
    async fn tracker(&self, messages: &[Message], tools: &[ToolSchema]) -> LLMChunk {
        if self.tracker_calls.fetch_add(1, Ordering::SeqCst) == 0 {
            self.tracker_entered.notify_one();
            self.tracker_release.notified().await;
        }
        assert_eq!(
            tools
                .iter()
                .map(|tool| tool.function.name.as_str())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from(["session_history", "session_history_current"])
        );
        assert!(!messages
            .iter()
            .any(|m| m.content.contains(PRIVATE_A) || m.content.contains(PRIVATE_B)));
        let change = messages
            .iter()
            .rev()
            .find_map(|message| {
                let (_, suffix) = message.content.split_once("immutable selector: ")?;
                serde_json::Deserializer::from_str(suffix)
                    .into_iter::<ObserverViewChange>()
                    .next()?
                    .ok()
            })
            .expect("actual canonical subscription notice");
        // A valid Supervisor publication is a test selector, never a grant
        // for this Child. Both native operations must refuse the unassigned
        // current-user Root, rather than accidentally borrowing global scope.
        let control_revision = self
            .control_revision
            .lock()
            .unwrap()
            .clone()
            .expect("Supervisor published its actual control view before releasing Tracker");
        for (action, call_id, args) in [
            (
                "export_context",
                format!("control-export-{}", change.event_id.as_str()),
                json!({"action":"export_context","session_id":CONTROL_ROOT}),
            ),
            (
                "read_context",
                format!("control-read-{}", change.event_id.as_str()),
                json!({"action":"read_context","session_id":CONTROL_ROOT,"revision":control_revision,
                    "file":"status","offset":0,"limit":40}),
            ),
        ] {
            let Some(result) = messages
                .iter()
                .find(|message| message.tool_call_id.as_deref() == Some(call_id.as_str()))
            else {
                return call("session_history", &call_id, args);
            };
            assert_eq!(
                result.tool_success,
                Some(false),
                "unassigned {action} must be denied: {}",
                result.content
            );
            // The current port exposes no structured target-grant code:
            // Store PermissionDenied is preserved by context_view's execution
            // error. Require that exact authorization failure for BOTH calls.
            // ENOENT, caller provenance, or a selector mismatch would otherwise
            // also return false for the Supervisor-published revision and would
            // not prove that the Observer's narrower target grant was enforced.
            assert!(result.content.contains(TARGET_GRANT_REJECTION),
                "unassigned {action} must fail at the Observer target-grant check before view IO/provenance, got: {}",
                result.content);
            assert!(!result.content.contains(PRIVATE_CONTROL));
            let denials = if action == "export_context" {
                &self.denied_control_exports
            } else {
                &self.denied_control_reads
            };
            denials
                .lock()
                .unwrap()
                .insert(change.event_id.as_str().to_string());
        }
        let attack_id = format!("attack-{}", change.event_id.as_str());
        let read_id = format!("status-{}", change.event_id.as_str());
        if !messages
            .iter()
            .any(|m| m.tool_call_id.as_deref() == Some(attack_id.as_str()))
        {
            return call(
                "session_control",
                &attack_id,
                json!({"action":"followup",
                "target_session_id":ROOT_B,
                "target_ref":self.followup_target.lock().unwrap().clone().expect("real catalog observed before Tracker release"),
                "operation_id":"6c934c1d-38cd-48db-b35e-fb5a6fed8071","message":"DENIED_TRACKER_ACTION"}),
            );
        }
        let denied = messages
            .iter()
            .find(|m| m.tool_call_id.as_deref() == Some(attack_id.as_str()))
            .unwrap();
        assert_eq!(denied.tool_success, Some(false), "{}", denied.content);
        if let Some(view) = tool_json(messages, &read_id) {
            assert!(view["content"]
                .as_str()
                .unwrap()
                .contains(&format!("- Root: {}", change.target_session_id)));
            self.observed
                .lock()
                .unwrap()
                .insert(change.target_session_id.clone());
            let summary = if change.target_session_id == ROOT_A {
                assert!(view["content"]
                    .as_str()
                    .unwrap()
                    .contains("- Has pending question: true"));
                BLOCKER
            } else {
                "EPIC_ROOT_B_PERSISTED_STATUS_OBSERVED"
            };
            self.reported_changes.lock().unwrap().push(change.clone());
            return LLMChunk::Token(format!(
                "{summary}; bounded observation {} of {}",
                change.sequence, change.target_session_id
            ));
        }
        call(
            "session_history",
            &read_id,
            json!({"action":"read_context","session_id":change.target_session_id,
            "revision":change.view_revision,"file":"status","offset":0,"limit":40}),
        )
    }
    async fn planner(&self, messages: &[Message]) -> LLMChunk {
        let request = self.planner_calls.fetch_add(1, Ordering::SeqCst);
        assert!(request < 3, "one bounded planner only");
        if request == 0 {
            self.planner_entered.notify_one();
            self.planner_release.notified().await;
            return call(
                "Read",
                "epic-planner-read",
                json!({"file_path":self.workspace.join("source.txt")}),
            );
        }
        let source = messages
            .iter()
            .find(|message| message.tool_call_id.as_deref() == Some("epic-planner-read"))
            .expect("actual delegated Planner Read result");
        assert_ne!(
            source.tool_success,
            Some(false),
            "Supervisor file prohibition must not leak into ordinary Planner permissions: {}",
            source.content
        );
        assert!(source.content.contains("EPIC_SOURCE_EVIDENCE"));
        if request == 1 {
            return call(
                "Write",
                "epic-planner-denied-write",
                json!({"file_path":self.workspace.join("must-not-exist.txt"),"content":"must be denied"}),
            );
        }
        let denial = messages
            .iter()
            .find(|m| m.tool_call_id.as_deref() == Some("epic-planner-denied-write"))
            .unwrap();
        assert_eq!(denial.tool_success, Some(false), "{}", denial.content);
        assert!(!self.workspace.join("must-not-exist.txt").exists());
        LLMChunk::Token(json!({"version":1,"outcome":"completed","summary":"A bounded Root B action is proposed",
            "reported_evidence":[{"description":"Read source.txt in this isolated workspace","reference":"source.txt","sha256":null}],
            "reported_verification":[{"check":"implementation","reported_status":"not_run","details":"planning only"}],
            "proposals":[],"blockers":[],"open_decisions":[],"plan_markdown":format!(
                "# Root B plan\n## Chosen action\nEPIC_SELECTED_ACTION: perform the authorized bounded followup.\n## Appendix\n{}",
                format!("{APPENDIX} ").repeat(1000))}).to_string())
    }
}
/// Controlled input tool, using the same supported overlay and generic
/// NeedsHuman protocol as the existing real SessionControl Host tests. It is
/// not a claim that a retired AskUser builtin is registered in the product.
struct ExistingOwnerQuestion;
#[async_trait]
impl Tool for ExistingOwnerQuestion {
    fn name(&self) -> &str {
        QUESTION_TOOL
    }
    fn description(&self) -> &str {
        "Ask the existing Root owner to choose a bounded approach"
    }
    fn parameters_schema(&self) -> Value {
        json!({"type":"object","properties":{},"additionalProperties":false})
    }
    async fn invoke(&self, _: Value, ctx: ToolCtx) -> Result<ToolOutcome, ToolError> {
        let question = "Which bounded Root A approach is authorized?";
        Ok(ToolOutcome::NeedsHuman {
            question: PendingQuestion {
                tool_call_id: ctx.tool_call_id.to_string(),
                tool_name: QUESTION_TOOL.into(),
                question: question.into(),
                options: vec!["A".into(), "B".into()],
                allow_custom: true,
                source: PendingQuestionSource::PauseTool,
            },
            result: ToolResult::text(
                true,
                json!({"status":"awaiting_user_input","question":question,
                "options":["A","B"],"allow_custom":true})
                .to_string(),
            ),
        })
    }
}

#[async_trait]
impl LLMProvider for Probe {
    async fn chat_stream(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        _: Option<u32>,
        model: &str,
    ) -> Result<LLMStream, LLMError> {
        assert!(
            self.requests.fetch_add(1, Ordering::SeqCst) < 64,
            "bounded full-chain model request count"
        );
        assert!(
            !messages
                .iter()
                .any(|message| message.content.contains(PRIVATE_CONTROL)),
            "unassigned control transcript must never enter this chain's model context"
        );
        let chunk = match model {
            PARENT => {
                assert!(
                    !tools.iter().any(|tool| matches!(
                        tool.function.name.as_str(),
                        "Read" | "Write" | "Bash" | "exec_command"
                    )),
                    "Supervisor provider catalog must require delegation"
                );
                let chunk = self.parent(messages);
                let output = match &chunk {
                    LLMChunk::Token(text) => json!({"token":text}),
                    LLMChunk::ToolCalls(calls) => json!({"calls":calls.iter()
                        .map(|call|json!({"id":call.id,"name":call.function.name})).collect::<Vec<_>>()}),
                    _ => json!({"kind":"other"}),
                };
                self.parent_requests
                    .lock()
                    .unwrap()
                    .push((messages.to_vec(), output));
                chunk
            }
            TRACKER => self.tracker(messages, tools).await,
            PLANNER => self.planner(messages).await,
            BLOCKED_MODEL => {
                assert_eq!(
                    self.blocked_calls.fetch_add(1, Ordering::SeqCst),
                    0,
                    "existing Root A must remain suspended without an answer"
                );
                assert!(messages
                    .iter()
                    .any(|message| message.content.contains(PRIVATE_A)));
                assert!(tools.iter().any(|tool| tool.function.name == QUESTION_TOOL));
                call(QUESTION_TOOL, "epic-existing-owner-question", json!({}))
            }
            WORKER => {
                assert_eq!(
                    self.worker_calls.fetch_add(1, Ordering::SeqCst),
                    0,
                    "followup never re-executes"
                );
                assert!(messages.iter().any(|m| m.content.contains(PRIVATE_B)));
                assert!(messages.iter().any(|m| m.content.contains(FOLLOWUP)));
                assert!(!messages.iter().any(|m| m.content.contains(PRIVATE_A)));
                LLMChunk::Token("EPIC_ORIGINAL_WORKER_FINISHED".into())
            }
            other => panic!("unrequested model route: {other}"),
        };
        Ok(Box::pin(futures::stream::iter([
            Ok(chunk),
            Ok(LLMChunk::ProviderUsage {
                input_tokens: Some(7),
                output_tokens: Some(3),
                total_tokens: Some(10),
                reasoning_tokens: None,
                cache_creation_input_tokens: None,
                cache_read_input_tokens: None,
                cache_write_input_tokens: None,
            }),
            Ok(LLMChunk::Done),
        ])))
    }
}

async fn host(home: &Path, probe: Arc<Probe>) -> AppState {
    let mut config = Config::from_data_dir(Some(home.into()));
    config.provider = PROVIDER.into();
    config.features.provider_model_ref = true;
    config.defaults = Some(
        serde_json::from_value(json!({"chat":{"provider":PROVIDER,"model":PARENT},
        "subagent_models":{"planner":{"provider":PROVIDER,"model":PLANNER}}}))
        .unwrap(),
    );
    config.subagents_mut().max_concurrent = Some(2);
    config
        .memory_mut()
        .get_or_insert_with(Default::default)
        .auto_dream_enabled = false;
    let mut state =
        AppState::new_with_provider_for_legacy_ingress_test(home.into(), config, probe.clone())
            .await
            .unwrap();
    // The test question is an input simulator on the same supported Root
    // surface hot-reload seam as session_control's real clarification cases.
    // Native activation still binds the actual Actor writer and event handoff.
    let root = Arc::new(crate::tools::OverlayToolExecutor::new(
        state.tools_for(crate::tools::ToolSurface::Root),
        Arc::new(ExistingOwnerQuestion),
    ));
    state
        .child_completion_coordinator
        .set_root_tools(root.clone())
        .await;
    state.tool_factory = crate::tools::ToolSurfaceFactory::new(
        state.tools_for(crate::tools::ToolSurface::Base),
        state.tools_for(crate::tools::ToolSurface::WithTask),
        root,
    );
    state.provider_registry.replace_with(ProviderRegistry::new(
        HashMap::from([(PROVIDER.into(), probe as Arc<dyn LLMProvider>)]),
        PROVIDER.into(),
    ));
    state
}
async fn load(state: &AppState, id: &str) -> Session {
    state.storage.load_session(id).await.unwrap().unwrap()
}
async fn user(state: &AppState, id: &str, content: &str) {
    let receipt = state
        .session_messenger
        .admit_with_activation_intent(
            SessionMessageEnvelope::user_input(id, content),
            SessionActivationPolicy::RespectSpecificWait,
            None,
        )
        .await
        .unwrap();
    state
        .session_messenger
        .activate_prepared(&receipt)
        .await
        .unwrap();
}
async fn wait(
    state: &AppState,
    id: &str,
    stage: &str,
    probe: &Probe,
    predicate: impl Fn(&Session) -> bool,
) -> Session {
    match tokio::time::timeout(WAIT, async {
        loop {
            let session = load(state, id).await;
            if predicate(&session) {
                break session;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    {
        Ok(session) => session,
        Err(_) => {
            let session = load(state, id).await;
            let inbox = state.session_inbox.inspect(id).await.unwrap();
            let requests = {
                let requests = probe.parent_requests.lock().unwrap();
                requests
                    .iter()
                    .enumerate()
                    .rev()
                    .take(3)
                    .map(|(index, (messages, output))| {
                        json!({"index":index,"total_messages":messages.len(),
                        "input":recent_messages(messages),"output":output})
                    })
                    .collect::<Vec<_>>()
            };
            let reports: Vec<_> = probe
                .reported_changes
                .lock()
                .unwrap()
                .iter()
                .rev()
                .take(8)
                .map(|change| {
                    json!({"event_id":change.event_id,"sequence":change.sequence,
                    "target":change.target_session_id})
                })
                .collect();
            let observer =
                tokio::time::timeout(Duration::from_secs(2), state.storage.load_session(OBSERVER))
                    .await
                    .ok()
                    .and_then(Result::ok)
                    .flatten();
            let child_ids: Vec<_> = session
                .agent_runtime_state
                .as_ref()
                .and_then(|runtime| runtime.waiting_for_children.as_ref())
                .map(|wait| wait.child_session_ids.iter().take(2).cloned().collect())
                .unwrap_or_default();
            let mut waited_children = Vec::new();
            for child_id in child_ids {
                let snapshot = tokio::time::timeout(Duration::from_secs(2), async {
                    let child = state.storage.load_session(&child_id).await
                        .map_err(|error|error.to_string())?
                        .ok_or_else(|| "canonical waited Child missing".to_string())?;
                    let inbox = state.session_inbox.inspect(&child_id).await
                        .map_err(|error|error.to_string())?;
                    let owner = state.session_activation_router.current_run_id(&child_id).await;
                    let raw_source = child.metadata.get("runtime.child_completion_source_v1");
                    Ok::<Value,String>(json!({"status":child.last_run_status(),"error":child.last_run_error(),
                        "active_owner":owner,"parent":child.parent_session_id,
                        "completion_source_metadata":{"present":raw_source.is_some(),
                            "raw_unvalidated":raw_source.filter(|raw|raw.len()<=4096),
                            "validation":"diagnostic only; not an authority or publisher result"},
                        "planner_contract":bamboo_engine::session_app::child_session::planner_artifact::uses_artifact_contract(&child),
                        "messages":recent_messages(&child.messages),
                        "inbox":{"pending":inbox.pending,"claimed":inbox.claimed,
                            "generation":inbox.generation,"activation_generation":inbox.activation_generation}}))
                }).await;
                waited_children.push(match snapshot {
                    Ok(Ok(snapshot)) => json!({"id":child_id,"snapshot":snapshot}),
                    Ok(Err(error)) => json!({"id":child_id,"unavailable":error}),
                    Err(_) => json!({"id":child_id,"unavailable":"diagnostic read timeout"}),
                });
            }
            eprintln!(
                "EPIC_CHAIN_DIAGNOSTIC {}",
                json!({"stage":stage,
                "requests":probe.requests.load(Ordering::SeqCst),
                "tracker_calls":probe.tracker_calls.load(Ordering::SeqCst),
                "planner_calls":probe.planner_calls.load(Ordering::SeqCst),
                "worker_calls":probe.worker_calls.load(Ordering::SeqCst),
                "parent_requests":requests,"emitted_tracker_reports":reports,
                "waited_children":waited_children,
                "parent_artifact":artifact(&session.messages),
                "canonical_parent_messages":recent_messages(&session.messages),
                "observer":observer.as_ref().map(|child|json!({"status":child.last_run_status(),
                    "error":child.last_run_error(),"messages":recent_messages(&child.messages)})),
                "subscription":session.supervisor_management.as_ref()
                    .and_then(|management|management.subscriptions.get(OBSERVER)).map(|subscription|
                        json!({"enqueued":subscription.window.enqueued_sequence,
                            "pending":subscription.window.pending,"failures":subscription.failures,
                            "last_error":subscription.last_error}))})
            );
            panic!(
                "epic chain timeout stage={stage} id={id} status={:?} error={:?} wait={:?} inbox={inbox:?}",
                session.last_run_status(),
                session.last_run_error(),
                session
                    .agent_runtime_state
                    .as_ref()
                    .and_then(|runtime| runtime.waiting_for_children.as_ref())
            );
        }
    }
}
async fn quiet(state: &AppState, id: &str) -> Session {
    let result = tokio::time::timeout(WAIT, async {
        loop {
            let session = load(state, id).await;
            let inbox = state.session_inbox.inspect(id).await.unwrap();
            if session.last_run_status().as_deref() == Some("completed")
                && state
                    .session_activation_router
                    .current_run_id(id)
                    .await
                    .is_none()
                && inbox.pending == 0
                && inbox.claimed == 0
            {
                break session;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    match result {
        Ok(session) => session,
        Err(_) => panic!(
            "epic quiescence timeout id={id}; status={:?}; inbox={:?}; owner={:?}",
            load(state, id).await.last_run_status(),
            state.session_inbox.inspect(id).await.unwrap(),
            state.session_activation_router.current_run_id(id).await
        ),
    }
}
fn marker(session: &Session, text: &str) -> bool {
    session
        .messages
        .iter()
        .any(|message| message.content == text)
}
fn subscription(parent: &Session) -> bamboo_domain::ObserverSubscription {
    parent.supervisor_management.as_ref().unwrap().subscriptions[OBSERVER].clone()
}
fn context_identity(session: &Session) -> Value {
    json!({"id":session.id,"birth":session.created_at,"kind":session.kind,
        "root_session_id":session.root_session_id,"spawn_depth":session.spawn_depth,"parent":session.parent_session_id,
        "parent_birth":session.parent_created_at,"authority":session.authority_identity,
        "project":session.project_id_meta(),"workspace":session.workspace,"model_ref":session.model_ref,
        "provider":session.provider_name(),"permissions":session.agent_runtime_state.as_ref().map(|s|s.effective_permission_mode())})
}

#[tokio::test]
async fn two_project_tracker_planner_followup_and_restart_preserve_original_authority() {
    let home = tempfile::tempdir().unwrap();
    let workspace = home.path().join("supervisor-workspace");
    std::fs::create_dir(&workspace).unwrap();
    std::fs::write(workspace.join("source.txt"), "EPIC_SOURCE_EVIDENCE\n").unwrap();
    let probe = Arc::new(Probe::new(workspace.clone()));
    let state = host(home.path(), probe.clone()).await;
    let service = SupervisorSessionService::new(state.session_store.clone());
    let receipt = service.get_or_create_default(PARENT).await.unwrap();
    let reference = SupervisorReference::from(&receipt);
    let parent_id = &reference.session_id;
    let mut parent = load(&state, parent_id).await;
    parent.model_ref = Some(ProviderModelRef::new(PROVIDER, PARENT));
    parent.set_provider_name(PROVIDER);
    parent.workspace = Some(workspace.to_string_lossy().into());
    parent.set_workspace_path_meta(workspace.to_string_lossy());
    parent
        .agent_runtime_state
        .get_or_insert_with(Default::default)
        .set_permission_mode(SessionPermissionMode::Bypass);
    state.storage.save_session(&parent).await.unwrap();
    for (id, private) in [(ROOT_A, PRIVATE_A), (ROOT_B, PRIVATE_B)] {
        let project = state.project_store.create(id, None).unwrap();
        let directory = home.path().join(id);
        std::fs::create_dir(&directory).unwrap();
        let model = if id == ROOT_A { BLOCKED_MODEL } else { WORKER };
        let mut root = Session::new(id, model);
        root.set_project_id_meta(project.id.to_string());
        root.model_ref = Some(ProviderModelRef::new(PROVIDER, model));
        root.set_provider_name(PROVIDER);
        root.workspace = Some(directory.to_string_lossy().into());
        root.set_workspace_path_meta(directory.to_string_lossy());
        root.agent_runtime_state
            .get_or_insert_with(Default::default)
            .set_permission_mode(SessionPermissionMode::Bypass);
        root.add_message(Message::user(private));
        state.storage.save_session(&root).await.unwrap();
        if id == ROOT_A {
            let control_workspace = home.path().join(CONTROL_ROOT);
            std::fs::create_dir(&control_workspace).unwrap();
            let mut control = Session::new(CONTROL_ROOT, WORKER);
            control.set_project_id_meta(project.id.to_string());
            control.model_ref = Some(ProviderModelRef::new(PROVIDER, WORKER));
            control.set_provider_name(PROVIDER);
            control.workspace = Some(control_workspace.to_string_lossy().into());
            control.set_workspace_path_meta(control_workspace.to_string_lossy());
            control
                .agent_runtime_state
                .get_or_insert_with(Default::default)
                .set_permission_mode(SessionPermissionMode::Bypass);
            control.add_message(Message::user(PRIVATE_CONTROL));
            state.storage.save_session(&control).await.unwrap();
        }
    }
    let original_control = load(&state, CONTROL_ROOT).await;
    // Produce the blocker through the actual Inbox/Host/Actor execution route,
    // then its real tool call and runtime clarification checkpoint. The
    // question itself is never assigned into Session by the fixture.
    user(
        &state,
        ROOT_A,
        "Ask the existing owner which bounded approach is authorized; wait for their answer.",
    )
    .await;
    let paused = wait(
        &state,
        ROOT_A,
        "original-root-question",
        &probe,
        |session| {
            session.last_run_status().as_deref() == Some("suspended")
                && session
                    .pending_question
                    .as_ref()
                    .is_some_and(|question| question.tool_call_id == "epic-existing-owner-question")
        },
    )
    .await;
    tokio::time::timeout(WAIT, async {
        while state
            .session_activation_router
            .current_run_id(ROOT_A)
            .await
            .is_some()
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the actual blocked Root must release its execution owner before the snapshot");
    assert_eq!(paused.last_run_status().as_deref(), Some("suspended"));
    assert_eq!(probe.blocked_calls.load(Ordering::SeqCst), 1);
    // No configure_project_scope or attach call: Supervisor management is
    // required to cover this application's current user by default. The owner
    // must make the existing catalog/control/Observer registration resolver
    // consume that contract; this fixture cannot manufacture it. Tracker
    // registration below deliberately retains its explicit fixed target set.
    let original_a = load(&state, ROOT_A).await;
    assert_eq!(original_a.last_run_status().as_deref(), Some("suspended"));
    assert_eq!(
        serde_json::to_value(&original_a.pending_question).unwrap(),
        serde_json::to_value(&paused.pending_question).unwrap()
    );
    let original_b = load(&state, ROOT_B).await;
    let parent = load(&state, parent_id).await;
    let parent_identity = context_identity(&parent);
    let creator = state.supervisor_observer_creator();
    let (child_workspace, workspace_source) = creator
        .resolve_child_workspace(&parent, None)
        .await
        .unwrap();
    create_child_action(&creator, CreateChildInput { parent_session:parent, child_id:OBSERVER.into(),
        title:"Finite Epic tracker".into(), responsibility:"Observe only assigned bounded state".into(),
        assignment_prompt:"Read the supplied immutable views; report blockers without controlling either Root.".into(),
        subagent_type:"tracker".into(), workspace:child_workspace, workspace_source,
        model_override:None, model_ref_override:Some(ProviderModelRef::new(PROVIDER, TRACKER)),
        runtime_metadata:HashMap::new(), read_only:true, auto_run:false, reasoning_effort:None,
        lifecycle:None, resident_name:None, resident_context:None, disabled_tools:None, context_fork:None }).await.unwrap();
    let revision = state
        .storage
        .inspect_supervisor_scope(&reference)
        .await
        .unwrap()
        .state_revision;
    state
        .subscribe_supervisor_observer(super::observer_subscriptions::ObserverSubscriptionStart {
            supervisor: reference.clone(),
            expected_state_revision: revision,
            operation_id: Uuid::new_v4(),
            child_id: OBSERVER.into(),
            targets: BTreeSet::from([ROOT_A.into(), ROOT_B.into()]),
            filter: BTreeSet::from([ObserverChangeKind::Status, ObserverChangeKind::NeedsInput]),
            min_interval_ms: 1000,
        })
        .await
        .unwrap();
    tokio::time::timeout(WAIT, probe.tracker_entered.notified())
        .await
        .unwrap();
    user(&state, parent_id, USER).await;
    let grant = load(&state, parent_id)
        .await
        .supervisor_management
        .unwrap()
        .observers[OBSERVER]
        .target_links
        .keys()
        .cloned()
        .collect::<BTreeSet<_>>();
    assert_eq!(
        grant,
        BTreeSet::from([ROOT_A.to_string(), ROOT_B.to_string()]),
        "Tracker registration stays narrower than the Supervisor current-user catalog"
    );
    let idle = wait(
        &state,
        parent_id,
        "idle-parent-active-tracker",
        &probe,
        |s| {
            s.last_run_status().as_deref() == Some("completed")
                && marker(s, "EPIC_IDLE_PARENT_FINISHED_WITH_ACTIVE_TRACKER")
        },
    )
    .await;
    assert!(idle
        .agent_runtime_state
        .as_ref()
        .unwrap()
        .waiting_for_children
        .is_none());
    assert_eq!(probe.worker_calls.load(Ordering::SeqCst), 0);
    probe.tracker_release.notify_one();
    tokio::time::timeout(WAIT, probe.planner_entered.notified())
        .await
        .unwrap();
    let planning = wait(&state, parent_id, "planner-wait-registered", &probe, |s| {
        s.agent_runtime_state
            .as_ref()
            .is_some_and(|r| r.waiting_for_children.is_some())
    })
    .await;
    let planner_wait = planning
        .agent_runtime_state
        .as_ref()
        .unwrap()
        .waiting_for_children
        .clone();
    assert_eq!(probe.worker_calls.load(Ordering::SeqCst), 0);
    quiet(&state, OBSERVER).await;
    state
        .observer_subscriptions
        .tick(chrono::Utc::now() + chrono::Duration::seconds(2))
        .await;
    let during = wait(
        &state,
        parent_id,
        "report-during-planner-wait",
        &probe,
        |s| marker(s, DURING_PLAN),
    )
    .await;
    assert_eq!(
        during
            .agent_runtime_state
            .as_ref()
            .unwrap()
            .waiting_for_children,
        planner_wait
    );
    assert_eq!(probe.planner_calls.load(Ordering::SeqCst), 1);
    let export_denials = probe.denied_control_exports.lock().unwrap().clone();
    let read_denials = probe.denied_control_reads.lock().unwrap().clone();
    assert_eq!(export_denials, read_denials);
    assert!(
        read_denials.len() >= 2,
        "both source activations must reject the extra default-visible Root"
    );
    assert_eq!(
        *probe.observed.lock().unwrap(),
        BTreeSet::from([ROOT_A.into(), ROOT_B.into()])
    );
    assert_eq!(
        serde_json::to_value(load(&state, ROOT_A).await).unwrap(),
        serde_json::to_value(&original_a).unwrap()
    );
    assert_eq!(
        serde_json::to_value(load(&state, ROOT_B).await).unwrap(),
        serde_json::to_value(&original_b).unwrap()
    );
    probe.planner_release.notify_one();
    let finished = wait(
        &state,
        parent_id,
        "planner-artifact-followup",
        &probe,
        |s| s.last_run_status().as_deref() == Some("completed") && marker(s, DONE),
    )
    .await;
    let worker = wait(&state, ROOT_B, "original-worker-finished", &probe, |s| {
        s.last_run_status().as_deref() == Some("completed")
            && marker(s, "EPIC_ORIGINAL_WORKER_FINISHED")
    })
    .await;
    assert_eq!(probe.worker_calls.load(Ordering::SeqCst), 1);
    assert_eq!(probe.blocked_calls.load(Ordering::SeqCst), 1);
    assert_eq!(probe.planner_calls.load(Ordering::SeqCst), 3);
    assert_eq!(context_identity(&worker), context_identity(&original_b));
    assert!(!workspace.join("must-not-exist.txt").exists());
    assert!(finished
        .agent_runtime_state
        .as_ref()
        .unwrap()
        .waiting_for_children
        .is_none());
    let selector =
        artifact(&finished.messages).expect("canonical artifact reference")["read"].clone();
    let planner_id = selector["child_session_id"].as_str().unwrap();
    let actual_planner = load(&state, planner_id).await;
    assert!(
        actual_planner
            .agent_runtime_state
            .as_ref()
            .unwrap()
            .read_only
    );
    assert_eq!(
        actual_planner.parent_session_id.as_deref(),
        Some(parent_id.as_str())
    );
    let children: BTreeSet<_> = state
        .storage
        .list_child_run_statuses(parent_id)
        .await
        .unwrap()
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    assert_eq!(
        children,
        BTreeSet::from([OBSERVER.to_string(), planner_id.to_string()])
    );
    assert_eq!(context_identity(&finished), parent_identity);
    assert!(finished
        .agent_runtime_state
        .as_ref()
        .unwrap()
        .plan_mode
        .is_none());
    let incarnation = finished.supervisor_incarnation_id();
    // Reconcile the genuine worker run change before freezing the cursor. A
    // post-followup observation is meaningful; it is not a duplicate old event.
    for n in 1..=3 {
        state
            .observer_subscriptions
            .tick(chrono::Utc::now() + chrono::Duration::seconds(n * 3))
            .await;
        quiet(&state, OBSERVER).await;
        quiet(&state, parent_id).await;
    }
    let before_restart = load(&state, parent_id).await;
    let checkpoint = subscription(&before_restart);
    let calls = probe.requests.load(Ordering::SeqCst);
    let tracker_calls = probe.tracker_calls.load(Ordering::SeqCst);
    let first_receipt = tool_json(&before_restart.messages, "epic-plan-followup").unwrap();
    let first_followup = tool_arguments(&before_restart.messages, "epic-plan-followup");
    assert_eq!(
        first_followup["target_ref"],
        *probe.followup_target.lock().unwrap().as_ref().unwrap(),
        "the original worker selector comes from real discovery"
    );
    assert_eq!(first_receipt["admitted"], true);
    assert!(first_receipt["receipt_id"].as_str().is_some());
    assert!(first_receipt["generation"]
        .as_u64()
        .is_some_and(|generation| generation > 0));
    state.shutdown().await;
    drop(state);
    let reopened = host(home.path(), probe.clone()).await;
    crate::app_state::wake_reconciler::reconcile_startup(
        reopened.session_store.clone(),
        reopened.session_activation_router.clone(),
        reopened.mcp_proxy_shutdown.clone(),
    )
    .await;
    reopened
        .observer_subscriptions
        .tick(chrono::Utc::now() + chrono::Duration::minutes(1))
        .await;
    // Join the real producers and consumers before asserting counters. A
    // synchronous count immediately after scheduling could miss a late replay.
    quiet(&reopened, OBSERVER).await;
    let restored_worker = quiet(&reopened, ROOT_B).await;
    let restored = quiet(&reopened, parent_id).await;
    assert_eq!(probe.tracker_calls.load(Ordering::SeqCst), tracker_calls);
    assert_eq!(
        restored_worker
            .messages
            .iter()
            .filter(|message| message.content.contains(FOLLOWUP))
            .count(),
        1
    );
    assert_eq!(restored.supervisor_incarnation_id(), incarnation);
    assert_eq!(subscription(&restored), checkpoint);
    assert_eq!(
        serde_json::to_value(&restored.supervisor_management).unwrap(),
        serde_json::to_value(&before_restart.supervisor_management).unwrap()
    );
    assert_eq!(artifact(&restored.messages).unwrap()["read"], selector);
    assert_eq!(
        probe.requests.load(Ordering::SeqCst),
        calls,
        "startup must not replay acknowledged work"
    );
    user(&reopened, parent_id, COLD).await;
    wait(
        &reopened,
        parent_id,
        "cold-identical-followup-retry",
        &probe,
        |s| s.last_run_status().as_deref() == Some("completed") && marker(s, COLD_DONE),
    )
    .await;
    quiet(&reopened, OBSERVER).await;
    let final_worker = quiet(&reopened, ROOT_B).await;
    let cold = quiet(&reopened, parent_id).await;
    let cold_receipt = tool_json(&cold.messages, "epic-cold-followup").unwrap();
    assert_eq!(
        tool_arguments(&cold.messages, "epic-cold-followup"),
        first_followup,
        "cold retry preserves the exact target selection, operation and input"
    );
    assert_eq!(cold_receipt["receipt_id"], first_receipt["receipt_id"]);
    assert_eq!(cold_receipt["generation"], first_receipt["generation"]);
    assert_eq!(probe.tracker_calls.load(Ordering::SeqCst), tracker_calls);
    assert_eq!(
        final_worker
            .messages
            .iter()
            .filter(|message| message.content.contains(FOLLOWUP))
            .count(),
        1
    );
    assert!(final_worker.messages.len() >= original_b.messages.len());
    assert_eq!(
        serde_json::to_value(&final_worker.messages[..original_b.messages.len()]).unwrap(),
        serde_json::to_value(&original_b.messages).unwrap(),
        "original Root B history remains an exact prefix"
    );
    assert_eq!(artifact(&cold.messages).unwrap()["read"], selector);
    assert_eq!(context_identity(&cold), parent_identity);
    assert_eq!(probe.worker_calls.load(Ordering::SeqCst), 1);
    assert_eq!(probe.blocked_calls.load(Ordering::SeqCst), 1);
    assert_eq!(probe.planner_calls.load(Ordering::SeqCst), 3);
    assert_eq!(
        cold.messages.iter().filter(|m| m.content == USER).count(),
        1
    );
    assert_eq!(
        cold.messages.iter().filter(|m| m.content == COLD).count(),
        1
    );
    assert_eq!(
        context_identity(&load(&reopened, ROOT_B).await),
        context_identity(&original_b)
    );
    assert_eq!(
        serde_json::to_value(load(&reopened, ROOT_A).await).unwrap(),
        serde_json::to_value(original_a).unwrap()
    );
    assert!(cold
        .agent_runtime_state
        .as_ref()
        .unwrap()
        .waiting_for_children
        .is_none());
    assert_eq!(
        serde_json::to_value(load(&reopened, CONTROL_ROOT).await).unwrap(),
        serde_json::to_value(original_control).unwrap(),
        "the unassigned control Root is untouched"
    );
    reopened.shutdown().await;
}
