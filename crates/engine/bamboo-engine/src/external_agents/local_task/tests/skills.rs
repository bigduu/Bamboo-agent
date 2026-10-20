use super::*;
use crate::runtime::config::SkillExecutionHost;
use bamboo_agent_core::tools::{FunctionCall, ToolExecutionContext, ToolOutcome};
use serde_json::json;
use std::collections::BTreeSet;

pub(in crate::external_agents::local_task) fn observe_bound(
    host: &Arc<skill_host::StrictChildSkills>,
) {
    if let Some(slot) = taps().lock().unwrap().get_mut(&host_session(host)) {
        *slot = Arc::downgrade(host);
    }
}
fn host_session(host: &skill_host::StrictChildSkills) -> String {
    host.session_id().to_owned()
}
fn taps() -> &'static Mutex<HashMap<String, std::sync::Weak<skill_host::StrictChildSkills>>> {
    static TAPS: std::sync::OnceLock<
        Mutex<HashMap<String, std::sync::Weak<skill_host::StrictChildSkills>>>,
    > = std::sync::OnceLock::new();
    TAPS.get_or_init(Mutex::default)
}
fn retain(id: &str) -> Arc<skill_host::StrictChildSkills> {
    taps()
        .lock()
        .unwrap()
        .remove(id)
        .unwrap()
        .upgrade()
        .expect("actual strict local execution binds its empty host")
}
const NAMES: &[&str] = &[
    "Read",
    "load_skill",
    "read_skill_resource",
    "skills_list",
    "skills_read",
];
#[derive(Default)]
struct ProofTools(AtomicUsize);
#[async_trait]
impl ToolExecutor for ProofTools {
    async fn execute(&self, _: &ToolCall) -> Result<ToolResult, ToolError> {
        panic!("strict local task must retain its context/fence wrapper")
    }
    async fn execute_with_context_outcome(
        &self,
        call: &ToolCall,
        ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolOutcome, ToolError> {
        assert_eq!(call.function.name, "Read");
        assert!(ctx.session_id.is_some() && ctx.tool_call_id == call.id);
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(ToolOutcome::Completed(ToolResult {
            success: true,
            result: "ACTUAL_ALLOWED_LOCAL_TASK".into(),
            display_preference: None,
            images: vec![],
        }))
    }
    fn list_tools(&self) -> Vec<ToolSchema> {
        NAMES.iter().map(|name|serde_json::from_value(json!({"type":"function","function":{"name":name,"description":"local proof","parameters":{"type":"object"}}})).unwrap()).collect()
    }
    fn exact_tool_owner(&self, name: &str) -> Option<&dyn ToolExecutor> {
        NAMES.contains(&name).then_some(self)
    }
}
struct ProofCeiling;
#[async_trait]
impl NativeToolCeilingSource for ProofCeiling {
    async fn observe(&self, _: &Session) -> Result<Vec<String>, String> {
        Ok(NAMES.iter().map(|n| (*n).into()).collect())
    }
}
struct SkillCall {
    messages: Vec<Message>,
    schemas: BTreeSet<String>,
    reply: oneshot::Sender<bool>,
}
struct SkillsProvider(mpsc::UnboundedSender<SkillCall>, AtomicUsize);
#[async_trait]
impl LLMProvider for SkillsProvider {
    async fn chat_stream(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        _: Option<u32>,
        _: &str,
    ) -> Result<LLMStream, LLMError> {
        let index = self.1.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = oneshot::channel();
        self.0
            .send(SkillCall {
                messages: messages.to_vec(),
                schemas: tools.iter().map(|t| t.function.name.clone()).collect(),
                reply: tx,
            })
            .unwrap();
        assert!(rx.await.unwrap(), "test releases actual provider");
        let chunks = if index == 0 {
            vec![
                LLMChunk::ToolCalls(vec![ToolCall {
                    id: "strict-real-read".into(),
                    tool_type: "function".into(),
                    function: FunctionCall {
                        name: "Read".into(),
                        arguments: "{}".into(),
                    },
                }]),
                LLMChunk::Done,
            ]
        } else {
            let result = messages
                .iter()
                .find(|m| m.tool_call_id.as_deref() == Some("strict-real-read"))
                .unwrap();
            assert_eq!(result.tool_success, Some(true));
            assert!(result.content.contains("ACTUAL_ALLOWED_LOCAL_TASK"));
            vec![
                LLMChunk::Token("Strict local task completed".into()),
                LLMChunk::Done,
            ]
        };
        Ok(Box::pin(futures::stream::iter(chunks.into_iter().map(Ok))))
    }
}
async fn strict_child(h: &Harness, suffix: &str) -> Session {
    let mut child = Session::new_child_of(
        format!("{}-{suffix}", h.parent.id),
        &h.parent,
        MODEL,
        suffix,
    );
    let packet = bamboo_domain::ChildContextPacket {
        version: 1,
        objective: "Perform the allowed local task".into(),
        constraints: vec!["Keep existing tool authority".into()],
        acceptance: vec!["Complete a normal task and answer".into()],
        non_goals: vec!["No Skill or Source access".into()],
        necessary_user_instructions: vec![],
        recorded_decisions: vec![],
        source_user_message_ids: vec![],
        background_message_ids: vec![],
    };
    let resolved = packet
        .resolve(&h.parent, "Perform the allowed local task")
        .unwrap();
    let binding = bamboo_domain::ChildContextBinding::new(
        &h.parent,
        &child.id,
        resolved.required_brief.clone(),
        resolved,
    )
    .unwrap();
    binding.install(&mut child).unwrap();
    child.add_message(Message::system("Complete the current task briefly."));
    child.add_message(binding.assignment_message());
    child.set_last_run_status("queued");
    child.metadata.insert(
        "skill_runtime_selected_skill_ids".into(),
        "[\"stale-legacy\"]".into(),
    );
    child
        .metadata
        .insert("skill_runtime_selection_source".into(), "explicit".into());
    child.metadata.insert(
        "skill_runtime_last_load_summary".into(),
        "STALE_SKILL_BYTES_MUST_STAY_INERT".into(),
    );
    h.store.save_session(&child).await.unwrap();
    child
}
async fn next(rx: &mut mpsc::UnboundedReceiver<SkillCall>) -> SkillCall {
    tokio::time::timeout(WAIT, rx.recv())
        .await
        .unwrap()
        .unwrap()
}
fn assert_empty_skills(call: &SkillCall) {
    assert_eq!(call.schemas, BTreeSet::from(["Read".into()]));
    assert!(call
        .messages
        .iter()
        .all(|m| !m.content.contains("STALE_SKILL_BYTES_MUST_STAY_INERT")
            && !m.content.contains("Required Explicit Workflow Activation")
            && !m.content.contains("Using Skills")));
}

#[tokio::test]
async fn strict_local_skills_real_actor_current_input_nonew_and_normal_task() {
    let home = tempfile::tempdir().unwrap();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let provider = Arc::new(SkillsProvider(tx, AtomicUsize::new(0)));
    let tools = Arc::new(ProofTools::default());
    let mut h = Harness::open_with_tools(
        home.path(),
        "strict-skills-root",
        provider,
        1,
        tools.clone(),
        Arc::new(ProofCeiling),
    )
    .await;
    let child = strict_child(&h, "task").await;
    taps()
        .lock()
        .unwrap()
        .insert(child.id.clone(), std::sync::Weak::new());
    let envelope =
        SessionMessageEnvelope::user_input(child.id.clone(), "ACTUAL_CURRENT_CHILD_INPUT");
    let id = envelope.id.clone();
    h.messenger
        .admit_with_activation_intent(
            envelope,
            bamboo_domain::SessionActivationPolicy::RespectSpecificWait,
            None,
        )
        .await
        .unwrap();
    h.launch(&child).await.unwrap();
    let first = next(&mut rx).await;
    assert_empty_skills(&first);
    assert!(first
        .messages
        .iter()
        .any(|m| m.content.contains("ACTUAL_CURRENT_CHILD_INPUT")));
    let host = retain(&child.id);
    let (seen, batches, execution) = host.observations();
    assert!(seen >= 1 && batches >= 1 && execution.is_some());
    let live = h.store.load_session(&child.id).await.unwrap().unwrap();
    assert_eq!(
        host.render_skill_prompt(&live, execution.as_deref().unwrap())
            .await
            .unwrap(),
        ""
    );
    first.reply.send(true).unwrap();
    let second = next(&mut rx).await;
    assert_empty_skills(&second);
    assert!(
        host.observations().0 >= 2,
        "NoNew round stays inside the same empty host"
    );
    second.reply.send(true).unwrap();
    let (completed, status, error) = h.completion().await;
    assert_eq!(completed, child.id);
    assert_eq!(status, "completed", "{error:?}");
    let saved = h.assert_terminal(&child, "completed").await;
    assert_eq!(tools.0.load(Ordering::SeqCst), 1);
    assert!(saved
        .messages
        .iter()
        .any(|m| m.role == Role::Assistant && m.content.contains("Strict local task completed")));
    assert!(h.inbox.was_admitted(&child.id, &id).await.unwrap());
    assert!(host
        .render_skill_prompt(&saved, execution.as_deref().unwrap())
        .await
        .is_err());
    h.runner.shutdown().await.unwrap();
}

#[tokio::test]
async fn strict_local_skills_cancelled_execution_cannot_reuse_retained_host() {
    let home = tempfile::tempdir().unwrap();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let provider = Arc::new(SkillsProvider(tx, AtomicUsize::new(0)));
    let mut h = Harness::open_with_tools(
        home.path(),
        "strict-cancel-root",
        provider,
        1,
        Arc::new(ProofTools::default()),
        Arc::new(ProofCeiling),
    )
    .await;
    let child = strict_child(&h, "cancel").await;
    taps()
        .lock()
        .unwrap()
        .insert(child.id.clone(), std::sync::Weak::new());
    h.launch(&child).await.unwrap();
    let call = next(&mut rx).await;
    assert_empty_skills(&call);
    let host = retain(&child.id);
    let execution = host.observations().2.unwrap();
    let live = h.store.load_session(&child.id).await.unwrap().unwrap();
    assert!(host
        .render_skill_prompt(&live, "forged-execution")
        .await
        .is_err());
    let mut forged = live.clone();
    forged.parent_created_at = Some(live.created_at);
    assert!(host.render_skill_prompt(&forged, &execution).await.is_err());
    let runner = h
        .ctx
        .agent_runners
        .read()
        .await
        .get(&child.id)
        .unwrap()
        .cancel_token
        .clone();
    runner.cancel();
    let (_, status, error) = h.completion().await;
    assert_eq!(status, "cancelled", "{error:?}");
    assert!(host.render_skill_prompt(&live, &execution).await.is_err());
    drop(call);
    h.assert_terminal(&child, "cancelled").await;
    h.runner.shutdown().await.unwrap();
}

#[tokio::test]
async fn nonstrict_local_child_does_not_receive_empty_skills_host() {
    let home = tempfile::tempdir().unwrap();
    let mut control = ProviderControl::new();
    let mut h = Harness::open(
        home.path(),
        "ordinary-local-root",
        control.provider.clone(),
        1,
    )
    .await;
    let child = h.child("ordinary", "ordinary local task").await;
    taps()
        .lock()
        .unwrap()
        .insert(child.id.clone(), std::sync::Weak::new());
    h.launch(&child).await.unwrap();
    let call = control.next().await;
    call.complete("ordinary child complete");
    let (_, status, error) = h.completion().await;
    assert_eq!(status, "completed", "{error:?}");
    assert!(taps()
        .lock()
        .unwrap()
        .remove(&child.id)
        .unwrap()
        .upgrade()
        .is_none());
    h.assert_terminal(&child, "completed").await;
    h.runner.shutdown().await.unwrap();
}
