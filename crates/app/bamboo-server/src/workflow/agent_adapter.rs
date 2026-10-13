//! Pinned read-only named profiles executed through the existing local Child route.
use std::collections::BTreeSet;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex, OnceLock,
};

use async_trait::async_trait;
use bamboo_agent_core::Session;
use bamboo_domain::{
    AdmissionCommit, AdmissionGate, ChildContextBinding, WorkflowFailure, WorkflowFailureCode,
};
use bamboo_engine::session_app::child_session::named_profile::{
    committed_child_activation, create_profile_child, PROFILE_EXPLICIT_MODEL_KEY,
};
use bamboo_engine::session_app::child_session::{ChildSessionPort, CreateChildInput};
use bamboo_engine::{AgentStepPort, AgentStepResult, NamedAgentSpec};
use bamboo_subagent::proto::{WorkflowAgentUsage, WORKFLOW_USAGE_REQUESTED_KEY};
use dashmap::DashMap;
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;

use crate::tools::ChildSessionAdapter;

pub(super) const READ_ONLY_AGENTS: &[&str] = &["explorer", "reviewer", "independent-reviewer"];

#[derive(Default)]
pub(super) struct WorkflowAgentAdapter {
    child: OnceLock<Arc<ChildSessionAdapter>>,
    attempts: Arc<DashMap<String, Arc<Attempt>>>,
}

struct Attempt {
    root_run_id: String,
    parent_id: String,
    child_id: String,
    cancellation: CancellationToken,
    admission: AdmissionGate,
    done: CancellationToken,
    result: Mutex<Option<Result<AgentStepResult, String>>>,
    stopped: AtomicBool,
}

struct CancelOnDrop(Arc<Attempt>);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.admission.cancel_if_pending();
        self.0.cancellation.cancel();
    }
}

impl WorkflowAgentAdapter {
    pub(super) fn bind(&self, child: Arc<ChildSessionAdapter>) -> Result<(), String> {
        self.child
            .set(child)
            .map_err(|_| "workflow Child adapter already bound".into())
    }

    async fn run_attempt(
        child: Arc<ChildSessionAdapter>,
        attempt: Arc<Attempt>,
        input: CreateChildInput,
        profile: bamboo_engine::session_app::child_session::named_profile::ResolvedChildProfile,
    ) -> Result<AgentStepResult, String> {
        let parent = input.parent_session.clone();
        if attempt.cancellation.is_cancelled() {
            attempt.stopped.store(true, Ordering::Release);
            return Ok(AgentStepResult {
                output: Value::Null,
                tokens: 0,
                cost_micros: None,
                failure: Some(agent_failure(
                    WorkflowFailureCode::Cancelled,
                    "workflow Agent cancelled before child creation",
                )),
                attempt_id: None,
            });
        }
        if create_profile_child(child.as_ref(), input, profile)
            .await
            .is_err()
        {
            // Creation publishes no queue job; no provider has been invoked.
            attempt.stopped.store(true, Ordering::Release);
            return Err("workflow Agent child creation failed".into());
        }
        let created = child
            .load_child_for_parent(&parent.id, &attempt.child_id)
            .await
            .map_err(|_| "workflow Agent child readback unavailable")?;
        let binding = ChildContextBinding::from_session(&created)
            .map_err(|_| "workflow Agent assignment binding invalid")?
            .ok_or("workflow Agent assignment binding missing")?;
        let admitted = tokio::select! {
            biased;
            _ = attempt.cancellation.cancelled() => {
                attempt.admission.cancel_if_pending();
                Ok(AdmissionCommit::Cancelled)
            }
            admitted = child.admit_workflow_child(&parent, &created, &attempt.admission) => admitted,
        };
        if !matches!(admitted, Ok(AdmissionCommit::Committed(()))) {
            attempt.cancellation.cancel();
        }
        loop {
            if attempt.cancellation.is_cancelled() {
                child
                    .cancel_workflow_child_and_wait(&attempt.child_id, &attempt.admission)
                    .await
                    .map_err(|_| "workflow Agent child stop unconfirmed")?;
                let current = child
                    .load_child_for_parent(&parent.id, &attempt.child_id)
                    .await
                    .map_err(|_| "workflow Agent cancelled child readback unavailable")?;
                attempt.stopped.store(true, Ordering::Release);
                return Self::observed_result(
                    child.as_ref(),
                    &current,
                    created.created_at,
                    attempt.admission.is_cancelled(),
                    Value::Null,
                    Some(agent_failure(
                        WorkflowFailureCode::Cancelled,
                        "workflow Agent cancelled",
                    )),
                )
                .await;
            }
            let current = child
                .load_child_for_parent(&parent.id, &attempt.child_id)
                .await
                .map_err(|_| "workflow Agent child readback unavailable")?;
            let status = current.last_run_status();
            if status.as_deref().is_some_and(|s| {
                matches!(
                    s,
                    "completed" | "error" | "timeout" | "cancelled" | "skipped"
                )
            }) && !child.is_child_running(&attempt.child_id).await
            {
                let (output, failure) = if status.as_deref() == Some("completed") {
                    let projected = bamboo_engine::session_app::child_session::inspect_child_report_action(
                        child.as_ref(), &parent.id, &attempt.child_id, "typed_result",
                        &json!({"expected_child_created_at":created.created_at,"expected_assignment_sha256":binding.assignment_sha256}),
                    ).await.map_err(|_| "workflow Agent report readback unavailable")?;
                    if projected["available"] == true {
                        (projected["child_report"].clone(), None)
                    } else {
                        (
                            Value::Null,
                            Some(agent_failure(
                                WorkflowFailureCode::InvalidOutput,
                                "workflow Agent typed report unavailable",
                            )),
                        )
                    }
                } else {
                    (
                        Value::Null,
                        Some(agent_failure(
                            WorkflowFailureCode::ExecutionFailed,
                            "workflow Agent child failed",
                        )),
                    )
                };
                if !child.workflow_child_stop_confirmed(&current).await {
                    return Err("workflow Agent child terminal stop unconfirmed".into());
                }
                attempt.stopped.store(true, Ordering::Release);
                return Self::observed_result(
                    child.as_ref(),
                    &current,
                    created.created_at,
                    false,
                    output,
                    failure,
                )
                .await;
            }
            tokio::select! {
                _ = attempt.cancellation.cancelled() => {},
                _ = tokio::time::sleep(std::time::Duration::from_millis(50)) => {},
            }
        }
    }

    fn observed_usage(child: &Session) -> Option<WorkflowAgentUsage> {
        let activation = committed_child_activation(child)?;
        if child
            .metadata
            .get(bamboo_subagent::proto::WORKFLOW_TERMINAL_OBSERVATION_KEY)
            != Some(&activation)
        {
            return None;
        }
        WorkflowAgentUsage::from_session(child, &activation)
    }

    async fn observed_partial_usage(
        port: &ChildSessionAdapter,
        child: &Session,
    ) -> Option<WorkflowAgentUsage> {
        let runners = port.agent_runners.read().await;
        match runners.get(&child.id) {
            None => Self::observed_usage(child),
            Some(runner)
                if !matches!(runner.status, bamboo_engine::AgentStatus::Running)
                    && child
                        .metadata
                        .get(bamboo_subagent::proto::WORKFLOW_TERMINAL_OBSERVATION_KEY)
                        == Some(&runner.run_id) =>
            {
                WorkflowAgentUsage::from_session(child, &runner.run_id)
            }
            _ => None,
        }
    }

    async fn observed_result(
        port: &ChildSessionAdapter,
        child: &Session,
        birth: chrono::DateTime<chrono::Utc>,
        never_admitted: bool,
        output: Value,
        failure: Option<WorkflowFailure>,
    ) -> Result<AgentStepResult, String> {
        if child.created_at != birth {
            return Err("workflow Agent child identity changed".into());
        }
        let tokens = if never_admitted {
            0
        } else {
            let usage = if failure.is_some() {
                Self::observed_partial_usage(port, child).await
            } else {
                Self::observed_usage(child)
            };
            usage.ok_or("workflow Agent cumulative token observation unavailable; usage is not measured")?.total_tokens()
        };
        Ok(AgentStepResult {
            output,
            tokens,
            cost_micros: None,
            failure,
            attempt_id: None,
        })
    }
}

#[async_trait]
impl AgentStepPort for WorkflowAgentAdapter {
    async fn resolve(
        &self,
        name: &str,
        session_id: &str,
    ) -> Result<Option<NamedAgentSpec>, String> {
        if !READ_ONLY_AGENTS.contains(&name) {
            return Ok(None);
        }
        let child = self.child.get().ok_or("workflow Child adapter not bound")?;
        let parent = child
            .storage
            .load_session(session_id)
            .await
            .map_err(|_| "workflow Agent parent unavailable")?
            .ok_or("workflow Agent parent missing")?;
        let profile = child
            .resolve_named_profile(&parent, name)
            .await
            .map_err(|_| "workflow Agent named profile unavailable")?;
        Ok(profile
            .filter(|profile| profile.read_only())
            .map(|profile| NamedAgentSpec {
                name: name.into(),
                allowed_capabilities: BTreeSet::from(["read".into()]),
                profile: Some(Arc::new(profile)),
                cost_supported: false,
            }))
    }

    async fn execute(
        &self,
        spec: &NamedAgentSpec,
        prompt: Value,
        model: Option<&str>,
        effort: Option<&str>,
        capabilities: &BTreeSet<String>,
        session_id: &str,
        root_run_id: &str,
        cancellation: CancellationToken,
    ) -> Result<AgentStepResult, String> {
        if !READ_ONLY_AGENTS.contains(&spec.name.as_str())
            || capabilities != &BTreeSet::from(["read".to_string()])
        {
            return Err("workflow Agent capabilities unsupported".into());
        }
        let profile = spec
            .profile
            .as_ref()
            .filter(|profile| profile.read_only())
            .ok_or("workflow Agent pinned profile unavailable")?
            .as_ref()
            .clone();
        let child = self
            .child
            .get()
            .ok_or("workflow Child adapter not bound")?
            .clone();
        let parent = child
            .storage
            .load_session(session_id)
            .await
            .map_err(|_| "workflow Agent parent unavailable")?
            .ok_or("workflow Agent parent missing")?;
        let workspace = bamboo_agent_core::workspace_state::ensure_session_workspace(
            session_id,
            parent.workspace.as_ref().map(std::path::PathBuf::from),
        )
        .ok_or("workflow Agent session workspace unavailable")?;
        let child_id = uuid::Uuid::new_v4().to_string();
        let mut metadata = child.resolve_runtime_metadata(&spec.name).await;
        metadata.insert(WORKFLOW_USAGE_REQUESTED_KEY.to_string(), "true".into());
        if model.is_some() {
            metadata.insert(PROFILE_EXPLICIT_MODEL_KEY.into(), "true".into());
        }
        let default_provider = child
            .config
            .read()
            .await
            .effective_default_provider()
            .to_string();
        let model_ref = model.map(|model| bamboo_domain::ProviderModelRef {
            provider: parent
                .model_ref
                .as_ref()
                .map(|m| m.provider.clone())
                .or_else(|| parent.provider_name())
                .unwrap_or_else(|| default_provider.clone()),
            model: model.into(),
            reasoning_effort: None,
        });
        let reasoning_effort = effort
            .map(|value| serde_json::from_value(json!(value)))
            .transpose()
            .map_err(|_| "workflow Agent reasoning effort invalid")?;
        let input = CreateChildInput {
            parent_session: parent,
            child_id: child_id.clone(),
            title: format!("Workflow: {}", spec.name),
            responsibility: "Workflow named-agent step".into(),
            assignment_prompt: prompt
                .as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| prompt.to_string()),
            subagent_type: spec.name.clone(),
            workspace: workspace.to_string_lossy().into_owned(),
            workspace_source: bamboo_engine::project_context::WorkspaceSource::Session,
            model_override: model.map(str::to_owned),
            model_ref_override: model_ref,
            runtime_metadata: metadata,
            read_only: true,
            auto_run: false,
            reasoning_effort,
            lifecycle: Some("oneshot".into()),
            resident_name: None,
            resident_context: None,
            disabled_tools: None,
            context_fork: None,
        };
        let attempt = Arc::new(Attempt {
            root_run_id: root_run_id.into(),
            parent_id: session_id.into(),
            child_id: child_id.clone(),
            cancellation,
            admission: AdmissionGate::default(),
            done: CancellationToken::new(),
            result: Mutex::new(None),
            stopped: AtomicBool::new(false),
        });
        self.attempts.insert(child_id.clone(), attempt.clone());
        let guard = CancelOnDrop(attempt.clone());
        let owned = attempt.clone();
        tokio::spawn(async move {
            let mut result = Self::run_attempt(child.clone(), owned.clone(), input, profile).await;
            if result.is_err() && !owned.stopped.load(Ordering::Acquire) {
                owned.cancellation.cancel();
                if child
                    .cancel_workflow_child_and_wait(&owned.child_id, &owned.admission)
                    .await
                    .is_ok()
                {
                    owned.stopped.store(true, Ordering::Release);
                    if let Ok(current) = child
                        .load_child_for_parent(&owned.parent_id, &owned.child_id)
                        .await
                    {
                        if let Some(usage) =
                            Self::observed_partial_usage(child.as_ref(), &current).await
                        {
                            result = Ok(AgentStepResult {
                                output: Value::Null,
                                tokens: usage.total_tokens(),
                                cost_micros: None,
                                attempt_id: None,
                                failure: Some(agent_failure(
                                    WorkflowFailureCode::ExecutionFailed,
                                    "workflow Agent child failed",
                                )),
                            });
                        }
                    }
                }
            }
            if let Ok(result) = &mut result {
                result.attempt_id = Some(owned.child_id.clone());
            }
            *owned.result.lock().unwrap() = Some(result);
            owned.done.cancel();
        });
        attempt.done.cancelled().await;
        if attempt.cancellation.is_cancelled() {
            // Cancellation drain owns usage even if the waiting execute future
            // wakes first, so an unavailable measurement cannot become Cancelled.
            return Err("workflow Agent cancelled; attempt awaits drain".into());
        }
        // Retain ownership until Engine commits usage. Dropping its waiting
        // future before that handoff leaves this result available to drain.
        let result = attempt
            .result
            .lock()
            .unwrap()
            .clone()
            .ok_or("workflow Agent result unavailable")?;
        // Drop cancels only this already-stopped attempt, never a later retry.
        drop(guard);
        result
    }

    fn acknowledge_result(&self, attempt_id: &str) -> bool {
        self.attempts
            .remove_if(attempt_id, |_, attempt| {
                attempt.done.is_cancelled() && attempt.stopped.load(Ordering::Acquire)
            })
            .is_some()
    }

    async fn drain_cancelled(
        &self,
        root_run_id: &str,
    ) -> (Vec<Result<AgentStepResult, String>>, Option<String>) {
        let attempts: Vec<_> = self
            .attempts
            .iter()
            .filter(|row| row.root_run_id == root_run_id && row.cancellation.is_cancelled())
            .map(|row| row.value().clone())
            .collect();
        let Some(child) = self.child.get() else {
            return (Vec::new(), None);
        };
        let mut usage = Vec::new();
        let mut error = None;
        for attempt in &attempts {
            attempt.done.cancelled().await;
            if !attempt.stopped.load(Ordering::Acquire)
                && child
                    .cancel_workflow_child_and_wait(&attempt.child_id, &attempt.admission)
                    .await
                    .is_ok()
            {
                attempt.stopped.store(true, Ordering::Release);
                if let Ok(current) = child
                    .load_child_for_parent(&attempt.parent_id, &attempt.child_id)
                    .await
                {
                    if let Some(observation) =
                        Self::observed_partial_usage(child.as_ref(), &current).await
                    {
                        *attempt.result.lock().unwrap() = Some(Ok(AgentStepResult {
                            output: Value::Null,
                            tokens: observation.total_tokens(),
                            cost_micros: None,
                            attempt_id: Some(attempt.child_id.clone()),
                            failure: Some(agent_failure(
                                WorkflowFailureCode::Cancelled,
                                "workflow Agent cancelled",
                            )),
                        }));
                    }
                }
            }
            if !attempt.stopped.load(Ordering::Acquire) {
                error = Some("workflow Agent child stop unconfirmed".into());
            }
        }
        // No await after ownership transfer: callers hold the usage ledger and
        // account every returned row in the same poll, even if later dropped.
        for attempt in attempts {
            if attempt.stopped.load(Ordering::Acquire) {
                // Competing run/cancel drains consume one attempt exactly once.
                if self.attempts.remove(&attempt.child_id).is_some() {
                    if let Some(result) = attempt.result.lock().unwrap().take() {
                        usage.push(result);
                    }
                }
            }
        }
        (usage, error)
    }
}

fn agent_failure(code: WorkflowFailureCode, message: &str) -> WorkflowFailure {
    WorkflowFailure {
        code,
        message: message.into(),
        retryable: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_agent_core::Message;

    use bamboo_llm::{LLMError, LLMProvider, LLMStream};

    struct NeverProvider;
    #[async_trait]
    impl LLMProvider for NeverProvider {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[bamboo_agent_core::tools::ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> Result<LLMStream, LLMError> {
            panic!("preflight must not invoke provider")
        }
    }

    fn profile(home: &std::path::Path, body: &str) {
        std::fs::create_dir_all(home.join("agents")).unwrap();
        std::fs::write(home.join("agents/reviewer.md"), format!("---\nschema_version: 1\nname: reviewer\ndescription: Bounded reviewer\ntools:\n  allow: [Read]\n---\n{body}\n")).unwrap();
    }

    async fn fixture() -> (tempfile::TempDir, crate::app_state::AppState, Session) {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().canonicalize().unwrap();
        let workspace = home.join("workspace");
        std::fs::create_dir(&workspace).unwrap();
        profile(&home, "PINNED_PRIVATE_PROFILE");
        let skill = home.join("skills/agent-flow");
        std::fs::create_dir_all(&skill).unwrap();
        std::fs::write(
            skill.join("SKILL.md"),
            "---\nname: agent-flow\ndescription: Bounded agent flow\n---\nRead review.\n",
        )
        .unwrap();
        std::fs::write(skill.join("workflow.yaml"), "workflow_schema: 1\nid: agent-flow\nrevision: 1\ninvocation_policy: {explicit: true, automatic: false}\ninput_schema: {type: object, additionalProperties: true}\nsteps:\n  - id: review\n    type: agent\n    agent: reviewer\n    prompt: {from: literal, value: Read assigned file}\n    capabilities: [read]\nplan: {type: step, step: review}\nbudgets:\n  max_concurrency: 1\n  max_agents: 1\n  max_steps: 4\n  max_retries: 0\n  max_nesting_depth: 1\n  wall_time_ms: 10000\n  max_tokens: 1000\n  max_cost_micros: 10\n").unwrap();
        let state = crate::app_state::AppState::new_with_provider(
            home,
            serde_json::from_value(
                json!({"subagents":{"runtime":"actor","executor":"bamboo_runtime"}}),
            )
            .unwrap(),
            Arc::new(NeverProvider),
        )
        .await
        .unwrap();
        let mut parent = Session::new("workflow-agent-parent", "model");
        parent.workspace = Some(workspace.to_string_lossy().into_owned());
        state.storage.save_session(&parent).await.unwrap();
        (temp, state, parent)
    }

    #[tokio::test]
    async fn workflow_agent_production_start_rejects_money_before_child_or_provider() {
        let (_temp, state, parent) = fixture().await;
        let result = state
            .workflow_runs
            .start(&parent.id, "agent-flow", 1, json!({}), None)
            .await;
        assert!(
            matches!(
                result,
                Err(bamboo_engine::WorkflowRunError::UnsupportedMonetaryBudget)
            ),
            "{result:?}"
        );
        let child = state.workflow_runs.agents.child.get().unwrap();
        assert!(child.list_children(&parent.id).await.is_empty());
        assert!(state
            .workflow_runs
            .engine
            .list_run_ids()
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn workflow_agent_resolve_pins_profile_before_live_catalog_change() {
        let (_temp, state, parent) = fixture().await;
        let adapter = &state.workflow_runs.agents;
        let selected = adapter
            .resolve("reviewer", &parent.id)
            .await
            .unwrap()
            .unwrap();
        let pinned = selected.profile.clone().unwrap();
        profile(&state.app_data_dir, "REPLACEMENT_PRIVATE_PROFILE");
        let reloaded = adapter
            .resolve("reviewer", &parent.id)
            .await
            .unwrap()
            .unwrap();
        assert_ne!(pinned, reloaded.profile.unwrap());
        assert_eq!(selected.profile.unwrap(), pinned);
        // Actual Child creation requires the native worker capability handshake
        // and is covered by the production serve/subagent-worker fixture.
    }

    #[tokio::test]
    async fn workflow_agent_empty_capabilities_do_not_create_child() {
        let (_temp, state, parent) = fixture().await;
        let adapter = &state.workflow_runs.agents;
        let selected = adapter
            .resolve("reviewer", &parent.id)
            .await
            .unwrap()
            .unwrap();
        assert!(adapter
            .execute(
                &selected,
                json!("Read"),
                None,
                None,
                &BTreeSet::new(),
                &parent.id,
                "workflow-empty-caps",
                CancellationToken::new()
            )
            .await
            .is_err());
        assert!(adapter
            .child
            .get()
            .unwrap()
            .list_children(&parent.id)
            .await
            .is_empty());
    }

    #[tokio::test]
    async fn workflow_agent_missing_usage_is_distinct_from_physical_stop() {
        let (_temp, state, parent) = fixture().await;
        let port = state.workflow_runs.agents.child.get().unwrap();
        let mut child = Session::new_child_of("child", &parent, "model", "reviewer");
        child.agent_runtime_state = Some(bamboo_domain::AgentRuntimeState::new(""));
        child.set_last_run_status("completed");
        let mut runner = bamboo_engine::AgentRunner::new();
        runner.run_id = "activation".into();
        runner.status = bamboo_engine::AgentStatus::Completed;
        port.agent_runners
            .write()
            .await
            .insert(child.id.clone(), runner);
        assert!(!port.workflow_child_stop_confirmed(&child).await);
        child.metadata.insert(
            bamboo_subagent::proto::WORKFLOW_TERMINAL_OBSERVATION_KEY.into(),
            "activation".into(),
        );
        assert!(port.workflow_child_stop_confirmed(&child).await);
        let result = WorkflowAgentAdapter::observed_result(
            port.as_ref(),
            &child,
            child.created_at,
            false,
            json!({}),
            None,
        )
        .await;
        assert!(result.unwrap_err().contains("usage is not measured"));
        child.set_last_run_status("error");
        child.metadata.insert(bamboo_subagent::proto::WORKFLOW_USAGE_OBSERVATION_KEY.into(), json!({"type":"workflow_agent_usage","activation_run_id":"activation","child_session_id":child.id,"child_created_at":child.created_at,"prompt_tokens":11,"completion_tokens":7}).to_string());
        let partial = WorkflowAgentAdapter::observed_partial_usage(port.as_ref(), &child)
            .await
            .unwrap();
        assert_eq!(partial.total_tokens(), 18);
        child.metadata.insert(
            bamboo_subagent::proto::WORKFLOW_TERMINAL_OBSERVATION_KEY.into(),
            "old-activation".into(),
        );
        assert!(!port.workflow_child_stop_confirmed(&child).await);
        port.agent_runners.write().await.remove(&child.id);
        assert!(!port.workflow_child_stop_confirmed(&child).await);
        assert!(
            WorkflowAgentAdapter::observed_partial_usage(port.as_ref(), &child)
                .await
                .is_none()
        );
    }
    #[tokio::test]
    async fn workflow_agent_precancel_has_known_zero_usage_and_no_child() {
        let (_temp, state, parent) = fixture().await;
        let adapter = &state.workflow_runs.agents;
        let selected = adapter
            .resolve("reviewer", &parent.id)
            .await
            .unwrap()
            .unwrap();
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        assert!(adapter
            .execute(
                &selected,
                json!("Read"),
                None,
                None,
                &BTreeSet::from(["read".into()]),
                &parent.id,
                "pre-cancel",
                cancellation
            )
            .await
            .is_err());
        let (mut results, stop_error) = adapter.drain_cancelled("pre-cancel").await;
        assert!(stop_error.is_none());
        assert_eq!(results.len(), 1);
        let result = results.remove(0).unwrap();
        assert_eq!(result.tokens, 0);
        assert_eq!(result.failure.unwrap().code, WorkflowFailureCode::Cancelled);
        assert!(adapter
            .child
            .get()
            .unwrap()
            .list_children(&parent.id)
            .await
            .is_empty());
    }

    #[tokio::test]
    async fn workflow_agent_runner_absence_does_not_confirm_activated_child_stop() {
        let (_temp, state, parent) = fixture().await;
        let child_port = state.workflow_runs.agents.child.get().unwrap();
        let mut child =
            Session::new_child_of("activated-without-terminal", &parent, "model", "reviewer");
        child
            .metadata
            .insert(WORKFLOW_USAGE_REQUESTED_KEY.into(), "true".into());
        child.agent_runtime_state = Some(bamboo_domain::AgentRuntimeState::new(""));
        child.set_last_run_status("running");
        state.storage.save_session(&child).await.unwrap();
        assert!(child_port
            .cancel_child_run_and_wait(&child.id)
            .await
            .is_err());
        child.metadata.insert(
            bamboo_subagent::proto::WORKFLOW_TERMINAL_OBSERVATION_KEY.into(),
            "activation".into(),
        );
        state.storage.save_session(&child).await.unwrap();
        assert!(child_port
            .cancel_child_run_and_wait(&child.id)
            .await
            .is_err());
        let mut runner = bamboo_engine::AgentRunner::new();
        runner.run_id = "activation".into();
        runner.status = bamboo_engine::AgentStatus::Error("final save failed".into());
        child_port
            .agent_runners
            .write()
            .await
            .insert(child.id.clone(), runner);
        child_port
            .cancel_child_run_and_wait(&child.id)
            .await
            .unwrap();
    }
}
