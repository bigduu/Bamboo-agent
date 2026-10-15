//! Tests-only child of workflow_run::tests. Install as
//! workflow_run/tests/running_await_window_tests.rs and add
//! `mod running_await_window_tests;` to tests.rs.
//!
//! Uses only APIs already present on 8da/98975df9. A real Parallel plan enters
//! branch B before A collapses a RunningHandle through the public old API.
//! A remains pending until B actually invokes another external action. Thus a
//! correct B refusal drops A; A cannot fail first and cancel B to mask the bug.

use super::*;

const RUNNING_A: &str = "running-await-window-a";
const TOOL_B: &str = "running-await-window-b";

#[derive(Clone, Copy, Debug)]
enum WaitingBranch {
    ToolAuthorization,
    AgentStructuredRetry,
}

#[derive(Default)]
struct AwaitWindowProbe {
    tool_calls: std::sync::Mutex<Vec<String>>,
    b_authorizations: AtomicUsize,
    b_authorization_waited: AtomicBool,
    b_authorization_returned: AtomicBool,
    agent_calls: AtomicUsize,
    first_agent_returned: AtomicBool,
    a_collapsed: std::sync::Mutex<Option<ToolResult>>,
    a_kills: AtomicUsize,
    a_drops: AtomicUsize,
    a_finished: AtomicBool,
    b_waiting: tokio::sync::Notify,
    a_collapsed_ready: tokio::sync::Notify,
    a_return_allowed: tokio::sync::Notify,
}

struct InvocationDrop(Arc<AwaitWindowProbe>);

impl Drop for InvocationDrop {
    fn drop(&mut self) {
        self.0.a_drops.fetch_add(1, Ordering::SeqCst);
    }
}

struct WindowTools(Arc<AwaitWindowProbe>);

#[async_trait]
impl ToolExecutor for WindowTools {
    async fn execute(&self, _: &ToolCall) -> Result<ToolResult, ToolError> {
        unreachable!("Workflow must dispatch through its real outcome-aware port")
    }

    async fn execute_with_context_outcome(
        &self,
        call: &ToolCall,
        _: ToolExecutionContext<'_>,
    ) -> Result<ToolOutcome, ToolError> {
        self.0
            .tool_calls
            .lock()
            .unwrap()
            .push(call.function.name.clone());
        match call.function.name.as_str() {
            RUNNING_A => {
                let _on_drop = InvocationDrop(self.0.clone());
                // Tool row: B has passed dispatch's entry check and is parked
                // in runtime authorization. Agent row: its first real attempt
                // has already entered the Agent port and is waiting to return.
                self.0.b_waiting.notified().await;
                let cleanup = self.0.clone();
                let collapsed = ToolOutcome::Running(RunningHandle {
                    tool_call_id: call.id.clone(),
                    ack: ToolResult::text(true, "ordinary Running acknowledgment"),
                    completion: RunningCompletion::Detached,
                    wait_kind: AsyncWaitKind::AsyncTools,
                    kill: Box::new(move || {
                        cleanup.a_kills.fetch_add(1, Ordering::SeqCst);
                    }),
                })
                .into_tool_result();
                *self.0.a_collapsed.lock().unwrap() = Some(collapsed);
                // This rendezvous is after the real collapse on old and new
                // source. It never writes the host's sticky token itself.
                self.0.a_collapsed_ready.notify_one();
                self.0.a_return_allowed.notified().await;
                self.0.a_finished.store(true, Ordering::SeqCst);
                // A deliberately conceals the collapsed value. A host-only
                // report, rather than this forged success, must govern B.
                Ok(ToolOutcome::Completed(ToolResult::text(
                    true,
                    r#"{"wrapped_running":true}"#,
                )))
            }
            TOOL_B => {
                assert!(self.0.b_authorization_returned.load(Ordering::SeqCst));
                assert!(self.0.a_collapsed.lock().unwrap().is_some());
                // Only an actual forbidden invocation releases A. With the fix
                // this branch is never reached; fail-fast drops A instead.
                self.0.a_return_allowed.notify_one();
                Ok(ToolOutcome::Completed(ToolResult::text(
                    true,
                    r#"{"external_b_ran":true}"#,
                )))
            }
            other => panic!("unexpected fixture tool {other}"),
        }
    }

    fn list_tools(&self) -> Vec<bamboo_agent_core::tools::ToolSchema> {
        Vec::new()
    }
}

struct RuntimeAuthorizationGate(Arc<AwaitWindowProbe>);

#[async_trait]
impl WorkflowPolicyPort for RuntimeAuthorizationGate {
    async fn authorize(
        &self,
        session_id: &str,
        target: &WorkflowPolicyTarget,
        requested: &BTreeSet<String>,
        trusted: bool,
    ) -> PermissionDecision {
        // Keep the existing concrete read/trust checks. No fake approval,
        // Supervisor identity, Session owner or capability is introduced.
        let decision = MockPolicy
            .authorize(session_id, target, requested, trusted)
            .await;
        if matches!(target, WorkflowPolicyTarget::Tool(name) if name == TOOL_B) {
            let visit = self.0.b_authorizations.fetch_add(1, Ordering::SeqCst);
            if visit == 1 {
                // The first visit is real preflight; the second is B's real
                // per-dispatch revalidation. Gate only that second visit.
                self.0.b_authorization_waited.store(true, Ordering::SeqCst);
                self.0.b_waiting.notify_one();
                self.0.a_collapsed_ready.notified().await;
                self.0
                    .b_authorization_returned
                    .store(true, Ordering::SeqCst);
            }
        }
        decision
    }
}

struct StructuredAttemptGate(Arc<AwaitWindowProbe>);

#[async_trait]
impl AgentStepPort for StructuredAttemptGate {
    async fn resolve(
        &self,
        name: &str,
        session_id: &str,
    ) -> Result<Option<NamedAgentSpec>, String> {
        MockAgents.resolve(name, session_id).await
    }

    async fn execute(
        &self,
        _: &NamedAgentSpec,
        _: Value,
        _: Option<&str>,
        _: Option<&str>,
        _: &BTreeSet<String>,
        _: &str,
        _: &str,
        _: tokio_util::sync::CancellationToken,
    ) -> Result<AgentStepResult, String> {
        let attempt = self.0.agent_calls.fetch_add(1, Ordering::SeqCst) + 1;
        let output = if attempt == 1 {
            self.0.b_waiting.notify_one();
            self.0.a_collapsed_ready.notified().await;
            self.0.first_agent_returned.store(true, Ordering::SeqCst);
            // A real measured result, rejected by the definition's output
            // schema. The existing structured-output loop would try again.
            json!("invalid structured output")
        } else {
            assert!(self.0.a_collapsed.lock().unwrap().is_some());
            self.0.a_return_allowed.notify_one();
            json!({"accepted":true})
        };
        Ok(AgentStepResult {
            output,
            tokens: 10,
            cost_micros: Some(2),
            attempt_id: None,
            failure: None,
        })
    }
}

async fn run_await_window(branch: WaitingBranch, restricted: bool) {
    let directory = tempfile::tempdir().unwrap();
    let repository =
        Arc::new(FileWorkflowRunRepository::new(directory.path().to_path_buf()).unwrap());
    let probe = Arc::new(AwaitWindowProbe::default());
    let agents: Arc<dyn AgentStepPort> = match branch {
        WaitingBranch::ToolAuthorization => Arc::new(MockAgents),
        WaitingBranch::AgentStructuredRetry => Arc::new(StructuredAttemptGate(probe.clone())),
    };
    let engine = WorkflowRunEngine::new(
        repository.clone(),
        Arc::new(WindowTools(probe.clone())),
        agents,
        Arc::new(MockDefinitions::default()),
        Arc::new(RuntimeAuthorizationGate(probe.clone())),
        Arc::new(MockSecrets),
        budgets(),
    );
    let mut a = tool_step("a", RUNNING_A, json!({}));
    a.failure = FailurePolicy::ContinueWithError;
    let b = match branch {
        WaitingBranch::ToolAuthorization => tool_step("b", TOOL_B, json!({})),
        WaitingBranch::AgentStructuredRetry => WorkflowStepDefinition {
            id: "b".into(),
            kind: WorkflowStepKind::Agent {
                agent: "reviewer".into(),
                prompt: json!({"request":"synthetic structured result"}),
                model: None,
                effort: None,
                capabilities: vec!["read".into()],
                structured_output_attempts: 2,
            },
            failure: FailurePolicy::FailFast,
            output_schema: Some(json!({
                "type":"object",
                "properties":{"accepted":{"type":"boolean"}},
                "required":["accepted"],
                "additionalProperties":false
            })),
        },
    };
    let flow = definition(
        vec![a, b],
        WorkflowPlan::Parallel {
            nodes: vec![
                WorkflowPlan::Step { step: "a".into() },
                WorkflowPlan::Step { step: "b".into() },
            ],
        },
    );
    let run = engine.run(request(flow, json!({})));
    let result = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        if restricted {
            bamboo_agent_core::tools::with_suspension_disabled(run).await
        } else {
            run.await
        }
    })
    .await
    .expect("the real rendezvous must finish or fail-fast drop A without sleep")
    .unwrap();

    // On 8da both rows cross the actual external boundary: tool calls [A,B]
    // or Agent calls=2. A returns successfully only because that later call
    // releases it. These are intended behavior REDs, not cancellation timing.
    match branch {
        WaitingBranch::ToolAuthorization => {
            assert_eq!(probe.b_authorizations.load(Ordering::SeqCst), 2);
            assert!(probe.b_authorization_waited.load(Ordering::SeqCst));
            assert!(probe.b_authorization_returned.load(Ordering::SeqCst));
            assert_eq!(
                probe.tool_calls.lock().unwrap().as_slice(),
                if restricted {
                    &[RUNNING_A][..]
                } else {
                    &[RUNNING_A, TOOL_B][..]
                }
            );
            assert_eq!(probe.agent_calls.load(Ordering::SeqCst), 0);
        }
        WaitingBranch::AgentStructuredRetry => {
            assert!(probe.first_agent_returned.load(Ordering::SeqCst));
            assert_eq!(probe.tool_calls.lock().unwrap().as_slice(), &[RUNNING_A]);
            assert_eq!(
                probe.agent_calls.load(Ordering::SeqCst),
                if restricted { 1 } else { 2 }
            );
            // Count actual returned usage, not conservative reserved attempts.
            assert_eq!(result.usage.tokens, if restricted { 10 } else { 20 });
            assert_eq!(
                result.usage.cost_micros,
                Some(if restricted { 2 } else { 4 })
            );
        }
    }
    let collapsed_success = probe.a_collapsed.lock().unwrap().as_ref().unwrap().success;
    assert_eq!(collapsed_success, !restricted);
    assert_eq!(
        probe.a_kills.load(Ordering::SeqCst),
        usize::from(restricted)
    );
    assert_eq!(probe.a_drops.load(Ordering::SeqCst), 1);
    assert_eq!(probe.a_finished.load(Ordering::SeqCst), !restricted);
    assert!(result.suspension.is_none());
    if restricted {
        assert_eq!(result.status, WorkflowRunStatus::Failed);
        assert_eq!(result.steps["b"].status, WorkflowStepStatus::Failed);
        let error = result.failure.as_ref().unwrap();
        assert_eq!(error.code, WorkflowFailureCode::ExecutionFailed);
        assert!(!error.retryable);
    } else {
        assert_eq!(result.status, WorkflowRunStatus::Succeeded);
        assert_eq!(result.steps["a"].status, WorkflowStepStatus::Succeeded);
        assert_eq!(result.steps["b"].status, WorkflowStepStatus::Succeeded);
    }
    assert_eq!(repository.load(&result.run_id).await.unwrap(), Some(result));
    assert_eq!(engine.runtime_resource_counts(), (0, 0));
}

#[tokio::test]
async fn restricted_parallel_running_blocks_tool_after_its_authorization_await() {
    run_await_window(WaitingBranch::ToolAuthorization, true).await;
}

#[tokio::test]
async fn restricted_parallel_running_blocks_next_structured_agent_attempt() {
    run_await_window(WaitingBranch::AgentStructuredRetry, true).await;
}

#[tokio::test]
async fn ordinary_parallel_running_wrapper_keeps_authorization_and_structured_retry_behavior() {
    for branch in [
        WaitingBranch::ToolAuthorization,
        WaitingBranch::AgentStructuredRetry,
    ] {
        run_await_window(branch, false).await;
    }
}
