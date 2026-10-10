use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bamboo_agent_core::tools::{
    AsyncWaitKind, RunningCompletion, RunningHandle, ToolCall, ToolError, ToolExecutionContext,
    ToolExecutionSessionFlags, ToolExecutor, ToolOutcome, ToolResult,
};
use bamboo_domain::*;
use chrono::Utc;
use serde_json::{json, Value};
use tokio::io::AsyncWriteExt;

use super::*;

fn budgets() -> WorkflowBudgets {
    WorkflowBudgets {
        max_concurrency: 4,
        max_agents: 4,
        max_steps: 64,
        max_retries: 4,
        max_nesting_depth: 4,
        wall_time_ms: 10_000,
        max_tokens: Some(10_000),
        max_cost_micros: Some(10_000),
    }
}

fn tool_step(id: &str, tool: &str, args: Value) -> WorkflowStepDefinition {
    WorkflowStepDefinition {
        id: id.to_string(),
        kind: WorkflowStepKind::Tool {
            tool: tool.to_string(),
            args,
            capabilities: vec!["read".to_string()],
        },
        failure: FailurePolicy::FailFast,
        output_schema: None,
    }
}

fn definition(steps: Vec<WorkflowStepDefinition>, plan: WorkflowPlan) -> WorkflowRunDefinition {
    WorkflowRunDefinition {
        workflow_schema: 1,
        id: "review".to_string(),
        revision: 7,
        input_schema: json!({"type":"object","properties":{"items":{"type":"array","items":{"type":"integer"}}},"additionalProperties":true}),
        output_schema: None,
        steps,
        plan,
        budgets: budgets(),
    }
}

fn snapshot(run_id: &str, status: WorkflowRunStatus, sequence: u64) -> WorkflowRunSnapshot {
    let now = Utc::now();
    let definition = definition(
        vec![tool_step("echo", "echo", json!({}))],
        WorkflowPlan::Step {
            step: "echo".to_string(),
        },
    );
    let definition_bundle = WorkflowDefinitionBundle {
        publication_revision: 1,
        root_id: definition.id.clone(),
        root_revision: definition.revision,
        root_invocation_policy: json!({"explicit": true, "automatic": true}),
        definitions: BTreeMap::from([(
            WorkflowDefinitionBundle::key(&definition.id, definition.revision),
            definition.clone(),
        )]),
    };
    WorkflowRunSnapshot {
        run_id: run_id.to_string(),
        parent_run_id: None,
        parent_step_id: None,
        session_id: "session-1".to_string(),
        definition,
        definition_bundle,
        definition_bundle_hash: "test-bundle-hash".to_string(),
        validated_args: json!({}),
        status,
        steps: BTreeMap::new(),
        usage: WorkflowBudgetUsage::default(),
        last_sequence: sequence,
        output: None,
        failure: None,
        suspension: None,
        created_at: now,
        updated_at: now,
    }
}

fn run_event(run_id: &str, sequence: u64, kind: WorkflowRunEventKind) -> WorkflowRunEvent {
    WorkflowRunEvent {
        run_id: run_id.to_string(),
        sequence,
        at: Utc::now(),
        step_id: None,
        kind,
    }
}

#[tokio::test]
async fn repository_promotes_journal_committed_staged_snapshot() {
    let directory = tempfile::tempdir().unwrap();
    let repository = FileWorkflowRunRepository::new(directory.path().to_path_buf()).unwrap();
    let first = snapshot("run-1", WorkflowRunStatus::Queued, 1);
    repository
        .create(
            &first,
            &run_event("run-1", 1, WorkflowRunEventKind::RunQueued),
        )
        .await
        .unwrap();

    let second = snapshot("run-1", WorkflowRunStatus::Running, 2);
    let event = run_event("run-1", 2, WorkflowRunEventKind::RunStarted);
    let run_dir = directory.path().join("run-1");
    tokio::fs::write(
        run_dir.join(".snapshot-2.tmp"),
        serde_json::to_vec(&second).unwrap(),
    )
    .await
    .unwrap();
    let mut journal = tokio::fs::OpenOptions::new()
        .append(true)
        .open(run_dir.join("journal.jsonl"))
        .await
        .unwrap();
    journal
        .write_all(&serde_json::to_vec(&event).unwrap())
        .await
        .unwrap();
    journal.write_all(b"\n").await.unwrap();
    journal.sync_all().await.unwrap();

    let recovered = repository.load("run-1").await.unwrap().unwrap();
    assert_eq!(recovered.status, WorkflowRunStatus::Running);
    assert_eq!(recovered.last_sequence, 2);
    assert!(!run_dir.join(".snapshot-2.tmp").exists());
}

#[tokio::test]
async fn repository_truncates_torn_tail_before_next_commit() {
    let directory = tempfile::tempdir().unwrap();
    let repository = FileWorkflowRunRepository::new(directory.path().to_path_buf()).unwrap();
    let first = snapshot("run-2", WorkflowRunStatus::Queued, 1);
    repository
        .create(
            &first,
            &run_event("run-2", 1, WorkflowRunEventKind::RunQueued),
        )
        .await
        .unwrap();
    let journal_path = directory.path().join("run-2/journal.jsonl");
    let mut journal = tokio::fs::OpenOptions::new()
        .append(true)
        .open(&journal_path)
        .await
        .unwrap();
    journal.write_all(br#"{"partial":"#).await.unwrap();
    journal.sync_all().await.unwrap();
    repository.load("run-2").await.unwrap();

    let second = snapshot("run-2", WorkflowRunStatus::Running, 2);
    repository
        .commit(
            &second,
            &run_event("run-2", 2, WorkflowRunEventKind::RunStarted),
        )
        .await
        .unwrap();
    let events = repository.events_since("run-2", 0).await.unwrap();
    assert_eq!(
        events
            .iter()
            .map(|event| event.sequence)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
}

#[tokio::test]
async fn repository_retries_create_after_empty_torn_attempt_and_replaces_snapshot() {
    let directory = tempfile::tempdir().unwrap();
    let repository = FileWorkflowRunRepository::new(directory.path().to_path_buf()).unwrap();
    let run_dir = directory.path().join("run-3");
    tokio::fs::create_dir(&run_dir).await.unwrap();
    tokio::fs::write(run_dir.join("journal.jsonl"), b"torn")
        .await
        .unwrap();
    let first = snapshot("run-3", WorkflowRunStatus::Queued, 1);
    repository
        .create(
            &first,
            &run_event("run-3", 1, WorkflowRunEventKind::RunQueued),
        )
        .await
        .unwrap();
    let second = snapshot("run-3", WorkflowRunStatus::Running, 2);
    repository
        .commit(
            &second,
            &run_event("run-3", 2, WorkflowRunEventKind::RunStarted),
        )
        .await
        .unwrap();
    let third = snapshot("run-3", WorkflowRunStatus::Suspended, 3);
    repository
        .commit(
            &third,
            &run_event(
                "run-3",
                3,
                WorkflowRunEventKind::RunSuspended {
                    reason: "restart".to_string(),
                },
            ),
        )
        .await
        .unwrap();
    assert_eq!(
        repository
            .load("run-3")
            .await
            .unwrap()
            .unwrap()
            .last_sequence,
        3
    );
}

#[tokio::test]
async fn repository_same_run_commit_lock_is_atomic_for_100_races() {
    let directory = tempfile::tempdir().unwrap();
    let repository =
        Arc::new(FileWorkflowRunRepository::new(directory.path().to_path_buf()).unwrap());
    for round in 0..100 {
        let run_id = format!("commit-race-{round}");
        let queued = snapshot(&run_id, WorkflowRunStatus::Queued, 1);
        repository
            .create(
                &queued,
                &run_event(&run_id, 1, WorkflowRunEventKind::RunQueued),
            )
            .await
            .unwrap();
        let mut left_snapshot = snapshot(&run_id, WorkflowRunStatus::Running, 2);
        left_snapshot.validated_args = json!({"winner": "left"});
        let mut right_snapshot = snapshot(&run_id, WorkflowRunStatus::Running, 2);
        right_snapshot.validated_args = json!({"winner": "right"});
        let left = {
            let repository = repository.clone();
            let running = left_snapshot;
            let run_id = run_id.clone();
            tokio::spawn(async move {
                repository
                    .commit(
                        &running,
                        &run_event(&run_id, 2, WorkflowRunEventKind::RunStarted),
                    )
                    .await
            })
        };
        let right = {
            let repository = repository.clone();
            let running = right_snapshot;
            let run_id = run_id.clone();
            tokio::spawn(async move {
                repository
                    .commit(
                        &running,
                        &run_event(&run_id, 2, WorkflowRunEventKind::RunStarted),
                    )
                    .await
            })
        };
        let (left, right) = tokio::join!(left, right);
        let successes = [left.unwrap(), right.unwrap()]
            .into_iter()
            .filter(Result::is_ok)
            .count();
        assert_eq!(successes, 1, "round {round}");
        let loaded = repository.load(&run_id).await.unwrap().unwrap();
        let events = repository.events_since(&run_id, 0).await.unwrap();
        assert_eq!(loaded.last_sequence, 2);
        assert_eq!(events.len(), 2);
        assert_eq!(events[1].sequence, 2);
    }
}

#[test]
fn compiler_rejects_unknown_schema_keywords_duplicate_plan_and_parallel_cycle() {
    let mut bad_schema = definition(
        vec![tool_step("one", "echo", json!({}))],
        WorkflowPlan::Step {
            step: "one".to_string(),
        },
    );
    bad_schema.input_schema = json!({"type":"object","patternProperties":{}});
    assert!(matches!(
        CompiledWorkflow::compile(bad_schema),
        Err(WorkflowCompileError::InvalidSchema(_))
    ));

    let duplicate = definition(
        vec![tool_step("one", "echo", json!({}))],
        WorkflowPlan::Sequence {
            nodes: vec![
                WorkflowPlan::Step {
                    step: "one".to_string(),
                },
                WorkflowPlan::Step {
                    step: "one".to_string(),
                },
            ],
        },
    );
    assert!(CompiledWorkflow::compile(duplicate).is_err());

    let cycle = definition(
        vec![
            tool_step("a", "echo", json!({"from":"step","step":"c","pointer":""})),
            tool_step("b", "echo", json!({})),
            tool_step("c", "echo", json!({})),
        ],
        WorkflowPlan::Sequence {
            nodes: vec![
                WorkflowPlan::Parallel {
                    nodes: vec![
                        WorkflowPlan::Step {
                            step: "a".to_string(),
                        },
                        WorkflowPlan::Step {
                            step: "b".to_string(),
                        },
                    ],
                },
                WorkflowPlan::Step {
                    step: "c".to_string(),
                },
            ],
        },
    );
    assert!(matches!(
        CompiledWorkflow::compile(cycle),
        Err(WorkflowCompileError::Cycle(_))
    ));
}

#[test]
fn compiler_enforces_execution_order_and_typed_value_reference_scopes() {
    let mut left = tool_step("left", "echo", json!({}));
    left.output_schema = Some(json!({
        "type":"object",
        "properties":{"value":{"type":"string"}},
        "required":["value"],
        "additionalProperties":false
    }));
    let parallel_sibling = definition(
        vec![
            left.clone(),
            tool_step(
                "right",
                "echo",
                json!({"from":"step","step":"left","pointer":"/value"}),
            ),
        ],
        WorkflowPlan::Parallel {
            nodes: vec![
                WorkflowPlan::Step {
                    step: "left".to_string(),
                },
                WorkflowPlan::Step {
                    step: "right".to_string(),
                },
            ],
        },
    );
    assert!(matches!(
        CompiledWorkflow::compile(parallel_sibling),
        Err(WorkflowCompileError::InvalidStep { step, .. }) if step == "right"
    ));

    let bad_args_pointer = definition(
        vec![tool_step(
            "bad-args",
            "echo",
            json!({"from":"args","pointer":"/missing"}),
        )],
        WorkflowPlan::Step {
            step: "bad-args".to_string(),
        },
    );
    assert!(CompiledWorkflow::compile(bad_args_pointer).is_err());

    let item_outside_map = definition(
        vec![tool_step(
            "bad-item",
            "echo",
            json!({"from":"item","name":"row","pointer":""}),
        )],
        WorkflowPlan::Step {
            step: "bad-item".to_string(),
        },
    );
    assert!(CompiledWorkflow::compile(item_outside_map).is_err());

    let non_array_map = definition(
        vec![tool_step("mapped", "echo", json!({}))],
        WorkflowPlan::Map {
            source: ValueRef::Args {
                pointer: "/missing".to_string(),
            },
            item: "row".to_string(),
            body: Box::new(WorkflowPlan::Step {
                step: "mapped".to_string(),
            }),
        },
    );
    assert!(CompiledWorkflow::compile(non_array_map).is_err());

    let mut future = tool_step("future", "echo", json!({}));
    future.output_schema = Some(json!({
        "type":"array",
        "items":{"type":"integer"}
    }));
    let future_map_source = definition(
        vec![
            tool_step(
                "mapped",
                "echo",
                json!({"from":"item","name":"row","pointer":""}),
            ),
            future,
        ],
        WorkflowPlan::Sequence {
            nodes: vec![
                WorkflowPlan::Map {
                    source: ValueRef::Step {
                        step: "future".to_string(),
                        pointer: "".to_string(),
                    },
                    item: "row".to_string(),
                    body: Box::new(WorkflowPlan::Step {
                        step: "mapped".to_string(),
                    }),
                },
                WorkflowPlan::Step {
                    step: "future".to_string(),
                },
            ],
        },
    );
    assert!(CompiledWorkflow::compile(future_map_source).is_err());

    let valid_map = definition(
        vec![tool_step(
            "mapped",
            "echo",
            json!({"from":"item","name":"row","pointer":""}),
        )],
        WorkflowPlan::Map {
            source: ValueRef::Args {
                pointer: "/items".to_string(),
            },
            item: "row".to_string(),
            body: Box::new(WorkflowPlan::Step {
                step: "mapped".to_string(),
            }),
        },
    );
    CompiledWorkflow::compile(valid_map).expect("valid map item binding");

    let map_body_output_outside_map = definition(
        vec![
            tool_step(
                "mapped",
                "echo",
                json!({"from":"item","name":"row","pointer":""}),
            ),
            tool_step(
                "after-map",
                "echo",
                json!({"from":"step","step":"mapped","pointer":""}),
            ),
        ],
        WorkflowPlan::Sequence {
            nodes: vec![
                WorkflowPlan::Map {
                    source: ValueRef::Args {
                        pointer: "/items".to_string(),
                    },
                    item: "row".to_string(),
                    body: Box::new(WorkflowPlan::Step {
                        step: "mapped".to_string(),
                    }),
                },
                WorkflowPlan::Step {
                    step: "after-map".to_string(),
                },
            ],
        },
    );
    assert!(matches!(
        CompiledWorkflow::compile(map_body_output_outside_map),
        Err(WorkflowCompileError::InvalidStep { step, .. }) if step == "after-map"
    ));
}

struct MockTools;

#[async_trait]
impl ToolExecutor for MockTools {
    async fn execute(&self, call: &ToolCall) -> Result<ToolResult, ToolError> {
        let args: Value = serde_json::from_str(&call.function.arguments).unwrap();
        match call.function.name.as_str() {
            "echo" => Ok(ToolResult::text(
                true,
                serde_json::to_string(&args).unwrap(),
            )),
            "flaky" => Err(ToolError::Execution("retry me".to_string())),
            "fail" => Ok(ToolResult::text(false, "expected failure")),
            "slow" => {
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                Ok(ToolResult::text(true, "{}"))
            }
            "secretout" => Ok(ToolResult::text(true, r#"{"token":"raw-secret"}"#)),
            "baresecret" => Ok(ToolResult::text(true, r#""sk-raw-secret""#)),
            "secretfail" => Ok(ToolResult::text(false, "api_key=raw-secret")),
            _ => Err(ToolError::NotFound(call.function.name.clone())),
        }
    }

    fn list_tools(&self) -> Vec<bamboo_agent_core::tools::ToolSchema> {
        Vec::new()
    }
}

struct StaticSessionPermissions(ToolExecutionSessionFlags);

#[async_trait]
impl WorkflowSessionPermissionPort for StaticSessionPermissions {
    async fn flags_for_session(
        &self,
        _session_id: &str,
    ) -> Result<ToolExecutionSessionFlags, String> {
        Ok(self.0)
    }
}

#[derive(Default)]
struct ContextRecordingTools {
    calls: AtomicUsize,
    flags: std::sync::Mutex<Vec<ToolExecutionSessionFlags>>,
}

#[async_trait]
impl ToolExecutor for ContextRecordingTools {
    async fn execute(&self, _call: &ToolCall) -> Result<ToolResult, ToolError> {
        unreachable!("workflow dispatch must use the context-aware path")
    }

    async fn execute_with_context_outcome(
        &self,
        _call: &ToolCall,
        ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolOutcome, ToolError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.flags.lock().unwrap().push(ToolExecutionSessionFlags {
            bypass_permissions: ctx.bypass_permissions,
            auto_approve_permissions: ctx.auto_approve_permissions,
            plan_read_only: ctx.plan_read_only,
        });
        Ok(ToolOutcome::Completed(ToolResult::text(true, "{}")))
    }

    fn list_tools(&self) -> Vec<bamboo_agent_core::tools::ToolSchema> {
        Vec::new()
    }
}

struct CapturingTools(Arc<std::sync::Mutex<Option<Value>>>);

#[async_trait]
impl ToolExecutor for CapturingTools {
    async fn execute(&self, call: &ToolCall) -> Result<ToolResult, ToolError> {
        let args = serde_json::from_str(&call.function.arguments).unwrap();
        *self.0.lock().unwrap() = Some(args);
        if call.function.name == "echo-secret" {
            Ok(ToolResult::text(true, r#""resolved-test-value""#))
        } else {
            Ok(ToolResult::text(true, r#"{"ok":true}"#))
        }
    }

    fn list_tools(&self) -> Vec<bamboo_agent_core::tools::ToolSchema> {
        Vec::new()
    }
}

struct ApprovalTools;

#[async_trait]
impl ToolExecutor for ApprovalTools {
    async fn execute(&self, _call: &ToolCall) -> Result<ToolResult, ToolError> {
        unreachable!("outcome-aware path must be used")
    }

    async fn execute_with_context_outcome(
        &self,
        call: &ToolCall,
        _ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolOutcome, ToolError> {
        Ok(ToolOutcome::NeedsHuman {
            question: bamboo_agent_core::PendingQuestion {
                tool_call_id: call.id.clone(),
                tool_name: call.function.name.clone(),
                question: "Approve?".to_string(),
                options: vec!["yes".to_string(), "no".to_string()],
                allow_custom: false,
                source: bamboo_agent_core::PendingQuestionSource::PauseTool,
            },
            result: ToolResult::text(false, "approval required"),
        })
    }

    fn list_tools(&self) -> Vec<bamboo_agent_core::tools::ToolSchema> {
        Vec::new()
    }
}

struct RunningTools(Arc<AtomicBool>);

#[async_trait]
impl ToolExecutor for RunningTools {
    async fn execute(&self, _call: &ToolCall) -> Result<ToolResult, ToolError> {
        unreachable!("outcome-aware path must be used")
    }

    async fn execute_with_context_outcome(
        &self,
        call: &ToolCall,
        _ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolOutcome, ToolError> {
        let killed = self.0.clone();
        Ok(ToolOutcome::Running(RunningHandle {
            tool_call_id: call.id.clone(),
            ack: ToolResult::text(true, "running"),
            completion: RunningCompletion::Detached,
            wait_kind: AsyncWaitKind::AsyncTools,
            kill: Box::new(move || killed.store(true, Ordering::SeqCst)),
        }))
    }

    fn list_tools(&self) -> Vec<bamboo_agent_core::tools::ToolSchema> {
        Vec::new()
    }
}

struct MockAgents;

#[async_trait]
impl AgentStepPort for MockAgents {
    async fn resolve(
        &self,
        name: &str,
        _session_id: &str,
    ) -> Result<Option<NamedAgentSpec>, String> {
        Ok((name == "reviewer").then(|| NamedAgentSpec {
            name: name.to_string(),
            allowed_capabilities: BTreeSet::from(["read".to_string()]),
            profile: None,
            cost_supported: true,
        }))
    }

    async fn execute(
        &self,
        _spec: &NamedAgentSpec,
        prompt: Value,
        _model: Option<&str>,
        _effort: Option<&str>,
        _capabilities: &BTreeSet<String>,
        _session_id: &str,
        _root_run_id: &str,
        _cancellation: tokio_util::sync::CancellationToken,
    ) -> Result<AgentStepResult, String> {
        Ok(AgentStepResult {
            output: json!({"reviewed": prompt}),
            tokens: 10,
            cost_micros: Some(2),
            attempt_id: None,
            failure: None,
        })
    }
}

struct CountingAgents(AtomicUsize);

#[async_trait]
impl AgentStepPort for CountingAgents {
    async fn resolve(
        &self,
        name: &str,
        _session_id: &str,
    ) -> Result<Option<NamedAgentSpec>, String> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(Some(NamedAgentSpec {
            name: name.to_string(),
            allowed_capabilities: BTreeSet::from(["read".to_string()]),
            profile: None,
            cost_supported: true,
        }))
    }

    async fn execute(
        &self,
        _spec: &NamedAgentSpec,
        prompt: Value,
        _model: Option<&str>,
        _effort: Option<&str>,
        _capabilities: &BTreeSet<String>,
        _session_id: &str,
        _root_run_id: &str,
        _cancellation: tokio_util::sync::CancellationToken,
    ) -> Result<AgentStepResult, String> {
        Ok(AgentStepResult {
            output: prompt,
            tokens: 1,
            cost_micros: Some(1),
            attempt_id: None,
            failure: None,
        })
    }
}

struct FlakyOnceTools(AtomicUsize);

#[async_trait]
impl ToolExecutor for FlakyOnceTools {
    async fn execute(&self, call: &ToolCall) -> Result<ToolResult, ToolError> {
        match call.function.name.as_str() {
            "flaky-once" if self.0.fetch_add(1, Ordering::SeqCst) == 0 => {
                Err(ToolError::Execution("transient".to_string()))
            }
            "flaky-once" => Ok(ToolResult::text(true, r#"{"ok":true}"#)),
            "sibling" => {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                Ok(ToolResult::text(true, r#"{"sibling":true}"#))
            }
            other => Err(ToolError::NotFound(other.to_string())),
        }
    }

    fn list_tools(&self) -> Vec<bamboo_agent_core::tools::ToolSchema> {
        Vec::new()
    }
}

struct FlakyMapTools(AtomicUsize);

#[async_trait]
impl ToolExecutor for FlakyMapTools {
    async fn execute(&self, call: &ToolCall) -> Result<ToolResult, ToolError> {
        let item: Value = serde_json::from_str(&call.function.arguments).unwrap();
        if item == json!(1) && self.0.fetch_add(1, Ordering::SeqCst) == 0 {
            Err(ToolError::Execution("transient map item".to_string()))
        } else {
            Ok(ToolResult::text(
                true,
                serde_json::to_string(&item).unwrap(),
            ))
        }
    }

    fn list_tools(&self) -> Vec<bamboo_agent_core::tools::ToolSchema> {
        Vec::new()
    }
}

struct MapParallelTools;

#[async_trait]
impl ToolExecutor for MapParallelTools {
    async fn execute(&self, call: &ToolCall) -> Result<ToolResult, ToolError> {
        let item: Value = serde_json::from_str(&call.function.arguments).unwrap();
        match call.function.name.as_str() {
            "fail-zero" if item == json!(0) => Ok(ToolResult::text(false, "expected item failure")),
            "fail-zero" => Ok(ToolResult::text(true, item.to_string())),
            "slow-item" => {
                let delay = if item == json!(0) { 50 } else { 10 };
                tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                Ok(ToolResult::text(true, item.to_string()))
            }
            other => Err(ToolError::NotFound(other.to_string())),
        }
    }

    fn list_tools(&self) -> Vec<bamboo_agent_core::tools::ToolSchema> {
        Vec::new()
    }
}

struct GatedTools {
    started: Arc<tokio::sync::Semaphore>,
    release: Arc<tokio::sync::Notify>,
}

#[async_trait]
impl ToolExecutor for GatedTools {
    async fn execute(&self, call: &ToolCall) -> Result<ToolResult, ToolError> {
        let args: Value = serde_json::from_str(&call.function.arguments).unwrap();
        match call.function.name.as_str() {
            "gate" => {
                self.started.add_permits(1);
                self.release.notified().await;
                Ok(ToolResult::text(true, "null"))
            }
            "echo" => Ok(ToolResult::text(
                true,
                serde_json::to_string(&args).unwrap(),
            )),
            other => Err(ToolError::NotFound(other.to_string())),
        }
    }

    fn list_tools(&self) -> Vec<bamboo_agent_core::tools::ToolSchema> {
        Vec::new()
    }
}

struct MutableDefinitions {
    pins: AtomicUsize,
    definitions: std::sync::Mutex<HashMap<(String, u64), WorkflowRunDefinition>>,
}

#[async_trait]
impl WorkflowDefinitionPort for MutableDefinitions {
    async fn pin_bundle(
        &self,
        root: &WorkflowRunDefinition,
    ) -> Result<WorkflowDefinitionBundle, String> {
        self.pins.fetch_add(1, Ordering::SeqCst);
        let mut definitions = self
            .definitions
            .lock()
            .unwrap()
            .values()
            .cloned()
            .map(|definition| {
                (
                    WorkflowDefinitionBundle::key(&definition.id, definition.revision),
                    definition,
                )
            })
            .collect::<BTreeMap<_, _>>();
        definitions.insert(
            WorkflowDefinitionBundle::key(&root.id, root.revision),
            root.clone(),
        );
        Ok(WorkflowDefinitionBundle {
            publication_revision: 10,
            root_id: root.id.clone(),
            root_revision: root.revision,
            root_invocation_policy: json!({"explicit": true, "automatic": true}),
            definitions,
        })
    }
}

#[derive(Default)]
struct MockDefinitions(HashMap<(String, u64), WorkflowRunDefinition>);

#[async_trait]
impl WorkflowDefinitionPort for MockDefinitions {
    async fn pin_bundle(
        &self,
        root: &WorkflowRunDefinition,
    ) -> Result<WorkflowDefinitionBundle, String> {
        let mut definitions = self
            .0
            .values()
            .cloned()
            .map(|definition| {
                (
                    WorkflowDefinitionBundle::key(&definition.id, definition.revision),
                    definition,
                )
            })
            .collect::<BTreeMap<_, _>>();
        definitions.insert(
            WorkflowDefinitionBundle::key(&root.id, root.revision),
            root.clone(),
        );
        Ok(WorkflowDefinitionBundle {
            publication_revision: 1,
            root_id: root.id.clone(),
            root_revision: root.revision,
            root_invocation_policy: json!({"explicit": true, "automatic": true}),
            definitions,
        })
    }
}

struct MockPolicy;

#[async_trait]
impl WorkflowPolicyPort for MockPolicy {
    async fn authorize(
        &self,
        _session_id: &str,
        _target: &WorkflowPolicyTarget,
        requested: &BTreeSet<String>,
        workspace_trusted: bool,
    ) -> PermissionDecision {
        if !workspace_trusted || !requested.iter().all(|capability| capability == "read") {
            PermissionDecision::Deny(
                "workflow policy denied capability or untrusted workspace".to_string(),
            )
        } else {
            PermissionDecision::Allow
        }
    }
}

struct MockSecrets;

#[async_trait]
impl WorkflowSecretResolverPort for MockSecrets {
    async fn resolve(
        &self,
        _session_id: &str,
        capability: &str,
    ) -> Result<WorkflowSecretMaterial, String> {
        if capability == "test/read-token" {
            Ok(WorkflowSecretMaterial::new(
                "resolved-test-value".to_string(),
            ))
        } else {
            Err("unknown capability".to_string())
        }
    }
}

fn engine(directory: &std::path::Path, definitions: MockDefinitions) -> Arc<WorkflowRunEngine> {
    WorkflowRunEngine::new(
        Arc::new(FileWorkflowRunRepository::new(directory.to_path_buf()).unwrap()),
        Arc::new(MockTools),
        Arc::new(MockAgents),
        Arc::new(definitions),
        Arc::new(MockPolicy),
        Arc::new(MockSecrets),
        budgets(),
    )
}

fn request(definition: WorkflowRunDefinition, args: Value) -> StartWorkflowRun {
    StartWorkflowRun {
        definition,
        args,
        session_id: "session-real".to_string(),
        workspace_trusted: true,
        allowed_capabilities: vec!["read".to_string()],
    }
}

fn choice_plan(
    condition: ValueRef,
    then_branch: WorkflowPlan,
    else_branch: WorkflowPlan,
) -> WorkflowPlan {
    serde_json::from_value(json!({
        "type":"choice", "condition":condition,
        "then_branch":then_branch, "else_branch":else_branch
    }))
    .expect("Choice plan loads")
}

fn choice_leaf(step: &str) -> WorkflowPlan {
    WorkflowPlan::Step {
        step: step.to_string(),
    }
}

#[derive(Default)]
struct ChoiceRecordingTools {
    calls: std::sync::Mutex<Vec<(String, Value)>>,
    conditions: std::sync::Mutex<HashMap<i64, usize>>,
    failures: AtomicUsize,
}

#[async_trait]
impl ToolExecutor for ChoiceRecordingTools {
    async fn execute(&self, call: &ToolCall) -> Result<ToolResult, ToolError> {
        let args: Value = serde_json::from_str(&call.function.arguments).unwrap();
        self.calls
            .lock()
            .unwrap()
            .push((call.function.name.clone(), args.clone()));
        let output = match call.function.name.as_str() {
            "choice-condition" => {
                let row = args.as_i64().unwrap();
                let mut conditions = self.conditions.lock().unwrap();
                let attempts = conditions.entry(row).or_default();
                let selected = row != 0 || *attempts == 0;
                *attempts += 1;
                json!(selected)
            }
            "choice-transient"
                if args == json!(0) && self.failures.fetch_add(1, Ordering::SeqCst) == 0 =>
            {
                return Err(ToolError::Execution(
                    "transient selected branch".to_string(),
                ));
            }
            _ => args,
        };
        Ok(ToolResult::text(true, output.to_string()))
    }

    fn list_tools(&self) -> Vec<bamboo_agent_core::tools::ToolSchema> {
        Vec::new()
    }
}

fn choice_engine(
    directory: &std::path::Path,
    tools: Arc<ChoiceRecordingTools>,
) -> Arc<WorkflowRunEngine> {
    WorkflowRunEngine::new(
        Arc::new(FileWorkflowRunRepository::new(directory.to_path_buf()).unwrap()),
        tools,
        Arc::new(MockAgents),
        Arc::new(MockDefinitions::default()),
        Arc::new(MockPolicy),
        Arc::new(MockSecrets),
        budgets(),
    )
}

#[tokio::test]
async fn workflow_choice_dispatches_only_selected_branch_and_sequence_continues() {
    for approved in [true, false] {
        let directory = tempfile::tempdir().unwrap();
        let tools = Arc::new(ChoiceRecordingTools::default());
        let engine = choice_engine(directory.path(), tools.clone());
        let mut flow = definition(
            vec![
                tool_step("yes", "echo", json!({"selected":true})),
                tool_step("no", "echo", json!({"selected":false})),
                tool_step("after", "echo", json!({"after":true})),
            ],
            WorkflowPlan::Sequence {
                nodes: vec![
                    choice_plan(
                        ValueRef::Args {
                            pointer: "/approved".to_string(),
                        },
                        choice_leaf("yes"),
                        choice_leaf("no"),
                    ),
                    choice_leaf("after"),
                ],
            },
        );
        flow.input_schema = json!({"type":"object","properties":{"approved":{"type":"boolean"}},"required":["approved"],"additionalProperties":false});
        let pinned = flow.clone();
        let result = engine
            .run(request(flow, json!({"approved":approved})))
            .await
            .unwrap();
        assert_eq!(result.status, WorkflowRunStatus::Succeeded);
        assert_eq!(result.output, Some(json!({"after":true})));
        assert_eq!(result.usage.steps, 2);
        let (chosen, unchosen) = if approved {
            ("yes", "no")
        } else {
            ("no", "yes")
        };
        assert_eq!(result.steps[chosen].status, WorkflowStepStatus::Succeeded);
        let skipped = &result.steps[unchosen];
        assert_eq!(skipped.status, WorkflowStepStatus::Skipped);
        assert_eq!(skipped.attempts, 0);
        assert!(skipped.output.is_none() && skipped.input_hash.is_empty());
        assert_eq!(
            tools.calls.lock().unwrap().as_slice(),
            &[
                ("echo".to_string(), json!({"selected":approved})),
                ("echo".to_string(), json!({"after":true})),
            ]
        );
        let progress = engine.progress(&result.run_id, 0).await.unwrap();
        assert!(progress
            .events
            .windows(2)
            .all(|pair| pair[1].sequence == pair[0].sequence + 1));
        assert_eq!(
            progress
                .events
                .iter()
                .filter(
                    |event| matches!(event.kind, WorkflowRunEventKind::StepSkipped { .. })
                        && event.step_id.as_deref() == Some(unchosen)
                )
                .count(),
            1
        );
        assert!(matches!(
            progress.events.last().unwrap().kind,
            WorkflowRunEventKind::RunSucceeded { .. }
        ));
        let reloaded = FileWorkflowRunRepository::new(directory.path().to_path_buf())
            .unwrap()
            .load(&result.run_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reloaded, result);
        assert_eq!(reloaded.definition, pinned);
        assert_eq!(reloaded.definition_bundle.root(), Some(&pinned));
    }
}

#[tokio::test]
async fn workflow_choice_previous_boolean_output_selects_tool_or_agent() {
    for approved in [true, false] {
        let directory = tempfile::tempdir().unwrap();
        let tools = Arc::new(ChoiceRecordingTools::default());
        let engine = choice_engine(directory.path(), tools.clone());
        let mut producer = tool_step("approved", "echo", json!(approved));
        producer.output_schema = Some(json!({"type":"boolean"}));
        let agent = WorkflowStepDefinition {
            id: "agent".to_string(),
            kind: WorkflowStepKind::Agent {
                agent: "reviewer".to_string(),
                prompt: json!({"selected":false}),
                model: None,
                effort: None,
                capabilities: vec!["read".to_string()],
                structured_output_attempts: 1,
            },
            failure: FailurePolicy::FailFast,
            output_schema: None,
        };
        let flow = definition(
            vec![
                producer,
                tool_step("tool", "echo", json!({"selected":true})),
                agent,
            ],
            WorkflowPlan::Sequence {
                nodes: vec![
                    choice_leaf("approved"),
                    choice_plan(
                        ValueRef::Step {
                            step: "approved".to_string(),
                            pointer: String::new(),
                        },
                        choice_leaf("tool"),
                        choice_leaf("agent"),
                    ),
                ],
            },
        );
        let result = engine.run(request(flow, json!({}))).await.unwrap();
        assert_eq!(result.status, WorkflowRunStatus::Succeeded);
        assert_eq!(
            result.output,
            Some(if approved {
                json!({"selected":true})
            } else {
                json!({"reviewed":{"selected":false}})
            })
        );
        assert_eq!(result.usage.agents, u32::from(!approved));
        assert_eq!(
            tools.calls.lock().unwrap().len(),
            if approved { 2 } else { 1 }
        );
        assert_eq!(
            result.steps[if approved { "agent" } else { "tool" }].status,
            WorkflowStepStatus::Skipped
        );
    }
}

#[tokio::test]
async fn workflow_choice_missing_condition_dispatches_neither_branch() {
    let directory = tempfile::tempdir().unwrap();
    let tools = Arc::new(ChoiceRecordingTools::default());
    let engine = choice_engine(directory.path(), tools.clone());
    let mut flow = definition(
        vec![
            tool_step("yes", "echo", json!(true)),
            tool_step("no", "echo", json!(false)),
        ],
        choice_plan(
            ValueRef::Args {
                pointer: "/approved".to_string(),
            },
            choice_leaf("yes"),
            choice_leaf("no"),
        ),
    );
    flow.input_schema = json!({"type":"object","properties":{"approved":{"type":"boolean"}},"additionalProperties":false});
    let result = engine.run(request(flow, json!({}))).await.unwrap();
    assert_eq!(result.status, WorkflowRunStatus::Failed);
    assert_eq!(
        result.failure.as_ref().unwrap().code,
        WorkflowFailureCode::UnknownReference
    );
    assert!(tools.calls.lock().unwrap().is_empty());
    assert_eq!(result.usage.steps, 0);
}

#[tokio::test]
async fn workflow_choice_non_boolean_input_is_rejected_before_dispatch() {
    let directory = tempfile::tempdir().unwrap();
    let tools = Arc::new(ChoiceRecordingTools::default());
    let engine = choice_engine(directory.path(), tools.clone());
    let mut flow = definition(
        vec![
            tool_step("yes", "echo", json!(true)),
            tool_step("no", "echo", json!(false)),
        ],
        choice_plan(
            ValueRef::Args {
                pointer: "/approved".to_string(),
            },
            choice_leaf("yes"),
            choice_leaf("no"),
        ),
    );
    flow.input_schema = json!({"type":"object","properties":{"approved":{"type":"boolean"}},"required":["approved"],"additionalProperties":false});
    for value in [json!("false"), json!(0), Value::Null] {
        assert!(matches!(
            engine
                .run(request(flow.clone(), json!({"approved":value})))
                .await,
            Err(WorkflowRunError::InvalidInput(_))
        ));
    }
    assert!(tools.calls.lock().unwrap().is_empty());
    assert!(engine.list_run_ids().await.unwrap().is_empty());
}

#[tokio::test]
async fn workflow_choice_map_items_budget_counts_larger_branch() {
    let directory = tempfile::tempdir().unwrap();
    let tools = Arc::new(ChoiceRecordingTools::default());
    let engine = choice_engine(directory.path(), tools.clone());
    let mut flow = definition(
        vec![
            tool_step("first", "echo", json!("first")),
            tool_step("yes", "echo", json!({"from":"item","name":"row"})),
            tool_step("no", "echo", json!({"from":"item","name":"row"})),
        ],
        WorkflowPlan::Map {
            source: ValueRef::Args {
                pointer: "/rows".to_string(),
            },
            item: "row".to_string(),
            body: Box::new(choice_plan(
                ValueRef::Item {
                    name: "row".to_string(),
                    pointer: String::new(),
                },
                WorkflowPlan::Sequence {
                    nodes: vec![choice_leaf("first"), choice_leaf("yes")],
                },
                choice_leaf("no"),
            )),
        },
    );
    flow.input_schema = json!({"type":"object","properties":{"rows":{"type":"array","items":{"type":"boolean"}}},"required":["rows"],"additionalProperties":false});
    flow.budgets.max_steps = 4;
    let result = engine
        .run(request(flow.clone(), json!({"rows":[true,false]})))
        .await
        .unwrap();
    assert_eq!(result.status, WorkflowRunStatus::Succeeded);
    assert_eq!(result.output, Some(json!([true, false])));
    assert_eq!(result.usage.steps, 3);
    assert_eq!(
        result.steps["no@root[0]"].status,
        WorkflowStepStatus::Skipped
    );
    assert_eq!(
        result.steps["yes@root[1]"].status,
        WorkflowStepStatus::Skipped
    );
    assert_eq!(
        result.steps["first@root[1]"].status,
        WorkflowStepStatus::Skipped
    );
    let dispatched = tools.calls.lock().unwrap().len();
    flow.budgets.max_steps = 3;
    flow.budgets.max_agents = 3;
    let limited = engine
        .run(request(flow, json!({"rows":[true,false]})))
        .await
        .unwrap();
    assert_eq!(
        limited.failure.as_ref().unwrap().code,
        WorkflowFailureCode::BudgetExceeded
    );
    assert_eq!(limited.usage.steps, 0);
    assert_eq!(tools.calls.lock().unwrap().len(), dispatched);
}

#[tokio::test]
async fn workflow_choice_retry_switch_clears_only_current_map_scope_outputs() {
    let directory = tempfile::tempdir().unwrap();
    let tools = Arc::new(ChoiceRecordingTools::default());
    let engine = choice_engine(directory.path(), tools.clone());
    let mut producer = tool_step(
        "condition",
        "choice-condition",
        json!({"from":"item","name":"row"}),
    );
    producer.output_schema = Some(json!({"type":"boolean"}));
    let flow = definition(
        vec![
            producer,
            tool_step("mapped", "echo", json!({"from":"item","name":"value"})),
            tool_step(
                "transient",
                "choice-transient",
                json!({"from":"item","name":"row"}),
            ),
            tool_step("else", "echo", json!({"from":"item","name":"row"})),
        ],
        WorkflowPlan::Map {
            source: ValueRef::Args {
                pointer: "/items".to_string(),
            },
            item: "row".to_string(),
            body: Box::new(WorkflowPlan::Retry {
                max_attempts: 2,
                delay_ms: 0,
                node: Box::new(WorkflowPlan::Sequence {
                    nodes: vec![
                        choice_leaf("condition"),
                        choice_plan(
                            ValueRef::Step {
                                step: "condition".to_string(),
                                pointer: String::new(),
                            },
                            WorkflowPlan::Sequence {
                                nodes: vec![
                                    WorkflowPlan::Map {
                                        source: ValueRef::Args {
                                            pointer: "/items".to_string(),
                                        },
                                        item: "value".to_string(),
                                        body: Box::new(choice_leaf("mapped")),
                                    },
                                    choice_leaf("transient"),
                                ],
                            },
                            choice_leaf("else"),
                        ),
                    ],
                }),
            }),
        },
    );
    let result = engine
        .run(request(flow, json!({"items":[0,1]})))
        .await
        .unwrap();
    assert_eq!(result.status, WorkflowRunStatus::Succeeded);
    assert_eq!(result.output, Some(json!([0, 1])));
    assert_eq!(result.usage.retries, 1);
    assert_eq!(result.steps["condition@root[0]"].attempts, 2);
    for index in 0..2 {
        let skipped = &result.steps[&format!("mapped@root[0][{index}]")];
        assert_eq!(skipped.status, WorkflowStepStatus::Skipped);
        assert_eq!(skipped.attempts, 1);
        assert!(skipped.output.is_none());
        let sibling = &result.steps[&format!("mapped@root[1][{index}]")];
        assert_eq!(sibling.status, WorkflowStepStatus::Succeeded);
        assert_eq!(sibling.output, Some(json!(index)));
    }
    assert_eq!(
        result.steps["transient@root[0]"].status,
        WorkflowStepStatus::Skipped
    );
    assert!(result.steps["transient@root[0]"].output.is_none());
    assert_eq!(
        result.steps["else@root[0]"].status,
        WorkflowStepStatus::Succeeded
    );
    assert_eq!(
        result.steps["else@root[1]"].status,
        WorkflowStepStatus::Skipped
    );
    let progress = engine.progress(&result.run_id, 0).await.unwrap();
    assert!(progress
        .events
        .windows(2)
        .all(|pair| pair[1].sequence == pair[0].sequence + 1));
    for index in 0..2 {
        let id = format!("mapped@root[0][{index}]");
        assert!(progress
            .events
            .iter()
            .any(|event| event.step_id.as_deref() == Some(id.as_str())
                && matches!(event.kind, WorkflowRunEventKind::StepSkipped { .. })));
    }
}

#[tokio::test]
async fn workflow_choice_preflight_rejects_unselected_forbidden_branch() {
    let directory = tempfile::tempdir().unwrap();
    let tools = Arc::new(ChoiceRecordingTools::default());
    let engine = choice_engine(directory.path(), tools.clone());
    let mut denied = tool_step("denied", "echo", json!(false));
    if let WorkflowStepKind::Tool { capabilities, .. } = &mut denied.kind {
        *capabilities = vec!["write".to_string()];
    }
    let flow = definition(
        vec![tool_step("yes", "echo", json!(true)), denied],
        choice_plan(
            ValueRef::Literal { value: json!(true) },
            choice_leaf("yes"),
            choice_leaf("denied"),
        ),
    );
    assert!(matches!(
        engine.run(request(flow, json!({}))).await,
        Err(WorkflowRunError::Preflight(_))
    ));
    assert!(tools.calls.lock().unwrap().is_empty());
    assert!(engine.list_run_ids().await.unwrap().is_empty());
}

#[tokio::test]
async fn workflow_choice_parallel_cancellation_closes_selected_step() {
    let directory = tempfile::tempdir().unwrap();
    let started = Arc::new(tokio::sync::Semaphore::new(0));
    let engine = WorkflowRunEngine::new(
        Arc::new(FileWorkflowRunRepository::new(directory.path().to_path_buf()).unwrap()),
        Arc::new(GatedTools {
            started: started.clone(),
            release: Arc::new(tokio::sync::Notify::new()),
        }),
        Arc::new(MockAgents),
        Arc::new(MockDefinitions::default()),
        Arc::new(MockPolicy),
        Arc::new(MockSecrets),
        budgets(),
    );
    let flow = definition(
        vec![
            tool_step("selected", "gate", json!({})),
            tool_step("unselected", "echo", json!({})),
            tool_step("sibling", "echo", json!({})),
        ],
        WorkflowPlan::Parallel {
            nodes: vec![
                choice_plan(
                    ValueRef::Literal { value: json!(true) },
                    choice_leaf("selected"),
                    choice_leaf("unselected"),
                ),
                choice_leaf("sibling"),
            ],
        },
    );
    let runner = {
        let engine = engine.clone();
        tokio::spawn(async move { engine.run(request(flow, json!({}))).await.unwrap() })
    };
    let permit = tokio::time::timeout(std::time::Duration::from_secs(10), started.acquire())
        .await
        .unwrap()
        .unwrap();
    permit.forget();
    let run_id = engine.list_run_ids().await.unwrap().pop().unwrap();
    engine.cancel(&run_id).await.unwrap();
    let result = runner.await.unwrap();
    assert_eq!(result.status, WorkflowRunStatus::Cancelled);
    assert_eq!(
        result.steps["selected"].status,
        WorkflowStepStatus::Cancelled
    );
    assert_eq!(
        result.steps["unselected"].status,
        WorkflowStepStatus::Skipped
    );
    assert!(result.steps["unselected"].output.is_none());
    assert!(!result.steps.values().any(|step| matches!(
        step.status,
        WorkflowStepStatus::Running | WorkflowStepStatus::Queued
    )));
    assert!(!engine
        .progress(&run_id, 0)
        .await
        .unwrap()
        .events
        .iter()
        .any(|event| matches!(event.kind, WorkflowRunEventKind::RunSucceeded { .. })));
}

#[tokio::test]
async fn workflow_choice_recovery_retains_pinned_plan_without_false_completion() {
    let directory = tempfile::tempdir().unwrap();
    let repository =
        Arc::new(FileWorkflowRunRepository::new(directory.path().to_path_buf()).unwrap());
    let flow = definition(
        vec![
            tool_step("yes", "echo", json!(true)),
            tool_step("no", "echo", json!(false)),
        ],
        choice_plan(
            ValueRef::Literal { value: json!(true) },
            choice_leaf("yes"),
            choice_leaf("no"),
        ),
    );
    let mut queued = snapshot("choice-recovery", WorkflowRunStatus::Queued, 1);
    queued.definition = flow.clone();
    queued.definition_bundle.definitions.insert(
        WorkflowDefinitionBundle::key(&flow.id, flow.revision),
        flow.clone(),
    );
    repository
        .create(
            &queued,
            &run_event("choice-recovery", 1, WorkflowRunEventKind::RunQueued),
        )
        .await
        .unwrap();
    let mut running = queued;
    running.status = WorkflowRunStatus::Running;
    running.last_sequence = 2;
    running.steps.insert(
        "yes".to_string(),
        WorkflowStepSnapshot {
            id: "yes".to_string(),
            status: WorkflowStepStatus::Running,
            input_hash: String::new(),
            output: None,
            failure: None,
            attempts: 1,
        },
    );
    repository
        .commit(
            &running,
            &run_event("choice-recovery", 2, WorkflowRunEventKind::RunStarted),
        )
        .await
        .unwrap();
    let engine = choice_engine(directory.path(), Arc::new(ChoiceRecordingTools::default()));
    let recovered = engine.recover().await.unwrap().pop().unwrap();
    assert_eq!(recovered.status, WorkflowRunStatus::Suspended);
    assert_eq!(recovered.steps["yes"].status, WorkflowStepStatus::Suspended);
    assert_eq!(recovered.definition, flow);
    assert_eq!(recovered.definition_bundle, running.definition_bundle);
    assert_eq!(
        recovered.definition_bundle_hash,
        running.definition_bundle_hash
    );
    assert!(!repository
        .events_since("choice-recovery", 0)
        .await
        .unwrap()
        .iter()
        .any(|event| matches!(event.kind, WorkflowRunEventKind::RunSucceeded { .. })));
}

#[tokio::test]
async fn engine_runs_sequence_parallel_map_and_rebuilds_progress_from_sequence() {
    let directory = tempfile::tempdir().unwrap();
    let workflow = definition(
        vec![
            tool_step("first", "echo", json!({"from":"args","pointer":"/items"})),
            tool_step("left", "echo", json!({"side":"left"})),
            tool_step("right", "echo", json!({"side":"right"})),
            tool_step(
                "mapped",
                "echo",
                json!({"from":"item","name":"item","pointer":""}),
            ),
        ],
        WorkflowPlan::Sequence {
            nodes: vec![
                WorkflowPlan::Step {
                    step: "first".to_string(),
                },
                WorkflowPlan::Parallel {
                    nodes: vec![
                        WorkflowPlan::Step {
                            step: "left".to_string(),
                        },
                        WorkflowPlan::Step {
                            step: "right".to_string(),
                        },
                    ],
                },
                WorkflowPlan::Map {
                    source: ValueRef::Args {
                        pointer: "/items".to_string(),
                    },
                    item: "item".to_string(),
                    body: Box::new(WorkflowPlan::Step {
                        step: "mapped".to_string(),
                    }),
                },
            ],
        },
    );
    let engine = engine(directory.path(), MockDefinitions::default());
    let snapshot = engine
        .run(request(workflow, json!({"items":[1,2,3]})))
        .await
        .unwrap();
    assert_eq!(snapshot.status, WorkflowRunStatus::Succeeded);
    assert_eq!(snapshot.output, Some(json!([1, 2, 3])));
    assert_eq!(
        snapshot
            .steps
            .values()
            .filter(|step| step.status == WorkflowStepStatus::Succeeded)
            .count(),
        6
    );
    let progress = engine.progress(&snapshot.run_id, 0).await.unwrap();
    assert_eq!(
        progress.events.last().unwrap().sequence,
        progress.snapshot.last_sequence
    );
    assert!(matches!(
        progress.events.last().unwrap().kind,
        WorkflowRunEventKind::RunSucceeded { .. }
    ));
}

#[tokio::test]
async fn workflow_plan_auto_blocks_mutation_before_dispatch_and_allows_read_context() {
    let directory = tempfile::tempdir().unwrap();
    let tools = Arc::new(ContextRecordingTools::default());
    let engine = WorkflowRunEngine::new(
        Arc::new(FileWorkflowRunRepository::new(directory.path().to_path_buf()).unwrap()),
        tools.clone(),
        Arc::new(MockAgents),
        Arc::new(MockDefinitions::default()),
        Arc::new(MockPolicy),
        Arc::new(MockSecrets),
        budgets(),
    );
    let plan_auto = ToolExecutionSessionFlags {
        bypass_permissions: false,
        auto_approve_permissions: true,
        plan_read_only: true,
    };
    engine.set_session_permission_port(Arc::new(StaticSessionPermissions(plan_auto)));

    let blocked = engine
        .run(request(
            definition(
                vec![tool_step("write", "Write", json!({"file_path":"blocked"}))],
                WorkflowPlan::Step {
                    step: "write".to_string(),
                },
            ),
            json!({}),
        ))
        .await
        .unwrap();
    assert_eq!(blocked.status, WorkflowRunStatus::Failed);
    assert_eq!(tools.calls.load(Ordering::SeqCst), 0);
    assert!(blocked
        .failure
        .as_ref()
        .is_some_and(|failure| failure.message.contains("Plan mode")));

    let allowed = engine
        .run(request(
            definition(
                vec![tool_step("read", "Read", json!({"file_path":"safe"}))],
                WorkflowPlan::Step {
                    step: "read".to_string(),
                },
            ),
            json!({}),
        ))
        .await
        .unwrap();
    assert_eq!(allowed.status, WorkflowRunStatus::Succeeded);
    assert_eq!(tools.calls.load(Ordering::SeqCst), 1);
    assert_eq!(tools.flags.lock().unwrap().as_slice(), &[plan_auto]);
}

#[tokio::test]
async fn engine_agent_output_budget_and_permission_are_server_enforced() {
    let directory = tempfile::tempdir().unwrap();
    let agent = WorkflowStepDefinition {
        id: "review".to_string(),
        kind: WorkflowStepKind::Agent {
            agent: "reviewer".to_string(),
            prompt: json!({"from":"args","pointer":""}),
            model: Some("test:model".to_string()),
            effort: Some("high".to_string()),
            capabilities: vec!["read".to_string()],
            structured_output_attempts: 2,
        },
        failure: FailurePolicy::FailFast,
        output_schema: Some(
            json!({"type":"object","required":["reviewed"],"additionalProperties":true}),
        ),
    };
    let workflow = definition(
        vec![agent],
        WorkflowPlan::Step {
            step: "review".to_string(),
        },
    );
    let engine = engine(directory.path(), MockDefinitions::default());
    let snapshot = engine
        .run(request(workflow.clone(), json!({"x":1})))
        .await
        .unwrap();
    assert_eq!(snapshot.status, WorkflowRunStatus::Succeeded);
    assert_eq!(snapshot.usage.agents, 1);
    assert_eq!(snapshot.usage.tokens, 10);

    let mut untrusted = request(workflow, json!({"x":1}));
    untrusted.workspace_trusted = false;
    assert!(matches!(
        engine.run(untrusted).await,
        Err(WorkflowRunError::Preflight(_))
    ));
}

#[tokio::test]
async fn review_orchestration_dogfood_runs_pinned_tool_and_parallel_agent_pipeline() {
    let directory = tempfile::tempdir().unwrap();
    let mut inspect = tool_step(
        "inspect",
        "echo",
        json!({"patch":{"from":"args","pointer":"/patch"}}),
    );
    inspect.output_schema = Some(json!({
        "type":"object",
        "properties":{"patch":{"type":"string"}},
        "required":["patch"],
        "additionalProperties":false
    }));
    let review = |id: &str| WorkflowStepDefinition {
        id: id.to_string(),
        kind: WorkflowStepKind::Agent {
            agent: "reviewer".to_string(),
            prompt: json!({
                "focus": id,
                "patch":{"from":"step","step":"inspect","pointer":"/patch"}
            }),
            model: Some("test:review".to_string()),
            effort: Some("high".to_string()),
            capabilities: vec!["read".to_string()],
            structured_output_attempts: 2,
        },
        failure: FailurePolicy::FailFast,
        output_schema: Some(json!({
            "type":"object",
            "properties":{"reviewed":{"type":"object","additionalProperties":true}},
            "required":["reviewed"],
            "additionalProperties":false
        })),
    };
    let mut report = tool_step(
        "report",
        "echo",
        json!({
            "correctness":{"from":"step","step":"correctness","pointer":"/reviewed"},
            "security":{"from":"step","step":"security","pointer":"/reviewed"}
        }),
    );
    report.output_schema = Some(json!({"type":"object","additionalProperties":true}));
    let mut workflow = definition(
        vec![inspect, review("correctness"), review("security"), report],
        WorkflowPlan::Sequence {
            nodes: vec![
                WorkflowPlan::Step {
                    step: "inspect".to_string(),
                },
                WorkflowPlan::Parallel {
                    nodes: vec![
                        WorkflowPlan::Step {
                            step: "correctness".to_string(),
                        },
                        WorkflowPlan::Step {
                            step: "security".to_string(),
                        },
                    ],
                },
                WorkflowPlan::Step {
                    step: "report".to_string(),
                },
            ],
        },
    );
    workflow.input_schema = json!({
        "type":"object",
        "properties":{"patch":{"type":"string"}},
        "required":["patch"],
        "additionalProperties":false
    });
    let engine = engine(directory.path(), MockDefinitions::default());
    let succeeded = engine
        .run(request(workflow, json!({"patch":"diff --git a/a b/a"})))
        .await
        .unwrap();
    assert_eq!(succeeded.status, WorkflowRunStatus::Succeeded);
    assert_eq!(succeeded.usage.agents, 2);
    assert_eq!(
        succeeded.steps["report"].status,
        WorkflowStepStatus::Succeeded
    );
    assert_eq!(
        engine
            .progress(&succeeded.run_id, 0)
            .await
            .unwrap()
            .snapshot,
        succeeded
    );
}

#[tokio::test]
async fn named_agent_is_resolved_once_and_reused_from_the_pinned_preflight_snapshot() {
    let directory = tempfile::tempdir().unwrap();
    let agents = Arc::new(CountingAgents(AtomicUsize::new(0)));
    let engine = WorkflowRunEngine::new(
        Arc::new(FileWorkflowRunRepository::new(directory.path().to_path_buf()).unwrap()),
        Arc::new(MockTools),
        agents.clone(),
        Arc::new(MockDefinitions::default()),
        Arc::new(MockPolicy),
        Arc::new(MockSecrets),
        budgets(),
    );
    let agent_step = |id: &str| WorkflowStepDefinition {
        id: id.to_string(),
        kind: WorkflowStepKind::Agent {
            agent: "reviewer".to_string(),
            prompt: json!({"step": id}),
            model: None,
            effort: None,
            capabilities: vec!["read".to_string()],
            structured_output_attempts: 1,
        },
        failure: FailurePolicy::FailFast,
        output_schema: None,
    };
    let workflow = definition(
        vec![agent_step("first"), agent_step("second")],
        WorkflowPlan::Sequence {
            nodes: vec![
                WorkflowPlan::Step {
                    step: "first".to_string(),
                },
                WorkflowPlan::Step {
                    step: "second".to_string(),
                },
            ],
        },
    );
    let succeeded = engine.run(request(workflow, json!({}))).await.unwrap();
    assert_eq!(succeeded.status, WorkflowRunStatus::Succeeded);
    assert_eq!(agents.0.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn retry_wrapping_parallel_preserves_retryable_failure_and_cumulative_attempts() {
    let directory = tempfile::tempdir().unwrap();
    let engine = WorkflowRunEngine::new(
        Arc::new(FileWorkflowRunRepository::new(directory.path().to_path_buf()).unwrap()),
        Arc::new(FlakyOnceTools(AtomicUsize::new(0))),
        Arc::new(MockAgents),
        Arc::new(MockDefinitions::default()),
        Arc::new(MockPolicy),
        Arc::new(MockSecrets),
        budgets(),
    );
    let workflow = definition(
        vec![
            tool_step("flaky", "flaky-once", json!({})),
            tool_step("sibling", "sibling", json!({})),
        ],
        WorkflowPlan::Retry {
            node: Box::new(WorkflowPlan::Parallel {
                nodes: vec![
                    WorkflowPlan::Step {
                        step: "flaky".to_string(),
                    },
                    WorkflowPlan::Step {
                        step: "sibling".to_string(),
                    },
                ],
            }),
            max_attempts: 2,
            delay_ms: 0,
        },
    );
    let succeeded = engine.run(request(workflow, json!({}))).await.unwrap();
    assert_eq!(succeeded.status, WorkflowRunStatus::Succeeded);
    assert_eq!(succeeded.usage.retries, 1);
    assert_eq!(succeeded.steps["flaky"].attempts, 2);
    assert_eq!(succeeded.steps["sibling"].attempts, 2);
}

#[tokio::test]
async fn retry_wrapping_map_retries_transient_items_with_cumulative_attempts() {
    let directory = tempfile::tempdir().unwrap();
    let engine = WorkflowRunEngine::new(
        Arc::new(FileWorkflowRunRepository::new(directory.path().to_path_buf()).unwrap()),
        Arc::new(FlakyMapTools(AtomicUsize::new(0))),
        Arc::new(MockAgents),
        Arc::new(MockDefinitions::default()),
        Arc::new(MockPolicy),
        Arc::new(MockSecrets),
        budgets(),
    );
    let workflow = definition(
        vec![tool_step(
            "mapped",
            "flaky-map",
            json!({"from":"item","name":"item","pointer":""}),
        )],
        WorkflowPlan::Retry {
            node: Box::new(WorkflowPlan::Map {
                source: ValueRef::Args {
                    pointer: "/items".to_string(),
                },
                item: "item".to_string(),
                body: Box::new(WorkflowPlan::Step {
                    step: "mapped".to_string(),
                }),
            }),
            max_attempts: 2,
            delay_ms: 0,
        },
    );
    let succeeded = engine
        .run(request(workflow, json!({"items":[1,2]})))
        .await
        .unwrap();
    assert_eq!(succeeded.status, WorkflowRunStatus::Succeeded);
    assert_eq!(succeeded.output, Some(json!([1, 2])));
    assert_eq!(succeeded.usage.retries, 1);
    assert_eq!(succeeded.steps["mapped@root[0]"].attempts, 2);
    assert_eq!(succeeded.steps["mapped@root[1]"].attempts, 2);
    let events = engine.progress(&succeeded.run_id, 0).await.unwrap().events;
    assert!(events.iter().any(|event| {
        event.step_id.as_deref() == Some("mapped@root[0]")
            && matches!(event.kind, WorkflowRunEventKind::StepFailed { .. })
    }));
}

#[tokio::test]
async fn map_of_parallel_fail_fast_cancellation_is_isolated_per_item_scope() {
    let directory = tempfile::tempdir().unwrap();
    let engine = WorkflowRunEngine::new(
        Arc::new(FileWorkflowRunRepository::new(directory.path().to_path_buf()).unwrap()),
        Arc::new(MapParallelTools),
        Arc::new(MockAgents),
        Arc::new(MockDefinitions::default()),
        Arc::new(MockPolicy),
        Arc::new(MockSecrets),
        budgets(),
    );
    let item_ref = json!({"from":"item","name":"item","pointer":""});
    let workflow = definition(
        vec![
            tool_step("maybe-fail", "fail-zero", item_ref.clone()),
            tool_step("slow", "slow-item", item_ref),
        ],
        WorkflowPlan::Map {
            source: ValueRef::Args {
                pointer: "/items".to_string(),
            },
            item: "item".to_string(),
            body: Box::new(WorkflowPlan::Parallel {
                nodes: vec![
                    WorkflowPlan::Step {
                        step: "maybe-fail".to_string(),
                    },
                    WorkflowPlan::Step {
                        step: "slow".to_string(),
                    },
                ],
            }),
        },
    );
    let failed = engine
        .run(request(workflow, json!({"items":[0,1]})))
        .await
        .unwrap();
    assert_eq!(failed.status, WorkflowRunStatus::Failed);
    assert_eq!(
        failed.steps["maybe-fail@root[0]"].status,
        WorkflowStepStatus::Failed
    );
    assert_eq!(
        failed.steps["slow@root[0]"].status,
        WorkflowStepStatus::Cancelled
    );
    assert_eq!(
        failed.steps["maybe-fail@root[1]"].status,
        WorkflowStepStatus::Succeeded
    );
    assert_eq!(
        failed.steps["slow@root[1]"].status,
        WorkflowStepStatus::Succeeded
    );
    assert!(failed.failure.as_ref().unwrap().message.contains("item[0]"));

    let events = engine.progress(&failed.run_id, 0).await.unwrap().events;
    assert!(!events.iter().any(|event| {
        event
            .step_id
            .as_deref()
            .is_some_and(|id| id.ends_with("@root[1]"))
            && matches!(event.kind, WorkflowRunEventKind::StepCancelled)
    }));
    let mut cancelled = BTreeSet::new();
    for event in events {
        let Some(step_id) = event.step_id else {
            continue;
        };
        match event.kind {
            WorkflowRunEventKind::StepCancelled => {
                cancelled.insert(step_id);
            }
            WorkflowRunEventKind::StepCompleted { .. } => assert!(
                !cancelled.contains(&step_id),
                "step {step_id} completed after cancellation"
            ),
            _ => {}
        }
    }
}

#[tokio::test]
async fn nested_workflow_is_exact_revision_and_progress_is_linked() {
    let directory = tempfile::tempdir().unwrap();
    let nested = WorkflowRunDefinition {
        id: "child".to_string(),
        revision: 2,
        ..definition(
            vec![tool_step(
                "child-step",
                "echo",
                json!({"from":"args","pointer":""}),
            )],
            WorkflowPlan::Step {
                step: "child-step".to_string(),
            },
        )
    };
    let parent_step = WorkflowStepDefinition {
        id: "nested".to_string(),
        kind: WorkflowStepKind::Workflow {
            workflow_id: "child".to_string(),
            revision: 2,
            args: json!({"from":"args","pointer":""}),
        },
        failure: FailurePolicy::FailFast,
        output_schema: None,
    };
    let parent = definition(
        vec![parent_step],
        WorkflowPlan::Step {
            step: "nested".to_string(),
        },
    );
    let definitions = MockDefinitions(HashMap::from([(("child".to_string(), 2), nested)]));
    let engine = engine(directory.path(), definitions);
    let result = engine
        .run(request(parent, json!({"task":"review"})))
        .await
        .unwrap();
    assert_eq!(result.status, WorkflowRunStatus::Succeeded);
    let ids = engine.list_run_ids().await.unwrap();
    assert_eq!(ids.len(), 2);
    let mut child = None;
    for id in ids {
        let snapshot = engine.progress(&id, 0).await.unwrap().snapshot;
        if snapshot.parent_run_id.is_some() {
            child = Some(snapshot);
            break;
        }
    }
    let child = child.unwrap();
    assert_eq!(child.parent_run_id.as_deref(), Some(result.run_id.as_str()));
    assert_eq!(child.parent_step_id.as_deref(), Some("nested"));
}

#[tokio::test]
async fn nested_dispatch_uses_one_pinned_bundle_despite_live_definition_mutation() {
    let directory = tempfile::tempdir().unwrap();
    let old_child = WorkflowRunDefinition {
        id: "mutable-child".to_string(),
        revision: 2,
        ..definition(
            vec![tool_step("child", "echo", json!({"version":"old"}))],
            WorkflowPlan::Step {
                step: "child".to_string(),
            },
        )
    };
    let mut new_child = old_child.clone();
    if let WorkflowStepKind::Tool { args, .. } = &mut new_child.steps[0].kind {
        *args = json!({"version":"new"});
    }
    let definitions = Arc::new(MutableDefinitions {
        pins: AtomicUsize::new(0),
        definitions: std::sync::Mutex::new(HashMap::from([(
            (old_child.id.clone(), old_child.revision),
            old_child,
        )])),
    });
    let started = Arc::new(tokio::sync::Semaphore::new(0));
    let release = Arc::new(tokio::sync::Notify::new());
    let engine = WorkflowRunEngine::new(
        Arc::new(FileWorkflowRunRepository::new(directory.path().to_path_buf()).unwrap()),
        Arc::new(GatedTools {
            started: started.clone(),
            release: release.clone(),
        }),
        Arc::new(MockAgents),
        definitions.clone(),
        Arc::new(MockPolicy),
        Arc::new(MockSecrets),
        budgets(),
    );
    let nested = WorkflowStepDefinition {
        id: "nested".to_string(),
        kind: WorkflowStepKind::Workflow {
            workflow_id: "mutable-child".to_string(),
            revision: 2,
            args: json!({}),
        },
        failure: FailurePolicy::FailFast,
        output_schema: None,
    };
    let parent = definition(
        vec![tool_step("gate", "gate", json!({})), nested],
        WorkflowPlan::Sequence {
            nodes: vec![
                WorkflowPlan::Step {
                    step: "gate".to_string(),
                },
                WorkflowPlan::Step {
                    step: "nested".to_string(),
                },
            ],
        },
    );
    let running = engine.start(request(parent, json!({}))).await.unwrap();
    let permit = started.acquire().await.unwrap();
    permit.forget();
    definitions
        .definitions
        .lock()
        .unwrap()
        .insert((new_child.id.clone(), new_child.revision), new_child);
    release.notify_waiters();

    let finished = loop {
        let snapshot = engine.progress(&running.run_id, 0).await.unwrap().snapshot;
        if snapshot.status.is_terminal() {
            break snapshot;
        }
        tokio::task::yield_now().await;
    };
    assert_eq!(finished.status, WorkflowRunStatus::Succeeded);
    assert_eq!(finished.output, Some(json!({"version":"old"})));
    assert_eq!(definitions.pins.load(Ordering::SeqCst), 1);
    assert_eq!(
        finished
            .definition_bundle
            .get("mutable-child", 2)
            .unwrap()
            .steps[0]
            .kind,
        WorkflowStepKind::Tool {
            tool: "echo".to_string(),
            args: json!({"version":"old"}),
            capabilities: vec!["read".to_string()],
        }
    );
}

#[tokio::test]
async fn skip_dependents_retry_exhaustion_and_recovery_have_typed_events() {
    let directory = tempfile::tempdir().unwrap();
    let mut failing = tool_step("fail", "fail", json!({}));
    failing.failure = FailurePolicy::SkipDependents;
    let workflow = definition(
        vec![failing, tool_step("never", "echo", json!({}))],
        WorkflowPlan::Sequence {
            nodes: vec![
                WorkflowPlan::Step {
                    step: "fail".to_string(),
                },
                WorkflowPlan::Step {
                    step: "never".to_string(),
                },
            ],
        },
    );
    let engine = engine(directory.path(), MockDefinitions::default());
    let failed = engine.run(request(workflow, json!({}))).await.unwrap();
    assert_eq!(failed.status, WorkflowRunStatus::Failed);
    assert_eq!(failed.steps["never"].status, WorkflowStepStatus::Skipped);

    let recovery_dir = tempfile::tempdir().unwrap();
    let repository =
        Arc::new(FileWorkflowRunRepository::new(recovery_dir.path().to_path_buf()).unwrap());
    let queued = snapshot("stale", WorkflowRunStatus::Queued, 1);
    repository
        .create(
            &queued,
            &run_event("stale", 1, WorkflowRunEventKind::RunQueued),
        )
        .await
        .unwrap();
    let recovery_engine = WorkflowRunEngine::new(
        repository.clone(),
        Arc::new(MockTools),
        Arc::new(MockAgents),
        Arc::new(MockDefinitions::default()),
        Arc::new(MockPolicy),
        Arc::new(MockSecrets),
        budgets(),
    );
    let recovered = recovery_engine.recover().await.unwrap();
    assert_eq!(recovered[0].status, WorkflowRunStatus::Suspended);
    let events = repository.events_since("stale", 0).await.unwrap();
    assert!(matches!(
        events.last().unwrap().kind,
        WorkflowRunEventKind::RunSuspended { .. }
    ));
}

#[tokio::test]
async fn retry_exhaustion_and_step_budget_are_typed_failures() {
    let directory = tempfile::tempdir().unwrap();
    let mut workflow = definition(
        vec![tool_step("flaky", "flaky", json!({}))],
        WorkflowPlan::Retry {
            node: Box::new(WorkflowPlan::Step {
                step: "flaky".to_string(),
            }),
            max_attempts: 3,
            delay_ms: 0,
        },
    );
    workflow.budgets.max_retries = 2;
    let engine = engine(directory.path(), MockDefinitions::default());
    let failed = engine.run(request(workflow, json!({}))).await.unwrap();
    assert_eq!(failed.status, WorkflowRunStatus::Failed);
    assert_eq!(
        failed.failure.as_ref().unwrap().code,
        WorkflowFailureCode::RetryExhausted
    );
    assert_eq!(failed.usage.retries, 2);
    assert_eq!(failed.steps["flaky"].attempts, 3);

    let mut limited = definition(
        vec![
            tool_step("one", "echo", json!(1)),
            tool_step("two", "echo", json!(2)),
        ],
        WorkflowPlan::Sequence {
            nodes: vec![
                WorkflowPlan::Step {
                    step: "one".to_string(),
                },
                WorkflowPlan::Step {
                    step: "two".to_string(),
                },
            ],
        },
    );
    limited.budgets.max_steps = 1;
    limited.budgets.max_agents = 0;
    let failed = engine.run(request(limited, json!({}))).await.unwrap();
    assert_eq!(
        failed.failure.as_ref().unwrap().code,
        WorkflowFailureCode::BudgetExceeded
    );
    assert_eq!(failed.usage.steps, 1);
}

#[tokio::test]
async fn cancellation_is_idempotent_and_never_publishes_success() {
    let directory = tempfile::tempdir().unwrap();
    let workflow = definition(
        vec![tool_step("slow", "slow", json!({}))],
        WorkflowPlan::Step {
            step: "slow".to_string(),
        },
    );
    let engine = engine(directory.path(), MockDefinitions::default());
    let runner = {
        let engine = engine.clone();
        tokio::spawn(async move { engine.run(request(workflow, json!({}))).await.unwrap() })
    };
    let run_id = loop {
        if let Some(id) = engine.list_run_ids().await.unwrap().into_iter().next() {
            break id;
        }
        tokio::task::yield_now().await;
    };
    loop {
        if !engine
            .progress(&run_id, 0)
            .await
            .unwrap()
            .snapshot
            .steps
            .is_empty()
        {
            break;
        }
        tokio::task::yield_now().await;
    }
    let cancelled = engine.cancel(&run_id).await.unwrap();
    assert_eq!(cancelled.status, WorkflowRunStatus::Cancelled);
    assert_eq!(
        engine.cancel(&run_id).await.unwrap().status,
        WorkflowRunStatus::Cancelled
    );
    let final_snapshot = runner.await.unwrap();
    assert_eq!(final_snapshot.status, WorkflowRunStatus::Cancelled);
    let events = engine.progress(&run_id, 0).await.unwrap().events;
    assert!(events
        .iter()
        .any(|event| matches!(event.kind, WorkflowRunEventKind::StepCancelled)));
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event.kind, WorkflowRunEventKind::RunCancelled))
            .count(),
        1
    );
    assert!(!events
        .iter()
        .any(|event| matches!(event.kind, WorkflowRunEventKind::RunSucceeded { .. })));
}

#[tokio::test]
async fn templates_do_not_interpolate_and_secret_material_never_reaches_terminal_event() {
    let directory = tempfile::tempdir().unwrap();
    let literal = "$(touch /tmp/workflow-injection) {{args.secret}}";
    let workflow = definition(
        vec![tool_step("literal", "echo", json!({"text": literal}))],
        WorkflowPlan::Step {
            step: "literal".to_string(),
        },
    );
    let engine = engine(directory.path(), MockDefinitions::default());
    let succeeded = engine.run(request(workflow, json!({}))).await.unwrap();
    assert_eq!(succeeded.output, Some(json!({"text": literal})));

    let secret_output = definition(
        vec![tool_step("leak", "secretout", json!({}))],
        WorkflowPlan::Step {
            step: "leak".to_string(),
        },
    );
    let failed = engine.run(request(secret_output, json!({}))).await.unwrap();
    assert_eq!(failed.status, WorkflowRunStatus::Failed);
    assert!(failed.steps["leak"].output.is_none());
    let serialized =
        serde_json::to_string(&engine.progress(&failed.run_id, 0).await.unwrap()).unwrap();
    assert!(!serialized.contains("raw-secret"));

    let secret_input = definition(
        vec![tool_step("echo", "echo", json!({}))],
        WorkflowPlan::Step {
            step: "echo".to_string(),
        },
    );
    assert!(matches!(
        engine
            .run(request(secret_input, json!({"password":"plaintext"})))
            .await,
        Err(WorkflowRunError::InvalidInput(_))
    ));

    let secret_definition = definition(
        vec![tool_step(
            "definition-leak",
            "echo",
            json!({"api_key":"raw-secret"}),
        )],
        WorkflowPlan::Step {
            step: "definition-leak".to_string(),
        },
    );
    assert!(matches!(
        engine.run(request(secret_definition, json!({}))).await,
        Err(WorkflowRunError::Preflight(_))
    ));

    let secret_error = definition(
        vec![tool_step("error-leak", "secretfail", json!({}))],
        WorkflowPlan::Step {
            step: "error-leak".to_string(),
        },
    );
    let failed = engine.run(request(secret_error, json!({}))).await.unwrap();
    let serialized =
        serde_json::to_string(&engine.progress(&failed.run_id, 0).await.unwrap()).unwrap();
    assert!(!serialized.contains("raw-secret"));

    for sensitive in ["credential", "access_token", "secret_key"] {
        let mut args = serde_json::Map::new();
        args.insert(sensitive.to_string(), json!("plaintext"));
        let workflow = definition(
            vec![tool_step("echo", "echo", json!({}))],
            WorkflowPlan::Step {
                step: "echo".to_string(),
            },
        );
        assert!(matches!(
            engine.run(request(workflow, Value::Object(args))).await,
            Err(WorkflowRunError::InvalidInput(_))
        ));
    }

    let bare_secret_output = definition(
        vec![tool_step("bare", "baresecret", json!({}))],
        WorkflowPlan::Step {
            step: "bare".to_string(),
        },
    );
    let failed = engine
        .run(request(bare_secret_output, json!({})))
        .await
        .unwrap();
    assert_eq!(failed.status, WorkflowRunStatus::Failed);
    let serialized =
        serde_json::to_string(&engine.progress(&failed.run_id, 0).await.unwrap()).unwrap();
    assert!(!serialized.contains("sk-raw-secret"));
}

#[tokio::test]
async fn typed_secret_handle_is_resolved_only_for_tool_dispatch_and_never_persisted_raw() {
    let directory = tempfile::tempdir().unwrap();
    let captured = Arc::new(std::sync::Mutex::new(None));
    let engine = WorkflowRunEngine::new(
        Arc::new(FileWorkflowRunRepository::new(directory.path().to_path_buf()).unwrap()),
        Arc::new(CapturingTools(captured.clone())),
        Arc::new(MockAgents),
        Arc::new(MockDefinitions::default()),
        Arc::new(MockPolicy),
        Arc::new(MockSecrets),
        budgets(),
    );
    let mut workflow = definition(
        vec![tool_step(
            "use-secret",
            "capture",
            json!({"credential":{"from":"args","pointer":"/credential"}}),
        )],
        WorkflowPlan::Step {
            step: "use-secret".to_string(),
        },
    );
    workflow.input_schema = json!({
        "type":"object",
        "properties":{
            "credential":{
                "type":"object",
                "x-bamboo-secret":true,
                "additionalProperties":false
            }
        },
        "required":["credential"],
        "additionalProperties":false
    });
    let succeeded = engine
        .run(request(
            workflow,
            json!({"credential":{"$secret":"test/read-token"}}),
        ))
        .await
        .unwrap();
    assert_eq!(succeeded.status, WorkflowRunStatus::Succeeded);
    assert_eq!(
        captured.lock().unwrap().as_ref().unwrap()["credential"],
        "resolved-test-value"
    );
    let progress = engine.progress(&succeeded.run_id, 0).await.unwrap();
    let serialized = serde_json::to_string(&progress).unwrap();
    assert!(serialized.contains("test/read-token"));
    assert!(!serialized.contains("resolved-test-value"));

    let journal = tokio::fs::read_to_string(
        directory
            .path()
            .join(&succeeded.run_id)
            .join("journal.jsonl"),
    )
    .await
    .unwrap();
    assert!(!journal.contains("resolved-test-value"));

    let mut echo_workflow = definition(
        vec![tool_step(
            "echo-secret",
            "echo-secret",
            json!({"credential":{"from":"args","pointer":"/credential"}}),
        )],
        WorkflowPlan::Step {
            step: "echo-secret".to_string(),
        },
    );
    echo_workflow.input_schema = json!({
        "type":"object",
        "properties":{"credential":{"x-bamboo-secret":true}},
        "required":["credential"],
        "additionalProperties":false
    });
    let failed = engine
        .run(request(
            echo_workflow,
            json!({"credential":{"$secret":"test/read-token"}}),
        ))
        .await
        .unwrap();
    assert_eq!(failed.status, WorkflowRunStatus::Failed);
    let serialized =
        serde_json::to_string(&engine.progress(&failed.run_id, 0).await.unwrap()).unwrap();
    assert!(!serialized.contains("resolved-test-value"));
}

#[tokio::test]
async fn structured_agent_output_retries_are_bounded() {
    let directory = tempfile::tempdir().unwrap();
    let agent = WorkflowStepDefinition {
        id: "agent".to_string(),
        kind: WorkflowStepKind::Agent {
            agent: "reviewer".to_string(),
            prompt: json!({}),
            model: None,
            effort: None,
            capabilities: vec!["read".to_string()],
            structured_output_attempts: 2,
        },
        failure: FailurePolicy::FailFast,
        output_schema: Some(
            json!({"type":"object","required":["impossible"],"additionalProperties":true}),
        ),
    };
    let workflow = definition(
        vec![agent],
        WorkflowPlan::Step {
            step: "agent".to_string(),
        },
    );
    let engine = engine(directory.path(), MockDefinitions::default());
    let failed = engine.run(request(workflow, json!({}))).await.unwrap();
    assert_eq!(failed.status, WorkflowRunStatus::Failed);
    assert_eq!(
        failed.steps["agent"].failure.as_ref().unwrap().code,
        WorkflowFailureCode::InvalidOutput
    );
    assert_eq!(failed.usage.agents, 2);
}

#[tokio::test]
async fn parallel_and_map_partial_failures_preserve_branch_indices() {
    let directory = tempfile::tempdir().unwrap();
    let parallel = definition(
        vec![
            tool_step("bad", "fail", json!({})),
            tool_step("good", "echo", json!({"ok":true})),
        ],
        WorkflowPlan::Parallel {
            nodes: vec![
                WorkflowPlan::Step {
                    step: "bad".to_string(),
                },
                WorkflowPlan::Step {
                    step: "good".to_string(),
                },
            ],
        },
    );
    let engine = engine(directory.path(), MockDefinitions::default());
    let failed = engine.run(request(parallel, json!({}))).await.unwrap();
    assert_eq!(failed.steps["good"].status, WorkflowStepStatus::Cancelled);
    assert!(failed
        .failure
        .as_ref()
        .unwrap()
        .message
        .contains("branch[0]"));

    let map = definition(
        vec![tool_step(
            "bad-item",
            "fail",
            json!({"from":"item","name":"item","pointer":""}),
        )],
        WorkflowPlan::Map {
            source: ValueRef::Args {
                pointer: "/items".to_string(),
            },
            item: "item".to_string(),
            body: Box::new(WorkflowPlan::Step {
                step: "bad-item".to_string(),
            }),
        },
    );
    let failed = engine
        .run(request(map, json!({"items":[1,2]})))
        .await
        .unwrap();
    let message = &failed.failure.as_ref().unwrap().message;
    assert!(message.contains("item[0]") && message.contains("item[1]"));
}

#[tokio::test]
async fn nested_depth_and_shared_step_budget_cannot_be_bypassed() {
    let directory = tempfile::tempdir().unwrap();
    let mut child = WorkflowRunDefinition {
        id: "child-budget".to_string(),
        revision: 3,
        ..definition(
            vec![tool_step("child-work", "echo", json!({}))],
            WorkflowPlan::Step {
                step: "child-work".to_string(),
            },
        )
    };
    child.budgets.max_steps = 1;
    child.budgets.max_agents = 0;
    let parent_step = WorkflowStepDefinition {
        id: "child-call".to_string(),
        kind: WorkflowStepKind::Workflow {
            workflow_id: "child-budget".to_string(),
            revision: 3,
            args: json!({}),
        },
        failure: FailurePolicy::FailFast,
        output_schema: None,
    };
    let mut parent = definition(
        vec![parent_step.clone()],
        WorkflowPlan::Step {
            step: "child-call".to_string(),
        },
    );
    parent.budgets.max_steps = 1;
    parent.budgets.max_agents = 0;
    let definitions = MockDefinitions(HashMap::from([(
        ("child-budget".to_string(), 3),
        child.clone(),
    )]));
    let budget_engine = engine(directory.path(), definitions);
    let failed = budget_engine.run(request(parent, json!({}))).await.unwrap();
    assert_eq!(failed.status, WorkflowRunStatus::Failed);
    assert!(failed
        .failure
        .as_ref()
        .unwrap()
        .message
        .contains("step budget"));

    let mut shallow = definition(
        vec![parent_step],
        WorkflowPlan::Step {
            step: "child-call".to_string(),
        },
    );
    shallow.budgets.max_nesting_depth = 1;
    let definitions = MockDefinitions(HashMap::from([(("child-budget".to_string(), 3), child)]));
    let rejected = engine(directory.path(), definitions)
        .run(request(shallow, json!({})))
        .await;
    assert!(matches!(rejected, Err(WorkflowRunError::Preflight(_))));
}

#[tokio::test]
async fn agent_and_actual_usage_limits_are_persisted_before_failure() {
    let directory = tempfile::tempdir().unwrap();
    let agent = WorkflowStepDefinition {
        id: "agent".to_string(),
        kind: WorkflowStepKind::Agent {
            agent: "reviewer".to_string(),
            prompt: json!({}),
            model: None,
            effort: None,
            capabilities: vec!["read".to_string()],
            structured_output_attempts: 2,
        },
        failure: FailurePolicy::FailFast,
        output_schema: Some(
            json!({"type":"object","required":["missing"],"additionalProperties":true}),
        ),
    };
    let mut agent_limited = definition(
        vec![agent.clone()],
        WorkflowPlan::Step {
            step: "agent".to_string(),
        },
    );
    agent_limited.budgets.max_agents = 1;
    let engine = engine(directory.path(), MockDefinitions::default());
    let failed = engine.run(request(agent_limited, json!({}))).await.unwrap();
    assert_eq!(
        failed.failure.as_ref().unwrap().code,
        WorkflowFailureCode::BudgetExceeded
    );
    assert_eq!(failed.usage.agents, 1);

    let mut usage_limited = definition(
        vec![agent],
        WorkflowPlan::Step {
            step: "agent".to_string(),
        },
    );
    usage_limited.steps[0].output_schema = None;
    usage_limited.budgets.max_tokens = Some(5);
    usage_limited.budgets.max_cost_micros = Some(1);
    let failed = engine.run(request(usage_limited, json!({}))).await.unwrap();
    assert_eq!(
        failed.failure.as_ref().unwrap().code,
        WorkflowFailureCode::BudgetExceeded
    );
    assert_eq!(failed.usage.tokens, 10);
    assert_eq!(failed.usage.cost_micros, Some(2));
    let persisted = engine.progress(&failed.run_id, 0).await.unwrap().snapshot;
    assert_eq!(persisted.usage, failed.usage);
}

#[tokio::test]
async fn zero_token_or_cost_budget_fails_before_agent_dispatch() {
    struct ExecutionCountingAgents(Arc<AtomicUsize>);

    #[async_trait]
    impl AgentStepPort for ExecutionCountingAgents {
        async fn resolve(
            &self,
            name: &str,
            _session_id: &str,
        ) -> Result<Option<NamedAgentSpec>, String> {
            Ok(Some(NamedAgentSpec {
                name: name.to_string(),
                allowed_capabilities: BTreeSet::from(["read".to_string()]),
                profile: None,
                cost_supported: true,
            }))
        }

        async fn execute(
            &self,
            _spec: &NamedAgentSpec,
            _prompt: Value,
            _model: Option<&str>,
            _effort: Option<&str>,
            _capabilities: &BTreeSet<String>,
            _session_id: &str,
            _root_run_id: &str,
            _cancellation: tokio_util::sync::CancellationToken,
        ) -> Result<AgentStepResult, String> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(AgentStepResult {
                output: json!({"unexpected": true}),
                tokens: 1,
                cost_micros: Some(1),
                attempt_id: None,
                failure: None,
            })
        }
    }

    let directory = tempfile::tempdir().unwrap();
    let executions = Arc::new(AtomicUsize::new(0));
    let engine = WorkflowRunEngine::new(
        Arc::new(FileWorkflowRunRepository::new(directory.path().to_path_buf()).unwrap()),
        Arc::new(MockTools),
        Arc::new(ExecutionCountingAgents(executions.clone())),
        Arc::new(MockDefinitions::default()),
        Arc::new(MockPolicy),
        Arc::new(MockSecrets),
        budgets(),
    );
    let agent_step = WorkflowStepDefinition {
        id: "agent".to_string(),
        kind: WorkflowStepKind::Agent {
            agent: "reviewer".to_string(),
            prompt: json!({}),
            model: None,
            effort: None,
            capabilities: vec!["read".to_string()],
            structured_output_attempts: 1,
        },
        failure: FailurePolicy::FailFast,
        output_schema: None,
    };

    for (max_tokens, max_cost_micros) in [(Some(0), Some(10)), (Some(10), Some(0))] {
        let mut workflow = definition(
            vec![agent_step.clone()],
            WorkflowPlan::Step {
                step: "agent".to_string(),
            },
        );
        workflow.budgets.max_tokens = max_tokens;
        workflow.budgets.max_cost_micros = max_cost_micros;
        let failed = engine.run(request(workflow, json!({}))).await.unwrap();
        assert_eq!(failed.status, WorkflowRunStatus::Failed);
        assert_eq!(
            failed.failure.as_ref().unwrap().code,
            WorkflowFailureCode::BudgetExceeded
        );
        assert_eq!(failed.usage.tokens, 0);
        assert_eq!(failed.usage.cost_micros, Some(0));
    }
    assert_eq!(
        executions.load(Ordering::SeqCst),
        0,
        "an exhausted zero budget must fail closed before external agent execution"
    );
}

#[tokio::test]
async fn recovery_suspends_running_steps_without_false_completion() {
    let directory = tempfile::tempdir().unwrap();
    let repository =
        Arc::new(FileWorkflowRunRepository::new(directory.path().to_path_buf()).unwrap());
    let queued = snapshot("running-recovery", WorkflowRunStatus::Queued, 1);
    repository
        .create(
            &queued,
            &run_event("running-recovery", 1, WorkflowRunEventKind::RunQueued),
        )
        .await
        .unwrap();
    let mut running = snapshot("running-recovery", WorkflowRunStatus::Running, 2);
    running.steps.insert(
        "echo".to_string(),
        WorkflowStepSnapshot {
            id: "echo".to_string(),
            status: WorkflowStepStatus::Running,
            input_hash: "abc".to_string(),
            output: None,
            failure: None,
            attempts: 1,
        },
    );
    repository
        .commit(
            &running,
            &run_event("running-recovery", 2, WorkflowRunEventKind::RunStarted),
        )
        .await
        .unwrap();
    let engine = WorkflowRunEngine::new(
        repository.clone(),
        Arc::new(MockTools),
        Arc::new(MockAgents),
        Arc::new(MockDefinitions::default()),
        Arc::new(MockPolicy),
        Arc::new(MockSecrets),
        budgets(),
    );
    let recovered = engine.recover().await.unwrap().pop().unwrap();
    assert_eq!(recovered.status, WorkflowRunStatus::Suspended);
    assert_eq!(
        recovered.steps["echo"].status,
        WorkflowStepStatus::Suspended
    );
    let events = repository
        .events_since("running-recovery", 0)
        .await
        .unwrap();
    assert!(events
        .iter()
        .any(|event| matches!(event.kind, WorkflowRunEventKind::StepSuspended { .. })));
    assert!(!events
        .iter()
        .any(|event| matches!(event.kind, WorkflowRunEventKind::RunSucceeded { .. })));
}

#[tokio::test]
async fn outcome_aware_tool_approval_suspends_step_and_run() {
    let directory = tempfile::tempdir().unwrap();
    let engine = WorkflowRunEngine::new(
        Arc::new(FileWorkflowRunRepository::new(directory.path().to_path_buf()).unwrap()),
        Arc::new(ApprovalTools),
        Arc::new(MockAgents),
        Arc::new(MockDefinitions::default()),
        Arc::new(MockPolicy),
        Arc::new(MockSecrets),
        budgets(),
    );
    let workflow = definition(
        vec![tool_step("approval", "approval", json!({}))],
        WorkflowPlan::Step {
            step: "approval".to_string(),
        },
    );
    let suspended = engine.run(request(workflow, json!({}))).await.unwrap();
    assert_eq!(suspended.status, WorkflowRunStatus::Suspended);
    assert_eq!(
        suspended.steps["approval"].status,
        WorkflowStepStatus::Suspended
    );
    assert!(matches!(
        suspended.suspension,
        Some(WorkflowSuspensionContext::ToolApproval {
            ref step_id,
            ref tool,
            ..
        }) if step_id == "approval" && tool == "approval"
    ));
    assert!(matches!(
        engine
            .restart(&suspended.run_id, true, vec!["read".to_string()])
            .await,
        Err(WorkflowRunError::Preflight(_))
    ));
    let events = engine.progress(&suspended.run_id, 0).await.unwrap().events;
    assert!(matches!(
        events.last().unwrap().kind,
        WorkflowRunEventKind::RunSuspended { .. }
    ));
    assert!(!events.iter().any(|event| matches!(
        event.kind,
        WorkflowRunEventKind::RunSucceeded { .. } | WorkflowRunEventKind::RunFailed { .. }
    )));
}

#[tokio::test]
async fn omitted_definition_usage_limits_inherit_server_ceilings() {
    let directory = tempfile::tempdir().unwrap();
    let agent = WorkflowStepDefinition {
        id: "agent".to_string(),
        kind: WorkflowStepKind::Agent {
            agent: "reviewer".to_string(),
            prompt: json!({}),
            model: None,
            effort: None,
            capabilities: vec!["read".to_string()],
            structured_output_attempts: 1,
        },
        failure: FailurePolicy::FailFast,
        output_schema: None,
    };
    let mut workflow = definition(
        vec![agent],
        WorkflowPlan::Step {
            step: "agent".to_string(),
        },
    );
    workflow.budgets.max_tokens = None;
    workflow.budgets.max_cost_micros = None;
    let mut ceilings = budgets();
    ceilings.max_tokens = Some(5);
    ceilings.max_cost_micros = Some(1);
    let engine = WorkflowRunEngine::new(
        Arc::new(FileWorkflowRunRepository::new(directory.path().to_path_buf()).unwrap()),
        Arc::new(MockTools),
        Arc::new(MockAgents),
        Arc::new(MockDefinitions::default()),
        Arc::new(MockPolicy),
        Arc::new(MockSecrets),
        ceilings,
    );
    let failed = engine.run(request(workflow, json!({}))).await.unwrap();
    assert_eq!(
        failed.failure.as_ref().unwrap().code,
        WorkflowFailureCode::BudgetExceeded
    );
    assert_eq!(
        (failed.usage.tokens, failed.usage.cost_micros),
        (10, Some(2))
    );
}

#[tokio::test]
async fn map_cardinality_fails_before_item_futures_are_created() {
    let directory = tempfile::tempdir().unwrap();
    let mut workflow = definition(
        vec![tool_step(
            "mapped",
            "echo",
            json!({"from":"item","name":"item","pointer":""}),
        )],
        WorkflowPlan::Map {
            source: ValueRef::Args {
                pointer: "/items".to_string(),
            },
            item: "item".to_string(),
            body: Box::new(WorkflowPlan::Step {
                step: "mapped".to_string(),
            }),
        },
    );
    workflow.budgets.max_steps = 2;
    workflow.budgets.max_agents = 0;
    let engine = engine(directory.path(), MockDefinitions::default());
    let failed = engine
        .run(request(workflow, json!({"items":[1,2,3]})))
        .await
        .unwrap();
    assert_eq!(
        failed.failure.as_ref().unwrap().code,
        WorkflowFailureCode::BudgetExceeded
    );
    assert!(failed.steps.is_empty());
}

#[tokio::test]
async fn wall_timeout_finalizes_step_before_terminal_run_event() {
    let directory = tempfile::tempdir().unwrap();
    let mut workflow = definition(
        vec![tool_step("slow", "slow", json!({}))],
        WorkflowPlan::Step {
            step: "slow".to_string(),
        },
    );
    workflow.budgets.wall_time_ms = 50;
    let engine = engine(directory.path(), MockDefinitions::default());
    let failed = engine.run(request(workflow, json!({}))).await.unwrap();
    assert_eq!(failed.steps["slow"].status, WorkflowStepStatus::Failed);
    let events = engine.progress(&failed.run_id, 0).await.unwrap().events;
    let step = events
        .iter()
        .position(|event| matches!(event.kind, WorkflowRunEventKind::StepFailed { .. }))
        .unwrap();
    let run = events
        .iter()
        .position(|event| matches!(event.kind, WorkflowRunEventKind::RunFailed { .. }))
        .unwrap();
    assert!(step < run);
    assert_eq!(engine.runtime_resource_counts(), (0, 0));
}

#[tokio::test]
async fn timeout_and_cancel_commit_boundaries_remain_consistent_for_100_rounds() {
    let timeout_dir = tempfile::tempdir().unwrap();
    let timeout_engine = engine(timeout_dir.path(), MockDefinitions::default());
    for round in 0..100 {
        let mut workflow = definition(
            vec![tool_step("slow", "slow", json!({"round": round}))],
            WorkflowPlan::Step {
                step: "slow".to_string(),
            },
        );
        workflow.budgets.wall_time_ms = 1;
        let failed = timeout_engine
            .run(request(workflow, json!({})))
            .await
            .unwrap();
        assert_eq!(failed.status, WorkflowRunStatus::Failed, "round {round}");
        assert_eq!(
            failed.steps["slow"].status,
            WorkflowStepStatus::Failed,
            "round {round}"
        );
        let progress = timeout_engine.progress(&failed.run_id, 0).await.unwrap();
        assert_eq!(
            progress.snapshot.last_sequence,
            progress.events.len() as u64
        );
        assert!(progress
            .events
            .windows(2)
            .all(|pair| pair[1].sequence == pair[0].sequence + 1));
    }
    assert_eq!(timeout_engine.runtime_resource_counts(), (0, 0));

    let cancel_dir = tempfile::tempdir().unwrap();
    let cancel_engine = engine(cancel_dir.path(), MockDefinitions::default());
    for round in 0..100 {
        let workflow = definition(
            vec![tool_step("slow", "slow", json!({"round": round}))],
            WorkflowPlan::Step {
                step: "slow".to_string(),
            },
        );
        let started = cancel_engine
            .start(request(workflow, json!({})))
            .await
            .unwrap();
        let cancelled = cancel_engine.cancel(&started.run_id).await.unwrap();
        assert_eq!(
            cancelled.status,
            WorkflowRunStatus::Cancelled,
            "round {round}"
        );
        let progress = cancel_engine.progress(&started.run_id, 0).await.unwrap();
        assert_eq!(
            progress.snapshot.last_sequence,
            progress.events.len() as u64
        );
        assert!(!progress
            .events
            .iter()
            .any(|event| matches!(event.kind, WorkflowRunEventKind::RunSucceeded { .. })));
    }
    for _ in 0..1000 {
        if cancel_engine.runtime_resource_counts() == (0, 0) {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(cancel_engine.runtime_resource_counts(), (0, 0));
}

#[tokio::test]
async fn start_returns_typed_compile_error_without_run_residue() {
    let directory = tempfile::tempdir().unwrap();
    let engine = engine(directory.path(), MockDefinitions::default());
    let mut invalid = definition(
        vec![tool_step("echo", "echo", json!({}))],
        WorkflowPlan::Step {
            step: "echo".to_string(),
        },
    );
    invalid.workflow_schema = 99;
    assert!(matches!(
        engine.start(request(invalid, json!({}))).await,
        Err(WorkflowRunError::Compile(
            WorkflowCompileError::UnsupportedSchema(99)
        ))
    ));
    assert!(engine.list_run_ids().await.unwrap().is_empty());
}

#[tokio::test]
async fn unowned_running_tool_is_killed_before_workflow_suspends() {
    let directory = tempfile::tempdir().unwrap();
    let killed = Arc::new(AtomicBool::new(false));
    let engine = WorkflowRunEngine::new(
        Arc::new(FileWorkflowRunRepository::new(directory.path().to_path_buf()).unwrap()),
        Arc::new(RunningTools(killed.clone())),
        Arc::new(MockAgents),
        Arc::new(MockDefinitions::default()),
        Arc::new(MockPolicy),
        Arc::new(MockSecrets),
        budgets(),
    );
    let workflow = definition(
        vec![tool_step("detached", "detached", json!({}))],
        WorkflowPlan::Step {
            step: "detached".to_string(),
        },
    );
    let suspended = engine.run(request(workflow, json!({}))).await.unwrap();
    assert_eq!(suspended.status, WorkflowRunStatus::Suspended);
    assert!(killed.load(Ordering::SeqCst));
    assert!(matches!(
        suspended.suspension,
        Some(WorkflowSuspensionContext::ToolRunning {
            ref step_id,
            ref tool,
            killed: true,
            ..
        }) if step_id == "detached" && tool == "detached"
    ));
    assert!(matches!(
        engine
            .restart(&suspended.run_id, true, vec!["read".to_string()])
            .await,
        Err(WorkflowRunError::Preflight(_))
    ));
}

struct UnpricedWorkflowAgents {
    calls: AtomicUsize,
    fail: bool,
}

#[async_trait]
impl AgentStepPort for UnpricedWorkflowAgents {
    async fn resolve(
        &self,
        name: &str,
        _session_id: &str,
    ) -> Result<Option<NamedAgentSpec>, String> {
        Ok(Some(NamedAgentSpec {
            name: name.into(),
            allowed_capabilities: BTreeSet::from(["read".into()]),
            profile: None,
            cost_supported: false,
        }))
    }
    async fn execute(
        &self,
        _spec: &NamedAgentSpec,
        _prompt: Value,
        _model: Option<&str>,
        _effort: Option<&str>,
        _capabilities: &BTreeSet<String>,
        _session_id: &str,
        _root_run_id: &str,
        _cancellation: tokio_util::sync::CancellationToken,
    ) -> Result<AgentStepResult, String> {
        let index = self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(AgentStepResult {
            output: if index == 0 {
                json!("invalid")
            } else {
                json!({"ok":true})
            },
            tokens: if index == 0 { 17 } else { 23 },
            cost_micros: None,
            attempt_id: None,
            failure: self.fail.then(|| WorkflowFailure {
                code: WorkflowFailureCode::ExecutionFailed,
                message: "child failed after provider usage".into(),
                retryable: false,
            }),
        })
    }
}

fn workflow_agent_step() -> WorkflowStepDefinition {
    WorkflowStepDefinition {
        id: "review".into(),
        kind: WorkflowStepKind::Agent {
            agent: "reviewer".into(),
            prompt: json!({"from":"args","pointer":""}),
            model: None,
            effort: None,
            capabilities: vec!["read".into()],
            structured_output_attempts: 2,
        },
        failure: FailurePolicy::FailFast,
        output_schema: Some(
            json!({"type":"object","properties":{"ok":{"type":"boolean"}},"required":["ok"],"additionalProperties":false}),
        ),
    }
}

fn unpriced_workflow_engine(
    directory: &std::path::Path,
    agents: Arc<dyn AgentStepPort>,
) -> Arc<WorkflowRunEngine> {
    let mut limits = budgets();
    limits.max_cost_micros = None;
    WorkflowRunEngine::new(
        Arc::new(FileWorkflowRunRepository::new(directory.to_path_buf()).unwrap()),
        Arc::new(MockTools),
        agents,
        Arc::new(MockDefinitions::default()),
        Arc::new(MockPolicy),
        Arc::new(MockSecrets),
        limits,
    )
}

#[tokio::test]
async fn workflow_agent_unpriced_finite_money_is_rejected_before_dispatch() {
    let directory = tempfile::tempdir().unwrap();
    let agents = Arc::new(UnpricedWorkflowAgents {
        calls: AtomicUsize::new(0),
        fail: false,
    });
    let engine = unpriced_workflow_engine(directory.path(), agents.clone());
    let workflow = definition(vec![workflow_agent_step()], choice_leaf("review"));
    assert!(matches!(
        engine.run(request(workflow, json!({}))).await,
        Err(WorkflowRunError::UnsupportedMonetaryBudget)
    ));
    assert_eq!(agents.calls.load(Ordering::SeqCst), 0);
    assert!(engine.list_run_ids().await.unwrap().is_empty());
}

#[tokio::test]
async fn workflow_agent_unpriced_structured_retries_accumulate_usage_with_null_cost() {
    let directory = tempfile::tempdir().unwrap();
    let agents = Arc::new(UnpricedWorkflowAgents {
        calls: AtomicUsize::new(0),
        fail: false,
    });
    let engine = unpriced_workflow_engine(directory.path(), agents.clone());
    let mut workflow = definition(vec![workflow_agent_step()], choice_leaf("review"));
    workflow.budgets.max_cost_micros = None;
    let result = engine.run(request(workflow, json!({}))).await.unwrap();
    assert_eq!(result.status, WorkflowRunStatus::Succeeded);
    assert_eq!(result.usage.tokens, 40);
    assert_eq!(result.usage.agents, 2);
    assert_eq!(result.usage.cost_micros, None);
    assert!(serde_json::to_value(&result.usage).unwrap()["cost_micros"].is_null());
    assert_eq!(
        engine
            .progress(&result.run_id, 0)
            .await
            .unwrap()
            .snapshot
            .usage,
        result.usage
    );
}

#[tokio::test]
async fn workflow_agent_unpriced_failed_attempt_retains_observed_usage() {
    let directory = tempfile::tempdir().unwrap();
    let agents = Arc::new(UnpricedWorkflowAgents {
        calls: AtomicUsize::new(0),
        fail: true,
    });
    let engine = unpriced_workflow_engine(directory.path(), agents.clone());
    let mut workflow = definition(vec![workflow_agent_step()], choice_leaf("review"));
    workflow.budgets.max_cost_micros = None;
    let result = engine.run(request(workflow, json!({}))).await.unwrap();
    assert_eq!(result.status, WorkflowRunStatus::Failed);
    assert_eq!(result.usage.tokens, 17);
    assert_eq!(result.usage.cost_micros, None);
    assert_eq!(agents.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn workflow_agent_unpriced_token_limit_is_accounted_after_provider_and_prevents_retry() {
    let directory = tempfile::tempdir().unwrap();
    let agents = Arc::new(UnpricedWorkflowAgents {
        calls: AtomicUsize::new(0),
        fail: false,
    });
    let engine = unpriced_workflow_engine(directory.path(), agents.clone());
    let mut workflow = definition(vec![workflow_agent_step()], choice_leaf("review"));
    workflow.budgets.max_cost_micros = None;
    workflow.budgets.max_tokens = Some(10);
    let result = engine.run(request(workflow, json!({}))).await.unwrap();
    assert_eq!(result.status, WorkflowRunStatus::Failed);
    assert_eq!(
        result.failure.unwrap().code,
        WorkflowFailureCode::BudgetExceeded
    );
    assert_eq!(
        result.usage.tokens, 17,
        "provider completion can exceed cap; no next attempt is admitted"
    );
    assert_eq!(agents.calls.load(Ordering::SeqCst), 1);
}

struct DrainingWorkflowAgents {
    started: tokio::sync::Semaphore,
    stopped: tokio::sync::Semaphore,
    cancellation: std::sync::Mutex<Option<tokio_util::sync::CancellationToken>>,
    consumed: AtomicBool,
    missing_usage: bool,
}

impl DrainingWorkflowAgents {
    fn new(missing_usage: bool) -> Self {
        Self {
            started: tokio::sync::Semaphore::new(0),
            stopped: tokio::sync::Semaphore::new(0),
            cancellation: std::sync::Mutex::new(None),
            consumed: AtomicBool::new(false),
            missing_usage,
        }
    }
}

#[async_trait]
impl AgentStepPort for DrainingWorkflowAgents {
    async fn resolve(
        &self,
        name: &str,
        _session_id: &str,
    ) -> Result<Option<NamedAgentSpec>, String> {
        Ok(Some(NamedAgentSpec {
            name: name.into(),
            allowed_capabilities: BTreeSet::from(["read".into()]),
            profile: None,
            cost_supported: false,
        }))
    }
    async fn execute(
        &self,
        _spec: &NamedAgentSpec,
        _prompt: Value,
        _model: Option<&str>,
        _effort: Option<&str>,
        _capabilities: &BTreeSet<String>,
        _session_id: &str,
        _root_run_id: &str,
        cancellation: tokio_util::sync::CancellationToken,
    ) -> Result<AgentStepResult, String> {
        *self.cancellation.lock().unwrap() = Some(cancellation.clone());
        let _on_drop = cancellation.clone().drop_guard();
        self.started.add_permits(1);
        cancellation.cancelled().await;
        let permit = self.stopped.acquire().await.unwrap();
        permit.forget();
        Err("cancelled attempt usage is consumed by drain".into())
    }
    async fn drain_cancelled(
        &self,
        _root_run_id: &str,
    ) -> (Vec<Result<AgentStepResult, String>>, Option<String>) {
        let token = self.cancellation.lock().unwrap().clone();
        if !token.is_some_and(|token| token.is_cancelled()) {
            return (vec![], None);
        }
        let permit = self.stopped.acquire().await.unwrap();
        permit.forget();
        if self.consumed.swap(true, Ordering::SeqCst) {
            return (vec![], None);
        }
        (
            vec![if self.missing_usage {
                Err("cumulative token observation unavailable".into())
            } else {
                Ok(AgentStepResult {
                    output: Value::Null,
                    tokens: 17,
                    cost_micros: None,
                    attempt_id: None,
                    failure: None,
                })
            }],
            None,
        )
    }
}

#[tokio::test]
async fn workflow_agent_cancel_waits_for_stop_and_retains_cancelled_usage() {
    let directory = tempfile::tempdir().unwrap();
    let agents = Arc::new(DrainingWorkflowAgents::new(false));
    let engine = unpriced_workflow_engine(directory.path(), agents.clone());
    let mut workflow = definition(vec![workflow_agent_step()], choice_leaf("review"));
    workflow.budgets.max_cost_micros = None;
    let running = engine.start(request(workflow, json!({}))).await.unwrap();
    agents.started.acquire().await.unwrap().forget();
    let cancelling = {
        let engine = engine.clone();
        let id = running.run_id.clone();
        tokio::spawn(async move { engine.cancel(&id).await })
    };
    tokio::task::yield_now().await;
    assert_eq!(
        engine
            .progress(&running.run_id, 0)
            .await
            .unwrap()
            .snapshot
            .status,
        WorkflowRunStatus::Running
    );
    assert!(!cancelling.is_finished());
    agents.stopped.add_permits(8);
    let cancelled = cancelling.await.unwrap().unwrap();
    assert_eq!(cancelled.status, WorkflowRunStatus::Cancelled);
    assert_eq!(cancelled.usage.tokens, 17);
    assert_eq!(cancelled.usage.cost_micros, None);
}

#[tokio::test]
async fn workflow_agent_wall_time_waits_for_child_stop_and_accounts_usage() {
    let directory = tempfile::tempdir().unwrap();
    let agents = Arc::new(DrainingWorkflowAgents::new(false));
    let engine = unpriced_workflow_engine(directory.path(), agents.clone());
    let mut workflow = definition(vec![workflow_agent_step()], choice_leaf("review"));
    workflow.budgets.max_cost_micros = None;
    workflow.budgets.wall_time_ms = 10_000;
    let running = engine.start(request(workflow, json!({}))).await.unwrap();
    agents.started.acquire().await.unwrap().forget();
    tokio::time::pause();
    tokio::time::advance(std::time::Duration::from_millis(10_000)).await;
    let token = agents.cancellation.lock().unwrap().clone().unwrap();
    token.cancelled().await;
    assert_eq!(
        engine
            .progress(&running.run_id, 0)
            .await
            .unwrap()
            .snapshot
            .status,
        WorkflowRunStatus::Running
    );
    agents.stopped.add_permits(8);
    let terminal = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let snapshot = engine.progress(&running.run_id, 0).await.unwrap().snapshot;
            if snapshot.status.is_terminal() {
                break snapshot;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        terminal.failure.unwrap().code,
        WorkflowFailureCode::BudgetExceeded
    );
    assert_eq!(terminal.usage.tokens, 17);
}

#[tokio::test]
async fn workflow_agent_stopped_missing_usage_fails_without_permanent_cleanup_pending() {
    let directory = tempfile::tempdir().unwrap();
    let agents = Arc::new(DrainingWorkflowAgents::new(true));
    let engine = unpriced_workflow_engine(directory.path(), agents.clone());
    let mut workflow = definition(vec![workflow_agent_step()], choice_leaf("review"));
    workflow.budgets.max_cost_micros = None;
    let running = engine.start(request(workflow, json!({}))).await.unwrap();
    agents.started.acquire().await.unwrap().forget();
    let cancelling = {
        let engine = engine.clone();
        let id = running.run_id.clone();
        tokio::spawn(async move { engine.cancel(&id).await })
    };
    tokio::task::yield_now().await;
    agents.stopped.add_permits(8);
    let failed = cancelling.await.unwrap().unwrap();
    assert_eq!(failed.status, WorkflowRunStatus::Failed);
    assert_eq!(
        failed.failure.unwrap().code,
        WorkflowFailureCode::ExecutionFailed
    );
    assert_eq!(failed.usage.cost_micros, None);
}

struct RetainedWorkflowAgents {
    started: tokio::sync::Semaphore,
    release: tokio::sync::Semaphore,
    returned: tokio::sync::Semaphore,
    retained: AtomicBool,
}

impl RetainedWorkflowAgents {
    fn result() -> AgentStepResult {
        AgentStepResult {
            output: json!({"ok": true}),
            tokens: 17,
            cost_micros: None,
            failure: None,
            attempt_id: Some("retained-attempt".into()),
        }
    }
}

#[async_trait]
impl AgentStepPort for RetainedWorkflowAgents {
    async fn resolve(
        &self,
        name: &str,
        _session_id: &str,
    ) -> Result<Option<NamedAgentSpec>, String> {
        Ok(Some(NamedAgentSpec {
            name: name.into(),
            allowed_capabilities: BTreeSet::from(["read".into()]),
            profile: None,
            cost_supported: false,
        }))
    }
    async fn execute(
        &self,
        _spec: &NamedAgentSpec,
        _prompt: Value,
        _model: Option<&str>,
        _effort: Option<&str>,
        _capabilities: &BTreeSet<String>,
        _session_id: &str,
        _root_run_id: &str,
        _cancellation: tokio_util::sync::CancellationToken,
    ) -> Result<AgentStepResult, String> {
        self.started.add_permits(1);
        self.release.acquire().await.unwrap().forget();
        self.retained.store(true, Ordering::SeqCst);
        self.returned.add_permits(1);
        Ok(Self::result())
    }
    fn acknowledge_result(&self, _id: &str) -> bool {
        self.retained.swap(false, Ordering::SeqCst)
    }
    async fn drain_cancelled(
        &self,
        _run: &str,
    ) -> (Vec<Result<AgentStepResult, String>>, Option<String>) {
        (
            if self.retained.swap(false, Ordering::SeqCst) {
                vec![Ok(Self::result())]
            } else {
                vec![]
            },
            None,
        )
    }
}

async fn workflow_agent_handoff_control(wall_timeout: bool) {
    let directory = tempfile::tempdir().unwrap();
    let agents = Arc::new(RetainedWorkflowAgents {
        started: tokio::sync::Semaphore::new(0),
        release: tokio::sync::Semaphore::new(0),
        returned: tokio::sync::Semaphore::new(0),
        retained: AtomicBool::new(false),
    });
    let engine = unpriced_workflow_engine(directory.path(), agents.clone());
    let mut workflow = definition(vec![workflow_agent_step()], choice_leaf("review"));
    workflow.budgets.max_cost_micros = None;
    let running = engine.start(request(workflow, json!({}))).await.unwrap();
    agents.started.acquire().await.unwrap().forget();
    let (active_ledger, cancellation) = engine.test_active_ledger(&running.run_id);
    let ledger = active_ledger.lock().await;
    agents.release.add_permits(1);
    agents.returned.acquire().await.unwrap().forget();
    // The result exists, but its handoff cannot commit while this ledger is held.
    tokio::task::yield_now().await;
    let cancel = if wall_timeout {
        tokio::time::pause();
        tokio::time::advance(std::time::Duration::from_secs(10)).await;
        cancellation.cancelled().await;
        None
    } else {
        let engine = engine.clone();
        let id = running.run_id.clone();
        let task = tokio::spawn(async move { engine.cancel(&id).await });
        cancellation.cancelled().await;
        Some(task)
    };
    drop(ledger);
    if let Some(cancel) = cancel {
        cancel.await.unwrap().unwrap();
    }
    let terminal = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let snapshot = engine.progress(&running.run_id, 0).await.unwrap().snapshot;
            if snapshot.status.is_terminal() && !engine.is_run_active(&running.run_id) {
                break snapshot;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        terminal.usage.tokens, 17,
        "exactly one result handoff survives cancellation/drop"
    );
    assert_eq!(terminal.usage.cost_micros, None);
    assert_eq!(
        terminal.status,
        if wall_timeout {
            WorkflowRunStatus::Failed
        } else {
            WorkflowRunStatus::Cancelled
        }
    );
    assert!(!agents.retained.load(Ordering::SeqCst));
}

#[tokio::test]
async fn workflow_agent_result_handoff_cancel_counts_once() {
    workflow_agent_handoff_control(false).await;
}

#[tokio::test]
async fn workflow_agent_result_handoff_timeout_retains_usage() {
    workflow_agent_handoff_control(true).await;
}

fn completed_prefix_checkpoint(run_id: &str) -> WorkflowRunSnapshot {
    use sha2::{Digest, Sha256};
    let mut checkpoint = snapshot(run_id, WorkflowRunStatus::Suspended, 1);
    let mut prefix = tool_step("prefix", "echo", json!({"from":"args"}));
    prefix.output_schema = Some(
        json!({"type":"object","required":["value"],"properties":{"value":{"type":"integer"}},"additionalProperties":false}),
    );
    checkpoint.definition = definition(
        vec![
            prefix,
            tool_step("suffix", "echo", json!({"from":"step","step":"prefix"})),
        ],
        WorkflowPlan::Sequence {
            nodes: vec![choice_leaf("prefix"), choice_leaf("suffix")],
        },
    );
    checkpoint.definition_bundle.root_id = checkpoint.definition.id.clone();
    checkpoint.definition_bundle.root_revision = checkpoint.definition.revision;
    checkpoint.definition_bundle.definitions = BTreeMap::from([(
        WorkflowDefinitionBundle::key(&checkpoint.definition.id, checkpoint.definition.revision),
        checkpoint.definition.clone(),
    )]);
    checkpoint.definition_bundle_hash = hex::encode(Sha256::digest(
        serde_json::to_vec(&checkpoint.definition_bundle).unwrap(),
    ));
    checkpoint.validated_args = json!({"value":42});
    checkpoint.steps.insert(
        "prefix".into(),
        WorkflowStepSnapshot {
            id: "prefix".into(),
            status: WorkflowStepStatus::Succeeded,
            input_hash: hex::encode(Sha256::digest(
                serde_json::to_vec(&checkpoint.validated_args).unwrap(),
            )),
            output: Some(checkpoint.validated_args.clone()),
            failure: None,
            attempts: 1,
        },
    );
    checkpoint.usage.steps = 1;
    checkpoint.usage.tokens = 3;
    checkpoint.usage.cost_micros = Some(5);
    checkpoint.suspension = Some(WorkflowSuspensionContext::Recovery {
        reason: "process restarted".into(),
    });
    checkpoint
}

async fn seed_completed_prefix(
    repository: &dyn WorkflowRunRepository,
    checkpoint: &WorkflowRunSnapshot,
) {
    repository
        .create(
            checkpoint,
            &run_event(
                &checkpoint.run_id,
                checkpoint.last_sequence,
                WorkflowRunEventKind::RunSuspended {
                    reason: "process restarted".into(),
                },
            ),
        )
        .await
        .unwrap();
}

async fn await_continuation(engine: &WorkflowRunEngine, run_id: &str) -> WorkflowProgress {
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            let progress = engine.progress(run_id, 0).await.unwrap();
            if progress.snapshot.status.is_terminal() && !engine.is_run_active(run_id) {
                return progress;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("continuation settles")
}

#[tokio::test]
async fn completed_prefix_continuation_preserves_checkpoint_and_finalizes_complete_prefix() {
    for complete in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let repository =
            Arc::new(FileWorkflowRunRepository::new(directory.path().to_path_buf()).unwrap());
        let mut checkpoint = completed_prefix_checkpoint("completed-prefix");
        if complete {
            let mut suffix = checkpoint.steps["prefix"].clone();
            suffix.id = "suffix".into();
            checkpoint.steps.insert("suffix".into(), suffix);
            checkpoint.usage.steps = 2;
        }
        seed_completed_prefix(repository.as_ref(), &checkpoint).await;
        let engine = engine(directory.path(), MockDefinitions::default());
        let started = engine
            .continue_completed_prefix(
                &checkpoint.run_id,
                &checkpoint.session_id,
                true,
                vec!["read".into()],
            )
            .await
            .unwrap();
        assert_eq!(started.run_id, checkpoint.run_id);
        let progress = await_continuation(&engine, &checkpoint.run_id).await;
        let saved = progress.snapshot;
        assert_eq!(saved.status, WorkflowRunStatus::Succeeded);
        assert_eq!(saved.output, Some(json!({"value":42})));
        assert_eq!(saved.definition_bundle, checkpoint.definition_bundle);
        assert_eq!(
            saved.definition_bundle_hash,
            checkpoint.definition_bundle_hash
        );
        assert_eq!(saved.validated_args, checkpoint.validated_args);
        assert_eq!(saved.created_at, checkpoint.created_at);
        assert_eq!(saved.steps["prefix"], checkpoint.steps["prefix"]);
        assert_eq!(saved.usage.steps, 2);
        assert_eq!(saved.usage.tokens, 3);
        assert_eq!(saved.usage.cost_micros, Some(5));
        assert_eq!(saved.steps["suffix"].attempts, 1);
        assert!(!progress
            .events
            .iter()
            .any(|event| event.step_id.as_deref() == Some("prefix")));
        assert_eq!(
            progress
                .events
                .iter()
                .filter(|event| matches!(event.kind, WorkflowRunEventKind::StepStarted))
                .count(),
            usize::from(!complete)
        );
        assert!(progress
            .events
            .windows(2)
            .all(|pair| pair[1].sequence == pair[0].sequence + 1));
        assert_eq!(engine.runtime_resource_counts(), (0, 0));
    }
}

#[tokio::test]
async fn completed_prefix_continuation_refuses_ambiguous_or_invalid_checkpoints_without_writes() {
    type Mutation = fn(&mut WorkflowRunSnapshot);
    let cases: &[(&str, Mutation)] = &[
        ("running", |s| s.status = WorkflowRunStatus::Running),
        ("terminal", |s| s.status = WorkflowRunStatus::Succeeded),
        ("nested-run", |s| s.parent_run_id = Some("parent".into())),
        ("approval", |s| {
            s.suspension = Some(WorkflowSuspensionContext::ToolApproval {
                step_id: "prefix".into(),
                tool: "echo".into(),
                tool_call_id: "call".into(),
            })
        }),
        ("running-tool", |s| {
            s.suspension = Some(WorkflowSuspensionContext::ToolRunning {
                step_id: "prefix".into(),
                tool: "echo".into(),
                tool_call_id: "call".into(),
                killed: true,
            })
        }),
        ("empty-prefix", |s| {
            s.steps.clear();
            s.usage.steps = 0;
        }),
        ("missing-output", |s| {
            s.steps.get_mut("prefix").unwrap().output = None
        }),
        ("bad-output", |s| {
            s.steps.get_mut("prefix").unwrap().output = Some(json!({"value":"wrong-type"}))
        }),
        ("bad-input-hash", |s| {
            s.steps.get_mut("prefix").unwrap().input_hash = "changed".into()
        }),
        ("extra-reservation", |s| s.usage.steps += 1),
        ("entered-suffix", |s| {
            let mut state = s.steps["prefix"].clone();
            state.id = "suffix".into();
            state.status = WorkflowStepStatus::Suspended;
            s.steps.insert("suffix".into(), state);
            s.usage.steps += 1;
        }),
        ("non-prefix-success", |s| {
            let mut state = s.steps.remove("prefix").unwrap();
            state.id = "suffix".into();
            s.steps.insert("suffix".into(), state);
        }),
        ("extra-state", |s| {
            let mut state = s.steps["prefix"].clone();
            state.id = "unknown".into();
            s.steps.insert("unknown".into(), state);
        }),
        ("changed-bundle", |s| {
            s.definition_bundle.publication_revision += 1
        }),
        ("changed-schema", |s| s.definition.workflow_schema = 99),
        ("nonflat", |s| {
            s.definition.plan = WorkflowPlan::Parallel {
                nodes: vec![choice_leaf("prefix"), choice_leaf("suffix")],
            }
        }),
        ("agent", |s| {
            let mut step = workflow_agent_step();
            step.id = "suffix".into();
            s.definition.steps[1] = step;
        }),
        ("mutating", |s| {
            if let WorkflowStepKind::Tool { capabilities, .. } = &mut s.definition.steps[1].kind {
                capabilities.push("write".into());
            }
        }),
        ("steps-exhausted", |s| s.definition.budgets.max_steps = 1),
        ("tokens-exceeded", |s| s.usage.tokens = 10001),
        ("cost-exceeded", |s| s.usage.cost_micros = Some(10001)),
        ("wall-time-exhausted", |s| {
            s.created_at -= chrono::Duration::seconds(11)
        }),
    ];
    for (label, mutate) in cases {
        let directory = tempfile::tempdir().unwrap();
        let repository = FileWorkflowRunRepository::new(directory.path().to_path_buf()).unwrap();
        let mut checkpoint = completed_prefix_checkpoint(label);
        mutate(&mut checkpoint);
        if *label != "changed-bundle" {
            use sha2::{Digest, Sha256};
            checkpoint.definition_bundle.definitions.insert(
                WorkflowDefinitionBundle::key(
                    &checkpoint.definition.id,
                    checkpoint.definition.revision,
                ),
                checkpoint.definition.clone(),
            );
            checkpoint.definition_bundle_hash = hex::encode(Sha256::digest(
                serde_json::to_vec(&checkpoint.definition_bundle).unwrap(),
            ));
        }
        seed_completed_prefix(&repository, &checkpoint).await;
        let engine = engine(directory.path(), MockDefinitions::default());
        assert!(
            engine
                .continue_completed_prefix(label, &checkpoint.session_id, true, vec!["read".into()])
                .await
                .is_err(),
            "{label}"
        );
        assert_eq!(
            repository.load(label).await.unwrap().unwrap(),
            checkpoint,
            "{label}"
        );
        assert_eq!(
            repository.events_since(label, 0).await.unwrap().len(),
            1,
            "{label}"
        );
        assert_eq!(engine.runtime_resource_counts(), (0, 0), "{label}");
    }
    let directory = tempfile::tempdir().unwrap();
    let repository = FileWorkflowRunRepository::new(directory.path().to_path_buf()).unwrap();
    let checkpoint = completed_prefix_checkpoint("session-policy");
    seed_completed_prefix(&repository, &checkpoint).await;
    let engine = engine(directory.path(), MockDefinitions::default());
    assert!(matches!(
        engine
            .continue_completed_prefix("missing", &checkpoint.session_id, true, vec!["read".into()])
            .await,
        Err(WorkflowRunError::NotFound)
    ));
    assert!(matches!(
        engine
            .continue_completed_prefix(
                &checkpoint.run_id,
                "other-session",
                true,
                vec!["read".into()]
            )
            .await,
        Err(WorkflowRunError::NotFound)
    ));
    assert!(engine
        .continue_completed_prefix(
            &checkpoint.run_id,
            &checkpoint.session_id,
            false,
            vec!["read".into()]
        )
        .await
        .is_err());
    assert_eq!(
        repository.load(&checkpoint.run_id).await.unwrap().unwrap(),
        checkpoint
    );
}

struct RefuseContinuationCommit(FileWorkflowRunRepository);

#[async_trait]
impl WorkflowRunRepository for RefuseContinuationCommit {
    async fn create(
        &self,
        snapshot: &WorkflowRunSnapshot,
        event: &WorkflowRunEvent,
    ) -> std::io::Result<()> {
        self.0.create(snapshot, event).await
    }
    async fn commit(
        &self,
        snapshot: &WorkflowRunSnapshot,
        event: &WorkflowRunEvent,
    ) -> std::io::Result<()> {
        if matches!(event.kind, WorkflowRunEventKind::RunStarted) {
            return Err(std::io::Error::other(
                "injected continuation commit refusal",
            ));
        }
        self.0.commit(snapshot, event).await
    }
    async fn load(&self, run_id: &str) -> std::io::Result<Option<WorkflowRunSnapshot>> {
        self.0.load(run_id).await
    }
    async fn events_since(
        &self,
        run_id: &str,
        sequence: u64,
    ) -> std::io::Result<Vec<WorkflowRunEvent>> {
        self.0.events_since(run_id, sequence).await
    }
    async fn list_run_ids(&self) -> std::io::Result<Vec<String>> {
        self.0.list_run_ids().await
    }
}

#[tokio::test]
async fn completed_prefix_continuation_commit_failure_dispatches_nothing() {
    let directory = tempfile::tempdir().unwrap();
    let repository = Arc::new(RefuseContinuationCommit(
        FileWorkflowRunRepository::new(directory.path().to_path_buf()).unwrap(),
    ));
    let checkpoint = completed_prefix_checkpoint("commit-failure");
    seed_completed_prefix(repository.as_ref(), &checkpoint).await;
    let tools = Arc::new(ContextRecordingTools::default());
    let engine = WorkflowRunEngine::new(
        repository.clone(),
        tools.clone(),
        Arc::new(MockAgents),
        Arc::new(MockDefinitions::default()),
        Arc::new(MockPolicy),
        Arc::new(MockSecrets),
        budgets(),
    );
    assert!(matches!(
        engine
            .continue_completed_prefix(
                &checkpoint.run_id,
                &checkpoint.session_id,
                true,
                vec!["read".into()]
            )
            .await,
        Err(WorkflowRunError::Storage(_))
    ));
    assert_eq!(tools.calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        repository.load(&checkpoint.run_id).await.unwrap().unwrap(),
        checkpoint
    );
    assert_eq!(
        repository
            .events_since(&checkpoint.run_id, 0)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(engine.runtime_resource_counts(), (0, 0));
}

struct ContinuationGateTools {
    calls: AtomicUsize,
    entered: tokio::sync::Notify,
    release: tokio::sync::Semaphore,
}

#[async_trait]
impl ToolExecutor for ContinuationGateTools {
    async fn execute(&self, call: &ToolCall) -> Result<ToolResult, ToolError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.entered.notify_one();
        let _permit = self.release.acquire().await.unwrap();
        Ok(ToolResult::text(true, call.function.arguments.clone()))
    }
    fn list_tools(&self) -> Vec<bamboo_agent_core::tools::ToolSchema> {
        Vec::new()
    }
}

#[tokio::test]
async fn completed_prefix_concurrent_continuations_have_one_owner_and_cancel_keeps_usage() {
    for round in 0..20 {
        let directory = tempfile::tempdir().unwrap();
        let repository =
            Arc::new(FileWorkflowRunRepository::new(directory.path().to_path_buf()).unwrap());
        let checkpoint = completed_prefix_checkpoint(&format!("continue-race-{round}"));
        seed_completed_prefix(repository.as_ref(), &checkpoint).await;
        let tools = Arc::new(ContinuationGateTools {
            calls: AtomicUsize::new(0),
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Semaphore::new(0),
        });
        let engine = WorkflowRunEngine::new(
            repository,
            tools.clone(),
            Arc::new(MockAgents),
            Arc::new(MockDefinitions::default()),
            Arc::new(MockPolicy),
            Arc::new(MockSecrets),
            budgets(),
        );
        let (left, right) = tokio::join!(
            engine.continue_completed_prefix(
                &checkpoint.run_id,
                &checkpoint.session_id,
                true,
                vec!["read".into()]
            ),
            engine.continue_completed_prefix(
                &checkpoint.run_id,
                &checkpoint.session_id,
                true,
                vec!["read".into()]
            ),
        );
        assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
        tokio::time::timeout(std::time::Duration::from_secs(3), tools.entered.notified())
            .await
            .expect("suffix entered its explicit gate");
        assert_eq!(tools.calls.load(Ordering::SeqCst), 1);
        let cancelled = engine.cancel(&checkpoint.run_id).await.unwrap();
        assert_eq!(cancelled.status, WorkflowRunStatus::Cancelled);
        // Ordinary Tool cancellation keeps its existing lifecycle. Release the
        // test Tool explicitly, then require its owner to drain without sleep.
        tools.release.add_permits(1);
        let progress = await_continuation(&engine, &checkpoint.run_id).await;
        assert_eq!(
            progress.snapshot.steps["prefix"],
            checkpoint.steps["prefix"]
        );
        assert_eq!(progress.snapshot.usage.tokens, checkpoint.usage.tokens);
        assert_eq!(
            progress.snapshot.usage.cost_micros,
            checkpoint.usage.cost_micros
        );
        assert_eq!(
            progress
                .events
                .iter()
                .filter(|event| matches!(event.kind, WorkflowRunEventKind::RunStarted))
                .count(),
            1
        );
        assert!(!progress
            .events
            .iter()
            .any(|event| event.step_id.as_deref() == Some("prefix")));
        assert_eq!(tools.calls.load(Ordering::SeqCst), 1);
    }
}

struct PausedInactiveCancellation {
    repository: FileWorkflowRunRepository,
    loads: AtomicUsize,
    paused: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

#[async_trait]
impl WorkflowRunRepository for PausedInactiveCancellation {
    async fn create(
        &self,
        snapshot: &WorkflowRunSnapshot,
        event: &WorkflowRunEvent,
    ) -> std::io::Result<()> {
        self.repository.create(snapshot, event).await
    }
    async fn commit(
        &self,
        snapshot: &WorkflowRunSnapshot,
        event: &WorkflowRunEvent,
    ) -> std::io::Result<()> {
        self.repository.commit(snapshot, event).await
    }
    async fn load(&self, run_id: &str) -> std::io::Result<Option<WorkflowRunSnapshot>> {
        if self.loads.fetch_add(1, Ordering::SeqCst) == 1 {
            self.paused.notify_one();
            self.release.notified().await;
        }
        self.repository.load(run_id).await
    }
    async fn events_since(
        &self,
        run_id: &str,
        since: u64,
    ) -> std::io::Result<Vec<WorkflowRunEvent>> {
        self.repository.events_since(run_id, since).await
    }
    async fn list_run_ids(&self) -> std::io::Result<Vec<String>> {
        self.repository.list_run_ids().await
    }
}

#[tokio::test]
async fn completed_prefix_inactive_cancel_owns_admission_before_terminal_commit() {
    let directory = tempfile::tempdir().unwrap();
    let repository = Arc::new(PausedInactiveCancellation {
        repository: FileWorkflowRunRepository::new(directory.path().to_path_buf()).unwrap(),
        loads: AtomicUsize::new(0),
        paused: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
    });
    let checkpoint = completed_prefix_checkpoint("inactive-cancel-race");
    seed_completed_prefix(repository.as_ref(), &checkpoint).await;
    let tools = Arc::new(ContextRecordingTools::default());
    let engine = WorkflowRunEngine::new(
        repository.clone(),
        tools.clone(),
        Arc::new(MockAgents),
        Arc::new(MockDefinitions::default()),
        Arc::new(MockPolicy),
        Arc::new(MockSecrets),
        budgets(),
    );
    let cancellation = {
        let engine = engine.clone();
        let run_id = checkpoint.run_id.clone();
        tokio::spawn(async move { engine.cancel(&run_id).await })
    };
    tokio::time::timeout(
        std::time::Duration::from_secs(3),
        repository.paused.notified(),
    )
    .await
    .unwrap();
    assert!(engine.is_run_active(&checkpoint.run_id));
    assert!(engine
        .continue_completed_prefix(
            &checkpoint.run_id,
            &checkpoint.session_id,
            true,
            vec!["read".into()]
        )
        .await
        .is_err());
    assert_eq!(tools.calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        repository
            .repository
            .load(&checkpoint.run_id)
            .await
            .unwrap()
            .unwrap(),
        checkpoint
    );
    repository.release.notify_one();
    let cancelled = tokio::time::timeout(std::time::Duration::from_secs(3), cancellation)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(cancelled.status, WorkflowRunStatus::Cancelled);
    assert_eq!(cancelled.usage, checkpoint.usage);
    assert_eq!(cancelled.steps, checkpoint.steps);
    assert!(engine
        .continue_completed_prefix(
            &checkpoint.run_id,
            &checkpoint.session_id,
            true,
            vec!["read".into()]
        )
        .await
        .is_err());
    assert_eq!(tools.calls.load(Ordering::SeqCst), 0);
    assert_eq!(engine.runtime_resource_counts(), (0, 0));
}

#[tokio::test]
async fn completed_prefix_corrupt_saved_run_id_refuses_without_owner_or_other_run_writes() {
    let directory = tempfile::tempdir().unwrap();
    let repository =
        Arc::new(FileWorkflowRunRepository::new(directory.path().to_path_buf()).unwrap());
    let first = completed_prefix_checkpoint("corrupt-requested-run");
    let second = completed_prefix_checkpoint("unrelated-saved-run");
    seed_completed_prefix(repository.as_ref(), &first).await;
    seed_completed_prefix(repository.as_ref(), &second).await;
    let path = directory.path().join(&first.run_id).join("snapshot.json");
    let mut corrupt = first.clone();
    corrupt.run_id = second.run_id.clone();
    tokio::fs::write(&path, serde_json::to_vec(&corrupt).unwrap())
        .await
        .unwrap();
    let watched = [
        path,
        directory.path().join(&first.run_id).join("journal.jsonl"),
        directory.path().join(&second.run_id).join("snapshot.json"),
        directory.path().join(&second.run_id).join("journal.jsonl"),
    ];
    let before = futures::future::join_all(watched.iter().map(tokio::fs::read))
        .await
        .into_iter()
        .map(Result::unwrap)
        .collect::<Vec<_>>();
    let tools = Arc::new(ContextRecordingTools::default());
    let engine = WorkflowRunEngine::new(
        repository,
        tools.clone(),
        Arc::new(MockAgents),
        Arc::new(MockDefinitions::default()),
        Arc::new(MockPolicy),
        Arc::new(MockSecrets),
        budgets(),
    );
    assert!(matches!(
        engine
            .continue_completed_prefix(&first.run_id, &first.session_id, true, vec!["read".into()])
            .await,
        Err(WorkflowRunError::NotFound)
    ));
    assert_eq!(tools.calls.load(Ordering::SeqCst), 0);
    assert_eq!(engine.runtime_resource_counts(), (0, 0));
    let after = futures::future::join_all(watched.iter().map(tokio::fs::read))
        .await
        .into_iter()
        .map(Result::unwrap)
        .collect::<Vec<_>>();
    assert_eq!(
        after, before,
        "both runs' saved bytes must remain unchanged"
    );
}
