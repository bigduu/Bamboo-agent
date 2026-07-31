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
    async fn resolve(&self, name: &str) -> Result<Option<NamedAgentSpec>, String> {
        Ok((name == "reviewer").then(|| NamedAgentSpec {
            name: name.to_string(),
            allowed_capabilities: BTreeSet::from(["read".to_string()]),
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
    ) -> Result<AgentStepResult, String> {
        Ok(AgentStepResult {
            output: json!({"reviewed": prompt}),
            tokens: 10,
            cost_micros: 2,
        })
    }
}

struct CountingAgents(AtomicUsize);

#[async_trait]
impl AgentStepPort for CountingAgents {
    async fn resolve(&self, name: &str) -> Result<Option<NamedAgentSpec>, String> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(Some(NamedAgentSpec {
            name: name.to_string(),
            allowed_capabilities: BTreeSet::from(["read".to_string()]),
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
    ) -> Result<AgentStepResult, String> {
        Ok(AgentStepResult {
            output: prompt,
            tokens: 1,
            cost_micros: 1,
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
    assert_eq!(failed.usage.cost_micros, 2);
    let persisted = engine.progress(&failed.run_id, 0).await.unwrap().snapshot;
    assert_eq!(persisted.usage, failed.usage);
}

#[tokio::test]
async fn zero_token_or_cost_budget_fails_before_agent_dispatch() {
    struct ExecutionCountingAgents(Arc<AtomicUsize>);

    #[async_trait]
    impl AgentStepPort for ExecutionCountingAgents {
        async fn resolve(&self, name: &str) -> Result<Option<NamedAgentSpec>, String> {
            Ok(Some(NamedAgentSpec {
                name: name.to_string(),
                allowed_capabilities: BTreeSet::from(["read".to_string()]),
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
        ) -> Result<AgentStepResult, String> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(AgentStepResult {
                output: json!({"unexpected": true}),
                tokens: 1,
                cost_micros: 1,
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
        assert_eq!(failed.usage.cost_micros, 0);
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
    assert_eq!((failed.usage.tokens, failed.usage.cost_micros), (10, 2));
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
