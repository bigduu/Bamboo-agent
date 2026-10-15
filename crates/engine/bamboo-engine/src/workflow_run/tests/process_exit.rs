//! Real process exits around the existing readonly completed-prefix contract.

use std::ffi::OsStr;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use super::*;

const CHILD_TEST: &str = "workflow_run::tests::process_exit::process_exit_child";
const CHILD_ENV: &str = "BAMBOO_WORKFLOW_PROCESS_EXIT_HELPER";
const DIRECTORY_ENV: &str = "BAMBOO_WORKFLOW_PROCESS_EXIT_DIRECTORY";
const BOUNDARY_ENV: &str = "BAMBOO_WORKFLOW_PROCESS_EXIT_BOUNDARY";
const CHILD_DEADLINE: Duration = Duration::from_secs(10);
const PREFIX_TOOL: &str = "process_exit_prefix";
const SUFFIX_TOOL: &str = "process_exit_suffix";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ExitBoundary {
    Completed,
    Unknown,
}

impl ExitBoundary {
    fn key(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Unknown => "unknown",
        }
    }

    fn code(self) -> i32 {
        match self {
            Self::Completed => 81,
            Self::Unknown => 82,
        }
    }
}

// Keep the original journal, snapshots and child diagnostics on every assertion
// failure. These files are test observations, never a recovery authority.
struct ExitFixture(Option<tempfile::TempDir>);

impl ExitFixture {
    fn new() -> Self {
        Self(Some(tempfile::tempdir().unwrap()))
    }

    fn path(&self) -> &Path {
        self.0.as_ref().unwrap().path()
    }

    fn run_child(&self, boundary: ExitBoundary) {
        let stdout = self.path().join("child-stdout.txt");
        let stderr = self.path().join("child-stderr.txt");
        let process = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", CHILD_TEST, "--nocapture"])
            .env(CHILD_ENV, CHILD_TEST)
            .env(DIRECTORY_ENV, self.path())
            .env(BOUNDARY_ENV, boundary.key())
            .stdin(Stdio::null())
            .stdout(File::create(&stdout).unwrap())
            .stderr(File::create(&stderr).unwrap())
            .spawn()
            .unwrap();
        let mut child = OwnedChild {
            process,
            reaped: false,
            stdout,
            stderr,
        };
        let deadline = Instant::now() + CHILD_DEADLINE;
        loop {
            if let Some(status) = child.process.try_wait().unwrap() {
                child.reaped = true;
                assert_eq!(
                    status.code(),
                    Some(boundary.code()),
                    "helper missed {boundary:?} exit boundary: {status}; {}",
                    child.diagnostics()
                );
                return;
            }
            if Instant::now() >= deadline {
                let cleanup = child.stop();
                panic!(
                    "helper missed {boundary:?} exit boundary within {CHILD_DEADLINE:?}; cleanup: {cleanup:?}; {}",
                    child.diagnostics()
                );
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    fn calls(&self) -> Vec<Value> {
        std::fs::read_to_string(self.path().join("tool-calls.jsonl"))
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    fn original(&self) -> WorkflowRunSnapshot {
        serde_json::from_slice(&std::fs::read(self.path().join("created.json")).unwrap()).unwrap()
    }
}

impl Drop for ExitFixture {
    fn drop(&mut self) {
        if std::thread::panicking() {
            let path = self.0.take().unwrap().keep();
            eprintln!(
                "process-exit failure evidence retained at {}",
                path.display()
            );
        }
    }
}

struct OwnedChild {
    process: Child,
    reaped: bool,
    stdout: PathBuf,
    stderr: PathBuf,
}

impl OwnedChild {
    fn stop(&mut self) -> io::Result<()> {
        if !self.reaped {
            let _ = self.process.kill();
            self.process.wait()?;
            self.reaped = true;
        }
        Ok(())
    }

    fn diagnostics(&self) -> String {
        format!(
            "stdout: {}; stderr: {}",
            std::fs::read_to_string(&self.stdout).unwrap_or_default(),
            std::fs::read_to_string(&self.stderr).unwrap_or_default()
        )
    }
}

impl Drop for OwnedChild {
    fn drop(&mut self) {
        if let Err(error) = self.stop() {
            eprintln!("could not reap owned child {}: {error}", self.process.id());
        }
    }
}

struct ExitRepository {
    inner: FileWorkflowRunRepository,
    directory: PathBuf,
    boundary: ExitBoundary,
}

#[async_trait]
impl WorkflowRunRepository for ExitRepository {
    async fn create(
        &self,
        snapshot: &WorkflowRunSnapshot,
        event: &WorkflowRunEvent,
    ) -> io::Result<()> {
        self.inner.create(snapshot, event).await?;
        let mut receipt = File::create(self.directory.join("created.json"))?;
        receipt.write_all(&serde_json::to_vec(snapshot).map_err(io::Error::other)?)?;
        receipt.sync_all()
    }

    async fn commit(
        &self,
        snapshot: &WorkflowRunSnapshot,
        event: &WorkflowRunEvent,
    ) -> io::Result<()> {
        self.inner.commit(snapshot, event).await?;
        if self.boundary == ExitBoundary::Completed
            && event.step_id.as_deref() == Some("prefix")
            && matches!(event.kind, WorkflowRunEventKind::StepCompleted { .. })
        {
            // The real FileRepo commit returned, but the engine cannot enter or
            // reserve the suffix: this terminates the entire child process.
            std::process::exit(self.boundary.code());
        }
        Ok(())
    }

    async fn load(&self, run_id: &str) -> io::Result<Option<WorkflowRunSnapshot>> {
        self.inner.load(run_id).await
    }

    async fn events_since(&self, run_id: &str, sequence: u64) -> io::Result<Vec<WorkflowRunEvent>> {
        self.inner.events_since(run_id, sequence).await
    }

    async fn list_run_ids(&self) -> io::Result<Vec<String>> {
        self.inner.list_run_ids().await
    }
}

struct ExitTools {
    observations: PathBuf,
    boundary: Option<ExitBoundary>,
}

#[async_trait]
impl ToolExecutor for ExitTools {
    async fn execute(&self, call: &ToolCall) -> Result<ToolResult, ToolError> {
        let args: Value = serde_json::from_str(&call.function.arguments).unwrap();
        let mut observations = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.observations)
            .unwrap();
        serde_json::to_writer(
            &mut observations,
            &json!({"tool":call.function.name,"args":args}),
        )
        .unwrap();
        observations.write_all(b"\n").unwrap();
        observations.sync_all().unwrap();
        if call.function.name == SUFFIX_TOOL && self.boundary == Some(ExitBoundary::Unknown) {
            // The test oracle proves dispatch occurred. No result has returned
            // to the engine, and no StepCompleted may be inferred from it.
            std::process::exit(ExitBoundary::Unknown.code());
        }
        let output = match call.function.name.as_str() {
            PREFIX_TOOL => json!({"value":args["value"].as_i64().unwrap() + 1}),
            SUFFIX_TOOL => args,
            _ => return Err(ToolError::NotFound(call.function.name.clone())),
        };
        Ok(ToolResult::text(true, output.to_string()))
    }

    fn list_tools(&self) -> Vec<bamboo_agent_core::tools::ToolSchema> {
        Vec::new()
    }
}

fn process_definition() -> WorkflowRunDefinition {
    let mut prefix = tool_step("prefix", PREFIX_TOOL, json!({"from":"args"}));
    prefix.output_schema = Some(
        json!({"type":"object","required":["value"],"properties":{"value":{"type":"integer"}},"additionalProperties":false}),
    );
    definition(
        vec![
            prefix,
            tool_step(
                "suffix",
                SUFFIX_TOOL,
                json!({"from":"step","step":"prefix"}),
            ),
        ],
        WorkflowPlan::Sequence {
            nodes: vec![choice_leaf("prefix"), choice_leaf("suffix")],
        },
    )
}

fn process_engine(
    repository: Arc<dyn WorkflowRunRepository>,
    directory: &Path,
    boundary: Option<ExitBoundary>,
) -> Arc<WorkflowRunEngine> {
    WorkflowRunEngine::new(
        repository,
        Arc::new(ExitTools {
            observations: directory.join("tool-calls.jsonl"),
            boundary,
        }),
        Arc::new(MockAgents),
        Arc::new(MockDefinitions::default()),
        Arc::new(MockPolicy),
        Arc::new(MockSecrets),
        budgets(),
    )
}

// Inert in an ordinary full-suite invocation. Only the exact helper selected
// by the parent's child-only environment may terminate a process.
#[test]
fn process_exit_child() {
    if std::env::var_os(CHILD_ENV).as_deref() != Some(OsStr::new(CHILD_TEST)) {
        return;
    }
    assert_eq!(
        std::env::args().skip(1).collect::<Vec<_>>(),
        vec!["--exact", CHILD_TEST, "--nocapture"]
    );
    let directory = PathBuf::from(std::env::var_os(DIRECTORY_ENV).unwrap());
    assert!(directory.is_absolute() && directory.is_dir());
    let boundary = match std::env::var(BOUNDARY_ENV).unwrap().as_str() {
        "completed" => ExitBoundary::Completed,
        "unknown" => ExitBoundary::Unknown,
        other => panic!("unexpected process-exit boundary {other}"),
    };
    let repository = Arc::new(ExitRepository {
        inner: FileWorkflowRunRepository::new(directory.join("runs")).unwrap(),
        directory: directory.clone(),
        boundary,
    });
    let engine = process_engine(repository, &directory, Some(boundary));
    let result = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap()
        .block_on(engine.run(request(process_definition(), json!({"value":41}))));
    panic!("engine returned before {boundary:?} process exit: {result:?}");
}

fn assert_identity(saved: &WorkflowRunSnapshot, original: &WorkflowRunSnapshot) {
    assert_eq!(saved.run_id, original.run_id);
    assert_eq!(saved.parent_run_id, original.parent_run_id);
    assert_eq!(saved.parent_step_id, original.parent_step_id);
    assert_eq!(saved.session_id, original.session_id);
    assert_eq!(saved.definition, original.definition);
    assert_eq!(saved.definition_bundle, original.definition_bundle);
    assert_eq!(
        saved.definition_bundle_hash,
        original.definition_bundle_hash
    );
    assert_eq!(saved.validated_args, json!({"value":41}));
    assert_eq!(saved.validated_args, original.validated_args);
    assert_eq!(saved.created_at, original.created_at);
    assert_eq!(saved.definition.budgets.wall_time_ms, 10_000);
}

fn assert_journal(events: &[WorkflowRunEvent], snapshot: &WorkflowRunSnapshot) {
    assert_eq!(events.len() as u64, snapshot.last_sequence);
    for (index, event) in events.iter().enumerate() {
        assert_eq!(event.run_id, snapshot.run_id);
        assert_eq!(event.sequence, index as u64 + 1);
    }
}

fn reservations(events: &[WorkflowRunEvent]) -> usize {
    events
        .iter()
        .filter(|event| {
            matches!(&event.kind, WorkflowRunEventKind::Phase { name } if name == "step_reserved")
        })
        .count()
}

async fn cold_checkpoint(
    fixture: &ExitFixture,
    boundary: ExitBoundary,
) -> (
    Arc<FileWorkflowRunRepository>,
    WorkflowRunSnapshot,
    Vec<WorkflowRunEvent>,
) {
    // This constructor is deliberately reached only after run_child has
    // confirmed and reaped a real process exit; no pre-exit repository survives.
    let repository = Arc::new(FileWorkflowRunRepository::new(fixture.path().join("runs")).unwrap());
    let ids = repository.list_run_ids().await.unwrap();
    assert_eq!(ids, vec![fixture.original().run_id]);
    let checkpoint = repository.load(&ids[0]).await.unwrap().unwrap();
    let events = repository.events_since(&ids[0], 0).await.unwrap();
    assert_identity(&checkpoint, &fixture.original());
    assert_eq!(checkpoint.status, WorkflowRunStatus::Running);
    let steps = if boundary == ExitBoundary::Completed {
        1
    } else {
        2
    };
    assert_eq!(checkpoint.steps.len(), steps);
    assert_eq!(
        checkpoint.steps["prefix"].status,
        WorkflowStepStatus::Succeeded
    );
    assert_eq!(checkpoint.steps["prefix"].attempts, 1);
    assert_eq!(checkpoint.steps["prefix"].output, Some(json!({"value":42})));
    assert!(checkpoint.steps["prefix"].failure.is_none());
    assert!(checkpoint.output.is_none());
    assert!(checkpoint.failure.is_none());
    assert!(checkpoint.suspension.is_none());
    assert_eq!(
        checkpoint.usage,
        WorkflowBudgetUsage {
            steps: steps as u32,
            ..Default::default()
        }
    );
    assert_eq!(reservations(&events), steps);
    assert_eq!(
        events
            .iter()
            .filter(|event| event.step_id.as_deref() == Some("prefix")
                && matches!(event.kind, WorkflowRunEventKind::StepCompleted { .. }))
            .count(),
        1
    );
    let mut calls = vec![json!({"tool":PREFIX_TOOL,"args":{"value":41}})];
    if boundary == ExitBoundary::Completed {
        assert!(!checkpoint.steps.contains_key("suffix"));
        assert!(!events
            .iter()
            .any(|event| event.step_id.as_deref() == Some("suffix")));
    } else {
        calls.push(json!({"tool":SUFFIX_TOOL,"args":{"value":42}}));
    }
    assert_eq!(fixture.calls(), calls);
    assert_journal(&events, &checkpoint);
    (repository, checkpoint, events)
}

#[tokio::test]
async fn process_exit_after_completed_prefix_cold_reopens_and_continues_same_run() {
    let fixture = ExitFixture::new();
    fixture.run_child(ExitBoundary::Completed);
    let (repository, checkpoint, before) = cold_checkpoint(&fixture, ExitBoundary::Completed).await;
    assert!(matches!(
        before.last().unwrap().kind,
        WorkflowRunEventKind::StepCompleted { .. }
    ));
    assert_eq!(before.last().unwrap().step_id.as_deref(), Some("prefix"));

    let engine = process_engine(repository.clone(), fixture.path(), None);
    let recovered = engine.recover().await.unwrap();
    assert_eq!(recovered.len(), 1);
    let suspended = &recovered[0];
    assert_identity(suspended, &fixture.original());
    assert_eq!(suspended.status, WorkflowRunStatus::Suspended);
    assert!(matches!(
        suspended.suspension,
        Some(WorkflowSuspensionContext::Recovery { .. })
    ));
    assert_eq!(suspended.steps, checkpoint.steps);
    assert_eq!(suspended.usage, checkpoint.usage);
    assert_eq!(suspended.last_sequence, checkpoint.last_sequence + 1);
    assert_eq!(WorkflowRunEngine::completed_prefix(suspended).unwrap(), 1);
    assert_eq!(fixture.calls().len(), 1, "recover never dispatches a tool");
    let started = engine
        .continue_completed_prefix(
            &checkpoint.run_id,
            &checkpoint.session_id,
            true,
            vec!["read".into()],
        )
        .await
        .unwrap();
    assert_identity(&started, &fixture.original());
    assert_eq!(started.steps, checkpoint.steps);
    assert_eq!(started.usage, checkpoint.usage);
    let progress = await_continuation(&engine, &checkpoint.run_id).await;
    let saved = progress.snapshot;
    assert_identity(&saved, &fixture.original());
    assert_eq!(saved.status, WorkflowRunStatus::Succeeded);
    assert_eq!(saved.output, Some(json!({"value":42})));
    assert_eq!(saved.steps["prefix"], checkpoint.steps["prefix"]);
    assert_eq!(saved.steps["suffix"].status, WorkflowStepStatus::Succeeded);
    assert_eq!(saved.steps["suffix"].attempts, 1);
    assert_eq!(
        saved.usage,
        WorkflowBudgetUsage {
            steps: 2,
            ..checkpoint.usage.clone()
        }
    );
    assert_eq!(
        fixture.calls(),
        vec![
            json!({"tool":PREFIX_TOOL,"args":{"value":41}}),
            json!({"tool":SUFFIX_TOOL,"args":{"value":42}}),
        ]
    );
    assert_eq!(&progress.events[..before.len()], before.as_slice());
    assert_journal(&progress.events, &saved);
    assert_eq!(reservations(&progress.events), 2);
    for step in ["prefix", "suffix"] {
        assert_eq!(
            progress
                .events
                .iter()
                .filter(|event| event.step_id.as_deref() == Some(step)
                    && matches!(event.kind, WorkflowRunEventKind::StepStarted))
                .count(),
            1
        );
        assert_eq!(
            progress
                .events
                .iter()
                .filter(|event| event.step_id.as_deref() == Some(step)
                    && matches!(event.kind, WorkflowRunEventKind::StepCompleted { .. }))
                .count(),
            1
        );
    }
    assert_eq!(engine.runtime_resource_counts(), (0, 0));
}

#[tokio::test]
async fn process_exit_with_unknown_suffix_result_cold_reopens_and_refuses_continuation() {
    let fixture = ExitFixture::new();
    fixture.run_child(ExitBoundary::Unknown);
    let (repository, checkpoint, before) = cold_checkpoint(&fixture, ExitBoundary::Unknown).await;
    assert_eq!(
        checkpoint.steps["suffix"].status,
        WorkflowStepStatus::Running
    );
    assert_eq!(checkpoint.steps["suffix"].attempts, 1);
    assert!(checkpoint.steps["suffix"].output.is_none());
    assert!(matches!(
        before.last().unwrap().kind,
        WorkflowRunEventKind::StepStarted
    ));
    assert_eq!(before.last().unwrap().step_id.as_deref(), Some("suffix"));
    assert!(!before
        .iter()
        .any(|event| event.step_id.as_deref() == Some("suffix")
            && matches!(event.kind, WorkflowRunEventKind::StepCompleted { .. })));

    let engine = process_engine(repository.clone(), fixture.path(), None);
    let recovered = engine.recover().await.unwrap();
    assert_eq!(recovered.len(), 1);
    let suspended = &recovered[0];
    assert_identity(suspended, &fixture.original());
    assert_eq!(suspended.status, WorkflowRunStatus::Suspended);
    assert!(matches!(
        suspended.suspension,
        Some(WorkflowSuspensionContext::Recovery { .. })
    ));
    assert_eq!(suspended.steps["prefix"], checkpoint.steps["prefix"]);
    assert_eq!(
        suspended.steps["suffix"].status,
        WorkflowStepStatus::Suspended
    );
    assert_eq!(suspended.steps["suffix"].attempts, 1);
    assert_eq!(
        suspended.steps["suffix"].input_hash,
        checkpoint.steps["suffix"].input_hash
    );
    assert!(suspended.steps["suffix"].output.is_none());
    assert_eq!(
        suspended.steps["suffix"].failure.as_ref().unwrap().code,
        WorkflowFailureCode::RecoverySuspended
    );
    assert_eq!(suspended.usage, checkpoint.usage);
    assert_eq!(suspended.last_sequence, checkpoint.last_sequence + 2);
    let after_recovery = repository
        .events_since(&checkpoint.run_id, 0)
        .await
        .unwrap();
    assert_eq!(&after_recovery[..before.len()], before.as_slice());
    assert_journal(&after_recovery, suspended);
    let run_directory = fixture.path().join("runs").join(&checkpoint.run_id);
    let snapshot_bytes = std::fs::read(run_directory.join("snapshot.json")).unwrap();
    let journal_bytes = std::fs::read(run_directory.join("journal.jsonl")).unwrap();
    assert!(matches!(
        engine
            .continue_completed_prefix(
                &checkpoint.run_id,
                &checkpoint.session_id,
                true,
                vec!["read".into()]
            )
            .await,
        Err(WorkflowRunError::Preflight(_))
    ));
    assert_eq!(
        repository.load(&checkpoint.run_id).await.unwrap().unwrap(),
        *suspended
    );
    assert_eq!(
        repository
            .events_since(&checkpoint.run_id, 0)
            .await
            .unwrap(),
        after_recovery
    );
    assert_eq!(
        std::fs::read(run_directory.join("snapshot.json")).unwrap(),
        snapshot_bytes
    );
    assert_eq!(
        std::fs::read(run_directory.join("journal.jsonl")).unwrap(),
        journal_bytes
    );
    assert_eq!(
        fixture.calls(),
        vec![
            json!({"tool":PREFIX_TOOL,"args":{"value":41}}),
            json!({"tool":SUFFIX_TOOL,"args":{"value":42}}),
        ]
    );
    assert_eq!(reservations(&after_recovery), 2);
    assert!(!after_recovery
        .iter()
        .any(|event| event.step_id.as_deref() == Some("suffix")
            && matches!(event.kind, WorkflowRunEventKind::StepCompleted { .. })));
    assert!(!after_recovery
        .iter()
        .any(|event| matches!(event.kind, WorkflowRunEventKind::RunSucceeded { .. })));
    assert!(suspended.output.is_none());
    assert_eq!(engine.runtime_resource_counts(), (0, 0));
}
