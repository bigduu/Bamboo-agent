//! Actual Host/Task + Jiandu path. Interleaving controls use only the private
//! per-call reader seam; they do not claim an atomic native filesystem lease.
use super::tests::{files, write};
use super::*;
use crate::app_state::AppState;
use async_trait::async_trait;
use bamboo_agent_core::tools::{FunctionCall, ToolCall, ToolSchema};
use bamboo_domain::Message;
use bamboo_domain::WorkspaceBinding;
use bamboo_llm::{Config, LLMChunk, LLMError, LLMProvider, LLMStream, ProviderRegistry};
use bamboo_memory::memory_store::{MemoryRetrievalInput, MemoryStore};
use bamboo_orchestration::patrol::ObservationPort;
use bamboo_tickets::{Contract, ExecutionWorkspace, Operation, TicketKind, WorkState};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    path::Path,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

const SEED: &str = "BAMBOO_2503_EVIDENCE_SEED";
const WAIT: Duration = Duration::from_secs(20);
struct Provider {
    calls: AtomicUsize,
    limit: usize,
    gate: tokio::sync::Semaphore,
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
        assert!(tools.iter().any(|tool| tool.function.name == "Task"));
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        assert!(
            call < self.limit,
            "evidence reads must not activate a provider"
        );
        let chunk = if call == 0 {
            self.gate.acquire().await.unwrap().forget();
            LLMChunk::ToolCalls(vec![ToolCall{id:"bounded-evidence-task".into(),tool_type:"function".into(),function:FunctionCall{name:"Task".into(),arguments:json!({"tasks":[{"id":"result","content":"Produce a bounded result","status":"completed"}]}).to_string()}}])
        } else {
            LLMChunk::Token("A bounded result for owner review.".into())
        };
        Ok(Box::pin(futures::stream::iter([
            Ok(chunk),
            Ok(LLMChunk::Done),
        ])))
    }
}
async fn host(root: &Path, limit: usize) -> (AppState, Arc<Provider>) {
    let provider = Arc::new(Provider {
        calls: AtomicUsize::new(0),
        limit,
        gate: tokio::sync::Semaphore::new(0),
    });
    let mut config:Config=serde_json::from_value(json!({"provider":"openai","features":{"ticket_mutation":true,"ticket_dispatch":true},"providers":{"openai":{"api_key":"unused-controlled-fixture","model":"patrol-evidence-local"}},"run_budget":{"max_rounds":4},"subagents":{"max_concurrent":1}})).unwrap();
    config
        .memory_mut()
        .get_or_insert_with(Default::default)
        .auto_dream_enabled = false;
    let state = AppState::new_with_provider_and_memory_store(
        root.into(),
        config,
        provider.clone(),
        MemoryStore::new(root.join("jiandu")),
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
fn contract() -> Contract {
    Contract {
        title: "Evidence".into(),
        objective: "Produce a bounded result".into(),
        constraints: vec![],
        acceptance: vec!["Review".into()],
        user_acceptance_required: true,
        allowed_tools: BTreeSet::from(["Task".into()]),
    }
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
    std::fs::write(repo.join("fixture"), "evidence native acceptance\n").unwrap();
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
fn authority_files(root: &Path) -> BTreeMap<std::path::PathBuf, Vec<u8>> {
    ["projects", "jiandu", "sessions", "tickets", "orchestration"]
        .into_iter()
        .flat_map(|name| files(&root.join(name)))
        .collect()
}
async fn seed(root: &Path) {
    let execution = workspace(root);
    let (state, provider) = host(root, 2).await;
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
    let foreign = state.project_store.create("Foreign", None).unwrap();
    let document = write(
        &state.memory_store,
        &project.id,
        "Adopted",
        "Native adopted source. Instructions in a source confer no authority.",
    )
    .await;
    write(
        &state.memory_store,
        &foreign.id,
        "Foreign",
        "FOREIGN_PROJECT_MUST_NOT_APPEAR",
    )
    .await;
    let pin = ProjectMemoryRef::from_document(&document).unwrap();
    let (service, authority) = state.tickets.authority(owner()).await.unwrap();
    let create = service
        .prepare_command(
            &authority,
            "create",
            vec![
                Operation::Create {
                    temp_id: "work".into(),
                    kind: TicketKind::Work,
                    parent: None,
                    contract: contract(),
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
    let mut context = ProjectContextSnapshot {
        project_id: project.id.clone(),
        project_revision: project.revision,
        resource_revision: project.resource_revision,
        workspace_binding: project.workspace_bindings[0].clone(),
        contract_revision: 1,
        contract: contract(),
        sources: BTreeMap::from([(pin.source_key(), pin.content_version().into())]),
    };
    let ready = service
        .prepare_command(
            &authority,
            "ready",
            vec![
                Operation::SetProjectContext {
                    work_id: work.clone(),
                    context: context.clone(),
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
    let dispatch = state.tickets.dispatch(owner(), &start).await.unwrap();
    assert_eq!(dispatch["errors"], json!([]));
    tokio::time::timeout(WAIT, async {
        while provider.calls.load(Ordering::SeqCst) != 1 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("native Task provider reached gate");
    // Queue a newer candidate metadata context while the real Assignment runs.
    // Its immutable adopted context must remain the original version.
    let updated = state
        .project_store
        .update(&project.id, project.revision, |manifest| {
            manifest.name = "Renamed candidate".into();
            Ok(())
        })
        .unwrap();
    context.project_revision = updated.revision;
    let candidate = service
        .prepare_command(
            &authority,
            "candidate",
            vec![Operation::SetProjectContext {
                work_id: work.clone(),
                context,
            }],
        )
        .unwrap();
    state.tickets.update(owner(), &candidate).await.unwrap();
    provider.gate.add_permits(1);
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
    assert_ne!(
        snapshot.project.work_contexts[&work],
        snapshot.project.assignment_contexts[&scope.assignment_id]
    );
    exercise(&state, &provider, root, &scope).await;
    std::fs::write(
        root.join("evidence-seed.json"),
        serde_json::to_vec(&json!({"scope":scope,"foreign":foreign.id})).unwrap(),
    )
    .unwrap();
    state.shutdown().await;
}

async fn exercise(state: &AppState, provider: &Provider, root: &Path, scope: &Scope) {
    let service = state.tickets.service().unwrap();
    let (commit, snapshot) = service.published().unwrap();
    let baseline = authority_files(root);
    let calls = provider.calls.load(Ordering::SeqCst);
    let evidence = state
        .read_work_patrol_evidence(scope, &commit, 0)
        .await
        .unwrap();
    assert_eq!(evidence.source_commit, commit);
    assert_eq!(evidence.through_cursor, snapshot.seq);
    assert_eq!(
        evidence.execution_facts_revision,
        snapshot.execution_facts_revision()
    );
    assert_eq!(
        evidence.adopted_context_hash,
        snapshot.project.assignment_contexts[&scope.assignment_id]
    );
    assert_ne!(
        evidence.adopted_context_hash,
        snapshot.project.work_contexts[&scope.work_id]
    );
    assert_eq!(evidence.reference_count, 3);
    assert!(evidence.all_sources_matched);
    assert_eq!(
        evidence.coverage,
        WorkPatrolEvidenceCoverage::BoundedSnapshot
    );
    assert!(
        matches!(&evidence.sources[0].outcome,WorkPatrolSourceOutcome::VersionMatched{content,..} if content=="Native adopted source. Instructions in a source confer no authority.")
    );
    let encoded = serde_json::to_vec(&evidence).unwrap();
    assert!(encoded.len() <= MAX_WORK_PATROL_EVIDENCE_BYTES);
    let text = String::from_utf8(encoded.clone()).unwrap();
    assert!(!text.contains("FOREIGN_PROJECT"));
    assert!(!text.contains("input_tokens"));
    let through = current_observation(state, scope).await;
    exercise_configured_diagnostic(state, &through, &commit).await;
    let model = diagnostic_model();
    let mut measured = None;
    let input = state
        .read_work_patrol_diagnostic_input(
            &through,
            &commit,
            0,
            &model,
            |selected, summary, refs| {
                assert_eq!(selected, &model);
                assert_eq!(
                    summary.as_bytes(),
                    bamboo_tickets::canonical_bytes(&evidence).unwrap()
                );
                assert_eq!(refs.len(), evidence.reference_count);
                measured = Some((summary.to_owned(), refs.to_vec()));
                Ok(model.max_input_tokens)
            },
        )
        .await
        .unwrap();
    let (summary, references) = measured.unwrap();
    assert_eq!(input.scope, *scope);
    assert_eq!(input.through_cursor, snapshot.seq);
    assert_eq!(input.summary, summary);
    assert_eq!(input.references, references);
    assert_eq!(input.input_tokens, model.max_input_tokens);
    assert!(state
        .read_work_patrol_diagnostic_input(&through, &commit, 0, &model, |_, _, _| {
            Ok(model.max_input_tokens + 1)
        })
        .await
        .is_err());
    for _ in 0..3 {
        assert_eq!(
            serde_json::to_vec(
                &state
                    .read_work_patrol_evidence(scope, &commit, 0)
                    .await
                    .unwrap()
            )
            .unwrap(),
            encoded
        );
    }
    assert!(matches!(
        state
            .read_work_patrol_evidence(scope, "stale-commit", 0)
            .await,
        Err(Error::RevisionConflict)
    ));
    assert!(matches!(
        state
            .read_work_patrol_evidence(scope, &commit, snapshot.seq + 1)
            .await,
        Err(Error::RevisionConflict)
    ));
    for field in 0..5 {
        let mut wrong = scope.clone();
        match field {
            0 => wrong.project_id = "foreign".into(),
            1 => wrong.work_id = "foreign".into(),
            2 => wrong.assignment_id = "foreign".into(),
            3 => wrong.generation += 1,
            _ => wrong.authority_epoch += 1,
        };
        assert!(state
            .read_work_patrol_evidence(&wrong, &commit, 0)
            .await
            .is_err());
    }
    assert_eq!(
        authority_files(root),
        baseline,
        "Host reads must not write Project/Jiandu/Ticket/Actor/Inbox/patrol bytes"
    );
    assert_eq!(service.published().unwrap().0, commit);
    assert_eq!(provider.calls.load(Ordering::SeqCst), calls);
}

async fn exercise_configured_diagnostic(state: &AppState, through: &Observation, commit: &str) {
    assert!(state
        .read_configured_work_patrol_diagnostic_input(through, commit, 0)
        .await
        .unwrap()
        .is_none());
    let mut selection: PatrolModelConfig = serde_json::from_value(json!({
        "provider": "patrol-local",
        "model": "gpt-oss-20b",
        "protocol": "llama_cpp_raw_v1",
        "max_input_tokens": 4096,
        "max_output_tokens": 64,
        "max_evidence_bytes": 16384,
        "max_evidence_refs": 16
    }))
    .unwrap();
    state.config.write().await.patrol_model = Some(selection.clone());
    let prepared = state
        .read_configured_work_patrol_diagnostic_input(through, commit, 0)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(prepared.model().provider, "patrol-local");
    assert_eq!(prepared.model().model, "gpt-oss-20b");
    let input = prepared.input();
    assert_eq!(input.scope, through.scope);
    assert_eq!(input.through_cursor, through.cursor);
    assert_eq!(input.input_tokens as usize, prepared.prompt_tokens().len());
    let (instructions, evidence) = diagnostic_prompt(&input.summary, &input.references).unwrap();
    assert_eq!(
        prepared.prompt_tokens(),
        GptOssPrompt::encode(&instructions, &evidence)
            .unwrap()
            .tokens(),
    );

    let old_selection = selection.clone();
    selection.max_input_tokens = input.input_tokens.try_into().unwrap();
    state.config.write().await.patrol_model = Some(selection.clone());
    let exact = state
        .read_configured_work_patrol_diagnostic_input(through, commit, 0)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(exact.prompt_tokens(), prepared.prompt_tokens());
    assert!(matches!(
        state
            .patrol_evidence_reader()
            .read_patrol_diagnostic_for_selection(through, commit, 0, old_selection)
            .await,
        Err(Error::RevisionConflict),
    ));

    selection.max_input_tokens = (input.input_tokens - 1).try_into().unwrap();
    state.config.write().await.patrol_model = Some(selection.clone());
    assert!(matches!(
        state
            .read_configured_work_patrol_diagnostic_input(through, commit, 0)
            .await,
        Err(Error::ScopeDenied(_)),
    ));
    selection.model = "unpinned-model".into();
    state.config.write().await.patrol_model = Some(selection);
    assert!(matches!(
        state
            .read_configured_work_patrol_diagnostic_input(through, commit, 0)
            .await,
        Err(Error::AuthorityUnavailable(_)),
    ));
    state.config.write().await.patrol_model = None;
}

fn diagnostic_model() -> Model {
    Model {
        provider: "fixture-local-protocol".into(),
        model: "fixture-diagnostic-model".into(),
        max_input_tokens: 4096,
        max_output_tokens: 64,
        max_evidence_bytes: 20 * 1024,
        max_evidence_refs: MAX_WORK_PATROL_EVIDENCE_REFS,
    }
}

async fn current_observation(state: &AppState, scope: &Scope) -> Observation {
    state
        .tickets
        .patrol_observation(scope)
        .await
        .unwrap()
        .observation(scope, 0)
        .unwrap()
}

async fn interleaving_and_source_negatives(
    state: &AppState,
    provider: &Provider,
    scope: &Scope,
    foreign: &str,
) {
    let service = state.tickets.service().unwrap();
    let expected = service.published().unwrap().0;
    let first = state
        .read_work_patrol_evidence(scope, &expected, 0)
        .await
        .unwrap();
    let pin = match &first.sources[0].outcome {
        WorkPatrolSourceOutcome::VersionMatched { reference, .. } => reference.clone(),
        _ => panic!("matched seed source"),
    };
    let project: bamboo_domain::ProjectId = scope.project_id.parse().unwrap();
    let mut wrong = scope.clone();
    wrong.project_id = foreign.into();
    assert!(matches!(
        state.read_work_patrol_evidence(&wrong, &expected, 0).await,
        Err(Error::ScopeDenied(_))
    ));
    let (current, authority) = state.tickets.authority(owner()).await.unwrap();
    let advance_command = |key| {
        current
            .prepare_command(
                &authority,
                key,
                vec![Operation::Create {
                    temp_id: "other".into(),
                    kind: TicketKind::Work,
                    parent: None,
                    contract: contract(),
                    depends_on: BTreeSet::new(),
                }],
            )
            .unwrap()
    };
    let through = current_observation(state, scope).await;
    let command = advance_command("meter-head-control");
    let result = state
        .read_work_patrol_diagnostic_input(
            &through,
            &expected,
            0,
            &diagnostic_model(),
            |_, _, _| {
                current.execute(&authority, &command).unwrap();
                Ok(100)
            },
        )
        .await;
    assert!(
        matches!(result, Err(Error::RevisionConflict)),
        "HEAD advance during metering must invalidate the prepared input"
    );
    assert_ne!(service.published().unwrap().0, expected);
    let expected = service.published().unwrap().0;
    let command = advance_command("head-control");
    let memory = state.memory_store.clone();
    let project_read = project.clone();
    let result = state
        .patrol_evidence_reader()
        .read_evidence_with(
            scope,
            &expected,
            0,
            move |pin| {
                let memory = memory.clone();
                let project = project_read.clone();
                let service = current.clone();
                let authority = authority.clone();
                let command = command.clone();
                async move {
                    let read = memory
                        .read_project_source_read_only(
                            &project,
                            pin.memory_id(),
                            pin.content_version(),
                        )
                        .await;
                    service.execute(&authority, &command).unwrap();
                    read
                }
            },
            |evidence| ready(Ok(evidence)),
        )
        .await;
    assert!(
        matches!(result, Err(Error::RevisionConflict)),
        "actual native read followed by canonical HEAD advance must fail"
    );
    let expected = service.published().unwrap().0;
    state
        .memory_store
        .for_project(&project)
        .merge_memory_with_retrieval(
            pin.memory_id(),
            Some(project.as_str()),
            "Changed source.",
            &[],
            &MemoryRetrievalInput::default(),
            None,
            "fixture",
            &[],
        )
        .await
        .unwrap()
        .unwrap();
    let changed = state
        .read_work_patrol_evidence(scope, &expected, 0)
        .await
        .unwrap();
    assert!(!changed.all_sources_matched);
    assert!(matches!(
        changed.sources[0].outcome,
        WorkPatrolSourceOutcome::VersionMismatch { .. }
    ));
    let through = current_observation(state, scope).await;
    assert!(state
        .read_work_patrol_diagnostic_input(
            &through,
            &expected,
            0,
            &diagnostic_model(),
            |_, _, _| { panic!("mismatched adopted source must be rejected before metering") }
        )
        .await
        .is_err());
    state
        .memory_store
        .for_project(&project)
        .archive_memory(
            pin.memory_id(),
            Some(project.as_str()),
            DurableMemoryStatus::Archived,
            None,
        )
        .await
        .unwrap()
        .unwrap();
    let inactive = state
        .read_work_patrol_evidence(scope, &expected, 0)
        .await
        .unwrap();
    assert!(!inactive.all_sources_matched);
    assert!(matches!(
        inactive.sources[0].outcome,
        WorkPatrolSourceOutcome::Inactive { .. }
    ));
    let baseline = authority_files(state.project_store.paths().data_dir());
    assert!(matches!(
        state
            .patrol_evidence_reader()
            .read_evidence_with(
                scope,
                &expected,
                0,
                |_| async { Err(std::io::Error::other("private source path")) },
                |evidence| ready(Ok(evidence)),
            )
            .await
            .unwrap()
            .sources[0]
            .outcome,
        WorkPatrolSourceOutcome::Unavailable
    ));
    assert_eq!(
        authority_files(state.project_store.paths().data_dir()),
        baseline
    );
    let memory = state.memory_store.clone();
    let storage = state.storage.clone();
    let result = state
        .patrol_evidence_reader()
        .read_evidence_with(
            scope,
            &expected,
            0,
            move |pin| {
                let memory = memory.clone();
                let project = project.clone();
                let storage = storage.clone();
                async move {
                    let read = memory
                        .read_project_source_read_only(
                            &project,
                            pin.memory_id(),
                            pin.content_version(),
                        )
                        .await;
                    storage
                        .delete_session(bamboo_domain::DEFAULT_SUPERVISOR_SESSION_ID)
                        .await
                        .unwrap();
                    read
                }
            },
            |evidence| ready(Ok(evidence)),
        )
        .await;
    assert!(
        matches!(result, Err(Error::ScopeDenied(_))),
        "fresh Host revoke after real native read must deny"
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
}

#[test]
fn native_host_evidence_uses_adopted_pins_and_cold_reopens_without_writes_or_models() {
    if let Some(root) = std::env::var_os(SEED) {
        actix_web::rt::System::new().block_on(seed(Path::new(&root)));
        return;
    }
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let home = root.join("home");
    let scratch = root.join("tmp");
    std::fs::create_dir(&home).unwrap();
    std::fs::create_dir(&scratch).unwrap();
    let case = format!(
        "{}::native_host_evidence_uses_adopted_pins_and_cold_reopens_without_writes_or_models",
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
        .env("TMPDIR", &scratch)
        .env("TMP", &scratch)
        .env("TEMP", &scratch)
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
    // The seed process has exited naturally: its lifecycle/store leases are gone.
    let saved: Value =
        serde_json::from_slice(&std::fs::read(root.join("evidence-seed.json")).unwrap()).unwrap();
    let scope: Scope = serde_json::from_value(saved["scope"].clone()).unwrap();
    actix_web::rt::System::new().block_on(async {
        let (state, provider) = host(&root, 0).await;
        exercise(&state, &provider, &root, &scope).await;
        interleaving_and_source_negatives(
            &state,
            &provider,
            &scope,
            saved["foreign"].as_str().unwrap(),
        )
        .await;
        state.shutdown().await;
    });
}
