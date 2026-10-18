//! Opt-in real TCP routes for Lotus Sprint acceptance, with persistent synthetic facts.
//! Run this test alone. No model, activation router, RunSpec, or real Worker is invoked.
use actix_web::{web, App, HttpServer};
use anyhow::{ensure, Context, Result as FixtureResult};
use async_trait::async_trait;
use bamboo_agent_core::tools::ToolSchema;
use bamboo_config::Config;
use bamboo_domain::{
    Message, ProjectId, Role, Session, SessionAuthorityIdentity, SessionKind, WorkspaceBinding,
};
use bamboo_engine::{
    ticket_runtime::{self, TICKET_DISPATCH_KEY},
    ticket_worker_plan::{tickets::*, TICKET_LOCAL_PLAN_KEY},
};
use bamboo_llm::{LLMError, LLMProvider, LLMStream, ProviderRegistry};
use bamboo_server::AppState;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fs::{self, OpenOptions},
    io::Write,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

const MARKER_FILE: &str = ".sprint-browser-fixture.json";
const MARKER_KIND: &str = "bamboo-lotus-sprint-browser-synthetic-v1";
const MODEL: &str = "synthetic-unused";
const DISABLED_PROVIDER_URL: &str = "http://127.0.0.1:9/v1";

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SeedMarker {
    kind: String,
    version: u32,
    root: PathBuf,
    created_at: String,
    config_fingerprint: String,
    binding: ScopeBinding,
    project_id: ProjectId,
    project_created_at: String,
    work_ids: BTreeMap<String, String>,
    sprint_ids: BTreeMap<String, SprintId>,
    sprint_created_seq: BTreeMap<String, u64>,
    supervisor_id: String,
    supervisor_created_at: String,
    child_id: String,
    child_created_at: String,
    start_session_id: String,
    start_session_created_at: String,
    session_hashes: BTreeMap<String, String>,
    assignment_id: String,
    runtime_receipt: RuntimeReceipt,
    seed_snapshot: SnapshotRef,
    seed_receipt_hashes: BTreeMap<String, String>,
}

#[derive(Default)]
struct NoExecutionProvider(AtomicUsize);

#[async_trait]
impl LLMProvider for NoExecutionProvider {
    async fn chat_stream(
        &self,
        _: &[Message],
        _: &[ToolSchema],
        _: Option<u32>,
        _: &str,
    ) -> std::result::Result<LLMStream, LLMError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err(LLMError::Api(
            "Sprint browser fixture forbids model execution".into(),
        ))
    }

    async fn list_models(&self) -> std::result::Result<Vec<String>, LLMError> {
        Ok(vec![MODEL.into()])
    }
}

fn absolute_env(name: &str) -> FixtureResult<PathBuf> {
    let path =
        PathBuf::from(std::env::var_os(name).with_context(|| format!("{name} is required"))?);
    ensure!(path.is_absolute(), "{name} must be absolute");
    Ok(path)
}

fn plain_directory(path: &Path) -> FixtureResult<()> {
    let metadata = fs::symlink_metadata(path)?;
    ensure!(
        metadata.is_dir() && !metadata.file_type().is_symlink(),
        "plain directory required: {}",
        path.display()
    );
    Ok(())
}

fn plain_file(path: &Path) -> FixtureResult<()> {
    let metadata = fs::symlink_metadata(path)?;
    ensure!(
        metadata.is_file() && !metadata.file_type().is_symlink(),
        "plain file required: {}",
        path.display()
    );
    Ok(())
}

fn new_output(path: &Path) -> FixtureResult<PathBuf> {
    ensure!(path.is_absolute(), "output path must be absolute");
    let parent = path
        .parent()
        .context("output needs a parent")?
        .canonicalize()?;
    plain_directory(&parent)?;
    let path = parent.join(path.file_name().context("output needs a file name")?);
    ensure!(
        fs::symlink_metadata(&path).is_err_and(|e| e.kind() == std::io::ErrorKind::NotFound),
        "output already exists or is unavailable: {}",
        path.display()
    );
    Ok(path)
}

#[derive(Debug)]
struct OwnStop;
impl std::fmt::Display for OwnStop {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("owned fixture stop requested")
    }
}
impl std::error::Error for OwnStop {}

fn stop_requested(path: &Path) -> FixtureResult<bool> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            ensure!(
                metadata.is_file() && !metadata.file_type().is_symlink(),
                "owned stop marker must be a plain file"
            );
            Ok(true)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn check_stop(path: &Path) -> FixtureResult<()> {
    if stop_requested(path)? {
        return Err(OwnStop.into());
    }
    Ok(())
}

fn write_new_json(path: &Path, value: &impl Serialize) -> FixtureResult<()> {
    let mut output = OpenOptions::new().write(true).create_new(true).open(path)?;
    output.write_all(&serde_json::to_vec_pretty(value)?)?;
    output.write_all(b"\n")?;
    output.sync_all()?;
    Ok(())
}

fn prepare_root(requested: &Path, seed: bool) -> FixtureResult<PathBuf> {
    match fs::symlink_metadata(requested) {
        Ok(_) => plain_directory(requested)?,
        Err(error) if seed && error.kind() == std::io::ErrorKind::NotFound => {
            let parent = requested
                .parent()
                .context("root needs a parent")?
                .canonicalize()?;
            plain_directory(&parent)?;
            fs::create_dir(parent.join(requested.file_name().context("root needs a name")?))?;
        }
        Err(error) => return Err(error.into()),
    }
    let root = requested.canonicalize()?;
    if seed {
        ensure!(
            fs::read_dir(&root)?.next().is_none(),
            "seed requires an empty, independent root"
        );
    } else {
        // Do not load config, storage, or other data from an unrecognized root.
        plain_file(&root.join(MARKER_FILE))?;
    }
    Ok(root)
}

fn read_marker(root: &Path) -> FixtureResult<SeedMarker> {
    let path = root.join(MARKER_FILE);
    plain_file(&path)?;
    ensure!(
        fs::metadata(&path)?.len() <= 32 * 1024,
        "fixture marker exceeds its bound"
    );
    let marker: SeedMarker = serde_json::from_slice(&fs::read(path)?)?;
    ensure!(
        marker.kind == MARKER_KIND && marker.version == 1 && marker.root == root,
        "unknown fixture root"
    );
    ensure!(
        marker.work_ids.len() == 5 && marker.sprint_ids.len() == 3,
        "incomplete fixture identities"
    );
    for role in [
        "backlog",
        "planned",
        "active",
        "carried_planned",
        "carried_backlog",
    ] {
        uuid::Uuid::parse_str(
            marker
                .work_ids
                .get(role)
                .context("missing fixture Work role")?,
        )?;
    }
    for role in ["planned", "active", "closed"] {
        ensure!(
            marker.sprint_ids.contains_key(role) && marker.sprint_created_seq.contains_key(role),
            "missing fixture Sprint role"
        );
    }
    uuid::Uuid::parse_str(&marker.start_session_id)?;
    ensure!(
        marker.start_session_id != marker.supervisor_id
            && marker.start_session_id != marker.child_id,
        "start Session must be independent"
    );
    plain_directory(&root.join("workspace"))?;
    plain_directory(&root.join("jiandu"))?;
    Ok(marker)
}

fn scope_root(root: &Path, binding: &ScopeBinding) -> FixtureResult<PathBuf> {
    let incarnation = binding
        .scope_id
        .strip_prefix("supervisor/")
        .context("invalid fixture scope")?;
    let incarnation = uuid::Uuid::parse_str(incarnation)?;
    let tickets = root.join("tickets");
    plain_directory(&tickets)?;
    let scope = tickets.join(incarnation.to_string());
    plain_directory(&scope)?;
    plain_file(&scope.join("HEAD"))?;
    Ok(scope)
}

fn scope_snapshot(commit: &str, snapshot: &Snapshot) -> Value {
    json!({"commit":commit,"seq":snapshot.seq,"authority_epoch":snapshot.authority_epoch,"schema":snapshot.schema,
        "binding":snapshot.binding,"ticket_ids":snapshot.tickets.keys().collect::<Vec<_>>(),
        "assignment_ids":snapshot.assignments.keys().collect::<Vec<_>>(),"sprint_ids":snapshot.sprint.sprints.keys().collect::<Vec<_>>(),
        "receipt_count":snapshot.receipts.len(),"receipt_ids":snapshot.receipts.keys().collect::<Vec<_>>(),
        "receipt_hash":content_hash(&canonical_bytes(&snapshot.receipts).expect("receipt serialization")),
        "tickets_hash":content_hash(&canonical_bytes(&snapshot.tickets).expect("Ticket serialization")),
        "assignments_hash":content_hash(&canonical_bytes(&snapshot.assignments).expect("Assignment serialization")),
        "sprint_hash":content_hash(&canonical_bytes(&snapshot.sprint).expect("Sprint serialization")),
        "project_hash":content_hash(&canonical_bytes(&snapshot.project).expect("Project serialization")),
        "intents_hash":content_hash(&canonical_bytes(&snapshot.intents).expect("intent serialization"))})
}

fn ticket_command(
    service: &TicketService,
    who: &Authority,
    id: &str,
    operations: Vec<Operation>,
) -> FixtureResult<OperationReceipt> {
    let command = service.prepare_command(who, id, operations)?;
    Ok(service.execute(who, &command)?)
}

fn sprint_command(
    service: &TicketService,
    who: &Authority,
    project: &ProjectId,
    id: &SprintId,
    operation: &str,
    action: SprintAction,
) -> FixtureResult<()> {
    let snapshot = service.published()?.1;
    service.execute_sprint(
        who,
        &SprintCommand {
            operation_id: format!("fixture-{operation}"),
            binding: snapshot.binding,
            expected_seq: snapshot.seq,
            expected_epoch: snapshot.authority_epoch,
            project_id: project.clone(),
            sprint_id: id.clone(),
            source: "synthetic Sprint browser fixture; no execution authority".into(),
            action,
        },
    )?;
    Ok(())
}

fn sprint_revision(service: &TicketService, id: &SprintId) -> FixtureResult<u64> {
    Ok(service
        .published()?
        .1
        .sprint
        .sprints
        .get(id)
        .context("Sprint missing")?
        .revision)
}

async fn seed_state(
    state: &AppState,
    root: &Path,
    config_fingerprint: String,
) -> FixtureResult<SeedMarker> {
    let workspace = root.join("workspace").to_string_lossy().into_owned();
    let project = state.project_store.create_with_project_path(
        "Synthetic Sprint acceptance",
        Some("Persistent fixture data; no model execution".into()),
        workspace.clone(),
        Vec::new(),
    )?;
    let (service, owner) = state
        .tickets
        .authority(Principal::User {
            user_id: "host-owner".into(),
        })
        .await?;
    let binding = service.published()?.1.binding;
    let mut supervisor = state
        .storage
        .load_root_authority(&binding.supervisor_session_id)
        .await?
        .context("canonical Supervisor missing")?;
    supervisor.set_project_id_meta(project.id.as_str());
    supervisor.set_workspace_path_meta(workspace.clone());
    supervisor.metadata_version = supervisor
        .metadata_version
        .checked_add(1)
        .context("metadata revision overflow")?;
    state.storage.save_session(&supervisor).await?;
    let supervisor = state
        .storage
        .load_root_authority(&binding.supervisor_session_id)
        .await?
        .context("canonical Supervisor context missing")?;
    let mut operations = Vec::new();
    for role in [
        "backlog",
        "planned",
        "active",
        "carried_planned",
        "carried_backlog",
    ] {
        let contract = Contract {
            title: format!("Fixture {role}"),
            objective: "Inspect persisted Sprint planning".into(),
            constraints: vec!["Synthetic host records only; never execute a model".into()],
            acceptance: vec!["Explicit owner review".into()],
            user_acceptance_required: true,
            allowed_tools: BTreeSet::from(["Task".into()]),
        };
        operations.extend([
            Operation::Create {
                temp_id: role.into(),
                kind: TicketKind::Work,
                parent: None,
                contract: contract.clone(),
                depends_on: BTreeSet::new(),
            },
            Operation::AssociateProject {
                ticket_id: role.into(),
                project_id: project.id.clone(),
            },
            Operation::SetProjectContext {
                work_id: role.into(),
                context: ProjectContextSnapshot {
                    project_id: project.id.clone(),
                    project_revision: project.revision,
                    resource_revision: project.resource_revision,
                    workspace_binding: WorkspaceBinding {
                        path: workspace.clone(),
                        label: None,
                        git_common_dir: None,
                    },
                    contract_revision: 1,
                    contract,
                    sources: BTreeMap::from([("synthetic-fixture-spec".into(), "v1".into())]),
                },
            },
            Operation::Ready {
                work_id: role.into(),
            },
        ]);
    }
    let work_ids = ticket_command(&service, &owner, "fixture-works", operations)?.ids;
    let sprint_ids: BTreeMap<String, SprintId> = BTreeMap::from([
        ("planned".into(), SprintId::parse("fixture-planned")?),
        ("active".into(), SprintId::parse("fixture-active")?),
        ("closed".into(), SprintId::parse("fixture-closed")?),
    ]);
    for (role, id) in &sprint_ids {
        sprint_command(
            &service,
            &owner,
            &project.id,
            id,
            &format!("create-{role}"),
            SprintAction::Create {
                definition: SprintDefinition {
                    name: format!("Fixture {role}"),
                    goal: "Explicit planning and carry-over evidence".into(),
                    starts_at: None,
                    ends_at: None,
                },
            },
        )?;
    }
    for (role, work_role) in [
        ("planned", "planned"),
        ("closed", "carried_planned"),
        ("closed", "carried_backlog"),
        ("active", "active"),
    ] {
        let id = &sprint_ids[role];
        sprint_command(
            &service,
            &owner,
            &project.id,
            id,
            &format!("add-{work_role}"),
            SprintAction::Add {
                expected_revision: sprint_revision(&service, id)?,
                work_id: work_ids[work_role].clone(),
            },
        )?;
    }
    let closed = &sprint_ids["closed"];
    sprint_command(
        &service,
        &owner,
        &project.id,
        closed,
        "start-closed",
        SprintAction::Start {
            expected_revision: sprint_revision(&service, closed)?,
        },
    )?;
    let planned = &sprint_ids["planned"];
    sprint_command(
        &service,
        &owner,
        &project.id,
        closed,
        "close-with-explicit-carry",
        SprintAction::Close {
            expected_revision: sprint_revision(&service, closed)?,
            dispositions: BTreeMap::from([
                (
                    work_ids["carried_planned"].clone(),
                    SprintDestination::Sprint {
                        sprint_id: planned.clone(),
                    },
                ),
                (
                    work_ids["carried_backlog"].clone(),
                    SprintDestination::Backlog,
                ),
            ]),
            target_revisions: BTreeMap::from([(
                planned.clone(),
                sprint_revision(&service, planned)?,
            )]),
        },
    )?;
    let active = &sprint_ids["active"];
    sprint_command(
        &service,
        &owner,
        &project.id,
        active,
        "start-active",
        SprintAction::Start {
            expected_revision: sprint_revision(&service, active)?,
        },
    )?;

    // Core Start fixes the spec only. No adapter, router registration, provider or RunSpec is called.
    let started = ticket_command(
        &service,
        &owner,
        "fixture-core-start",
        vec![Operation::Start {
            work_id: work_ids["active"].clone(),
            temp_id: "assignment".into(),
            workspace: Some(ExecutionWorkspace {
                repo: workspace.clone(),
                base_commit: "a".repeat(40),
                branch: "synthetic-fixture".into(),
                worktree: workspace.clone(),
                write_roots: vec![workspace.clone()],
                claims: BTreeSet::from([format!("worktree:{workspace}")]),
            }),
        }],
    )?;
    let assignment_id = started.ids["assignment"].clone();
    let snapshot = service.published()?.1;
    let assignment = &snapshot.assignments[&assignment_id];
    let intent = &snapshot.intents[&assignment.dispatch_key];
    let mut dispatch = ticket_runtime::dispatch_binding(
        &service,
        &assignment.dispatch_key,
        &intent.immutable_spec,
    )?;
    let child_id = ticket_runtime::ticket_child_id(&assignment.dispatch_key);
    let mut child = Session::new_child_of(
        child_id.clone(),
        &supervisor,
        MODEL,
        "Synthetic persisted Ticket Child; no real run",
    );
    child.metadata.insert(
        TICKET_DISPATCH_KEY.into(),
        serde_json::to_string(&dispatch)?,
    );
    child
        .metadata
        .insert(TICKET_LOCAL_PLAN_KEY.into(), assignment_id.clone());
    child
        .metadata
        .insert("fixture.synthetic.activation".into(), MARKER_KIND.into());
    child.set_workspace_path_meta(workspace.clone());
    child.set_project_id_meta(project.id.as_str());
    child.set_last_run_status("completed");
    state.storage.save_session(&child).await?;
    let mut persisted = state
        .storage
        .load_runtime_control_plane(&child_id)
        .await?
        .context("canonical Child birth missing")?;
    ensure!(
        persisted.kind == SessionKind::Child
            && persisted.parent_created_at == Some(supervisor.created_at),
        "canonical Child lineage changed"
    );
    let mut start = Session::new(uuid::Uuid::new_v4().to_string(), MODEL);
    ensure!(
        start.created_at > persisted.created_at && start.created_at > supervisor.created_at,
        "new start Session must be created later"
    );
    start.title = "Synthetic initial conversation; no execution".into();
    start.set_project_id_meta(project.id.as_str());
    start.set_workspace_path_meta(workspace);
    state.storage.save_session(&start).await?;
    let start = state
        .storage
        .load_root_authority(&start.id)
        .await?
        .context("canonical initial Root missing")?;
    let receipt = RuntimeReceipt {
        dispatch_key: assignment.dispatch_key.clone(),
        spec_hash: intent.spec_hash.clone(),
        run_id: "synthetic-host-activation-no-execution".into(),
        session_id: persisted.id.clone(),
    };
    dispatch.receipt = Some(receipt.clone());
    dispatch.receipt_created_at = Some(persisted.created_at);
    persisted.metadata.insert(
        TICKET_DISPATCH_KEY.into(),
        serde_json::to_string(&dispatch)?,
    );
    persisted.metadata_version = persisted
        .metadata_version
        .checked_add(1)
        .context("metadata revision overflow")?;
    state.storage.save_session(&persisted).await?;
    let saved = state
        .storage
        .load_runtime_control_plane(&child_id)
        .await?
        .context("canonical receipt missing")?;
    ensure!(
        saved.created_at == persisted.created_at && saved.id == receipt.session_id,
        "receipt birth changed"
    );
    let runtime = Authority::from_verified_host(snapshot.binding, Principal::Runtime);
    ticket_command(
        &service,
        &runtime,
        "fixture-synthetic-admission",
        vec![
            Operation::Admitted {
                assignment_id: assignment_id.clone(),
                receipt: receipt.clone(),
            },
            Operation::Running {
                assignment_id: assignment_id.clone(),
            },
        ],
    )?;
    ticket_command(
        &service,
        &owner,
        "fixture-pause-synthetic",
        vec![Operation::Pause {
            work_id: work_ids["active"].clone(),
            reason: "Synthetic fixture is stopped; never resume a real Worker".into(),
        }],
    )?;
    ticket_command(
        &service,
        &runtime,
        "fixture-confirm-synthetic-stop",
        vec![Operation::ConfirmStopped {
            assignment_id: assignment_id.clone(),
            effects_reconciled: true,
        }],
    )?;
    let (commit, snapshot) = service.published()?;
    let mut marker = SeedMarker {
        kind: MARKER_KIND.into(),
        version: 1,
        root: root.into(),
        created_at: chrono::Utc::now().to_rfc3339(),
        config_fingerprint,
        binding: snapshot.binding.clone(),
        project_id: project.id,
        project_created_at: project.created_at.to_rfc3339(),
        work_ids,
        sprint_ids: sprint_ids.clone(),
        sprint_created_seq: sprint_ids
            .iter()
            .map(|(role, id)| (role.clone(), snapshot.sprint.sprints[id].created_seq))
            .collect(),
        supervisor_id: supervisor.id,
        supervisor_created_at: supervisor.created_at.to_rfc3339(),
        child_id,
        child_created_at: saved.created_at.to_rfc3339(),
        start_session_id: start.id,
        start_session_created_at: start.created_at.to_rfc3339(),
        session_hashes: BTreeMap::new(),
        assignment_id,
        runtime_receipt: receipt,
        seed_snapshot: SnapshotRef {
            commit,
            seq: snapshot.seq,
            authority_epoch: snapshot.authority_epoch,
        },
        seed_receipt_hashes: snapshot
            .receipts
            .iter()
            .map(|(id, receipt)| Ok((id.clone(), content_hash(&canonical_bytes(receipt)?))))
            .collect::<FixtureResult<_>>()?,
    };
    for record in persisted_sessions(state, &marker).await? {
        marker.session_hashes.insert(
            record["role"]
                .as_str()
                .context("Session role missing")?
                .into(),
            record["sha256"]
                .as_str()
                .context("Session hash missing")?
                .into(),
        );
    }
    write_new_json(&root.join(MARKER_FILE), &marker)?;
    Ok(marker)
}

async fn verify_identities(state: &AppState, marker: &SeedMarker) -> FixtureResult<Value> {
    let facade = state
        .config_facade
        .as_ref()
        .context("fixture modular config missing")?;
    ensure!(
        content_hash(&canonical_bytes(&facade.effective_config())?) == marker.config_fingerprint,
        "synthetic config changed"
    );
    let service = state.tickets.service()?;
    let snapshot = service.published()?.1;
    ensure!(
        snapshot.binding == marker.binding,
        "fixture scope identity changed"
    );
    let project = state.project_store.get(&marker.project_id)?;
    ensure!(
        project.created_at.to_rfc3339() == marker.project_created_at,
        "Project birth changed"
    );
    for id in marker.work_ids.values() {
        ensure!(
            snapshot
                .tickets
                .get(id)
                .is_some_and(|w| w.kind == TicketKind::Work)
                && snapshot.project.links.get(id) == Some(&marker.project_id),
            "seed Work identity or Project changed"
        );
    }
    for (role, id) in &marker.sprint_ids {
        let sprint = snapshot
            .sprint
            .sprints
            .get(id)
            .context("seed Sprint missing")?;
        ensure!(
            sprint.project_id == marker.project_id
                && sprint.created_seq == marker.sprint_created_seq[role],
            "Sprint identity changed"
        );
    }
    for (id, expected) in &marker.seed_receipt_hashes {
        let receipt = snapshot
            .receipts
            .get(id)
            .context("seed audit receipt disappeared")?;
        ensure!(
            &content_hash(&canonical_bytes(receipt)?) == expected,
            "seed audit receipt changed"
        );
    }
    let supervisor = state
        .storage
        .load_root_authority(&marker.supervisor_id)
        .await?
        .context("canonical Supervisor missing")?;
    ensure!(
        supervisor.created_at.to_rfc3339() == marker.supervisor_created_at
            && supervisor.kind == SessionKind::Root,
        "Supervisor birth changed"
    );
    let start = state
        .storage
        .load_root_authority(&marker.start_session_id)
        .await?
        .context("canonical initial Root missing")?;
    ensure!(
        start.created_at.to_rfc3339() == marker.start_session_created_at
            && start.kind == SessionKind::Root
            && start.authority_identity == SessionAuthorityIdentity::Ordinary
            && start.parent_session_id.is_none()
            && start.created_at > supervisor.created_at,
        "initial Root birth changed"
    );
    let child = state
        .storage
        .load_runtime_control_plane(&marker.child_id)
        .await?
        .context("canonical Child missing")?;
    ensure!(
        start.created_at > child.created_at,
        "initial Root must remain newer than Child"
    );
    ensure!(
        child.id == ticket_runtime::ticket_child_id(&marker.runtime_receipt.dispatch_key)
            && child.kind == SessionKind::Child
            && child.parent_session_id.as_deref() == Some(marker.supervisor_id.as_str())
            && child.parent_created_at == Some(supervisor.created_at)
            && child.created_at.to_rfc3339() == marker.child_created_at,
        "canonical Child identity or birth changed"
    );
    ensure!(
        child
            .messages
            .iter()
            .all(|message| message.role == Role::System),
        "fixture Child acquired non-system messages"
    );
    ensure!(
        child.metadata.get(TICKET_LOCAL_PLAN_KEY) == Some(&marker.assignment_id),
        "Child Assignment link changed"
    );
    let dispatch = ticket_runtime::read_dispatch(&child)?.context("Child dispatch missing")?;
    ensure!(
        dispatch.binding == marker.binding
            && dispatch.assignment_id == marker.assignment_id
            && dispatch.receipt.as_ref() == Some(&marker.runtime_receipt)
            && dispatch.receipt_created_at == Some(child.created_at),
        "canonical receipt link changed"
    );
    let assignment = snapshot
        .assignments
        .get(&marker.assignment_id)
        .context("seed Assignment missing")?;
    ensure!(
        assignment.work_id == marker.work_ids["active"]
            && assignment.process_stopped
            && assignment.runtime.as_ref() == Some(&marker.runtime_receipt),
        "synthetic stopped Assignment changed"
    );
    for session in [&supervisor, &child, &start] {
        ensure!(
            session.project_id_meta().as_deref() == Some(marker.project_id.as_str())
                && session.workspace_path_meta().as_deref()
                    == Some(marker.root.join("workspace").to_string_lossy().as_ref()),
            "Session Project/workspace changed"
        );
        ensure!(
            session
                .messages
                .iter()
                .all(|message| message.role == Role::System),
            "fixture Session acquired non-system messages"
        );
    }
    let intent = snapshot
        .intents
        .get(&marker.runtime_receipt.dispatch_key)
        .context("seed intent missing")?;
    let observation = ticket_runtime::query_dispatch(
        state.storage.as_ref(),
        &service,
        &marker.runtime_receipt.dispatch_key,
        &intent.immutable_spec,
    )
    .await?;
    ensure!(
        matches!(&observation, ticket_runtime::DispatchObservation::Terminal { receipt, terminal_status }
        if receipt == &marker.runtime_receipt && terminal_status == "completed"),
        "canonical Child join is not the synthetic terminal receipt"
    );
    let records = persisted_sessions(state, marker).await?;
    for record in &records {
        ensure!(
            marker
                .session_hashes
                .get(record["role"].as_str().context("Session role missing")?)
                .is_some_and(|hash| record["sha256"].as_str() == Some(hash.as_str())),
            "persisted Session main bytes changed"
        );
    }
    Ok(
        json!({"supervisor_id":supervisor.id,"supervisor_created_at":supervisor.created_at,
        "child_id":child.id,"child_created_at":child.created_at,"parent_session_id":child.parent_session_id,
        "parent_created_at":child.parent_created_at,"child_message_count":child.messages.len(),
        "child_dispatch":dispatch,"assignment_process_stopped":assignment.process_stopped,
        "start_session_id":start.id,"start_session_created_at":start.created_at,"dispatch_observation":observation,"persisted_sessions":records}),
    )
}

async fn persisted_sessions(state: &AppState, marker: &SeedMarker) -> FixtureResult<Vec<Value>> {
    let store_root = state.session_store.sessions_root_dir().canonicalize()?;
    ensure!(
        store_root.starts_with(&marker.root),
        "Session store escaped the fixture root"
    );
    let mut records = Vec::new();
    for (role, id) in [
        ("root", &marker.supervisor_id),
        ("child", &marker.child_id),
        ("start", &marker.start_session_id),
    ] {
        let session = state
            .storage
            .load_session(id)
            .await?
            .context("persisted fixture Session missing")?;
        ensure!(
            session.id == *id
                && session
                    .messages
                    .iter()
                    .all(|message| message.role == Role::System),
            "persisted Session identity or messages changed"
        );
        let relative = state
            .session_store
            .resolve_rel_path(id)
            .await
            .context("canonical Session path missing")?;
        let relative = Path::new(&relative);
        ensure!(
            !relative.is_absolute()
                && relative
                    .components()
                    .all(|part| matches!(part, std::path::Component::Normal(_))),
            "invalid canonical Session path"
        );
        let path = state.app_data_dir.join(relative).join("session.json");
        plain_file(&path)?;
        let path = path.canonicalize()?;
        ensure!(
            path.starts_with(&store_root),
            "Session main escaped its store"
        );
        let expected = if role == "child" {
            store_root
                .join(&marker.supervisor_id)
                .join("children")
                .join(id)
        } else {
            store_root.join(id)
        }
        .join("session.json");
        ensure!(
            path == expected,
            "Session main is outside its canonical tree"
        );
        records.push(json!({"role":role,"id":session.id,"kind":session.kind,"project_id":session.project_id_meta(),
            "parent_session_id":session.parent_session_id,"root_session_id":session.root_session_id,
            "created_at":session.created_at,"path":path,"sha256":content_hash(&fs::read(&path)?)}));
    }
    Ok(records)
}

async fn probe(
    client: &reqwest::Client,
    base: &str,
    path: &str,
    body: Option<Value>,
    stop: &Path,
    proofs: &mut Vec<Value>,
) -> FixtureResult<Value> {
    check_stop(stop)?;
    let method = if body.is_some() { "POST" } else { "GET" };
    let request = match &body {
        Some(body) => client.post(format!("{base}{path}")).json(body),
        None => client.get(format!("{base}{path}")),
    };
    let response = request
        .send()
        .await
        .with_context(|| format!("{method} {path}"))?;
    let status = response.status();
    ensure!(status.is_success(), "{method} {path} returned {status}");
    let response: Value = response.json().await?;
    proofs.push(json!({"method":method,"path":path,"request":body,"status":status.as_u16(),"response":response}));
    Ok(response)
}

async fn run_fixture(mode: &str) -> FixtureResult<()> {
    ensure!(
        matches!(mode, "seed" | "reopen"),
        "BAMBOO_SPRINT_FIXTURE_MODE must be seed or reopen"
    );
    let requested_root = absolute_env("BAMBOO_SPRINT_FIXTURE_ROOT")?;
    let requested_receipt = absolute_env("BAMBOO_SPRINT_FIXTURE_RECEIPT")?;
    ensure!(
        requested_receipt
            .extension()
            .is_some_and(|extension| extension == "json"),
        "receipt needs a .json extension"
    );
    let address: SocketAddr = std::env::var("BAMBOO_SPRINT_FIXTURE_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:19562".into())
        .parse()?;
    ensure!(
        address.ip().is_loopback(),
        "fixture listener must be loopback-only"
    );
    let frontend = std::env::var_os("BAMBOO_SPRINT_FIXTURE_FRONTEND")
        .map(PathBuf::from)
        .map(|path| -> FixtureResult<_> {
            ensure!(path.is_absolute(), "frontend dist must be absolute");
            plain_directory(&path)?;
            let path = path.canonicalize()?;
            plain_file(&path.join("index.html"))?;
            Ok(path)
        })
        .transpose()?;
    let seed = mode == "seed";
    let root = prepare_root(&requested_root, seed)?;
    let receipt_path = new_output(&requested_receipt)?;
    let shutdown_marker = receipt_path.with_extension("stop");
    let shutdown_receipt = new_output(&receipt_path.with_extension("shutdown.json"))?;
    let existing = if seed {
        None
    } else {
        Some(read_marker(&root)?)
    };
    let source = std::env::var("BAMBOO_FIXTURE_SOURCE").ok();
    if stop_requested(&shutdown_marker)? {
        write_new_json(
            &shutdown_receipt,
            &json!({"mode":mode,"root":root,"source":source,"reason":"own shutdown marker before startup",
            "ready_published":false,"http_server_started":false,"root_preserved":true,"actual_process_exit":"launcher must wait for the process"}),
        )?;
        return Ok(());
    }
    // Refuse ambient credentials before Config's environment hydration can see them.
    ensure!(
        !std::env::vars_os()
            .any(|(name, value)| name.to_string_lossy().ends_with("API_KEY") && !value.is_empty()),
        "clear API_KEY environment variables before this isolated fixture"
    );
    let before_open = existing
        .as_ref()
        .map(|marker| -> FixtureResult<_> {
            let offline = TicketService::open_offline(
                scope_root(&root, &marker.binding)?,
                marker.binding.clone(),
            )?;
            Ok(offline.published()?)
        })
        .transpose()?;
    bamboo_config::paths::init_bamboo_dir(root.clone());
    ensure!(
        bamboo_config::paths::bamboo_dir() == root,
        "run this opt-in fixture alone in a fresh process"
    );
    if seed {
        fs::create_dir(root.join("workspace"))?;
        fs::create_dir(root.join("jiandu"))?;
        let config: Config = serde_json::from_value(json!({
            "provider":"openai","http_proxy":"","https_proxy":"",
            "default_work_area":{"path":root.join("workspace")},
            "features":{"ticket_mutation":true,"ticket_dispatch":false},
            "providers":{"openai":{"api_key":"","model":MODEL,"base_url":DISABLED_PROVIDER_URL}},
            "setup":{"completed":true,"version":1,"completed_at":chrono::Utc::now().to_rfc3339()},
            "memory":{"auto_dream_enabled":false,"gardener_enabled":false,"dedup_gardener_enabled":false,
                "granularity_freshness_gardener_enabled":false,"ledger_gardener_enabled":false,"ledger_distillation_enabled":false}
        }))?;
        config.save_to_dir(root.clone())?;
    }
    if stop_requested(&shutdown_marker)? {
        write_new_json(
            &shutdown_receipt,
            &json!({"mode":mode,"root":root,"source":source,"reason":"own shutdown marker before AppState",
            "ready_published":false,"http_server_started":false,"root_preserved":true,"actual_process_exit":"launcher must wait for the process"}),
        )?;
        return Ok(());
    }
    // Validate a reopened policy before constructing any provider, MCP or other runtime.
    // This fingerprint is a fixture identity witness, never another config authority.
    let config_fingerprint = {
        let facade = bamboo_config::ConfigFacade::open_or_migrate(&root)?;
        let persisted = facade.effective_config();
        let mut runtime = persisted.clone();
        runtime.apply_runtime_env_overrides();
        let bytes = canonical_bytes(&persisted)?;
        ensure!(
            canonical_bytes(&runtime)? == bytes,
            "clear runtime Config overrides for this isolated fixture"
        );
        let fingerprint = content_hash(&bytes);
        if let Some(marker) = &existing {
            ensure!(
                fingerprint == marker.config_fingerprint,
                "reopen policy changed; refusing runtime initialization"
            );
        }
        fingerprint
    };
    let state = AppState::new_with_memory_store(
        root.clone(),
        bamboo_memory::memory_store::MemoryStore::new(root.join("jiandu")),
    )
    .await?;
    if stop_requested(&shutdown_marker)? {
        state.shutdown().await;
        write_new_json(
            &shutdown_receipt,
            &json!({"mode":mode,"root":root,"source":source,"reason":"own shutdown marker after AppState",
            "ready_published":false,"http_server_started":false,"root_preserved":true,"actual_process_exit":"launcher must wait for the process"}),
        )?;
        return Ok(());
    }
    {
        let config = state.config.read().await;
        let provider = config
            .providers()
            .openai
            .as_ref()
            .context("synthetic provider missing")?;
        ensure!(
            config.features.ticket_mutation
                && !config.features.ticket_dispatch
                && provider.api_key.is_empty()
                && provider.api_key_encrypted.is_none()
                && provider.credential_ref.is_none()
                && provider.model.as_deref() == Some(MODEL)
                && provider.base_url.as_deref() == Some(DISABLED_PROVIDER_URL),
            "fixture config must retain disabled dispatch and credential-free synthetic provider"
        );
        ensure!(
            config
                .extra
                .get("setup")
                .and_then(|v| v.get("completed"))
                .and_then(Value::as_bool)
                == Some(true),
            "persisted setup completion missing"
        );
    }
    // Replace both reloadable/default and model-ref handles with an in-memory refusal.
    let no_execution = Arc::new(NoExecutionProvider::default());
    let provider: Arc<dyn LLMProvider> = no_execution.clone();
    *state.provider.write().await = provider.clone();
    state.provider_registry.replace_with(ProviderRegistry::new(
        HashMap::from([("openai".into(), provider)]),
        "openai".into(),
    ));
    let marker = match existing {
        Some(marker) => marker,
        None => seed_state(&state, &root, config_fingerprint).await?,
    };
    let service = state.tickets.service()?;
    let (ready_commit, ready_snapshot) = service.published()?;
    if let Some((_, before)) = &before_open {
        ensure!(
            canonical_bytes(&before.tickets)? == canonical_bytes(&ready_snapshot.tickets)?
                && canonical_bytes(&before.assignments)?
                    == canonical_bytes(&ready_snapshot.assignments)?
                && canonical_bytes(&before.receipts)? == canonical_bytes(&ready_snapshot.receipts)?
                && canonical_bytes(&before.sprint)? == canonical_bytes(&ready_snapshot.sprint)?
                && canonical_bytes(&before.project)? == canonical_bytes(&ready_snapshot.project)?
                && canonical_bytes(&before.intents)? == canonical_bytes(&ready_snapshot.intents)?,
            "reopen changed canonical Work, Assignment, history or audit facts"
        );
    }
    let identities_before = verify_identities(&state, &marker).await?;
    let state = web::Data::new(state);
    let listener = std::net::TcpListener::bind(address)?;
    let base = format!("http://{}", listener.local_addr()?);
    let server_state = state.clone();
    let server = HttpServer::new(move || {
        let frontend = frontend.clone();
        App::new()
            .app_data(server_state.clone())
            .configure(bamboo_server::configure_routes)
            .configure(move |config| {
                if let Some(path) = frontend {
                    config.service(actix_files::Files::new("/", path).index_file("index.html"));
                }
            })
    })
    .workers(1)
    .disable_signals()
    .shutdown_timeout(5)
    .listen(listener)?
    .run();
    let handle = server.handle();
    let task = actix_web::rt::spawn(server);
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    let mut probes = Vec::new();
    let mut ready_published = false;
    let lifetime = async {
        probe(&client, &base, "/healthz", None, &shutdown_marker, &mut probes).await?;
        let setup = probe(&client, &base, "/api/v1/setup/status", None, &shutdown_marker, &mut probes).await?;
        ensure!(setup["is_complete"] == true, "Lotus setup would block the fixture");
        probe(&client, &base, "/api/v1/projects", None, &shutdown_marker, &mut probes).await?;
        probe(&client, &base, &format!("/api/v1/projects/{}", marker.project_id), None, &shutdown_marker, &mut probes).await?;
        probe(&client, &base, "/api/v1/tickets/scope", None, &shutdown_marker, &mut probes).await?;
        for filter in [Value::Null, json!({"kind":"backlog"}), json!({"kind":"sprint","sprint_id":marker.sprint_ids["planned"]}), json!({"kind":"sprint","sprint_id":marker.sprint_ids["active"]}), json!({"kind":"sprint","sprint_id":marker.sprint_ids["closed"]})] {
            probe(&client, &base, "/api/v1/tickets/project-board", Some(json!({"project_id":marker.project_id,"limit":100,"include_cancelled":true,"sprint_filter":filter})), &shutdown_marker, &mut probes).await?;
        }
        probe(&client, &base, "/api/v1/tickets/sprints/list", Some(json!({"project_id":marker.project_id,"limit":100})), &shutdown_marker, &mut probes).await?;
        for id in marker.sprint_ids.values() {
            probe(&client, &base, "/api/v1/tickets/sprints/inspect", Some(json!({"project_id":marker.project_id,"sprint_id":id})), &shutdown_marker, &mut probes).await?;
        }
        let roots = probe(&client, &base, "/api/v1/sessions?kind=root&limit=100", None, &shutdown_marker, &mut probes).await?;
        ensure!(roots["sessions"].as_array().and_then(|rows| rows.first()).is_some_and(|row| row["id"] == marker.start_session_id), "initial Root is not the newest real Root");
        let children = probe(&client, &base, &format!("/api/v1/sessions?kind=child&root_session_id={}&limit=100", marker.supervisor_id), None, &shutdown_marker, &mut probes).await?;
        ensure!(children["sessions"].as_array().is_some_and(|rows| rows.iter().any(|row| row["id"] == marker.child_id)), "root-scoped listing omitted canonical Child");
        for id in [&marker.supervisor_id, &marker.child_id, &marker.start_session_id] {
            let detail = probe(&client, &base, &format!("/api/v1/sessions/{id}"), None, &shutdown_marker, &mut probes).await?;
            ensure!(detail["session"]["id"].as_str() == Some(id.as_str())
                && detail["session"]["project_id"] == marker.project_id.as_str()
                && detail["session"]["is_running"] == false, "real Session GET identity, Project or stop state mismatch");
            if id == &marker.supervisor_id { ensure!(detail["session"]["subagent_count"].as_u64().is_some_and(|count| count >= 1), "root did not count its canonical Child"); }
            let history = probe(&client, &base, &format!("/api/v1/sessions/{id}/history"), None, &shutdown_marker, &mut probes).await?;
            ensure!(history["messages"].as_array().is_some_and(|messages| messages.iter().all(|message| message["role"] == "system")), "real Session history contains non-system messages");
        }
        ensure!(no_execution.0.load(Ordering::SeqCst) == 0, "startup attempted model execution");
        check_stop(&shutdown_marker)?;
        let assignment = &ready_snapshot.assignments[&marker.assignment_id];
        let current_exe = std::env::current_exe()?;
        let mut proof = json!({"status":"ready","schema_version":1,"fixture_kind":"lotus381-production-routes","fixture_id":marker.project_id,
            "fixture_mode":mode,"seed_reused":!seed,"isolated":true,"synthetic_data":true,
            "child_creation_supported":true,"reopen_supported":true,"process_pid":std::process::id(),
            "mode":mode,"base_url":base,"root":root,"source":source,
            "source_contract":"caller-supplied expected SHA is evidence only; launcher verifies binary provenance separately",
            "project_id":marker.project_id,"work_id":marker.work_ids["active"],"work_ids":marker.work_ids,"works":marker.work_ids,
            "sprint_ids":marker.sprint_ids,"supervisor_id":marker.supervisor_id,"child_id":marker.child_id,
            "root_session_id":marker.supervisor_id,"target_session_id":marker.child_id,"start_session_id":marker.start_session_id,
            "assignment_id":marker.assignment_id,"runtime_receipt":marker.runtime_receipt,
            "runtime":{"run_id":marker.runtime_receipt.run_id,"generation":assignment.generation,"authority_epoch":assignment.authority_epoch},
            "runtime_epoch_contract":"Assignment adopted epoch is historical; current scope epoch is reported separately"});
        if let Value::Object(fields) = json!({
            "persisted_sessions":identities_before["persisted_sessions"],"session_store_root":state.session_store.sessions_root_dir(),
            "sprint":{"active_id":marker.sprint_ids["active"],"closed_id":marker.sprint_ids["closed"],
                "backlog_work_ids":[marker.work_ids["backlog"],marker.work_ids["carried_backlog"]],
                "member_work_ids":[marker.work_ids["planned"],marker.work_ids["carried_planned"],marker.work_ids["active"]],
                "planning_commands_allowed":false,"capability_contract":"read-only browser harness; production Scope authority is unchanged"},
            "activation":"synthetic host activation; canonical persisted Ticket/Child join, no router, RunSpec, model run or Worker execution",
            "auth":"loopback local_bypass; no real credentials","ticket_dispatch":false,"provider_execution":false,
            "root_workspace":root.join("workspace"),"jiandu_root":root.join("jiandu"),
            "binary":current_exe,"current_exe":current_exe,"shutdown_marker":shutdown_marker,"shutdown_receipt":shutdown_receipt,
            "seed_marker":root.join(MARKER_FILE),"seed_marker_hash":content_hash(&fs::read(root.join(MARKER_FILE))?),
            "seed_created_at":marker.created_at,"seed_snapshot":marker.seed_snapshot}) {
            proof.as_object_mut().expect("proof object").extend(fields);
        }
        if let Value::Object(fields) = json!({
            "before_open":before_open.as_ref().map(|(commit,snapshot)| scope_snapshot(commit,snapshot)),
            "after_open":scope_snapshot(&ready_commit,&ready_snapshot),"scope_snapshot":scope_snapshot(&ready_commit,&ready_snapshot),
            "identities":identities_before,"startup_probes":probes,"ttl_seconds":1800}) {
            proof.as_object_mut().expect("proof object").extend(fields);
        }
        write_new_json(&receipt_path, &proof)?;
        ready_published = true;
        println!("SPRINT_BROWSER_FIXTURE {}", receipt_path.display());
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1800);
        loop {
            check_stop(&shutdown_marker)?;
            if tokio::time::Instant::now() >= deadline { break; }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        Ok::<_, anyhow::Error>(())
    }.await;
    drop(client);
    handle.stop(true).await;
    task.await??;
    state.shutdown().await;
    let (commit, snapshot) = service.published()?;
    let identities_after = verify_identities(&state, &marker).await?;
    let reason = match &lifetime {
        Ok(()) => "fixture TTL expired",
        Err(error) if error.is::<OwnStop>() => "own shutdown marker",
        Err(_) => "startup or listening failure",
    };
    write_new_json(
        &shutdown_receipt,
        &json!({"mode":mode,"root":root,"source":source,"reason":reason,"ready_published":ready_published,
        "http_server_stopped":true,"actual_process_exit":"not observed here; launcher must wait for the process",
        "seed_marker":root.join(MARKER_FILE),"seed_marker_hash":content_hash(&fs::read(root.join(MARKER_FILE))?),
        "before_listening":scope_snapshot(&ready_commit,&ready_snapshot),"after_listening":scope_snapshot(&commit,&snapshot),
        "identities_before":identities_before,"identities_after":identities_after,"startup_probes":probes,
        "failure":lifetime.as_ref().err().filter(|error| !error.is::<OwnStop>()).map(|error| format!("{error:#}")),
        "blocked_model_execution_attempts":no_execution.0.load(Ordering::SeqCst),"root_preserved":true}),
    )?;
    match lifetime {
        Err(error) if !error.is::<OwnStop>() => Err(error),
        _ => Ok(()),
    }
}

#[actix_web::test]
async fn lotus_sprint_browser_fixture() {
    let Some(mode) = std::env::var_os("BAMBOO_SPRINT_FIXTURE_MODE") else {
        return;
    };
    run_fixture(mode.to_str().expect("fixture mode must be UTF-8"))
        .await
        .expect("isolated Sprint browser fixture");
}
