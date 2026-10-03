//! Explicit offline Host ports. Preview validates a bounded temporary copy;
//! it never runs initialization/migration against the user's source directory.
use anyhow::{bail, ensure, Context};
use bamboo_domain::{
    ActorDirectoryEntry, ActorLogicalState, AgentStatusState, Session, SessionAuthorityIdentity,
    Storage, DEFAULT_SUPERVISOR_SESSION_ID,
};
use bamboo_engine::{ticket_runtime, ticket_worker_plan::tickets::*};
use bamboo_storage::SessionStoreV2;
use clap::Subcommand;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};
mod reconcile;

#[derive(Debug, Subcommand)]
pub enum TicketCommands {
    /// Observe an uncertain file effect offline; print an exact User request.
    FileReconcilePlan {
        #[arg(long)]
        data_dir: PathBuf,
        #[arg(long)]
        assignment_id: String,
        #[arg(long)]
        effect_id: String,
        #[arg(long)]
        operation_id: String,
        #[arg(long)]
        evidence: String,
    },
    /// Consume a reviewed file observation request; never rewrite the file.
    FileReconcile {
        #[arg(long)]
        data_dir: PathBuf,
        #[arg(long)]
        request: PathBuf,
    },
    /// Read-only legacy Task mapping and ownership eligibility.
    Preview {
        #[arg(long)]
        data_dir: PathBuf,
        #[arg(long,default_value=DEFAULT_SUPERVISOR_SESSION_ID)]
        source_session: String,
    },
    /// Explicitly attach an inert canonical Supervisor; never a title match.
    Attach {
        #[arg(long)]
        data_dir: PathBuf,
        #[arg(long)]
        expected_snapshot: String,
        #[arg(long)]
        operation_id: String,
        #[arg(long = "task-id")]
        tasks: Vec<String>,
    },
    /// Import selected inert legacy Tasks into the existing scope as needs-review.
    Import {
        #[arg(long)]
        data_dir: PathBuf,
        #[arg(long)]
        source_session: String,
        #[arg(long)]
        expected_snapshot: String,
        #[arg(long)]
        operation_id: String,
        #[arg(long = "task-id", required = true)]
        tasks: Vec<String>,
    },
    /// Export a verified fixed commit. This copy always stays read-only.
    Backup {
        #[arg(long)]
        data_dir: PathBuf,
        #[arg(long)]
        destination: PathBuf,
    },
    /// Prepare a reviewable exact migration request, with no source writes.
    MigrationPlan {
        #[arg(long)]
        data_dir: PathBuf,
        #[arg(long)]
        destination: PathBuf,
        #[arg(long)]
        operation_id: String,
    },
    /// Stop both Hosts first, then consume the reviewed JSON request offline.
    Migrate {
        #[arg(long)]
        data_dir: PathBuf,
        #[arg(long)]
        request: PathBuf,
    },
}

pub async fn run(command: TicketCommands) -> anyhow::Result<()> {
    let value = match command {
        TicketCommands::FileReconcilePlan {
            data_dir,
            assignment_id,
            effect_id,
            operation_id,
            evidence,
        } => {
            reconcile::plan(
                &data_dir,
                &assignment_id,
                &effect_id,
                &operation_id,
                &evidence,
            )
            .await?
        }
        TicketCommands::FileReconcile { data_dir, request } => {
            reconcile::commit(&data_dir, &request).await?
        }
        TicketCommands::Preview {
            data_dir,
            source_session,
        } => preview(&data_dir, &source_session).await?,
        TicketCommands::Attach {
            data_dir,
            expected_snapshot,
            operation_id,
            tasks,
        } => {
            legacy_commit(
                &data_dir,
                DEFAULT_SUPERVISOR_SESSION_ID,
                &expected_snapshot,
                &operation_id,
                &tasks,
                true,
            )
            .await?
        }
        TicketCommands::Import {
            data_dir,
            source_session,
            expected_snapshot,
            operation_id,
            tasks,
        } => {
            legacy_commit(
                &data_dir,
                &source_session,
                &expected_snapshot,
                &operation_id,
                &tasks,
                false,
            )
            .await?
        }
        TicketCommands::Backup {
            data_dir,
            destination,
        } => {
            let snapshot = read_source(&data_dir, DEFAULT_SUPERVISOR_SESSION_ID).await?;
            let binding = binding(&snapshot.session)?;
            let service =
                TicketService::open_offline(scope_root(&data_dir, &snapshot.session)?, binding)?;
            json!({"status":"read_only_backup","commit":service.export(destination)?})
        }
        TicketCommands::MigrationPlan {
            data_dir,
            destination,
            operation_id,
        } => migration_plan(&data_dir, &destination, &operation_id).await?,
        TicketCommands::Migrate { data_dir, request } => {
            let bytes = fs::read(request)?;
            ensure!(bytes.len() <= 32 * 1024, "migration request exceeds budget");
            migrate(&data_dir, &serde_json::from_slice(&bytes)?).await?
        }
    };
    println!("{}", serde_json::to_string_pretty(&value)?);
    Ok(())
}

struct Temporary(PathBuf);
impl Drop for Temporary {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct SourceFile {
    bytes: Vec<u8>,
}
type SourceFiles = BTreeMap<String, SourceFile>;
struct SourceSnapshot {
    session: Session,
    files: SourceFiles,
    file_bytes: Vec<u8>,
    cold: bool,
}

fn selector(id: &str) -> anyhow::Result<()> {
    ensure!(
        !id.is_empty()
            && id.len() <= 128
            && !id.contains(['/', '\\'])
            && !id.contains("..")
            && !id.chars().any(char::is_control),
        "invalid source Session selector"
    );
    Ok(())
}
fn collect(root: &Path, relative: &Path, files: &mut SourceFiles) -> anyhow::Result<()> {
    ensure!(
        relative.components().count() <= 8,
        "source directory depth exceeds budget"
    );
    for entry in fs::read_dir(root.join(relative))? {
        let entry = entry?;
        let path = relative.join(entry.file_name());
        let meta = fs::symlink_metadata(entry.path())?;
        ensure!(!meta.file_type().is_symlink(), "symlink in source snapshot");
        if meta.is_dir() {
            collect(root, &path, files)?;
        } else {
            ensure!(meta.is_file(), "nonregular source snapshot entry");
            let name = path
                .to_str()
                .context("source snapshot needs UTF-8 filenames")?;
            if name.ends_with(".lock") || name.contains(".staging-") {
                continue;
            }
            ensure!(
                meta.len() <= store::MAX_ARTIFACT_BYTES as u64 && files.len() < 512,
                "source snapshot file budget exceeded"
            );
            files.insert(
                name.to_owned(),
                SourceFile {
                    bytes: fs::read(entry.path())?,
                },
            );
        }
    }
    Ok(())
}
fn real_directories(path: &Path) -> anyhow::Result<()> {
    let mut current = PathBuf::new();
    for part in path.components() {
        current.push(part.as_os_str());
        match fs::symlink_metadata(&current) {
            Ok(m) => ensure!(
                m.is_dir() && !m.file_type().is_symlink(),
                "snapshot destination component is not a real directory"
            ),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                fs::create_dir(&current)?;
                std::fs::File::open(current.parent().context("directory parent")?)?.sync_all()?;
            }
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}
fn restore(root: &Path, files: &SourceFiles) -> anyhow::Result<()> {
    real_directories(root)?;
    for (name, file) in files {
        let path = Path::new(name);
        ensure!(
            !path.is_absolute()
                && path
                    .components()
                    .all(|p| matches!(p, std::path::Component::Normal(_))),
            "unsafe snapshot member"
        );
        let target = root.join(path);
        let parent = target.parent().context("missing parent")?;
        real_directories(parent)?;
        // Each existing component is checked; no symlink can redirect a member.
        let mut current = root.to_path_buf();
        ensure!(
            !fs::symlink_metadata(&current)?.file_type().is_symlink(),
            "snapshot destination symlink"
        );
        for part in path.components() {
            current.push(part.as_os_str());
            if current.exists() {
                ensure!(
                    !fs::symlink_metadata(&current)?.file_type().is_symlink(),
                    "snapshot destination symlink"
                );
            }
        }
        if target.exists() {
            ensure!(
                fs::read(&target)? == file.bytes,
                "existing destination snapshot differs"
            );
            continue;
        }
        let mut output = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&target)?;
        output.write_all(&file.bytes)?;
        output.sync_all()?;
        std::fs::File::open(parent)?.sync_all()?;
    }
    std::fs::File::open(root)?.sync_all()?;
    Ok(())
}

async fn read_source(data: &Path, id: &str) -> anyhow::Result<SourceSnapshot> {
    selector(id)?;
    let data = data.canonicalize()?;
    let directory = data.join("sessions").join(id);
    ensure!(
        !fs::symlink_metadata(&directory)?.file_type().is_symlink(),
        "source Session directory is a symlink"
    );
    ensure!(
        directory.join("root-tool-authority.json").is_file(),
        "uninitialized legacy authority: read-only reconciliation required"
    );
    if id == DEFAULT_SUPERVISOR_SESSION_ID {
        ensure!(
            directory.join("supervisor-authority.json").is_file(),
            "canonical Supervisor proof required"
        );
    }
    let mut files = SourceFiles::new();
    collect(&directory, Path::new(""), &mut files)?;
    let file_bytes = canonical_bytes(&files)?;
    ensure!(
        file_bytes.len() <= store::MAX_ARTIFACT_BYTES,
        "complete source snapshot exceeds 1 MiB; split/reconcile, never truncate"
    );
    let temp = Temporary(
        std::env::temp_dir()
            .canonicalize()?
            .join(format!("bamboo-ticket-preview-{}", uuid::Uuid::new_v4())),
    );
    restore(&temp.0.join("sessions").join(id), &files)?;
    let storage = SessionStoreV2::new(temp.0.clone()).await?;
    let session = storage
        .load_root_authority(id)
        .await?
        .context("canonical source Root missing")?;
    let actor = files
        .get("actor-authority.json")
        .map(|f| serde_json::from_slice::<ActorDirectoryEntry>(&f.bytes))
        .transpose()?;
    let cold = actor
        .as_ref()
        .is_none_or(|a| a.actor.state == ActorLogicalState::Cold && a.activation.is_none())
        && session.last_run_status().is_none()
        && session
            .agent_runtime_state
            .as_ref()
            .is_none_or(|r| r.status == AgentStatusState::Idle)
        && !files.keys().any(|p| p.starts_with("children/"));
    let mut after = SourceFiles::new();
    collect(&directory, Path::new(""), &mut after)?;
    ensure!(
        files == after,
        "source changed during read; retry a fixed snapshot"
    );
    Ok(SourceSnapshot {
        session,
        files,
        file_bytes,
        cold,
    })
}

fn binding(session: &Session) -> anyhow::Result<ScopeBinding> {
    let SessionAuthorityIdentity::Supervisor { incarnation_id } = session.authority_identity else {
        bail!("canonical Supervisor required")
    };
    ensure!(
        session.root_orchestration_only_enabled() && session.root_tool_authority_revision > 0,
        "explicit attach required"
    );
    Ok(ScopeBinding {
        scope_id: format!("supervisor/{incarnation_id}"),
        supervisor_session_id: session.id.clone(),
        binding_revision: session.root_tool_authority_revision,
    })
}
fn scope_root(data: &Path, session: &Session) -> anyhow::Result<PathBuf> {
    let SessionAuthorityIdentity::Supervisor { incarnation_id } = session.authority_identity else {
        bail!("canonical Supervisor required")
    };
    Ok(data
        .canonicalize()?
        .join("tickets")
        .join(incarnation_id.to_string()))
}
fn owner(binding: ScopeBinding) -> Authority {
    Authority::from_verified_host(
        binding,
        Principal::User {
            user_id: "offline-host-owner".into(),
        },
    )
}

pub async fn preview(data: &Path, id: &str) -> anyhow::Result<Value> {
    let source = read_source(data, id).await?;
    let tasks = source
        .session
        .task_list
        .as_ref()
        .map(|l| l.items.as_slice())
        .unwrap_or(&[]);
    Ok(
        json!({"source_session_id":id,"source_snapshot_hash":content_hash(&canonical_bytes(&source.session)?),
        "read_only":!source.cold,"reason":if source.cold{"inert canonical Root; explicit reviewed import eligible"}else{"active/previous execution requires owned-stop reconciliation; preview only"},
        "task_count":tasks.len(),"truncated":tasks.len()>32,"omitted_count":tasks.len().saturating_sub(32),
        "mapping":tasks.iter().take(32).map(|t|json!({"task_id":t.id,"title":t.description,"original_state":t.status,"import_state":"needs_review","parent_id":t.parent_id,"depends_on":t.depends_on})).collect::<Vec<_>>()}),
    )
}

pub async fn legacy_commit(
    data: &Path,
    id: &str,
    expected: &str,
    operation_id: &str,
    tasks: &[String],
    attach: bool,
) -> anyhow::Result<Value> {
    ensure!(
        (attach || !tasks.is_empty())
            && tasks.len() <= 32
            && tasks.iter().all(|t| t != "_scope_attach")
            && tasks.iter().collect::<BTreeSet<_>>().len() == tasks.len(),
        "select at most 32 distinct nonreserved Tasks"
    );
    let source = read_source(data, id).await?;
    let mut supervisor = if attach {
        source.session.clone()
    } else {
        read_source(data, DEFAULT_SUPERVISOR_SESSION_ID)
            .await?
            .session
    };
    if attach {
        ensure!(
            id == DEFAULT_SUPERVISOR_SESSION_ID,
            "explicit canonical Supervisor attach only"
        );
        supervisor.set_root_orchestration_only(true)?;
    }
    let scope = scope_root(data, &supervisor)?;
    // Replay the original receipt before checking current source/CAS. Turning
    // orchestration-only on intentionally changed the Supervisor snapshot.
    if scope.join("HEAD").exists() {
        let service = TicketService::open_offline(&scope, binding(&supervisor)?)?;
        let snapshot = service.published()?.1;
        if let Some(receipt) = snapshot.receipts.get(operation_id) {
            let command: Command = serde_json::from_str(&receipt.canonical_request)?;
            ensure!(
                receipt.principal == "user:offline-host-owner",
                "receipt subject"
            );
            let sources = command
                .operations
                .iter()
                .filter_map(|o| match o {
                    Operation::Import { source, .. } | Operation::AttachLegacy { source } => {
                        Some(source)
                    }
                    _ => None,
                })
                .collect::<Vec<_>>();
            ensure!(
                sources.len() == command.operations.len()
                    && !sources.is_empty()
                    && sources
                        .iter()
                        .all(|s| s.session_id == id && s.snapshot_hash == expected),
                "idempotency_conflict"
            );
            let selected = sources
                .iter()
                .filter(|s| s.task_id != "_scope_attach")
                .map(|s| s.task_id.clone())
                .collect::<BTreeSet<_>>();
            ensure!(
                selected == tasks.iter().cloned().collect()
                    && sources.iter().any(|s| s.task_id == "_scope_attach") == attach,
                "idempotency_conflict"
            );
            if attach {
                finish_attach(data, &supervisor, expected).await?;
            }
            return Ok(json!({"status":"imported_needs_review","receipt":receipt,"replayed":true}));
        }
    }
    ensure!(
        source.cold,
        "source is read-only: canonical execution ownership is not inert"
    );
    let bytes = canonical_bytes(&source.session)?;
    ensure!(
        content_hash(&bytes) == expected,
        "revision_conflict: source snapshot changed"
    );
    let mut operations = Vec::new();
    let binding = binding(&supervisor)?;
    let service = if scope.join("HEAD").exists() {
        let offline = TicketService::open_offline(&scope, binding.clone())?;
        drop(offline);
        TicketService::open(&scope, binding.clone())?
    } else {
        ensure!(attach, "scope missing; explicit attach first");
        TicketService::open(&scope, binding.clone())?
    };
    let authority = owner(binding.clone());
    let artifact = service.store_artifact(
        &Authority::from_verified_host(binding, Principal::Runtime),
        &bytes,
    )?;
    let source_ref = |task_id: String, original_state: String| ImportSource {
        session_id: id.into(),
        task_id,
        snapshot_hash: expected.into(),
        original_state,
        artifact: Some(artifact.clone()),
    };
    if attach {
        operations.push(Operation::AttachLegacy {
            source: source_ref("_scope_attach".into(), "explicit_scope_attach".into()),
        });
    }
    let items = source
        .session
        .task_list
        .as_ref()
        .map(|l| l.items.as_slice())
        .unwrap_or(&[]);
    for id in tasks {
        let task = items
            .iter()
            .find(|t| &t.id == id)
            .context("selected Task not found")?;
        operations.push(Operation::Import {
            temp_id: format!("legacy-{}", task.id),
            contract: Contract {
                title: task.description.chars().take(240).collect(),
                objective: task.description.clone(),
                constraints: vec![
                    "Imported immutable history only; no old execution authority transferred"
                        .into(),
                ],
                acceptance: if task.completion_criteria.is_empty() {
                    vec!["Explicit review and fresh verified evidence required".into()]
                } else {
                    task.completion_criteria.clone()
                },
                user_acceptance_required: true,
                allowed_tools: BTreeSet::from(["Task".into()]),
            },
            source: source_ref(
                task.id.clone(),
                serde_json::to_value(&task.status)?
                    .as_str()
                    .context("Task state")?
                    .into(),
            ),
        });
    }
    let command = service.prepare_command(&authority, operation_id, operations)?;
    let receipt = service.execute(&authority, &command)?;
    if attach {
        finish_attach(data, &supervisor, expected).await?;
    }
    Ok(
        json!({"status":"imported_needs_review","receipt":receipt,"restart_host_required":attach,"source_snapshot_hash":expected}),
    )
}

async fn finish_attach(data: &Path, attached: &Session, expected: &str) -> anyhow::Result<()> {
    let actual = SessionStoreV2::new(data.canonicalize()?).await?;
    let current = actual
        .load_root_authority(DEFAULT_SUPERVISOR_SESSION_ID)
        .await?
        .context("Supervisor disappeared")?;
    if !current.root_orchestration_only_enabled() {
        ensure!(
            content_hash(&canonical_bytes(&current)?) == expected,
            "source changed: attached HEAD remains unavailable pending reconciliation"
        );
        actual.save_session(attached).await?;
    }
    ensure!(
        ticket_runtime::verified_scope_binding(&actual, DEFAULT_SUPERVISOR_SESSION_ID)
            .await?
            .0
            == binding(attached)?,
        "canonical attach binding failed"
    );
    Ok(())
}

fn require_scope_stopped(source: &SourceSnapshot, snapshot: &Snapshot) -> anyhow::Result<()> {
    if let Some(file) = source.files.get("actor-authority.json") {
        let actor: ActorDirectoryEntry = serde_json::from_slice(&file.bytes)?;
        actor.validate()?;
        ensure!(
            actor.actor.matches_session(&source.session)
                && actor.actor.state == ActorLogicalState::Cold
                && actor
                    .activation
                    .as_ref()
                    .is_none_or(|a| !a.status.is_live()),
            "Supervisor activation is live/unknown"
        );
    }
    ensure!(
        source
            .session
            .agent_runtime_state
            .as_ref()
            .is_none_or(|r| matches!(
                r.status,
                AgentStatusState::Idle
                    | AgentStatusState::Completed
                    | AgentStatusState::Cancelled
                    | AgentStatusState::Failed
            )),
        "Supervisor has active/unknown execution"
    );
    ensure!(
        source
            .session
            .last_run_status()
            .as_deref()
            .is_none_or(|s| matches!(s, "completed" | "cancelled" | "failed")),
        "Supervisor has active/unknown run"
    );
    let known = snapshot
        .assignments
        .values()
        .map(|a| ticket_runtime::ticket_child_id(&a.dispatch_key))
        .collect::<BTreeSet<_>>();
    for name in source.files.keys().filter(|n| n.starts_with("children/")) {
        let child = name.split('/').nth(1).context("child path")?;
        ensure!(
            known.contains(child),
            "untracked legacy child ownership; read-only migration"
        );
    }
    ensure!(
        snapshot.assignments.values().all(|a| a.process_stopped
            && a.effects
                .values()
                .all(|e| !matches!(e.state, EffectState::Started | EffectState::OutcomeUnknown))),
        "executions/effects not confirmed stopped/reconciled"
    );
    Ok(())
}

pub async fn migration_plan(
    data: &Path,
    destination: &Path,
    operation_id: &str,
) -> anyhow::Result<Value> {
    let source = read_source(data, DEFAULT_SUPERVISOR_SESSION_ID).await?;
    let binding = binding(&source.session)?;
    let root = scope_root(data, &source.session)?;
    let service = TicketService::open_offline(&root, binding.clone())?;
    let (commit, snapshot) = service.published()?;
    require_scope_stopped(&source, &snapshot)?;
    let destination = destination.canonicalize()?;
    let dest = destination
        .join("tickets")
        .join(root.file_name().context("scope folder")?);
    ensure!(
        destination != data.canonicalize()? && !dest.exists(),
        "destination must be a new scope"
    );
    let request = MigrationRequest {
        operation_id: operation_id.into(),
        binding,
        expected_commit: commit,
        expected_seq: snapshot.seq,
        expected_epoch: snapshot.authority_epoch,
        source_root: root.to_string_lossy().into_owned(),
        destination_root: dest.to_string_lossy().into_owned(),
        supervisor_snapshot_hash: Some(content_hash(&source.file_bytes)),
    };
    Ok(
        json!({"request":request,"source_snapshot_bytes":source.file_bytes.len(),"original_authority_stopped":true,"executions_stopped":true,"destination_is_read_only_until_activation":true}),
    )
}

pub async fn migrate(data: &Path, request: &MigrationRequest) -> anyhow::Result<Value> {
    let source = read_source(data, DEFAULT_SUPERVISOR_SESSION_ID).await?;
    let binding = binding(&source.session)?;
    let root = scope_root(data, &source.session)?;
    let service = TicketService::open_offline(&root, binding.clone())?;
    let (_, snapshot) = service.published()?;
    require_scope_stopped(&source, &snapshot)?;
    ensure!(
        request.binding == binding && request.source_root == root.to_string_lossy(),
        "migration source binding"
    );
    let dest = Path::new(&request.destination_root);
    let dest_data = dest
        .parent()
        .and_then(Path::parent)
        .context("destination scope path")?;
    ensure!(
        dest == dest_data
            .join("tickets")
            .join(root.file_name().context("scope name")?),
        "destination scope layout"
    );
    ensure!(
        dest_data.canonicalize()? == dest_data && dest_data != data.canonicalize()?,
        "canonical distinct destination required"
    );
    real_directories(dest.parent().context("destination parent")?)?;
    let proof = StoppedScopeProof::from_verified_host(binding.clone())
        .with_verified_supervisor_snapshot(source.file_bytes.clone());
    let authority = owner(binding.clone());
    let receipt = service.retire_for_migration(&authority, request, &proof)?;
    let snapshot = service.published()?.1;
    let migration = snapshot.migration.as_ref().context("retirement missing")?;
    ensure!(migration.request == *request, "idempotency_conflict");
    if migration.stage == MigrationStage::SourceRetired {
        // A completed destination HEAD can survive an error consuming source;
        // export never overwrites it. Incomplete copies resume exact objects.
        let already_activated = if dest.join("HEAD").exists() {
            let copy = TicketService::open_offline(dest, binding.clone())?;
            copy.published()?.1.migration.as_ref().is_some_and(|r| {
                r.stage == MigrationStage::DestinationActivated && r.request == *request
            })
        } else {
            false
        };
        if !already_activated {
            service.export_retired_migration(&authority, request, &proof)?;
        }
    }
    let copy = TicketService::open_offline(dest, binding)?;
    let frozen = migration
        .supervisor_snapshot
        .as_ref()
        .context("migration needs full Supervisor snapshot")?;
    let files: SourceFiles = serde_json::from_slice(&service.read_artifact(
        &authority,
        frozen,
        store::MAX_ARTIFACT_BYTES,
    )?)?;
    if dest.join("BACKUP_READ_ONLY").exists() {
        restore(
            &dest_data
                .join("sessions")
                .join(DEFAULT_SUPERVISOR_SESSION_ID),
            &files,
        )?;
        let storage = SessionStoreV2::new(dest_data.to_path_buf()).await?;
        storage.rebuild_index_from_disk().await?;
        ensure!(
            ticket_runtime::verified_scope_binding(&storage, DEFAULT_SUPERVISOR_SESSION_ID)
                .await?
                .0
                == request.binding,
            "copied canonical Supervisor proof mismatch"
        );
    }
    let observed = copy.activate_migrated_copy(&service, &authority, request, &proof)?;
    Ok(
        json!({"status":"migrated","receipt":receipt,"migration":observed,"destination_epoch":copy.published()?.1.authority_epoch,"source_health":"read_only","restart_host_required":true}),
    )
}
