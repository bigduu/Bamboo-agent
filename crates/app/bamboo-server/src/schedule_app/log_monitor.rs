//! Supervisor-owned, read-only log checks on the existing Scheduler.

use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use bamboo_agent_core::tools::{Tool, ToolCtx, ToolError, ToolOutcome, ToolResult};
use bamboo_agent_core::workspace_state::WorkspaceResolver;
use bamboo_domain::{
    ProjectId, ProjectStatus, Session, SessionActivationPolicy, SessionCallerReference,
    SessionMessageBody, SessionMessageContent, SessionMessageEnvelope, SessionMessageId,
    SessionMessageKind, SessionMessageSource, SessionRuntimeInstruction, Storage,
    SupervisorReference, DEFAULT_SUPERVISOR_SESSION_ID,
};
use bamboo_projects::ProjectStore;
use cap_std::ambient_authority;
use cap_std::fs::{Dir, File, Metadata};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::{ScheduleContext, ScheduleEntry, ScheduleRunJob, ScheduleRunStatus, ScheduleStore};

pub(crate) const TOOL_NAME: &str = "supervisor_log_monitor";
const MAX_ACTIVE_MONITORS: usize = 8;
const MIN_INTERVAL_SECONDS: u64 = 15;
const MAX_INTERVAL_SECONDS: u64 = 86_400;
const SCAN_BYTES: usize = 64 * 1024;
const CHECKPOINT_BYTES: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct MonitorOwner {
    supervisor: SupervisorReference,
    created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct MonitorRequest {
    project_id: ProjectId,
    relative_path: String,
    every_seconds: u64,
    #[serde(default)]
    contains: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct MonitorSpec {
    pub(super) project_id: ProjectId,
    pub(super) relative_path: String,
    every_seconds: u64,
    contains: Option<String>,
    workspace: PathBuf,
    workspace_identity: FileIdentity,
}

impl MonitorSpec {
    fn matches_request(&self, request: &MonitorRequest) -> bool {
        self.project_id == request.project_id
            && self.relative_path == request.relative_path
            && self.every_seconds == request.every_seconds
            && self.contains == request.contains
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct FileIdentity {
    device: u64,
    inode: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct LogCursor {
    file: FileIdentity,
    offset: u64,
    skip_partial_line: bool,
    checkpoint: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct LogAlert {
    scan_run_id: String,
    matched_lines: usize,
    observed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct LogMonitor {
    owner: MonitorOwner,
    pub(super) spec: MonitorSpec,
    pub(super) active: bool,
    generation: Uuid,
    session_created_at: DateTime<Utc>,
    cursor: LogCursor,
    pending_alert: Option<LogAlert>,
}

fn denied(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::PermissionDenied, message)
}

fn monitor_request_valid(request: &MonitorRequest) -> bool {
    let path = Path::new(&request.relative_path);
    (MIN_INTERVAL_SECONDS..=MAX_INTERVAL_SECONDS).contains(&request.every_seconds)
        && (1..=512).contains(&request.relative_path.len())
        && !request.relative_path.chars().any(char::is_control)
        && path
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
        && request.contains.as_ref().is_none_or(|pattern| {
            (1..=80).contains(&pattern.len())
                && !pattern.trim().is_empty()
                && !pattern.chars().any(char::is_control)
        })
}

#[cfg(any(unix, windows))]
fn file_metadata(metadata: &Metadata) -> io::Result<(FileIdentity, u64)> {
    #[cfg(windows)]
    use cap_fs_ext::MetadataExt;
    #[cfg(unix)]
    use cap_std::fs::MetadataExt;
    Ok((
        FileIdentity {
            device: metadata.dev(),
            inode: metadata.ino(),
        },
        metadata.nlink(),
    ))
}

#[cfg(not(any(unix, windows)))]
fn file_metadata(_: &Metadata) -> io::Result<(FileIdentity, u64)> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "Log monitor file identity is unavailable",
    ))
}

#[cfg(unix)]
fn open_child_directory(parent: &Dir, name: &Path) -> io::Result<Dir> {
    use rustix::fs::{openat, Mode, OFlags};
    use std::os::fd::AsFd;
    let fd = openat(
        parent.as_fd(),
        name,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )?;
    Ok(Dir::from_std_file(fd.into()))
}

#[cfg(windows)]
fn open_child_directory(parent: &Dir, name: &Path) -> io::Result<Dir> {
    use cap_fs_ext::DirExt;
    parent.open_dir_nofollow(name)
}

#[cfg(not(any(unix, windows)))]
fn open_child_directory(_: &Dir, _: &Path) -> io::Result<Dir> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "No-follow log reads are unavailable",
    ))
}

fn open_workspace(path: &Path) -> io::Result<Dir> {
    let mut parts = path.components().peekable();
    let mut anchor = PathBuf::new();
    if let Some(Component::Prefix(prefix)) = parts.peek() {
        anchor.push(prefix.as_os_str());
        parts.next();
    }
    if parts.next() != Some(Component::RootDir) {
        return Err(denied("Log monitor requires an absolute Project workspace"));
    }
    anchor.push(std::path::MAIN_SEPARATOR_STR);
    let mut dir = Dir::open_ambient_dir(&anchor, ambient_authority())?;
    for part in parts {
        let Component::Normal(part) = part else {
            return Err(denied("Invalid Project workspace component"));
        };
        dir = open_child_directory(&dir, Path::new(part))?;
    }
    Ok(dir)
}

fn open_log(workspace: Dir, relative_path: &str) -> io::Result<(File, FileIdentity)> {
    let mut parent = workspace;
    let mut parts = Path::new(relative_path).components().peekable();
    while let Some(Component::Normal(part)) = parts.next() {
        let name = Path::new(part);
        if parts.peek().is_some() {
            parent = open_child_directory(&parent, name)?;
            continue;
        }
        let named = parent.symlink_metadata(name)?;
        let (identity, links) = file_metadata(&named)?;
        if !named.is_file() || links != 1 {
            return Err(denied("Log monitor requires a regular file with one link"));
        }
        let file = open_regular_log(&parent, name)?;
        let opened = file.metadata()?;
        if !opened.is_file() || file_metadata(&opened)? != (identity, 1) {
            return Err(denied("Log file changed while opening"));
        }
        return Ok((file, identity));
    }
    Err(denied(
        "Log monitor path must be relative to the registered Project",
    ))
}

fn open_regular_log(parent: &Dir, name: &Path) -> io::Result<File> {
    #[cfg(unix)]
    {
        use rustix::fs::{openat, Mode, OFlags};
        use std::os::fd::AsFd;
        let fd = openat(
            parent.as_fd(),
            name,
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
            Mode::empty(),
        )?;
        Ok(File::from_std(fd.into()))
    }
    #[cfg(windows)]
    {
        use cap_fs_ext::{FollowSymlinks, OpenOptionsFollowExt};
        parent.open_with(
            name,
            cap_std::fs::OpenOptions::new()
                .read(true)
                .follow(FollowSymlinks::No),
        )
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = (parent, name);
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "No-follow log reads are unavailable",
        ))
    }
}

fn checkpoint(file: &mut File, offset: u64) -> io::Result<([u8; 32], Option<u8>)> {
    let len = offset.min(CHECKPOINT_BYTES as u64) as usize;
    let mut bytes = [0; CHECKPOINT_BYTES];
    file.seek(SeekFrom::Start(offset - len as u64))?;
    file.read_exact(&mut bytes[..len])?;
    Ok((
        Sha256::digest(&bytes[..len]).into(),
        bytes[..len].last().copied(),
    ))
}

fn initial_cursor(path: &Path, relative_path: &str) -> io::Result<(FileIdentity, LogCursor)> {
    let workspace = open_workspace(path)?;
    let (workspace_identity, _) = file_metadata(&workspace.dir_metadata()?)?;
    let (mut file, identity) = open_log(workspace, relative_path)?;
    let offset = file.metadata()?.len();
    let (checkpoint, last) = checkpoint(&mut file, offset)?;
    Ok((
        workspace_identity,
        LogCursor {
            file: identity,
            offset,
            skip_partial_line: last.is_some_and(|byte| byte != b'\n'),
            checkpoint,
        },
    ))
}

fn matches_anomaly(line: &[u8], pattern: Option<&str>) -> bool {
    if let Some(pattern) = pattern {
        return !pattern.is_empty()
            && line
                .windows(pattern.len())
                .any(|window| window.eq_ignore_ascii_case(pattern.as_bytes()));
    }
    line.split(|byte| !byte.is_ascii_alphanumeric() && *byte != b'_')
        .any(|word| {
            [b"ERROR".as_slice(), b"FATAL", b"PANIC"]
                .iter()
                .any(|level| word.eq_ignore_ascii_case(level))
        })
}

fn scan_new_lines(spec: &MonitorSpec, cursor: &LogCursor) -> io::Result<(LogCursor, usize)> {
    let workspace = open_workspace(&spec.workspace)?;
    if file_metadata(&workspace.dir_metadata()?)?.0 != spec.workspace_identity {
        return Err(denied("Log monitor Project workspace was replaced"));
    }
    let (mut file, identity) = open_log(workspace, &spec.relative_path)?;
    let same_contents = cursor.file == identity
        && cursor.offset <= file.metadata()?.len()
        && checkpoint(&mut file, cursor.offset)?.0 == cursor.checkpoint;
    let start = if same_contents { cursor.offset } else { 0 };
    file.seek(SeekFrom::Start(start))?;
    let mut bytes = Vec::with_capacity(SCAN_BYTES);
    Read::by_ref(&mut file)
        .take(SCAN_BYTES as u64)
        .read_to_end(&mut bytes)?;
    let skip_partial_line = same_contents && cursor.skip_partial_line;
    let prefix = if skip_partial_line {
        bytes
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(bytes.len(), |at| at + 1)
    } else {
        0
    };
    let tail = &bytes[prefix..];
    let complete = tail
        .iter()
        .rposition(|byte| *byte == b'\n')
        .map_or(0, |at| at + 1);
    let matched = tail[..complete]
        .split_inclusive(|byte| *byte == b'\n')
        .filter(|line| matches_anomaly(line, spec.contains.as_deref()))
        .count();
    let overlong = prefix == 0 && complete == 0 && bytes.len() == SCAN_BYTES;
    let consumed = if overlong {
        bytes.len()
    } else {
        prefix + complete
    };
    let offset = start
        .checked_add(consumed as u64)
        .ok_or_else(|| io::Error::other("Log offset overflow"))?;
    let (checkpoint, last) = checkpoint(&mut file, offset)?;
    Ok((
        LogCursor {
            file: identity,
            offset,
            skip_partial_line: overlong || (skip_partial_line && last != Some(b'\n')),
            checkpoint,
        },
        matched,
    ))
}

fn workspace_path(
    projects: &ProjectStore,
    resolver: &WorkspaceResolver,
    project_id: &ProjectId,
) -> io::Result<PathBuf> {
    if projects.get(project_id).map_err(io::Error::other)?.status != ProjectStatus::Active {
        return Err(denied("Log monitor Project is archived"));
    }
    crate::project_context::validate_workspace_assignment_with_resolver(
        projects,
        Some(project_id),
        None,
        resolver,
    )
    .map_err(io::Error::other)?
    .ok_or_else(|| denied("The active Project has no registered workspace"))
}

async fn current_owner(storage: &dyn Storage, owner: &MonitorOwner) -> io::Result<()> {
    let root = storage
        .load_root_authority(&owner.supervisor.session_id)
        .await?
        .filter(|session| {
            session.created_at == owner.created_at
                && session.supervisor_incarnation_id() == Some(owner.supervisor.incarnation_id)
        })
        .ok_or_else(|| denied("Log monitor Supervisor lifetime changed"))?;
    if storage
        .find_host_session_scope(&SessionCallerReference::from(&root))
        .await?
        .is_some()
    {
        return Err(denied(
            "Finite Host scopes require an explicit log monitoring grant",
        ));
    }
    Ok(())
}

pub(super) async fn run_log_monitor(
    ctx: &ScheduleContext,
    job: &ScheduleRunJob,
) -> io::Result<Option<ScheduleRunStatus>> {
    let Some(entry) = ctx.schedule_store.log_monitor(&job.schedule_id).await else {
        return Ok(None);
    };
    let mut monitor = entry.log_monitor.expect("selected log monitor");
    if !monitor.active {
        return Ok(Some(ScheduleRunStatus::Skipped));
    }
    let storage = ctx.agent.storage().as_ref();
    current_owner(storage, &monitor.owner).await?;
    if workspace_path(
        &ctx.project_store,
        &ctx.workspace_resolver,
        &monitor.spec.project_id,
    )? != monitor.spec.workspace
    {
        return Err(denied("Log monitor Project workspace binding changed"));
    }
    let monitor_session = storage.load_root_authority(&job.schedule_id).await?;
    if !monitor_session.is_some_and(|session| {
        session.created_at == monitor.session_created_at
            && session.project_id_meta().as_deref() == Some(monitor.spec.project_id.as_str())
    }) {
        return Err(denied("Log monitor Session lifetime changed"));
    }
    ctx.schedule_store
        .bind_run_session(&job.schedule_id, &job.run_id, &job.schedule_id)
        .await?;
    let scan = if monitor.pending_alert.is_none() {
        let spec = monitor.spec.clone();
        let cursor = monitor.cursor.clone();
        Some(
            tokio::task::spawn_blocking(move || scan_new_lines(&spec, &cursor))
                .await
                .map_err(io::Error::other)?,
        )
    } else {
        None
    };
    // Do not hold the mutation gate during file I/O. Once stopped, an in-flight
    // read may return but cannot change the cursor or issue a new alert, even
    // after a quick stop and resume.
    let _gate = ctx.schedule_store.log_monitor_gate.lock().await;
    if !ctx
        .schedule_store
        .log_monitor(&job.schedule_id)
        .await
        .and_then(|entry| entry.log_monitor)
        .is_some_and(|current| current.active && current.generation == monitor.generation)
    {
        return Ok(Some(ScheduleRunStatus::Skipped));
    }
    current_owner(storage, &monitor.owner).await?;
    if workspace_path(
        &ctx.project_store,
        &ctx.workspace_resolver,
        &monitor.spec.project_id,
    )? != monitor.spec.workspace
    {
        return Err(denied("Log monitor Project workspace binding changed"));
    }
    if let Some(scan) = scan {
        let (cursor, matched_lines) = scan?;
        monitor.cursor = cursor;
        monitor.pending_alert = (matched_lines != 0).then(|| LogAlert {
            scan_run_id: job.run_id.clone(),
            matched_lines,
            observed_at: Utc::now(),
        });
        ctx.schedule_store
            .save_log_monitor(&job.schedule_id, monitor.clone())
            .await?;
    }
    if let Some(alert) = monitor.pending_alert.as_ref() {
        deliver_alert(ctx, &job.schedule_id, &monitor, alert).await?;
        monitor.pending_alert = None;
        ctx.schedule_store
            .save_log_monitor(&job.schedule_id, monitor)
            .await?;
    }
    Ok(Some(ScheduleRunStatus::Success))
}

async fn deliver_alert(
    ctx: &ScheduleContext,
    monitor_id: &str,
    monitor: &LogMonitor,
    alert: &LogAlert,
) -> io::Result<()> {
    let inbox = ctx
        .agent
        .session_inbox()
        .filter(|inbox| inbox.supports_runtime_target_lifetime())
        .ok_or_else(|| denied("Log monitor requires a lifetime-bound Session Inbox"))?;
    let activation = ctx
        .agent
        .activation_router()
        .ok_or_else(|| denied("Log monitor requires the native activation router"))?;
    let target = &monitor.owner.supervisor.session_id;
    let data = json!({
        "monitor_session_id":monitor_id, "scan_run_id": alert.scan_run_id,
        "project_id":monitor.spec.project_id, "relative_path":monitor.spec.relative_path,
        "matched_lines":alert.matched_lines,
    });
    let envelope = SessionMessageEnvelope {
        id: SessionMessageId::stable("supervisor-log-monitor:v1", &json!([monitor.owner, data])),
        source: SessionMessageSource::Runtime { subsystem: TOOL_NAME.to_owned() },
        target_session_id: target.clone(),
        kind: SessionMessageKind::RuntimeInstruction,
        body: SessionMessageBody::RuntimeInstruction(SessionRuntimeInstruction {
            instruction: "log_monitor_alert_v1".into(),
            content: Some(SessionMessageContent::text(format!(
                "A scheduled log check found {} matching new lines in {} under Project {}. Monitor Session {} recorded the check.",
                alert.matched_lines, json!(monitor.spec.relative_path), monitor.spec.project_id, monitor_id,
            ))),
            data: Some(data),
            provider_message: None,
        }),
        created_at: alert.observed_at,
        thread_id: None,
        in_reply_to: None,
        attempt: None,
        correlation_id: None,
    };
    let receipt = inbox
        .deliver_runtime_for_target(&envelope, monitor.owner.created_at)
        .await
        .map_err(io::Error::other)?;
    inbox
        .mark_message_activation_eligible(
            target,
            &receipt,
            SessionActivationPolicy::RespectSpecificWait,
        )
        .await
        .map_err(io::Error::other)?;
    activation
        .reconcile_wake(target, Utc::now())
        .await
        .map_err(io::Error::other)?;
    Ok(())
}

#[derive(Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
enum Args {
    Start {
        operation_id: Uuid,
        project_id: ProjectId,
        relative_path: String,
        every_seconds: u64,
        #[serde(default)]
        contains: Option<String>,
    },
    Stop {
        monitor_id: String,
    },
    Resume {
        monitor_id: String,
    },
    List,
}

pub(crate) struct SupervisorLogMonitorTool {
    pub(crate) store: Arc<ScheduleStore>,
    pub(crate) storage: Arc<dyn Storage>,
    pub(crate) projects: Arc<ProjectStore>,
    pub(crate) workspace: WorkspaceResolver,
}

impl SupervisorLogMonitorTool {
    async fn start(
        &self,
        supervisor: &Session,
        owner: MonitorOwner,
        operation_id: Uuid,
        request: MonitorRequest,
    ) -> io::Result<ScheduleEntry> {
        if operation_id.is_nil() || !monitor_request_valid(&request) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Invalid log monitor operation, interval, path or match text",
            ));
        }
        let id = format!("log-monitor-{operation_id}");
        if let Some(existing) = self.store.log_monitor(&id).await {
            let monitor = existing.log_monitor.as_ref().expect("selected log monitor");
            return if monitor.owner == owner && monitor.spec.matches_request(&request) {
                Ok(existing)
            } else {
                Err(denied(
                    "Log monitor operation ID is bound to a different request",
                ))
            };
        }
        let registrations = self.store.log_monitors().await;
        if registrations
            .iter()
            .filter(|entry| {
                entry
                    .log_monitor
                    .as_ref()
                    .is_some_and(|monitor| monitor.active && monitor.owner == owner)
            })
            .count()
            >= MAX_ACTIVE_MONITORS
        {
            return Err(denied("Log monitor capacity is exhausted"));
        }
        let workspace = workspace_path(&self.projects, &self.workspace, &request.project_id)?;
        let (workspace_identity, cursor) = tokio::task::spawn_blocking({
            let workspace = workspace.clone();
            let log = request.relative_path.clone();
            move || initial_cursor(&workspace, &log)
        })
        .await
        .map_err(io::Error::other)??;
        current_owner(self.storage.as_ref(), &owner).await?;
        if workspace_path(&self.projects, &self.workspace, &request.project_id)? != workspace {
            return Err(denied("Log monitor Project workspace binding changed"));
        }
        let session_created_at = self
            .ensure_session(&id, supervisor, &owner, &request, &workspace)
            .await?;
        let every_seconds = request.every_seconds;
        self.store
            .insert_log_monitor(
                id,
                LogMonitor {
                    owner,
                    spec: MonitorSpec {
                        project_id: request.project_id,
                        relative_path: request.relative_path,
                        contains: request.contains,
                        every_seconds,
                        workspace,
                        workspace_identity,
                    },
                    active: true,
                    generation: Uuid::new_v4(),
                    cursor,
                    session_created_at,
                    pending_alert: None,
                },
                every_seconds,
            )
            .await
    }

    async fn ensure_session(
        &self,
        id: &str,
        supervisor: &Session,
        owner: &MonitorOwner,
        request: &MonitorRequest,
        workspace: &Path,
    ) -> io::Result<DateTime<Utc>> {
        if let Some(existing) = self.storage.load_root_authority(id).await? {
            if existing
                .metadata
                .get("log_monitor_supervisor")
                .is_some_and(|value| value == &owner.supervisor.incarnation_id.to_string())
                && existing.project_id_meta().as_deref() == Some(request.project_id.as_str())
            {
                return Ok(existing.created_at);
            }
            return Err(denied(
                "Log monitor Session ID already belongs to another lifetime",
            ));
        }
        let mut session = Session::new(id, &supervisor.model);
        session.title = format!("Log monitor for {}", request.relative_path);
        session.model_ref = supervisor.model_ref.clone();
        session.set_project_id_meta(request.project_id.to_string());
        let workspace = bamboo_config::paths::path_to_display_string(workspace);
        session.workspace = Some(workspace.clone());
        session.set_workspace_path_meta(workspace);
        session
            .metadata
            .insert("created_by_schedule_id".into(), id.to_owned());
        session.metadata.insert(
            "log_monitor_supervisor".into(),
            owner.supervisor.incarnation_id.to_string(),
        );
        self.storage.save_session(&session).await?;
        Ok(session.created_at)
    }

    async fn set_active(
        &self,
        id: &str,
        owner: &MonitorOwner,
        active: bool,
    ) -> io::Result<ScheduleEntry> {
        let entry = self
            .store
            .log_monitor(id)
            .await
            .filter(|entry| {
                entry
                    .log_monitor
                    .as_ref()
                    .is_some_and(|monitor| &monitor.owner == owner)
            })
            .ok_or_else(|| {
                denied("Log monitor belongs to another Supervisor lifetime or is unavailable")
            })?;
        let mut monitor = entry.log_monitor.clone().expect("selected log monitor");
        if monitor.active == active {
            return Ok(entry);
        }
        if active {
            if workspace_path(&self.projects, &self.workspace, &monitor.spec.project_id)?
                != monitor.spec.workspace
            {
                return Err(denied("Log monitor Project workspace binding changed"));
            }
            let active_count = self
                .store
                .log_monitors()
                .await
                .iter()
                .filter(|entry| {
                    entry
                        .log_monitor
                        .as_ref()
                        .is_some_and(|monitor| monitor.active && &monitor.owner == owner)
                })
                .count();
            if active_count >= MAX_ACTIVE_MONITORS {
                return Err(denied("Active log monitor capacity is exhausted"));
            }
        }
        monitor.active = active;
        monitor.generation = Uuid::new_v4();
        self.store.save_log_monitor(id, monitor).await
    }
}

fn monitor_view(entry: ScheduleEntry) -> Value {
    let monitor = entry.log_monitor.expect("selected log monitor");
    json!({
        "monitor_id":entry.id, "session_id":entry.id,
        "active":monitor.active, "project_id":monitor.spec.project_id,
        "relative_path":monitor.spec.relative_path, "every_seconds":monitor.spec.every_seconds,
        "contains":monitor.spec.contains,
        "next_check_at":entry.state.next_fire_at.filter(|_| monitor.active),
        "last_check_at":entry.state.last_finished_at,
        "consecutive_failures":entry.state.consecutive_failures,
        "alert_pending":monitor.pending_alert.is_some(),
    })
}

#[async_trait]
impl Tool for SupervisorLogMonitorTool {
    fn name(&self) -> &str {
        TOOL_NAME
    }

    fn description(&self) -> &str {
        "Start, list, stop or resume a read-only scheduled log monitor in a registered Project. The monitor starts at the current end of the file and sends anomalies to this Supervisor."
    }

    fn parameters_schema(&self) -> Value {
        json!({
            "type":"object", "additionalProperties":false, "required":["action"],
            "properties":{
                "action":{"type":"string","enum":["start","list","stop","resume"]},
                "operation_id":{"type":"string","format":"uuid","description":"Stable operation ID required for start. Reuse for the same request."},
                "project_id":{"type":"string","description":"Registered active Project ID for start."},
                "relative_path":{"type":"string","description":"Log file path relative to the registered workspace."},
                "every_seconds":{"type":"integer","minimum":MIN_INTERVAL_SECONDS,"maximum":MAX_INTERVAL_SECONDS},
                "contains":{"type":"string","maxLength":80,"description":"Optional literal matched without ASCII case. The default detects ERROR, FATAL and PANIC."},
                "monitor_id":{"type":"string","description":"Existing monitor ID for stop or resume."},
            }
        })
    }

    async fn invoke(&self, args: Value, ctx: ToolCtx) -> Result<ToolOutcome, ToolError> {
        let args: Args = serde_json::from_value(args)
            .map_err(|error| ToolError::InvalidArguments(error.to_string()))?;
        if ctx.plan_read_only && !matches!(args, Args::List) {
            return Err(ToolError::Execution(
                "Log monitor changes require execution mode".into(),
            ));
        }
        let caller = ctx
            .executing_caller_for(DEFAULT_SUPERVISOR_SESSION_ID)
            .ok_or_else(|| {
                ToolError::Execution(
                    "Log monitor requires the original executing Supervisor".into(),
                )
            })?;
        let reference = ctx
            .executing_supervisor_for(DEFAULT_SUPERVISOR_SESSION_ID)
            .map(|observation| observation.supervisor_reference())
            .ok_or_else(|| {
                ToolError::Execution("Log monitor requires Supervisor authority".into())
            })?;
        let _gate = self.store.log_monitor_gate.lock().await;
        let result: io::Result<Value> = async {
            let session = self.storage.load_execution_admission_snapshot(DEFAULT_SUPERVISOR_SESSION_ID)
                .await?.filter(|session| caller.matches_session(session)
                    && session.supervisor_incarnation_id() == Some(reference.incarnation_id))
                .ok_or_else(|| denied("Log monitor caller changed"))?;
            let owner = MonitorOwner { supervisor: reference, created_at: session.created_at };
            current_owner(self.storage.as_ref(), &owner).await?;
            match args {
                Args::List => {
                    let mut entries = self.store.log_monitors().await;
                    entries.retain(|entry| entry.log_monitor.as_ref().is_some_and(|monitor| monitor.owner == owner));
                    entries.sort_by(|left, right| left.id.cmp(&right.id));
                    Ok(json!({"monitors":entries.into_iter().map(monitor_view).collect::<Vec<_>>() }))
                }
                Args::Start { operation_id, project_id, relative_path, every_seconds, contains } => {
                    self.start(&session, owner, operation_id, MonitorRequest { project_id, relative_path, every_seconds, contains })
                        .await.map(monitor_view)
                }
                Args::Stop { monitor_id } => self.set_active(&monitor_id, &owner, false).await.map(monitor_view),
                Args::Resume { monitor_id } => self.set_active(&monitor_id, &owner, true).await.map(monitor_view),
            }
        }.await;
        let view = result.map_err(|error| ToolError::Execution(error.to_string()))?;
        Ok(ToolOutcome::Completed(ToolResult::text(
            true,
            view.to_string(),
        )))
    }
}

#[cfg(all(test, any(unix, windows)))]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn log_monitor_reads_complete_new_lines_and_follows_file_rotation() {
        let temp = tempfile::tempdir().unwrap();
        let workspace = temp.path().canonicalize().unwrap();
        let path = workspace.join("service.log");
        std::fs::write(&path, b"ERROR old line\n").unwrap();
        let (workspace_identity, mut cursor) = initial_cursor(&workspace, "service.log").unwrap();
        let spec = MonitorSpec {
            project_id: ProjectId::parse("test-project").unwrap(),
            relative_path: "service.log".into(),
            contains: None,
            every_seconds: 15,
            workspace,
            workspace_identity,
        };
        let mut log = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        log.write_all(b"ER").unwrap();
        assert_eq!(scan_new_lines(&spec, &cursor).unwrap(), (cursor.clone(), 0));
        log.write_all(b"ROR new line\nINFO healthy\n").unwrap();
        let matched;
        (cursor, matched) = scan_new_lines(&spec, &cursor).unwrap();
        assert_eq!(matched, 1);
        assert_eq!(scan_new_lines(&spec, &cursor).unwrap(), (cursor.clone(), 0));
        drop(log);
        std::fs::rename(&path, path.with_extension("prev")).unwrap();
        std::fs::write(&path, b"FATAL rotated file\n").unwrap();
        let (rotated, matched) = scan_new_lines(&spec, &cursor).unwrap();
        assert_eq!(matched, 1);
        assert_ne!(rotated.file, cursor.file);
        assert_eq!(
            scan_new_lines(&spec, &rotated).unwrap(),
            (rotated.clone(), 0)
        );
        std::fs::write(
            &path,
            b"ERROR recreated bytes at the same inode with a longer line\n",
        )
        .unwrap();
        assert_eq!(scan_new_lines(&spec, &rotated).unwrap().1, 1);
    }

    #[tokio::test]
    async fn log_monitor_restart_recovers_cursor_and_pending_alert_from_interrupted_check() {
        let temp = tempfile::tempdir().unwrap();
        let workspace = temp.path().canonicalize().unwrap();
        std::fs::write(workspace.join("service.log"), "INFO already read\n").unwrap();
        let (workspace_identity, cursor) = initial_cursor(&workspace, "service.log").unwrap();
        let now = Utc::now();
        let id = "log-monitor-restart";
        let store = ScheduleStore::new(workspace.clone()).await.unwrap();
        let mut monitor = LogMonitor {
            owner: MonitorOwner {
                supervisor: SupervisorReference {
                    session_id: DEFAULT_SUPERVISOR_SESSION_ID.into(),
                    incarnation_id: Uuid::new_v4(),
                },
                created_at: now,
            },
            spec: MonitorSpec {
                project_id: ProjectId::parse("test-project").unwrap(),
                relative_path: "service.log".into(),
                contains: None,
                every_seconds: 15,
                workspace: workspace.clone(),
                workspace_identity,
            },
            active: true,
            generation: Uuid::new_v4(),
            session_created_at: now,
            cursor,
            pending_alert: None,
        };
        let entry = store
            .insert_log_monitor(id.into(), monitor.clone(), 15)
            .await
            .unwrap();
        let first = store
            .claim_due_runs(entry.state.next_fire_at.unwrap())
            .await
            .unwrap()
            .pop()
            .unwrap();
        store.mark_run_started(id, &first.run_id).await.unwrap();
        let mut writer = std::fs::OpenOptions::new()
            .append(true)
            .open(workspace.join("service.log"))
            .unwrap();
        writer.write_all(b"ERROR before restart\n").unwrap();
        let (cursor, matched_lines) = scan_new_lines(&monitor.spec, &monitor.cursor).unwrap();
        assert_eq!(matched_lines, 1);
        monitor.cursor = cursor;
        monitor.pending_alert = Some(LogAlert {
            scan_run_id: first.run_id.clone(),
            matched_lines,
            observed_at: now,
        });
        store.save_log_monitor(id, monitor.clone()).await.unwrap();
        drop(store);

        let store = ScheduleStore::new(workspace).await.unwrap();
        let entry = store.log_monitor(id).await.unwrap();
        assert_eq!(entry.log_monitor, Some(monitor));
        assert_eq!(
            store.get_run_record(&first.run_id).await.unwrap().status,
            ScheduleRunStatus::Cancelled
        );
        let next = store
            .claim_due_runs(entry.state.next_fire_at.unwrap())
            .await
            .unwrap();
        assert_eq!(next.len(), 1);
        assert_ne!(next[0].run_id, first.run_id);
    }

    #[cfg(unix)]
    #[test]
    fn log_monitor_rejects_symbolic_links_and_traversal() {
        let temp = tempfile::tempdir().unwrap();
        let workspace = temp.path().canonicalize().unwrap();
        std::fs::write(workspace.join("actual.log"), "error").unwrap();
        std::os::unix::fs::symlink(workspace.join("actual.log"), workspace.join("alias.log"))
            .unwrap();
        assert!(initial_cursor(&workspace, "alias.log").is_err());
        assert!(initial_cursor(&workspace, "../actual.log").is_err());
        std::fs::hard_link(workspace.join("actual.log"), workspace.join("hard.log")).unwrap();
        assert!(initial_cursor(&workspace, "hard.log").is_err());
    }
}
