//! Project-local scheduling, durable Inbox delivery and optional scan summaries.

use super::*;
use actix_web::{web, HttpRequest, HttpResponse};
use bamboo_domain::{
    ProjectId, ProjectManifest, ProviderModelRef, Role, Session, SessionActivationPolicy,
    SessionMessageBody, SessionMessageContent, SessionMessageEnvelope, SessionMessageId,
    SessionMessageKind, SessionMessageSource, SessionRuntimeInstruction, TokenUsage,
};
use bamboo_llm::work_text::{self, WorkTextClient, WorkTextReply};
use bamboo_orchestration::project_scan as policy;
use chrono::{DateTime, Duration, Utc};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::Write,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const SUBSYSTEM: &str = "project_scan";
const INSTRUCTION: &str = "project_scan_candidate_v1";
const INTERVAL: Duration = Duration::seconds(60);
const START_WINDOW: Duration = Duration::minutes(5);
const CALL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(40);

#[derive(Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum Mode {
    #[default]
    Off,
    Enabled,
    Paused,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Action {
    Enable,
    Pause,
    Resume,
    Disable,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Control {
    action: Action,
}

#[derive(Clone, Copy, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum Outcome {
    #[default]
    Idle,
    Waiting,
    Capacity,
    ModelUnavailable,
    InputTooLarge,
    Scanning,
    Declined,
    Unconfirmed,
    Pending,
    Notified,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Permit {
    project_id: ProjectId,
    work_id: String,
    record_revision: u64,
    epoch: Uuid,
    call_id: Uuid,
}

#[derive(Clone, Deserialize, Serialize)]
struct Notice {
    permit: Permit,
    manager_id: String,
    manager_created_at: DateTime<Utc>,
    created_at: DateTime<Utc>,
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
enum Handoff {
    Pending {
        notice: Notice,
    },
    Notified {
        notice: Notice,
        expires_at: DateTime<Utc>,
    },
}

impl Handoff {
    fn notice(&self) -> &Notice {
        match self {
            Self::Pending { notice } | Self::Notified { notice, .. } => notice,
        }
    }
}

#[derive(Deserialize, Serialize)]
struct Check {
    work_id: String,
    title: String,
    blocker: Option<policy::Blocker>,
    summary: Option<String>,
}

impl Check {
    fn new(work: &Ticket, blocker: Option<policy::Blocker>) -> Self {
        Self {
            work_id: work.id.clone(),
            title: work.contract.title.chars().take(120).collect(),
            blocker,
            summary: None,
        }
    }
}

#[derive(Default, Deserialize, Serialize)]
struct Usage {
    model_requests: u64,
    unknown_requests: u64,
    tokens: TokenUsage,
}

#[derive(Deserialize, Serialize)]
struct Claim {
    call_id: Uuid,
    expires_at: DateTime<Utc>,
}

#[derive(Default, Deserialize, Serialize)]
struct Entry {
    mode: Mode,
    epoch: Uuid,
    next_at: Option<DateTime<Utc>>,
    call: Option<Claim>,
    // The previous scanner cached model rejections here. Older readers still
    // require the field, but those results cannot suppress a Ready Work.
    #[serde(default, skip_deserializing, rename = "checked")]
    legacy_checked: BTreeMap<String, u64>,
    handoff: Option<Handoff>,
    last_check: Option<Check>,
    outcome: Outcome,
    usage: Usage,
}

impl Entry {
    fn controls(&mut self, action: Action) -> Result<()> {
        let mode = match (self.mode, action) {
            (_, Action::Enable) | (Mode::Enabled | Mode::Paused, Action::Resume) => Mode::Enabled,
            (Mode::Enabled | Mode::Paused, Action::Pause) => Mode::Paused,
            (_, Action::Disable) => Mode::Off,
            _ => {
                return Err(Error::InvalidTransition(
                    "Project scan is not in that state".into(),
                ))
            }
        };
        if self.mode != mode {
            self.mode = mode;
            self.epoch = Uuid::new_v4();
            self.next_at = (mode == Mode::Enabled).then(Utc::now);
            self.handoff = None;
            self.last_check = None;
            self.outcome = Outcome::Idle;
        }
        Ok(())
    }

    fn same_epoch(&self, permit: &Permit) -> bool {
        self.mode == Mode::Enabled && self.epoch == permit.epoch
    }

    fn allows_start(&self, permit: &Permit, now: DateTime<Utc>) -> bool {
        self.same_epoch(permit)
            && matches!(&self.handoff, Some(Handoff::Notified { notice, expires_at })
                if &notice.permit == permit && *expires_at > now)
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Saved {
    version: u32,
    projects: BTreeMap<ProjectId, Entry>,
}

pub(super) struct ScanFile {
    target: PathBuf,
    _lock: File,
    saved: Saved,
}

impl ScanFile {
    fn path(root: &Path) -> PathBuf {
        root.join("orchestration").join("project-scan-v1.json")
    }

    fn open(root: &Path) -> Result<Self> {
        let target = Self::path(root);
        let path = target.parent().expect("Project scan directory");
        fs::create_dir_all(path)?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(path.join("project-scan-v1.lock"))?;
        FileExt::try_lock_exclusive(&lock).map_err(|error| {
            if error.kind() == io::ErrorKind::WouldBlock {
                Error::RevisionConflict
            } else {
                error.into()
            }
        })?;
        let saved = match fs::read(&target) {
            Ok(bytes) => serde_json::from_slice::<Saved>(&bytes)?,
            Err(error) if error.kind() == io::ErrorKind::NotFound => Saved {
                version: 1,
                projects: BTreeMap::new(),
            },
            Err(error) => return Err(error.into()),
        };
        if saved.version != 1 {
            return Err(Error::AuthorityUnavailable(
                "Unknown Project scan state version".into(),
            ));
        }
        Ok(Self {
            target,
            _lock: lock,
            saved,
        })
    }

    fn save(&self) -> Result<()> {
        let directory = self.target.parent().expect("Project scan directory");
        let mut next = tempfile::NamedTempFile::new_in(directory)?;
        serde_json::to_writer(&mut next, &self.saved)?;
        next.flush()?;
        next.as_file().sync_all()?;
        next.persist(&self.target).map_err(|error| error.error)?;
        #[cfg(unix)]
        File::open(directory)?.sync_all()?;
        Ok(())
    }
}

pub async fn status(
    state: web::Data<super::super::AppState>,
    request: HttpRequest,
    project: web::Path<ProjectId>,
) -> std::result::Result<HttpResponse, crate::handlers::agent::tickets::TicketHttpError> {
    crate::handlers::agent::tickets::user(&state, &request).await?;
    Ok(HttpResponse::Ok()
        .insert_header((actix_web::http::header::CACHE_CONTROL, "no-store"))
        .json(state.tickets.project_scan_control(&project, None).await?))
}

pub async fn control(
    state: web::Data<super::super::AppState>,
    request: HttpRequest,
    project: web::Path<ProjectId>,
    control: web::Json<Control>,
) -> std::result::Result<HttpResponse, crate::handlers::agent::tickets::TicketHttpError> {
    crate::handlers::agent::tickets::user(&state, &request).await?;
    Ok(HttpResponse::Ok().json(
        state
            .tickets
            .project_scan_control(&project, Some(control.action))
            .await?,
    ))
}

fn fast_model(config: &Config) -> Option<&ProviderModelRef> {
    config.defaults.as_ref()?.fast.as_ref()
}

fn feature_enabled(config: &Config) -> bool {
    config.features.ticket_mutation && config.features.ticket_dispatch
}

async fn select_work<'a>(
    service: &Arc<TicketService>,
    snapshot: &'a Snapshot,
    project: &ProjectManifest,
) -> Result<(Option<&'a Ticket>, Option<Check>)> {
    let mut ready = snapshot
        .tickets
        .values()
        .filter(|work| policy::ready_card(snapshot, &project.id, work))
        .collect::<Vec<_>>();
    ready.sort_unstable_by(|a, b| policy::order(a).cmp(&policy::order(b)));
    let mut first_blocked = None;
    let mut workspaces = None;
    for work in ready {
        let blocker = match policy::blocker(snapshot, work) {
            Some(blocker) => Some(blocker),
            None => {
                // Only inspect registered worktrees when a card has passed the
                // ledger checks. Reuse the result for the rest of this poll.
                let workspaces = match &workspaces {
                    Some(workspaces) => workspaces,
                    None => workspaces.insert(scan_workspaces(project.clone()).await?),
                };
                scan_inputs(service, work, workspaces).await?
            }
        };
        if let Some(blocker) = blocker {
            first_blocked.get_or_insert_with(|| Check::new(work, Some(blocker)));
        } else {
            return Ok((Some(work), Some(Check::new(work, None))));
        }
    }
    Ok((None, first_blocked))
}

async fn scan_workspaces(project: ProjectManifest) -> Result<Vec<ExecutionWorkspace>> {
    tokio::task::spawn_blocking(move || {
        project
            .workspace_roots()
            .filter_map(|root| project_manager::selected_workspace(&project, Some(root)).ok())
            .map(|(_, workspace)| workspace)
            .collect()
    })
    .await
    .map_err(|error| Error::AuthorityUnavailable(error.to_string()))
}

async fn scan_inputs(
    service: &Arc<TicketService>,
    work: &Ticket,
    workspaces: &[ExecutionWorkspace],
) -> Result<Option<policy::Blocker>> {
    if workspaces.is_empty() {
        return Ok(Some(policy::Blocker::WorkspaceUnavailable));
    }
    let files = required_files(work);
    if files.is_empty() {
        return Ok(None);
    }
    let service = service.clone();
    let workspaces = workspaces.to_vec();
    tokio::task::spawn_blocking(move || {
        let mut unavailable = None;
        for workspace in &workspaces {
            match first_missing_file(&service, workspace, &files) {
                None => return None,
                Some(path) => {
                    unavailable.get_or_insert(path);
                }
            }
        }
        unavailable.map(|path| policy::Blocker::RequiredFileUnavailable { path })
    })
    .await
    .map_err(|error| Error::AuthorityUnavailable(error.to_string()))
}

fn required_files(work: &Ticket) -> BTreeSet<String> {
    work.draft_brief
        .as_ref()
        .map(|brief| brief.required_files.clone())
        .unwrap_or_default()
}

fn first_missing_file(
    service: &TicketService,
    workspace: &ExecutionWorkspace,
    files: &BTreeSet<String>,
) -> Option<String> {
    files
        .iter()
        .find(|path| service.verify_work_input(workspace, path).is_err())
        .cloned()
}

pub(super) async fn verify_required_inputs(
    service: &Arc<TicketService>,
    work: &Ticket,
    workspace: &ExecutionWorkspace,
) -> Result<()> {
    let files = required_files(work);
    if files.is_empty() {
        return Ok(());
    }
    if !work.contract.allowed_tools.contains("Read") {
        return Err(Error::ScopeDenied(
            "Required inputs need a Read contract".into(),
        ));
    }
    let service = service.clone();
    let workspace = workspace.clone();
    let missing =
        tokio::task::spawn_blocking(move || first_missing_file(&service, &workspace, &files))
            .await
            .map_err(|error| Error::AuthorityUnavailable(error.to_string()))?;
    if let Some(path) = missing {
        return Err(Error::ResourceBlocked(format!(
            "Required worktree file is unavailable: {path}"
        )));
    }
    Ok(())
}

pub(super) fn latest_permit(session: &Session) -> Result<Option<Permit>> {
    let Some(metadata) = session
        .messages
        .iter()
        .rev()
        .find(|message| message.role == Role::User)
        .and_then(|message| message.metadata.as_ref())
        .and_then(|metadata| metadata.get("session_message"))
    else {
        return Ok(None);
    };
    if metadata
        .pointer("/body/instruction")
        .and_then(Value::as_str)
        != Some(INSTRUCTION)
    {
        return Ok(None);
    }
    let source = serde_json::from_value::<SessionMessageSource>(metadata["source"].clone())?;
    if !matches!(source, SessionMessageSource::Runtime { subsystem } if subsystem == SUBSYSTEM) {
        return Err(Error::ScopeDenied(
            "Project scan requires a Host notice".into(),
        ));
    }
    Ok(Some(serde_json::from_value(
        metadata["body"]["data"].clone(),
    )?))
}

impl Permit {
    pub(super) fn validate_work(
        &self,
        project: &ProjectId,
        snapshot: &Snapshot,
        work: &Ticket,
    ) -> Result<()> {
        if &self.project_id != project
            || self.work_id != work.id
            || self.record_revision != work.record_revision
            || !policy::eligible(snapshot, project, work)
        {
            return Err(Error::ScopeDenied(
                "Project scan Work or card revision changed".into(),
            ));
        }
        Ok(())
    }

    pub(super) fn start_key(&self) -> String {
        SessionMessageId::stable("project-scan-start-v1", &json!(self)).to_string()
    }
}

impl TicketApplication {
    fn scan_host(&self) -> Result<&managed_work::ManagedWorkHost> {
        self.managed_work_host
            .get()
            .ok_or_else(|| Error::AuthorityUnavailable("Project Work Host is unavailable".into()))
    }

    async fn project_scan_control(
        &self,
        project_id: &ProjectId,
        action: Option<Action>,
    ) -> Result<Value> {
        let host = self.scan_host()?;
        let config = self.config.read().await;
        let model = fast_model(&config).map(|target| target.model.clone());
        let enabled = feature_enabled(&config);
        drop(config);
        if action.is_some_and(|action| matches!(action, Action::Enable | Action::Resume)) {
            if !enabled || model.is_none() {
                return Err(Error::ResourceBlocked(
                    "Configure Fast model and enable Work dispatch".into(),
                ));
            }
            project_manager::ensure_manager(
                &host.creator.projects,
                &host.persistence,
                &self.config,
                project_id,
            )
            .await?;
        }
        let _guard = self.project_scan_lock.lock().await;
        project_manager::active_project(&host.creator.projects, project_id)?;
        let mut file = ScanFile::open(&self.root)?;
        if let Some(action) = action {
            file.saved
                .projects
                .entry(project_id.clone())
                .or_default()
                .controls(action)?;
            file.save()?;
        }
        let absent = Entry::default();
        let entry = file.saved.projects.get(project_id).unwrap_or(&absent);
        Ok(json!({
            "project_id": project_id, "model": model, "available": enabled,
            "mode": entry.mode, "last_result": entry.outcome, "next_at": entry.next_at,
            "last_check": entry.last_check,
            "in_flight": entry.call.as_ref().is_some_and(|call| call.expires_at > Utc::now()),
            "usage": entry.usage,
        }))
    }

    /// Hold this decision fence until the Ticket start is durably admitted.
    pub(super) async fn scan_start_guard(
        &self,
        permit: &Permit,
    ) -> Result<(tokio::sync::MutexGuard<'_, ()>, ScanFile)> {
        let guard = self.project_scan_lock.lock().await;
        let file = ScanFile::open(&self.root)?;
        if !file
            .saved
            .projects
            .get(&permit.project_id)
            .is_some_and(|entry| entry.allows_start(permit, Utc::now()))
        {
            return Err(Error::ScopeDenied(
                "Project scan is paused, off, expired, or superseded".into(),
            ));
        }
        Ok((guard, file))
    }

    pub(in crate::app_state) fn spawn_project_scans(self: &Arc<Self>, shutdown: CancellationToken) {
        let tickets = self.clone();
        tokio::spawn(async move {
            let mut timer = tokio::time::interval(std::time::Duration::from_secs(2));
            timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => break,
                    _ = timer.tick() => {}
                }
                tokio::select! {
                    _ = shutdown.cancelled() => break,
                    result = tickets.project_scan_tick(Utc::now()) => {
                        if let Err(error) = result {
                            tracing::warn!(%error, "Project scan tick could not complete");
                        }
                    }
                }
            }
        });
    }

    async fn take_scan_project(&self, now: DateTime<Utc>) -> Result<Option<(ProjectId, Uuid)>> {
        if !ScanFile::path(&self.root).try_exists()? {
            return Ok(None);
        }
        let _guard = self.project_scan_lock.lock().await;
        let mut file = ScanFile::open(&self.root)?;
        let next = file
            .saved
            .projects
            .iter_mut()
            .filter(|(_, entry)| {
                entry.mode == Mode::Enabled
                    && entry.next_at.is_some_and(|next| next <= now)
                    && entry
                        .call
                        .as_ref()
                        .is_none_or(|call| call.expires_at <= now)
            })
            .min_by_key(|(_, entry)| entry.next_at);
        let Some((project, entry)) = next else {
            return Ok(None);
        };
        let selected = (project.clone(), entry.epoch);
        entry.call = None;
        entry.next_at = Some(now + INTERVAL);
        file.save()?;
        Ok(Some(selected))
    }

    pub(super) async fn project_scan_tick(self: &Arc<Self>, now: DateTime<Utc>) -> Result<()> {
        let Some((project_id, epoch)) = self.take_scan_project(now).await? else {
            return Ok(());
        };
        if self.deliver_scan_notice(&project_id).await? {
            return Ok(());
        }
        let host = self.scan_host()?;
        let project = project_manager::active_project(&host.creator.projects, &project_id)?;
        let (service, _) = self.authority(Principal::Runtime).await?;
        let snapshot = service.published()?.1;
        let config = self.config.read().await.clone();
        if !feature_enabled(&config) {
            return Ok(());
        }
        let (work, check) = select_work(&service, &snapshot, &project).await?;
        let capacity = bamboo_orchestration::check_work_dispatch_capacity(
            &snapshot,
            config.features.work_execution.max_parallel,
        )
        .is_ok();
        let manager = if work.is_some() && capacity {
            Some(
                project_manager::ensure_manager(
                    &host.creator.projects,
                    &host.persistence,
                    &self.config,
                    &project_id,
                )
                .await?,
            )
        } else {
            None
        };
        let summary_call = work
            .zip(manager.as_ref())
            .and_then(|(work, _)| policy::summary_messages(work))
            .zip(fast_model(&config))
            .and_then(|(messages, target)| {
                let (provider, managed) = host
                    .providers
                    .provider_for_configured_model_with_origin(&config, target)
                    .ok()?;
                (managed && provider.known_pre_dispatch_error().is_none())
                    .then_some((messages, target, provider))
            });

        let permit = {
            let _guard = self.project_scan_lock.lock().await;
            let mut file = ScanFile::open(&self.root)?;
            let Some(entry) = file.saved.projects.get_mut(&project_id).filter(|entry| {
                entry.mode == Mode::Enabled
                    && entry.epoch == epoch
                    && entry.handoff.is_none()
                    && entry.call.is_none()
            }) else {
                return Ok(());
            };
            entry.last_check = check;
            let Some((work, manager)) = work.zip(manager) else {
                entry.outcome = if work.is_some() {
                    Outcome::Capacity
                } else {
                    Outcome::Waiting
                };
                return file.save();
            };
            let permit = Permit {
                project_id: project_id.clone(),
                work_id: work.id.clone(),
                record_revision: work.record_revision,
                epoch,
                call_id: Uuid::new_v4(),
            };
            entry.handoff = Some(Handoff::Pending {
                notice: Notice {
                    permit: permit.clone(),
                    manager_id: manager.id,
                    manager_created_at: manager.created_at,
                    created_at: Utc::now(),
                },
            });
            entry.outcome = Outcome::Pending;
            if summary_call.is_some() {
                entry.call = Some(Claim {
                    call_id: permit.call_id,
                    expires_at: Utc::now() + INTERVAL,
                });
                entry.usage.model_requests = entry.usage.model_requests.saturating_add(1);
                entry.usage.unknown_requests = entry.usage.unknown_requests.saturating_add(1);
                entry.outcome = Outcome::Scanning;
            }
            file.save()?;
            permit
        };
        if let Some((messages, target, provider)) = summary_call {
            let reply = match WorkTextClient::from_config(&config) {
                Ok(client) => {
                    let tickets = self.clone();
                    let proof = permit.clone();
                    let route = target.clone();
                    let expected_provider = provider.clone();
                    work_text::project_scan(
                        &client,
                        provider.as_ref(),
                        work_text::Request {
                            model: &target.model,
                            messages: &messages,
                            input_cap: policy::INPUT_TOKENS,
                            output_cap: policy::OUTPUT_TOKENS,
                            remaining: CALL_TIMEOUT,
                        },
                        Arc::new(move || {
                            let tickets = tickets.clone();
                            let proof = proof.clone();
                            let route = route.clone();
                            let expected_provider = expected_provider.clone();
                            Box::pin(async move {
                                tickets
                                    .confirm_scan_call(&proof, &route, &expected_provider)
                                    .await
                                    .map_err(|_| {
                                        bamboo_llm::LLMError::RequestFailed(
                                            "Project scan admission changed".into(),
                                        )
                                    })
                            })
                        }),
                        target
                            .reasoning_effort
                            .or(Some(bamboo_domain::ReasoningEffort::Low)),
                    )
                    .await
                }
                Err(_) => WorkTextReply::NotSent,
            };
            self.settle_summary(&permit, reply).await?;
        }
        self.deliver_scan_notice(&project_id).await?;
        Ok(())
    }

    async fn confirm_scan_call(
        &self,
        permit: &Permit,
        target: &ProviderModelRef,
        expected_provider: &Arc<dyn bamboo_llm::LLMProvider>,
    ) -> Result<()> {
        let (service, _) = self.authority(Principal::Runtime).await?;
        let _guard = self.project_scan_lock.lock().await;
        let file = ScanFile::open(&self.root)?;
        if !file.saved.projects.get(&permit.project_id).is_some_and(|entry| {
            entry.same_epoch(permit)
                && matches!(&entry.handoff, Some(Handoff::Pending { notice }) if &notice.permit == permit)
                && entry.call.as_ref().is_some_and(
                    |call| call.call_id == permit.call_id && call.expires_at > Utc::now()
                )
        }) {
            return Err(Error::RevisionConflict);
        }
        let host = self.scan_host()?;
        project_manager::active_project(&host.creator.projects, &permit.project_id)?;
        let current = self.config.read().await;
        let (provider, managed) = host
            .providers
            .provider_for_configured_model_with_origin(&current, target)
            .map_err(|_| Error::RevisionConflict)?;
        if !managed
            || !feature_enabled(&current)
            || fast_model(&current) != Some(target)
            || !Arc::ptr_eq(expected_provider, &provider)
        {
            return Err(Error::RevisionConflict);
        }
        let snapshot = service.published()?.1;
        let work = snapshot
            .tickets
            .get(&permit.work_id)
            .ok_or(Error::RevisionConflict)?;
        permit.validate_work(&permit.project_id, &snapshot, work)
    }

    async fn settle_summary(&self, permit: &Permit, reply: WorkTextReply) -> Result<()> {
        let _guard = self.project_scan_lock.lock().await;
        let mut file = ScanFile::open(&self.root)?;
        let entry = file
            .saved
            .projects
            .get_mut(&permit.project_id)
            .ok_or(Error::RevisionConflict)?;
        if !entry
            .call
            .as_ref()
            .is_some_and(|call| call.call_id == permit.call_id)
        {
            return Ok(());
        }
        entry.call = None;
        let summary = match reply {
            WorkTextReply::Complete { text, exact_usage } => {
                if let Some(usage) = exact_usage {
                    entry.usage.tokens.add_assign(usage);
                    entry.usage.unknown_requests = entry.usage.unknown_requests.saturating_sub(1);
                }
                text.as_deref().and_then(policy::parse_summary)
            }
            WorkTextReply::NotSent => {
                entry.usage.model_requests = entry.usage.model_requests.saturating_sub(1);
                entry.usage.unknown_requests = entry.usage.unknown_requests.saturating_sub(1);
                None
            }
            _ => None,
        };
        if entry.same_epoch(permit)
            && matches!(&entry.handoff, Some(Handoff::Pending { notice }) if &notice.permit == permit)
        {
            entry.outcome = Outcome::Pending;
            if let Some(check) = entry.last_check.as_mut() {
                check.summary = summary;
            }
        }
        file.save()
    }

    async fn deliver_scan_notice(&self, project_id: &ProjectId) -> Result<bool> {
        let _guard = self.project_scan_lock.lock().await;
        let mut file = ScanFile::open(&self.root)?;
        let Some(entry) = file
            .saved
            .projects
            .get_mut(project_id)
            .filter(|entry| entry.mode == Mode::Enabled)
        else {
            return Ok(false);
        };
        let Some(handoff) = entry.handoff.as_ref() else {
            return Ok(false);
        };
        let config = self.config.read().await;
        if !feature_enabled(&config) {
            return Ok(true);
        }
        let max_parallel = config.features.work_execution.max_parallel;
        drop(config);
        let host = self.scan_host()?;
        let project = project_manager::active_project(&host.creator.projects, project_id)?;
        let (service, _) = self.authority(Principal::Runtime).await?;
        let snapshot = service.published()?.1;
        let notice = handoff.notice();
        let fresh = entry.same_epoch(&notice.permit)
            && snapshot
                .tickets
                .get(&notice.permit.work_id)
                .is_some_and(|work| {
                    notice
                        .permit
                        .validate_work(project_id, &snapshot, work)
                        .is_ok()
                })
            && project.project_manager.as_ref().is_some_and(|binding| {
                binding.session_id.to_string() == notice.manager_id
                    && binding.created_at == notice.manager_created_at
            });
        let expired =
            matches!(handoff, Handoff::Notified { expires_at, .. } if *expires_at <= Utc::now());
        if !fresh || expired {
            entry.handoff = None;
            entry.last_check = None;
            entry.outcome = Outcome::Unconfirmed;
            file.save()?;
            return Ok(false);
        }
        if matches!(handoff, Handoff::Notified { .. }) {
            return Ok(true);
        }
        if bamboo_orchestration::check_work_dispatch_capacity(&snapshot, max_parallel).is_err() {
            entry.outcome = Outcome::Capacity;
            file.save()?;
            return Ok(true);
        }
        let notice = notice.clone();
        let manager = self.storage.load_session(&notice.manager_id).await?;
        if !manager
            .as_ref()
            .is_some_and(|manager| project_manager::manager_matches(&project, manager))
        {
            entry.handoff = None;
            entry.last_check = None;
            entry.outcome = Outcome::Unconfirmed;
            file.save()?;
            return Ok(false);
        }
        let content = format!(
            "Project scan selected Work {}. Ready with accepted dependencies, an allowed tool contract, and a prepared worktree. Inspect the Work, choose its Section by content, and call project_manager_work to start it.",
            notice.permit.work_id,
        );
        let envelope = SessionMessageEnvelope {
            id: SessionMessageId::stable(
                INSTRUCTION,
                &json!({
                    "permit": notice.permit, "manager_id": notice.manager_id,
                    "manager_created_at": notice.manager_created_at,
                }),
            ),
            source: SessionMessageSource::Runtime {
                subsystem: SUBSYSTEM.into(),
            },
            target_session_id: notice.manager_id.clone(),
            kind: SessionMessageKind::RuntimeInstruction,
            body: SessionMessageBody::RuntimeInstruction(SessionRuntimeInstruction {
                instruction: INSTRUCTION.into(),
                content: Some(SessionMessageContent::text(content)),
                data: Some(serde_json::to_value(&notice.permit)?),
                provider_message: None,
            }),
            created_at: notice.created_at,
            thread_id: None,
            in_reply_to: None,
            attempt: None,
            correlation_id: None,
        };
        let messenger = &host.creator.messenger;
        let admission = messenger
            .admit_runtime_for_target(envelope, notice.manager_created_at)
            .await
            .map_err(|error| Error::AuthorityUnavailable(error.to_string()))?;
        messenger
            .prepare_message_activation(&admission, SessionActivationPolicy::RespectSpecificWait)
            .await
            .map_err(|error| Error::AuthorityUnavailable(error.to_string()))?;
        entry.handoff = Some(Handoff::Notified {
            notice,
            expires_at: Utc::now() + START_WINDOW,
        });
        entry.outcome = Outcome::Notified;
        file.save()?;
        drop(file);
        drop(_guard);
        if let Err(error) = messenger.activate_prepared(&admission).await {
            tracing::warn!(%error, "Project scan notice committed; activation will be reconciled");
        }
        Ok(true)
    }
}
