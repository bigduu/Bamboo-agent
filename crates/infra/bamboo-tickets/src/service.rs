use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::{Arc, Mutex},
};

use crate::{
    canonical_bytes, content_hash, model::*, store::FileStore, Error, Health, PublicationFault,
    Result,
};

/// Constructed only by the embedding trusted host after validating its current
/// Supervisor/Runtime proof. Deliberately has no Deserialize implementation.
#[derive(Clone, Debug)]
pub struct Authority {
    pub(crate) binding: ScopeBinding,
    pub(crate) principal: Principal,
}

#[derive(Clone, Debug)]
pub enum Principal {
    Supervisor {
        session_id: String,
    },
    User {
        user_id: String,
    },
    Runtime,
    Worker {
        assignment_id: String,
        generation: u64,
        run_id: String,
        session_id: String,
    },
}

impl Authority {
    pub fn principal(&self) -> &Principal {
        &self.principal
    }
    /// This Rust port is host-only. Never map client JSON directly to this call.
    pub fn from_verified_host(binding: ScopeBinding, principal: Principal) -> Self {
        Self { binding, principal }
    }
    fn identity(&self) -> String {
        match &self.principal {
            Principal::Supervisor { session_id } => format!("supervisor:{session_id}"),
            Principal::User { user_id } => format!("user:{user_id}"),
            Principal::Runtime => "runtime".into(),
            Principal::Worker {
                assignment_id,
                generation,
                run_id,
                session_id,
            } => format!("worker:{assignment_id}:{generation}:{run_id}:{session_id}"),
        }
    }
    fn supervisor(&self) -> Result<()> {
        match &self.principal {
            Principal::Supervisor { session_id }
                if session_id == &self.binding.supervisor_session_id =>
            {
                Ok(())
            }
            Principal::User { .. } => Ok(()),
            _ => Err(Error::ScopeDenied(
                "Supervisor/User authority required".into(),
            )),
        }
    }
    fn runtime(&self) -> Result<()> {
        if matches!(self.principal, Principal::Runtime) {
            Ok(())
        } else {
            Err(Error::ScopeDenied(
                "trusted Runtime authority required".into(),
            ))
        }
    }
    fn user(&self) -> Result<()> {
        if matches!(self.principal, Principal::User { .. }) {
            Ok(())
        } else {
            Err(Error::ScopeDenied("explicit User decision required".into()))
        }
    }
    fn worker(&self, assignment: &Assignment) -> Result<()> {
        match (&self.principal, &assignment.runtime) {
            (
                Principal::Worker {
                    assignment_id,
                    generation,
                    run_id,
                    session_id,
                },
                Some(receipt),
            ) if assignment_id == &assignment.id
                && generation == &assignment.generation
                && run_id == &receipt.run_id
                && session_id == &receipt.session_id =>
            {
                Ok(())
            }
            _ => Err(Error::ScopeDenied(
                "worker execution identity mismatch".into(),
            )),
        }
    }
}

#[derive(Clone)]
pub struct TicketService {
    inner: Arc<Mutex<FileStore>>,
}

impl TicketService {
    pub fn open(root: impl AsRef<Path>, binding: ScopeBinding) -> Result<Self> {
        let mut store = FileStore::open(root.as_ref(), binding)?;
        if store.health == Health::Writable {
            if let Err(error) =
                validate_snapshot(&store.published.as_ref().expect("healthy store snapshot").1)
            {
                store.health = Health::ReadOnly {
                    reason: error.to_string(),
                };
            }
        }
        if store.health == Health::Writable {
            let mut snapshot = store
                .published
                .as_ref()
                .expect("healthy store snapshot")
                .1
                .clone();
            validate_snapshot(&snapshot)?;
            snapshot.authority_epoch = snapshot
                .authority_epoch
                .checked_add(1)
                .ok_or_else(|| Error::AuthorityUnavailable("epoch overflow".into()))?;
            snapshot.seq += 1;
            // Old running permissions never survive a writer restart. Admission
            // reconciliation must revalidate them; unknown effects stay isolated.
            for assignment in snapshot.assignments.values_mut() {
                if matches!(
                    assignment.state,
                    AssignmentState::DispatchPending
                        | AssignmentState::Admitted
                        | AssignmentState::Running
                        | AssignmentState::Cancelling
                ) || assignment.state == AssignmentState::Blocked && !assignment.process_stopped
                {
                    assignment.state = AssignmentState::OutcomeUnknown;
                    assignment.updated_seq = snapshot.seq;
                    assignment.record_revision += 1;
                    if let Some(work) = snapshot.tickets.get_mut(&assignment.work_id) {
                        if work.active_assignment.as_deref() == Some(&assignment.id)
                            && work.state == WorkState::Active
                        {
                            work.state = WorkState::Blocked;
                            work.blocked = Some(BlockReason {
                                reason:
                                    "writer restarted; Runtime execution requires reconciliation"
                                        .into(),
                                resume_state: WorkState::Ready,
                            });
                            touch(work, snapshot.seq);
                        }
                    }
                }
            }
            for request in snapshot.requests.values_mut() {
                if matches!(request.kind, RequestKind::Approval { .. })
                    && matches!(
                        request.status,
                        RequestStatus::Open | RequestStatus::Approved
                    )
                {
                    request.status = RequestStatus::Expired;
                    request.updated_seq = snapshot.seq;
                }
            }
            store.publish(snapshot)?;
        }
        Ok(Self {
            inner: Arc::new(Mutex::new(store)),
        })
    }

    pub fn health(&self) -> Health {
        self.inner.lock().expect("store mutex").health.clone()
    }

    /// Trusted host view only; Worker-facing reads use the bounded scoped API.
    pub fn published(&self) -> Result<(String, Snapshot)> {
        self.inner
            .lock()
            .expect("store mutex")
            .published
            .clone()
            .ok_or_else(|| Error::AuthorityUnavailable("no verified published snapshot".into()))
    }

    pub fn fixed_snapshot(&self, commit: &str) -> Result<Snapshot> {
        self.inner
            .lock()
            .expect("store mutex")
            .load_snapshot(commit)
    }

    pub fn export(&self, destination: impl AsRef<Path>) -> Result<String> {
        self.inner
            .lock()
            .expect("store mutex")
            .export(destination.as_ref())
    }

    /// Host computes the content hash from verified output bytes, never from
    /// a Worker's declared URI/hash. Publication occurs with the Submission.
    pub fn store_artifact(&self, authority: &Authority, bytes: &[u8]) -> Result<Artifact> {
        let store = self.inner.lock().expect("store mutex");
        let snapshot = &store
            .published
            .as_ref()
            .ok_or_else(|| Error::AuthorityUnavailable("no verified snapshot".into()))?
            .1;
        validate_authority(authority, snapshot)?;
        authority.runtime()?;
        store.store_artifact(bytes)
    }

    /// Own submissions and versioned dependency inputs only for a Worker;
    /// Supervisor/User reads still require a referenced scope Artifact.
    pub fn read_artifact(
        &self,
        authority: &Authority,
        artifact: &Artifact,
        budget: usize,
    ) -> Result<Vec<u8>> {
        let store = self.inner.lock().expect("store mutex");
        let snapshot = &store
            .published
            .as_ref()
            .ok_or_else(|| Error::AuthorityUnavailable("no verified snapshot".into()))?
            .1;
        validate_authority(authority, snapshot)?;
        let permitted = snapshot.submissions.values().any(|submission| {
            submission.artifacts.contains(artifact)
                && match &authority.principal {
                    Principal::Worker { assignment_id, .. } => {
                        submission.assignment_id == *assignment_id
                            || snapshot.assignments[assignment_id]
                                .dependency_inputs
                                .iter()
                                .any(|input| input.submission_id == submission.id)
                    }
                    _ => true,
                }
        });
        if !permitted {
            return Err(Error::ScopeDenied(
                "Artifact is not a readable scope reference".into(),
            ));
        }
        store.read_artifact(artifact, budget)
    }

    pub(crate) fn snapshot_parent(&self, commit: &str) -> Result<Option<String>> {
        self.inner
            .lock()
            .expect("store mutex")
            .parent_commit(commit)
    }

    pub fn set_publication_fault(&self, fault: Option<PublicationFault>) {
        self.inner.lock().expect("store mutex").set_fault(fault);
    }

    #[cfg(feature = "test-utils")]
    pub fn set_operation_publication_fault(&self, prefix: String, fault: PublicationFault) {
        self.inner
            .lock()
            .expect("store mutex")
            .set_operation_fault(prefix, fault);
    }

    pub fn execute(&self, authority: &Authority, command: &Command) -> Result<OperationReceipt> {
        let mut store = self.inner.lock().expect("store mutex");
        let snapshot = &store
            .published
            .as_ref()
            .ok_or_else(|| Error::AuthorityUnavailable("no verified snapshot".into()))?
            .1;
        // Validate identity/scope/receipt subject BEFORE exposing duplicate receipts.
        validate_authority(authority, snapshot)?;
        if command.binding != snapshot.binding {
            return Err(Error::ScopeDenied("command scope".into()));
        }
        if command.operation_id.is_empty() || command.operation_id.len() > 256 {
            return Err(Error::InvalidTransition("invalid operation id".into()));
        }
        if let Some(source) = &command.source {
            validate_command_source(authority, source)?;
        }
        let hash = content_hash(&canonical_bytes(command)?);
        if let Some(receipt) = snapshot.receipts.get(&command.operation_id) {
            if receipt.principal != authority.identity() {
                return Err(Error::ScopeDenied("receipt subject".into()));
            }
            return if receipt.request_hash == hash {
                Ok(receipt.clone())
            } else {
                Err(Error::IdempotencyConflict)
            };
        }
        if store.health != Health::Writable {
            return Err(Error::AuthorityUnavailable(format!("{:?}", store.health)));
        }
        if command.expected_seq != snapshot.seq
            || command.expected_epoch != snapshot.authority_epoch
        {
            return Err(Error::RevisionConflict);
        }
        if command.operations.len() > 64 {
            return Err(Error::InvalidTransition(
                "operation batch exceeds 64".into(),
            ));
        }
        let mut next = snapshot.clone();
        next.seq += 1;
        let mut ids = BTreeMap::new();
        for operation in &command.operations {
            if let Operation::Start {
                workspace: Some(workspace),
                ..
            } = operation
            {
                for write_root in &workspace.write_roots {
                    let path = Path::new(write_root).canonicalize()?;
                    if store.root().starts_with(&path) || path.starts_with(store.root()) {
                        return Err(Error::ScopeDenied(
                            "Ticket authority cannot be in a Worker write root".into(),
                        ));
                    }
                }
            }
            apply(&mut next, authority, operation, &mut ids)?;
        }
        validate_snapshot(&next)?;
        let receipt = OperationReceipt {
            operation_id: command.operation_id.clone(),
            principal: authority.identity(),
            request_hash: hash,
            canonical_request: String::from_utf8(canonical_bytes(command)?)
                .expect("canonical JSON is UTF-8"),
            committed_seq: next.seq,
            ids,
        };
        next.receipts
            .insert(command.operation_id.clone(), receipt.clone());
        #[cfg(feature = "test-utils")]
        store.publish_operation(next, &command.operation_id)?;
        #[cfg(not(feature = "test-utils"))]
        store.publish(next)?;
        Ok(receipt)
    }

    /// Builds repeatable commands for a trusted host ingress/tool adapter.
    /// Replayed logical operations retain the original CAS request, not a new one.
    pub fn prepare_command(
        &self,
        authority: &Authority,
        operation_id: &str,
        operations: Vec<Operation>,
    ) -> Result<Command> {
        self.prepare_request(authority, operation_id, operations, None)
    }

    /// The original adapter input determines replay, including fields which do
    /// not change its projected operations. Generated operations remain frozen.
    pub fn prepare_source_command(
        &self,
        authority: &Authority,
        operation_id: &str,
        operations: Vec<Operation>,
        source: CommandSource,
    ) -> Result<Command> {
        validate_command_source(authority, &source)?;
        self.prepare_request(authority, operation_id, operations, Some(source))
    }

    fn prepare_request(
        &self,
        authority: &Authority,
        operation_id: &str,
        operations: Vec<Operation>,
        source: Option<CommandSource>,
    ) -> Result<Command> {
        let (_, snapshot) = self.published()?;
        validate_authority(authority, &snapshot)?;
        if let Some(receipt) = snapshot.receipts.get(operation_id) {
            if receipt.principal != authority.identity() {
                return Err(Error::ScopeDenied("receipt subject".into()));
            }
            if receipt.canonical_request.is_empty() {
                return Err(Error::AuthorityUnavailable(
                    "legacy receipt lacks canonical request; retry needs original command".into(),
                ));
            }
            let original: Command = serde_json::from_str(&receipt.canonical_request)?;
            let same_input = match &source {
                Some(source) => source_matches(&original, source)?,
                None => canonical_bytes(&original.operations)? == canonical_bytes(&operations)?,
            };
            if !same_input {
                return Err(Error::IdempotencyConflict);
            }
            return Ok(original);
        }
        Ok(Command {
            operation_id: operation_id.into(),
            binding: snapshot.binding,
            expected_seq: snapshot.seq,
            expected_epoch: snapshot.authority_epoch,
            operations,
            source,
        })
    }

    /// Read an immutable original command before regenerating any adapter IDs.
    /// Scope/subject and the complete canonical input are checked first. The
    /// caller may replay this exact command, never replace its typed operations.
    pub fn replay_source_command(
        &self,
        authority: &Authority,
        operation_id: &str,
        source: &CommandSource,
    ) -> Result<Option<Command>> {
        let (_, snapshot) = self.published()?;
        validate_authority(authority, &snapshot)?;
        let Some(receipt) = snapshot.receipts.get(operation_id) else {
            return Ok(None);
        };
        if receipt.principal != authority.identity() {
            return Err(Error::ScopeDenied("receipt subject".into()));
        }
        validate_command_source(authority, source)?;
        if receipt.canonical_request.is_empty() {
            return Err(Error::AuthorityUnavailable(
                "legacy receipt lacks canonical request; retry needs original command".into(),
            ));
        }
        let original: Command = serde_json::from_str(&receipt.canonical_request)?;
        if !source_matches(&original, source)? {
            return Err(Error::IdempotencyConflict);
        }
        Ok(Some(original))
    }

    /// Checks the tool entry, not just the final result. A lease is not permission.
    pub fn authorize_tool(&self, authority: &Authority, tool: &str) -> Result<()> {
        let (_, snapshot) = self.published()?;
        validate_authority(authority, &snapshot)?;
        let Principal::Worker { assignment_id, .. } = &authority.principal else {
            return Err(Error::ScopeDenied("Worker permit required".into()));
        };
        let assignment = assignment(&snapshot, assignment_id)?;
        authority.worker(assignment)?;
        active_permit(&snapshot, assignment)?;
        if !assignment.allowed_tools.contains(tool) {
            return Err(Error::ScopeDenied(
                "tool is outside contract capability".into(),
            ));
        }
        Ok(())
    }
}

fn validate_command_source(authority: &Authority, source: &CommandSource) -> Result<()> {
    if !matches!(authority.principal, Principal::Worker { .. }) {
        return Err(Error::ScopeDenied(
            "Worker adapter input requires Worker identity".into(),
        ));
    }
    match source {
        CommandSource::WorkerTask { arguments } if arguments.is_object() => {}
        _ => return Err(invalid("adapter arguments must be an object")),
    }
    if canonical_bytes(source)?.len() > 65536 {
        return Err(invalid("adapter input exceeds 64 KiB"));
    }
    Ok(())
}

fn source_matches(command: &Command, source: &CommandSource) -> Result<bool> {
    let stored = command.source.as_ref().ok_or_else(|| {
        Error::AuthorityUnavailable(
            "legacy adapter receipt lacks original input; retry needs original command".into(),
        )
    })?;
    Ok(canonical_bytes(stored)? == canonical_bytes(source)?)
}

pub(crate) fn validate_authority(authority: &Authority, snapshot: &Snapshot) -> Result<()> {
    if authority.binding != snapshot.binding {
        return Err(Error::ScopeDenied("scope binding changed".into()));
    }
    match &authority.principal {
        Principal::Supervisor { session_id }
            if session_id != &snapshot.binding.supervisor_session_id =>
        {
            Err(Error::ScopeDenied("Supervisor identity".into()))
        }
        Principal::User { user_id } if user_id.is_empty() => {
            Err(Error::ScopeDenied("User identity".into()))
        }
        Principal::Worker { assignment_id, .. } => {
            authority.worker(assignment(snapshot, assignment_id)?)
        }
        _ => Ok(()),
    }
}

fn invalid(reason: &str) -> Error {
    Error::InvalidTransition(reason.into())
}
fn ticket<'a>(snapshot: &'a Snapshot, id: &str) -> Result<&'a Ticket> {
    snapshot
        .tickets
        .get(id)
        .ok_or_else(|| invalid("ticket not found"))
}
fn assignment<'a>(snapshot: &'a Snapshot, id: &str) -> Result<&'a Assignment> {
    snapshot
        .assignments
        .get(id)
        .ok_or_else(|| invalid("assignment not found"))
}
fn resolve(id: &str, ids: &BTreeMap<String, String>) -> String {
    ids.get(id).cloned().unwrap_or_else(|| id.into())
}
fn allocate(temp_id: &str, ids: &mut BTreeMap<String, String>) -> Result<String> {
    if temp_id.is_empty() || ids.contains_key(temp_id) {
        return Err(invalid("duplicate/empty temporary id"));
    }
    let id = uuid::Uuid::new_v4().to_string();
    ids.insert(temp_id.into(), id.clone());
    Ok(id)
}
fn touch(ticket: &mut Ticket, seq: u64) {
    ticket.record_revision += 1;
    ticket.updated_seq = seq;
}
fn active_permit(snapshot: &Snapshot, assignment: &Assignment) -> Result<()> {
    current_attempt(snapshot, assignment)?;
    if assignment.process_stopped {
        return Err(Error::ScopeDenied("Worker process has stopped".into()));
    }
    Ok(())
}

fn current_attempt(snapshot: &Snapshot, assignment: &Assignment) -> Result<()> {
    let work = ticket(snapshot, &assignment.work_id)?;
    if assignment.authority_epoch != snapshot.authority_epoch
        || assignment.contract_revision != work.contract_revision
        || assignment.generation != work.generation
        || work.active_assignment.as_deref() != Some(&assignment.id)
        || work.state != WorkState::Active
        || !matches!(
            assignment.state,
            AssignmentState::Admitted | AssignmentState::Running
        )
    {
        return Err(Error::ScopeDenied(
            "run permission is stale or inactive".into(),
        ));
    }
    Ok(())
}

fn invalidate_requests(snapshot: &mut Snapshot, work_id: &str) {
    for request in snapshot
        .requests
        .values_mut()
        .filter(|r| r.work_id == work_id)
    {
        if matches!(
            request.status,
            RequestStatus::Open | RequestStatus::Approved
        ) {
            request.status = RequestStatus::Superseded;
            request.updated_seq = snapshot.seq;
        }
    }
}

fn invalidate_parent_goals(snapshot: &mut Snapshot, ticket_id: &str) {
    let mut next = snapshot
        .tickets
        .get(ticket_id)
        .and_then(|t| t.parent.clone());
    while let Some(id) = next {
        let parent = snapshot
            .tickets
            .get_mut(&id)
            .expect("validated containment parent");
        next = parent.parent.clone();
        if parent.state == WorkState::Accepted {
            parent.blocked = Some(BlockReason {
                reason: "child acceptance changed; Goal needs independent review".into(),
                resume_state: WorkState::Submitted,
            });
            parent.state = WorkState::Blocked;
            touch(parent, snapshot.seq);
        }
    }
}

fn invalidate_dependents(snapshot: &mut Snapshot, work_id: &str) {
    let mut affected = BTreeSet::new();
    let mut frontier = vec![work_id.to_string()];
    while let Some(id) = frontier.pop() {
        for consumer in snapshot
            .assignments
            .values()
            .filter(|a| a.dependency_inputs.iter().any(|i| i.work_id == id))
            .map(|a| a.work_id.clone())
        {
            if affected.insert(consumer.clone()) {
                frontier.push(consumer);
            }
        }
    }
    for id in affected {
        if let Some(work) = snapshot.tickets.get_mut(&id) {
            if work.state != WorkState::Cancelled {
                work.blocked = Some(BlockReason {
                    reason: "accepted dependency input revoked; review required".into(),
                    resume_state: work.state,
                });
                work.state = WorkState::Blocked;
                work.accepted_submission = None;
                touch(work, snapshot.seq);
            }
        }
        invalidate_requests(snapshot, &id);
    }
}

fn dependencies(snapshot: &Snapshot, work: &Ticket) -> Result<Vec<DependencyInput>> {
    work.depends_on
        .iter()
        .map(|id| {
            let prerequisite = ticket(snapshot, id)?;
            let submission_id = prerequisite
                .accepted_submission
                .as_ref()
                .filter(|_| prerequisite.state == WorkState::Accepted)
                .ok_or_else(|| {
                    Error::ResourceBlocked(format!("dependency {id} is not accepted"))
                })?;
            let submission = snapshot
                .submissions
                .get(submission_id)
                .ok_or_else(|| invalid("missing dependency submission"))?;
            Ok(DependencyInput {
                work_id: id.clone(),
                submission_id: submission_id.clone(),
                contract_revision: prerequisite.contract_revision,
                artifact_hashes: submission
                    .artifacts
                    .iter()
                    .map(|a| a.sha256.clone())
                    .collect(),
            })
        })
        .collect()
}

fn apply(
    snapshot: &mut Snapshot,
    authority: &Authority,
    operation: &Operation,
    ids: &mut BTreeMap<String, String>,
) -> Result<()> {
    use Operation::*;
    let seq = snapshot.seq;
    match operation {
        Create {
            temp_id,
            kind,
            parent,
            contract,
            depends_on,
        } => {
            authority.supervisor()?;
            if *kind == TicketKind::Step {
                return Err(invalid("Steps belong to Worker LocalPlan"));
            }
            validate_contract(contract)?;
            let id = allocate(temp_id, ids)?;
            snapshot.tickets.insert(
                id.clone(),
                Ticket {
                    id,
                    scope_id: snapshot.binding.scope_id.clone(),
                    kind: *kind,
                    parent: parent.as_ref().map(|p| resolve(p, ids)),
                    depends_on: depends_on.iter().map(|d| resolve(d, ids)).collect(),
                    record_revision: 1,
                    contract_revision: 1,
                    generation: 0,
                    contract: contract.clone(),
                    state: WorkState::Draft,
                    blocked: None,
                    paused: false,
                    archived: false,
                    active_assignment: None,
                    current_submission: None,
                    accepted_submission: None,
                    updated_seq: seq,
                    import_source: None,
                },
            );
            snapshot.graph_revision += 1;
        }
        Ready { work_id } => {
            authority.supervisor()?;
            let id = resolve(work_id, ids);
            let work = snapshot
                .tickets
                .get_mut(&id)
                .ok_or_else(|| invalid("ticket not found"))?;
            if work.state != WorkState::Draft && !(work.paused && work.active_assignment.is_none())
            {
                return Err(invalid("ready requires draft or a stopped explicit pause"));
            }
            work.state = WorkState::Ready;
            work.paused = false;
            work.blocked = None;
            touch(work, seq);
        }
        UpdateContract { work_id, contract } => {
            authority.supervisor()?;
            validate_contract(contract)?;
            let work_id = resolve(work_id, ids);
            let old = ticket(snapshot, &work_id)?.clone();
            if old.contract == *contract {
                return Ok(());
            }
            if old.kind != TicketKind::Work && old.kind != TicketKind::Goal {
                return Err(invalid("cannot edit Step contract"));
            }
            if let Some(id) = &old.active_assignment {
                let a = snapshot
                    .assignments
                    .get_mut(id)
                    .ok_or_else(|| invalid("assignment missing"))?;
                if a.state != AssignmentState::OutcomeUnknown {
                    a.state = AssignmentState::Cancelling;
                }
                a.record_revision += 1;
                a.updated_seq = seq;
            }
            let work = snapshot.tickets.get_mut(&work_id).expect("work");
            work.contract = contract.clone();
            work.contract_revision += 1;
            work.accepted_submission = None;
            work.current_submission = None;
            if old.active_assignment.is_some() || old.paused {
                work.state = WorkState::Blocked;
                work.blocked = Some(BlockReason {
                    reason: if old.paused {
                        "explicitly paused; contract changed"
                    } else {
                        "contract changed; waiting for old run to stop"
                    }
                    .into(),
                    resume_state: WorkState::Ready,
                });
            } else {
                work.state = WorkState::Ready;
                work.blocked = None;
            }
            touch(work, seq);
            invalidate_requests(snapshot, &work_id);
            invalidate_dependents(snapshot, &work_id);
            invalidate_parent_goals(snapshot, &work_id);
        }
        SetDependencies {
            work_id,
            depends_on,
        } => {
            authority.supervisor()?;
            let id = resolve(work_id, ids);
            let work = snapshot
                .tickets
                .get_mut(&id)
                .ok_or_else(|| invalid("work not found"))?;
            if work.active_assignment.is_some()
                || !matches!(work.state, WorkState::Draft | WorkState::Ready)
            {
                return Err(invalid("dependencies frozen during execution/review"));
            }
            work.depends_on = depends_on.iter().map(|id| resolve(id, ids)).collect();
            work.contract_revision += 1;
            touch(work, seq);
            snapshot.graph_revision += 1;
            invalidate_requests(snapshot, &id);
        }
        Start {
            work_id,
            temp_id,
            workspace,
        } => {
            authority.supervisor()?;
            let work_id = resolve(work_id, ids);
            let work = ticket(snapshot, &work_id)?.clone();
            if work.kind != TicketKind::Work
                || work.state != WorkState::Ready
                || work.active_assignment.is_some()
            {
                return Err(invalid(
                    "start requires ready Work without an active Assignment",
                ));
            }
            if let Some(workspace) = workspace {
                validate_workspace(workspace)?;
                let canonical = Path::new(&workspace.worktree)
                    .canonicalize()?
                    .to_string_lossy()
                    .into_owned();
                if !workspace.claims.contains(&format!("worktree:{canonical}")) {
                    return Err(invalid(
                        "code Assignment requires canonical worktree resource claim",
                    ));
                }
            }
            if let Some(candidate) = workspace {
                if snapshot
                    .assignments
                    .values()
                    .filter(|a| holds_resources(a))
                    .any(|a| {
                        a.workspace
                            .as_ref()
                            .is_some_and(|w| !candidate.claims.is_disjoint(&w.claims))
                    })
                {
                    return Err(Error::ResourceBlocked(
                        "workspace has an unconfirmed or active owner".into(),
                    ));
                }
            }
            let inputs = dependencies(snapshot, &work)?;
            let id = allocate(temp_id, ids)?;
            let generation = work.generation + 1;
            let dispatch_key = format!("{}/{}/{generation}", snapshot.binding.scope_id, id);
            let spec = DispatchSpec {
                binding: snapshot.binding.clone(),
                work_id: work_id.clone(),
                assignment_id: id.clone(),
                generation,
                contract_revision: work.contract_revision,
                authority_epoch: snapshot.authority_epoch,
                contract: work.contract.clone(),
                dependency_inputs: inputs.clone(),
                workspace: workspace.clone(),
            };
            let spec_hash = content_hash(&canonical_bytes(&spec)?);
            snapshot.assignments.insert(
                id.clone(),
                Assignment {
                    id: id.clone(),
                    work_id: work_id.clone(),
                    generation,
                    contract_revision: work.contract_revision,
                    authority_epoch: snapshot.authority_epoch,
                    record_revision: 1,
                    state: AssignmentState::DispatchPending,
                    dispatch_key: dispatch_key.clone(),
                    runtime: None,
                    process_stopped: false,
                    awaiting_request: None,
                    dependency_inputs: inputs,
                    plan: LocalPlan {
                        plan_revision: 0,
                        steps: Vec::new(),
                    },
                    workspace: workspace.clone(),
                    allowed_tools: work.contract.allowed_tools.clone(),
                    effects: BTreeMap::new(),
                    updated_seq: seq,
                },
            );
            snapshot.intents.insert(
                dispatch_key.clone(),
                DispatchIntent {
                    assignment_id: id.clone(),
                    dispatch_key,
                    immutable_spec: spec,
                    spec_hash,
                    receipt: None,
                },
            );
            let work = snapshot.tickets.get_mut(&work_id).expect("work");
            work.generation = generation;
            work.active_assignment = Some(id);
            work.state = WorkState::Active;
            work.current_submission = None;
            touch(work, seq);
        }
        Admitted {
            assignment_id,
            receipt,
        } => {
            authority.runtime()?;
            let a = assignment(snapshot, assignment_id)?.clone();
            let intent = snapshot
                .intents
                .get(&a.dispatch_key)
                .ok_or_else(|| invalid("missing intent"))?;
            if receipt.dispatch_key != a.dispatch_key
                || receipt.spec_hash != intent.spec_hash
                || receipt.run_id.is_empty()
                || receipt.session_id.is_empty()
            {
                return Err(invalid("Runtime receipt does not match immutable dispatch"));
            }
            if let Some(previous) = &a.runtime {
                if previous != receipt {
                    return Err(Error::IdempotencyConflict);
                }
                return Ok(());
            }
            if a.state != AssignmentState::DispatchPending
                || ticket(snapshot, &a.work_id)?.state == WorkState::Cancelled
            {
                return Err(invalid("assignment is not dispatchable"));
            }
            for other in snapshot
                .assignments
                .values()
                .filter(|other| other.id != a.id && holds_resources(other))
            {
                if a.workspace
                    .as_ref()
                    .zip(other.workspace.as_ref())
                    .is_some_and(|(a, b)| !a.claims.is_disjoint(&b.claims))
                {
                    return Err(Error::ResourceBlocked(
                        "workspace claim already owned".into(),
                    ));
                }
                if other
                    .runtime
                    .as_ref()
                    .is_some_and(|r| r.session_id == receipt.session_id)
                {
                    return Err(Error::ResourceBlocked(
                        "Session already has a writable Assignment".into(),
                    ));
                }
            }
            let a = snapshot
                .assignments
                .get_mut(assignment_id)
                .expect("assignment");
            a.runtime = Some(receipt.clone());
            a.state = AssignmentState::Admitted;
            a.record_revision += 1;
            a.updated_seq = seq;
            snapshot
                .intents
                .get_mut(&a.dispatch_key)
                .expect("intent")
                .receipt = Some(receipt.clone());
        }
        Running { assignment_id } => {
            authority.runtime()?;
            let a = assignment(snapshot, assignment_id)?;
            if a.state != AssignmentState::Admitted {
                return Err(invalid("running requires admitted"));
            }
            active_permit(snapshot, a)?;
            let a = snapshot
                .assignments
                .get_mut(assignment_id)
                .expect("assignment");
            a.state = AssignmentState::Running;
            a.record_revision += 1;
            a.updated_seq = seq;
        }
        ReplacePlan {
            assignment_id,
            expected_plan_revision,
            steps,
        } => {
            let a = assignment(snapshot, assignment_id)?;
            authority.worker(a)?;
            active_permit(snapshot, a)?;
            if *expected_plan_revision != a.plan.plan_revision {
                return Err(Error::RevisionConflict);
            }
            validate_plan(steps)?;
            let a = snapshot
                .assignments
                .get_mut(assignment_id)
                .expect("assignment");
            a.plan.steps = steps.clone();
            a.plan.plan_revision += 1;
            a.record_revision += 1;
            a.updated_seq = seq;
        }
        Ask {
            work_id,
            temp_id,
            prompt,
            action,
        } => {
            let work_id = resolve(work_id, ids);
            let work = ticket(snapshot, &work_id)?.clone();
            if work.state == WorkState::Cancelled {
                return Err(invalid("cancelled Work cannot ask"));
            }
            let assignment_id = if matches!(authority.principal, Principal::Worker { .. }) {
                let id = work
                    .active_assignment
                    .clone()
                    .ok_or_else(|| invalid("no active assignment"))?;
                let a = assignment(snapshot, &id)?;
                authority.worker(a)?;
                active_permit(snapshot, a)?;
                Some(id)
            } else {
                authority.supervisor()?;
                work.active_assignment.clone()
            };
            if prompt.trim().is_empty() {
                return Err(invalid("empty request prompt"));
            }
            let kind = match action {
                Some(action) => RequestKind::Approval {
                    action: action.clone(),
                    fingerprint: content_hash(&canonical_bytes(action)?),
                },
                None => RequestKind::Question,
            };
            let id = allocate(temp_id, ids)?;
            snapshot.requests.insert(
                id.clone(),
                PendingRequest {
                    id,
                    work_id,
                    assignment_id,
                    generation: work.generation,
                    contract_revision: work.contract_revision,
                    prompt_revision: 1,
                    kind,
                    prompt: prompt.clone(),
                    status: RequestStatus::Open,
                    answer: None,
                    consumed_attempt: None,
                    worker_packet_hash: None,
                    updated_seq: seq,
                },
            );
        }
        Answer {
            request_id,
            prompt_revision,
            answer,
        } => {
            authority.user()?;
            validate_request(snapshot, request_id, *prompt_revision)?;
            let request = snapshot.requests.get_mut(request_id).expect("request");
            if request.kind != RequestKind::Question {
                return Err(invalid("question answer cannot approve an action"));
            }
            if answer.trim().is_empty() {
                return Err(invalid("empty answer"));
            }
            request.answer = Some(answer.clone());
            request.status = RequestStatus::Answered;
            request.updated_seq = seq;
            let work_id = request.work_id.clone();
            let waiting = snapshot.assignments.values().any(|a| {
                a.work_id == work_id
                    && a.awaiting_request.is_some()
                    && a.process_stopped
                    && a.generation == snapshot.tickets[&work_id].generation
                    && a.contract_revision == snapshot.tickets[&work_id].contract_revision
            });
            let unanswered = snapshot
                .requests
                .values()
                .any(|r| r.work_id == work_id && r.status == RequestStatus::Open);
            let work = snapshot.tickets.get_mut(&work_id).expect("work");
            if waiting
                && !unanswered
                && work.state == WorkState::Blocked
                && !work.paused
                && work.active_assignment.is_none()
            {
                work.state = WorkState::Ready;
                work.blocked = None;
                touch(work, seq);
            }
        }
        YieldForInput {
            assignment_id,
            request_id,
            packet_hash,
        } => {
            let a = assignment(snapshot, assignment_id)?.clone();
            authority.worker(&a)?;
            active_permit(snapshot, &a)?;
            let request_id = resolve(request_id, ids);
            validate_request(snapshot, &request_id, 1)?;
            let r = &snapshot.requests[&request_id];
            if r.kind != RequestKind::Question
                || r.assignment_id.as_deref() != Some(assignment_id)
                || r.work_id != a.work_id
                || packet_hash.len() != 64
                || !packet_hash.bytes().all(|b| b.is_ascii_hexdigit())
            {
                return Err(invalid("question yield binding mismatch"));
            }
            snapshot
                .requests
                .get_mut(&request_id)
                .expect("request")
                .worker_packet_hash = Some(packet_hash.clone());
            let a = snapshot
                .assignments
                .get_mut(assignment_id)
                .expect("assignment");
            a.awaiting_request = Some(request_id);
            a.state = AssignmentState::Blocked;
            a.record_revision += 1;
            a.updated_seq = seq;
            let work = snapshot.tickets.get_mut(&a.work_id).expect("work");
            work.state = WorkState::Blocked;
            work.blocked = Some(BlockReason {
                reason: "awaiting a precise User answer and owned process stop".into(),
                resume_state: WorkState::Ready,
            });
            touch(work, seq);
        }
        DecideApproval {
            request_id,
            prompt_revision,
            fingerprint,
            approve,
        } => {
            authority.user()?;
            validate_request(snapshot, request_id, *prompt_revision)?;
            let request = snapshot.requests.get_mut(request_id).expect("request");
            if !matches!(&request.kind, RequestKind::Approval { fingerprint: actual, .. } if actual == fingerprint)
            {
                return Err(invalid("action fingerprint mismatch"));
            }
            request.status = if *approve {
                RequestStatus::Approved
            } else {
                RequestStatus::Denied
            };
            request.updated_seq = seq;
        }
        ConsumeApproval {
            request_id,
            fingerprint,
            attempt_id,
        } => {
            let request = snapshot
                .requests
                .get(request_id)
                .ok_or_else(|| invalid("request missing"))?
                .clone();
            let a = assignment(
                snapshot,
                request
                    .assignment_id
                    .as_deref()
                    .ok_or_else(|| invalid("approval has no execution binding"))?,
            )?
            .clone();
            authority.worker(&a)?;
            active_permit(snapshot, &a)?;
            if attempt_id.is_empty()
                || !matches!(&request.kind, RequestKind::Approval { fingerprint: actual, .. } if actual == fingerprint)
                || request.generation != a.generation
                || request.contract_revision != a.contract_revision
            {
                return Err(invalid("approval binding mismatch"));
            }
            if request.status == RequestStatus::Consumed
                && request.consumed_attempt.as_deref() == Some(attempt_id)
            {
                return Ok(());
            }
            if request.status != RequestStatus::Approved {
                return Err(invalid("action is not approved or already consumed"));
            }
            let request = snapshot.requests.get_mut(request_id).expect("request");
            request.status = RequestStatus::Consumed;
            request.consumed_attempt = Some(attempt_id.clone());
            request.updated_seq = seq;
            snapshot
                .assignments
                .get_mut(&a.id.clone())
                .expect("assignment")
                .effects
                .insert(
                    attempt_id.clone(),
                    Effect {
                        action_fingerprint: fingerprint.clone(),
                        state: EffectState::Planned,
                        provider_receipt: None,
                    },
                );
        }
        RecordEffect {
            assignment_id,
            attempt_id,
            effect,
        } => {
            let a = assignment(snapshot, assignment_id)?;
            authority.worker(a)?;
            active_permit(snapshot, a)?;
            let old = a
                .effects
                .get(attempt_id)
                .ok_or_else(|| invalid("action attempt was not atomically authorized"))?;
            if old.action_fingerprint != effect.action_fingerprint
                || !valid_effect_transition(old.state, effect.state)
                || (old.state == effect.state && old != effect)
            {
                return Err(invalid("invalid effect transition"));
            }
            if effect.state == EffectState::Succeeded
                && effect.provider_receipt.as_deref().is_none_or(str::is_empty)
            {
                return Err(invalid("successful effect requires provider receipt"));
            }
            let a = snapshot
                .assignments
                .get_mut(assignment_id)
                .expect("assignment");
            a.effects.insert(attempt_id.clone(), effect.clone());
            a.record_revision += 1;
            a.updated_seq = seq;
        }
        Submit {
            assignment_id,
            temp_id,
            artifacts,
            evidence,
        } => {
            let a = assignment(snapshot, assignment_id)?.clone();
            authority.worker(&a)?;
            if evidence.is_empty() || artifacts.is_empty() {
                return Err(invalid("submission requires artifacts and evidence"));
            }
            for artifact in artifacts {
                validate_artifact(artifact)?;
            }
            let work = ticket(snapshot, &a.work_id)?;
            let stale = current_attempt(snapshot, &a).is_err()
                || work.state == WorkState::Blocked
                || dependencies(snapshot, work).ok().as_ref() != Some(&a.dependency_inputs);
            let id = allocate(temp_id, ids)?;
            snapshot.submissions.insert(
                id.clone(),
                Submission {
                    id: id.clone(),
                    work_id: a.work_id.clone(),
                    assignment_id: a.id.clone(),
                    generation: a.generation,
                    contract_revision: a.contract_revision,
                    runtime: a.runtime.clone().expect("verified Runtime"),
                    dependency_inputs: a.dependency_inputs.clone(),
                    artifacts: artifacts.clone(),
                    evidence: evidence.clone(),
                    effects: a.effects.clone(),
                    stale,
                    updated_seq: seq,
                },
            );
            if !stale {
                let work = snapshot.tickets.get_mut(&a.work_id).expect("work");
                work.state = WorkState::Submitted;
                work.current_submission = Some(id);
                work.active_assignment = None;
                touch(work, seq);
                let assignment = snapshot
                    .assignments
                    .get_mut(assignment_id)
                    .expect("assignment");
                assignment.state = AssignmentState::Submitted;
                assignment.record_revision += 1;
                assignment.updated_seq = seq;
                invalidate_requests(snapshot, &a.work_id);
            }
        }
        Accept {
            work_id,
            submission_id,
            evidence,
        } => {
            authority.supervisor()?;
            let work = ticket(snapshot, work_id)?.clone();
            if work.contract.user_acceptance_required {
                authority.user()?;
            }
            let submission = snapshot
                .submissions
                .get(submission_id)
                .ok_or_else(|| invalid("submission missing"))?;
            if work.state != WorkState::Submitted
                || work.current_submission.as_deref() != Some(submission_id)
                || submission.work_id != *work_id
                || submission.stale
                || submission.generation != work.generation
                || submission.contract_revision != work.contract_revision
                || evidence.is_empty()
                || dependencies(snapshot, &work)? != submission.dependency_inputs
                || submission
                    .effects
                    .values()
                    .any(|e| matches!(e.state, EffectState::Started | EffectState::OutcomeUnknown))
            {
                return Err(invalid(
                    "acceptance requires current exact submission and evidence",
                ));
            }
            let work = snapshot.tickets.get_mut(work_id).expect("work");
            work.state = WorkState::Accepted;
            work.accepted_submission = Some(submission_id.clone());
            touch(work, seq);
        }
        AcceptGoal { goal_id, evidence } => {
            authority.supervisor()?;
            let goal = ticket(snapshot, goal_id)?;
            if goal.contract.user_acceptance_required {
                authority.user()?;
            }
            if goal.kind != TicketKind::Goal
                || evidence.is_empty()
                || goal.state == WorkState::Cancelled
                || snapshot
                    .tickets
                    .values()
                    .any(|t| t.parent.as_deref() == Some(goal_id) && t.state != WorkState::Accepted)
            {
                return Err(invalid(
                    "Goal requires its own evidence and accepted children",
                ));
            }
            let goal = snapshot.tickets.get_mut(goal_id).expect("goal");
            goal.state = WorkState::Accepted;
            touch(goal, seq);
        }
        Reject {
            work_id,
            submission_id,
            reason,
        } => {
            authority.supervisor()?;
            let work = snapshot
                .tickets
                .get_mut(work_id)
                .ok_or_else(|| invalid("work missing"))?;
            if work.state != WorkState::Submitted
                || work.current_submission.as_deref() != Some(submission_id)
                || reason.is_empty()
            {
                return Err(invalid("reject requires exact current submission"));
            }
            work.state = WorkState::Ready;
            work.current_submission = None;
            touch(work, seq);
        }
        Cancel { work_id } => {
            authority.supervisor()?;
            let work = snapshot
                .tickets
                .get_mut(work_id)
                .ok_or_else(|| invalid("work missing"))?;
            work.state = WorkState::Cancelled;
            work.paused = false;
            work.blocked = None;
            work.accepted_submission = None;
            touch(work, seq);
            if let Some(id) = &work.active_assignment {
                let a = snapshot.assignments.get_mut(id).expect("assignment");
                if a.state != AssignmentState::OutcomeUnknown {
                    a.state = AssignmentState::Cancelling;
                }
                a.record_revision += 1;
                a.updated_seq = seq;
            }
            invalidate_requests(snapshot, work_id);
            invalidate_dependents(snapshot, work_id);
            invalidate_parent_goals(snapshot, work_id);
        }
        Pause { work_id, reason } => {
            authority.supervisor()?;
            let old = ticket(snapshot, work_id)?.clone();
            if old.kind != TicketKind::Work
                || reason.trim().is_empty()
                || !matches!(
                    old.state,
                    WorkState::Draft | WorkState::Ready | WorkState::Active | WorkState::Blocked
                )
            {
                return Err(invalid("pause requires unfinished Work and a reason"));
            }
            if let Some(id) = &old.active_assignment {
                let a = snapshot.assignments.get_mut(id).expect("assignment");
                if !a.process_stopped {
                    if a.state != AssignmentState::OutcomeUnknown {
                        a.state = AssignmentState::Cancelling;
                    }
                    a.record_revision += 1;
                    a.updated_seq = seq;
                }
            }
            let work = snapshot.tickets.get_mut(work_id).expect("work");
            work.paused = true;
            work.state = WorkState::Blocked;
            work.blocked = Some(BlockReason {
                reason: reason.clone(),
                resume_state: WorkState::Ready,
            });
            touch(work, seq);
            invalidate_requests(snapshot, work_id);
        }
        ConfirmStopped {
            assignment_id,
            effects_reconciled,
        } => {
            authority.runtime()?;
            let a = assignment(snapshot, assignment_id)?.clone();
            if !*effects_reconciled
                || a.effects
                    .values()
                    .any(|e| matches!(e.state, EffectState::Started | EffectState::OutcomeUnknown))
            {
                return Err(Error::ResourceBlocked("effects not reconciled".into()));
            }
            if !matches!(
                a.state,
                AssignmentState::Cancelling
                    | AssignmentState::OutcomeUnknown
                    | AssignmentState::Failed
                    | AssignmentState::Submitted
            ) {
                return Err(invalid(
                    "stop confirmation requires terminal/cancelling/unknown attempt",
                ));
            }
            let a = snapshot
                .assignments
                .get_mut(assignment_id)
                .expect("assignment");
            a.process_stopped = true;
            if a.state != AssignmentState::Submitted {
                a.state = AssignmentState::Cancelled;
            }
            a.record_revision += 1;
            a.updated_seq = seq;
            let work = snapshot.tickets.get_mut(&a.work_id).expect("work");
            if work.active_assignment.as_deref() == Some(assignment_id) {
                work.active_assignment = None;
                if work.state == WorkState::Blocked
                    && !work.paused
                    && work
                        .blocked
                        .as_ref()
                        .is_some_and(|b| b.resume_state == WorkState::Ready)
                {
                    work.state = WorkState::Ready;
                    work.blocked = None;
                }
                touch(work, seq);
            }
        }
        RuntimeStopped {
            assignment_id,
            receipt,
            completed,
        } => {
            authority.runtime()?;
            let a = assignment(snapshot, assignment_id)?.clone();
            if a.runtime.as_ref() != Some(receipt) {
                return Err(Error::ScopeDenied(
                    "stop proof differs from Runtime receipt".into(),
                ));
            }
            let effects_unknown = a.effects.values().any(|effect| {
                matches!(
                    effect.state,
                    EffectState::Started | EffectState::OutcomeUnknown
                )
            });
            let cancelled = ticket(snapshot, &a.work_id)?.state == WorkState::Cancelled;
            let waiting = a.awaiting_request.is_some();
            let unanswered = snapshot
                .requests
                .values()
                .any(|r| r.work_id == a.work_id && r.status == RequestStatus::Open);
            let updated = snapshot
                .assignments
                .get_mut(assignment_id)
                .expect("assignment");
            updated.process_stopped = true;
            updated.record_revision += 1;
            updated.updated_seq = seq;
            if effects_unknown {
                updated.state = AssignmentState::OutcomeUnknown;
            } else if cancelled {
                updated.state = AssignmentState::Cancelled;
            } else if waiting {
                updated.state = AssignmentState::Blocked;
            } else if !completed && updated.state != AssignmentState::Submitted {
                updated.state = AssignmentState::Failed;
            }
            let work = snapshot.tickets.get_mut(&a.work_id).expect("work");
            // A late stopped run affects its own attempt only.
            if work.active_assignment.as_deref() == Some(assignment_id) {
                if waiting
                    && !effects_unknown
                    && !cancelled
                    && a.generation == work.generation
                    && a.contract_revision == work.contract_revision
                {
                    work.active_assignment = None;
                    if !unanswered && !work.paused {
                        work.state = WorkState::Ready;
                        work.blocked = None;
                    }
                    touch(work, seq);
                } else if (cancelled || work.paused) && !effects_unknown {
                    work.active_assignment = None;
                    touch(work, seq);
                } else if effects_unknown || !completed {
                    work.blocked = Some(BlockReason {
                        reason: if effects_unknown {
                            "stopped process has unreconciled external effects"
                        } else {
                            "Runtime stopped without a completed result"
                        }
                        .into(),
                        resume_state: WorkState::Ready,
                    });
                    if !cancelled {
                        work.state = WorkState::Blocked;
                    }
                    touch(work, seq);
                }
            }
        }
        ReconcileCompleted {
            assignment_id,
            receipt,
        } => {
            authority.runtime()?;
            let a = assignment(snapshot, assignment_id)?.clone();
            let work = ticket(snapshot, &a.work_id)?;
            if !a.process_stopped
                || a.runtime.as_ref() != Some(receipt)
                || a.effects.values().any(|effect| {
                    matches!(
                        effect.state,
                        EffectState::Started | EffectState::OutcomeUnknown
                    )
                })
                || work.generation != a.generation
                || work.contract_revision != a.contract_revision
                || work.active_assignment.as_deref() != Some(assignment_id)
                || work.state == WorkState::Cancelled
                || dependencies(snapshot, work)? != a.dependency_inputs
                || !matches!(
                    a.state,
                    AssignmentState::OutcomeUnknown | AssignmentState::Running
                )
            {
                return Err(invalid("canonical completed attempt cannot be reconciled"));
            }
            let a = snapshot
                .assignments
                .get_mut(assignment_id)
                .expect("assignment");
            a.authority_epoch = snapshot.authority_epoch;
            a.state = AssignmentState::Running;
            a.record_revision += 1;
            a.updated_seq = seq;
            let work = snapshot.tickets.get_mut(&a.work_id).expect("work");
            work.state = WorkState::Active;
            work.blocked = None;
            touch(work, seq);
        }
        OutcomeUnknown {
            assignment_id,
            reason,
        } => {
            authority.runtime()?;
            let a = snapshot
                .assignments
                .get_mut(assignment_id)
                .ok_or_else(|| invalid("assignment missing"))?;
            a.state = AssignmentState::OutcomeUnknown;
            a.record_revision += 1;
            a.updated_seq = seq;
            let work = snapshot.tickets.get_mut(&a.work_id).expect("work");
            if work.state != WorkState::Cancelled {
                work.blocked = Some(BlockReason {
                    reason: reason.clone(),
                    resume_state: work.state,
                });
                work.state = WorkState::Blocked;
                touch(work, seq);
            }
        }
        Reopen { work_id } => {
            authority.supervisor()?;
            let old = ticket(snapshot, work_id)?.clone();
            if let Some(id) = &old.active_assignment {
                let a = assignment(snapshot, id)?;
                if !a.process_stopped || holds_resources(a) {
                    return Err(Error::ResourceBlocked(
                        "old execution or unreconciled effects still own resources".into(),
                    ));
                }
            }
            let work = snapshot
                .tickets
                .get_mut(work_id)
                .ok_or_else(|| invalid("work missing"))?;
            work.active_assignment = None;
            work.paused = false;
            work.state = WorkState::Ready;
            work.blocked = None;
            work.accepted_submission = None;
            work.current_submission = None;
            touch(work, seq);
            invalidate_requests(snapshot, work_id);
            invalidate_dependents(snapshot, work_id);
            invalidate_parent_goals(snapshot, work_id);
        }
        Archive {
            ticket_id,
            archived,
        } => {
            authority.supervisor()?;
            let work = snapshot
                .tickets
                .get_mut(ticket_id)
                .ok_or_else(|| invalid("ticket missing"))?;
            work.archived = *archived;
            touch(work, seq);
        }
        ResolveMessage { resolution } => {
            authority.supervisor()?;
            if let Some(previous) = snapshot.resolutions.get(&resolution.message_id) {
                if canonical_bytes(previous)? != canonical_bytes(resolution)? {
                    return Err(Error::IdempotencyConflict);
                }
            } else {
                if snapshot
                    .resolutions
                    .values()
                    .map(|r| r.ingress_seq)
                    .max()
                    .unwrap_or(0)
                    + 1
                    != resolution.ingress_seq
                {
                    return Err(invalid("ingress resolution out of sequence"));
                }
                snapshot
                    .resolutions
                    .insert(resolution.message_id.clone(), resolution.clone());
            }
        }
        Import {
            temp_id,
            contract,
            source,
        } => {
            authority.supervisor()?;
            validate_contract(contract)?;
            if source.session_id.is_empty()
                || source.task_id.is_empty()
                || source.snapshot_hash.len() != 64
            {
                return Err(invalid("import source identity incomplete"));
            }
            if let Some(work) = snapshot.tickets.values().find(|w| {
                w.import_source.as_ref().is_some_and(|s| {
                    s.session_id == source.session_id
                        && s.task_id == source.task_id
                        && s.snapshot_hash == source.snapshot_hash
                })
            }) {
                ids.insert(temp_id.clone(), work.id.clone());
                return Ok(());
            }
            let id = allocate(temp_id, ids)?;
            snapshot.tickets.insert(id.clone(), Ticket { id, scope_id: snapshot.binding.scope_id.clone(), kind: TicketKind::Work,
                parent: None, depends_on: BTreeSet::new(), record_revision: 1, contract_revision: 1, generation: 0, contract: contract.clone(),
                state: WorkState::Blocked, blocked: Some(BlockReason { reason: "imported legacy task requires explicit review; no runtime ownership transferred".into(), resume_state: WorkState::Submitted }),
                paused: false, archived: false, active_assignment: None, current_submission: None, accepted_submission: None, updated_seq: seq, import_source: Some(source.clone()) });
            snapshot.graph_revision += 1;
        }
    }
    Ok(())
}

pub(crate) fn holds_resources(a: &Assignment) -> bool {
    (!a.process_stopped
        || a.effects.values().any(|effect| {
            matches!(
                effect.state,
                EffectState::Started | EffectState::OutcomeUnknown
            )
        }))
        && matches!(
            a.state,
            AssignmentState::DispatchPending
                | AssignmentState::Blocked
                | AssignmentState::Admitted
                | AssignmentState::Running
                | AssignmentState::Cancelling
                | AssignmentState::OutcomeUnknown
                | AssignmentState::Submitted
        )
}

fn validate_contract(contract: &Contract) -> Result<()> {
    if contract.title.trim().is_empty()
        || contract.objective.trim().is_empty()
        || contract.acceptance.is_empty()
    {
        return Err(invalid(
            "contract must include title, objective and acceptance",
        ));
    }
    Ok(())
}

fn validate_request(snapshot: &Snapshot, id: &str, prompt_revision: u64) -> Result<()> {
    let request = snapshot
        .requests
        .get(id)
        .ok_or_else(|| invalid("request missing"))?;
    let work = ticket(snapshot, &request.work_id)?;
    if request.status != RequestStatus::Open
        || request.prompt_revision != prompt_revision
        || request.generation != work.generation
        || request.contract_revision != work.contract_revision
        || work.state == WorkState::Cancelled
    {
        return Err(invalid("request is superseded, answered or expired"));
    }
    Ok(())
}

fn validate_plan(steps: &[LocalStep]) -> Result<()> {
    if steps.len() > 256 {
        return Err(invalid("LocalPlan exceeds 256 Steps"));
    }
    let ids: BTreeSet<_> = steps.iter().map(|s| s.id.as_str()).collect();
    if ids.len() != steps.len() || ids.contains("") {
        return Err(invalid("duplicate/empty Step id"));
    }
    for step in steps {
        if step
            .status
            .is_some_and(|status| (status == StepStatus::Completed) != step.completed)
        {
            return Err(invalid("Step status/completed mismatch"));
        }
        let mut seen = BTreeSet::new();
        let mut next = Some(step.id.as_str());
        while let Some(id) = next {
            if !seen.insert(id) {
                return Err(Error::DependencyCycle);
            }
            let step = steps
                .iter()
                .find(|s| s.id == id)
                .ok_or_else(|| invalid("Step parent outside LocalPlan"))?;
            next = step.parent.as_deref();
        }
    }
    Ok(())
}

fn validate_workspace(workspace: &ExecutionWorkspace) -> Result<()> {
    if workspace.repo.is_empty()
        || workspace.base_commit.len() < 40
        || workspace.branch.is_empty()
        || workspace.claims.is_empty()
    {
        return Err(invalid(
            "code Assignment needs explicit repo/base/branch/resource claims",
        ));
    }
    let root = Path::new(&workspace.worktree).canonicalize()?;
    if !Path::new(&workspace.worktree).is_absolute() {
        return Err(invalid("worktree must be absolute"));
    }
    for path in &workspace.write_roots {
        let path = Path::new(path);
        if !path.is_absolute()
            || path
                .components()
                .any(|c| matches!(c, std::path::Component::ParentDir))
            || !path.canonicalize()?.starts_with(&root)
        {
            return Err(Error::ScopeDenied("write root escapes worktree".into()));
        }
    }
    Ok(())
}

fn validate_artifact(artifact: &Artifact) -> Result<()> {
    if artifact.sha256.len() != 64
        || !artifact.sha256.bytes().all(|b| b.is_ascii_hexdigit())
        || !artifact.uri.starts_with("artifact://")
        || artifact.uri[11..].is_empty()
        || artifact.uri.contains("..")
        || artifact.uri.contains('%')
        || artifact.uri.contains('\\')
    {
        return Err(invalid("artifact URI/hash invalid"));
    }
    Ok(())
}

fn valid_effect_transition(from: EffectState, to: EffectState) -> bool {
    from == to
        || matches!(
            (from, to),
            (
                EffectState::Planned,
                EffectState::Started | EffectState::FailedBeforeEffect
            ) | (
                EffectState::Started,
                EffectState::Succeeded | EffectState::OutcomeUnknown
            )
        )
}

pub(crate) fn validate_snapshot(snapshot: &Snapshot) -> Result<()> {
    for (id, work) in &snapshot.tickets {
        if id != &work.id {
            return Err(invalid("Ticket manifest identity mismatch"));
        }
        if work.scope_id != snapshot.binding.scope_id {
            return Err(Error::ScopeDenied("cross-scope Ticket".into()));
        }
        if work.paused && (work.state != WorkState::Blocked || work.blocked.is_none()) {
            return Err(invalid("paused Work requires an explicit blocked reason"));
        }
        if let Some(parent) = &work.parent {
            let parent = ticket(snapshot, parent)?;
            if !matches!(
                (parent.kind, work.kind),
                (TicketKind::Goal, TicketKind::Goal | TicketKind::Work)
                    | (TicketKind::Work, TicketKind::Step)
                    | (TicketKind::Step, TicketKind::Step)
            ) {
                return Err(invalid("invalid containment kinds"));
            }
        }
        let mut seen = BTreeSet::new();
        let mut next = Some(work.id.as_str());
        while let Some(id) = next {
            if !seen.insert(id) {
                return Err(Error::DependencyCycle);
            }
            next = ticket(snapshot, id)?.parent.as_deref();
        }
        if !work.depends_on.is_empty() && work.kind != TicketKind::Work {
            return Err(invalid("only Work can have execution dependencies"));
        }
        let mut frontier: Vec<_> = work.depends_on.iter().map(String::as_str).collect();
        let mut visited = BTreeSet::new();
        while let Some(id) = frontier.pop() {
            if id == work.id {
                return Err(Error::DependencyCycle);
            }
            if visited.insert(id) {
                let prerequisite = ticket(snapshot, id)?;
                if prerequisite.kind != TicketKind::Work {
                    return Err(invalid("dependency prerequisite must be Work"));
                }
                frontier.extend(prerequisite.depends_on.iter().map(String::as_str));
            }
        }
        if let Some(id) = &work.active_assignment {
            if assignment(snapshot, id)?.work_id != work.id {
                return Err(invalid("active assignment mismatch"));
            }
        }
    }
    for a in snapshot.assignments.values() {
        ticket(snapshot, &a.work_id)?;
        if let Some(id) = &a.awaiting_request {
            let r = snapshot
                .requests
                .get(id)
                .ok_or_else(|| invalid("Assignment question reference missing"))?;
            if r.kind != RequestKind::Question
                || r.assignment_id.as_deref() != Some(a.id.as_str())
                || r.work_id != a.work_id
                || r.generation != a.generation
                || r.contract_revision != a.contract_revision
                || r.worker_packet_hash.as_ref().is_none_or(|h| h.len() != 64)
            {
                return Err(invalid("Assignment question binding invalid"));
            }
        }
        let intent = snapshot
            .intents
            .get(&a.dispatch_key)
            .ok_or_else(|| invalid("Assignment missing atomic DispatchIntent"))?;
        if intent.assignment_id != a.id
            || intent.spec_hash != content_hash(&canonical_bytes(&intent.immutable_spec)?)
        {
            return Err(invalid("DispatchIntent mismatch"));
        }
    }
    Ok(())
}
