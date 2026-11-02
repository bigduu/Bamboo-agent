//! Exact native wait reads. No ActorDirectory inspect/ensure (which can write),
//! transcript load, liveness inference, executable grant or runtime mutation.
use super::super::{Error, Principal, Result, TicketApplication};
use super::patrol_error;
use bamboo_domain::{AgentStatusState, Session, SessionKind};
use bamboo_engine::{
    project_context::{ProjectContextResolver, SessionProjectIdentity},
    ticket_runtime,
    ticket_worker_plan::{TICKET_LOCAL_PLAN_KEY, TICKET_QUESTION_YIELD_KEY},
};
use bamboo_orchestration::patrol::{host, ObservationPort, Scope, WaitKind};
use bamboo_tickets::{AssignmentState, Authority, RequestStatus, TicketService};
use serde_json::{json, Value};
use std::{collections::BTreeMap, sync::Arc};

fn denied() -> Error {
    Error::ScopeDenied("canonical native patrol wait identity changed".into())
}

fn millis(value: chrono::DateTime<chrono::Utc>) -> Result<u64> {
    value.timestamp_millis().try_into().map_err(|_| denied())
}

fn valid_anchor(wait: &host::RuntimeWait, birth_ms: u64, now_ms: u64) -> bool {
    wait.registered_at_ms >= birth_ms
        && wait.registered_at_ms <= now_ms
        && wait
            .timeout_at_ms
            .is_none_or(|at| at > wait.registered_at_ms)
}

fn native_identity(child: &Session, receipt: &bamboo_tickets::RuntimeReceipt) -> Result<bool> {
    let Some(run) = child.metadata.get("runtime.local_task.run_id") else {
        return Ok(false);
    };
    let birth = child
        .metadata
        .get("runtime.local_task.child_birth")
        .and_then(|raw| chrono::DateTime::parse_from_rfc3339(raw).ok())
        .ok_or_else(denied)?;
    if child.kind != SessionKind::Child
        || child.id != receipt.session_id
        || run != &receipt.run_id
        || birth != child.created_at
        || uuid::Uuid::parse_str(run).ok().is_none_or(|id| id.is_nil())
    {
        return Err(denied());
    }
    Ok(child.agent_runtime_state.as_ref().is_some_and(|runtime| {
        // Pipeline execution_id is a distinct UUID, never the Host Run ID.
        runtime.version == bamboo_domain::AGENT_RUNTIME_STATE_VERSION
            && runtime.run_id == receipt.run_id
    }))
}

fn lineage(child: &Session, parent: &Session, assignment: &str) -> Result<()> {
    fn project(session: &Session) -> Result<SessionProjectIdentity> {
        if let (Some(typed), Some(legacy)) = (
            session
                .runtime_metadata
                .as_ref()
                .and_then(|metadata| metadata.project_id.as_ref()),
            session.metadata.get("project_id"),
        ) {
            if typed.trim() != legacy.trim() {
                return Err(denied());
            }
        }
        let identity = ProjectContextResolver::session_project_identity(session);
        if matches!(identity, SessionProjectIdentity::Invalid { .. }) {
            return Err(denied());
        }
        Ok(identity)
    }
    if child.parent_session_id.as_deref() != Some(&parent.id)
        || child.parent_created_at != Some(parent.created_at)
        || child.root_session_id != parent.root_session_id
        || parent.spawn_depth.checked_add(1) != Some(child.spawn_depth)
        || child.metadata.get(TICKET_LOCAL_PLAN_KEY).map(String::as_str) != Some(assignment)
        // Ticket Work Project is independent of the Supervisor's tree Project.
        || project(child)? != project(parent)?
    {
        return Err(denied());
    }
    Ok(())
}

fn fingerprint(child: &Session) -> Result<String> {
    Ok(bamboo_tickets::content_hash(
        &bamboo_tickets::canonical_bytes(&json!({
            "id":child.id,"birth":child.created_at,"parent":child.parent_session_id,
            "parent_birth":child.parent_created_at,"root":child.root_session_id,
            "project":child.project_id_meta(),"launch":child.child_launch_generation(),
        "typed_project":child.runtime_metadata.as_ref().and_then(|metadata| metadata.project_id.as_ref()),
        "legacy_project":child.metadata.get("project_id"),
            "dispatch":ticket_runtime::read_dispatch(child)?,"runtime":child.agent_runtime_state,
        "plan":child.metadata.get(TICKET_LOCAL_PLAN_KEY),
            "state":child.metadata.get("runtime.local_task.state"),
            "run":child.metadata.get("runtime.local_task.run_id"),
            "native_birth":child.metadata.get("runtime.local_task.child_birth"),
            "observed":child.metadata.get("runtime.local_task.observed_at"),
            "instance":child.metadata.get("runtime.local_task.instance"),
            "stop":child.metadata.get("ticket.runtime.owned_stop.v1"),
            "yield":child.metadata.get(TICKET_QUESTION_YIELD_KEY),
            "completion":child.metadata.get("runtime.completion_reason"),
            "last_status":child.last_run_status(),
        }))?,
    ))
}

fn stopped_at(child: &Session, receipt: &bamboo_tickets::RuntimeReceipt) -> Result<u64> {
    let instance = child
        .metadata
        .get("runtime.local_task.instance")
        .and_then(|raw| uuid::Uuid::parse_str(raw).ok())
        .filter(|id| !id.is_nil())
        .ok_or_else(denied)?;
    let raw = child
        .metadata
        .get("ticket.runtime.owned_stop.v1")
        .filter(|raw| raw.len() <= 4096)
        .ok_or_else(denied)?;
    let proof: Value = serde_json::from_str(raw)?;
    if proof["receipt"] != serde_json::to_value(receipt)?
        || proof["child_birth"] != serde_json::to_value(child.created_at)?
        || proof["task_instance"] != instance.to_string()
        || !proof["pid"].is_null()
        || !proof["completed"].is_boolean()
    {
        return Err(denied());
    }
    child
        .metadata
        .get("runtime.local_task.observed_at")
        .and_then(|raw| chrono::DateTime::parse_from_rfc3339(raw).ok())
        .and_then(|at| at.timestamp_millis().try_into().ok())
        .ok_or_else(denied)
}

pub(super) async fn prepare(
    app: &TicketApplication,
    service: &Arc<TicketService>,
    authority: &Authority,
    scope: &Scope,
) -> Result<Option<host::RuntimeObservation>> {
    let mut port = host::read_observation(service, authority, scope).map_err(patrol_error)?;
    let ledger = port.observation(scope, 0).map_err(patrol_error)?;
    if ledger.execution_unknown || ledger.terminal {
        return Ok(None);
    }
    let (commit, snapshot) = service.published()?;
    let assignment = &snapshot.assignments[&scope.assignment_id];
    let Some(receipt) = assignment.runtime.as_ref() else {
        return Ok(None);
    };
    let intent = &snapshot.intents[&assignment.dispatch_key];
    let observed = ticket_runtime::query_dispatch(
        app.storage.as_ref(),
        service,
        &assignment.dispatch_key,
        &intent.immutable_spec,
    )
    .await?;
    match &observed {
        ticket_runtime::DispatchObservation::Admitted { receipt: found }
        | ticket_runtime::DispatchObservation::Terminal { receipt: found, .. }
            if found == receipt => {}
        _ => return Ok(None),
    }
    let Some(child) = app
        .storage
        .load_runtime_control_plane(&receipt.session_id)
        .await?
    else {
        return Ok(None);
    };
    if !native_identity(&child, receipt)? {
        return Ok(None);
    }
    let (_, parent) = ticket_runtime::verified_scope_binding_for_service(
        app.storage.as_ref(),
        service,
        &snapshot.binding.supervisor_session_id,
    )
    .await?;
    lineage(&child, &parent, &assignment.id)?;
    let dispatch = ticket_runtime::read_dispatch(&child)?.ok_or_else(denied)?;
    if dispatch.receipt.as_ref() != Some(receipt)
        || dispatch.receipt_created_at != Some(child.created_at)
        || dispatch.dispatch_key != assignment.dispatch_key
        || dispatch.spec_hash != intent.spec_hash
        || dispatch.assignment_id != assignment.id
        || dispatch.binding != snapshot.binding
        || child.parent_session_id.as_deref() != Some(&snapshot.binding.supervisor_session_id)
    {
        return Err(denied());
    }
    let runtime = child.agent_runtime_state.as_ref().unwrap();
    let evidence_ref = format!("patrol-native-wait-{}", fingerprint(&child)?);
    let native_state = child
        .metadata
        .get("runtime.local_task.state")
        .map(String::as_str);
    let wait = if let Some(request_id) = assignment.awaiting_request.as_ref() {
        let Some(request) = snapshot.requests.get(request_id) else {
            return Err(denied());
        };
        if request.status != RequestStatus::Open
            || assignment.state != AssignmentState::Blocked
            || !assignment.process_stopped
            || native_state != Some("stopped")
            || child.last_run_status().as_deref() != Some("completed")
            || child
                .metadata
                .get("runtime.completion_reason")
                .map(String::as_str)
                != Some("ticket_question_yield")
        {
            return Ok(None);
        }
        let Some(raw) = child
            .metadata
            .get(TICKET_QUESTION_YIELD_KEY)
            .filter(|raw| raw.len() <= 65_536)
        else {
            return Ok(None);
        };
        let original: bamboo_tickets::PendingRequest = serde_json::from_str(raw)?;
        if serde_json::to_value(&original)? != serde_json::to_value(request)? {
            return Err(denied());
        }
        host::RuntimeWait {
            kind: WaitKind::User,
            // Stable owned-stop observation anchors a PATROL DIAGNOSTIC
            // allowance. The Question has no business deadline and stays Open.
            registered_at_ms: stopped_at(&child, receipt)?,
            timeout_at_ms: None,
            request_id: Some(request_id.clone()),
            evidence_ref,
        }
    } else {
        if runtime.status != AgentStatusState::Suspended
            || !runtime
                .suspension
                .as_ref()
                .is_some_and(|state| state.resumable)
            || !matches!(native_state, Some("running" | "stopped"))
            || child.pending_question.is_some()
            || runtime
                .plan_mode
                .as_ref()
                .is_some_and(|plan| plan.status == bamboo_domain::PlanModeStatus::AwaitingApproval)
        {
            return Ok(None);
        }
        if native_state == Some("stopped") {
            let _ = stopped_at(&child, receipt)?;
        }
        let reason = runtime
            .suspension
            .as_ref()
            .map(|state| state.reason.as_str());
        match (&runtime.waiting_for_children, &runtime.waiting_for_bash) {
            (Some(wait), None)
                if reason == Some("waiting_for_children")
                    && !wait.child_session_ids.is_empty()
                    && wait.child_session_ids.len() <= 256 =>
            {
                host::RuntimeWait {
                    kind: WaitKind::Dependency,
                    registered_at_ms: millis(wait.registered_at)?,
                    timeout_at_ms: wait.timeout_at.map(millis).transpose()?,
                    request_id: None,
                    evidence_ref,
                }
            }
            (None, Some(wait))
                if reason == Some("waiting_for_bash")
                    && !wait.bash_ids.is_empty()
                    && wait.bash_ids.len() <= 256 =>
            {
                host::RuntimeWait {
                    kind: WaitKind::RunningTool,
                    registered_at_ms: millis(wait.registered_at)?,
                    timeout_at_ms: wait.timeout_at.map(millis).transpose()?,
                    request_id: None,
                    evidence_ref,
                }
            }
            _ => return Ok(None),
        }
    };
    let now = millis(chrono::Utc::now())?;
    if !valid_anchor(&wait, millis(child.created_at)?, now) {
        return Ok(None);
    }
    // A second compact control-plane read catches run replacement/answer/wait
    // changes during sampling without loading transcript or touching Actor rows.
    let Some(current) = app.storage.load_runtime_control_plane(&child.id).await? else {
        return Err(Error::RevisionConflict);
    };
    if fingerprint(&current)? != fingerprint(&child)? {
        return Err(Error::RevisionConflict);
    }
    let (fresh, _) = app.authority(Principal::Runtime).await?;
    if !Arc::ptr_eq(&fresh, service) || fresh.published()?.0 != commit {
        return Err(Error::RevisionConflict);
    }
    let (_, fresh_parent) = ticket_runtime::verified_scope_binding_for_service(
        app.storage.as_ref(),
        service,
        &snapshot.binding.supervisor_session_id,
    )
    .await?;
    lineage(&current, &fresh_parent, &assignment.id)?;
    Ok(Some(host::RuntimeObservation {
        scope: scope.clone(),
        source_commit: commit,
        receipt: receipt.clone(),
        wait,
    }))
}

pub(super) async fn prepare_selection(
    app: &TicketApplication,
    service: &Arc<TicketService>,
    authority: &Authority,
    selection: &bamboo_orchestration::patrol::program::ProgramSelection<'_>,
) -> Result<BTreeMap<Scope, host::RuntimeObservation>> {
    if selection.work_ids.len() > bamboo_tickets::CLAIM_CANDIDATE_LIMIT {
        return Err(Error::ContextBudgetExceeded);
    }
    let snapshot = service.published()?.1;
    let mut observations = BTreeMap::new();
    for work in selection.work_ids {
        let Some(assignment) =
            host::current_assignment_id(&snapshot, work).map_err(patrol_error)?
        else {
            continue;
        };
        let scope = host::scope_for_assignment(
            service,
            authority,
            selection.project_id.as_str(),
            work,
            &assignment,
        )
        .map_err(patrol_error)?;
        if let Some(observation) = prepare(app, service, authority, &scope).await? {
            observations.insert(scope, observation);
        }
    }
    Ok(observations)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pair() -> (Session, Session) {
        let parent = Session::new("patrol-parent", "controlled");
        let mut child = Session::new_child_of("patrol-child", &parent, "controlled", "Wait");
        child
            .metadata
            .insert(TICKET_LOCAL_PLAN_KEY.into(), "exact-assignment".into());
        (parent, child)
    }

    #[test]
    fn runtime_wait_requires_exact_parent_birth_root_depth_and_plan() {
        let (parent, child) = pair();
        assert!(lineage(&child, &parent, "exact-assignment").is_ok());
        for altered in [
            {
                let mut value = child.clone();
                value.root_session_id = "foreign-root".into();
                value
            },
            {
                let mut value = child.clone();
                value.parent_created_at = None;
                value
            },
            {
                let mut value = child.clone();
                value.spawn_depth += 1;
                value
            },
            {
                let mut value = child.clone();
                value
                    .metadata
                    .insert(TICKET_LOCAL_PLAN_KEY.into(), "foreign-assignment".into());
                value
            },
        ] {
            assert!(lineage(&altered, &parent, "exact-assignment").is_err());
        }
    }

    #[test]
    fn runtime_wait_inherits_parent_project_without_granting_the_work_project() {
        let (mut parent, mut child) = pair();
        let project = bamboo_domain::ProjectId::new().to_string();
        parent.set_project_id_meta(&project);
        child.set_project_id_meta(&project);
        assert!(lineage(&child, &parent, "exact-assignment").is_ok());
        child.set_project_id_meta(bamboo_domain::ProjectId::new().to_string());
        assert!(lineage(&child, &parent, "exact-assignment").is_err());
        child.set_project_id_meta(&project);
        child.metadata.insert(
            "project_id".into(),
            bamboo_domain::ProjectId::new().to_string(),
        );
        assert!(lineage(&child, &parent, "exact-assignment").is_err());
        child.set_project_id_meta("invalid-project");
        assert!(lineage(&child, &parent, "exact-assignment").is_err());
    }

    #[test]
    fn native_wait_run_binding_is_distinct_from_the_pipeline_execution_uuid() {
        let (_, mut child) = pair();
        let receipt = bamboo_tickets::RuntimeReceipt {
            dispatch_key: "dispatch".into(),
            spec_hash: "a".repeat(64),
            session_id: child.id.clone(),
            run_id: uuid::Uuid::new_v4().to_string(),
        };
        child
            .metadata
            .insert("runtime.local_task.run_id".into(), receipt.run_id.clone());
        child.metadata.insert(
            "runtime.local_task.child_birth".into(),
            child.created_at.to_rfc3339(),
        );
        let mut runtime = bamboo_domain::AgentRuntimeState::new(&receipt.run_id);
        runtime.execution_id = Some(uuid::Uuid::new_v4().to_string());
        child.agent_runtime_state = Some(runtime);
        assert!(native_identity(&child, &receipt).unwrap());
        child.agent_runtime_state.as_mut().unwrap().run_id = "unrelated-logical-run".into();
        assert!(!native_identity(&child, &receipt).unwrap());
        child.metadata.insert(
            "runtime.local_task.run_id".into(),
            uuid::Uuid::new_v4().to_string(),
        );
        assert!(native_identity(&child, &receipt).is_err());
    }

    #[test]
    fn unavailable_or_future_wait_time_never_becomes_an_expected_wait() {
        let mut wait = host::RuntimeWait {
            kind: WaitKind::User,
            registered_at_ms: 100,
            timeout_at_ms: None,
            request_id: Some("question".into()),
            evidence_ref: "canonical-stop".into(),
        };
        assert!(valid_anchor(&wait, 90, 100));
        assert!(!valid_anchor(&wait, 90, 99));
        assert!(!valid_anchor(&wait, 101, 102));
        wait.timeout_at_ms = Some(100);
        assert!(!valid_anchor(&wait, 90, 101));
        wait.timeout_at_ms = Some(110);
        assert!(
            valid_anchor(&wait, 90, 111),
            "expired real deadline is diagnostic evidence, never hidden by refreshing time"
        );
    }
}
