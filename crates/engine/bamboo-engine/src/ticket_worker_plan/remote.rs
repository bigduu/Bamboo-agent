//! Native Worker projection over the existing bounded HostBridge callback lane.
//! Host revalidates the live Run and service authority on every read/write.
use super::*;
use bamboo_subagent::executor::HostBridge;
use bamboo_tickets::WorkContextPacket;
use serde::{Deserialize, Serialize};

pub const TICKET_BOOTSTRAP_POSTURE_PENDING: &str = "ticket_bootstrap_posture_pending";

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanProjection {
    pub assignment_id: String,
    pub generation: u64,
    pub contract_revision: u64,
    pub packet_hash: String,
    pub binding: bamboo_tickets::ScopeBinding,
    pub task_list: Option<TaskList>,
    pub plan_revision: u64,
}

pub struct RemoteWorkerPlan {
    host: HostBridge,
    packet: WorkContextPacket,
    session_id: String,
    run_id: String,
    initial: PlanProjection,
}

impl RemoteWorkerPlan {
    pub async fn from_run(
        run: &bamboo_subagent::proto::RunSpec,
        host: Option<HostBridge>,
    ) -> Result<Option<Self>> {
        let mut packets = run
            .messages
            .iter()
            .filter(|m| m.get("role").and_then(|v| v.as_str()) == Some("system"))
            .filter_map(|m| m.get("metadata")?.get(TICKET_PLAN_PACKET_KEY));
        let Some(raw) = packets.next() else {
            return Ok(None);
        };
        if packets.next().is_some() {
            return Err(Error::ScopeDenied("multiple LocalPlan packets".into()));
        }
        let packet: WorkContextPacket = serde_json::from_value(raw.clone())?;
        let identity = run
            .logical_session
            .as_ref()
            .filter(|i| i.creation.is_some())
            .ok_or_else(|| {
                Error::ScopeDenied("native LocalPlan needs exact Child creation".into())
            })?;
        if identity.parent_session_id.as_deref() != Some(&packet.binding.supervisor_session_id) {
            return Err(Error::ScopeDenied(
                "native LocalPlan Supervisor differs".into(),
            ));
        }
        let run_id = run
            .activation_run_id
            .clone()
            .filter(|id| !id.is_empty())
            .ok_or_else(|| Error::ScopeDenied("native LocalPlan needs Host Run".into()))?;
        let host = host.ok_or_else(|| {
            Error::AuthorityUnavailable("native LocalPlan HostBridge missing".into())
        })?;
        Self::from_host_packet(host, packet, identity.session_id.clone(), run_id)
            .await
            .map(Some)
    }

    pub async fn from_host_packet(
        host: HostBridge,
        packet: WorkContextPacket,
        session_id: String,
        run_id: String,
    ) -> Result<Self> {
        if session_id.is_empty()
            || run_id.is_empty()
            || !packet.contract.allowed_tools.contains("Task")
        {
            return Err(Error::ScopeDenied(
                "invalid native LocalPlan binding".into(),
            ));
        }
        // Events and callback controls use distinct transport lanes. A queued
        // posture event may arrive after this read. Retry only the Host's exact
        // pending-posture response, never a mutation or general authority error.
        let request = serde_json::json!({(TICKET_PLAN_ACTION): {"read":true}});
        let mut attempts = 0;
        let data = loop {
            match host
                .subagent_call(request.clone(), "ticket-plan-bootstrap")
                .await
            {
                Ok(data) => break data,
                Err(error) if error == TICKET_BOOTSTRAP_POSTURE_PENDING && attempts < 16 => {
                    attempts += 1;
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
                Err(error) => return Err(Error::AuthorityUnavailable(error)),
            }
        };
        let initial: PlanProjection = serde_json::from_value(data)?;
        if initial.assignment_id != packet.assignment_id
            || initial.generation != packet.generation
            || initial.contract_revision != packet.contract_revision
            || initial.binding != packet.binding
            || initial.packet_hash != packet_fingerprint(&packet)?
        {
            return Err(Error::ScopeDenied(
                "LocalPlan packet differs from Host capability".into(),
            ));
        }
        if initial
            .task_list
            .as_ref()
            .is_some_and(|plan| plan.session_id != session_id)
        {
            return Err(Error::ScopeDenied("Host returned another plan".into()));
        }
        Ok(Self {
            host,
            packet,
            session_id,
            run_id,
            initial,
        })
    }

    fn validate(&self, session: &Session) -> Result<()> {
        if session.kind != SessionKind::Child
            || session.id != self.session_id
            || session.parent_session_id.as_deref()
                != Some(&self.packet.binding.supervisor_session_id)
            || session
                .agent_runtime_state
                .as_ref()
                .is_none_or(|s| s.run_id != self.run_id)
        {
            return Err(Error::ScopeDenied(
                "native LocalPlan identity changed".into(),
            ));
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl WorkerLocalPlan for RemoteWorkerPlan {
    fn run_id(&self) -> &str {
        &self.run_id
    }
    fn bind_session(&self, session: &mut Session) -> Result<()> {
        self.validate(session)?;
        session.metadata.insert(
            TICKET_LOCAL_PLAN_KEY.into(),
            self.packet.assignment_id.clone(),
        );
        session.task_list = self.initial.task_list.clone();
        session.set_task_list_version_meta(self.initial.plan_revision.to_string());
        Ok(())
    }
    async fn apply_task(
        &self,
        session: &mut Session,
        call_id: &str,
        args: &serde_json::Value,
    ) -> Result<TaskList> {
        self.validate(session)?;
        if session.metadata.get(TICKET_LOCAL_PLAN_KEY) != Some(&self.packet.assignment_id) {
            return Err(Error::ScopeDenied(
                "native LocalPlan capability missing".into(),
            ));
        }
        let data = self
            .host
            .subagent_call(
                serde_json::json!({(TICKET_PLAN_ACTION): {"task":args}}),
                call_id,
            )
            .await
            .map_err(Error::AuthorityUnavailable)?;
        let projection: PlanProjection = serde_json::from_value(data)?;
        if projection.assignment_id != self.packet.assignment_id
            || projection.generation != self.packet.generation
            || projection.contract_revision != self.packet.contract_revision
            || projection.binding != self.packet.binding
            || projection.packet_hash != packet_fingerprint(&self.packet)?
        {
            return Err(Error::ScopeDenied(
                "Host LocalPlan capability changed".into(),
            ));
        }
        let plan = projection
            .task_list
            .filter(|p| p.session_id == self.session_id)
            .ok_or_else(|| Error::ScopeDenied("Host omitted the exact plan".into()))?;
        if projection.plan_revision
            < session
                .task_list_version_meta()
                .and_then(|v| v.parse().ok())
                .unwrap_or(0)
        {
            return Err(Error::RevisionConflict);
        }
        session.task_list = Some(plan.clone());
        session.set_task_list_version_meta(projection.plan_revision.to_string());
        Ok(plan)
    }
}

/// Caller is a Host-loaded, live, creation-fenced Session. JSON only contains a
/// bounded Task payload; all execution identity comes from Host state.
pub fn apply_host_plan_request(
    service: &TicketService,
    caller: &Session,
    run_id: &str,
    args: &serde_json::Value,
    call_id: &str,
) -> Result<PlanProjection> {
    let body = args
        .as_object()
        .filter(|o| o.len() == 1)
        .and_then(|o| o.get(TICKET_PLAN_ACTION))
        .and_then(|v| v.as_object())
        .filter(|o| o.len() == 1)
        .ok_or_else(|| Error::InvalidTransition("invalid private Task request".into()))?;
    let assignment = caller
        .metadata
        .get(TICKET_LOCAL_PLAN_KEY)
        .ok_or_else(|| Error::ScopeDenied("Child has no LocalPlan capability".into()))?;
    let plan = TicketWorkerPlan::from_runtime_receipt(Arc::new(service.clone()), assignment)?;
    if plan.run_id() != run_id || plan.session_id != caller.id {
        return Err(Error::ScopeDenied(
            "Host Run differs from admission receipt".into(),
        ));
    }
    let mut projection = caller.clone();
    projection
        .agent_runtime_state
        .get_or_insert_with(Default::default)
        .run_id = run_id.into();
    plan.bind_session(&mut projection)?;
    let packet_hash =
        packet_fingerprint(&service.child_context_packet(&plan.authority, assignment, 65536)?)?;
    if body.get("read") != Some(&serde_json::Value::Bool(true)) {
        let task = body
            .get("task")
            .ok_or_else(|| Error::InvalidTransition("Task payload missing".into()))?;
        // Refuse a plan that cannot fit the existing bounded callback reply
        // before committing it. A transport budget must not become a partial
        // success in which the Host writes but the Worker cannot read its plan.
        let mut candidate = TaskTool::task_list_from_args_with_existing(
            task,
            &caller.id,
            projection.task_list.as_ref(),
            None,
        )
        .map_err(|e| Error::InvalidTransition(e.to_string()))?;
        let snapshot = service.published()?.1;
        let a = &snapshot.assignments[assignment];
        candidate.title = format!(
            "Local plan: {}",
            snapshot.tickets[&a.work_id].contract.title
        );
        let candidate = PlanProjection {
            assignment_id: assignment.clone(),
            generation: a.generation,
            contract_revision: a.contract_revision,
            packet_hash: packet_hash.clone(),
            binding: snapshot.binding,
            task_list: Some(candidate),
            plan_revision: a.plan.plan_revision.saturating_add(1),
        };
        if serde_json::to_vec(&serde_json::json!({"result":candidate}))?.len() > 16 * 1024 - 256 {
            return Err(Error::InvalidTransition(
                "native LocalPlan exceeds HostBridge reply budget".into(),
            ));
        }
        plan.apply_task(&mut projection, call_id, task)?;
    }
    let plan_revision = projection
        .task_list_version_meta()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let snapshot = service.published()?.1;
    let a = &snapshot.assignments[assignment];
    Ok(PlanProjection {
        assignment_id: assignment.clone(),
        generation: a.generation,
        contract_revision: a.contract_revision,
        packet_hash,
        binding: snapshot.binding,
        task_list: projection.task_list,
        plan_revision,
    })
}

fn packet_fingerprint(packet: &WorkContextPacket) -> Result<String> {
    Ok(bamboo_tickets::content_hash(
        &bamboo_tickets::canonical_bytes(packet)?,
    ))
}
