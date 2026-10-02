//! Six bounded model tools over the same application service as HTTP.
//! The original executing Supervisor observation is required on every call.
use crate::{
    app_state::ticket_application::TicketApplication,
    handlers::agent::tickets::{ChangesRequest, InspectRequest, SearchRequest},
};
use async_trait::async_trait;
use bamboo_agent_core::tools::{
    Tool, ToolClass, ToolCtx, ToolError, ToolExecutor, ToolOutcome, ToolResult,
};
use bamboo_tickets::*;
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;

mod schema;
#[cfg(test)]
mod tests;

pub const NAMES: [&str; 6] = [
    "work_overview",
    "work_search",
    "work_inspect",
    "work_changes",
    "work_update",
    "work_dispatch",
];

pub fn overlay(
    mut base: Arc<dyn ToolExecutor>,
    app: Arc<TicketApplication>,
) -> Arc<dyn ToolExecutor> {
    if app.service().is_err() {
        return base;
    }
    for name in NAMES {
        base = Arc::new(super::OverlayToolExecutor::new(
            base,
            Arc::new(TicketTool {
                app: app.clone(),
                name,
            }),
        ));
    }
    base
}

pub struct TicketTool {
    app: Arc<TicketApplication>,
    name: &'static str,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Mutation {
    operation_id: String,
    expected_seq: u64,
    expected_epoch: u64,
    operations: Vec<Operation>,
}

impl TicketTool {
    fn mutating(&self) -> bool {
        matches!(self.name, "work_update" | "work_dispatch")
    }

    async fn call(&self, args: Value, ctx: &ToolCtx) -> Result<Value> {
        let caller = ctx
            .session_id()
            .ok_or_else(|| Error::ScopeDenied("executing Supervisor context missing".into()))?;
        let reference = ctx
            .executing_supervisor_for(caller)
            .ok_or_else(|| {
                Error::ScopeDenied("original executing Supervisor lifetime missing".into())
            })?
            .supervisor_reference();
        if self.mutating() && ctx.plan_read_only {
            return Err(Error::ScopeDenied("Plan mode is read-only".into()));
        }
        let principal = Principal::Supervisor {
            session_id: caller.into(),
        };
        let (service, authority) = self.app.authority(principal.clone()).await?;
        let binding = service.published()?.1.binding;
        if binding.scope_id != format!("supervisor/{}", reference.incarnation_id)
            || binding.supervisor_session_id != reference.session_id
        {
            return Err(Error::ScopeDenied(
                "executing Supervisor lifetime changed".into(),
            ));
        }
        match self.name {
            "work_overview" => {
                if args.as_object().is_none_or(|o| !o.is_empty()) {
                    return Err(Error::InvalidTransition(
                        "overview accepts an empty object".into(),
                    ));
                }
                Ok(serde_json::to_value(service.work_overview(&authority)?)?)
            }
            "work_search" => {
                let request: SearchRequest = decode(args)?;
                Ok(serde_json::to_value(service.work_search(
                    &authority,
                    &request.filter,
                    request.limit,
                    request.cursor.as_ref(),
                    request.fixed_commit.as_deref(),
                )?)?)
            }
            "work_inspect" => {
                let request: InspectRequest = decode(args)?;
                Ok(serde_json::to_value(service.work_inspect(
                    &authority,
                    &request.ids,
                    request.depth,
                    request.budget_bytes,
                    request.fixed_commit.as_deref(),
                )?)?)
            }
            "work_changes" => {
                let request: ChangesRequest = decode(args)?;
                Ok(serde_json::to_value(service.work_changes(
                    &authority,
                    request.since_seq,
                    request.limit,
                    request.cursor.as_ref(),
                )?)?)
            }
            "work_update" | "work_dispatch" => {
                let request: Mutation = decode(args)?;
                if self.name == "work_update"
                    && request.operations.iter().any(|op| {
                        !matches!(
                            op,
                            Operation::Create { .. }
                                | Operation::Ready { .. }
                                | Operation::UpdateContract { .. }
                                | Operation::SetDependencies { .. }
                                | Operation::Ask { .. }
                                | Operation::Accept { .. }
                                | Operation::AcceptGoal { .. }
                                | Operation::Reject { .. }
                                | Operation::Reopen { .. }
                                | Operation::Archive { .. }
                        )
                    })
                {
                    return Err(Error::ScopeDenied("model update cannot publish Host ingress, Worker results or User decisions".into()));
                }
                let command = Command {
                    operation_id: request.operation_id,
                    binding,
                    expected_seq: request.expected_seq,
                    expected_epoch: request.expected_epoch,
                    operations: request.operations,
                    source: None,
                };
                if self.name == "work_dispatch" {
                    self.app.dispatch(principal, &command).await
                } else {
                    Ok(
                        json!({"status":"committed", "receipt":self.app.update(principal, &command).await?}),
                    )
                }
            }
            _ => Err(Error::InvalidTransition("unknown Ticket tool".into())),
        }
    }
}

fn decode<T: serde::de::DeserializeOwned>(value: Value) -> Result<T> {
    serde_json::from_value(value)
        .map_err(|e| Error::InvalidTransition(format!("invalid typed arguments: {e}")))
}

#[async_trait]
impl Tool for TicketTool {
    fn name(&self) -> &str {
        self.name
    }
    fn description(&self) -> &str {
        match self.name {
            "work_overview" => "Read exact published Work/Goal counts, pending questions/approvals and acceptance counts. Start here. Snapshot seq/epoch is the CAS base, not an execution permission.",
            "work_search" => "Search authoritative work summaries by lexical text/ID, kind, state, updated seq or archive status. Reuse the fixed commit/cursor across pages; report coverage/truncation.",
            "work_inspect" => "Inspect up to 32 scope IDs, bounded containment depth and byte budget. Read concrete contract revisions, requests and submissions before changing or accepting them.",
            "work_changes" => "Read stable published changes after since_seq. Retain the cursor high watermark; resync_required requires a fresh overview/snapshot.",
            "work_update" => "Commit typed Work/Goal operations with stable operation_id and expected seq/epoch. Independent work contracts do not alter the legacy Task plan. User-required acceptance and approvals are never conferred by a model tool. On 409 refresh and re-evaluate; same input replay returns the original receipt.",
            _ => "Commit start/steer/cancel/retry intents and enqueue through the existing Runtime. Returns accepted_for_dispatch with assignment receipt and admission observations immediately; this is not worker completion or acceptance. Unknown execution is quarantined, never automatically retried.",
        }
    }
    fn parameters_schema(&self) -> Value {
        schema::parameters(self.name)
    }
    fn classify(&self, _: &Value) -> ToolClass {
        if self.mutating() {
            ToolClass::MUTATING_SERIAL
        } else {
            ToolClass::READONLY_PARALLEL
        }
    }
    async fn invoke(
        &self,
        args: Value,
        ctx: ToolCtx,
    ) -> std::result::Result<ToolOutcome, ToolError> {
        let (success, value) = match self.call(args, &ctx).await {
            Ok(value) => (true, value),
            Err(error) => (
                false,
                json!({"error":error.to_string(), "status_code":error.status_code()}),
            ),
        };
        Ok(ToolOutcome::Completed(ToolResult::text(
            success,
            value.to_string(),
        )))
    }
}
