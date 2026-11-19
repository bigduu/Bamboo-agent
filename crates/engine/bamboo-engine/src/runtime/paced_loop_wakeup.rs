//! The scheduled Root can propose a delay only from its live provider call.

use std::{cell::RefCell, future::Future, sync::Arc};

use bamboo_agent_core::tools::{Tool, ToolExecutor};
use bamboo_domain::{
    RootActorRuntimeWrite, Session, SessionMessageEnvelope, PACED_LOOP_WAKEUP_TOOL_NAME,
};
use serde::Deserialize;

use super::config::AgentLoopConfig;
use super::managers::lifecycle::ordinary_standard_root_writer;

pub const PACED_LOOP_WAKEUP_EXECUTION_CONTRACT: &str = "bamboo.paced_loop.wakeup.v1";

#[derive(Clone)]
pub(crate) struct PacedLoopTurn {
    pub system_guidance: Option<Arc<str>>,
    pub adaptive_wakeup: bool,
}

tokio::task_local! {
    static WAKEUP_WRITER: RefCell<Option<(String, RootActorRuntimeWrite)>>;
}

pub(crate) fn scope_paced_loop_wakeup_writer<F: Future>(
    writer: Option<RootActorRuntimeWrite>,
    tool_call_id: &str,
    dispatch: F,
) -> impl Future<Output = F::Output> {
    let selected = writer.map(|writer| (tool_call_id.to_owned(), writer));
    // Keep the extra token and its future out of the enclosing dispatch
    // frames, including when the empty scope shadows a nested invocation.
    Box::pin(WAKEUP_WRITER.scope(RefCell::new(selected), dispatch))
}

/// Consume the exact sanctioned adaptive tool call's writer. The Host must
/// still match it to the current persisted iteration before any mutation.
pub fn take_paced_loop_wakeup_writer(tool_call_id: &str) -> Option<RootActorRuntimeWrite> {
    WAKEUP_WRITER
        .try_with(|selected| {
            let mut selected = selected.borrow_mut();
            if selected.as_ref()?.0 == tool_call_id {
                selected.take().map(|(_, writer)| writer)
            } else {
                None
            }
        })
        .ok()
        .flatten()
}

pub(crate) fn registered_paced_loop_wakeup_tool(tools: &dyn ToolExecutor) -> Option<Arc<dyn Tool>> {
    let owner = tools.exact_tool_owner(PACED_LOOP_WAKEUP_TOOL_NAME)?;
    if owner.local_execution_contract(PACED_LOOP_WAKEUP_TOOL_NAME)
        != Some(PACED_LOOP_WAKEUP_EXECUTION_CONTRACT)
    {
        return None;
    }
    owner
        .local_tool_instance(PACED_LOOP_WAKEUP_TOOL_NAME)
        .filter(|tool| {
            tool.name() == PACED_LOOP_WAKEUP_TOOL_NAME
                && tool.execution_contract() == Some(PACED_LOOP_WAKEUP_EXECUTION_CONTRACT)
        })
}

pub(crate) fn paced_loop_wakeup_writer(
    config: &AgentLoopConfig,
    session: &Session,
) -> Option<RootActorRuntimeWrite> {
    if !config.paced_loop_turn.as_ref()?.adaptive_wakeup {
        return None;
    }
    let writer = ordinary_standard_root_writer(config, session)?;
    let receipt = config.persistence.as_ref()?.root_inbox_receipt()?;
    if session
        .session_inbox_admission()?
        .receipt_for(receipt.id.as_str())
        .as_ref()
        != Some(receipt)
    {
        return None;
    }
    let message = session
        .messages
        .iter()
        .rev()
        .find(|message| message.id == receipt.id.as_str())?;
    let marker = message.metadata.as_ref()?.get("session_message")?;
    let envelope = SessionMessageEnvelope::deserialize(marker).ok()?;
    if envelope.target_session_id != session.id
        || envelope.paced_loop_run_id() != Some(writer.fence.run_id.as_str())
        || !bamboo_domain::is_matching_session_message(message, &envelope)
    {
        return None;
    }
    Some(writer)
}
