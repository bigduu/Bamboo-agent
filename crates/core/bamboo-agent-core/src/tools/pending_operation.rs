//! Host-owned binding of a parked operation, independent of live aliases.

use bamboo_domain::{Role, Session, SessionCallerReference};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::ToolCall;

pub const PENDING_TOOL_OPERATION_METADATA_KEY: &str = "permission.operation.v1";

/// The original bytes and selected owner are retained together. This record
/// cannot grant permissions or reconstruct a live cross-Session capability.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PendingToolOperation {
    version: u8,
    caller: SessionCallerReference,
    result_message_id: String,
    tool_call: ToolCall,
    execution_name: String,
    request_generation: String,
}

impl PendingToolOperation {
    pub fn capture(
        caller: SessionCallerReference,
        result_message_id: &str,
        tool_call: &ToolCall,
        execution_name: &str,
        request_generation: &str,
    ) -> Self {
        Self {
            version: 1,
            caller,
            result_message_id: result_message_id.into(),
            tool_call: tool_call.clone(),
            execution_name: execution_name.into(),
            request_generation: request_generation.into(),
        }
    }

    pub fn execution_name(&self) -> &str {
        &self.execution_name
    }

    /// Original host-captured identity, available only after `restore` validates
    /// the retained occurrence. It must never be refreshed from current config.
    pub fn caller(&self) -> &SessionCallerReference {
        &self.caller
    }

    pub fn metadata_value(&self) -> Value {
        serde_json::to_value(self).expect("pending operation binding serializes")
    }

    /// Only after validating the previous generation and the next typed gate.
    /// Preserve the original caller, spelling and argument bytes through repark.
    pub fn for_generation(&self, generation: &str) -> Self {
        let mut next = self.clone();
        next.request_generation = generation.into();
        next
    }

    pub fn restore(
        session: &Session,
        result_index: usize,
        tool_call: &ToolCall,
        request_generation: Option<&str>,
    ) -> Result<Option<Self>, &'static str> {
        let message = session
            .messages
            .get(result_index)
            .ok_or("result occurrence missing")?;
        if serde_json::from_str::<Value>(&message.content)
            .ok()
            .is_some_and(|payload| payload.get(PENDING_TOOL_OPERATION_METADATA_KEY).is_some())
        {
            return Err("pending operation binding is not accepted from a result payload");
        }
        let Some(value) = message
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.get(PENDING_TOOL_OPERATION_METADATA_KEY))
        else {
            return Ok(None);
        };
        let record: Self = serde_json::from_value(value.clone())
            .map_err(|_| "pending operation binding is malformed")?;
        let latest_result = session
            .messages
            .iter()
            .rposition(|message| message.tool_call_id.as_deref() == Some(tool_call.id.as_str()));
        let newer_call = session.messages[result_index + 1..].iter().any(|message| {
            message
                .tool_calls
                .as_ref()
                .is_some_and(|calls| calls.iter().any(|call| call.id == tool_call.id))
        });
        let preceding_call = session.messages[..result_index]
            .iter()
            .rev()
            .find(|message| {
                message
                    .tool_calls
                    .as_ref()
                    .is_some_and(|calls| calls.iter().any(|call| call.id == tool_call.id))
            });
        let exact_call = preceding_call.is_some_and(|message| {
            message.role == Role::Assistant
                && message.tool_calls.as_ref().is_some_and(|calls| {
                    let mut matching = calls.iter().filter(|call| call.id == tool_call.id);
                    matching.next() == Some(tool_call) && matching.next().is_none()
                })
        });
        if record.version != 1
            || !record.caller.matches_session(session)
            || record.result_message_id != message.id
            || record.tool_call != *tool_call
            || record.execution_name.trim().is_empty()
            || record.request_generation.trim().is_empty()
            || Some(record.request_generation.as_str()) != request_generation
            || message.role != Role::Tool
            || latest_result != Some(result_index)
            || session
                .messages
                .iter()
                .filter(|message| message.id == record.result_message_id)
                .count()
                != 1
            || newer_call
            || !exact_call
        {
            return Err("pending operation binding does not match the original caller/operation; resend explicitly");
        }
        Ok(Some(record))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_domain::{
        FunctionCall, Message, SessionAuthorityIdentity, DEFAULT_SUPERVISOR_SESSION_ID,
    };

    fn parked(mut session: Session, version: Option<u32>) -> (Session, ToolCall) {
        session.session_version = version;
        let call = ToolCall {
            id: "one-operation".into(),
            tool_type: "function".into(),
            function: FunctionCall {
                name: "default::spawnSession".into(),
                arguments: "{ \"prompt\": \"same bytes, including whitespace\" }".into(),
            },
        };
        let caller = SessionCallerReference::from(&session);
        session.add_message(Message::assistant("", Some(vec![call.clone()])));
        let mut result = Message::tool_result(&call.id, "awaiting approval");
        result.metadata = Some(serde_json::json!({
            PENDING_TOOL_OPERATION_METADATA_KEY: PendingToolOperation::capture(
                caller, &result.id, &call, "SubAgent", "generation-1").metadata_value()
        }));
        session.add_message(result);
        (session, call)
    }

    #[test]
    fn durable_binding_retains_original_bytes_for_all_roles_and_session_versions() {
        for version in [None, Some(1), Some(2)] {
            let root = Session::new("ordinary", "model");
            let child = Session::new_child_of("child", &root, "model", "short task");
            let mut supervisor = Session::new(DEFAULT_SUPERVISOR_SESSION_ID, "model");
            supervisor.authority_identity = SessionAuthorityIdentity::Supervisor {
                incarnation_id: uuid::Uuid::new_v4(),
            };
            for session in [root, child, supervisor] {
                let (session, call) = parked(session, version);
                let restored: Session =
                    serde_json::from_value(serde_json::to_value(&session).unwrap()).unwrap();
                let operation =
                    PendingToolOperation::restore(&restored, 1, &call, Some("generation-1"))
                        .unwrap()
                        .unwrap();
                assert_eq!(operation.tool_call, call);
                assert_eq!(operation.execution_name(), "SubAgent");
                assert!(operation.caller.matches_session(&restored));
            }
        }
    }

    #[test]
    fn binding_rejects_argument_rewrite_generation_or_caller_replacement() {
        let (session, call) = parked(Session::new("root", "model"), None);
        let mut changed = session.clone();
        changed.messages[0].tool_calls.as_mut().unwrap()[0]
            .function
            .arguments = "{}".into();
        let rewritten = changed.messages[0].tool_calls.as_ref().unwrap()[0].clone();
        assert!(
            PendingToolOperation::restore(&changed, 1, &rewritten, Some("generation-1")).is_err()
        );
        assert!(PendingToolOperation::restore(&session, 1, &call, Some("generation-2")).is_err());
        let mut changed = session.clone();
        changed.created_at += chrono::Duration::seconds(1);
        assert!(PendingToolOperation::restore(&changed, 1, &call, Some("generation-1")).is_err());
    }

    #[test]
    fn binding_cannot_be_moved_to_a_new_result_or_reused_call_occurrence() {
        let (session, call) = parked(Session::new("root", "model"), Some(2));
        let mut changed = session.clone();
        changed.messages[1].id = "different-result".into();
        assert!(PendingToolOperation::restore(&changed, 1, &call, Some("generation-1")).is_err());
        let mut changed = session.clone();
        changed.add_message(Message::assistant("", Some(vec![call.clone()])));
        assert!(PendingToolOperation::restore(&changed, 1, &call, Some("generation-1")).is_err());
        changed.add_message(session.messages[1].clone());
        assert!(PendingToolOperation::restore(&changed, 1, &call, Some("generation-1")).is_err());
        assert!(PendingToolOperation::restore(&changed, 3, &call, Some("generation-1")).is_err());
        // Duplicate durable result IDs cannot identify one exact occurrence.
        changed.messages[3].id = "new-occurrence".into();
        assert!(PendingToolOperation::restore(&changed, 3, &call, Some("generation-1")).is_err());
    }

    #[test]
    fn result_payload_may_not_supply_the_host_binding() {
        let (mut session, call) = parked(Session::new("root", "model"), None);
        let metadata = session.messages[1].metadata.take().unwrap();
        session.messages[1].content = metadata.to_string();
        assert!(PendingToolOperation::restore(&session, 1, &call, Some("generation-1")).is_err());
        session.messages[1].content = "legacy readable history".into();
        assert!(PendingToolOperation::restore(&session, 1, &call, None)
            .unwrap()
            .is_none());
    }

    #[test]
    fn next_generation_retains_original_caller_owner_and_argument_bytes() {
        let (mut session, call) = parked(Session::new("root", "model"), Some(1));
        let operation = PendingToolOperation::restore(&session, 1, &call, Some("generation-1"))
            .unwrap()
            .unwrap();
        let next = operation.for_generation("generation-2");
        assert_eq!(next.tool_call, operation.tool_call);
        assert_eq!(next.caller, operation.caller);
        assert_eq!(next.execution_name(), operation.execution_name());
        session.messages[1].metadata.as_mut().unwrap()[PENDING_TOOL_OPERATION_METADATA_KEY] =
            next.metadata_value();
        assert!(PendingToolOperation::restore(&session, 1, &call, Some("generation-1")).is_err());
        assert_eq!(
            PendingToolOperation::restore(&session, 1, &call, Some("generation-2")).unwrap(),
            Some(next)
        );
    }
}
