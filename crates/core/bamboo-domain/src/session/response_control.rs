//! Durable identity for consumed clarification and permission responses.
//!
//! Provider tool-call ids are not globally unique: a provider may reuse the
//! same id in a later round. Response-control state must therefore bind a
//! consumed response to the concrete tool-result message (and, for typed
//! permissions, its server-issued generation), rather than to the provider id
//! alone.

use serde::{Deserialize, Serialize};

use super::{Message, Session};

/// Legacy id-only ledger retained for reading sessions written by older
/// binaries. New writes also carry [`CONSUMED_RESPONSE_OCCURRENCES_KEY`].
pub const CONSUMED_CLARIFICATION_IDS_KEY: &str = "clarification.consumed_tool_call_ids";

/// Versioned occurrence-aware response ledger.
pub const CONSUMED_RESPONSE_OCCURRENCES_KEY: &str =
    "clarification.consumed_response_occurrences.v1";

/// Identity of one concrete pending-response occurrence.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResponseOccurrence {
    pub tool_call_id: String,
    pub tool_result_message_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub permission_generation: Option<String>,
}

/// Read the typed permission generation preserved on a tool-result message.
///
/// While the request is pending it lives in the synthesized JSON content.
/// Once answered, the response path preserves it in non-model-visible message
/// metadata before replacing that content with the selected response.
pub fn permission_request_generation(message: &Message) -> Option<String> {
    message
        .metadata
        .as_ref()
        .and_then(|metadata| metadata.get("permission_request"))
        .and_then(|request| request.get("request_generation"))
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|generation| !generation.is_empty())
        .map(ToOwned::to_owned)
        .or_else(|| {
            serde_json::from_str::<serde_json::Value>(&message.content)
                .ok()?
                .get("permission_request")?
                .get("request_generation")?
                .as_str()
                .map(str::trim)
                .filter(|generation| !generation.is_empty())
                .map(ToOwned::to_owned)
        })
}

/// Return the latest concrete tool-result occurrence for `tool_call_id`.
pub fn latest_response_occurrence(
    session: &Session,
    tool_call_id: &str,
) -> Option<ResponseOccurrence> {
    let message = session
        .messages
        .iter()
        .rev()
        .find(|message| message.tool_call_id.as_deref() == Some(tool_call_id))?;
    Some(ResponseOccurrence {
        tool_call_id: tool_call_id.to_string(),
        tool_result_message_id: message.id.clone(),
        permission_generation: permission_request_generation(message),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reused_tool_call_id_has_distinct_occurrence_and_generation() {
        let mut session = Session::new("session", "model");
        for (message_id, generation) in [("result-old", "old"), ("result-new", "new")] {
            let mut result = Message::tool_result(
                "reused",
                serde_json::json!({
                    "permission_request": { "request_generation": generation }
                })
                .to_string(),
            );
            result.id = message_id.to_string();
            session.add_message(result);
        }

        assert_eq!(
            latest_response_occurrence(&session, "reused"),
            Some(ResponseOccurrence {
                tool_call_id: "reused".to_string(),
                tool_result_message_id: "result-new".to_string(),
                permission_generation: Some("new".to_string()),
            })
        );
    }
}
