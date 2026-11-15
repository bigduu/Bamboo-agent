//! Request-only model advice for a quiet command with fresh polling evidence.

use serde_json::{json, Value};

const QUIET_COMMAND_ADVISORY_PREFIX: &str = "[runtime_progress_feedback_v1]\n";
const MAX_ID_CHARS: usize = 128;

#[derive(Debug, Clone)]
pub(super) struct QuietCommandEvidence {
    pub session_id: String,
    pub tool_call_id: String,
    pub elapsed_ms: u64,
    pub output_idle_ms: u64,
    pub consecutive_empty_readouts: u64,
}

pub(super) fn render_quiet_command_advisory(facts: QuietCommandEvidence) -> String {
    let (session_id, session_shortened) = bounded_id(&facts.session_id);
    let (tool_call_id, call_shortened) = bounded_id(&facts.tool_call_id);
    ascii_json_packet(json!({
        "schema": "runtime_progress_feedback_v1",
        "source": "runtime",
        "reason": "quiet_command",
        "evidence": {
            "session_id": session_id,
            "tool_call_id": tool_call_id,
            "elapsed_ms": facts.elapsed_ms,
            "output_idle_ms": facts.output_idle_ms,
            "consecutive_empty_readouts": facts.consecutive_empty_readouts,
            "last_status": "running",
            "output_bytes": 0,
            "exit_observed": false,
            "identifiers_shortened": session_shortened || call_shortened,
        },
        "guidance": "Repeated polls returned no new output. Consider waiting for a useful \
                     change, checking another progress signal, or trying a different \
                     approach. Use the exact session ID from the tool result when \
                     collecting the exit status. Silence alone does not establish an exit.",
    }))
}

fn bounded_id(id: &str) -> (String, bool) {
    let mut chars = id.chars();
    let bounded = chars.by_ref().take(MAX_ID_CHARS).collect();
    (bounded, chars.next().is_some())
}

// Escaping non-ASCII scalars preserves the packet's wire representation.
fn ascii_json_packet(value: Value) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let rendered = value.to_string();
    let mut packet = String::from(QUIET_COMMAND_ADVISORY_PREFIX);
    for ch in rendered.chars() {
        if ch.is_ascii() {
            packet.push(ch);
        } else {
            let mut units = [0; 2];
            for &unit in ch.encode_utf16(&mut units).iter() {
                packet.push_str("\\u");
                for shift in [12, 8, 4, 0] {
                    packet.push(char::from(HEX[usize::from((unit >> shift) & 0xf)]));
                }
            }
        }
    }
    packet
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quiet_advisory_keeps_running_evidence_and_bounds_identifiers() {
        let handle = "中🪷".repeat(100);
        let packet = render_quiet_command_advisory(QuietCommandEvidence {
            session_id: handle.clone(),
            tool_call_id: "poll-1".into(),
            elapsed_ms: u64::MAX,
            output_idle_ms: 90_000,
            consecutive_empty_readouts: 6,
        });
        assert!(packet.is_ascii());
        assert!(packet.len() < 4096);
        let value: Value =
            serde_json::from_str(packet.strip_prefix(QUIET_COMMAND_ADVISORY_PREFIX).unwrap())
                .unwrap();
        let evidence = &value["evidence"];
        let bounded = evidence["session_id"].as_str().unwrap();
        assert_eq!(bounded.chars().count(), MAX_ID_CHARS);
        assert!(handle.starts_with(bounded));
        assert_eq!(evidence["identifiers_shortened"], true);
        assert_eq!(evidence["tool_call_id"], "poll-1");
        assert_eq!(evidence["elapsed_ms"], u64::MAX);
        assert_eq!(evidence["last_status"], "running");
        assert_eq!(evidence["exit_observed"], false);
        assert!(evidence.get("exit_code").is_none());
        assert!(value.get("exhaustion").is_none());
    }
}
