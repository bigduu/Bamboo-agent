//! Recognize unchanged, successful Child inspection batches before compression.
//! This is a handoff to the existing durable wait, never a child timeout/cancel.
use std::collections::{BTreeMap, BTreeSet};

use bamboo_agent_core::tools::{parse_tool_args, ToolCall, ToolResult};
use sha2::{Digest, Sha256};

const UNCHANGED_ROUNDS: usize = 3;
const MAX_OBSERVATIONS: usize = 80;

#[derive(Debug, Clone, Default)]
pub(super) struct ChildInspectionProgress {
    current: Option<BTreeMap<[u8; 32], ChildObservation>>,
    previous: BTreeMap<[u8; 32], ChildObservation>,
    last_round: Option<usize>,
}

#[derive(Debug, Clone)]
struct ChildObservation {
    version: [u8; 32],
    target: String,
    unchanged: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_agent_core::tools::FunctionCall;
    use serde_json::json;

    fn call(name: &str, args: serde_json::Value) -> ToolCall {
        ToolCall {
            id: "new-call-id".into(),
            tool_type: "function".into(),
            function: FunctionCall {
                name: name.into(),
                arguments: args.to_string(),
            },
        }
    }

    #[test]
    fn child_inspection_wait_ignores_transport_churn_but_keeps_real_progress_and_paging() {
        let mut guard = ChildInspectionProgress::default();
        let inspect = call("SubAgent", json!({"intent":"inspect", "target":"child"}));
        for round in 0..3 {
            guard.begin_round(round);
            guard.observe(
                &inspect,
                &Ok(ToolResult::text(
                    true,
                    json!({
                        "actor_id":"child", "observed_status":"running", "message_count":4,
                        "last_event_at":round, "heartbeat":{"sequence":round},
                        "guidance":format!("running for {round} seconds"),
                    })
                    .to_string(),
                )),
                Some("SubAgent"),
            );
            assert_eq!(
                guard.finish_round(),
                if round == 2 {
                    vec!["child".to_string()]
                } else {
                    vec![]
                }
            );
        }
        for round in 3..9 {
            guard.begin_round(round);
            let paged = call(
                "SubAgent",
                json!({"intent":"inspect", "target":"child", "message":format!("{{\"view\":\"messages\",\"cursor\":\"page-{round}\"}}") }),
            );
            guard.observe(
                &paged,
                &Ok(ToolResult::text(
                    true,
                    json!({"actor_id":"child","message_count":round}).to_string(),
                )),
                Some("SubAgent"),
            );
            assert!(
                guard.finish_round().is_empty(),
                "new evidence/pages must remain readable"
            );
        }
    }

    #[test]
    fn child_inspection_wait_never_treats_failed_foreign_or_mixed_calls_as_a_quiet_batch() {
        let inspect = call("SubAgent", json!({"intent":"inspect", "target":"child"}));
        let output = Ok(ToolResult::text(
            true,
            json!({"actor_id":"child", "observed_status":"running"}).to_string(),
        ));
        let mut guard = ChildInspectionProgress::default();
        for round in 0..12 {
            guard.begin_round(round);
            guard.observe(&inspect, &output, Some("SubAgent"));
            match round % 4 {
                0 => guard.observe(&inspect, &Err("denied".into()), Some("SubAgent")),
                1 => guard.observe(&inspect, &output, Some("vendor::SubAgent")),
                2 => guard.observe(
                    &call("SubAgent", json!({"message":"continue work"})),
                    &output,
                    Some("SubAgent"),
                ),
                _ => guard.observe(
                    &call("session_control", json!({"action":"directory"})),
                    &output,
                    Some("session_control"),
                ),
            }
            assert!(guard.finish_round().is_empty());
        }
    }

    #[test]
    fn child_inspection_wait_alternating_targets_and_equivalent_selectors_cannot_refund_polling() {
        let mut guard = ChildInspectionProgress::default();
        for round in 0..7 {
            let target = ["a", "b", "c"][round % 3];
            let args = if round < 3 {
                json!({"intent":"inspect","target":target})
            } else {
                json!({"intent":"inspect","target":target,"message":"overview"})
            };
            guard.begin_round(round);
            guard.observe(
                &call("SubAgent", args),
                &Ok(ToolResult::text(
                    true,
                    json!({
                        "actor_id":target,"observed_status":"running"
                    })
                    .to_string(),
                )),
                Some("SubAgent"),
            );
            assert_eq!(
                guard.finish_round(),
                if round == 6 {
                    vec!["a".to_string(), "b".to_string(), "c".to_string()]
                } else {
                    vec![]
                }
            );
        }
    }
}

impl ChildInspectionProgress {
    pub(super) fn begin_round(&mut self, round: usize) {
        if self
            .last_round
            .is_some_and(|last| last.checked_add(1) != Some(round))
        {
            self.previous.clear();
        }
        self.last_round = Some(round);
        self.current = Some(BTreeMap::new());
    }

    pub(super) fn observe(
        &mut self,
        call: &ToolCall,
        outcome: &Result<ToolResult, String>,
        admitted_name: Option<&str>,
    ) {
        if self.current.is_none() {
            return;
        }
        let observation = (|| {
            // Only the exact admitted built-in can contribute. Discovery,
            // mutations, failures, mixed batches and foreign tools reset us.
            if admitted_name != Some("SubAgent")
                || !matches!(
                    super::policy::tool_policy_key(call, admitted_name),
                    super::policy::ToolCircuitKey::SubAgentInspection
                )
            {
                return None;
            }
            let args = parse_tool_args(&call.function.arguments).ok()?;
            let target = args
                .get("target")
                .or_else(|| args.get("child_session_id"))?
                .as_str()?
                .trim();
            if target.is_empty() || target.len() > 256 {
                return None;
            }
            let result = outcome
                .as_ref()
                .ok()
                .filter(|r| r.success && r.images.is_empty())?;
            let mut value: serde_json::Value = serde_json::from_str(&result.result).ok()?;
            if value
                .get("actor_id")
                .or_else(|| value.get("child_session_id"))?
                .as_str()?
                != target
            {
                return None;
            }
            // Transport activity and elapsed-time advice are not a new work
            // observation. Tokens/heartbeats alone must not fund more polling.
            let object = value.as_object_mut()?;
            object.remove("last_event_at");
            object.remove("heartbeat");
            object.remove("guidance");
            let target = target.to_owned();
            let query = if args["intent"] == "inspect" {
                let message = args["message"].as_str().unwrap_or("overview").trim();
                if message.starts_with('{') {
                    serde_json::from_str::<serde_json::Value>(message).ok()?
                } else {
                    serde_json::json!({"view":if message.is_empty() {"overview"} else {message}})
                }
            } else {
                args
            };
            let selector = serde_json::json!({"target":target,
                "view":query["view"].as_str().unwrap_or("overview"),
                "cursor":query["cursor"],"message_id":query["message_id"]});
            value.sort_all_objects();
            let key: [u8; 32] = Sha256::digest(serde_json::to_vec(&selector).ok()?).into();
            let version = Sha256::digest(serde_json::to_vec(&value).ok()?).into();
            Some((
                key,
                ChildObservation {
                    version,
                    target,
                    unchanged: 0,
                },
            ))
        })();
        match observation {
            Some((key, observation)) if self.current.as_ref().unwrap().len() < MAX_OBSERVATIONS => {
                self.current.as_mut().unwrap().insert(key, observation);
            }
            _ => self.current = None,
        }
    }

    /// A third unchanged observation in an inspection-only stretch hands off
    /// to the durable wait. Alternating targets cannot refund the same status.
    /// Let the model see any new evidence in this batch before suspending.
    /// These IDs select observations; they grant no authority to wait on peers.
    pub(super) fn finish_round(&mut self) -> Vec<String> {
        let Some(current) = self.current.take().filter(|batch| !batch.is_empty()) else {
            self.previous.clear();
            return Vec::new();
        };
        let new_selectors = current
            .keys()
            .filter(|key| !self.previous.contains_key(*key))
            .count();
        if self.previous.len().saturating_add(new_selectors) > MAX_OBSERVATIONS {
            self.previous.clear();
        }
        let mut all_unchanged = true;
        let mut due = false;
        for (key, mut observation) in current {
            if let Some(previous) = self
                .previous
                .get(&key)
                .filter(|old| old.version == observation.version)
            {
                observation.unchanged = previous.unchanged.saturating_add(1);
            } else {
                observation.unchanged = 1;
                all_unchanged = false;
            }
            due |= observation.unchanged >= UNCHANGED_ROUNDS;
            self.previous.insert(key, observation);
        }
        if !all_unchanged || !due {
            return Vec::new();
        }
        self.previous
            .values()
            .map(|observation| observation.target.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }
}
