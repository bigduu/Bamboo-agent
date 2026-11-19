use std::sync::Weak;

use async_trait::async_trait;
use bamboo_agent_core::tools::{Tool, ToolCtx, ToolError, ToolOutcome, ToolResult};
use bamboo_domain::{RootActorRuntimeWrite, PACED_LOOP_WAKEUP_TOOL_NAME};
use bamboo_engine::runtime::paced_loop_wakeup::{
    take_paced_loop_wakeup_writer, PACED_LOOP_WAKEUP_EXECUTION_CONTRACT,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::{supported_root, LoopStatus, PacedLoopError, PacedLoopState, PacedLoops};

const MIN_DELAY_SECONDS: u64 = 60;
const MAX_DELAY_SECONDS: u64 = 3600;
const MAX_REASON_BYTES: usize = 512;
pub(super) const ADAPTIVE_FALLBACK_SECONDS: u64 = 20 * 60;

#[derive(Clone, Copy, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum LoopMode {
    #[default]
    Fixed,
    Adaptive,
}

impl LoopMode {
    pub(super) fn is_fixed(&self) -> bool {
        matches!(self, Self::Fixed)
    }
}

#[derive(Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct AdaptiveTurn {
    pub(super) advice: Option<WakeupAdvice>,
}

#[derive(Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct WakeupAdvice {
    pub(super) delay_seconds: u64,
    pub(super) reason: String,
}

impl WakeupAdvice {
    pub(super) fn is_valid(&self) -> bool {
        (MIN_DELAY_SECONDS..=MAX_DELAY_SECONDS).contains(&self.delay_seconds)
            && !self.reason.trim().is_empty()
            && self.reason.len() <= MAX_REASON_BYTES
            && !self.reason.chars().any(char::is_control)
    }
}

pub(super) struct NextWakeup<'a> {
    pub(super) at: DateTime<Utc>,
    pub(super) advice: Option<&'a WakeupAdvice>,
    pub(super) awaiting_outcome: bool,
}

pub(in crate::app_state) struct ScheduleLoopWakeupTool {
    loops: Weak<PacedLoops>,
}

impl ScheduleLoopWakeupTool {
    pub(in crate::app_state) fn new(loops: Weak<PacedLoops>) -> Self {
        Self { loops }
    }
}

#[async_trait]
impl Tool for ScheduleLoopWakeupTool {
    fn name(&self) -> &str {
        PACED_LOOP_WAKEUP_TOOL_NAME
    }

    fn execution_contract(&self) -> Option<&'static str> {
        Some(PACED_LOOP_WAKEUP_EXECUTION_CONTRACT)
    }

    fn description(&self) -> &str {
        "Recommend when the next occurrence of this adaptive session loop should start. Call once at the end of the occurrence, based on what you observed, and then finish the turn. The delay runs from successful completion of this turn. Bamboo keeps a fallback wakeup if the turn fails or no valid suggestion is received. This tool applies only to the current adaptive occurrence."
    }

    fn parameters_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "delay_seconds": { "type": "integer", "minimum": MIN_DELAY_SECONDS, "maximum": MAX_DELAY_SECONDS },
                "reason": { "type": "string", "minLength": 1, "maxLength": MAX_REASON_BYTES, "description": "A brief single-line reason for this delay." }
            },
            "required": ["delay_seconds", "reason"],
            "additionalProperties": false
        })
    }

    async fn invoke(&self, args: Value, ctx: ToolCtx) -> Result<ToolOutcome, ToolError> {
        let mut advice: WakeupAdvice = serde_json::from_value(args).map_err(|_| {
            ToolError::InvalidArguments("Supply delay_seconds and a short reason".into())
        })?;
        advice.reason = advice.reason.trim().to_owned();
        if !advice.is_valid() {
            return Err(ToolError::InvalidArguments(
                "Delay must be 60–3600 seconds with a brief single-line reason".into(),
            ));
        }
        let unavailable = || {
            ToolError::Execution(
                "A wakeup suggestion requires this chat's live adaptive occurrence".into(),
            )
        };
        let owner = take_paced_loop_wakeup_writer(&ctx.tool_call_id)
            .filter(|owner| {
                !ctx.plan_read_only && ctx.session_id() == Some(owner.fence.actor_id.as_str())
            })
            .ok_or_else(unavailable)?;
        let loops = self.loops.upgrade().ok_or_else(unavailable)?;
        loops
            .propose_wakeup(&owner, &advice)
            .await
            .map_err(|error| ToolError::Execution(error.to_string()))?;
        Ok(ToolOutcome::Completed(ToolResult::text(
            true,
            json!({"delay_seconds": advice.delay_seconds, "reason": advice.reason, "applies_on": "successful_completion"}).to_string(),
        )))
    }
}

impl PacedLoops {
    async fn propose_wakeup(
        &self,
        owner: &RootActorRuntimeWrite,
        advice: &WakeupAdvice,
    ) -> Result<(), PacedLoopError> {
        let id = owner.fence.actor_id.as_str();
        let _guard = self.persistence.acquire_lock(id).await;
        let session = self.load(id).await?;
        let now = Utc::now();
        let unavailable =
            || PacedLoopError::Conflict("Adaptive occurrence is no longer active".into());
        let mut state = PacedLoopState::read(&session)?
            .filter(|state| {
                supported_root(&session)
                    && session.created_at == owner.created_at
                    && matches!(state.status(self.host_instance, now), LoopStatus::Scheduled)
                    && !state.dispatch_failed
                    && state.last_run_id.as_deref() == Some(owner.fence.run_id.as_str())
                    && state.outcome(&session).is_none()
            })
            .ok_or_else(unavailable)?;
        let turn = state.adaptive_turn.as_mut().ok_or_else(unavailable)?;
        if let Some(existing) = turn.advice.as_ref() {
            if existing == advice {
                return Ok(());
            }
            return Err(PacedLoopError::Conflict(
                "This adaptive occurrence already has a wakeup suggestion".into(),
            ));
        }
        turn.advice = Some(WakeupAdvice {
            delay_seconds: advice.delay_seconds,
            reason: advice.reason.clone(),
        });
        // The exact live Actor writer and the canonical loop record must both
        // still match under V2's cross-process write lock.
        self.commit(session, &state, Some(owner)).await?;
        Ok(())
    }
}
