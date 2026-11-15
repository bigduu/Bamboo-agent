use std::collections::{HashSet, VecDeque};

use bamboo_agent_core::{AgentError, Message, Role};
use bamboo_domain::capability_loading::resolve_tool_reference_name;
use bamboo_llm::provider::OutputLimitRecovery;

const MAX_STEPS: usize = 3;
const MAX_REQUESTS: usize = MAX_STEPS + 2;
const MAX_STEP_BYTES: usize = 320;
/// Used when this run has no configured token ceiling. Earlier usage counts
/// against it along with the planning, work and reporting requests.
pub(super) const DEFAULT_TOKEN_LIMIT: u64 = 256_000;

pub(super) fn forbidden_tool(reference: &str) -> bool {
    resolve_tool_reference_name(reference, |name| {
        matches!(
            name,
            "SubAgent"
                | "sub_task"
                | "spawn_session"
                | "session_control"
                | "session_message"
                | "ask_agent"
                | "deploy_agent"
                | "cluster"
        ) || name == bamboo_domain::DISCOVERY_CONTROL_FALLBACK_TOOL_NAME
    })
    .is_some()
}

pub(super) fn confirms_tool_result(reference: &str, id: &str, message: &Message) -> bool {
    if message.role != Role::Tool
        || message.tool_call_id.as_deref() != Some(id)
        || message.tool_success != Some(true)
    {
        return false;
    }
    // The shell API reports transport success for running commands and
    // nonzero exits. Those receipts do not complete a recovery dependency.
    let Some(shell) = resolve_tool_reference_name(reference, |name| {
        matches!(
            name,
            "Bash" | "bash" | "BashOutput" | "bash_output" | "exec_command" | "write_stdin"
        )
    }) else {
        return true;
    };
    serde_json::from_str::<serde_json::Value>(&message.content).is_ok_and(|body| {
        let terminal = matches!(body["status"].as_str(), Some("completed" | "exited"))
            || (matches!(shell.as_str(), "Bash" | "bash") && body.get("status").is_none());
        terminal && body["error"].is_null() && body["exit_code"].as_i64() == Some(0)
    })
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Stage {
    Plan,
    Work,
    Report,
}

enum Phase {
    Plan,
    Work(VecDeque<String>),
    Report,
    Finished,
    FailedStep,
}

pub(super) struct SplitRecovery {
    pub retry: OutputLimitRecovery,
    pub prompt_floor: u64,
    phase: Phase,
    requests: usize,
}

impl SplitRecovery {
    pub fn new(retry: OutputLimitRecovery, prompt_floor: u64) -> Self {
        Self {
            retry,
            prompt_floor,
            phase: Phase::Plan,
            requests: 0,
        }
    }

    pub fn next_request(&mut self) -> Result<(Stage, String), AgentError> {
        if matches!(self.phase, Phase::FailedStep) {
            return Err(AgentError::Tool(
                "output-limit recovery stopped at an unconfirmed step; progress is in the session"
                    .into(),
            ));
        }
        if self.requests == MAX_REQUESTS || matches!(self.phase, Phase::Finished) {
            return Err(AgentError::Budget(
                "output-limit task recovery request limit reached".into(),
            ));
        }
        self.requests += 1;
        let request = match &self.phase {
            Phase::Plan => (
                Stage::Plan,
                format!(
                    "Bamboo runtime recovery plan. Use the original request, current task list, \
                     and confirmed tool results above. Split the unfinished work into at most \
                     {MAX_STEPS} distinct steps in dependency order. Each step must fit one \
                     tool call or a short answer, and be at most {MAX_STEP_BYTES} UTF-8 bytes. \
                     Return only a JSON array of step descriptions. Return [] if there is no \
                     useful step left to perform. This request is for planning only.",
                ),
            ),
            Phase::Work(steps) => (
                Stage::Work,
                format!(
                    "Bamboo runtime recovery step. Reuse the confirmed results above. Complete \
                     only this step with one tool call or a short answer. Start a text answer \
                     with CHECKPOINT: and cite the evidence, or with BLOCKED: and explain what \
                     remains. Step description: {}",
                    steps.front().expect("work phase has a pending step"),
                ),
            ),
            Phase::Report => (
                Stage::Report,
                "Bamboo runtime recovery report. Answer the original request concisely from \
                 the confirmed results above. Identify any work that remains incomplete. \
                 This request is for reporting only."
                    .into(),
            ),
            Phase::Finished | Phase::FailedStep => unreachable!("checked before dispatch"),
        };
        if request.0 == Stage::Report {
            self.phase = Phase::Finished;
        }
        Ok(request)
    }

    pub fn accept_plan(&mut self, text: &str) -> Result<(), AgentError> {
        let invalid =
            || AgentError::LLMNonRetryable("output-limit recovery plan was invalid".into());
        if text.len() > MAX_STEPS * (MAX_STEP_BYTES + 8) + 32 {
            return Err(invalid());
        }
        let plan: Vec<String> = serde_json::from_str(text).map_err(|_| invalid())?;
        let mut seen = HashSet::new();
        if plan.len() > MAX_STEPS
            || plan.iter().any(|step| {
                let step = step.trim();
                step.is_empty() || step.len() > MAX_STEP_BYTES || !seen.insert(step)
            })
        {
            return Err(invalid());
        }
        self.phase = if plan.is_empty() {
            Phase::Report
        } else {
            Phase::Work(
                plan.into_iter()
                    .map(|step| step.trim().to_owned())
                    .collect(),
            )
        };
        Ok(())
    }

    pub fn continue_after_truncated_plan(&mut self) {
        debug_assert!(matches!(self.phase, Phase::Plan));
        self.phase = Phase::Work(VecDeque::from([
            "Complete the next useful unfinished action. If all work is done, confirm it from the evidence."
                .into(),
        ]));
    }

    pub fn finish_step(&mut self, successful: bool) {
        let Phase::Work(steps) = &mut self.phase else {
            return;
        };
        if !successful {
            self.phase = Phase::FailedStep;
        } else {
            steps.pop_front();
            if steps.is_empty() {
                self.phase = Phase::Report;
            }
        }
    }
}
