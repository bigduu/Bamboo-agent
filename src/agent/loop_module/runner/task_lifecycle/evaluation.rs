use std::collections::HashSet;
use std::sync::Arc;

use tokio::sync::mpsc;

use crate::agent::core::todo::{TaskBlocker, TaskBlockerKind, TaskEvidence, TaskEvidenceKind};
use crate::agent::core::{AgentError, AgentEvent, Session};
use crate::agent::llm::LLMProvider;
use crate::agent::loop_module::task_context::TaskLoopContext;
use crate::agent::loop_module::task_evaluation::evaluate_task_progress;
use crate::agent::metrics::TokenUsage as MetricsTokenUsage;
use crate::core::ReasoningEffort;

fn normalize_criterion(value: &str) -> Option<String> {
    let normalized = value
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .to_lowercase();
    if normalized.is_empty() {
        None
    } else {
        Some(normalized)
    }
}

fn parse_criterion_ref(value: &str) -> Option<usize> {
    let trimmed = value.trim().to_ascii_lowercase();
    let as_c_ref = trimmed
        .strip_prefix("criterion_")
        .or_else(|| trimmed.strip_prefix("criterion-"))
        .or_else(|| trimmed.strip_prefix('c'));
    if let Some(raw_index) = as_c_ref {
        return raw_index.parse::<usize>().ok().filter(|index| *index > 0);
    }
    None
}

fn missing_completion_criteria(required: &[String], criteria_met: &[String]) -> Vec<String> {
    let mut required_lookup = std::collections::HashMap::new();
    for (index, criterion) in required.iter().enumerate() {
        if let Some(normalized) = normalize_criterion(criterion) {
            required_lookup.insert(normalized, index + 1);
        }
    }

    let mut met_refs: HashSet<usize> = HashSet::new();
    for criterion in criteria_met {
        if let Some(index) = parse_criterion_ref(criterion) {
            met_refs.insert(index);
            continue;
        }
        if let Some(normalized) = normalize_criterion(criterion) {
            if let Some(index) = required_lookup.get(&normalized).copied() {
                met_refs.insert(index);
            }
        }
    }

    required
        .iter()
        .enumerate()
        .filter_map(|(index, criterion)| {
            if met_refs.contains(&(index + 1)) {
                return None;
            }
            Some(criterion.trim().to_string())
        })
        .collect()
}

fn append_text(existing: Option<String>, addition: &str) -> Option<String> {
    let addition = addition.trim();
    if addition.is_empty() {
        return existing;
    }
    match existing {
        Some(mut value) if !value.trim().is_empty() => {
            value.push('\n');
            value.push_str(addition);
            Some(value)
        }
        _ => Some(addition.to_string()),
    }
}

pub(super) async fn evaluate_round_task_progress(
    task_context: &mut Option<TaskLoopContext>,
    session: &mut Session,
    llm: Arc<dyn LLMProvider>,
    event_tx: &mpsc::Sender<AgentEvent>,
    session_id: &str,
    round_number: usize,
    model_name: Option<&str>,
    reasoning_effort: Option<ReasoningEffort>,
) -> Result<MetricsTokenUsage, AgentError> {
    fn append_structured_fields_to_session_task(
        session: &mut Session,
        item_id: &str,
        evidence: Option<&str>,
        blocker: Option<&str>,
    ) {
        let Some(task_list) = session.task_list.as_mut() else {
            return;
        };
        let Some(item) = task_list.items.iter_mut().find(|item| item.id == item_id) else {
            return;
        };

        let mut changed = false;
        if let Some(summary) = evidence
            .map(str::trim)
            .filter(|summary| !summary.is_empty())
        {
            item.push_evidence(TaskEvidence {
                kind: TaskEvidenceKind::Observation,
                summary: summary.to_string(),
                reference: None,
                tool_name: None,
                tool_call_id: None,
                round: None,
                success: None,
            });
            changed = true;
        }
        if let Some(summary) = blocker.map(str::trim).filter(|summary| !summary.is_empty()) {
            item.add_blocker(TaskBlocker {
                kind: TaskBlockerKind::Unknown,
                summary: summary.to_string(),
                waiting_on: None,
            });
            changed = true;
        }
        if changed {
            task_list.updated_at = chrono::Utc::now();
            session.updated_at = chrono::Utc::now();
        }
    }

    let Some(ctx_snapshot) = task_context.as_ref() else {
        return Ok(MetricsTokenUsage::default());
    };

    tracing::debug!(
        "[{}] Evaluating task list progress at end of round {}",
        session_id,
        round_number
    );

    let model = model_name
        .ok_or_else(|| AgentError::LLM("model_name is required in AgentLoopConfig".to_string()))?;

    let mut usage = MetricsTokenUsage::default();
    match evaluate_task_progress(
        ctx_snapshot,
        session,
        llm,
        event_tx,
        session_id,
        model,
        reasoning_effort,
    )
    .await
    {
        Ok(evaluation_result) => {
            usage.prompt_tokens = evaluation_result.prompt_tokens;
            usage.completion_tokens = evaluation_result.completion_tokens;
            usage.total_tokens = usage.prompt_tokens.saturating_add(usage.completion_tokens);

            if evaluation_result.needs_evaluation && !evaluation_result.updates.is_empty() {
                tracing::info!(
                    "[{}] LLM evaluated {} task item updates",
                    session_id,
                    evaluation_result.updates.len()
                );

                for update in evaluation_result.updates {
                    let mut status = update.status.clone();
                    let mut notes = update.notes.clone();
                    let mut evidence = update.evidence.clone();
                    let blocker = update.blocker.clone();
                    if matches!(status, crate::agent::core::TaskItemStatus::Completed) {
                        let required_criteria = session
                            .task_list
                            .as_ref()
                            .and_then(|task_list| {
                                task_list
                                    .items
                                    .iter()
                                    .find(|item| item.id == update.item_id)
                                    .map(|item| item.completion_criteria.clone())
                            })
                            .unwrap_or_default();
                        if !required_criteria.is_empty() {
                            let criteria_met = update.criteria_met.clone().unwrap_or_default();
                            let missing =
                                missing_completion_criteria(&required_criteria, &criteria_met);
                            if !missing.is_empty() {
                                status = crate::agent::core::TaskItemStatus::InProgress;
                                let gate_note = format!(
                                    "Completion criteria not fully met; keeping task in_progress. Missing: {}",
                                    missing.join(" | ")
                                );
                                notes = append_text(notes, &gate_note);
                                let gate_evidence = if criteria_met.is_empty() {
                                    format!(
                                        "No criteria were reported as met. Missing: {}",
                                        missing.join(" | ")
                                    )
                                } else {
                                    format!(
                                        "Criteria met: {} | Missing: {}",
                                        criteria_met.join(" | "),
                                        missing.join(" | ")
                                    )
                                };
                                evidence = append_text(evidence, &gate_evidence);
                                tracing::debug!(
                                    "[{}] Kept task {} in_progress due to unmet completion criteria",
                                    session_id,
                                    update.item_id
                                );
                            }
                        }
                    }
                    if let Some(ctx) = task_context.as_mut() {
                        ctx.update_item_status(&update.item_id, status.clone());
                        ctx.append_structured_feedback(
                            &update.item_id,
                            evidence.as_deref(),
                            blocker.as_deref(),
                        );
                    }
                    let _ = session.update_task_item(
                        &update.item_id,
                        status,
                        notes.as_deref(),
                        update.criteria_met.as_deref(),
                    );
                    append_structured_fields_to_session_task(
                        session,
                        &update.item_id,
                        evidence.as_deref(),
                        blocker.as_deref(),
                    );
                }
            }
        }
        Err(error) => {
            tracing::warn!("[{}] Task evaluation failed: {}", session_id, error);
        }
    }

    Ok(usage)
}

#[cfg(test)]
mod tests {
    use super::missing_completion_criteria;

    #[test]
    fn missing_completion_criteria_matches_case_and_whitespace_insensitively() {
        let required = vec![
            "All integration tests pass".to_string(),
            "No clippy warnings".to_string(),
        ];
        let met = vec![
            "  all   integration tests   pass  ".to_string(),
            "NO CLIPPY WARNINGS".to_string(),
        ];

        let missing = missing_completion_criteria(&required, &met);
        assert!(missing.is_empty());
    }

    #[test]
    fn missing_completion_criteria_reports_unmet_items() {
        let required = vec![
            "All integration tests pass".to_string(),
            "No clippy warnings".to_string(),
        ];
        let met = vec!["All integration tests pass".to_string()];

        let missing = missing_completion_criteria(&required, &met);
        assert_eq!(missing, vec!["No clippy warnings".to_string()]);
    }

    #[test]
    fn missing_completion_criteria_accepts_criterion_refs() {
        let required = vec![
            "All integration tests pass".to_string(),
            "No clippy warnings".to_string(),
        ];
        let met = vec!["c1".to_string(), "criterion_2".to_string()];

        let missing = missing_completion_criteria(&required, &met);
        assert!(missing.is_empty());
    }

    #[test]
    fn missing_completion_criteria_accepts_dash_style_criterion_ref() {
        let required = vec![
            "All integration tests pass".to_string(),
            "No clippy warnings".to_string(),
        ];
        let met = vec!["criterion-1".to_string(), "c2".to_string()];

        let missing = missing_completion_criteria(&required, &met);
        assert!(missing.is_empty());
    }
}
