use crate::agent::core::storage::SessionIndexEntry;

const MAX_INCLUDED_SESSIONS: usize = 12;
const MAX_SUMMARY_CHARS_PER_SESSION: usize = 800;

fn truncate_chars(value: &str, max_chars: usize) -> String {
    let mut out = String::new();
    for (count, ch) in value.chars().enumerate() {
        if count >= max_chars {
            out.push_str("...");
            return out;
        }
        out.push(ch);
    }
    out
}

pub fn build_consolidation_prompt(sessions: &[(SessionIndexEntry, Option<String>)]) -> String {
    let mut prompt = String::from("# Bamboo Dream Consolidation\n\n");
    prompt
        .push_str("You are performing a lightweight reflective consolidation pass for Bamboo.\n\n");
    prompt.push_str(
        "Your job is to synthesize durable cross-session signal from recent session activity into a concise notebook entry for future work.\n\n"
    );
    prompt.push_str("Requirements:\n");
    prompt.push_str("- Focus on durable facts, recurring goals, stable constraints, user preferences, active project directions, and unresolved blockers\n");
    prompt.push_str("- Prefer cross-session patterns over one-off chatter\n");
    prompt.push_str("- Do not include secrets, tokens, or highly transient details\n");
    prompt.push_str("- Separate active ongoing threads from completed or obsolete items\n");
    prompt.push_str("- Keep the final result compact and operational\n\n");
    prompt.push_str("Return markdown with these sections exactly:\n");
    prompt.push_str("1. ## Current durable context\n");
    prompt.push_str("2. ## Cross-session patterns\n");
    prompt.push_str("3. ## Active threads to remember\n");
    prompt.push_str("4. ## Stable constraints and preferences\n");
    prompt.push_str("5. ## Open risks or questions\n\n");

    prompt.push_str("## Recent sessions\n\n");
    if sessions.is_empty() {
        prompt.push_str("_(no recent sessions supplied)_\n");
        return prompt;
    }

    for (index, (entry, summary)) in sessions.iter().take(MAX_INCLUDED_SESSIONS).enumerate() {
        prompt.push_str(&format!(
            "### Session {}\n- id: {}\n- title: {}\n- kind: {:?}\n- updated_at: {}\n- message_count: {}\n",
            index + 1,
            entry.id,
            entry.title,
            entry.kind,
            entry.updated_at.to_rfc3339(),
            entry.message_count,
        ));
        if let Some(status) = entry
            .last_run_status
            .as_deref()
            .filter(|v| !v.trim().is_empty())
        {
            prompt.push_str(&format!("- last_run_status: {}\n", status));
        }
        if let Some(summary) = summary.as_deref().map(str::trim).filter(|v| !v.is_empty()) {
            prompt.push_str("- summary:\n");
            prompt.push_str("```md\n");
            prompt.push_str(&truncate_chars(summary, MAX_SUMMARY_CHARS_PER_SESSION));
            prompt.push_str("\n```\n");
        }
        prompt.push('\n');
    }

    if sessions.len() > MAX_INCLUDED_SESSIONS {
        prompt.push_str(&format!(
            "_Only the most recent {} sessions are included in this pass out of {} candidates._\n",
            MAX_INCLUDED_SESSIONS,
            sessions.len()
        ));
    }

    prompt
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::core::SessionKind;
    use chrono::Utc;

    fn sample_entry(id: &str) -> SessionIndexEntry {
        SessionIndexEntry {
            id: id.to_string(),
            kind: SessionKind::Root,
            rel_path: format!("sessions/{id}"),
            title: format!("Title for {id}"),
            pinned: false,
            parent_session_id: None,
            root_session_id: id.to_string(),
            spawn_depth: 0,
            model: "gpt-test".to_string(),
            reasoning_effort: None,
            created_by_schedule_id: None,
            schedule_run_id: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            last_activity_at: Utc::now(),
            message_count: 10,
            has_attachments: false,
            last_run_status: Some("completed".to_string()),
            last_run_error: None,
            token_usage: None,
        }
    }

    #[test]
    fn consolidation_prompt_includes_session_metadata_and_summary() {
        let prompt = build_consolidation_prompt(&[(
            sample_entry("session-1"),
            Some("Important summary".to_string()),
        )]);
        assert!(prompt.contains("Bamboo Dream Consolidation"));
        assert!(prompt.contains("session-1"));
        assert!(prompt.contains("Important summary"));
        assert!(prompt.contains("## Current durable context"));
    }
}
