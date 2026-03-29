use crate::agent::core::{ExternalMemory, Session};

use super::system_sections::strip_existing_prompt_block;

const EXTERNAL_MEMORY_START_MARKER: &str = "<!-- BAMBOO_EXTERNAL_MEMORY_START -->";
const EXTERNAL_MEMORY_END_MARKER: &str = "<!-- BAMBOO_EXTERNAL_MEMORY_END -->";
/// Max chars per-topic shown in the system prompt.
const EXTERNAL_MEMORY_PROMPT_MAX_CHARS_PER_TOPIC: usize = 4_000;
/// Max total chars for the entire memory section (all topics combined).
const EXTERNAL_MEMORY_PROMPT_MAX_TOTAL_CHARS: usize = 6_000;
const EXTERNAL_MEMORY_TOOL_NAME: &str = "memory_note";

/// Context usage percentage at which we warn the LLM to save memory.
/// This should be below the budget system's compression trigger (~90%).
const CONTEXT_PRESSURE_WARNING_THRESHOLD: f64 = 70.0;

pub(super) fn strip_existing_external_memory(prompt: &str) -> String {
    strip_existing_prompt_block(
        prompt,
        EXTERNAL_MEMORY_START_MARKER,
        EXTERNAL_MEMORY_END_MARKER,
    )
}

fn truncate_chars(value: &str, max_chars: usize) -> (String, bool) {
    let mut out = String::new();
    for (count, ch) in value.chars().enumerate() {
        if count >= max_chars {
            return (out, true);
        }
        out.push(ch);
    }
    (out, false)
}

pub(super) async fn inject_external_memory_into_system_message(session: &mut Session) {
    let Some(system_message) = session
        .messages
        .iter_mut()
        .find(|message| matches!(message.role, crate::agent::core::Role::System))
    else {
        return;
    };

    // Remove any previously injected memory block, then re-append a fresh
    // memory section for this round.
    let base_prompt = strip_existing_external_memory(&system_message.content);

    let memory = ExternalMemory::with_defaults();
    let session_id = session.id.as_str();

    // Discover all topics for this session.
    let topics = match memory.list_topics(session_id).await {
        Ok(t) => t,
        Err(error) => {
            tracing::warn!("[{}] Failed to list memory topics: {}", session_id, error);
            Vec::new()
        }
    };

    // Build per-topic snippets.
    struct TopicSnippet {
        name: String,
        content: String,
        truncated: bool,
        full_len: usize,
    }
    let mut snippets: Vec<TopicSnippet> = Vec::new();
    let mut total_chars = 0usize;

    for topic in &topics {
        let content = match memory.read_topic(session_id, topic).await {
            Ok(Some(c)) => c.trim().to_string(),
            _ => continue,
        };
        if content.is_empty() {
            continue;
        }
        let full_len = content.chars().count();
        // Budget: per-topic cap AND remaining total budget.
        let remaining = EXTERNAL_MEMORY_PROMPT_MAX_TOTAL_CHARS.saturating_sub(total_chars);
        let cap = remaining.min(EXTERNAL_MEMORY_PROMPT_MAX_CHARS_PER_TOPIC);
        if cap == 0 {
            snippets.push(TopicSnippet {
                name: topic.clone(),
                content: String::new(),
                truncated: true,
                full_len,
            });
            continue;
        }
        let (snippet, truncated) = truncate_chars(&content, cap);
        total_chars += snippet.chars().count();
        snippets.push(TopicSnippet {
            name: topic.clone(),
            content: snippet,
            truncated,
            full_len,
        });
    }

    // Build the section.
    let mut section = String::new();
    section.push_str("\n\n");
    section.push_str(EXTERNAL_MEMORY_START_MARKER);
    section.push('\n');
    section.push_str("## External Memory (Persistent)\n\n");
    section.push_str("You have access to a persistent per-session memory note.\n");
    section.push_str("- If you learn durable information that will help later (preferences, key decisions, constraints, environment details), update the note using the tool.\n");
    section.push_str("- Do NOT store secrets/tokens.\n");
    section.push_str(
        "- Keep the note concise and factual. If it gets too long, compress it (rewrite a shorter version) and replace it.\n\n",
    );
    section.push_str("Tool usage:\n");
    section.push_str(&format!(
        "- Append: call `{EXTERNAL_MEMORY_TOOL_NAME}` with `{{\"action\":\"append\",\"content\":\"...\"}}`\n"
    ));
    section.push_str(&format!(
        "- Replace (for compression): call `{EXTERNAL_MEMORY_TOOL_NAME}` with `{{\"action\":\"replace\",\"content\":\"...\"}}`\n"
    ));
    section.push_str(&format!(
        "- Read full note (if truncated): call `{EXTERNAL_MEMORY_TOOL_NAME}` with `{{\"action\":\"read\"}}`\n"
    ));
    section.push_str(&format!(
        "- Use separate topics: add `\"topic\":\"my-topic\"` to keep unrelated workstreams isolated\n"
    ));
    section.push_str(&format!(
        "- List topics: call `{EXTERNAL_MEMORY_TOOL_NAME}` with `{{\"action\":\"list_topics\"}}`\n\n"
    ));

    if snippets.is_empty() {
        section.push_str("### Current Note (markdown)\n");
        section.push_str("````md\n_(empty)_\n````\n");
    } else if snippets.len() == 1 && snippets[0].name == "default" {
        // Single default topic — keep the legacy-style display.
        let s = &snippets[0];
        section.push_str("### Current Note (markdown)\n");
        section.push_str("````md\n");
        if s.content.is_empty() {
            section.push_str("_(empty)_");
        } else {
            section.push_str(&s.content);
        }
        section.push_str("\n````\n");
        if s.truncated {
            section.push_str(&format!(
                "\nNote is truncated in the system prompt (showing first {} chars of {}). Use `{}` action=read to view it and then action=replace to compress it.\n",
                s.content.chars().count(), s.full_len, EXTERNAL_MEMORY_TOOL_NAME
            ));
        }
    } else {
        // Multiple topics — show each under a sub-heading.
        for s in &snippets {
            section.push_str(&format!("### Topic: `{}`\n", s.name));
            section.push_str("````md\n");
            if s.content.is_empty() {
                section.push_str("_(truncated — use action=read topic=");
                section.push_str(&s.name);
                section.push_str(" to view)_");
            } else {
                section.push_str(&s.content);
            }
            section.push_str("\n````\n");
            if s.truncated && !s.content.is_empty() {
                section.push_str(&format!(
                    "_(showing {} of {} chars — use action=read topic={} to see full content)_\n",
                    s.content.chars().count(),
                    s.full_len,
                    s.name
                ));
            }
        }
    }

    // ── Context pressure warning ────────────────────────────────────────
    // When the context window is filling up, proactively remind the LLM to
    // save important state to memory_note before the budget system begins
    // purging older messages.
    if let Some(ref usage) = session.token_usage {
        let denominator = if usage.max_context_tokens > 0 {
            usage.max_context_tokens
        } else {
            usage.budget_limit
        };
        if denominator > 0 {
            let pct = (usage.total_tokens as f64 / denominator as f64) * 100.0;
            if pct >= CONTEXT_PRESSURE_WARNING_THRESHOLD {
                section.push_str(&format!(
                    "\n> ⚠️ **Context window filling up (~{:.0}% used).** \
                     Older messages will soon be compressed and summarized. \
                     Save any important context (key decisions, file paths, \
                     architecture notes, progress state) to `{EXTERNAL_MEMORY_TOOL_NAME}` \
                     now so it persists across the compression boundary.\n",
                    pct
                ));
            }
        }
    }

    section.push('\n');
    section.push_str(EXTERNAL_MEMORY_END_MARKER);

    system_message.content = format!("{}{}", base_prompt.trim_end(), section);
}
