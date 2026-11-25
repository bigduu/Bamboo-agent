use std::path::{Path, PathBuf};

use crate::project_context::ProjectContextResolver;
use crate::runtime::config::PromptMemoryFlags;
use bamboo_agent_core::{PromptMemoryObservability, Role, Session};
use bamboo_domain::ledger::LedgerScope;
use bamboo_memory::ledger_store::{AgendaItem, AgendaSnapshot, LedgerStore};
use bamboo_memory::memory_store::truncate_chars;
use bamboo_metrics::types::{PromptMemoryExposureObservation, PromptMemoryRecallOutcome};

use super::system_sections::strip_existing_prompt_block;

const EXTERNAL_MEMORY_START_MARKER: &str = "<!-- BAMBOO_EXTERNAL_MEMORY_START -->";
const EXTERNAL_MEMORY_END_MARKER: &str = "<!-- BAMBOO_EXTERNAL_MEMORY_END -->";
pub(super) const LEGACY_EXTERNAL_MEMORY_RENDERED_KEY: &str = "external_memory_rendered";
/// Round-local ledger agenda and context usage. Prior sessions may still have
/// the legacy field containing rendered Jiandu fragments. Never read it here.
pub(crate) const EXTERNAL_MEMORY_RENDERED_KEY: &str = "runtime.ledger_and_usage_context.v1";
pub(crate) const PROMPT_MEMORY_OBSERVABILITY_KEY: &str = "runtime_prompt_memory_observability";

const LEDGER_AGENDA_PROMPT_MAX_CHARS: usize = 1_200;
const LEDGER_AGENDA_ITEMS_PER_BUCKET: usize = 5;
const LEDGER_AGENDA_HORIZON_DAYS: i64 = 7;
const CONTEXT_PRESSURE_WARNING_THRESHOLD: f64 = 70.0;

#[derive(Debug, Clone)]
struct LedgerAgendaSnippet {
    content: String,
    item_count: usize,
}

/// Trusted round facts for the existing memory exposure metrics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PromptMemoryExposureProvenance {
    project_id: Option<String>,
    query_present: bool,
}

impl PromptMemoryExposureProvenance {
    pub(crate) fn observation(
        &self,
        round_id: &str,
        session_id: &str,
        observed_at: chrono::DateTime<chrono::Utc>,
    ) -> PromptMemoryExposureObservation {
        PromptMemoryExposureObservation {
            schema_version: 1,
            round_id: round_id.to_string(),
            session_id: session_id.to_string(),
            project_id: self.project_id.clone(),
            observed_at,
            recall_enabled: false,
            query_present: self.query_present,
            recall_outcome: PromptMemoryRecallOutcome::Disabled,
            all_compact_exposed_count: 0,
            project_exposed_count: 0,
            out_of_project_only: false,
            compact_section_chars: 0,
            project_items: Vec::new(),
        }
    }

    #[cfg(test)]
    pub(crate) fn supported_empty_for_test(project_id: Option<&str>) -> Self {
        Self {
            project_id: project_id.map(str::to_string),
            query_present: false,
        }
    }
}

pub(super) fn strip_existing_external_memory(prompt: &str) -> String {
    strip_existing_prompt_block(
        prompt,
        EXTERNAL_MEMORY_START_MARKER,
        EXTERNAL_MEMORY_END_MARKER,
    )
}

pub(crate) fn rendered_external_memory_section(session: &Session) -> Option<String> {
    let content = session.metadata.get(EXTERNAL_MEMORY_RENDERED_KEY)?.trim();
    (!content.is_empty()).then(|| content.to_string())
}

pub(crate) async fn refresh_external_memory_context(
    session: &mut Session,
    prompt_memory_flags: PromptMemoryFlags,
    project_context_resolver: Option<&ProjectContextResolver>,
    app_data_dir: Option<&Path>,
) -> PromptMemoryExposureProvenance {
    let workspace = session.workspace_path_meta().map(PathBuf::from);
    let project_id = if let Some(resolver) = project_context_resolver {
        match resolver
            .resolve_memory_read_scope(session, workspace.as_deref())
            .await
        {
            Ok(scope) => scope,
            Err(error) => {
                tracing::warn!(session_id = %session.id, "failed to resolve Project for the round context: {error}");
                None
            }
        }
    } else {
        ProjectContextResolver::memory_read_identity_for_session(session)
    };
    let project_key = project_id.as_ref().map(ToString::to_string);
    let mut context = String::new();
    if prompt_memory_flags.ledger_agenda {
        let data_dir = resolve_ledger_data_dir(app_data_dir);
        if let Some(agenda) = load_ledger_agenda_snippet(&data_dir, project_key.as_deref()).await {
            tracing::debug!(session_id = %session.id, items = agenda.item_count, "ledger agenda loaded");
            context = render_ledger_agenda_section(&agenda);
        }
    }
    let context_pressure_warning = render_context_pressure_warning(session).unwrap_or_default();
    context.push_str(&context_pressure_warning);
    let context = context.trim();
    session.metadata.remove(LEGACY_EXTERNAL_MEMORY_RENDERED_KEY);
    if context.is_empty() {
        session.metadata.remove(EXTERNAL_MEMORY_RENDERED_KEY);
    } else {
        session
            .metadata
            .insert(EXTERNAL_MEMORY_RENDERED_KEY.into(), context.to_string());
    }

    let query_present = latest_user_query_text(session).is_some();
    let observability = PromptMemoryObservability {
        latest_user_query_present: query_present,
        resolved_project_key: project_key.clone(),
        session_notes_status: "disabled".into(),
        project_memory_index_status: "disabled".into(),
        relevant_memory_status: "disabled".into(),
        project_dream_status: "disabled".into(),
        global_dream_fallback_status: "disabled".into(),
        dream_source: "none".into(),
        context_pressure_warning_chars: context_pressure_warning.chars().count(),
        external_memory_section_chars: context.chars().count(),
        ..Default::default()
    };
    if let Ok(raw) = serde_json::to_string(&observability) {
        session
            .metadata
            .insert(PROMPT_MEMORY_OBSERVABILITY_KEY.into(), raw);
    }

    PromptMemoryExposureProvenance {
        project_id: project_key,
        query_present,
    }
}

/// Load the agenda from Bamboo's ledger store. Jiandu memory owns a separate
/// data root and is deliberately not used to locate prospective records.
fn resolve_ledger_data_dir(explicit: Option<&Path>) -> PathBuf {
    explicit
        .map(Path::to_path_buf)
        .unwrap_or_else(bamboo_config::paths::bamboo_dir)
}

async fn load_ledger_agenda_snippet(
    ledger_data_dir: &Path,
    project_key: Option<&str>,
) -> Option<LedgerAgendaSnippet> {
    let store = LedgerStore::new(ledger_data_dir);
    let mut scopes: Vec<(LedgerScope, Option<String>)> = vec![(LedgerScope::Global, None)];
    if let Some(project_key) = project_key {
        scopes.push((LedgerScope::Project, Some(project_key.to_string())));
    }
    let snapshot = store
        .agenda(&scopes, chrono::Utc::now(), LEDGER_AGENDA_HORIZON_DAYS)
        .await
        .ok()?;
    if snapshot.is_empty() {
        return None;
    }
    let item_count = snapshot.overdue.len()
        + snapshot.today.len()
        + snapshot.upcoming.len()
        + snapshot.undated.len();
    Some(LedgerAgendaSnippet {
        content: render_ledger_agenda_lines(&snapshot),
        item_count,
    })
}

fn render_ledger_agenda_lines(snapshot: &AgendaSnapshot) -> String {
    fn push_bucket(out: &mut String, label: &str, items: &[AgendaItem]) {
        for item in items.iter().take(LEDGER_AGENDA_ITEMS_PER_BUCKET) {
            let when = item
                .anchor_at
                .map(|at| format!(" — {}", at.format("%Y-%m-%d %H:%M UTC")))
                .unwrap_or_default();
            out.push_str(&format!(
                "- [{label}] `{}` ({}) {}{}\n",
                item.id,
                item.kind.as_str(),
                item.title,
                when,
            ));
        }
        let hidden = items.len().saturating_sub(LEDGER_AGENDA_ITEMS_PER_BUCKET);
        if hidden > 0 {
            out.push_str(&format!("- [{label}] …and {hidden} more\n"));
        }
    }

    let mut out = String::new();
    push_bucket(&mut out, "OVERDUE", &snapshot.overdue);
    push_bucket(&mut out, "NEXT 24H", &snapshot.today);
    push_bucket(&mut out, "UPCOMING", &snapshot.upcoming);
    push_bucket(&mut out, "OPEN", &snapshot.undated);
    out
}

fn render_ledger_agenda_section(snippet: &LedgerAgendaSnippet) -> String {
    let mut section = String::new();
    section.push_str("### Ledger Agenda (prospective records)\n");
    section.push_str(
        "The user's open commitments from the persistent ledger — surface anything urgent \
         proactively when relevant. Times are UTC.\n",
    );
    let (content, truncated) = truncate_chars(&snippet.content, LEDGER_AGENDA_PROMPT_MAX_CHARS);
    section.push_str(&content);
    if truncated {
        section.push_str("\n_(agenda truncated; use `ledger` action=agenda for the full view)_\n");
    }
    section.push('\n');
    section
}

pub(super) fn latest_user_query_text(session: &Session) -> Option<&str> {
    session.messages.iter().rev().find_map(|message| {
        if message.role != Role::User || bamboo_domain::is_system_resume_message(message) {
            return None;
        }
        let content = message.content.trim();
        (!content.is_empty()).then_some(content)
    })
}

fn render_context_pressure_warning(session: &Session) -> Option<String> {
    let usage = session.token_usage.as_ref()?;
    let denominator = if usage.max_context_tokens > 0 {
        usage.max_context_tokens
    } else {
        usage.budget_limit
    };
    if denominator == 0 {
        return None;
    }

    let pct = (usage.total_tokens as f64 / denominator as f64) * 100.0;
    if pct < CONTEXT_PRESSURE_WARNING_THRESHOLD {
        return None;
    }

    Some(format!(
        "\n> ⚠️ **Context window filling up (~{pct:.0}% used).** Older messages may be compressed. Continue the current task and consult session history if earlier details are needed.\n"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ledger_root_defaults_to_bamboo_and_preserves_explicit_override() {
        let explicit = PathBuf::from("explicit-bamboo-root");
        assert_eq!(resolve_ledger_data_dir(Some(explicit.as_path())), explicit);
        assert_eq!(
            resolve_ledger_data_dir(None),
            bamboo_config::paths::bamboo_dir()
        );
    }
}
