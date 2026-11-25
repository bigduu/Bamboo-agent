use bamboo_agent_core::Message;

use super::merge_system_prompt_with_contexts;
use super::system_sections::{strip_existing_skill_context, strip_existing_tool_guide_context};

async fn publish_project_dream(
    store: &bamboo_memory::memory_store::MemoryStore,
    project_key: &str,
    content: &str,
) {
    let generation = store
        .current_scope_generation(
            bamboo_memory::memory_store::MemoryScope::Project,
            Some(project_key),
        )
        .await
        .expect("read project generation");
    store
        .publish_dream_snapshot(
            bamboo_memory::memory_store::MemoryScope::Project,
            Some(project_key),
            &generation,
            content,
        )
        .await
        .expect("publish project dream");
}

#[test]
fn latest_user_query_skips_hidden_and_runtime_resume_messages() {
    let mut session = bamboo_agent_core::Session::new("recall-real-user", "test-model");
    session.add_message(bamboo_agent_core::Message::user("real release question"));

    let mut hidden = bamboo_agent_core::Message::user("hidden child completion");
    hidden.metadata = Some(serde_json::json!({
        "hidden_from_ui": true,
        "runtime_kind": "child_completion_resume"
    }));
    session.add_message(hidden);

    let mut runtime_kind_only = bamboo_agent_core::Message::user("internal retry notice");
    runtime_kind_only.metadata = Some(serde_json::json!({"runtime_kind": "retry_resume"}));
    session.add_message(runtime_kind_only);

    assert_eq!(
        super::external_memory::latest_user_query_text(&session),
        Some("real release question")
    );
}

#[test]
fn latest_user_query_returns_none_for_internal_or_blank_user_messages_only() {
    let mut session = bamboo_agent_core::Session::new("recall-no-real-user", "test-model");
    session.add_message(bamboo_agent_core::Message::user("   "));
    let mut hidden = bamboo_agent_core::Message::user("hidden resume");
    hidden.metadata = Some(serde_json::json!({"hidden_from_ui": true}));
    session.add_message(hidden);

    assert!(super::external_memory::latest_user_query_text(&session).is_none());
}

#[test]
fn merge_system_prompt_with_contexts_appends_both_contexts() {
    let merged = merge_system_prompt_with_contexts(
        "You are a helpful assistant.",
        "\n\n## Skill System\n\n### Available Skills\nDetails",
        "## Tool Usage Guidelines\n\n### File Reading Tools\nDetails",
    );
    assert!(merged.starts_with("You are a helpful assistant."));
    assert!(merged.contains("<!-- BAMBOO_SKILL_CONTEXT_START -->"));
    assert!(merged.contains("## Skill System"));
    assert!(merged.contains("<!-- BAMBOO_TOOL_GUIDE_START -->"));
    assert!(merged.contains("## Tool Usage Guidelines"));
}

#[test]
fn merge_system_prompt_with_contexts_handles_empty_base_prompt() {
    let merged = merge_system_prompt_with_contexts(
        "",
        "\n\n## Skill System\n\n### Available Skills",
        "## Tool Usage Guidelines\n\n### File Reading Tools",
    );
    assert_eq!(
        merged,
        "<!-- BAMBOO_SKILL_CONTEXT_START -->\n## Skill System\n\n### Available Skills\n<!-- BAMBOO_SKILL_CONTEXT_END -->\n\n<!-- BAMBOO_TOOL_GUIDE_START -->\n## Tool Usage Guidelines\n\n### File Reading Tools\n<!-- BAMBOO_TOOL_GUIDE_END -->"
    );
}

#[test]
fn strip_existing_skill_context_removes_previous_section() {
    let stripped = strip_existing_skill_context(
        "Base prompt\n\n<!-- BAMBOO_SKILL_CONTEXT_START -->\n## Skill System\n\n### Available Skills\nInstructions\n<!-- BAMBOO_SKILL_CONTEXT_END -->",
    );
    assert_eq!(stripped, "Base prompt");
}

#[test]
fn strip_existing_skill_context_does_not_remove_user_heading_without_markers() {
    let original = "Base prompt\n\n## Skill System\nThis heading belongs to user prompt.";
    let stripped = strip_existing_skill_context(original);
    assert_eq!(stripped, original);
}

#[test]
fn strip_existing_tool_guide_context_removes_previous_section() {
    let stripped = strip_existing_tool_guide_context(
        "Base prompt\n\n<!-- BAMBOO_TOOL_GUIDE_START -->\n## Tool Usage Guidelines\n\n### File Reading Tools\nInstructions\n<!-- BAMBOO_TOOL_GUIDE_END -->",
    );
    assert_eq!(stripped, "Base prompt");
}

#[test]
fn strip_existing_tool_guide_context_does_not_remove_user_heading_without_markers() {
    let original = "Base prompt\n\n## Tool Usage Guidelines\nUser custom section.";
    let stripped = strip_existing_tool_guide_context(original);
    assert_eq!(stripped, original);
}

#[test]
fn legacy_external_memory_is_stripped_from_the_system_prompt() {
    let prompt = concat!(
        "Base instructions\n\n",
        "<!-- BAMBOO_EXTERNAL_MEMORY_START -->\n",
        "Previously injected Jiandu text.\n",
        "<!-- BAMBOO_EXTERNAL_MEMORY_END -->"
    );
    assert_eq!(
        super::strip_existing_external_memory(prompt),
        "Base instructions"
    );
}

#[tokio::test]
async fn all_session_roles_keep_jiandu_memory_out_of_round_context() {
    use crate::runtime::config::PromptMemoryFlags;
    use bamboo_agent_core::{PromptMemoryObservability, Session};
    use bamboo_domain::ledger::{LedgerRecord, LedgerScope, RecordKind};
    use bamboo_domain::{ProjectId, SessionAuthorityIdentity, DEFAULT_SUPERVISOR_SESSION_ID};
    use bamboo_memory::ledger_store::LedgerStore;
    use bamboo_memory::memory_store::{DurableMemoryType, MemoryScope, MemoryStore};
    use bamboo_metrics::types::PromptMemoryRecallOutcome;

    let temp = tempfile::tempdir().expect("temp dir");
    let bamboo_root = temp.path().join("bamboo");
    let jiandu_root = temp.path().join("jiandu");
    let memory = MemoryStore::new(&jiandu_root);
    let ledger = LedgerStore::new(&bamboo_root);
    let project_id = ProjectId::parse("on-demand-project").expect("Project ID");
    let project_memory = memory.for_project(&project_id);

    let mut global_record =
        LedgerRecord::new("rec_global", RecordKind::Todo, "Pending global item");
    global_record.time.due_at = Some(chrono::Utc::now() - chrono::Duration::days(1));
    ledger
        .write_record(global_record, None)
        .await
        .expect("save global agenda item");
    let mut project_record =
        LedgerRecord::new("rec_project", RecordKind::Todo, "Pending Project item");
    project_record.scope = LedgerScope::Project;
    project_record.project_key = Some(project_id.to_string());
    ledger
        .write_record(project_record, None)
        .await
        .expect("save Project agenda item");

    for (store, scope, project_key, memory_type, title) in [
        (
            &memory,
            MemoryScope::Global,
            None,
            DurableMemoryType::Reference,
            "Global compass rule",
        ),
        (
            &project_memory,
            MemoryScope::Project,
            Some(project_id.as_str()),
            DurableMemoryType::Project,
            "Project compass rule",
        ),
    ] {
        store
            .write_memory(
                scope,
                project_key,
                memory_type,
                title,
                "A prior compass rule applies here.",
                &["compass".into()],
                None,
                "test-model",
                false,
                None,
            )
            .await
            .expect("seed durable memory");
    }
    publish_project_dream(&project_memory, project_id.as_str(), "Project dream marker").await;

    let mut supervisor = Session::new(DEFAULT_SUPERVISOR_SESSION_ID, "test-model");
    supervisor.authority_identity = SessionAuthorityIdentity::Supervisor {
        incarnation_id: uuid::Uuid::new_v4(),
    };
    let mut worker = Session::new("project-worker", "test-model");
    worker.set_project_id_meta(project_id.to_string());
    worker
        .metadata
        .insert("session.origin.v1".into(), "supervisor_created".into());
    let mut ultra = Session::new("ultra-root", "test-model");
    ultra.set_root_orchestration_only(true).unwrap();
    let mut child = Session::new_child_of("project-child", &worker, "test-model", "child");
    child.set_project_id_meta(project_id.to_string());
    let mut manual = Session::new("manual-project-root", "test-model");
    manual.set_project_id_meta(project_id.to_string());
    let flags = PromptMemoryFlags {
        project_prompt_injection: true,
        relevant_recall: true,
        relevant_recall_rerank: true,
        project_first_dream: true,
        ledger_agenda: true,
    };

    for mut session in [supervisor, worker, ultra, child, manual] {
        memory
            .write_session_topic(&session.id, "default", "Continuity note marker")
            .await
            .expect("save session note");
        session.add_message(Message::user("compass rule"));
        session.token_usage = Some(
            serde_json::from_value(serde_json::json!({
                "system_tokens": 0, "summary_tokens": 0, "window_tokens": 75,
                "total_tokens": 75, "budget_limit": 100,
                "truncation_occurred": false, "segments_removed": 0,
            }))
            .expect("set context pressure"),
        );
        let legacy_key = super::external_memory::LEGACY_EXTERNAL_MEMORY_RENDERED_KEY;
        session
            .metadata
            .insert(legacy_key.into(), "Legacy rendered memory marker".into());
        assert!(super::render_external_memory_section(&session).is_none());
        let observation =
            super::refresh_external_memory_context(&mut session, flags, None, Some(&bamboo_root))
                .await
                .observation("on-demand-round", &session.id, chrono::Utc::now());
        let rendered = super::render_external_memory_section(&session).expect("render agenda");
        assert!(rendered.contains("[OVERDUE] `rec_global`"));
        assert_eq!(
            rendered.contains("Pending Project item"),
            session.project_id_meta().is_some()
        );
        assert!(rendered.contains("Context window filling up (~75% used)"));
        assert!(!session.metadata.contains_key(legacy_key));
        for text in [
            "Continuity note marker",
            "Project compass rule",
            "Global compass rule",
            "Project dream marker",
            "Legacy rendered memory marker",
        ] {
            assert!(!rendered.contains(text), "{} exposed {text}", session.id);
        }
        assert!(!observation.recall_enabled);
        assert!(observation.query_present);
        assert_eq!(
            observation.recall_outcome,
            PromptMemoryRecallOutcome::Disabled
        );
        assert_eq!(observation.all_compact_exposed_count, 0);
        let observability: PromptMemoryObservability =
            serde_json::from_str(&session.metadata[super::PROMPT_MEMORY_OBSERVABILITY_KEY])
                .expect("prompt memory observability");
        assert_eq!(observability.session_notes_status, "disabled");
        assert_eq!(observability.project_memory_index_status, "disabled");
        assert_eq!(observability.relevant_memory_status, "disabled");
        assert_eq!(observability.project_dream_status, "disabled");
    }
}

#[tokio::test]
async fn round_context_clears_when_ledger_is_disabled_or_empty() {
    use crate::runtime::config::PromptMemoryFlags;
    use bamboo_agent_core::Session;
    use bamboo_domain::ledger::{LedgerRecord, RecordKind};
    use bamboo_memory::ledger_store::LedgerStore;

    let temp = tempfile::tempdir().expect("temp dir");
    let ledger = LedgerStore::new(temp.path());
    let mut session = Session::new("ledger-context", "test-model");
    let mut flags = PromptMemoryFlags {
        ledger_agenda: true,
        ..Default::default()
    };
    super::refresh_external_memory_context(&mut session, flags, None, Some(temp.path())).await;
    assert!(super::render_external_memory_section(&session).is_none());

    ledger
        .write_record(
            LedgerRecord::new("rec_open", RecordKind::Todo, "Clean the garage"),
            None,
        )
        .await
        .expect("save agenda item");
    super::refresh_external_memory_context(&mut session, flags, None, Some(temp.path())).await;
    let context = super::render_external_memory_section(&session).expect("agenda");
    assert!(context.contains("[OPEN] `rec_open`"));

    flags.ledger_agenda = false;
    super::refresh_external_memory_context(&mut session, flags, None, Some(temp.path())).await;
    assert!(super::render_external_memory_section(&session).is_none());
}
