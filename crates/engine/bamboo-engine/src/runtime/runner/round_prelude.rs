//! Minimal round prelude — provides `prepare_round` for lifecycle adapter
//! and `refresh_round_prompt_context` for the pipeline.

use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use crate::runtime::config::AgentLoopConfig;
use crate::runtime::task_context::TaskLoopContext;
use bamboo_agent_core::tools::ToolExecutor;
use bamboo_agent_core::{AgentError, Role, Session};
use bamboo_llm::LLMProvider;
use bamboo_metrics::MetricsCollector;

use super::prompt_context::{
    refresh_external_memory_context, PromptMemoryRuntimeContext, PROMPT_MEMORY_OBSERVABILITY_KEY,
};
use super::session_setup::prompt_setup::{persist_prompt_snapshot_metadata, PromptAssemblyReport};
use bamboo_agent_core::PromptSnapshot;

/// Round-prelude frame bundling per-round identification and observability
/// parameters.  Passed into [`prepare_round`] to keep its parameter count
/// below the clippy threshold.
pub(crate) struct RoundPreludeFrame<'a> {
    pub execution_id: &'a str,
    pub round: usize,
    pub max_rounds: usize,
    pub debug_enabled: bool,
    pub cancel_token: &'a CancellationToken,
    pub metrics_collector: Option<&'a MetricsCollector>,
    pub session_id: &'a str,
    pub model_name: &'a str,
}

// ---- prompt_updates functions ----

const RUNTIME_PROMPT_FLAGS_KEY: &str = "runtime_prompt_component_flags";
const RUNTIME_PROMPT_LENGTHS_KEY: &str = "runtime_prompt_component_lengths";
const RUNTIME_PROMPT_SECTION_LAYOUT_KEY: &str = "runtime_prompt_section_layout";

pub(crate) async fn refresh_round_prompt_context(
    session: &mut Session,
    prompt_memory_flags: crate::runtime::config::PromptMemoryFlags,
    runtime_context: Option<&PromptMemoryRuntimeContext>,
    project_context_resolver: Option<&crate::project_context::ProjectContextResolver>,
) -> Result<(), AgentError> {
    refresh_project_context(session, project_context_resolver).await?;
    refresh_external_memory_context(
        session,
        prompt_memory_flags,
        runtime_context,
        project_context_resolver,
    )
    .await;
    // Task list, goal, plan-mode, and plan-runtime context are NOT injected into
    // the system message — they are built as dedicated volatile blocks directly
    // from session state during request assembly (cache-stable system prefix).

    let session_id = session.id.clone();
    let prompt_for_metadata = session
        .messages
        .iter_mut()
        .find(|message| matches!(message.role, Role::System))
        .map(|system_message| system_message.content.clone());

    if let Some(prompt) = prompt_for_metadata {
        persist_round_prompt_metadata(session, &prompt);
        log_round_prompt_refresh_summary(session_id.as_str(), &prompt);
    }
    Ok(())
}

async fn refresh_project_context(
    session: &mut Session,
    resolver: Option<&crate::project_context::ProjectContextResolver>,
) -> Result<(), AgentError> {
    let Some(resolver) = resolver else {
        return Ok(());
    };
    resolver
        .refresh_session_prompt(session)
        .await
        .map(|_| ())
        .map_err(|error| AgentError::ProjectContext(error.to_string()))
}

// ---- round_state functions ----

pub(super) fn update_task_round_state(
    task_context: &mut Option<TaskLoopContext>,
    round: usize,
    max_rounds: usize,
) {
    if let Some(ctx) = task_context.as_mut() {
        ctx.current_round = round as u32;
        ctx.max_rounds = max_rounds as u32;
    }
}

pub(crate) fn new_execution_id() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

pub(super) fn build_round_id(session_id: &str, execution_id: &str, round: usize) -> String {
    debug_assert!(
        !execution_id.is_empty(),
        "round metrics require an execution identity"
    );
    format!("{session_id}-run-{execution_id}-round-{}", round + 1)
}

pub(super) fn build_auxiliary_round_id(
    session_id: &str,
    execution_id: &str,
    purpose: &str,
    round_number: usize,
) -> String {
    debug_assert!(
        !execution_id.is_empty(),
        "auxiliary round metrics require an execution identity"
    );
    format!("{session_id}-run-{execution_id}-{purpose}-round-{round_number}")
}

pub(super) fn log_round_start(
    debug_enabled: bool,
    session_id: &str,
    round: usize,
    max_rounds: usize,
    message_count: usize,
) {
    if debug_enabled {
        tracing::debug!(
            "[{}] round_start: {}",
            session_id,
            serde_json::json!({
                "round": round + 1,
                "total_rounds": max_rounds,
                "message_count": message_count,
            })
        );
    }
}

// ---- cancellation ----

fn ensure_not_cancelled(
    cancel_token: &CancellationToken,
    metrics_collector: Option<&MetricsCollector>,
    session_id: &str,
    message_count: usize,
) -> Result<(), AgentError> {
    if cancel_token.is_cancelled() {
        super::metrics_lifecycle::record_session_cancelled(
            metrics_collector,
            session_id,
            message_count as u32,
        );
        return Err(AgentError::Cancelled);
    }
    Ok(())
}

// ---- prompt metadata ----

fn persist_round_prompt_metadata(session: &mut Session, prompt: &str) {
    // Task list and external memory are sourced from session state/field (not
    // reparsed from system-message markers), since they ride volatile blocks now.
    let task_list_text = session.format_task_list_for_prompt();
    let external_memory = super::prompt_context::render_external_memory_section(session);
    let sections = build_round_prompt_sections(
        prompt,
        &task_list_text,
        external_memory.as_deref().unwrap_or_default(),
    );
    let report = PromptAssemblyReport::from_sections(sections, prompt);
    session.metadata.insert(
        RUNTIME_PROMPT_FLAGS_KEY.to_string(),
        report.component_flags_value(),
    );
    session.metadata.insert(
        RUNTIME_PROMPT_LENGTHS_KEY.to_string(),
        report.component_lengths_value(),
    );
    session.metadata.insert(
        RUNTIME_PROMPT_SECTION_LAYOUT_KEY.to_string(),
        report.section_layout_value(),
    );

    let task_list = (!task_list_text.trim().is_empty()).then(|| task_list_text.clone());

    let mut snapshot = super::session_setup::prompt_setup::read_prompt_snapshot_metadata(session)
        .unwrap_or_else(|| PromptSnapshot {
            base_system_prompt: session
                .metadata
                .get("base_system_prompt")
                .cloned()
                .unwrap_or_default(),
            enhancement_prompt: session.enhance_prompt(),
            project_context: session
                .metadata
                .get(crate::project_context::PROJECT_CONTEXT_RENDERED_KEY)
                .cloned(),
            workspace_context: super::session_setup::prompt_setup::workspace_context_from_session(
                session,
            ),
            instruction_context: session.workspace_path_meta().and_then(|workspace_path| {
                crate::runtime::context::instruction::build_instruction_prompt_context(
                    &workspace_path,
                )
            }),
            env_context: crate::runtime::context::build_env_prompt_context(),
            skill_context: session.metadata.get("skill.context").cloned(),
            tool_guide_context: None,
            dream_notebook: None,
            session_memory_note: None,
            project_memory_index: None,
            relevant_durable_memories: None,
            project_dream: None,
            global_dream_fallback: None,
            prompt_memory_observability: None,
            external_memory: None,
            task_list: None,
            effective_system_prompt: prompt.trim().to_string(),
        });
    let external_memory_parts =
        bamboo_agent_core::parse_prompt_external_memory_sections(external_memory.as_deref());
    snapshot.dream_notebook = external_memory_parts.dream_notebook;
    snapshot.session_memory_note = external_memory_parts.session_memory_note;
    snapshot.project_memory_index = external_memory_parts.project_memory_index;
    snapshot.relevant_durable_memories = external_memory_parts.relevant_durable_memories;
    snapshot.project_dream = external_memory_parts.project_dream;
    snapshot.global_dream_fallback = external_memory_parts.global_dream_fallback;
    snapshot.prompt_memory_observability = session
        .metadata
        .get(PROMPT_MEMORY_OBSERVABILITY_KEY)
        .and_then(|raw| {
            serde_json::from_str::<bamboo_agent_core::PromptMemoryObservability>(raw).ok()
        });
    snapshot.external_memory = external_memory;
    snapshot.task_list = task_list;
    snapshot.effective_system_prompt = prompt.trim().to_string();
    persist_prompt_snapshot_metadata(session, snapshot);
}

fn build_round_prompt_sections(
    prompt: &str,
    task_list: &str,
    external_memory: &str,
) -> Vec<super::session_setup::prompt_setup::PromptSection> {
    use super::session_setup::prompt_setup::{PromptLayer, PromptSection};

    vec![
        PromptSection::new("round_base_prompt", PromptLayer::CoreStatic, false, prompt),
        PromptSection::new(
            "external_memory",
            PromptLayer::EnvironmentWorkspace,
            true,
            external_memory,
        ),
        PromptSection::new(
            "task_list",
            PromptLayer::EnvironmentWorkspace,
            true,
            task_list,
        ),
    ]
}

fn log_round_prompt_refresh_summary(session_id: &str, prompt: &str) {
    tracing::info!(
        "[{}] Round prompt refresh summary: effective_len={} chars",
        session_id,
        prompt.len(),
    );
}

// ---- Main prepare_round function (for lifecycle adapter) ----

pub(crate) async fn prepare_round(
    session: &mut Session,
    task_context: &mut Option<TaskLoopContext>,
    config: &AgentLoopConfig,
    llm: Arc<dyn LLMProvider>,
    _tools: &dyn ToolExecutor,
    frame: &RoundPreludeFrame<'_>,
) -> Result<String, AgentError> {
    // Bind frame fields as locals so the rest of the function body stays unchanged.
    let round = frame.round;
    let max_rounds = frame.max_rounds;
    let cancel_token = frame.cancel_token;
    let metrics_collector = frame.metrics_collector;
    let session_id = frame.session_id;
    let model_name = frame.model_name;
    let debug_enabled = frame.debug_enabled;

    let runtime_context = PromptMemoryRuntimeContext {
        llm: config.background_model_provider.clone().unwrap_or(llm),
        background_model_name: config.background_model_name.clone(),
    };
    refresh_round_prompt_context(
        session,
        config.prompt_memory_flags,
        Some(&runtime_context),
        config.project_context_resolver.as_deref(),
    )
    .await?;
    update_task_round_state(task_context, round, max_rounds);

    let round_id = build_round_id(session_id, frame.execution_id, round);
    log_round_start(
        debug_enabled,
        session_id,
        round,
        max_rounds,
        session.messages.len(),
    );
    ensure_not_cancelled(
        cancel_token,
        metrics_collector,
        session_id,
        session.messages.len(),
    )?;

    super::metrics_lifecycle::record_round_started(
        metrics_collector,
        &round_id,
        session_id,
        model_name,
    );

    Ok(round_id)
}

#[cfg(test)]
mod project_prompt_tests {
    use async_trait::async_trait;
    use bamboo_agent_core::{Message, Session};
    use bamboo_domain::{ProjectId, ProjectResourceSummary, WorkspaceBinding};

    use crate::project_context::{
        ProjectContextError, ProjectContextResolver, ProjectContextSource, ProjectDescriptor,
    };

    struct StaticSource(ProjectDescriptor);

    #[async_trait]
    impl ProjectContextSource for StaticSource {
        async fn find_project(
            &self,
            project_id: &ProjectId,
        ) -> Result<Option<ProjectDescriptor>, ProjectContextError> {
            Ok((&self.0.id == project_id).then(|| self.0.clone()))
        }
    }

    struct OwnedWorkspaceSource {
        descriptor: ProjectDescriptor,
        owner: ProjectId,
    }

    #[async_trait]
    impl ProjectContextSource for OwnedWorkspaceSource {
        async fn find_project(
            &self,
            project_id: &ProjectId,
        ) -> Result<Option<ProjectDescriptor>, ProjectContextError> {
            Ok((&self.descriptor.id == project_id).then(|| self.descriptor.clone()))
        }

        async fn find_workspace_owner(
            &self,
            _workspace: &std::path::Path,
        ) -> Result<Option<ProjectId>, ProjectContextError> {
            Ok(Some(self.owner.clone()))
        }
    }

    #[tokio::test]
    async fn per_round_resolution_preserves_system_and_refreshes_workspace_metadata() {
        let directory = tempfile::tempdir().expect("tempdir");
        let first = directory.path().join("main");
        let second = directory.path().join("worktree");
        std::fs::create_dir_all(&first).expect("first");
        std::fs::create_dir_all(&second).expect("second");
        let first = first.canonicalize().expect("canonical first workspace");
        let second = second.canonicalize().expect("canonical second workspace");
        let project_id = ProjectId::parse("project-1").expect("project id");
        let descriptor = ProjectDescriptor {
            id: project_id.clone(),
            name: "Zenith".to_string(),
            project_path: Some(first.clone()),
            home: directory.path().join("projects/project-1"),
            workspace_bindings: vec![
                WorkspaceBinding {
                    path: first.to_string_lossy().to_string(),
                    label: None,
                    git_common_dir: None,
                },
                WorkspaceBinding {
                    path: second.to_string_lossy().to_string(),
                    label: None,
                    git_common_dir: None,
                },
            ],
            resources: ProjectResourceSummary {
                project_id: project_id.clone(),
                resource_revision: 1,
                resources: Vec::new(),
            },
            memory_read_roots: crate::project_context::ProjectMemoryReadRoots {
                primary: directory.path().join("projects/project-1/memory/v1"),
                legacy_aliases: Vec::new(),
            },
        };
        let resolver = ProjectContextResolver::new(std::sync::Arc::new(StaticSource(descriptor)));
        let mut session = Session::new("session-1", "model");
        session.set_project_id_meta(project_id.to_string());
        session.set_workspace_path_meta(first.to_string_lossy().to_string());
        session.add_message(Message::system("Base"));
        let system_id = session.messages[0].id.clone();

        super::refresh_project_context(&mut session, Some(&resolver))
            .await
            .expect("first Project refresh");
        assert_eq!(session.messages[0].id, system_id);
        assert_eq!(session.messages[0].content, "Base");
        let project_context = session
            .metadata
            .get(crate::project_context::PROJECT_CONTEXT_RENDERED_KEY)
            .expect("path-free Project model context")
            .clone();
        assert!(project_context.contains("Project ID: project-1"));
        assert!(!project_context.contains(directory.path().to_string_lossy().as_ref()));

        session.set_workspace_path_meta(second.to_string_lossy().to_string());
        super::refresh_project_context(&mut session, Some(&resolver))
            .await
            .expect("second Project refresh");
        let second_workspace = bamboo_config::paths::path_to_display_string(
            &second.canonicalize().expect("canonical second workspace"),
        );
        assert_eq!(session.messages[0].id, system_id);
        assert_eq!(session.messages[0].content, "Base");
        assert_eq!(
            session
                .metadata
                .get(crate::project_context::PROJECT_CONTEXT_RENDERED_KEY),
            Some(&project_context)
        );
        assert_eq!(
            session.workspace_path_meta().as_deref(),
            Some(second_workspace.as_str())
        );
        assert!(session
            .prompt_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.workspace_context.as_deref())
            .is_some_and(|context| context.contains(&second_workspace)));
        assert!(session.messages.iter().all(|message| {
            !message
                .content
                .contains(crate::runtime::context::PROJECT_CONTEXT_START_MARKER)
                && !message
                    .content
                    .contains(crate::runtime::context::WORKSPACE_CONTEXT_START_MARKER)
                && !message.content.contains(&second_workspace)
        }));
        assert_eq!(
            session
                .metadata
                .get(crate::project_context::WORKSPACE_BINDING_STATUS_METADATA_KEY)
                .map(String::as_str),
            Some(crate::project_context::WorkspaceBindingStatus::Registered.as_str())
        );
    }

    #[tokio::test]
    async fn round_project_refresh_fails_closed_for_invalid_missing_and_cross_project_context() {
        let directory = tempfile::tempdir().expect("tempdir");
        let workspace = directory.path().join("workspace");
        std::fs::create_dir_all(&workspace).expect("workspace");
        let project_id = ProjectId::parse("round-project").expect("project id");
        let descriptor = ProjectDescriptor {
            id: project_id.clone(),
            name: "Round Project".to_string(),
            project_path: Some(workspace.clone()),
            home: directory.path().join("projects/round-project"),
            workspace_bindings: Vec::new(),
            resources: ProjectResourceSummary {
                project_id: project_id.clone(),
                resource_revision: 1,
                resources: Vec::new(),
            },
            memory_read_roots: crate::project_context::ProjectMemoryReadRoots {
                primary: directory.path().join("projects/round-project/memory/v1"),
                legacy_aliases: Vec::new(),
            },
        };
        let resolver =
            ProjectContextResolver::new(std::sync::Arc::new(StaticSource(descriptor.clone())));

        let mut invalid = Session::new("round-invalid-project", "model");
        invalid
            .metadata
            .insert("project_id".to_string(), "../invalid".to_string());
        let error = super::refresh_project_context(&mut invalid, Some(&resolver))
            .await
            .expect_err("invalid identity must stop the round");
        assert!(matches!(
            error,
            bamboo_agent_core::AgentError::ProjectContext(ref message)
                if message.contains("invalid Project identity")
        ));

        let mut missing = Session::new("round-missing-project", "model");
        missing.set_project_id_meta("missing-project");
        let error = super::refresh_project_context(&mut missing, Some(&resolver))
            .await
            .expect_err("missing assigned Project must stop the round");
        assert!(matches!(
            error,
            bamboo_agent_core::AgentError::ProjectContext(ref message)
                if message.contains("unavailable")
        ));

        let foreign_owner = ProjectId::parse("foreign-owner").expect("foreign Project id");
        let owned_resolver =
            ProjectContextResolver::new(std::sync::Arc::new(OwnedWorkspaceSource {
                descriptor,
                owner: foreign_owner,
            }));
        let mut cross_project = Session::new("round-cross-project", "model");
        cross_project.set_project_id_meta(project_id);
        cross_project.set_workspace_path_meta(workspace.to_string_lossy().into_owned());
        let error = super::refresh_project_context(&mut cross_project, Some(&owned_resolver))
            .await
            .expect_err("cross-Project workspace must stop the round");
        assert!(matches!(
            error,
            bamboo_agent_core::AgentError::ProjectContext(ref message)
                if message.contains("belongs to Project")
        ));
    }

    #[tokio::test]
    async fn round_project_refresh_keeps_unassigned_unbound_legacy_session_executable() {
        let directory = tempfile::tempdir().expect("tempdir");
        let project_id = ProjectId::parse("unrelated-project").expect("project id");
        let descriptor = ProjectDescriptor {
            id: project_id.clone(),
            name: "Unrelated".to_string(),
            project_path: None,
            home: directory.path().join("projects/unrelated-project"),
            workspace_bindings: Vec::new(),
            resources: ProjectResourceSummary {
                project_id,
                resource_revision: 1,
                resources: Vec::new(),
            },
            memory_read_roots: crate::project_context::ProjectMemoryReadRoots {
                primary: directory
                    .path()
                    .join("projects/unrelated-project/memory/v1"),
                legacy_aliases: Vec::new(),
            },
        };
        let resolver = ProjectContextResolver::new(std::sync::Arc::new(StaticSource(descriptor)));
        let mut session = Session::new("round-unassigned-legacy", "model");
        session.add_message(Message::system("legacy base prompt"));

        super::refresh_project_context(&mut session, Some(&resolver))
            .await
            .expect("unassigned unbound legacy session remains executable");
        assert_eq!(session.messages[0].content, "legacy base prompt");
    }
}
