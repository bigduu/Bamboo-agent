//! Single authoritative pre-execution session mutation point.
//!
//! Historically three execution entry points each duplicated — with subtly
//! different logic — the work of (a) placing the authoritative leading System
//! message and (b) setting `session.model` before handing the session to the
//! agent loop. This module consolidates that into [`prepare_session_for_execution`]
//! so there is exactly one place that defines the pre-execution mutation.
//!
//! The three callers are:
//! - the SDK facade (`bamboo_sdk::agent::Agent::execute_internal`), which owns a
//!   configured instruction and model and passes both;
//! - the server spawn path (`runtime::execution::agent_spawn::spawn_session_execution`),
//!   whose caller has already placed the system prompt, so it passes `None` for
//!   `system_prompt` and only the resolved model;
//! - the child spawn path (`sdk::spawn::run_child_spawn`), likewise `None` for the
//!   system prompt and only the child model.

use std::path::Path;

use bamboo_agent_core::{AgentError, Message, Role, Session};

use crate::project_context::{
    ProjectContextResolver, SessionProjectIdentity, WorkspaceBindingStatus, WorkspaceSource,
    WORKSPACE_BINDING_STATUS_METADATA_KEY, WORKSPACE_SOURCE_METADATA_KEY,
};

/// A workspace that an adapter has already validated for one execution.
///
/// Keeping path, provenance, and Project-binding status in one value prevents
/// Schedule/Connect factories from publishing a path and taking their first
/// prompt snapshot before the two authority fields are present.
#[derive(Debug, Clone, Copy)]
pub struct ResolvedExecutionWorkspace<'a> {
    pub path: &'a Path,
    pub source: WorkspaceSource,
    pub binding_status: WorkspaceBindingStatus,
}

/// Publish an adapter-validated workspace and its typed provenance as one
/// pre-snapshot operation.
///
/// The supplied [`bamboo_agent_core::workspace_state::WorkspaceResolver`] must
/// be the same instance that performed validation. Publication can materialize
/// a resolver-owned fallback, so the returned path is the only path persisted
/// on the session.
pub fn publish_resolved_workspace_for_execution(
    session: &mut Session,
    workspace: ResolvedExecutionWorkspace<'_>,
    workspace_resolver: &bamboo_agent_core::workspace_state::WorkspaceResolver,
    publication_source: &str,
) {
    let final_workspace = workspace_resolver.publish_resolved_workspace(
        &session.id,
        workspace.path.to_path_buf(),
        publication_source,
    );
    session.set_workspace_path_meta(bamboo_config::paths::path_to_display_string(
        &final_workspace,
    ));
    session.metadata.insert(
        WORKSPACE_SOURCE_METADATA_KEY.to_string(),
        workspace.source.as_str().to_string(),
    );
    session.metadata.insert(
        WORKSPACE_BINDING_STATUS_METADATA_KEY.to_string(),
        workspace.binding_status.as_str().to_string(),
    );
}

/// Prepare a caller-owned session before any approved-tool replay or provider
/// execution.
///
/// This is the engine-owned external handoff seam used by the SDK. It is
/// deliberately idempotent and runs before a configured System prompt replaces
/// the caller's leading message:
///
/// - validate Project identity and required resolver authority without mutation;
/// - recover and remove legacy host-owned prompt blocks;
/// - validate Project workspace ownership when a resolver exists;
/// - fail closed for an assigned Project when no resolver authority exists;
/// - publish the exact runtime workspace and typed path/source/binding metadata;
/// - refresh the typed prompt snapshot without ever persisting host context in
///   System text.
pub async fn prepare_external_session_for_execution(
    session: &mut Session,
    project_context_resolver: Option<&ProjectContextResolver>,
) -> Result<(), AgentError> {
    // Establish that this process has the authority required for the session
    // before migrating any retryable legacy state. In particular, an assigned
    // session without a Project resolver must remain byte-for-byte retryable:
    // even a marker-only System message cannot be consumed on this error path.
    match ProjectContextResolver::session_project_identity(session) {
        SessionProjectIdentity::Invalid { raw, message } => {
            return Err(AgentError::ProjectContext(format!(
                "session carries an invalid Project identity '{raw}': {message}"
            )));
        }
        SessionProjectIdentity::Assigned(project_id) if project_context_resolver.is_none() => {
            return Err(AgentError::ProjectContext(format!(
                "assigned Project '{project_id}' requires a ProjectContextResolver before external execution"
            )));
        }
        SessionProjectIdentity::Assigned(_) | SessionProjectIdentity::Unassigned => {}
    }

    // Once authority is known to be sufficient, recover a legacy workspace
    // before resolution: its host-owned System block may be the sole source of
    // the path that the resolver must validate and publish.
    crate::runtime::runner::session_setup::migrate_legacy_workspace_prompt(session);

    if let Some(resolver) = project_context_resolver {
        resolver
            .refresh_session_prompt(session)
            .await
            .map_err(|error| AgentError::ProjectContext(error.to_string()))?;
        return Ok(());
    }

    // An unassigned embedding has no Project authority to consult, but it must
    // still hand the resolved workspace to tools before replay/provider work.
    // The process-global resolver is the compatibility authority for this SDK
    // shape. Unassigned workspaces are necessarily unregistered.
    let workspace = ProjectContextResolver::resolve_workspace_candidate(session, None)
        .map_err(|error| AgentError::ProjectContext(error.to_string()))?;
    if let Some(workspace) = workspace.as_deref() {
        let source = WorkspaceSource::from_metadata(
            session
                .metadata
                .get(WORKSPACE_SOURCE_METADATA_KEY)
                .map(String::as_str),
        );
        publish_resolved_workspace_for_execution(
            session,
            ResolvedExecutionWorkspace {
                path: workspace,
                source,
                binding_status: WorkspaceBindingStatus::Unregistered,
            },
            &bamboo_agent_core::workspace_state::WorkspaceResolver::from_process_globals(),
            "external_execution_prep",
        );
    } else {
        session.metadata.remove(WORKSPACE_SOURCE_METADATA_KEY);
        session
            .metadata
            .remove(WORKSPACE_BINDING_STATUS_METADATA_KEY);
    }
    crate::runner::refresh_prompt_snapshot(session);
    Ok(())
}

/// Apply the authoritative pre-execution mutations to `session`.
///
/// This encodes the single, authoritative behavior that every execution entry
/// point must share:
///
/// - If `system_prompt` is `Some`, it is applied as the session's **leading**
///   System message. The supplied prompt is authoritative: if the first message
///   is already a [`Role::System`] message it is *replaced* (never duplicated),
///   otherwise a System message is *inserted* at index 0. This guarantees a
///   caller-supplied session can't silently shadow the configured instruction.
/// - If `model` is `Some`, `session.model` is set to it.
///
/// Call sites that don't supply one of these inputs (e.g. the spawn paths, whose
/// caller already placed the system prompt) pass `None` for that parameter, so
/// behavior is identical to the previous inline logic.
pub fn prepare_session_for_execution(
    session: &mut Session,
    system_prompt: Option<&str>,
    model: Option<&str>,
) {
    if let Some(prompt) = system_prompt {
        match session.messages.first() {
            Some(first) if matches!(first.role, Role::System) => {
                session.messages[0] = Message::system(prompt.to_string());
            }
            _ => session
                .messages
                .insert(0, Message::system(prompt.to_string())),
        }
    }

    if let Some(model) = model {
        session.model = model.to_string();
    }

    // A configured prompt may itself have been copied from a legacy Bamboo
    // snapshot. Strip only recognized host-owned blocks after replacement;
    // recovered typed metadata remains authoritative. Every caller observes a
    // snapshot whose System bytes and typed workspace context agree.
    crate::runtime::runner::session_setup::migrate_legacy_workspace_prompt(session);
    crate::runner::refresh_prompt_snapshot(session);
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_agent_core::Message;
    use std::sync::Arc;

    struct EmptyProjectSource;

    #[async_trait::async_trait]
    impl crate::project_context::ProjectContextSource for EmptyProjectSource {
        async fn find_project(
            &self,
            _project_id: &bamboo_domain::ProjectId,
        ) -> Result<
            Option<crate::project_context::ProjectDescriptor>,
            crate::project_context::ProjectContextError,
        > {
            Ok(None)
        }
    }

    fn session_with(messages: Vec<Message>) -> Session {
        let mut s = Session::new("test-session", "old-model");
        s.messages = messages;
        s
    }

    #[test]
    fn empty_session_with_prompt_inserts_at_index_zero() {
        let mut session = session_with(vec![Message::user("hello")]);

        prepare_session_for_execution(&mut session, Some("you are helpful"), None);

        assert_eq!(session.messages.len(), 2);
        assert!(matches!(session.messages[0].role, Role::System));
        assert_eq!(session.messages[0].content, "you are helpful");
        assert!(matches!(session.messages[1].role, Role::User));
    }

    #[test]
    fn leading_system_is_replaced_not_duplicated() {
        let mut session = session_with(vec![
            Message::system("stale prompt"),
            Message::user("hello"),
        ]);

        prepare_session_for_execution(&mut session, Some("authoritative prompt"), None);

        // Replaced in place, no duplicate System message.
        assert_eq!(session.messages.len(), 2);
        assert!(matches!(session.messages[0].role, Role::System));
        assert_eq!(session.messages[0].content, "authoritative prompt");
        assert!(matches!(session.messages[1].role, Role::User));
    }

    #[test]
    fn leading_non_system_gets_prompt_inserted_at_zero() {
        let mut session =
            session_with(vec![Message::user("hello"), Message::assistant("hi", None)]);

        prepare_session_for_execution(&mut session, Some("you are helpful"), None);

        assert_eq!(session.messages.len(), 3);
        assert!(matches!(session.messages[0].role, Role::System));
        assert_eq!(session.messages[0].content, "you are helpful");
        assert!(matches!(session.messages[1].role, Role::User));
        assert!(matches!(session.messages[2].role, Role::Assistant));
    }

    #[test]
    fn model_is_set_when_some() {
        let mut session = session_with(vec![Message::user("hello")]);

        prepare_session_for_execution(&mut session, None, Some("new-model"));

        assert_eq!(session.model, "new-model");
        // No system prompt supplied → message list untouched.
        assert_eq!(session.messages.len(), 1);
        assert!(matches!(session.messages[0].role, Role::User));
    }

    #[test]
    fn none_inputs_leave_session_untouched() {
        let mut session = session_with(vec![Message::user("hello")]);

        prepare_session_for_execution(&mut session, None, None);

        assert_eq!(session.model, "old-model");
        assert_eq!(session.messages.len(), 1);
        assert!(session.prompt_snapshot.is_some());
    }

    #[tokio::test]
    async fn external_prep_migrates_serialized_legacy_workspace_idempotently_before_configured_prompt(
    ) {
        let root = tempfile::tempdir().expect("workspace root");
        let workspace = root.path().join("legacy-workspace");
        std::fs::create_dir_all(&workspace).expect("legacy workspace");
        let canonical = workspace.canonicalize().expect("canonical workspace");
        let display = bamboo_config::paths::path_to_display_string(&canonical);
        let legacy_block = crate::runtime::context::build_workspace_prompt_context(&display)
            .expect("legacy Workspace block");

        let mut legacy = Session::new("external-legacy", "old-model");
        legacy.add_message(Message::system(legacy_block));
        legacy.metadata.insert(
            "runtime_prompt_snapshot".to_string(),
            "stale-snapshot".to_string(),
        );
        let serialized = serde_json::to_vec(&legacy).expect("serialize legacy session");
        let mut session: Session =
            serde_json::from_slice(&serialized).expect("deserialize legacy session");

        let resolver = ProjectContextResolver::new_with_workspace_resolver(
            Arc::new(EmptyProjectSource),
            bamboo_agent_core::workspace_state::WorkspaceResolver::new(|| None, {
                let root = root.path().to_path_buf();
                move || bamboo_agent_core::workspace_state::WorkspaceRootConfig {
                    root: root.clone(),
                    confine: false,
                }
            }),
        );

        prepare_external_session_for_execution(&mut session, Some(&resolver))
            .await
            .expect("first external prep");
        assert!(
            session
                .messages
                .iter()
                .all(|message| !matches!(message.role, Role::System)),
            "a marker-only legacy System message must be removed"
        );
        assert_eq!(
            session.workspace_path_meta().as_deref(),
            Some(display.as_str())
        );
        assert_eq!(
            session
                .metadata
                .get(WORKSPACE_SOURCE_METADATA_KEY)
                .map(String::as_str),
            Some(WorkspaceSource::Session.as_str())
        );
        assert_eq!(
            session
                .metadata
                .get(WORKSPACE_BINDING_STATUS_METADATA_KEY)
                .map(String::as_str),
            Some(WorkspaceBindingStatus::Unregistered.as_str())
        );
        assert!(!session.metadata.contains_key("runtime_prompt_snapshot"));
        assert_eq!(
            bamboo_agent_core::workspace_state::get_workspace(&session.id),
            Some(canonical.clone())
        );

        let once = serde_json::to_value(&session).expect("first prepared session");
        prepare_external_session_for_execution(&mut session, Some(&resolver))
            .await
            .expect("second external prep");
        assert_eq!(
            serde_json::to_value(&session).expect("second prepared session"),
            once,
            "external prep must be an exact session-state no-op after the first handoff"
        );

        prepare_session_for_execution(&mut session, Some("configured System"), Some("new-model"));
        let systems = session
            .messages
            .iter()
            .filter(|message| matches!(message.role, Role::System))
            .collect::<Vec<_>>();
        assert_eq!(systems.len(), 1);
        assert_eq!(systems[0].content, "configured System");
        assert!(!systems[0].content.contains(&display));
        assert!(!systems[0].content.contains("BAMBOO_WORKSPACE_CONTEXT"));
        assert_eq!(session.model, "new-model");
        assert_eq!(
            bamboo_agent_core::workspace_state::get_workspace(&session.id),
            Some(canonical)
        );
        let snapshot = session
            .prompt_snapshot
            .as_ref()
            .expect("configured snapshot");
        assert_eq!(snapshot.effective_system_prompt, "configured System");
        let workspace_context = snapshot
            .workspace_context
            .as_deref()
            .expect("typed Workspace context");
        assert!(workspace_context.contains(&display));
        assert!(workspace_context.contains("Workspace source: session"));
        assert!(workspace_context.contains("Binding status: unregistered"));
    }

    #[tokio::test]
    async fn assigned_external_prep_without_resolver_fails_closed_before_mutating_retry_state() {
        let workspace = tempfile::tempdir().expect("legacy Workspace");
        let display = bamboo_config::paths::path_to_display_string(workspace.path());
        let legacy_block = crate::runtime::context::build_workspace_prompt_context(&display)
            .expect("legacy Workspace block");
        let mut session = session_with(vec![Message::system(format!(
            "caller System\n\n{legacy_block}"
        ))]);
        session.set_project_id_meta("project-external-prep");
        session.metadata.insert(
            crate::session_app::respond::PERMISSION_REEXECUTE_METADATA_KEY.to_string(),
            "retryable-tool-call".to_string(),
        );
        session.prompt_snapshot = Some(
            serde_json::from_value(serde_json::json!({
                "base_system_prompt": "stale caller snapshot",
                "effective_system_prompt": "stale caller snapshot"
            }))
            .expect("synthetic stale prompt snapshot"),
        );
        let before = serde_json::to_vec(&session).expect("serialize retryable legacy state");

        let error = prepare_external_session_for_execution(&mut session, None)
            .await
            .expect_err("assigned external execution requires resolver authority");

        assert!(matches!(error, AgentError::ProjectContext(_)));
        assert_eq!(
            serde_json::to_vec(&session).expect("serialize failed legacy state"),
            before,
            "missing resolver must leave the complete serialized Session state unchanged"
        );
        assert!(session.messages[0].content.contains(&display));
        assert!(session.messages[0]
            .content
            .contains("BAMBOO_WORKSPACE_CONTEXT"));
        assert_eq!(
            session
                .metadata
                .get(crate::session_app::respond::PERMISSION_REEXECUTE_METADATA_KEY)
                .map(String::as_str),
            Some("retryable-tool-call")
        );
        assert!(session.prompt_snapshot.is_some());
    }
}
