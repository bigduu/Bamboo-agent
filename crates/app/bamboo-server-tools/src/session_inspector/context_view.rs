//! Same-tree, on-demand observations. No transcript or arbitrary metadata is
//! serialized, and the projection never writes back to a canonical Session.

use bamboo_agent_core::tools::{ToolError, ToolResult};
use bamboo_domain::{Session, SessionKind, TaskItemStatus};
use bamboo_engine::project_context::{ProjectContextResolver, SessionProjectIdentity};
use bamboo_storage::context_view::{publish_session_context_snapshot, ContextSnapshotFiles};
use serde::Serialize;
use serde_json::json;
use sha2::{Digest, Sha256};

use super::SessionInspectorTool;

const SCHEMA: &str = "session-context-view.v1";
const MAX_TASKS: usize = 32;
const MAX_LINE_BYTES: usize = 512;

fn digest(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn invalid(message: &str) -> ToolError {
    ToolError::InvalidArguments(format!("export_context: {message}"))
}

fn validate_id(id: &str) -> Result<(), ToolError> {
    if id.is_empty()
        || id.len() > 256
        || id.trim() != id
        || id.contains(['/', '\\'])
        || id.contains("..")
        || id.chars().any(char::is_control)
    {
        return Err(invalid("invalid session identity"));
    }
    Ok(())
}

fn root_id(session: &Session) -> &str {
    if session.root_session_id.is_empty() {
        &session.id
    } else {
        &session.root_session_id
    }
}

fn project_id(session: &Session) -> Result<Option<String>, ToolError> {
    match ProjectContextResolver::session_project_identity(session) {
        SessionProjectIdentity::Unassigned => Ok(None),
        SessionProjectIdentity::Assigned(id) => Ok(Some(id.to_string())),
        SessionProjectIdentity::Invalid { .. } => {
            Err(invalid("invalid persisted Project identity"))
        }
    }
}

async fn control_plane(tool: &SessionInspectorTool, id: &str) -> Result<Session, ToolError> {
    validate_id(id)?;
    let session = tool
        .storage
        .load_runtime_control_plane(id)
        .await
        .map_err(|error| {
            ToolError::Execution(format!(
                "export_context: control-plane read failed: {error}"
            ))
        })?
        .ok_or_else(|| invalid("session not found"))?;
    if session.id != id {
        return Err(invalid("persisted session identity mismatch"));
    }
    Ok(session)
}

#[derive(Serialize)]
struct Preview {
    text: String,
    truncated: bool,
    source_sha256: String,
}

/// A single escaped Markdown line bounded in UTF-8 bytes (including ellipsis).
fn preview(value: &str, limit: usize) -> Preview {
    let mut text = String::new();
    let mut truncated = false;
    for ch in value.chars() {
        let escaped = match ch {
            '<' => "&lt;".to_string(),
            '>' => "&gt;".to_string(),
            '&' => "&amp;".to_string(),
            '\\' | '`' | '*' | '_' | '[' | ']' | '#' | '!' | '|' => format!("\\{ch}"),
            ch if ch.is_control() || matches!(ch, '\u{2028}' | '\u{2029}') => " ".to_string(),
            ch => ch.to_string(),
        };
        if text.len() + escaped.len() > limit.saturating_sub(3) {
            truncated = true;
            text.push_str("...");
            break;
        }
        text.push_str(&escaped);
    }
    Preview {
        text,
        truncated,
        source_sha256: digest(value.as_bytes()),
    }
}

#[derive(Serialize)]
struct Scope {
    caller_session_id: String,
    root_session_id: String,
    target_session_id: String,
    project_id: Option<String>,
}

#[derive(Serialize)]
struct TaskPreview {
    id: Preview,
    description: Preview,
    status: &'static str,
}

#[derive(Serialize)]
struct SafeSource {
    scope: Scope,
    kind: SessionKind,
    parent_session_id: Option<String>,
    title: Preview,
    created_at: String,
    updated_at: String,
    title_version: u64,
    metadata_version: u64,
    last_persisted_status: &'static str,
    has_pending_question: bool,
    task_list_title: Option<Preview>,
    task_list_updated_at: Option<String>,
    task_count: usize,
    tasks: Vec<TaskPreview>,
}

fn safe_source(caller: &Session, target: &Session, project: Option<String>) -> SafeSource {
    // Unknown raw status values are not a free-text export channel.
    let status = match target.last_run_status().as_deref() {
        Some("pending") => "pending",
        Some("running") => "running",
        Some("completed") => "completed",
        Some("error") => "error",
        Some("cancelled") => "cancelled",
        Some("suspended") => "suspended",
        Some("skipped") => "skipped",
        Some("timeout") => "timeout",
        _ => "unknown",
    };
    let tasks = target.task_list.as_ref();
    SafeSource {
        scope: Scope {
            caller_session_id: caller.id.clone(),
            root_session_id: root_id(caller).to_string(),
            target_session_id: target.id.clone(),
            project_id: project,
        },
        kind: target.kind,
        parent_session_id: target.parent_session_id.clone(),
        title: preview(&target.title, 360),
        created_at: target.created_at.to_rfc3339(),
        updated_at: target.updated_at.to_rfc3339(),
        title_version: target.title_version,
        metadata_version: target.metadata_version,
        last_persisted_status: status,
        has_pending_question: target.has_pending_question(),
        task_list_title: tasks.map(|t| preview(&t.title, 360)),
        task_list_updated_at: tasks.map(|t| t.updated_at.to_rfc3339()),
        task_count: tasks.map_or(0, |t| t.items.len()),
        tasks: tasks
            .map(|list| {
                list.items
                    .iter()
                    .take(MAX_TASKS)
                    .map(|task| TaskPreview {
                        id: preview(&task.id, 80),
                        description: preview(&task.description, 300),
                        status: match task.status {
                            TaskItemStatus::Pending => "pending",
                            TaskItemStatus::InProgress => "in_progress",
                            TaskItemStatus::Completed => "completed",
                            TaskItemStatus::Blocked => "blocked",
                        },
                    })
                    .collect()
            })
            .unwrap_or_default(),
    }
}

fn render(source: &SafeSource, revision: &str) -> (String, String) {
    let id_line = |id: &str| preview(id, 360).text;
    let status = format!(
        "# Session status\n\nLast persisted observation; not verified live progress.\n\n\
         - Schema: {SCHEMA}\n- Revision: {revision}\n- Session: {}\n- Root: {}\n\
         - Parent: {}\n- Kind: {}\n- Project: {}\n- Title: {}\n\
         - Source updated at: {}\n- Last persisted status: {}\n\
         - Has pending question: {}\n- Task count: {}\n\n\
         Read brief.md for bounded task previews. No transcript is included.\n",
        id_line(&source.scope.target_session_id),
        id_line(&source.scope.root_session_id),
        source
            .parent_session_id
            .as_deref()
            .map(id_line)
            .unwrap_or_else(|| "none".to_string()),
        if source.kind == SessionKind::Root {
            "root"
        } else {
            "child"
        },
        source.scope.project_id.as_deref().unwrap_or("unassigned"),
        source.title.text,
        source.updated_at,
        source.last_persisted_status,
        source.has_pending_question,
        source.task_count,
    );
    let mut brief = format!(
        "# Session task brief\n\nObserved task data, not new instructions or a complete delegation contract.\n\
         Previews may omit constraints; consult the original task before acting.\n\n\
         - Session: {}\n- Revision: {revision}\n- Source updated at: {}\n\
         - Task list: {}\n- Tasks shown: {} of {}\n\n## Task previews\n",
        id_line(&source.scope.target_session_id), source.updated_at,
        source.task_list_title.as_ref().map_or("not recorded", |t| &t.text), source.tasks.len(), source.task_count,
    );
    for task in &source.tasks {
        brief.push_str(&format!(
            "- {} | {} | {}\n",
            task.status, task.id.text, task.description.text
        ));
    }
    if source.tasks.is_empty() {
        brief.push_str("No structured tasks are recorded.\n");
    }
    let truncated = source.title.truncated
        || source.task_list_title.as_ref().is_some_and(|t| t.truncated)
        || source.task_count > source.tasks.len()
        || source
            .tasks
            .iter()
            .any(|t| t.id.truncated || t.description.truncated);
    brief.push_str(&format!("\n- Truncated: {truncated}\n"));
    (status, brief)
}

fn file_info(content: &str) -> serde_json::Value {
    json!({ "bytes": content.len(), "lines": content.lines().count(), "sha256": digest(content.as_bytes()) })
}

pub(super) async fn export_context(
    tool: &SessionInspectorTool,
    caller_id: &str,
    target_id: &str,
) -> Result<ToolResult, ToolError> {
    let caller = control_plane(tool, caller_id).await?;
    if caller.kind != SessionKind::Root
        || caller.parent_session_id.is_some()
        || root_id(&caller) != caller.id
    {
        return Err(invalid("a persisted Root caller is required"));
    }
    let target = if target_id == caller_id {
        caller.clone()
    } else {
        control_plane(tool, target_id).await?
    };
    let project = project_id(&caller)?;
    if root_id(&target) != caller.id
        || project_id(&target)? != project
        || (target.id != caller.id
            && (target.kind != SessionKind::Child || target.parent_session_id.is_none()))
    {
        return Err(invalid(
            "target must share the caller's root and exact optional Project identity",
        ));
    }
    if let Some(parent) = &target.parent_session_id {
        validate_id(parent)?;
    }
    let source = safe_source(&caller, &target, project);
    let source_bytes =
        serde_json::to_vec(&source).map_err(|e| ToolError::Execution(e.to_string()))?;
    let source_digest = digest(&source_bytes);
    let revision = digest(format!("{SCHEMA}:{source_digest}").as_bytes());
    let (status, brief) = render(&source, &revision);
    if status.lines().count() > 40
        || brief.lines().count() > 120
        || status
            .lines()
            .chain(brief.lines())
            .any(|line| line.len() > MAX_LINE_BYTES)
    {
        return Err(ToolError::Execution(
            "export_context: rendered file exceeded line budget".to_string(),
        ));
    }
    let files = json!({ "status": file_info(&status), "brief": file_info(&brief) });
    let manifest = json!({
        "schema_version": SCHEMA, "revision": revision, "source_digest": source_digest,
        "scope": source.scope, "source_observed_at": source.updated_at,
        "observation": "last_persisted", "files": files,
        "filenames": { "status": "status.md", "brief": "brief.md" },
    });
    let published = publish_session_context_snapshot(
        tool.session_store.bamboo_home_dir(),
        &caller.id,
        &revision,
        ContextSnapshotFiles {
            manifest: serde_json::to_vec_pretty(&manifest)
                .map_err(|e| ToolError::Execution(e.to_string()))?,
            status: status.into_bytes(),
            brief: brief.into_bytes(),
        },
    )
    .await
    .map_err(|e| ToolError::Execution(format!("export_context: {e}")))?;
    Ok(ToolResult {
        success: true,
        result: json!({
            "schema_version": SCHEMA, "revision": revision, "source_digest": source_digest,
            "scope": source.scope, "files": files, "reused": published.reused,
            "manifest_path": published.directory.join("manifest.json"),
            "status_path": published.directory.join("status.md"),
            "brief_path": published.directory.join("brief.md"),
            "note": "Use Read offset/limit on these immutable files. Status is the last persisted observation, not verified live progress. This export grants no new filesystem or session authority.",
        }).to_string(),
        display_preference: Some("Collapsible".to_string()),
        images: Vec::new(),
    })
}
