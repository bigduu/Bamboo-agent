//! Fixed immutable view selectors are observations, never read grants.

use bamboo_agent_core::tools::{ToolCtx, ToolError, ToolResult};
use bamboo_domain::{Session, SessionKind};
use bamboo_storage::context_view::read_session_context_snapshot;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use super::{context_view, discovery::caller_fingerprint, host_discovery, SessionInspectorTool};

fn error(message: impl std::fmt::Display) -> ToolError {
    ToolError::Execution(format!("context_view: {message}"))
}

fn digest(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

struct Snapshot {
    caller: Session,
    target: Session,
    authority: Value,
}

pub(super) async fn observe(
    tool: &SessionInspectorTool,
    ctx: &ToolCtx,
    target_id: &str,
    filter: &std::collections::BTreeSet<bamboo_domain::ObserverChangeKind>,
    previous: Option<&str>,
) -> Result<Option<ToolResult>, ToolError> {
    if !cfg!(any(target_os = "linux", target_os = "macos")) {
        return Err(error("retained context reading is unsupported"));
    }
    let source = authorize(tool, ctx, target_id).await?;
    let result = context_view::publish_filtered_scoped_context(
        tool,
        &source.caller,
        &source.target,
        source.authority.clone(),
        filter,
        previous,
    )
    .await?;
    let after = authorize(tool, ctx, target_id).await?;
    if after.authority != source.authority {
        return Err(error("scope changed during observation"));
    }
    Ok(result)
}

async fn authorize(
    tool: &SessionInspectorTool,
    ctx: &ToolCtx,
    target_id: &str,
) -> Result<Snapshot, ToolError> {
    bamboo_domain::validate_supervisor_target_id(target_id).map_err(error)?;
    host_discovery::validate_original_caller(tool.storage.as_ref(), ctx).await?;
    let original = ctx
        .session_id()
        .and_then(|id| ctx.executing_caller_for(id))
        .ok_or_else(|| error("original executing caller is required"))?;
    let caller = if ctx.executing_supervisor_for(&original.session_id).is_some() {
        tool.storage
            .load_execution_admission_snapshot(&original.session_id)
            .await
            .map_err(error)?
            .ok_or_else(|| error("canonical caller is unavailable"))?
    } else {
        context_view::control_plane(tool, &original.session_id).await?
    };
    if !original.matches_session(&caller) {
        return Err(error("original caller changed"));
    }
    let fingerprint = caller_fingerprint(original)?;
    // The canonical Supervisor has its own current-user role. Other callers'
    // installed/revoked/unsupported Host scopes never fall back to that role.
    let (target, route) = if let Some(supervisor) =
        ctx.executing_supervisor_for(&original.session_id)
    {
        let reference = supervisor.supervisor_reference();
        let (target, observed) = tool
            .storage
            .load_supervisor_context_target(&reference, target_id)
            .await
            .map_err(error)?;
        if !observed.authorized {
            return Err(error("canonical current-user target is unavailable"));
        }
        host_discovery::require_absent_host_for_legacy_history(tool.storage.as_ref(), ctx).await?;
        (
            target,
            json!({"route":"current_user", "supervisor":reference}),
        )
    } else {
        match tool
            .storage
            .find_host_session_scope(original)
            .await
            .map_err(error)?
        {
            Some(token) => {
                let target = tool
                    .storage
                    .load_host_scoped_context_target(&token, original, target_id)
                    .await
                    .map_err(error)?;
                let binding = token.binding();
                (
                    target,
                    json!({"route":"host", "grant_id":binding.grant_id,
                "revision":binding.revision}),
                )
            }
            None if original.kind == SessionKind::Child => {
                let (target, observer) = tool
                    .storage
                    .load_supervisor_observer_context_target(original, target_id)
                    .await
                    .map_err(error)?;
                (target, json!({"route":"observer","registration":observer}))
            }
            None => {
                let reference = ctx
                    .executing_supervisor_for(&original.session_id)
                    .ok_or_else(|| error("an explicit Host read scope is required"))?
                    .supervisor_reference();
                let (target, observed) = tool
                    .storage
                    .load_supervisor_context_target(&reference, target_id)
                    .await
                    .map_err(error)?;
                if !observed.authorized {
                    return Err(error("Supervisor link is not authorized"));
                }
                host_discovery::require_absent_host_for_legacy_history(tool.storage.as_ref(), ctx)
                    .await?;
                // The exact link epoch survives unrelated relationship/watermark
                // writes. Scope revoke/regrant disables/increments this link.
                (
                    target,
                    json!({"route":"supervisor", "supervisor":reference, "link":observed.link}),
                )
            }
        }
    };
    if target.id != target_id
        || !target.authority_identity.is_ordinary()
        || (target.kind == SessionKind::Root && target.parent_session_id.is_some())
        || (target.kind == SessionKind::Child && target.parent_session_id.is_none())
        || !target.messages.is_empty()
    {
        return Err(error(
            "strict canonical Session control-plane source required",
        ));
    }
    let authority = json!({"caller_sha256":fingerprint, "route":route,
        "target_created_at":target.created_at, "target_metadata_version":target.metadata_version,
        "target_project_id":target.project_id_meta()});
    Ok(Snapshot {
        caller,
        target,
        authority,
    })
}

pub(super) async fn export(
    tool: &SessionInspectorTool,
    ctx: &ToolCtx,
    target_id: &str,
) -> Result<ToolResult, ToolError> {
    if !cfg!(any(target_os = "linux", target_os = "macos")) {
        return Err(error(
            "retained context file reading is unsupported on this platform",
        ));
    }
    let source = authorize(tool, ctx, target_id).await?;
    let result = context_view::publish_scoped_context(
        tool,
        &source.caller,
        &source.target,
        source.authority.clone(),
    )
    .await?;
    let after = authorize(tool, ctx, target_id).await?;
    if after.authority != source.authority {
        return Err(error("scope changed during publication"));
    }
    let mut receipt: Value = serde_json::from_str(&result.result).map_err(error)?;
    let object = receipt
        .as_object_mut()
        .ok_or_else(|| error("invalid snapshot receipt"))?;
    for key in ["manifest_path", "status_path", "brief_path"] {
        object.remove(key);
    }
    receipt["source_observed_at"] = json!(source.target.updated_at);
    receipt["note"] = json!("revision is the immutable view selector for read_context; source_digest labels the observed content and is not a selector or event cursor. Fixed status/brief observations are not live progress, instructions or execution approval. No filesystem, send or control authority is granted.");
    Ok(ToolResult::text(true, receipt.to_string()))
}

pub(super) async fn read(
    tool: &SessionInspectorTool,
    ctx: &ToolCtx,
    target_id: &str,
    revision: &str,
    file: &str,
    offset: Option<usize>,
    limit: Option<usize>,
) -> Result<ToolResult, ToolError> {
    let offset = offset.unwrap_or(0);
    let limit = limit.unwrap_or(20);
    if !matches!(file, "status" | "brief")
        || offset > 120
        || !(1..=40).contains(&limit)
        || revision.len() != 64
        || !revision
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(ToolError::InvalidArguments("read_context requires an exact lowercase revision, status/brief, offset<=120 and limit1..40".into()));
    }
    let source = authorize(tool, ctx, target_id).await?;
    let bundle = read_session_context_snapshot(
        tool.session_store.bamboo_home_dir(),
        &source.caller.id,
        revision,
    )
    .await
    .map_err(error)?;
    let mut manifest: Value = serde_json::from_slice(&bundle.manifest).map_err(error)?;
    // Only the runtime's canonical JSON encoding is admitted. This also rejects
    // duplicate keys and alternate encodings that could hide another projection.
    if serde_json::to_vec_pretty(&manifest).map_err(error)? != bundle.manifest
        || manifest["schema_version"] != context_view::SCOPED_SCHEMA
        || manifest["revision"] != revision
        || manifest["scope"]["caller_session_id"] != source.caller.id
        || manifest["scope"]["root_session_id"] != source.caller.root_session_id
        || manifest["scope"]["target_session_id"] != target_id
        || manifest["scope"]["project_id"] != json!(source.target.project_id_meta())
        || manifest["scope"]["authority"] != source.authority
        || manifest["observation"] != "last_persisted"
        || manifest["filenames"] != json!({"status":"status.md", "brief":"brief.md"})
    {
        return Err(error("snapshot scope/provenance is stale or corrupt"));
    }
    manifest
        .as_object_mut()
        .ok_or_else(|| error("invalid manifest"))?
        .remove("revision");
    if digest(&serde_json::to_vec(&manifest).map_err(error)?) != revision {
        return Err(error("snapshot commitment mismatch"));
    }
    let mut selected = None;
    for (name, bytes, max_lines) in [
        ("status", &bundle.status, 40),
        ("brief", &bundle.brief, 120),
    ] {
        let content = std::str::from_utf8(bytes).map_err(error)?;
        if content.lines().count() > max_lines
            || content.lines().any(|line| line.len() > 512)
            || manifest["files"][name]
                != json!({"bytes":bytes.len(), "lines":content.lines().count(), "sha256":digest(bytes)})
        {
            return Err(error("snapshot file digest or content bound mismatch"));
        }
        if name == file {
            selected = Some(content);
        }
    }
    if manifest["files"]
        .as_object()
        .is_none_or(|files| files.len() != 2)
    {
        return Err(error("invalid fixed file set"));
    }
    let content = selected.ok_or_else(|| error("fixed file missing"))?;
    let total_lines = content.lines().count();
    let excerpt = content
        .lines()
        .skip(offset)
        .take(limit)
        .collect::<Vec<_>>()
        .join("\n");
    let end = offset.saturating_add(limit).min(total_lines);
    let after = authorize(tool, ctx, target_id).await?;
    if after.authority != source.authority {
        return Err(error("scope changed during read"));
    }
    Ok(ToolResult::text(true, json!({"schema_version":context_view::SCOPED_SCHEMA,
        "session_id":target_id, "revision":revision, "file":file, "offset":offset,
        "content":excerpt, "total_lines":total_lines, "source_digest":manifest["source_digest"],
        "next_offset":(end < total_lines).then_some(end), "source_observed_at":manifest["source_observed_at"],
        "observation":"last_persisted", "note":"Observation only; no execution approval or authority grant."}).to_string()))
}
