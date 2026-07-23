use bamboo_agent_core::tools::{ToolCall, ToolResult};
use bamboo_agent_core::{AgentEvent, Session};
use tokio::sync::mpsc;

pub(super) async fn maybe_apply_workspace_update(
    session: &mut Session,
    tool_call: &ToolCall,
    result: &ToolResult,
    session_id: &str,
    project_resolver: Option<&crate::project_context::ProjectContextResolver>,
    event_tx: &mpsc::Sender<AgentEvent>,
) {
    if let Some(mut update) =
        super::super::super::workspace_context::extract_workspace_path_from_tool_result(
            tool_call, result,
        )
    {
        if super::super::super::workspace_context::should_apply_workspace_update(session, tool_call)
        {
            // The server's Project-aware Workspace tool performs this exact
            // ownership check against the confinement-resolved destination
            // before mutating global workspace state. Trust its structured
            // result so a registry race after successful invocation cannot
            // leave global state changed while the live session refuses the
            // same update. Implicit Write/Edit relocations still need the
            // runner-side check because those tools do not own Workspace CAS.
            let explicit_workspace_tool =
                super::super::super::workspace_context::is_explicit_workspace_tool(tool_call);
            if !explicit_workspace_tool {
                if let Some(resolver) = project_resolver {
                    let workspace = std::path::Path::new(&update.path);
                    match resolver.workspace_owner(workspace).await {
                        Ok(owner) => {
                            let current = match crate::project_context::ProjectContextResolver::session_project_identity(session) {
                                crate::project_context::SessionProjectIdentity::Assigned(project_id) => Some(project_id),
                                crate::project_context::SessionProjectIdentity::Unassigned => None,
                                crate::project_context::SessionProjectIdentity::Invalid { raw, message } => {
                                    let error = serde_json::json!({
                                        "code": "invalid_project_identity",
                                        "message": format!(
                                            "Session carries an invalid Project identity '{raw}': {message}"
                                        ),
                                        "workspace": update.path,
                                    })
                                    .to_string();
                                    let _ = event_tx
                                        .send(AgentEvent::ToolError {
                                            tool_call_id: tool_call.id.clone(),
                                            error,
                                        })
                                        .await;
                                    return;
                                }
                            };
                            if owner.is_some() && owner != current {
                                let error = serde_json::json!({
                                "code": "project_workspace_conflict",
                                "message": "Tool result workspace belongs to another Project; session workspace was not changed",
                                "workspace": update.path,
                                "owner_project_id": owner,
                                "session_project_id": current,
                            })
                            .to_string();
                                tracing::warn!(
                                    session_id,
                                    tool = %tool_call.function.name,
                                    %error,
                                    "blocked implicit cross-Project workspace update"
                                );
                                let _ = event_tx
                                    .send(AgentEvent::ToolError {
                                        tool_call_id: tool_call.id.clone(),
                                        error,
                                    })
                                    .await;
                                return;
                            }
                            if owner.is_some() && owner == current {
                                update.binding_status =
                                    crate::project_context::WorkspaceBindingStatus::Registered;
                            }
                        }
                        Err(error) => {
                            tracing::warn!(
                                session_id,
                                tool = %tool_call.function.name,
                                %error,
                                "failed closed while checking implicit workspace ownership"
                            );
                            return;
                        }
                    }
                }
            }
            super::super::super::workspace_context::apply_workspace_path_to_session(
                session,
                &update.path,
                update.binding_status,
            );
            tracing::info!(
                "[{}] Updated session workspace_path via {}: {}",
                session_id,
                tool_call.function.name,
                update.path
            );
        }
    }
}
