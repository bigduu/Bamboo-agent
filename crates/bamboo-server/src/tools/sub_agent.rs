use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;
use uuid::Uuid;

use bamboo_engine::session_app::child_session::{self, ChildSessionPort, CreateChildInput};
use crate::tools::child_session_adapter::{tool_error_from_child_session, ChildSessionAdapter};
use bamboo_agent_core::tools::{Tool, ToolError, ToolExecutionContext, ToolResult};
use bamboo_domain::session::runtime_state::ChildWaitPolicy;
use bamboo_domain::subagent::SubagentProfileRegistry;
use bamboo_domain::ReasoningEffort;

// ---------------------------------------------------------------------------
// Args enum
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
enum SubAgentArgs {
    Create {
        #[serde(default)]
        title: Option<String>,
        #[serde(default)]
        description: String,
        #[serde(default)]
        responsibility: Option<String>,
        prompt: String,
        /// Subagent profile/role. Optional: defaults to `general-purpose` when
        /// omitted or empty, so a missing value never hard-fails a create.
        #[serde(default)]
        subagent_type: Option<String>,
        /// Working directory for the child. Optional: defaults to the parent
        /// session's workspace when omitted.
        #[serde(default)]
        workspace: Option<String>,
        #[serde(default)]
        auto_run: Option<bool>,
        /// When `true`, the parent suspends immediately and waits for THIS child
        /// to finish (the legacy one-shot behavior). Defaults to `false`:
        /// `create` runs the child in the background and returns right away so
        /// the parent can spawn more children. Call `action=wait` once, after
        /// spawning everything, to suspend until they finish.
        #[serde(default)]
        wait: Option<bool>,
        /// Optional reasoning effort for the child session. When omitted,
        /// the child stays at `None` so the provider's default applies
        /// (it does NOT inherit the parent's reasoning_effort). The LLM
        /// should pass an explicit value (e.g. `"low"` for cheap fan-outs,
        /// `"high"`/`"max"` for hard reasoning) when it has a preference.
        #[serde(default)]
        reasoning_effort: Option<ReasoningEffort>,
    },
    /// Suspend the parent run until its background child sessions finish.
    ///
    /// Spawn children with `action=create` (which no longer suspends), then call
    /// this once. By default it waits on every currently-active child; pass
    /// explicit `child_session_ids` to wait on a subset. If no children are
    /// active it is a no-op (the parent keeps running).
    Wait {
        #[serde(default)]
        child_session_ids: Option<Vec<String>>,
        /// Wait policy: `all` (default) resumes when every tracked child is
        /// terminal; `any` resumes on the first; `first_error` resumes early on
        /// any error/timeout/cancel.
        #[serde(default)]
        wait_for: Option<ChildWaitPolicy>,
    },
    List,
    Get {
        child_session_id: String,
    },
    Update {
        child_session_id: String,
        #[serde(default)]
        title: Option<String>,
        #[serde(default)]
        responsibility: Option<String>,
        #[serde(default)]
        prompt: Option<String>,
        #[serde(default)]
        subagent_type: Option<String>,
        #[serde(default)]
        reset_after_update: Option<bool>,
        #[serde(default)]
        auto_run: Option<bool>,
        /// Optional reasoning effort to apply to the existing child session.
        /// `Some(level)` overrides the current value; `None` (the default)
        /// leaves it unchanged.
        #[serde(default)]
        reasoning_effort: Option<ReasoningEffort>,
    },
    Run {
        child_session_id: String,
        #[serde(default)]
        reset_to_last_user: Option<bool>,
    },
    SendMessage {
        child_session_id: String,
        message: String,
        #[serde(default)]
        auto_run: Option<bool>,
        #[serde(default)]
        interrupt_running: Option<bool>,
    },
    Cancel {
        child_session_id: String,
    },
    Delete {
        child_session_id: String,
    },
    /// Enumerate the available subagent profiles (built-ins plus any
    /// user/project overrides). Read-only; does not touch any session.
    /// Useful both for the LLM (to discover roles before calling
    /// `create`) and for the frontend (to populate a role dropdown).
    ListProfiles,
}

// ---------------------------------------------------------------------------
// Normalization helpers (ported from legacy SpawnSessionTool)
// ---------------------------------------------------------------------------

fn normalize_required_text(value: Option<String>, field_name: &str) -> Result<String, ToolError> {
    let Some(value) = value else {
        return Err(ToolError::InvalidArguments(format!(
            "{field_name} must be non-empty"
        )));
    };
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(ToolError::InvalidArguments(format!(
            "{field_name} must be non-empty"
        )));
    }
    Ok(trimmed.to_string())
}

fn normalize_title(title: Option<String>, legacy_description: String) -> Result<String, ToolError> {
    let title = title.and_then(|value| {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    });
    let legacy_description = {
        let trimmed = legacy_description.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    };
    normalize_required_text(title.or(legacy_description), "title")
}

fn tool_result(value: serde_json::Value) -> Result<ToolResult, ToolError> {
    Ok(ToolResult {
        success: true,
        result: value.to_string(),
        display_preference: Some("Collapsible".to_string()),
    })
}

fn waiting_for_children_tool_result(mut value: serde_json::Value) -> Result<ToolResult, ToolError> {
    if let Some(object) = value.as_object_mut() {
        object.insert("runtime_control".to_string(), json!("waiting_for_children"));
        // Don't clobber a caller-provided policy (e.g. action=wait with
        // wait_for=any); only default it when absent.
        object
            .entry("wait_for".to_string())
            .or_insert_with(|| json!("all"));
        object.insert(
            "note".to_string(),
            json!("Child session queued. The parent run is suspended and will resume automatically when the child finishes or times out."),
        );
    }

    Ok(ToolResult {
        success: true,
        result: value.to_string(),
        display_preference: Some("runtime_control:waiting_for_children".to_string()),
    })
}

// ---------------------------------------------------------------------------
// Tool struct
// ---------------------------------------------------------------------------

pub struct SubAgentTool {
    adapter: Arc<ChildSessionAdapter>,
    /// Registry consulted by `action=list_profiles`. Held as `Arc` so the
    /// tool stays cheap to clone and share across executors.
    profiles: Arc<SubagentProfileRegistry>,
}

impl SubAgentTool {
    pub fn new(adapter: Arc<ChildSessionAdapter>, profiles: Arc<SubagentProfileRegistry>) -> Self {
        Self { adapter, profiles }
    }
}

#[async_trait]
impl Tool for SubAgentTool {
    fn name(&self) -> &str {
        "SubAgent"
    }

    fn description(&self) -> &str {
        "Create, inspect, and manage child sessions for explicitly requested delegated, parallel, or sub-agent work. A child session runs independently under the current root session with its own conversation context, can use a specialized subagent profile, streams progress back to the parent via sub_agent_* events, and can be reopened from the Sub-agents panel. \
PARALLEL FAN-OUT (important): action=create now runs the child in the BACKGROUND and returns immediately WITHOUT suspending the parent. To launch several agents in parallel, call create once per child (ideally several creates in a single turn), then call action=wait ONCE to suspend until they finish. Do NOT pass wait=true on each create for parallel work — that would serialize them (suspend after the first). action=wait defaults to waiting on every active child; if you forget to call it, the runtime auto-waits at the end of the turn so results are never lost. \
Use list/get to inspect existing children; use update/run/send_message/cancel/delete to manage existing children; use list_profiles to enumerate subagent roles. Use only when the user explicitly asks for delegation/parallelism or when a side task would otherwise flood the main context. Do not use for simple one-step tasks. Child sessions cannot spawn nested child sessions. IMPORTANT: When a child fails or needs redirection, prefer send_message over creating a duplicate child. Use list before create to avoid spawning redundant children."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["create", "wait", "list", "get", "update", "run", "send_message", "cancel", "delete", "list_profiles"],
                    "description": "Sub-agent lifecycle operation. To run work in parallel: call create once per child (this no longer suspends the parent — children run in the background), then call wait ONCE to suspend until they all finish. Use list/get to inspect; update/run/send_message/cancel/delete to manage existing children; list_profiles to enumerate available subagent roles before choosing subagent_type. \
A create call requires: title, responsibility, prompt, and subagent_type (workspace and subagent_type are optional and default to the parent's workspace / general-purpose). EXAMPLE create: {\"action\":\"create\",\"subagent_type\":\"researcher\",\"title\":\"Analyze auth module\",\"responsibility\":\"Map the auth flow and list its public API\",\"prompt\":\"Read crates/auth/src/lib.rs, summarize the login flow, and list every pub fn.\",\"workspace\":\"/abs/path/to/repo\"}. Then EXAMPLE wait: {\"action\":\"wait\"}."
                },
                "child_session_id": {
                    "type": "string",
                    "description": "Existing child session id. Required for get/update/run/send_message/cancel/delete."
                },
                "child_session_ids": {
                    "type": "array",
                    "items": { "type": "string" },
                    "description": "For wait: optional explicit subset of child sessions to wait on. Omit to wait on every currently-active child."
                },
                "wait_for": {
                    "type": "string",
                    "enum": ["all", "any", "first_error"],
                    "description": "For wait: resume policy. all (default) resumes when every tracked child is done; any resumes on the first; first_error resumes early on any error/timeout/cancel."
                },
                "wait": {
                    "type": "boolean",
                    "description": "For create: if true, suspend immediately and wait for just THIS child (legacy one-shot behavior). Defaults to false — create returns immediately and the child runs in the background; suspend later with action=wait."
                },
                "title": {
                    "type": "string",
                    "description": "Short title for a new or updated child session. Required for create. Displayed in the Sub-agents panel."
                },
                "description": {
                    "type": "string",
                    "description": "Legacy alias of title; prefer title."
                },
                "responsibility": {
                    "type": "string",
                    "description": "Single explicit responsibility for the child session. Required for create. Keep this narrow and non-overlapping with other child sessions."
                },
                "prompt": {
                    "type": "string",
                    "description": "Detailed task instructions, context, constraints, and expected output for the child session. Required for create; optional for update."
                },
                "subagent_type": {
                    "type": "string",
                    "description": "For create: the specialized child agent profile/role, e.g. general-purpose, researcher, coder, plan. Use plan/researcher for read-only exploration and coder/general-purpose for implementation when allowed. Optional — omitting it defaults to general-purpose — but you should pick the most fitting role. Call list_profiles to see the available roles."
                },
                "workspace": {
                    "type": "string",
                    "description": "For create: absolute path to the child session's working directory for file operations. Optional — defaults to the parent session's workspace when omitted."
                },
                "auto_run": {
                    "type": "boolean",
                    "description": "For create/send_message/update: whether to enqueue the child session immediately. Defaults to true for create/send_message and false for update."
                },
                "reset_after_update": {
                    "type": "boolean",
                    "description": "For update: whether to truncate messages after refreshed assignment. Defaults to true."
                },
                "reset_to_last_user": {
                    "type": "boolean",
                    "description": "For run: whether to truncate messages after the last user message before rerun. Defaults to true."
                },
                "message": {
                    "type": "string",
                    "description": "Follow-up instruction to append as a new user message for send_message. Required for send_message."
                },
                "interrupt_running": {
                    "type": "boolean",
                    "description": "For send_message/cancel: if true, cancel a currently running child session before appending or returning. Defaults to false for send_message. When false on a running child, the message is queued and will be picked up at the next turn boundary without canceling progress."
                },
                "reasoning_effort": {
                    "type": "string",
                    "enum": ["low", "medium", "high", "xhigh", "max"],
                    "description": "For create/update: reasoning effort level applied to the child session's own LLM calls. Use \"low\" for trivial fan-outs (e.g. simple lookups), \"medium\"/\"high\" for normal coding/analysis, \"xhigh\"/\"max\" for deep reasoning tasks. Omit to leave at provider default; the child does NOT inherit the parent's reasoning_effort."
                }
            },
            "required": ["action"],
            "additionalProperties": false
        })
    }

    async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError> {
        self.execute_with_context(args, ToolExecutionContext::none("tool_call"))
            .await
    }

    async fn execute_with_context(
        &self,
        args: serde_json::Value,
        ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolResult, ToolError> {
        let parent_session_id = ctx.session_id.ok_or_else(|| {
            ToolError::Execution("SubAgent requires a session_id in tool context".to_string())
        })?;

        // Backward compatibility: legacy SubAgent calls did not include an
        // "action" field and always meant "create". If action is missing,
        // default to "create" before deserializing the tagged enum.
        let mut args = args;
        if args.get("action").is_none() {
            args["action"] = json!("create");
        }

        let parsed: SubAgentArgs = serde_json::from_value(args).map_err(|error| {
            ToolError::InvalidArguments(format!("Invalid SubAgent args: {error}"))
        })?;

        // `list_profiles` is read-only and operates purely on the
        // in-memory profile registry, so we short-circuit before doing
        // any session lookup. This also lets the LLM call `list_profiles`
        // safely from any context (root or otherwise).
        if let SubAgentArgs::ListProfiles = parsed {
            return tool_result(self.list_profiles_payload());
        }

        let parent = self
            .adapter
            .as_ref()
            .load_root_session(parent_session_id)
            .await
            .map_err(tool_error_from_child_session)?;

        match parsed {
            SubAgentArgs::Create {
                title,
                description,
                responsibility,
                prompt,
                subagent_type,
                workspace,
                auto_run,
                wait,
                reasoning_effort,
            } => {
                let title = normalize_title(title, description)?;
                let responsibility = normalize_required_text(responsibility, "responsibility")?;
                let prompt = normalize_required_text(Some(prompt), "prompt")?;
                // subagent_type is optional: an omitted/blank value falls back to
                // the catch-all `general-purpose` profile (the same fallback
                // resolve_subagent_prompt already applies), so the model never
                // hard-fails a create just for leaving the role unspecified.
                let subagent_type = subagent_type
                    .map(|value| value.trim().to_string())
                    .filter(|value| !value.is_empty())
                    .unwrap_or_else(|| "general-purpose".to_string());
                // workspace is optional: default to the parent's workspace.
                let workspace = workspace
                    .map(|value| value.trim().to_string())
                    .filter(|value| !value.is_empty())
                    .or_else(|| parent.workspace.clone())
                    .ok_or_else(|| {
                        ToolError::InvalidArguments(
                            "workspace must be non-empty (parent has no workspace to inherit)"
                                .to_string(),
                        )
                    })?;

                if parent.model.trim().is_empty() {
                    return Err(ToolError::Execution(
                        "parent session model is empty".to_string(),
                    ));
                }

                let child_id = Uuid::new_v4().to_string();
                let model_ref_override = self.adapter.resolve_subagent_model(&subagent_type).await;
                let model_override = model_ref_override
                    .as_ref()
                    .map(|model_ref| model_ref.model.clone());
                let runtime_metadata = self.adapter.resolve_runtime_metadata(&subagent_type).await;
                let system_prompt_override =
                    Some(self.adapter.resolve_subagent_prompt(&subagent_type));

                let should_auto_run = auto_run.unwrap_or(true);
                let result = child_session::create_child_action(
                    self.adapter.as_ref(),
                    CreateChildInput {
                        parent_session: parent.clone(),
                        child_id: child_id.clone(),
                        title: title.clone(),
                        responsibility: responsibility.clone(),
                        assignment_prompt: prompt.clone(),
                        subagent_type: subagent_type.clone(),
                        workspace: workspace.clone(),
                        model_override,
                        model_ref_override,
                        runtime_metadata,
                        system_prompt_override,
                        auto_run: should_auto_run,
                        reasoning_effort,
                    },
                )
                .await
                .map_err(tool_error_from_child_session)?;

                // Ensure index entry is visible immediately (best-effort).
                let _ = self
                    .adapter
                    .session_store
                    .get_index_entry(&result.child_session_id)
                    .await;

                ctx.emit_tool_token(format!(
                    "Spawned child session: {}",
                    result.child_session_id
                ))
                .await;

                // `wait=true` preserves the legacy one-shot behavior: register a
                // wait for THIS child and suspend now. Default (`wait=false`) runs
                // the child in the background and returns immediately, so the
                // parent can keep spawning; it suspends later via `action=wait`.
                let should_wait = should_auto_run && wait.unwrap_or(false);
                if should_wait {
                    self.adapter
                        .register_parent_wait_for_child(
                            &parent.id,
                            &result.child_session_id,
                            None,
                        )
                        .await
                        .map_err(tool_error_from_child_session)?;
                }

                let status = if !should_auto_run {
                    "created"
                } else if should_wait {
                    "queued"
                } else {
                    "running_in_background"
                };
                let note = if should_wait {
                    "Child session queued (typically 30-120 seconds); the parent is suspended until it finishes. Use send_message (not create) to correct a child in place."
                } else if should_auto_run {
                    "Child session is running in the background (typically 30-120 seconds). Spawn any other children you need, then call action=wait once to suspend until they finish. Use send_message (not create) to correct a child in place."
                } else {
                    "Child session created (not started). Use action=run to start it. Use send_message (not create) to correct a child in place."
                };
                let payload = json!({
                    "title": title.clone(),
                    "description": title,
                    "responsibility": responsibility,
                    "prompt": prompt,
                    "subagent_type": subagent_type,
                    "child_session_id": result.child_session_id,
                    "parent_session_id": parent_session_id,
                    "model": result.model,
                    "reasoning_effort": reasoning_effort.map(|effort| effort.as_str()),
                    "status": status,
                    "note": note,
                });
                if should_wait {
                    waiting_for_children_tool_result(payload)
                } else {
                    tool_result(payload)
                }
            }
            SubAgentArgs::Wait {
                child_session_ids,
                wait_for,
            } => {
                let policy = wait_for.unwrap_or(ChildWaitPolicy::All);
                // Default to every currently-active child; honor an explicit
                // subset when provided.
                let targets = match child_session_ids {
                    Some(ids) if !ids.is_empty() => ids,
                    _ => self.adapter.active_child_ids(&parent.id).await,
                };

                if targets.is_empty() {
                    // Nothing to wait on — never register an empty wait (that
                    // would suspend the parent with no child able to resume it).
                    return tool_result(json!({
                        "status": "no_active_children",
                        "parent_session_id": parent_session_id,
                        "note": "No active child sessions to wait for; the parent continues running.",
                    }));
                }

                let count = self
                    .adapter
                    .register_parent_wait_for_children(&parent.id, &targets, policy)
                    .await
                    .map_err(tool_error_from_child_session)?;

                waiting_for_children_tool_result(json!({
                    "status": "waiting",
                    "parent_session_id": parent_session_id,
                    "child_session_ids": targets,
                    "wait_for": policy.as_str(),
                    "waiting_on": count,
                }))
            }
            SubAgentArgs::List => {
                let result =
                    child_session::list_children_action(self.adapter.as_ref(), &parent.id).await;
                tool_result(result)
            }
            SubAgentArgs::Get { child_session_id } => {
                let result = child_session::get_child_action(
                    self.adapter.as_ref(),
                    &parent.id,
                    child_session_id,
                )
                .await
                .map_err(tool_error_from_child_session)?;
                tool_result(result)
            }
            SubAgentArgs::Update {
                child_session_id,
                title,
                responsibility,
                prompt,
                subagent_type,
                reset_after_update,
                auto_run,
                reasoning_effort,
            } => {
                let result = child_session::update_child_action(
                    self.adapter.as_ref(),
                    &parent.id,
                    child_session_id.clone(),
                    title,
                    responsibility,
                    prompt,
                    subagent_type,
                    reset_after_update,
                    reasoning_effort,
                )
                .await
                .map_err(tool_error_from_child_session)?;

                let should_auto_run = auto_run.unwrap_or(false);
                if should_auto_run {
                    let child = self
                        .adapter
                        .load_child_for_parent(&parent.id, &child_session_id)
                        .await
                        .map_err(tool_error_from_child_session)?;
                    self.adapter
                        .enqueue_child_run(&parent, &child)
                        .await
                        .map_err(tool_error_from_child_session)?;
                    // Re-running an existing child keeps its synchronous "wait for
                    // the answer" semantics: register the wait + suspend. (enqueue
                    // itself no longer registers — that is now explicit.)
                    self.adapter
                        .register_parent_wait_for_child(&parent.id, &child_session_id, None)
                        .await
                        .map_err(tool_error_from_child_session)?;
                }

                if should_auto_run {
                    waiting_for_children_tool_result(result)
                } else {
                    tool_result(result)
                }
            }
            SubAgentArgs::Run {
                child_session_id,
                reset_to_last_user,
            } => {
                let result = child_session::run_child_action(
                    self.adapter.as_ref(),
                    &parent,
                    child_session_id.clone(),
                    reset_to_last_user,
                )
                .await
                .map_err(tool_error_from_child_session)?;
                // `run` keeps the synchronous retry semantics: wait for this child.
                self.adapter
                    .register_parent_wait_for_child(&parent.id, &child_session_id, None)
                    .await
                    .map_err(tool_error_from_child_session)?;
                waiting_for_children_tool_result(result)
            }
            SubAgentArgs::SendMessage {
                child_session_id,
                message,
                auto_run,
                interrupt_running,
            } => {
                let should_auto_run = auto_run.unwrap_or(true);
                let result = child_session::send_message_to_child_action(
                    self.adapter.as_ref(),
                    &parent,
                    child_session_id.clone(),
                    message,
                    auto_run,
                    interrupt_running,
                )
                .await
                .map_err(tool_error_from_child_session)?;
                let queued = should_auto_run
                    && result
                        .get("status")
                        .and_then(|value| value.as_str())
                        .is_some_and(|status| status == "queued");
                if queued {
                    // Sending + running keeps synchronous semantics: wait for the
                    // child's response. (enqueue no longer registers the wait.)
                    self.adapter
                        .register_parent_wait_for_child(&parent.id, &child_session_id, None)
                        .await
                        .map_err(tool_error_from_child_session)?;
                    waiting_for_children_tool_result(result)
                } else {
                    tool_result(result)
                }
            }
            SubAgentArgs::Cancel { child_session_id } => {
                let result = child_session::cancel_child_action(
                    self.adapter.as_ref(),
                    &parent.id,
                    child_session_id,
                )
                .await
                .map_err(tool_error_from_child_session)?;
                tool_result(result)
            }
            SubAgentArgs::Delete { child_session_id } => {
                let result = child_session::delete_child_action(
                    self.adapter.as_ref(),
                    &parent.id,
                    child_session_id,
                )
                .await
                .map_err(tool_error_from_child_session)?;
                tool_result(result)
            }
            // Already short-circuited above; kept here so the match stays
            // exhaustive without a wildcard.
            SubAgentArgs::ListProfiles => tool_result(self.list_profiles_payload()),
        }
    }
}

impl SubAgentTool {
    /// Build the JSON payload returned by `action=list_profiles`.
    ///
    /// Shape (kept stable as a public contract for the frontend and for
    /// the LLM):
    ///
    /// ```jsonc
    /// {
    ///   "profiles": [
    ///     {
    ///       "id": "researcher",
    ///       "display_name": "Researcher",
    ///       "description": "...",
    ///       "tools": { "mode": "allowlist", "allow": ["Read", "Grep"] },
    ///       "model_hint": null,
    ///       "default_responsibility": null,
    ///       "ui": { "icon": "🔎", "color": "blue" }
    ///       // NOTE: `system_prompt` is intentionally omitted from the
    ///       // listing — it can be lengthy and is not needed for UI/LLM
    ///       // selection. Use `action=get` on a child to inspect the
    ///       // resolved prompt of an active session.
    ///     }
    ///   ],
    ///   "fallback_id": "general-purpose",
    ///   "count": 6
    /// }
    /// ```
    fn list_profiles_payload(&self) -> serde_json::Value {
        // Project each profile into a UI-friendly shape that excludes the
        // (potentially large) `system_prompt`. This keeps the payload
        // small for both the LLM context window and the frontend list.
        let profiles: Vec<serde_json::Value> = self
            .profiles
            .iter()
            .map(|p| {
                json!({
                    "id": p.id,
                    "display_name": p.display_name,
                    "description": p.description,
                    "tools": p.tools,
                    "model_hint": p.model_hint,
                    "default_responsibility": p.default_responsibility,
                    "ui": p.ui,
                })
            })
            .collect();
        json!({
            "profiles": profiles,
            "fallback_id": self.profiles.fallback_id(),
            "count": self.profiles.len(),
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::time::Duration;
    use tokio::sync::{broadcast, RwLock};

    use crate::app_state::{AgentRunner, AgentStatus};
    use bamboo_engine::execution::spawn::{SpawnContext, SpawnScheduler};
    use bamboo_agent_core::storage::Storage;
    use bamboo_agent_core::tools::{ToolCall, ToolExecutor, ToolSchema};
    use bamboo_agent_core::{AgentEvent, Message, Role, Session};
    use bamboo_engine::metrics::storage::SqliteMetricsStorage;
    use bamboo_engine::MetricsCollector;
    use bamboo_engine::SkillManager;
    use bamboo_infrastructure::SessionStoreV2;
    use bamboo_infrastructure::{LLMError, LLMProvider, LLMStream};

    struct NoopProvider;

    #[async_trait::async_trait]
    impl LLMProvider for NoopProvider {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> Result<LLMStream, LLMError> {
            Err(LLMError::Api("noop".to_string()))
        }
    }

    struct NoopToolExecutor;

    #[async_trait::async_trait]
    impl ToolExecutor for NoopToolExecutor {
        async fn execute(&self, _call: &ToolCall) -> std::result::Result<ToolResult, ToolError> {
            Err(ToolError::NotFound("noop".to_string()))
        }

        fn list_tools(&self) -> Vec<ToolSchema> {
            Vec::new()
        }
    }

    fn make_temp_dir(prefix: &str) -> PathBuf {
        std::env::temp_dir().join(format!("{prefix}-{}", Uuid::new_v4()))
    }

    struct TestHarness {
        tool: SubAgentTool,
        adapter: Arc<ChildSessionAdapter>,
        storage: Arc<dyn Storage>,
        agent_runners: Arc<RwLock<HashMap<String, AgentRunner>>>,
        parent_session_id: String,
        child_session_id: String,
        parent_rx: broadcast::Receiver<AgentEvent>,
    }

    async fn build_test_harness() -> TestHarness {
        build_test_harness_with_resolver(None).await
    }

    async fn build_test_harness_with_resolver(
        subagent_model_resolver: crate::tools::OptionalSubagentModelResolver,
    ) -> TestHarness {
        let bamboo_home = make_temp_dir("bamboo-sub-agent-test");
        tokio::fs::create_dir_all(&bamboo_home).await.unwrap();

        let session_store = Arc::new(SessionStoreV2::new(bamboo_home.clone()).await.unwrap());
        let storage_dir = bamboo_home.join("storage");
        tokio::fs::create_dir_all(&storage_dir).await.unwrap();
        let jsonl = bamboo_infrastructure::JsonlStorage::new(&storage_dir);
        jsonl.init().await.unwrap();
        let storage: Arc<dyn Storage> = Arc::new(jsonl);

        let metrics_storage = Arc::new(SqliteMetricsStorage::new(bamboo_home.join("metrics.db")));
        let metrics_collector = MetricsCollector::spawn(metrics_storage, 7);

        let sessions_cache: bamboo_engine::SessionCache = Arc::new(dashmap::DashMap::new());
        let agent_runners = Arc::new(RwLock::new(HashMap::new()));
        let session_event_senders = Arc::new(RwLock::new(HashMap::<
            String,
            broadcast::Sender<AgentEvent>,
        >::new()));

        let parent_session_id = "root-session".to_string();
        let child_session_id = "child-session".to_string();
        let (parent_tx, parent_rx) = broadcast::channel(1000);
        {
            let mut senders = session_event_senders.write().await;
            senders.insert(parent_session_id.clone(), parent_tx);
        }

        let mut parent = Session::new(parent_session_id.clone(), "gpt-5");
        parent.title = "Root".to_string();
        storage.save_session(&parent).await.unwrap();
        session_store.save_session(&parent).await.unwrap();

        let mut child = Session::new_child(
            child_session_id.clone(),
            parent_session_id.clone(),
            "gpt-5",
            "Child session",
        );
        child
            .metadata
            .insert("last_run_status".to_string(), "completed".to_string());
        child.add_message(Message::system("child system"));
        child.add_message(Message::user("initial assignment"));
        child.add_message(Message::assistant("initial answer", None));
        storage.save_session(&child).await.unwrap();
        session_store.save_session(&child).await.unwrap();

        let agent_runtime = Arc::new(
            bamboo_engine::Agent::builder()
                .storage(storage.clone())
                .persistence(Arc::new(bamboo_infrastructure::LockedSessionStore::new(
                    storage.clone(),
                )))
                .attachment_reader(session_store.clone())
                .skill_manager(Arc::new(SkillManager::new()))
                .metrics_collector(metrics_collector)
                .config(Arc::new(RwLock::new(
                    bamboo_infrastructure::Config::default(),
                )))
                .provider(Arc::new(NoopProvider))
                .default_tools(Arc::new(NoopToolExecutor))
                .build()
                .expect("test agent should be fully configured"),
        );

        let scheduler = Arc::new(SpawnScheduler::new(SpawnContext {
            agent: agent_runtime,
            tools: Arc::new(NoopToolExecutor),
            sessions_cache: sessions_cache.clone(),
            agent_runners: agent_runners.clone(),
            session_event_senders: session_event_senders.clone(),
            external_child_runner: None,
            provider_router: None,
            app_data_dir: None,
            completion_handler: None,
            account_feed_inbox: None,
        }));

        let test_profiles = std::sync::Arc::new(
            bamboo_domain::subagent::SubagentProfileRegistry::builder()
                .extend(bamboo_engine::profiles::builtin::builtin_profiles())
                .build()
                .expect("builtin subagent profiles must build"),
        );
        let adapter = Arc::new(ChildSessionAdapter {
            session_store,
            storage: storage.clone(),
            persistence: Arc::new(bamboo_infrastructure::LockedSessionStore::new(
                storage.clone(),
            )),
            scheduler,
            sessions_cache,
            agent_runners: agent_runners.clone(),
            session_event_senders,
            subagent_model_resolver,
            config: Arc::new(RwLock::new(bamboo_infrastructure::Config::default())),
            subagent_profiles: test_profiles.clone(),
            tool_names: Vec::new(),
            parent_wait_slots: Arc::new(dashmap::DashMap::new()),
        });
        let tool = SubAgentTool::new(adapter.clone(), test_profiles);

        TestHarness {
            tool,
            adapter,
            storage,
            agent_runners,
            parent_session_id,
            child_session_id,
            parent_rx,
        }
    }

    // -----------------------------------------------------------------------
    // ④ Batched parent-wait registration
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn concurrent_parent_wait_registrations_all_land_in_wait_set() {
        let harness = build_test_harness().await;
        let adapter = harness.adapter.clone();
        let parent_id = harness.parent_session_id.clone();

        // Fire several registrations for the same parent concurrently, exactly as
        // a round of parallel `SubAgent.create` calls would.
        let child_ids: Vec<String> = (0..6).map(|i| format!("c-{i}")).collect();
        let mut handles = Vec::new();
        for id in &child_ids {
            let adapter = adapter.clone();
            let parent_id = parent_id.clone();
            let id = id.clone();
            handles.push(tokio::spawn(async move {
                adapter
                    .register_parent_wait_for_child(&parent_id, &id, Some("tc-1"))
                    .await
            }));
        }
        for h in handles {
            h.await.unwrap().expect("registration should succeed");
        }

        // Every child must be durably present in the parent's wait set, with no
        // duplicates — regardless of how the concurrent calls coalesced.
        let parent = harness
            .storage
            .load_session(&parent_id)
            .await
            .unwrap()
            .unwrap();
        let wait = parent
            .agent_runtime_state
            .expect("runtime state persisted")
            .waiting_for_children
            .expect("wait state persisted");
        let mut got = wait.child_session_ids.clone();
        got.sort();
        assert_eq!(got, child_ids, "all children must be registered exactly once");
        assert_eq!(
            parent.metadata.get("runtime.suspend_reason").map(String::as_str),
            Some("waiting_for_children")
        );
    }

    #[tokio::test]
    async fn repeated_registration_of_same_child_is_idempotent() {
        let harness = build_test_harness().await;
        let adapter = harness.adapter.clone();
        let parent_id = harness.parent_session_id.clone();

        for _ in 0..3 {
            adapter
                .register_parent_wait_for_child(&parent_id, "dup-child", None)
                .await
                .unwrap();
        }

        let parent = harness
            .storage
            .load_session(&parent_id)
            .await
            .unwrap()
            .unwrap();
        let wait = parent
            .agent_runtime_state
            .unwrap()
            .waiting_for_children
            .unwrap();
        assert_eq!(wait.child_session_ids, vec!["dup-child".to_string()]);
    }

    // -----------------------------------------------------------------------
    // Decoupled create + explicit SubAgent.wait
    // -----------------------------------------------------------------------

    fn ctx_for<'a>(session_id: &'a str, tool_call_id: &'static str) -> ToolExecutionContext<'a> {
        ToolExecutionContext {
            session_id: Some(session_id),
            tool_call_id,
            event_tx: None,
            available_tool_schemas: None,
        }
    }

    #[tokio::test]
    async fn create_without_subagent_type_defaults_to_general_purpose() {
        let harness = build_test_harness().await;
        let result = harness
            .tool
            .execute_with_context(
                json!({
                    "action": "create",
                    "title": "No Role Child",
                    "responsibility": "Do work",
                    "prompt": "Do the work",
                    "workspace": "/tmp/ws"
                    // subagent_type intentionally omitted
                }),
                ctx_for(&harness.parent_session_id, "tc_no_role"),
            )
            .await
            .expect("create must succeed without subagent_type");

        let payload: serde_json::Value = serde_json::from_str(&result.result).unwrap();
        assert_eq!(payload["subagent_type"].as_str(), Some("general-purpose"));
    }

    #[tokio::test]
    async fn create_with_wait_true_suspends_and_registers_wait() {
        let harness = build_test_harness().await;
        let result = harness
            .tool
            .execute_with_context(
                json!({
                    "action": "create",
                    "title": "Blocking Child",
                    "responsibility": "Do one thing",
                    "prompt": "Do it",
                    "subagent_type": "general-purpose",
                    "workspace": "/tmp/ws",
                    "wait": true
                }),
                ctx_for(&harness.parent_session_id, "tc_create_wait"),
            )
            .await
            .expect("create should succeed");

        assert_eq!(
            result.display_preference.as_deref(),
            Some("runtime_control:waiting_for_children"),
            "create wait=true must suspend the parent"
        );

        let parent = harness
            .storage
            .load_session(&harness.parent_session_id)
            .await
            .unwrap()
            .unwrap();
        let wait = parent
            .agent_runtime_state
            .expect("runtime state")
            .waiting_for_children
            .expect("wait registered");
        assert_eq!(wait.child_session_ids.len(), 1);
    }

    #[tokio::test]
    async fn wait_action_with_explicit_children_suspends_and_registers() {
        let harness = build_test_harness().await;
        let result = harness
            .tool
            .execute_with_context(
                json!({
                    "action": "wait",
                    "child_session_ids": ["k1", "k2", "k3"],
                    "wait_for": "any"
                }),
                ctx_for(&harness.parent_session_id, "tc_wait"),
            )
            .await
            .expect("wait should succeed");

        assert_eq!(
            result.display_preference.as_deref(),
            Some("runtime_control:waiting_for_children"),
            "wait must suspend the parent"
        );
        let payload: serde_json::Value = serde_json::from_str(&result.result).unwrap();
        assert_eq!(payload["status"].as_str(), Some("waiting"));
        assert_eq!(payload["wait_for"].as_str(), Some("any"));

        let parent = harness
            .storage
            .load_session(&harness.parent_session_id)
            .await
            .unwrap()
            .unwrap();
        let wait = parent
            .agent_runtime_state
            .unwrap()
            .waiting_for_children
            .unwrap();
        assert_eq!(
            wait.child_session_ids,
            vec!["k1".to_string(), "k2".to_string(), "k3".to_string()]
        );
        assert_eq!(wait.wait_for, ChildWaitPolicy::Any);
    }

    #[tokio::test]
    async fn wait_action_is_noop_when_no_active_children() {
        let harness = build_test_harness().await;
        // No explicit ids and (in the jsonl-backed harness) no derivable active
        // children → must NOT suspend, and must NOT register an empty wait.
        let result = harness
            .tool
            .execute_with_context(
                json!({ "action": "wait" }),
                ctx_for(&harness.parent_session_id, "tc_wait_noop"),
            )
            .await
            .expect("wait should succeed");

        assert_ne!(
            result.display_preference.as_deref(),
            Some("runtime_control:waiting_for_children"),
            "wait with no active children must not suspend"
        );
        let payload: serde_json::Value = serde_json::from_str(&result.result).unwrap();
        assert_eq!(payload["status"].as_str(), Some("no_active_children"));

        let parent = harness
            .storage
            .load_session(&harness.parent_session_id)
            .await
            .unwrap()
            .unwrap();
        assert!(parent
            .agent_runtime_state
            .and_then(|s| s.waiting_for_children)
            .is_none());
    }

    // -----------------------------------------------------------------------
    // Normalization tests
    // -----------------------------------------------------------------------

    #[test]
    fn normalize_title_accepts_legacy_description() {
        let title = normalize_title(None, "Search refs".to_string()).unwrap();
        assert_eq!(title, "Search refs");
    }

    #[test]
    fn normalize_title_prefers_title_over_description() {
        let title =
            normalize_title(Some("Real title".to_string()), "Legacy desc".to_string()).unwrap();
        assert_eq!(title, "Real title");
    }

    #[test]
    fn normalize_title_rejects_both_empty() {
        let err = normalize_title(None, "".to_string()).unwrap_err();
        assert!(matches!(err, ToolError::InvalidArguments(msg) if msg.contains("title")));
    }

    // -----------------------------------------------------------------------
    // Create action tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn create_requires_session_id_in_tool_context() {
        let harness = build_test_harness().await;

        let err = harness
            .tool
            .execute_with_context(
                json!({
                    "action": "create",
                    "title": "demo task",
                    "responsibility": "do something",
                    "prompt": "do something",
                    "subagent_type": "general-purpose",
                    "workspace": "/tmp/test-workspace"
                }),
                ToolExecutionContext::none("tool_call"),
            )
            .await
            .unwrap_err();

        match err {
            ToolError::Execution(msg) => {
                assert!(msg.contains("SubAgent requires a session_id in tool context"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn create_emits_sub_agent_started_event_after_queueing() {
        let mut harness = build_test_harness().await;

        let result = harness
            .tool
            .execute_with_context(
                json!({
                    "action": "create",
                    "title": "Child A",
                    "responsibility": "Investigate one module",
                    "prompt": "Read module and summarize",
                    "subagent_type": "general-purpose",
                    "workspace": "/tmp/test-workspace"
                }),
                ToolExecutionContext {
                    session_id: Some(harness.parent_session_id.as_str()),
                    tool_call_id: "tool_call_1",
                    event_tx: None,
                    available_tool_schemas: None,
                },
            )
            .await
            .expect("SubAgent should enqueue a child session");

        let parsed_result: serde_json::Value =
            serde_json::from_str(&result.result).expect("tool result should be JSON");
        let child_session_id = parsed_result
            .get("child_session_id")
            .and_then(|v| v.as_str())
            .expect("tool result should include child_session_id")
            .to_string();

        let started_event = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                match harness.parent_rx.recv().await {
                    Ok(AgentEvent::SubAgentStarted {
                        parent_session_id: pid,
                        child_session_id: cid,
                        ..
                    }) => break (pid, cid),
                    Ok(_) => continue,
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => {
                        panic!("parent stream closed before start event")
                    }
                }
            }
        })
        .await
        .expect("should receive SubAgentStarted event quickly");

        assert_eq!(started_event.0, harness.parent_session_id);
        assert_eq!(started_event.1, child_session_id);
    }

    #[tokio::test]
    async fn create_uses_async_subagent_model_resolver() {
        let resolver: crate::tools::SubagentModelResolver = Arc::new(|subagent_type: String| {
            Box::pin(async move {
                assert_eq!(subagent_type, "coder");
                Some(bamboo_domain::ProviderModelRef::new(
                    "openai",
                    "gpt-resolved-coder",
                ))
            })
        });
        let harness = build_test_harness_with_resolver(Some(resolver)).await;

        let result = harness
            .tool
            .execute_with_context(
                json!({
                    "action": "create",
                    "title": "Coder Child",
                    "responsibility": "Implement a focused change",
                    "prompt": "Patch one file",
                    "subagent_type": "coder",
                    "workspace": "/tmp/test-workspace",
                    "auto_run": false
                }),
                ToolExecutionContext {
                    session_id: Some(harness.parent_session_id.as_str()),
                    tool_call_id: "tool_call_async_resolver",
                    event_tx: None,
                    available_tool_schemas: None,
                },
            )
            .await
            .expect("SubAgent should create a child using async model resolver");

        let payload: serde_json::Value =
            serde_json::from_str(&result.result).expect("tool result should be JSON");
        assert_eq!(payload["model"], "gpt-resolved-coder");

        let child_id = payload["child_session_id"]
            .as_str()
            .expect("child_session_id should be present");
        let child = harness
            .storage
            .load_session(child_id)
            .await
            .unwrap()
            .expect("child session should exist");
        assert_eq!(child.model, "gpt-resolved-coder");
        assert_eq!(
            child.model_ref,
            Some(bamboo_domain::ProviderModelRef::new(
                "openai",
                "gpt-resolved-coder",
            ))
        );
        assert_eq!(
            child.metadata.get("provider_name").map(String::as_str),
            Some("openai")
        );
    }

    #[tokio::test]
    async fn backward_compat_legacy_subagent_call_without_action_defaults_to_create() {
        let harness = build_test_harness().await;

        let result = harness
            .tool
            .execute_with_context(
                json!({
                    "title": "Legacy Child",
                    "responsibility": "Test backward compat",
                    "prompt": "Do something",
                    "subagent_type": "general-purpose",
                    "workspace": "/tmp/test-workspace"
                }),
                ToolExecutionContext {
                    session_id: Some(harness.parent_session_id.as_str()),
                    tool_call_id: "tool_call_legacy",
                    event_tx: None,
                    available_tool_schemas: None,
                },
            )
            .await
            .expect("legacy SubAgent call without action should default to create");

        assert!(result.success);
        let parsed: serde_json::Value = serde_json::from_str(&result.result).unwrap();
        assert!(parsed.get("child_session_id").is_some());
    }

    // -----------------------------------------------------------------------
    // Management action tests for the unified SubAgent tool
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn send_message_appends_follow_up_without_replacing_history() {
        let harness = build_test_harness().await;

        let result = harness
            .tool
            .execute_with_context(
                json!({
                    "action": "send_message",
                    "child_session_id": harness.child_session_id,
                    "message": "continue with the failing parser path",
                    "auto_run": false
                }),
                ToolExecutionContext {
                    session_id: Some(harness.parent_session_id.as_str()),
                    tool_call_id: "tool_call_send_message",
                    event_tx: None,
                    available_tool_schemas: None,
                },
            )
            .await
            .expect("send_message should succeed");

        let payload: serde_json::Value =
            serde_json::from_str(&result.result).expect("tool result should be JSON");
        assert_eq!(payload["status"], "pending");

        let child = harness
            .storage
            .load_session(&harness.child_session_id)
            .await
            .unwrap()
            .expect("child session should exist");
        assert_eq!(child.messages.len(), 4);
        assert!(matches!(child.messages[2].role, Role::Assistant));
        assert!(matches!(child.messages[3].role, Role::User));
        assert_eq!(
            child.messages[3].content,
            "continue with the failing parser path"
        );
        assert_eq!(
            child.metadata.get("last_run_status").map(String::as_str),
            Some("pending")
        );
    }

    #[tokio::test]
    async fn send_message_queues_on_running_child_without_interrupt() {
        let harness = build_test_harness().await;
        {
            let mut runners = harness.agent_runners.write().await;
            let mut runner = AgentRunner::new();
            runner.status = AgentStatus::Running;
            runners.insert(harness.child_session_id.clone(), runner);
        }

        let result = harness
            .tool
            .execute_with_context(
                json!({
                    "action": "send_message",
                    "child_session_id": harness.child_session_id,
                    "message": "continue"
                }),
                ToolExecutionContext {
                    session_id: Some(harness.parent_session_id.as_str()),
                    tool_call_id: "tool_call_running",
                    event_tx: None,
                    available_tool_schemas: None,
                },
            )
            .await
            .expect("send_message should queue message on running child");

        assert!(result.success);
        let payload: serde_json::Value = serde_json::from_str(&result.result).unwrap();
        assert_eq!(payload["status"], "message_queued");
        assert_eq!(payload["message"], "continue");

        let child = harness
            .storage
            .load_session(&harness.child_session_id)
            .await
            .unwrap()
            .expect("child session should exist");
        // Message is NOT appended to messages array while child is running;
        // it is stored in metadata for the agent loop to merge at turn boundaries.
        assert_eq!(child.messages.len(), 3);
        let pending_raw = child
            .metadata
            .get("pending_injected_messages")
            .expect("pending_injected_messages should be set");
        let pending: Vec<child_session::QueuedInjectedMessage> =
            serde_json::from_str(pending_raw).expect("should parse queued messages");
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].content, "continue");
    }

    #[tokio::test]
    async fn send_message_can_interrupt_running_child() {
        let harness = build_test_harness().await;
        let cancel_token = {
            let mut runners = harness.agent_runners.write().await;
            let mut runner = AgentRunner::new();
            runner.status = AgentStatus::Running;
            let cancel_token = runner.cancel_token.clone();
            runners.insert(harness.child_session_id.clone(), runner);
            cancel_token
        };

        let runners_for_status = harness.agent_runners.clone();
        let child_id_for_status = harness.child_session_id.clone();
        let waiter = tokio::spawn(async move {
            cancel_token.cancelled().await;
            let mut runners = runners_for_status.write().await;
            if let Some(runner) = runners.get_mut(&child_id_for_status) {
                runner.status = AgentStatus::Cancelled;
            }
        });

        let result = harness
            .tool
            .execute_with_context(
                json!({
                    "action": "send_message",
                    "child_session_id": harness.child_session_id,
                    "message": "continue from latest state",
                    "auto_run": false,
                    "interrupt_running": true
                }),
                ToolExecutionContext {
                    session_id: Some(harness.parent_session_id.as_str()),
                    tool_call_id: "tool_call_interrupt_running",
                    event_tx: None,
                    available_tool_schemas: None,
                },
            )
            .await
            .expect("send_message should interrupt running child");

        waiter.await.expect("waiter task should finish");

        let payload: serde_json::Value =
            serde_json::from_str(&result.result).expect("tool result should be JSON");
        assert_eq!(payload["status"], "pending");
        assert_eq!(payload["auto_run"], false);

        let child = harness
            .storage
            .load_session(&harness.child_session_id)
            .await
            .unwrap()
            .expect("child session should exist");
        assert!(matches!(
            child.messages.last().map(|m| &m.role),
            Some(Role::User)
        ));
        assert_eq!(
            child.messages.last().map(|m| m.content.as_str()),
            Some("continue from latest state")
        );
        assert_eq!(
            child.metadata.get("last_run_status").map(String::as_str),
            Some("pending")
        );
    }

    #[tokio::test]
    async fn send_message_can_queue_child_immediately() {
        let mut harness = build_test_harness().await;

        let result = harness
            .tool
            .execute_with_context(
                json!({
                    "action": "send_message",
                    "child_session_id": harness.child_session_id,
                    "message": "retry with a narrower scope"
                }),
                ToolExecutionContext {
                    session_id: Some(harness.parent_session_id.as_str()),
                    tool_call_id: "tool_call_queue",
                    event_tx: None,
                    available_tool_schemas: None,
                },
            )
            .await
            .expect("send_message should queue the child");

        let payload: serde_json::Value =
            serde_json::from_str(&result.result).expect("tool result should be JSON");
        assert_eq!(payload["status"], "queued");
        assert_eq!(payload["auto_run"], true);

        let started_event = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                match harness.parent_rx.recv().await {
                    Ok(AgentEvent::SubAgentStarted {
                        parent_session_id,
                        child_session_id,
                        ..
                    }) => break (parent_session_id, child_session_id),
                    Ok(_) => continue,
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => {
                        panic!("parent stream closed before start event")
                    }
                }
            }
        })
        .await
        .expect("should receive SubAgentStarted event");

        assert_eq!(started_event.0, harness.parent_session_id);
        assert_eq!(started_event.1, harness.child_session_id);
    }

    #[tokio::test]
    async fn cancel_stops_running_child() {
        let harness = build_test_harness().await;
        let cancel_token = {
            let mut runners = harness.agent_runners.write().await;
            let mut runner = AgentRunner::new();
            runner.status = AgentStatus::Running;
            let token = runner.cancel_token.clone();
            runners.insert(harness.child_session_id.clone(), runner);
            token
        };

        let runners_for_wait = harness.agent_runners.clone();
        let child_id_for_wait = harness.child_session_id.clone();
        let waiter = tokio::spawn(async move {
            cancel_token.cancelled().await;
            let mut runners = runners_for_wait.write().await;
            if let Some(runner) = runners.get_mut(&child_id_for_wait) {
                runner.status = AgentStatus::Cancelled;
            }
        });

        let result = harness
            .tool
            .execute_with_context(
                json!({
                    "action": "cancel",
                    "child_session_id": harness.child_session_id
                }),
                ToolExecutionContext {
                    session_id: Some(harness.parent_session_id.as_str()),
                    tool_call_id: "tool_call_cancel",
                    event_tx: None,
                    available_tool_schemas: None,
                },
            )
            .await
            .expect("cancel should succeed");

        waiter.await.expect("waiter should finish");

        let payload: serde_json::Value =
            serde_json::from_str(&result.result).expect("tool result should be JSON");
        assert_eq!(payload["status"], "cancelled");
        assert_eq!(payload["child_session_id"], harness.child_session_id);
    }

    #[tokio::test]
    async fn list_returns_children() {
        let harness = build_test_harness().await;

        let result = harness
            .tool
            .execute_with_context(
                json!({"action": "list"}),
                ToolExecutionContext {
                    session_id: Some(harness.parent_session_id.as_str()),
                    tool_call_id: "tool_call_list",
                    event_tx: None,
                    available_tool_schemas: None,
                },
            )
            .await
            .expect("list should succeed");

        let payload: serde_json::Value =
            serde_json::from_str(&result.result).expect("tool result should be JSON");
        let children = payload["children"]
            .as_array()
            .expect("list result should have children array");
        assert_eq!(children.len(), 1);
        assert_eq!(children[0]["child_session_id"], harness.child_session_id);
        assert_eq!(payload["count"], 1);
    }

    #[tokio::test]
    async fn get_returns_runner_diagnostics() {
        let harness = build_test_harness().await;

        // Set up a running runner with diagnostic fields populated.
        {
            let mut runners = harness.agent_runners.write().await;
            let mut runner = AgentRunner::new();
            runner.status = AgentStatus::Running;
            runner.last_tool_name = Some("Read".to_string());
            runner.last_tool_phase = Some("begin".to_string());
            runner.round_count = 3;
            runners.insert(harness.child_session_id.clone(), runner);
        }

        let result = harness
            .tool
            .execute_with_context(
                json!({
                    "action": "get",
                    "child_session_id": harness.child_session_id
                }),
                ToolExecutionContext {
                    session_id: Some(harness.parent_session_id.as_str()),
                    tool_call_id: "tool_call_get_diagnostics",
                    event_tx: None,
                    available_tool_schemas: None,
                },
            )
            .await
            .expect("get should succeed");

        let payload: serde_json::Value =
            serde_json::from_str(&result.result).expect("tool result should be JSON");
        assert_eq!(payload["child_session_id"], harness.child_session_id);
        assert_eq!(payload["is_running"], true);
        assert_eq!(payload["last_tool_name"], "Read");
        assert_eq!(payload["last_tool_phase"], "begin");
        assert_eq!(payload["round_count"], 3);
        assert!(payload["runner_started_at"].is_string());
        assert!(payload.get("guidance").is_some());
    }

    #[tokio::test]
    async fn create_returns_duration_hint() {
        let harness = build_test_harness().await;

        let result = harness
            .tool
            .execute_with_context(
                json!({
                    "action": "create",
                    "title": "Test Child",
                    "responsibility": "Do something",
                    "prompt": "Do something useful",
                    "subagent_type": "general-purpose",
                    "workspace": "/tmp/test-workspace"
                }),
                ToolExecutionContext {
                    session_id: Some(harness.parent_session_id.as_str()),
                    tool_call_id: "tool_call_create_hint",
                    event_tx: None,
                    available_tool_schemas: None,
                },
            )
            .await
            .expect("create should succeed");

        let payload: serde_json::Value =
            serde_json::from_str(&result.result).expect("tool result should be JSON");
        let note = payload["note"].as_str().expect("note should be present");
        assert!(
            note.contains("30-120 seconds"),
            "note should contain estimated duration hint: {note}"
        );
        assert!(
            note.contains("send_message"),
            "note should mention send_message: {note}"
        );
        // Default create now runs in the background and does NOT suspend the
        // parent: the result must not carry the waiting_for_children control.
        assert_ne!(
            result.display_preference.as_deref(),
            Some("runtime_control:waiting_for_children"),
            "default create must not suspend the parent"
        );
        assert_eq!(payload["status"].as_str(), Some("running_in_background"));
    }

    #[tokio::test]
    async fn create_persists_explicit_reasoning_effort_to_child_session() {
        let harness = build_test_harness().await;

        let result = harness
            .tool
            .execute_with_context(
                json!({
                    "action": "create",
                    "title": "Reasoning Child",
                    "responsibility": "Investigate hard problem",
                    "prompt": "Think carefully step by step",
                    "subagent_type": "general-purpose",
                    "workspace": "/tmp/test-workspace",
                    "auto_run": false,
                    "reasoning_effort": "high"
                }),
                ToolExecutionContext {
                    session_id: Some(harness.parent_session_id.as_str()),
                    tool_call_id: "tool_call_create_with_effort",
                    event_tx: None,
                    available_tool_schemas: None,
                },
            )
            .await
            .expect("create should succeed");

        let payload: serde_json::Value =
            serde_json::from_str(&result.result).expect("tool result should be JSON");
        assert_eq!(
            payload["reasoning_effort"].as_str(),
            Some("high"),
            "tool result should echo the resolved reasoning_effort"
        );

        let child_id = payload["child_session_id"]
            .as_str()
            .expect("child_session_id present")
            .to_string();
        let child = harness
            .storage
            .load_session(&child_id)
            .await
            .expect("child should be persisted")
            .expect("child session should exist");
        assert_eq!(
            child.reasoning_effort,
            Some(bamboo_domain::ReasoningEffort::High),
            "child.reasoning_effort should reflect the explicit override"
        );
    }

    #[tokio::test]
    async fn create_without_reasoning_effort_leaves_child_at_provider_default() {
        let harness = build_test_harness().await;

        let result = harness
            .tool
            .execute_with_context(
                json!({
                    "action": "create",
                    "title": "Default Child",
                    "responsibility": "Quick lookup",
                    "prompt": "Read a file and summarise",
                    "subagent_type": "general-purpose",
                    "workspace": "/tmp/test-workspace",
                    "auto_run": false
                }),
                ToolExecutionContext {
                    session_id: Some(harness.parent_session_id.as_str()),
                    tool_call_id: "tool_call_create_default_effort",
                    event_tx: None,
                    available_tool_schemas: None,
                },
            )
            .await
            .expect("create should succeed");

        let payload: serde_json::Value =
            serde_json::from_str(&result.result).expect("tool result should be JSON");
        assert!(
            payload["reasoning_effort"].is_null(),
            "tool result should report null reasoning_effort when omitted, got {:?}",
            payload["reasoning_effort"]
        );

        let child_id = payload["child_session_id"]
            .as_str()
            .expect("child_session_id present")
            .to_string();
        let child = harness
            .storage
            .load_session(&child_id)
            .await
            .expect("child should be persisted")
            .expect("child session should exist");
        assert_eq!(
            child.reasoning_effort, None,
            "child.reasoning_effort should stay at None (provider default) when caller omits it; \
             children must NOT inherit the parent's reasoning_effort"
        );
    }

    #[tokio::test]
    async fn update_can_change_reasoning_effort_on_existing_child() {
        let harness = build_test_harness().await;

        // Pre-condition: the seeded child has reasoning_effort = None.
        let seeded = harness
            .storage
            .load_session(&harness.child_session_id)
            .await
            .expect("seeded child should load")
            .expect("seeded child exists");
        assert_eq!(seeded.reasoning_effort, None);

        let _ = harness
            .tool
            .execute_with_context(
                json!({
                    "action": "update",
                    "child_session_id": harness.child_session_id,
                    "reasoning_effort": "max"
                }),
                ToolExecutionContext {
                    session_id: Some(harness.parent_session_id.as_str()),
                    tool_call_id: "tool_call_update_effort",
                    event_tx: None,
                    available_tool_schemas: None,
                },
            )
            .await
            .expect("update should succeed");

        let updated = harness
            .storage
            .load_session(&harness.child_session_id)
            .await
            .expect("updated child should load")
            .expect("child still exists");
        assert_eq!(
            updated.reasoning_effort,
            Some(bamboo_domain::ReasoningEffort::Max),
            "update should persist the new reasoning_effort"
        );
    }

    #[tokio::test]
    async fn delete_removes_child() {
        let harness = build_test_harness().await;

        let result = harness
            .tool
            .execute_with_context(
                json!({
                    "action": "delete",
                    "child_session_id": harness.child_session_id
                }),
                ToolExecutionContext {
                    session_id: Some(harness.parent_session_id.as_str()),
                    tool_call_id: "tool_call_delete",
                    event_tx: None,
                    available_tool_schemas: None,
                },
            )
            .await
            .expect("delete should succeed");

        let payload: serde_json::Value =
            serde_json::from_str(&result.result).expect("tool result should be JSON");
        assert_eq!(payload["deleted"], true);

        let child = harness
            .storage
            .load_session(&harness.child_session_id)
            .await
            .unwrap();
        assert!(child.is_none());
    }

    /// `action=list_profiles` returns every built-in profile (without
    /// the `system_prompt` body), reports the registry's fallback id,
    /// and uses the registry's stable insertion order. The shape of
    /// this payload is a public contract — UI / LLM rely on it.
    #[tokio::test]
    async fn list_profiles_returns_builtin_catalog() {
        let harness = build_test_harness().await;

        let result = harness
            .tool
            .execute_with_context(
                json!({"action": "list_profiles"}),
                ToolExecutionContext {
                    session_id: Some(harness.parent_session_id.as_str()),
                    tool_call_id: "tool_call_list_profiles",
                    event_tx: None,
                    available_tool_schemas: None,
                },
            )
            .await
            .expect("list_profiles should succeed");

        let payload: serde_json::Value =
            serde_json::from_str(&result.result).expect("tool result should be JSON");

        // Top-level shape.
        let profiles = payload["profiles"]
            .as_array()
            .expect("list_profiles must return a `profiles` array");
        assert!(
            profiles.len() >= 6,
            "expected at least 6 built-in profiles, got {}",
            profiles.len()
        );
        assert_eq!(payload["count"], profiles.len());
        assert_eq!(payload["fallback_id"], "general-purpose");

        // Required fields per profile, and explicit guarantee that we
        // do NOT leak `system_prompt` (could be very large).
        for entry in profiles {
            assert!(entry.get("id").and_then(|v| v.as_str()).is_some());
            assert!(entry.get("display_name").and_then(|v| v.as_str()).is_some());
            assert!(entry.get("tools").is_some());
            assert!(
                entry.get("system_prompt").is_none(),
                "system_prompt must NOT be returned by list_profiles",
            );
        }

        // Built-in catalogue must include the documented baseline ids
        // so the LLM can rely on them being present.
        let ids: Vec<&str> = profiles
            .iter()
            .map(|p| p["id"].as_str().unwrap_or(""))
            .collect();
        for required in [
            "general-purpose",
            "plan",
            "researcher",
            "coder",
            "reviewer",
            "tester",
        ] {
            assert!(
                ids.contains(&required),
                "built-in profile `{required}` missing from list_profiles output (got: {ids:?})"
            );
        }
    }

    /// `list_profiles` is read-only and must not require a real,
    /// loadable parent session. We pass a non-existent session_id and
    /// expect success (registry is consulted directly, no session
    /// lookup is performed).
    #[tokio::test]
    async fn list_profiles_does_not_load_parent_session() {
        let harness = build_test_harness().await;

        let result = harness
            .tool
            .execute_with_context(
                json!({"action": "list_profiles"}),
                ToolExecutionContext {
                    session_id: Some("non-existent-session-id"),
                    tool_call_id: "tool_call_list_profiles_no_session",
                    event_tx: None,
                    available_tool_schemas: None,
                },
            )
            .await
            .expect("list_profiles should succeed even when the parent session id is unknown");

        let payload: serde_json::Value =
            serde_json::from_str(&result.result).expect("tool result should be JSON");
        assert!(payload["profiles"].as_array().is_some());
    }

    #[tokio::test]
    async fn create_requires_workspace() {
        let harness = build_test_harness().await;

        let err = harness
            .tool
            .execute_with_context(
                json!({
                    "action": "create",
                    "title": "No Workspace Child",
                    "responsibility": "Test workspace validation",
                    "prompt": "Do something",
                    "subagent_type": "general-purpose"
                }),
                ToolExecutionContext {
                    session_id: Some(harness.parent_session_id.as_str()),
                    tool_call_id: "tool_call_no_workspace",
                    event_tx: None,
                    available_tool_schemas: None,
                },
            )
            .await
            .unwrap_err();

        match err {
            ToolError::InvalidArguments(msg) => {
                assert!(
                    msg.contains("workspace"),
                    "error should mention workspace: {msg}"
                );
            }
            other => panic!("expected InvalidArguments error, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn create_sets_child_workspace() {
        let harness = build_test_harness().await;

        let result = harness
            .tool
            .execute_with_context(
                json!({
                    "action": "create",
                    "title": "Workspace Child",
                    "responsibility": "Test workspace propagation",
                    "prompt": "Do something",
                    "subagent_type": "general-purpose",
                    "workspace": "/tmp/test-workspace",
                    "auto_run": false
                }),
                ToolExecutionContext {
                    session_id: Some(harness.parent_session_id.as_str()),
                    tool_call_id: "tool_call_workspace",
                    event_tx: None,
                    available_tool_schemas: None,
                },
            )
            .await
            .expect("create should succeed with workspace");

        let payload: serde_json::Value =
            serde_json::from_str(&result.result).expect("tool result should be JSON");
        let child_id = payload["child_session_id"]
            .as_str()
            .expect("child_session_id should be present")
            .to_string();

        let child = harness
            .storage
            .load_session(&child_id)
            .await
            .expect("child should be persisted")
            .expect("child session should exist");
        assert_eq!(
            child.workspace,
            Some("/tmp/test-workspace".to_string()),
            "child workspace should be set from create args"
        );
    }

    // -----------------------------------------------------------------------
    // S-T5.2 — End-to-end policy enforcement for a read-only allowlist child.
    //
    // The `researcher` builtin profile is a read-only Allowlist. A researcher
    // child must have mutating tools (Edit/Write) enforced by BOTH halves of
    // the tool-policy machinery (TD-7):
    //   1. Discovery (schema): the profile's `disabled_tools` — computed by
    //      `disabled_tools_for_profile` over the real builtin tool surface —
    //      contains Edit and Write, so they are absent from the advertised
    //      schema for the child.
    //   2. Execution (safety net): `PolicyAwareToolExecutor` rejects an Edit /
    //      Write call from a `subagent_type=researcher` child at execute time,
    //      while still permitting allowlisted tools (Read).
    //
    // This pins the anti-fork invariant: the same canonical profile drives both
    // layers; there is no forked policy definition.
    // -----------------------------------------------------------------------

    /// A recording executor used to prove the runtime safety net forwards vs
    /// blocks calls. Forwards always succeed; blocked calls never reach it.
    struct PolicyRecordingExecutor {
        executed: Arc<RwLock<Vec<String>>>,
    }

    #[async_trait::async_trait]
    impl ToolExecutor for PolicyRecordingExecutor {
        async fn execute(
            &self,
            call: &ToolCall,
        ) -> std::result::Result<
            bamboo_agent_core::tools::ToolResult,
            bamboo_agent_core::tools::ToolError,
        > {
            self.executed.write().await.push(call.function.name.clone());
            Ok(bamboo_agent_core::tools::ToolResult {
                success: true,
                result: "ok".to_string(),
                display_preference: None,
            })
        }

        async fn execute_with_context(
            &self,
            call: &ToolCall,
            _ctx: bamboo_agent_core::tools::ToolExecutionContext<'_>,
        ) -> std::result::Result<
            bamboo_agent_core::tools::ToolResult,
            bamboo_agent_core::tools::ToolError,
        > {
            self.executed.write().await.push(call.function.name.clone());
            Ok(bamboo_agent_core::tools::ToolResult {
                success: true,
                result: "ok".to_string(),
                display_preference: None,
            })
        }

        fn list_tools(&self) -> Vec<ToolSchema> {
            Vec::new()
        }
    }

    fn researcher_builtin_profile() -> bamboo_domain::subagent::SubagentProfile {
        bamboo_engine::profiles::builtin::builtin_profiles()
            .into_iter()
            .find(|p| p.id == "researcher")
            .expect("researcher builtin profile must exist")
    }

    fn make_tool_call(name: &str) -> ToolCall {
        ToolCall {
            id: "policy_call".to_string(),
            tool_type: "function".to_string(),
            function: bamboo_agent_core::tools::FunctionCall {
                name: name.to_string(),
                arguments: "{}".to_string(),
            },
        }
    }

    #[test]
    fn researcher_disabled_tools_block_edit_and_write_in_schema() {
        // Layer 1 (discovery): the schema-level disabled set excludes mutating
        // tools for the read-only researcher allowlist.
        let researcher = researcher_builtin_profile();
        let all_tool_names: Vec<String> = bamboo_domain::tool_names::BUILTIN_TOOL_NAMES
            .iter()
            .map(|s| s.to_string())
            .collect();

        let disabled =
            bamboo_domain::subagent::disabled_tools_for_profile(&researcher.tools, &all_tool_names);

        assert!(
            disabled.iter().any(|t| t == "Edit"),
            "researcher schema must disable Edit; disabled={disabled:?}"
        );
        assert!(
            disabled.iter().any(|t| t == "Write"),
            "researcher schema must disable Write; disabled={disabled:?}"
        );
        // Sanity: an allowlisted read-only tool stays enabled.
        assert!(
            !disabled.iter().any(|t| t == "Read"),
            "researcher schema must keep Read enabled; disabled={disabled:?}"
        );
    }

    #[tokio::test]
    async fn researcher_child_blocks_edit_and_write_at_execute() {
        // Layer 2 (execution safety net): a researcher child has Edit / Write
        // rejected at execute time by PolicyAwareToolExecutor, while Read is
        // still forwarded. Uses the canonical builtin registry + the new
        // `bamboo_tools` path via the server's re-export shim.
        let executed = Arc::new(RwLock::new(Vec::<String>::new()));
        let inner: Arc<dyn ToolExecutor> = Arc::new(PolicyRecordingExecutor {
            executed: executed.clone(),
        });

        let registry = Arc::new(
            bamboo_domain::subagent::SubagentProfileRegistry::builder()
                .extend(bamboo_engine::profiles::builtin::builtin_profiles())
                .build()
                .expect("builtin subagent profiles must build"),
        );

        let mut child =
            Session::new_child("researcher-child", "root", "test-model", "Research child");
        child
            .metadata
            .insert("subagent_type".to_string(), "researcher".to_string());
        let sessions: bamboo_engine::SessionCache = Arc::new(dashmap::DashMap::new());
        sessions.insert(
            "researcher-child".to_string(),
            Arc::new(parking_lot::RwLock::new(child)),
        );

        let executor = crate::tools::PolicyAwareToolExecutor::new(inner, registry, sessions);

        let ctx = bamboo_agent_core::tools::ToolExecutionContext {
            session_id: Some("researcher-child"),
            tool_call_id: "policy_call",
            event_tx: None,
            available_tool_schemas: None,
        };

        // Edit blocked at execute.
        let edit_err = executor
            .execute_with_context(&make_tool_call("Edit"), ctx)
            .await
            .expect_err("Edit must be blocked for a researcher child");
        match edit_err {
            bamboo_agent_core::tools::ToolError::Execution(msg) => {
                assert!(msg.contains("Edit"), "error should name the tool: {msg}");
                assert!(
                    msg.contains("researcher"),
                    "error should name the subagent_type: {msg}"
                );
            }
            other => panic!("expected Execution error, got {other:?}"),
        }

        // Write blocked at execute.
        let write_err = executor
            .execute_with_context(&make_tool_call("Write"), ctx)
            .await
            .expect_err("Write must be blocked for a researcher child");
        assert!(
            matches!(
                write_err,
                bamboo_agent_core::tools::ToolError::Execution(ref msg) if msg.contains("Write")
            ),
            "Write must be rejected with an Execution error naming the tool"
        );

        // Allowlisted Read still forwarded to the inner executor.
        executor
            .execute_with_context(&make_tool_call("Read"), ctx)
            .await
            .expect("Read is allowlisted and must be forwarded");

        // Only Read reached the inner executor; Edit/Write were stopped above it.
        assert_eq!(
            executed.read().await.as_slice(),
            &["Read".to_string()],
            "only the allowlisted Read should reach the inner executor"
        );
    }
}
