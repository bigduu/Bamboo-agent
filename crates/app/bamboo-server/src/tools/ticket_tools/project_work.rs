use super::*;
use crate::app_state::ticket_application::project_manager::{
    ManagerRequest, SupervisorProjectRequest,
};

const MANAGE: &str = "project_manager_work";
const CONTACT: &str = "work_project_manager";

pub(super) fn overlay(
    mut tools: Arc<dyn ToolExecutor>,
    app: &Arc<TicketApplication>,
) -> Arc<dyn ToolExecutor> {
    for name in [MANAGE, CONTACT] {
        tools = Arc::new(crate::tools::OverlayToolExecutor::new(
            tools,
            Arc::new(ProjectWorkTool {
                app: Arc::downgrade(app),
                name,
            }),
        ));
    }
    tools
}

struct ProjectWorkTool {
    app: Weak<TicketApplication>,
    name: &'static str,
}

impl ProjectWorkTool {
    async fn call(&self, args: Value, ctx: &ToolCtx) -> Result<Value> {
        let caller = ctx
            .session_id()
            .and_then(|id| ctx.executing_caller_for(id))
            .ok_or_else(|| Error::ScopeDenied("Verified Project Session required".into()))?;
        let app = self
            .app
            .upgrade()
            .ok_or_else(|| Error::AuthorityUnavailable("Ticket Host unavailable".into()))?;
        if ctx.plan_read_only && matches!(args["action"].as_str(), Some("start" | "send")) {
            return Err(Error::ScopeDenied("Plan mode is read-only".into()));
        }
        if self.name == CONTACT {
            app.supervisor_project_manager(caller, decode::<SupervisorProjectRequest>(args)?)
                .await
        } else {
            app.project_manager_work(caller, decode::<ManagerRequest>(args)?)
                .await
        }
    }
}

#[async_trait]
impl Tool for ProjectWorkTool {
    fn name(&self) -> &str {
        self.name
    }

    fn description(&self) -> &str {
        if self.name == CONTACT {
            "Supervisor only. List Projects, inspect one Project's Work, or send an instruction to its Project Manager. Delivery is asynchronous."
        } else {
            "Project Manager only. List or inspect this Project's Work and sections; start an existing Work on explicit user instruction. Select a matching existing section ID and a registered prepared worktree, or omit it when there is only one."
        }
    }

    fn parameters_schema(&self) -> Value {
        if self.name == CONTACT {
            json!({
                "type": "object", "additionalProperties": false,
                "properties": {
                    "action": {"type":"string","enum":["projects","inspect","send"]},
                    "project_id": {"type":"string","description":"Exact Project ID for inspect or send"},
                    "message": {"type":"string","description":"Instruction for send"},
                    "operation_id": {"type":"string","format":"uuid","description":"Required for send; reuse on retries"}
                },
                "required":["action"]
            })
        } else {
            json!({
                "type": "object", "additionalProperties": false,
                "properties": {
                    "action":{"type":"string","enum":["list","inspect","start"]},
                    "work_id":{"type":"string","description":"Exact Work ID for inspect or start"},
                    "section_id":{"type":["string","null"],"description":"Existing Section ID for start, required when this Project has sections"},
                    "worktree":{"type":["string","null"],"description":"Registered isolated worktree for start"},
                    "operation_id":{"type":"string","format":"uuid","description":"Required for start; reuse after an uncertain result"}
                },
                "required":["action"]
            })
        }
    }

    fn classify(&self, args: &Value) -> ToolClass {
        if matches!(
            args["action"].as_str(),
            Some("projects" | "list" | "inspect")
        ) {
            ToolClass::READONLY_PARALLEL
        } else {
            ToolClass::MUTATING_SERIAL
        }
    }

    async fn invoke(
        &self,
        args: Value,
        ctx: ToolCtx,
    ) -> std::result::Result<ToolOutcome, ToolError> {
        let (success, result) = match self.call(args, &ctx).await {
            Ok(result) => (true, result),
            Err(error) => (
                false,
                json!({"error":error.to_string(), "status_code":error.status_code()}),
            ),
        };
        Ok(ToolOutcome::Completed(ToolResult::text(
            success,
            result.to_string(),
        )))
    }
}
