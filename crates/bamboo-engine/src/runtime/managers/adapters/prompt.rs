use std::sync::Arc;

use bamboo_agent_core::Role;
use bamboo_agent_core::tools::ToolExecutor;
use bamboo_agent_core::Session;

use crate::runtime::config::AgentLoopConfig;
use crate::runtime::managers::prompt::{PromptAssemblyOutput, PromptManager};

/// Default prompt manager that delegates to existing runner functions.
pub struct DefaultPromptManager {
    tools: Arc<dyn ToolExecutor>,
}

impl DefaultPromptManager {
    pub fn new(tools: Arc<dyn bamboo_agent_core::tools::ToolExecutor>) -> Self {
        Self { tools }
    }
}

#[async_trait::async_trait]
impl PromptManager for DefaultPromptManager {
    async fn assemble_prompt(
        &self,
        session: &mut Session,
        config: &AgentLoopConfig,
    ) -> PromptAssemblyOutput {
        let base_prompt =
            crate::runtime::runner::session_setup::prompt_setup::resolve_base_prompt_for_language(
                config, session,
            );

        let tool_schemas =
            crate::runtime::runner::session_setup::tool_schemas::resolve_available_tool_schemas_for_session(
                config,
                self.tools.as_ref(),
                session,
            );

        let tool_guide_context =
            crate::runtime::runner::session_setup::prompt_setup::build_tool_guide_context(
                config,
                &tool_schemas,
                base_prompt,
                &session.id,
            );

        let skill_context = session
            .metadata
            .get("skill.context")
            .cloned()
            .unwrap_or_default();

        let _report =
            crate::runtime::runner::session_setup::prompt_setup::apply_system_prompt_contexts(
                session,
                config,
                &skill_context,
                &tool_guide_context,
            );

        let effective_system_prompt = session
            .messages
            .iter()
            .find_map(|m| {
                if matches!(m.role, Role::System) {
                    Some(m.content.clone())
                } else {
                    None
                }
            })
            .unwrap_or_default();

        PromptAssemblyOutput {
            effective_system_prompt,
            skill_context,
            tool_guide_context,
        }
    }

    async fn refresh_external_memory(&self, session: &mut Session, config: &AgentLoopConfig) {
        crate::runtime::runner::prompt_context::inject_external_memory_into_system_message(
            session,
            config.prompt_memory_flags,
            None,
        )
        .await;
    }

    fn refresh_task_list(&self, session: &mut Session) {
        crate::runtime::runner::prompt_context::inject_task_list_into_system_message(session);
    }
}
