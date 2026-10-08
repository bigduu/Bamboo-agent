//! Construction owner for the existing legacy Skill Tool overlays.

use std::sync::Arc;

use bamboo_agent_core::tools::ToolExecutor;
use bamboo_llm::Config;
use bamboo_skills::SkillManager;
use tokio::sync::RwLock;

use super::{LoadSkillTool, ReadSkillResourceTool};
use crate::OverlayToolExecutor;

/// The consumer's existing permission-wrapped provider surface and typed policy.
/// An absent policy retains the legacy fail-closed provider behavior.
pub struct LegacySkillContextRegistry {
    pub tools: Arc<dyn ToolExecutor>,
    pub permission_config: Option<Arc<bamboo_tools::permission::PermissionConfig>>,
}

/// Add the existing load/read overlays without changing caller authority.
///
/// Server supplies its Project store and pre-Skill context registry. Deployed
/// workers omit both. Callers retain responsibility for excluding strict-native
/// surfaces; construction performs no IO or policy resolution.
pub fn assemble_legacy_skill_tools(
    base: Arc<dyn ToolExecutor>,
    skill_manager: Arc<SkillManager>,
    config: Arc<RwLock<Config>>,
    session_repo: bamboo_engine::SessionRepository,
    project_store: Option<Arc<bamboo_projects::ProjectStore>>,
    context_registry: Option<LegacySkillContextRegistry>,
) -> Arc<dyn ToolExecutor> {
    let mut load = LoadSkillTool::new(skill_manager.clone(), config.clone(), session_repo.clone());
    if let Some(projects) = project_store.as_ref() {
        load = load.with_project_store(projects.clone());
    }
    if let Some(registry) = context_registry {
        load = load
            .with_permission_checked_context_registry(registry.tools, registry.permission_config);
    }
    let with_load: Arc<dyn ToolExecutor> = Arc::new(OverlayToolExecutor::new(base, Arc::new(load)));
    let mut read = ReadSkillResourceTool::new(skill_manager, config, session_repo);
    if let Some(projects) = project_store {
        read = read.with_project_store(projects);
    }
    Arc::new(OverlayToolExecutor::new(with_load, Arc::new(read)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_agent_core::storage::Storage;
    use bamboo_agent_core::tools::{FunctionCall, ToolCall, ToolExecutionContext};
    use bamboo_agent_core::Session;
    use serde_json::json;

    fn call(name: &str, skill_id: &str) -> ToolCall {
        ToolCall {
            id: format!("assembly-{name}"),
            tool_type: "function".into(),
            function: FunctionCall {
                name: name.into(),
                arguments: json!({"skill_id": skill_id, "resource_path": "references/proof.txt"})
                    .to_string(),
            },
        }
    }

    #[tokio::test]
    async fn assembly_preserves_real_base_owners_schemas_receipts_provider_failures_and_plan_denials(
    ) {
        for registry_kind in ["worker", "untyped", "allow", "provider-fails"] {
            let home = tempfile::tempdir().unwrap();
            let skill = home.path().join("skills/assembly");
            std::fs::create_dir_all(skill.join("references")).unwrap();
            std::fs::write(skill.join("SKILL.md"), "---\nname: assembly\ndescription: Assembly\nmetadata:\n  dynamic_context:\n    - id: proof\n      tool: Read\n      input: {path: provider.txt}\n      max_chars: 512\n      timeout_ms: 1000\n---\nASSEMBLY_INSTRUCTIONS\n").unwrap();
            std::fs::write(skill.join("references/proof.txt"), "ASSEMBLY_RESOURCE\n").unwrap();
            if registry_kind == "provider-fails" {
                std::fs::File::create(home.path().join("provider.txt"))
                    .unwrap()
                    .set_len(64 * 1024 * 1024)
                    .unwrap();
            } else {
                std::fs::write(home.path().join("provider.txt"), "SUPPLIED_BASE_PROVIDER\n")
                    .unwrap();
            }
            let manager = Arc::new(SkillManager::with_config(bamboo_skills::SkillStoreConfig {
                skills_dir: home.path().join("skills"),
                ..Default::default()
            }));
            manager.initialize().await.unwrap();
            let config = Arc::new(RwLock::new(Config::default()));
            let storage: Arc<dyn Storage> = Arc::new(
                bamboo_storage::SessionStoreV2::new(home.path().join("sessions"))
                    .await
                    .unwrap(),
            );
            let repo = bamboo_engine::SessionRepository::new(
                Default::default(),
                storage.clone(),
                Arc::new(bamboo_storage::LockedSessionStore::new(storage.clone())),
            );
            let policy = Arc::new(bamboo_tools::permission::PermissionConfig::new());
            let base: Arc<dyn ToolExecutor> = Arc::new(
                bamboo_tools::BuiltinToolExecutor::new_with_config_and_permissions(
                    config.clone(),
                    Arc::new(bamboo_tools::permission::ConfigPermissionChecker::new(
                        policy.clone(),
                    )),
                ),
            );
            let typed_policy =
                matches!(registry_kind, "allow" | "provider-fails").then(|| policy.clone());
            let registry = (registry_kind != "worker").then(|| LegacySkillContextRegistry {
                tools: base.clone(),
                permission_config: typed_policy.clone(),
            });
            let assembled = assemble_legacy_skill_tools(
                base.clone(),
                manager.clone(),
                config.clone(),
                repo.clone(),
                None,
                registry,
            );
            // Retain the actual pre-extraction constructor path as a parity oracle.
            let mut old_load = LoadSkillTool::new(manager.clone(), config.clone(), repo.clone());
            if registry_kind != "worker" {
                old_load =
                    old_load.with_permission_checked_context_registry(base.clone(), typed_policy);
            }
            let old: Arc<dyn ToolExecutor> = Arc::new(OverlayToolExecutor::new(
                Arc::new(OverlayToolExecutor::new(base.clone(), Arc::new(old_load))),
                Arc::new(ReadSkillResourceTool::new(
                    manager,
                    config.clone(),
                    repo.clone(),
                )),
            ));
            assert_eq!(
                serde_json::to_value(assembled.list_tools()).unwrap(),
                serde_json::to_value(old.list_tools()).unwrap()
            );
            let original_owner = base.exact_tool_owner("Read").unwrap();
            assert!(std::ptr::addr_eq(
                original_owner,
                assembled.exact_tool_owner("Read").unwrap()
            ));
            assert!(!assembled.owns_exact_tool("skills_list"));
            assert!(!assembled.owns_exact_tool("skills_read"));
            for name in [
                "load_skill",
                "read_skill_resource",
                "Read",
                "functions.load_skill",
            ] {
                let call = call(name, "assembly");
                assert_eq!(
                    assembled.call_parallel_classification(&call),
                    old.call_parallel_classification(&call)
                );
            }
            let mut receipts = Vec::new();
            let mut blocks = Vec::new();
            for (id, tools) in [("original", &old), ("assembled", &assembled)] {
                let mut session = Session::new(id, "test-model");
                session.set_workspace_path_meta(home.path().to_string_lossy().into_owned());
                session.metadata.insert(
                    bamboo_skills::runtime_metadata::SKILL_RUNTIME_SELECTED_SKILL_IDS_KEY.into(),
                    "[\"assembly\"]".into(),
                );
                session
                    .metadata
                    .insert("external".into(), "retained".into());
                repo.save(&mut session).await.unwrap();
                let read = call("read_skill_resource", "assembly");
                let mut ctx = ToolExecutionContext::none(&read.id);
                ctx.session_id = Some(id);
                assert!(
                    tools.execute_with_context(&read, ctx).await.is_err(),
                    "resource is still gated before load"
                );
                let load = call("load_skill", "assembly");
                let mut ctx = ToolExecutionContext::none(&load.id);
                ctx.session_id = Some(id);
                ctx.bypass_permissions = true;
                let mut plan_ctx = ToolExecutionContext::none(&load.id);
                plan_ctx.session_id = Some(id);
                plan_ctx.plan_read_only = true;
                assert!(tools.execute_with_context(&load, plan_ctx).await.is_err());
                let result = tools.execute_with_context(&load, ctx).await.unwrap();
                assert!(result.success);
                let receipt: serde_json::Value = serde_json::from_str(&result.result).unwrap();
                assert_eq!(receipt["activation_status"], "active");
                receipts.push(receipt);
                let saved = storage.load_session(id).await.unwrap().unwrap();
                assert_eq!(saved.metadata["external"], "retained");
                assert!(saved.metadata
                    [bamboo_skills::runtime_metadata::LAST_LOADED_SKILL_SUMMARY_METADATA_KEY]
                    .contains("assembly"));
                let active: bamboo_skills::ActiveWorkflow = serde_json::from_str(
                    &saved.metadata[bamboo_skills::ACTIVE_WORKFLOW_METADATA_KEY],
                )
                .unwrap();
                assert_eq!(active.dynamic_context.len(), 1);
                let block = &active.dynamic_context[0];
                assert_eq!(
                    block.provenance,
                    if matches!(registry_kind, "worker" | "untyped") {
                        "typed_authority_unavailable"
                    } else {
                        "registered_tool_permission_checked"
                    }
                );
                if registry_kind == "allow" {
                    assert!(block.content.contains("SUPPLIED_BASE_PROVIDER"));
                } else {
                    assert!(block.content.is_empty());
                    assert!(block.diagnostic.is_some());
                }
                blocks.push((
                    block.status,
                    block.provenance.clone(),
                    block.content.clone(),
                ));
                let mut ctx = ToolExecutionContext::none(&read.id);
                ctx.session_id = Some(id);
                let result = tools.execute_with_context(&read, ctx).await.unwrap();
                assert!(result.success && result.result.contains("ASSEMBLY_RESOURCE"));
                let saved = storage.load_session(id).await.unwrap().unwrap();
                assert!(saved.metadata
                    [bamboo_skills::runtime_metadata::LAST_RESOURCE_READ_SUMMARY_METADATA_KEY]
                    .contains("references/proof.txt"));
            }
            assert_eq!(receipts[0], receipts[1]);
            assert_eq!(blocks[0], blocks[1]);
            config.write().await.skills.disabled = vec!["assembly".into()];
            for (id, tools) in [("original", &old), ("assembled", &assembled)] {
                let load = call("load_skill", "assembly");
                let mut ctx = ToolExecutionContext::none(&load.id);
                ctx.session_id = Some(id);
                assert!(tools.execute_with_context(&load, ctx).await.is_err());
                assert!(tools.execute(&load).await.is_err());
            }
        }
    }
}
