//! Host-selected profile application on the existing strict local Child route.
//! The persisted selection is an audit/replay binding, not a new storage grant.
use bamboo_domain::{ProviderModelRef, Role, Session};
use bamboo_skills::named_agents::{NamedAgentDefinition, NamedAgentProfileIdentity};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::ChildSessionError;

pub const NAMED_PROFILE_BINDING_KEY: &str = "child.named_profile.v1";
/// Host creator signal derived from the existing explicit create.model input.
pub const PROFILE_EXPLICIT_MODEL_KEY: &str = "child.named_profile.explicit_model.v1";
const NATIVE_NAMES: [&str; 5] = ["Bash", "Edit", "Glob", "Read", "Write"];

/// Only the host catalog producer constructs this value. No request decoder,
/// serialization or Debug output exposes its private instruction body.
pub struct ResolvedChildProfile {
    identity: NamedAgentProfileIdentity,
    prompt: String,
    model: Option<ProviderModelRef>,
    tools: Vec<String>,
    read_only: bool,
}

impl std::fmt::Debug for ResolvedChildProfile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResolvedChildProfile")
            .field("identity", &self.identity)
            .finish_non_exhaustive()
    }
}

fn rejected() -> ChildSessionError {
    ChildSessionError::Execution("named_profile_invalid_or_unsupported".into())
}

fn hash(text: &str) -> String {
    hex::encode(Sha256::digest(text.as_bytes()))
}

impl ResolvedChildProfile {
    pub fn from_catalog(
        identity: NamedAgentProfileIdentity,
        definition: &NamedAgentDefinition,
        parent: &Session,
        default_provider: &str,
    ) -> Result<Self, ChildSessionError> {
        if definition.name() != identity.name
            || definition.revision() != identity.revision
            || definition
                .tool_allow()
                .iter()
                .chain(definition.tool_deny())
                .any(|name| !NATIVE_NAMES.contains(&name.as_str()))
        {
            return Err(rejected());
        }
        let model = definition
            .model_hint()
            .map(|hint| {
                let (provider, model) = hint.split_once(':').unwrap_or_else(|| {
                    (
                        parent
                            .model_ref
                            .as_ref()
                            .map(|model| model.provider.as_str())
                            .or_else(|| parent.metadata.get("provider_name").map(String::as_str))
                            .unwrap_or(default_provider),
                        hint,
                    )
                });
                if provider.trim().is_empty() || model.trim().is_empty() {
                    return Err(rejected());
                }
                Ok(ProviderModelRef {
                    provider: provider.to_owned(),
                    model: model.to_owned(),
                    reasoning_effort: None,
                })
            })
            .transpose()?;
        let read_only = matches!(
            identity.name.as_str(),
            "explorer" | "reviewer" | "independent-reviewer"
        );
        let tools = NATIVE_NAMES
            .iter()
            .filter(|name| {
                (definition.tool_allow().is_empty()
                    || definition
                        .tool_allow()
                        .iter()
                        .any(|allowed| allowed == **name))
                    && !definition.tool_deny().iter().any(|denied| denied == **name)
                    && (!read_only || matches!(**name, "Read" | "Glob"))
            })
            .map(|name| (*name).to_owned())
            .collect();
        Ok(Self {
            identity,
            prompt: definition.system_prompt().to_owned(),
            model,
            tools,
            read_only,
        })
    }

    pub(super) fn model(&self) -> Option<&ProviderModelRef> {
        self.model.as_ref()
    }

    pub(super) fn read_only(&self) -> bool {
        self.read_only
    }

    pub(super) fn append_prompt(&self, base: &str) -> String {
        format!(
            "{base}\n\n## Selected child role: {}\n{}",
            self.identity.name, self.prompt
        )
    }

    pub(super) fn bind(
        self,
        child: &mut Session,
        parent: &Session,
    ) -> Result<(), ChildSessionError> {
        let base = child
            .metadata
            .get("base_system_prompt")
            .ok_or_else(rejected)?;
        let mut tools = self.tools;
        if let Some(binding) = read_binding(parent)? {
            tools.retain(|tool| binding.tools.contains(tool));
        }
        let binding = Binding {
            version: 1,
            name: self.identity.name,
            source: serde_json::to_value(self.identity.source).map_err(|_| rejected())?,
            project_id: self.identity.project_id.map(|id| id.to_string()),
            scope_project_id:
                match crate::project_context::ProjectContextResolver::session_project_identity(
                    child,
                ) {
                    crate::project_context::SessionProjectIdentity::Assigned(id) => {
                        Some(id.to_string())
                    }
                    crate::project_context::SessionProjectIdentity::Unassigned => None,
                    crate::project_context::SessionProjectIdentity::Invalid { .. } => {
                        return Err(rejected())
                    }
                },
            revision: self.identity.revision,
            child_id: child.id.clone(),
            child_created_at: child.created_at,
            parent_id: parent.id.clone(),
            root_id: child.root_session_id.clone(),
            model: child.model.clone(),
            model_ref: child.model_ref.clone(),
            read_only: child
                .agent_runtime_state
                .as_ref()
                .is_some_and(|r| r.read_only),
            prompt_sha256: hash(base),
            tools,
        };
        child.metadata.insert(
            NAMED_PROFILE_BINDING_KEY.into(),
            serde_json::to_string(&binding).map_err(|_| rejected())?,
        );
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Binding {
    version: u32,
    name: String,
    source: serde_json::Value,
    project_id: Option<String>,
    scope_project_id: Option<String>,
    revision: String,
    child_id: String,
    child_created_at: chrono::DateTime<chrono::Utc>,
    parent_id: String,
    root_id: String,
    model: String,
    model_ref: Option<ProviderModelRef>,
    read_only: bool,
    prompt_sha256: String,
    tools: Vec<String>,
}

fn read_binding(session: &Session) -> Result<Option<Binding>, ChildSessionError> {
    let Some(raw) = session.metadata.get(NAMED_PROFILE_BINDING_KEY) else {
        return Ok(None);
    };
    if raw.len() > 4096 {
        return Err(rejected());
    }
    let binding: Binding = serde_json::from_str(raw).map_err(|_| rejected())?;
    let project = crate::project_context::ProjectContextResolver::session_project_identity(session);
    let project = match project {
        crate::project_context::SessionProjectIdentity::Assigned(id) => Some(id.to_string()),
        crate::project_context::SessionProjectIdentity::Unassigned => None,
        crate::project_context::SessionProjectIdentity::Invalid { .. } => return Err(rejected()),
    };
    let source_valid =
        binding.source == "builtin" || binding.source == "global" || binding.source == "project";
    if binding.version != 1
        || !source_valid
        || binding.child_id != session.id
        || binding.child_created_at != session.created_at
        || session.parent_session_id.as_deref() != Some(binding.parent_id.as_str())
        || binding.root_id != session.root_session_id
        || binding.model != session.model
        || binding.model_ref != session.model_ref
        || binding.scope_project_id != project
        || (binding.source == "project" && binding.project_id != project)
        || (binding.source != "project" && binding.project_id.is_some())
        || binding.name.is_empty()
        || binding.name.len() > 64
        || binding.revision.len() != 64
        || !binding.revision.bytes().all(|b| b.is_ascii_hexdigit())
        || binding.tools.len() > 5
        || binding
            .tools
            .iter()
            .any(|t| !NATIVE_NAMES.contains(&t.as_str()))
        || binding
            .tools
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            != binding.tools.len()
        || binding.read_only
            != session
                .agent_runtime_state
                .as_ref()
                .is_some_and(|r| r.read_only)
        || session
            .metadata
            .get("base_system_prompt")
            .is_none_or(|base| hash(base) != binding.prompt_sha256)
        || session
            .messages
            .iter()
            .find(|m| m.role == Role::System)
            .is_none_or(|message| {
                !message
                    .content
                    .contains(session.metadata.get("base_system_prompt").unwrap())
            })
    {
        return Err(rejected());
    }
    Ok(Some(binding))
}

/// Validate the frozen producer observation without reopening the catalog.
pub fn validate_named_profile(session: &Session) -> Result<(), ChildSessionError> {
    read_binding(session).map(|_| ())
}

pub fn named_profile_tool_names(
    session: &Session,
) -> Result<Option<Vec<String>>, ChildSessionError> {
    read_binding(session).map(|binding| binding.map(|binding| binding.tools))
}

pub fn has_named_profile(session: &Session) -> bool {
    session.metadata.contains_key(NAMED_PROFILE_BINDING_KEY)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_domain::Message;
    use bamboo_skills::named_agents::{
        NamedAgentLimits, NamedAgentProfileStatus, ScopedNamedAgentCatalog,
    };

    fn definition(name: &str, tools: &str) -> String {
        format!("---\nschema_version: 1\nname: {name}\ndescription: Safe role metadata\nmodel_hint: openai:role-model\n{tools}\n---\nPRIVATE_ROLE_BODY\n")
    }

    fn selected(home: &std::path::Path, name: &str, parent: &Session) -> ResolvedChildProfile {
        let catalog =
            ScopedNamedAgentCatalog::discover(home, None, NamedAgentLimits::default()).unwrap();
        let identity = catalog
            .metadata()
            .entries
            .iter()
            .find(|row| {
                row.status == NamedAgentProfileStatus::Selectable
                    && row.identity.as_ref().is_some_and(|i| i.name == name)
            })
            .unwrap()
            .identity
            .clone()
            .unwrap();
        ResolvedChildProfile::from_catalog(
            identity.clone(),
            catalog.get(&identity).unwrap(),
            parent,
            "openai",
        )
        .unwrap()
    }

    fn bound(profile: ResolvedChildProfile, parent: &Session) -> Session {
        let mut child = Session::new_child_of("profile-child", parent, "role-model", "role");
        if let Some(project) = parent.project_id_meta() {
            child.set_project_id_meta(project);
        }
        child.model_ref = profile.model.clone();
        child
            .agent_runtime_state
            .get_or_insert_with(Default::default)
            .read_only = profile.read_only;
        let prompt = profile.append_prompt("GLOBAL_CUSTOM_BASE");
        child
            .metadata
            .insert("base_system_prompt".into(), prompt.clone());
        child.add_message(Message::system(prompt));
        profile.bind(&mut child, parent).unwrap();
        child
    }

    #[test]
    fn exact_selection_composes_custom_base_and_survives_source_reload() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().canonicalize().unwrap();
        std::fs::create_dir(home.join("agents")).unwrap();
        let file = home.join("agents/implementer.md");
        std::fs::write(
            &file,
            definition(
                "implementer",
                "tools:\n  allow: [Read, Write]\n  deny: [Edit]",
            ),
        )
        .unwrap();
        let parent = Session::new("profile-parent", "parent-model");
        let profile = selected(&home, "implementer", &parent);
        assert!(!format!("{profile:?}").contains("PRIVATE_ROLE_BODY"));
        let child = bound(profile, &parent);
        assert!(child.messages[0].content.contains("GLOBAL_CUSTOM_BASE"));
        assert!(child.messages[0].content.contains("PRIVATE_ROLE_BODY"));
        assert_eq!(child.model_ref.as_ref().unwrap().model, "role-model");
        assert_eq!(
            named_profile_tool_names(&child).unwrap().unwrap(),
            ["Read", "Write"]
        );
        let binding = child.metadata[NAMED_PROFILE_BINDING_KEY].clone();
        assert!(!binding.contains("PRIVATE_ROLE_BODY"));
        std::fs::write(&file, definition("implementer", "tools:\n  allow: [Read]")).unwrap();
        validate_named_profile(&child).unwrap();
        assert_eq!(child.metadata[NAMED_PROFILE_BINDING_KEY], binding);
        let later = bound(selected(&home, "implementer", &parent), &parent);
        assert_ne!(later.metadata[NAMED_PROFILE_BINDING_KEY], binding);
        assert_eq!(named_profile_tool_names(&later).unwrap().unwrap(), ["Read"]);
    }

    #[test]
    fn readonly_roles_only_narrow_and_binding_changes_fail_closed() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().canonicalize().unwrap();
        std::fs::create_dir(home.join("agents")).unwrap();
        let parent = Session::new("profile-parent", "parent-model");
        for name in ["explorer", "reviewer", "independent-reviewer"] {
            std::fs::write(
                home.join(format!("agents/{name}.md")),
                definition(name, "tools:\n  allow: [Read, Glob, Write]"),
            )
            .unwrap();
            let child = bound(selected(&home, name, &parent), &parent);
            assert!(child.agent_runtime_state.as_ref().unwrap().read_only);
            assert_eq!(
                named_profile_tool_names(&child).unwrap().unwrap(),
                ["Glob", "Read"]
            );
            for case in 0..4 {
                let mut changed = child.clone();
                match case {
                    0 => changed.model = "other-model".into(),
                    1 => changed.created_at += chrono::Duration::nanoseconds(1),
                    2 => changed.agent_runtime_state.as_mut().unwrap().read_only = false,
                    _ => changed.messages[0].content = "role removed".into(),
                }
                assert!(validate_named_profile(&changed).is_err());
            }
        }
    }

    #[test]
    fn unsupported_composition_declarations_do_not_become_native_grants() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().canonicalize().unwrap();
        std::fs::create_dir(home.join("agents")).unwrap();
        std::fs::write(
            home.join("agents/composer.md"),
            definition("composer", "tools:\n  allow: [SubAgent]"),
        )
        .unwrap();
        let catalog =
            ScopedNamedAgentCatalog::discover(&home, None, NamedAgentLimits::default()).unwrap();
        let identity = catalog.metadata().entries[0].identity.clone().unwrap();
        assert!(ResolvedChildProfile::from_catalog(
            identity.clone(),
            catalog.get(&identity).unwrap(),
            &Session::new("parent", "model"),
            "openai"
        )
        .is_err());
    }

    #[test]
    fn accepted_large_profile_body_is_composed_complete() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().canonicalize().unwrap();
        std::fs::create_dir(home.join("agents")).unwrap();
        let body = "Preserve bounded instructions. "
            .repeat(1024)
            .trim_end()
            .to_owned();
        assert!(body.len() > 16 * 1024);
        assert!(body.len() <= NamedAgentLimits::default().max_prompt_bytes);
        std::fs::write(
            home.join("agents/implementer.md"),
            definition("implementer", "tools:\n  allow: [Read]")
                .replace("PRIVATE_ROLE_BODY", &body),
        )
        .unwrap();
        let parent = Session::new("parent", "model");
        let child = bound(selected(&home, "implementer", &parent), &parent);
        assert!(child.messages[0].content.ends_with(&body));
        assert!(child.messages[0].content.contains("GLOBAL_CUSTOM_BASE"));
        validate_named_profile(&child).unwrap();
    }

    #[test]
    fn builtin_binding_retains_assigned_scope_without_a_source_project() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().canonicalize().unwrap();
        let catalog = ScopedNamedAgentCatalog::discover_with_builtins(
            &home,
            None,
            NamedAgentLimits::default(),
        )
        .unwrap();
        let identity = catalog
            .metadata()
            .entries
            .iter()
            .find_map(|row| row.identity.as_ref().filter(|id| id.name == "explorer"))
            .unwrap()
            .clone();
        assert!(identity.project_id.is_none());
        let mut parent = Session::new("parent", "model");
        parent.set_project_id_meta("assigned-project");
        let profile = ResolvedChildProfile::from_catalog(
            identity.clone(),
            catalog.get(&identity).unwrap(),
            &parent,
            "openai",
        )
        .unwrap();
        let child = bound(profile, &parent);
        let binding = read_binding(&child).unwrap().unwrap();
        assert_eq!(binding.source, "builtin");
        assert!(binding.project_id.is_none());
        assert_eq!(
            binding.scope_project_id.as_deref(),
            Some("assigned-project")
        );
        assert!(binding.read_only);
        let mut source_project = child.clone();
        let mut value: serde_json::Value =
            serde_json::from_str(&source_project.metadata[NAMED_PROFILE_BINDING_KEY]).unwrap();
        value["project_id"] = "assigned-project".into();
        source_project
            .metadata
            .insert(NAMED_PROFILE_BINDING_KEY.into(), value.to_string());
        assert!(validate_named_profile(&source_project).is_err());
        let mut foreign_scope = child;
        foreign_scope.set_project_id_meta("foreign-project");
        assert!(validate_named_profile(&foreign_scope).is_err());
    }
}
