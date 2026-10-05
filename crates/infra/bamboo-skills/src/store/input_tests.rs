//! Input-only acceptance for the pinned Codex parser and host sidecar projection.

use super::*;
use crate::catalog::{load_bundle_metadata, WorkflowSource};
use crate::store::builtin::{builtin_workflow_catalog_entry, load_builtin_skill_bundles};
use tokio::fs;

async fn skill(root: &Path, id: &str, label: &str, frontmatter: &str) -> PathBuf {
    let dir = root.join(id);
    fs::create_dir_all(dir.join("agents")).await.unwrap();
    fs::write(
        dir.join("SKILL.md"),
        format!("---\nname: Display {label}\ndescription: Description {label}\n{frontmatter}---\nBody {label}\n"),
    )
    .await
    .unwrap();
    dir
}

async fn openai(dir: &Path, summary: &str, implicit: bool) {
    fs::write(
        dir.join("agents/openai.yaml"),
        format!("interface:\n  short_description: '{summary}'\npolicy:\n  allow_implicit_invocation: {implicit}\n"),
    )
    .await
    .unwrap();
}

async fn input(
    store: &SkillStore,
    id: &str,
    mode: Option<&str>,
) -> (SkillDefinition, WorkflowCatalogEntry) {
    let (skills, catalog) = store.skills_and_catalog_for_mode(mode).await.unwrap();
    let skill = skills.into_iter().find(|skill| skill.id == id).unwrap();
    let entry = catalog
        .entries
        .into_iter()
        .find(|entry| entry.id == id)
        .unwrap();
    (skill, entry)
}

#[tokio::test]
async fn codex_input_optional_sidecars_and_frontmatter_precedence() {
    let temp = tempfile::tempdir().unwrap();
    let root = skill(
        temp.path(),
        "portable",
        "初始",
        "metadata:\n  short-description: Front   summary\n  custom: preserved\n",
    )
    .await;
    let store = SkillStore::new(SkillStoreConfig {
        skills_dir: temp.path().to_path_buf(),
        ..Default::default()
    });
    store.initialize().await.unwrap();
    let (initial, entry) = input(&store, "portable", None).await;
    assert_eq!(initial.name, "Display 初始");
    assert_eq!(initial.short_description.as_deref(), Some("Front summary"));
    assert_eq!(entry.invocation_policy["automatic"], true);

    openai(&root, "Sidecar  summary", false).await;
    let raw = crate::catalog::read_openai_metadata_through_resources(
        &root,
        SkillSnapshotLimits::default().max_file_bytes,
    )
    .await
    .unwrap();
    assert_eq!(
        raw,
        fs::read(root.join("agents/openai.yaml")).await.unwrap()
    );
    assert!(
        crate::catalog::read_openai_metadata_through_resources(&root, 8)
            .await
            .is_none()
    );

    let (definition, entry) = input(&store, "portable", None).await;
    assert_eq!(definition.short_description, initial.short_description);
    assert_eq!(definition.metadata, initial.metadata);
    assert_eq!(definition.prompt, initial.prompt);
    assert_eq!(
        entry.invocation_policy,
        serde_json::json!({"explicit": true, "automatic": false})
    );

    for optional in [
        "interface: [\n",
        "policy:\n  allow_implicit_invocation: wrong\n",
    ] {
        fs::write(root.join("agents/openai.yaml"), optional)
            .await
            .unwrap();
        let (definition, entry) = input(&store, "portable", None).await;
        assert_eq!(definition.short_description, initial.short_description);
        assert_eq!(entry.status, WorkflowStatus::Valid);
        assert_eq!(entry.invocation_policy["automatic"], true);
    }
    for summary in ["", "x"] {
        openai(&root, &summary.repeat(1025), false).await;
        let metadata = load_bundle_metadata(&root).await.unwrap();
        assert!(metadata.short_description.is_none());
        assert_eq!(metadata.invocation_policy["automatic"], false);
    }
    fs::write(root.join("agents/openai.yaml"), "interface:\n  short_description: [bad, shape]\npolicy:\n  allow_implicit_invocation: false\n").await.unwrap();
    assert_eq!(
        load_bundle_metadata(&root).await.unwrap().invocation_policy["automatic"],
        false,
        "descriptive error must not drop a valid deny"
    );
}

#[tokio::test]
async fn codex_input_host_denies_legacy_manual_only_and_invalid_policy_lkg() {
    let temp = tempfile::tempdir().unwrap();
    let root = skill(
        temp.path(),
        "portable",
        "N",
        "metadata:\n  legacy_manual_only: true\n  custom: retained\nallowed-tools: Read\n",
    )
    .await;
    fs::write(
        root.join("agents/bamboo.yaml"),
        "invocation_policy:\n  explicit: false\n  automatic: true\n  extension: retained\n",
    )
    .await
    .unwrap();
    openai(&root, "N summary", true).await;
    let store = SkillStore::new(SkillStoreConfig {
        skills_dir: temp.path().to_path_buf(),
        ..Default::default()
    });
    store.initialize().await.unwrap();
    let (before, before_entry) = input(&store, "portable", None).await;
    assert_eq!(before.short_description.as_deref(), Some("N summary"));
    assert_eq!(before.tool_refs, ["Read"]);
    assert_eq!(before_entry.invocation_policy["explicit"], false);
    assert_eq!(before_entry.invocation_policy["automatic"], false);
    assert_eq!(before_entry.invocation_policy["extension"], "retained");
    openai(&root, "N+1 summary", false).await;
    skill(temp.path(), "portable", "N+1", "").await;
    for broken in [
        "invocation_policy: [\n",
        "invocation_policy: unrestricted\n",
        "invocation_policy:\n  explicit: false\n  automatic: wrong\n",
    ] {
        fs::write(root.join("agents/bamboo.yaml"), broken)
            .await
            .unwrap();
        let (retained, entry) = input(&store, "portable", None).await;
        assert_eq!(retained, before);
        assert_eq!(entry.status, WorkflowStatus::Invalid);
        assert_eq!(entry.invocation_policy, before_entry.invocation_policy);
        assert_eq!(entry.content_digest, before_entry.content_digest);
    }
    fs::write(
        root.join("agents/bamboo.yaml"),
        "invocation_policy:\n  explicit: false\n  automatic: true\n",
    )
    .await
    .unwrap();
    let (recovered, entry) = input(&store, "portable", None).await;
    assert_eq!(recovered.short_description.as_deref(), Some("N+1 summary"));
    assert_eq!(recovered.prompt, "Body N+1");
    assert_eq!(entry.status, WorkflowStatus::Valid);
    assert_eq!(
        entry.invocation_policy,
        serde_json::json!({"explicit": false, "automatic": false})
    );

    fs::write(
        root.join("agents/bamboo.yaml"),
        "invocation_policy:\n  explicit: true\n  automatic: false\n",
    )
    .await
    .unwrap();
    openai(&root, "Allowed summary", true).await;
    let (_, entry) = input(&store, "portable", None).await;
    assert_eq!(
        entry.invocation_policy,
        serde_json::json!({"explicit": true, "automatic": false})
    );
}

#[tokio::test]
async fn codex_input_orchestration_keeps_classification_and_intersects_host_policy() {
    let temp = tempfile::tempdir().unwrap();
    let root = skill(temp.path(), "compose", "compose", "").await;
    fs::write(root.join("workflow.yaml"), "id: compose\nname: Compose\ndescription: Composes a task\nversion: '7'\ninvocation_policy:\n  explicit: true\n  automatic: true\ncomposition:\n  type: call\n  tool: Read\n  args: {}\n").await.unwrap();
    fs::write(
        root.join("agents/bamboo.yaml"),
        "version: ignored\ninvocation_policy:\n  explicit: false\n  automatic: true\n",
    )
    .await
    .unwrap();
    openai(&root, "Compose summary", false).await;
    let metadata = load_bundle_metadata(&root).await.unwrap();
    assert_eq!(metadata.kind, WorkflowKind::Orchestration);
    assert_eq!(metadata.version, "7");
    assert_eq!(
        metadata.short_description.as_deref(),
        Some("Compose summary")
    );
    assert_eq!(
        metadata.invocation_policy,
        serde_json::json!({"explicit": false, "automatic": false})
    );
    fs::write(
        root.join("agents/bamboo.yaml"),
        "invocation_policy:\n  explicit: wrong\n",
    )
    .await
    .unwrap();
    assert!(
        load_bundle_metadata(&root).await.is_err(),
        "a selected workflow cannot conceal a malformed host restriction"
    );
}

#[tokio::test]
async fn codex_input_source_mode_refresh_is_one_publication() {
    let temp = tempfile::tempdir().unwrap();
    let global = temp.path().join("data/skills");
    let project = temp.path().join("project-home");
    let workspace = temp.path().join("workspace");
    fs::create_dir_all(&global).await.unwrap();
    fs::create_dir_all(&project).await.unwrap();
    fs::create_dir_all(&workspace).await.unwrap();
    let store = SkillStore::new_with_resource_scope(
        SkillStoreConfig {
            skills_dir: global.clone(),
            ..Default::default()
        },
        Arc::new(RetainedResourceBudget::default()),
        SkillSnapshotLimits::default(),
        Some(project.clone()),
        Some(workspace.clone()),
    );
    store.initialize().await.unwrap();
    let (builtin, entry) = input(&store, "review", None).await;
    assert_eq!(entry.source, WorkflowSource::Builtin);
    assert_eq!(
        builtin.short_description.as_deref(),
        Some("Find actionable code risks with evidence")
    );
    let bundle = load_builtin_skill_bundles()
        .unwrap()
        .into_iter()
        .find(|bundle| bundle.skill.id == "review")
        .unwrap();
    let projected = builtin_workflow_catalog_entry(&bundle, entry.revision).unwrap();
    assert_eq!(projected.invocation_policy, entry.invocation_policy);
    assert_eq!(projected.content_digest, entry.content_digest);

    let user_root = skill(&global, "review", "user", "").await;
    openai(&user_root, "User summary", false).await;
    let (user, entry) = input(&store, "review", None).await;
    assert_eq!(entry.source, WorkflowSource::User);
    assert_eq!(user.short_description.as_deref(), Some("User summary"));
    assert_eq!(entry.invocation_policy["automatic"], false);
    let project_root = skill(&project.join("skills"), "review", "project", "").await;
    openai(&project_root, "Project summary", true).await;
    let (project_skill, entry) = input(&store, "review", None).await;
    assert_eq!(entry.source, WorkflowSource::Project);
    assert_eq!(
        project_skill.short_description.as_deref(),
        Some("Project summary")
    );
    assert_eq!(entry.invocation_policy["automatic"], true);
    let workspace_root = skill(&workspace.join(".bamboo/skills"), "review", "workspace", "").await;
    openai(&workspace_root, "Workspace summary", true).await;
    let (default_skill, default_entry) = input(&store, "review", None).await;
    assert_eq!(default_entry.source, WorkflowSource::Workspace);
    assert_eq!(
        default_skill.short_description.as_deref(),
        Some("Workspace summary")
    );
    let mode_root = skill(&workspace.join(".bamboo/skills-code"), "review", "mode", "").await;
    openai(&mode_root, "Mode summary", false).await;
    let (mode, mode_entry) = input(&store, "review", Some("code")).await;
    assert_eq!(mode.short_description.as_deref(), Some("Mode summary"));
    assert_eq!(mode.prompt, "Body mode");
    assert_eq!(mode_entry.invocation_policy["automatic"], false);
    assert_eq!(
        input(&store, "review", None).await,
        (default_skill.clone(), default_entry.clone())
    );
    openai(&workspace_root, "Workspace refreshed", false).await;
    let (refreshed, refreshed_entry) = input(&store, "review", None).await;
    assert_eq!(refreshed.prompt, default_skill.prompt);
    assert_eq!(
        refreshed.short_description.as_deref(),
        Some("Workspace refreshed")
    );
    assert_eq!(refreshed_entry.invocation_policy["automatic"], false);
    assert_ne!(refreshed_entry.content_digest, default_entry.content_digest);
    assert!(refreshed_entry.revision > default_entry.revision);
    assert_eq!(
        input(&store, "review", Some("code")).await,
        (mode, mode_entry)
    );
    // Absent optional metadata falls back within the same winner, never to a shadowed source.
    fs::remove_file(workspace_root.join("agents/openai.yaml"))
        .await
        .unwrap();
    let (without, entry) = input(&store, "review", None).await;
    assert!(without.short_description.is_none());
    assert_eq!(entry.source, WorkflowSource::Workspace);
    assert_eq!(entry.invocation_policy["automatic"], true);
}

#[cfg(unix)]
#[tokio::test]
async fn codex_input_ignores_linked_optional_sidecar_files_and_directories() {
    use std::os::unix::fs::symlink;
    let temp = tempfile::tempdir().unwrap();
    let root = skill(&temp.path().join("skills"), "portable", "local", "").await;
    let outside = temp.path().join("outside");
    fs::create_dir_all(&outside).await.unwrap();
    fs::write(outside.join("openai.yaml"), "interface:\n  short_description: EXTERNAL PRIVATE SUMMARY\npolicy:\n  allow_implicit_invocation: false\n").await.unwrap();
    symlink(outside.join("openai.yaml"), root.join("agents/openai.yaml")).unwrap();
    assert!(crate::catalog::read_openai_metadata_through_resources(
        &root,
        SkillSnapshotLimits::default().max_file_bytes
    )
    .await
    .is_none());
    let store = SkillStore::new(SkillStoreConfig {
        skills_dir: temp.path().join("skills"),
        ..Default::default()
    });
    store.initialize().await.unwrap();
    let (definition, entry) = input(&store, "portable", None).await;
    assert!(definition.short_description.is_none());
    assert_eq!(entry.invocation_policy["automatic"], true);
    assert!(!serde_json::to_string(&definition)
        .unwrap()
        .contains("EXTERNAL PRIVATE SUMMARY"));
    fs::remove_file(root.join("agents/openai.yaml"))
        .await
        .unwrap();
    fs::remove_dir(root.join("agents")).await.unwrap();
    symlink(&outside, root.join("agents")).unwrap();
    assert!(crate::catalog::read_openai_metadata_through_resources(
        &root,
        SkillSnapshotLimits::default().max_file_bytes
    )
    .await
    .is_none());
    assert_eq!(input(&store, "portable", None).await, (definition, entry));
}
