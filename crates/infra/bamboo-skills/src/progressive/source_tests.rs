use super::source::{SourceLimits, SourcePool};
use crate::store::storage::SkillDirectorySource;
use crate::{SkillStore, SkillStoreConfig, WorkflowStatus};
use std::path::Path;
use std::sync::Arc;

const MAIN: &str = "---\nname: source-fixture\ndescription: source fixture\n---\nBody\n";

struct Fixture {
    _directory: tempfile::TempDir,
    root: std::path::PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        // Store discovery includes builtin/plugins/legacy siblings: keep every
        // fixture's parent unique instead of reading shared system-temp siblings.
        let root = directory.path().join("fixture");
        std::fs::create_dir(&root).unwrap();
        Self {
            _directory: directory,
            root,
        }
    }

    fn path(&self) -> &Path {
        &self.root
    }
}

fn bundle(base: &Path) -> std::path::PathBuf {
    let root = base.join("source-fixture");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("SKILL.md"), MAIN).unwrap();
    root
}

fn store(base: &Path) -> SkillStore {
    SkillStore::new(SkillStoreConfig {
        skills_dir: base.into(),
        ..Default::default()
    })
}

async fn row(store: &SkillStore) -> crate::WorkflowCatalogEntry {
    store
        .skill_catalog_snapshot()
        .await
        .entries
        .into_iter()
        .find(|entry| entry.id == "source-fixture")
        .unwrap()
}

#[tokio::test]
async fn unchanged_capture_is_noop_but_raw_outer_whitespace_changes_revision() {
    let temp = Fixture::new();
    let root = bundle(temp.path());
    let store = store(temp.path());
    store.reload().await.unwrap();
    let before = row(&store).await;
    store.reload().await.unwrap();
    assert_eq!(row(&store).await.revision, before.revision);
    std::fs::write(root.join("SKILL.md"), format!("{MAIN}\n")).unwrap();
    store.reload().await.unwrap();
    assert!(
        row(&store).await.revision > before.revision,
        "raw SKILL.md changes must defeat parsed-definition no-op"
    );
}

#[tokio::test]
async fn byte_identical_main_replacement_changes_revision() {
    let temp = Fixture::new();
    let root = bundle(temp.path());
    let store = store(temp.path());
    store.reload().await.unwrap();
    let before = row(&store).await;
    std::fs::rename(root.join("SKILL.md"), root.join("previous")).unwrap();
    std::fs::write(root.join("SKILL.md"), MAIN).unwrap();
    std::fs::remove_file(root.join("previous")).unwrap();
    store.reload().await.unwrap();
    assert!(
        row(&store).await.revision > before.revision,
        "physical main identity must defeat identical-byte no-op"
    );
}

#[tokio::test]
async fn policy_capture_a_cannot_authorize_auxiliary_b() {
    for (captured, auxiliary) in [(true, false), (false, true)] {
        let temp = Fixture::new();
        let root = bundle(temp.path());
        std::fs::create_dir(root.join("agents")).unwrap();
        std::fs::write(
            root.join("agents/openai.yaml"),
            format!("policy:\n  allow_implicit_invocation: {captured}\n"),
        )
        .unwrap();
        let store = store(temp.path());
        store.set_source_snapshot_hook(move || {
            std::fs::write(
                root.join("agents/openai.yaml"),
                format!("policy:\n  allow_implicit_invocation: {auxiliary}\n"),
            )
            .unwrap();
        });
        store.reload().await.unwrap();
        assert_eq!(
            row(&store).await.status,
            WorkflowStatus::Invalid,
            "captured policy and auxiliary snapshot must be coherent"
        );
        assert!(!store.source_bindings().await.contains_key("source-fixture"));
        assert!(
            store.get_skill("source-fixture").await.is_err(),
            "a first rejected capture has no management LKG"
        );
    }
}

#[tokio::test]
async fn true_absent_is_valid_but_openai_read_error_is_unavailable() {
    let temp = Fixture::new();
    let root = bundle(temp.path());
    let store = store(temp.path());
    store.reload().await.unwrap();
    assert_eq!(row(&store).await.status, WorkflowStatus::Valid);
    std::fs::create_dir_all(root.join("agents/openai.yaml")).unwrap();
    store.reload().await.unwrap();
    assert_eq!(
        row(&store).await.status,
        WorkflowStatus::Invalid,
        "an existing unreadable sidecar cannot become Absent/default allow"
    );
    assert!(!store.source_bindings().await.contains_key("source-fixture"));
}

fn capture(pool: &SourcePool, base: &Path) -> super::source::CapturedSkill {
    pool.capture(
        base,
        Path::new("source-fixture"),
        SkillDirectorySource::Global,
        None,
        4096,
    )
    .unwrap()
}

#[test]
fn hundreds_of_bundles_share_one_retained_root_and_keep_scope_separate() {
    let temp = Fixture::new();
    let pool = SourcePool::default();
    bundle(temp.path());
    let first = capture(&pool, temp.path());
    let mut snapshots = Vec::new();
    for index in 0..300 {
        let name = format!("bundle-{index}");
        std::fs::create_dir(temp.path().join(&name)).unwrap();
        std::fs::write(temp.path().join(&name).join("SKILL.md"), MAIN).unwrap();
        let next = pool
            .capture(
                temp.path(),
                Path::new(&name),
                SkillDirectorySource::Workspace,
                Some("focused".into()),
                4096,
            )
            .unwrap();
        assert!(first.binding.same_root(&next.binding));
        assert_ne!(first.binding, next.binding);
        snapshots.push(next);
    }
    assert_eq!(pool.counts(), (1, 0, 1));
    drop(snapshots);
    assert_eq!(pool.counts(), (1, 0, 1));
    drop(first);
    pool.prune();
    assert_eq!(pool.counts(), (0, 0, 0));
}

#[test]
fn bounded_pool_preserves_old_references_and_prunes_dead_index_keys() {
    let temp = Fixture::new();
    let other = tempfile::tempdir().unwrap();
    bundle(temp.path());
    bundle(other.path());
    let pool = SourcePool::new(SourceLimits {
        roots: 1,
        temporary: 8,
        index: 1,
    });
    let old = capture(&pool, temp.path());
    let duplicate = capture(&pool, temp.path());
    assert!(old.binding.same_root(&duplicate.binding));
    assert!(pool.admit(other.path()).is_err());
    assert_eq!(pool.counts(), (1, 0, 1));
    drop(duplicate);
    let held = old.binding.clone();
    drop(old);
    assert!(pool.admit(other.path()).is_err());
    drop(held);
    for index in 0..40 {
        let path = other.path().join(format!("root-{index}"));
        std::fs::create_dir(&path).unwrap();
        let root = pool.admit(&path).unwrap();
        assert_eq!(pool.counts(), (1, 0, 1));
        drop(root);
    }
    pool.prune();
    assert_eq!(pool.counts(), (0, 0, 0));
}

#[test]
fn temporary_budget_and_io_failure_release_every_charge() {
    let temp = Fixture::new();
    bundle(temp.path());
    let too_small = SourcePool::new(SourceLimits {
        roots: 1,
        temporary: 1,
        index: 1,
    });
    assert!(too_small.admit(temp.path()).is_err());
    assert_eq!(too_small.counts(), (0, 0, 0));
    let pool = SourcePool::default();
    assert!(pool.admit(&temp.path().join("absent")).is_err());
    assert_eq!(pool.counts(), (0, 0, 0));
    assert!(pool
        .capture(
            temp.path(),
            Path::new("../escape"),
            SkillDirectorySource::Global,
            None,
            4096
        )
        .is_err());
    pool.prune();
    assert_eq!(pool.counts(), (0, 0, 0));
    capture(&pool, temp.path());
    pool.prune();
    assert_eq!(pool.counts(), (0, 0, 0));
}

#[tokio::test]
async fn cancelling_a_root_consumer_releases_its_retained_reference() {
    let temp = Fixture::new();
    let pool = Arc::new(SourcePool::default());
    let root = pool.admit(temp.path()).unwrap();
    let (ready, received) = tokio::sync::oneshot::channel();
    let consumer = tokio::spawn(async move {
        let _root = root;
        ready.send(()).unwrap();
        std::future::pending::<()>().await;
    });
    received.await.unwrap();
    assert_eq!(pool.counts().0, 1);
    consumer.abort();
    assert!(consumer.await.unwrap_err().is_cancelled());
    pool.prune();
    assert_eq!(pool.counts(), (0, 0, 0));
}

#[test]
fn released_anchor_is_readmitted_but_unchanged_live_anchor_is_reused() {
    let temp = Fixture::new();
    bundle(temp.path());
    let pool = SourcePool::default();
    let first = capture(&pool, temp.path());
    let second = capture(&pool, temp.path());
    assert_eq!(first.binding, second.binding);
    // Keep the admission nonce without keeping the source root alive.
    let before = first.binding.anchor();
    drop(first);
    drop(second);
    let after = capture(&pool, temp.path());
    assert_ne!(before, after.binding.anchor());
    assert_eq!(pool.counts(), (1, 0, 1));
}

#[tokio::test]
async fn byte_identical_bundle_and_source_root_replacement_invalidate_bindings() {
    for replace_source in [false, true] {
        let temp = Fixture::new();
        let base = temp.path().join("skills");
        let root = bundle(&base);
        let store = store(&base);
        store.reload().await.unwrap();
        let old = store
            .source_bindings()
            .await
            .remove("source-fixture")
            .unwrap();
        let before = row(&store).await;
        let replaced = if replace_source { &base } else { &root };
        std::fs::rename(replaced, temp.path().join("old")).unwrap();
        bundle(&base);
        store.reload().await.unwrap();
        let new = store
            .source_bindings()
            .await
            .remove("source-fixture")
            .unwrap();
        assert_ne!(old, new);
        assert_eq!(old.same_root(&new), !replace_source);
        assert!(row(&store).await.revision > before.revision);
        assert!(old
            .validate(&store.source_pool(), &Default::default(), 4096)
            .is_err());
    }
}

#[tokio::test]
async fn policy_present_absent_and_same_bytes_replacement_cannot_race_publication() {
    for transition in 0..3 {
        let temp = Fixture::new();
        let root = bundle(temp.path());
        std::fs::create_dir(root.join("agents")).unwrap();
        let policy = root.join("agents/openai.yaml");
        if transition != 0 {
            std::fs::write(&policy, "policy:\n  allow_implicit_invocation: false\n").unwrap();
        }
        let store = store(temp.path());
        store.set_source_snapshot_hook(move || match transition {
            0 => std::fs::write(&policy, "policy:\n  allow_implicit_invocation: false\n").unwrap(),
            1 => std::fs::remove_file(&policy).unwrap(),
            _ => {
                std::fs::rename(&policy, root.join("old-policy")).unwrap();
                std::fs::write(&policy, "policy:\n  allow_implicit_invocation: false\n").unwrap();
            }
        });
        store.reload().await.unwrap();
        assert_eq!(row(&store).await.status, WorkflowStatus::Invalid);
        assert!(!store.source_bindings().await.contains_key("source-fixture"));
        store.reload().await.unwrap();
        assert_eq!(row(&store).await.status, WorkflowStatus::Valid);
        assert!(store.source_bindings().await.contains_key("source-fixture"));
    }
}

#[tokio::test]
async fn invalid_lkg_refresh_failure_removal_and_recovery_never_reuse_old_authority() {
    let temp = Fixture::new();
    let base = temp.path().join("skills");
    let root = bundle(&base);
    let store = store(&base);
    store.reload().await.unwrap();
    let held = store
        .source_bindings()
        .await
        .remove("source-fixture")
        .unwrap();
    std::fs::write(
        root.join("SKILL.md"),
        "---\nname: source-fixture\nallowed-tools: [\n---\nBAD",
    )
    .unwrap();
    store.reload().await.unwrap();
    assert_eq!(row(&store).await.status, WorkflowStatus::Invalid);
    assert_eq!(
        store.get_skill("source-fixture").await.unwrap().prompt,
        "Body"
    );
    assert!(!store.source_bindings().await.contains_key("source-fixture"));
    std::fs::write(root.join("SKILL.md"), MAIN).unwrap();
    store.reload().await.unwrap();
    assert!(store.source_bindings().await.contains_key("source-fixture"));
    std::fs::create_dir(root.join("agents")).unwrap();
    std::fs::write(root.join("agents/bamboo.yaml"), "invocation_policy: [bad]").unwrap();
    store.reload().await.unwrap();
    assert!(!store.source_bindings().await.contains_key("source-fixture"));
    assert_eq!(
        store.get_skill("source-fixture").await.unwrap().prompt,
        "Body"
    );
    std::fs::remove_file(root.join("agents/bamboo.yaml")).unwrap();
    store.reload().await.unwrap();
    std::fs::write(
        root.join("too-large.txt"),
        vec![b'x'; crate::store::SkillSnapshotLimits::default().max_file_bytes + 1],
    )
    .unwrap();
    assert!(store.reload().await.is_err());
    assert_eq!(
        store.get_skill("source-fixture").await.unwrap().prompt,
        "Body"
    );
    assert!(!store.source_bindings().await.contains_key("source-fixture"));
    assert_eq!(
        store.source_pool().counts().0,
        1,
        "external old binding remains charged"
    );
    drop(held);
    std::fs::remove_file(root.join("too-large.txt")).unwrap();
    store.reload().await.unwrap();
    assert!(store.source_bindings().await.contains_key("source-fixture"));
    std::fs::remove_dir_all(&root).unwrap();
    store.reload().await.unwrap();
    assert!(store.source_bindings().await.is_empty());
    store.source_pool().prune();
    assert_eq!(store.source_pool().counts(), (0, 0, 0));
}

#[tokio::test]
async fn optional_malformed_openai_is_tolerant_but_valid_deny_and_host_denies_intersect() {
    let temp = Fixture::new();
    let root = bundle(temp.path());
    std::fs::create_dir(root.join("agents")).unwrap();
    std::fs::write(root.join("agents/openai.yaml"), "policy: [malformed").unwrap();
    let store = store(temp.path());
    store.reload().await.unwrap();
    assert_eq!(row(&store).await.status, WorkflowStatus::Valid);
    assert!(store.source_bindings().await.contains_key("source-fixture"));
    std::fs::write(
        root.join("agents/openai.yaml"),
        "policy:\n  allow_implicit_invocation: false\n",
    )
    .unwrap();
    std::fs::write(
        root.join("agents/bamboo.yaml"),
        "invocation_policy:\n  explicit: false\n  automatic: true\n",
    )
    .unwrap();
    store.reload().await.unwrap();
    let entry = row(&store).await;
    assert_eq!(entry.invocation_policy["explicit"], false);
    assert_eq!(entry.invocation_policy["automatic"], false);
    assert!(store.source_bindings().await.contains_key("source-fixture"));
}

#[test]
fn bounded_regular_utf8_input_and_unknown_io_errors_are_not_absent() {
    let temp = Fixture::new();
    let root = bundle(temp.path());
    let pool = SourcePool::default();
    let initial = capture(&pool, temp.path());
    initial
        .binding
        .validate(&pool, &initial.policies, 4096)
        .unwrap();
    std::fs::write(root.join("SKILL.md"), [0xff, 0xfe]).unwrap();
    assert!(pool
        .capture(
            temp.path(),
            Path::new("source-fixture"),
            SkillDirectorySource::Global,
            None,
            4096
        )
        .is_err());
    std::fs::write(root.join("SKILL.md"), MAIN).unwrap();
    assert!(pool
        .capture(
            temp.path(),
            Path::new("source-fixture"),
            SkillDirectorySource::Global,
            None,
            MAIN.len() - 1
        )
        .is_err());
    std::fs::write(root.join("agents"), "not a directory").unwrap();
    assert!(pool
        .capture(
            temp.path(),
            Path::new("source-fixture"),
            SkillDirectorySource::Global,
            None,
            4096
        )
        .is_err());
}

#[cfg(unix)]
#[test]
fn file_and_directory_links_and_fifo_cannot_supply_policy_or_main() {
    use std::os::unix::fs::symlink;
    let temp = Fixture::new();
    let root = bundle(temp.path());
    let outside = tempfile::tempdir().unwrap();
    std::fs::write(
        outside.path().join("openai.yaml"),
        "policy:\n  allow_implicit_invocation: true",
    )
    .unwrap();
    let pool = SourcePool::default();
    std::fs::create_dir(root.join("agents")).unwrap();
    for target in [
        outside.path().join("openai.yaml"),
        outside.path().join("missing"),
    ] {
        symlink(target, root.join("agents/openai.yaml")).unwrap();
        assert!(pool
            .capture(
                temp.path(),
                Path::new("source-fixture"),
                SkillDirectorySource::Global,
                None,
                4096
            )
            .is_err());
        std::fs::remove_file(root.join("agents/openai.yaml")).unwrap();
    }
    std::fs::remove_dir(root.join("agents")).unwrap();
    for target in [outside.path().to_path_buf(), outside.path().join("missing")] {
        symlink(target, root.join("agents")).unwrap();
        assert!(pool
            .capture(
                temp.path(),
                Path::new("source-fixture"),
                SkillDirectorySource::Global,
                None,
                4096
            )
            .is_err());
        std::fs::remove_file(root.join("agents")).unwrap();
    }
    std::fs::remove_file(root.join("SKILL.md")).unwrap();
    let path =
        std::ffi::CString::new(root.join("SKILL.md").as_os_str().as_encoded_bytes()).unwrap();
    assert_eq!(unsafe { libc::mkfifo(path.as_ptr(), 0o600) }, 0);
    let started = std::time::Instant::now();
    assert!(pool
        .capture(
            temp.path(),
            Path::new("source-fixture"),
            SkillDirectorySource::Global,
            None,
            4096
        )
        .is_err());
    assert!(started.elapsed() < std::time::Duration::from_secs(2));
}

#[cfg(unix)]
#[test]
fn trusted_root_aliases_share_handles_without_merging_scope() {
    let temp = Fixture::new();
    let base = temp.path().join("skills");
    bundle(&base);
    let alias = temp.path().join("alias");
    std::os::unix::fs::symlink(&base, &alias).unwrap();
    let pool = SourcePool::default();
    let global = capture(&pool, &base);
    let project = pool
        .capture(
            &alias,
            Path::new("source-fixture"),
            SkillDirectorySource::Project,
            None,
            4096,
        )
        .unwrap();
    assert!(global.binding.same_root(&project.binding));
    assert_ne!(global.binding, project.binding);
    assert_eq!(pool.counts(), (1, 0, 1));
}

#[cfg(windows)]
#[test]
fn windows_real_opened_handle_identity_admits_and_reuses_regular_sources() {
    let temp = Fixture::new();
    bundle(temp.path());
    let pool = SourcePool::default();
    let first = capture(&pool, temp.path());
    let second = capture(&pool, temp.path());
    assert_eq!(first.binding, second.binding);
    assert!(first.binding.same_root(&second.binding));
    first
        .binding
        .validate(&pool, &first.policies, 4096)
        .unwrap();
}

#[tokio::test]
async fn default_mode_project_workspace_publications_share_budget_and_preserve_winners() {
    let temp = Fixture::new();
    let base = temp.path().join("data/skills");
    bundle(&base);
    let mode = temp.path().join("data/skills-focused");
    let mode_root = bundle(&mode);
    std::fs::write(mode_root.join("SKILL.md"), MAIN.replace("Body", "Mode")).unwrap();
    let workspace = temp.path().join("workspace");
    let workspace_root = bundle(&workspace.join(".bamboo/skills"));
    std::fs::write(
        workspace_root.join("SKILL.md"),
        MAIN.replace("Body", "Workspace"),
    )
    .unwrap();
    let project = temp.path().join("project");
    let project_root = bundle(&project.join("skills"));
    std::fs::write(
        project_root.join("SKILL.md"),
        MAIN.replace("Body", "Project"),
    )
    .unwrap();
    let store = SkillStore::new_with_source_limits(
        SkillStoreConfig {
            skills_dir: base,
            ..Default::default()
        },
        SourceLimits {
            roots: 4,
            temporary: 8,
            index: 4,
        },
    );
    store.reload().await.unwrap();
    assert_eq!(
        store
            .get_skill_for_mode("source-fixture", Some("focused"))
            .await
            .unwrap()
            .prompt,
        "Mode"
    );
    let workspace_store = store.skill_store_for_workspace(&workspace).await.unwrap();
    assert_eq!(
        workspace_store
            .get_skill("source-fixture")
            .await
            .unwrap()
            .prompt,
        "Workspace"
    );
    let project_store = store
        .skill_store_for_project_workspace(&bamboo_domain::ProjectId::new(), &project, None)
        .await
        .unwrap();
    assert_eq!(
        project_store
            .get_skill("source-fixture")
            .await
            .unwrap()
            .prompt,
        "Project"
    );
    assert!(Arc::ptr_eq(
        &store.source_pool(),
        &workspace_store.source_pool()
    ));
    assert!(Arc::ptr_eq(
        &store.source_pool(),
        &project_store.source_pool()
    ));
    assert_eq!(store.source_pool().counts(), (4, 0, 4));
    assert!(store.source_bindings().await.contains_key("source-fixture"));
    assert!(workspace_store
        .source_bindings()
        .await
        .contains_key("source-fixture"));
    assert!(project_store
        .source_bindings()
        .await
        .contains_key("source-fixture"));
}

#[tokio::test]
async fn actual_builtin_materialization_uses_the_same_source_boundary() {
    let temp = Fixture::new();
    let base = temp.path().join("data/skills");
    let store = store(&base);
    store.initialize().await.unwrap();
    let sources = store.source_bindings().await;
    let creator = sources
        .get("skill-creator")
        .expect("materialized builtin binding");
    assert!(temp
        .path()
        .join("data/skills-builtin-v1/skill-creator/SKILL.md")
        .is_file());
    assert_eq!(store.source_pool().counts().0, 1);
    let public = serde_json::to_value(store.skill_catalog_snapshot().await).unwrap();
    for entry in public["entries"].as_array().unwrap() {
        for private in ["signature", "bundle", "root", "anchor"] {
            assert!(entry.get(private).is_none());
        }
    }
    let captured = store
        .source_pool()
        .capture(
            &temp.path().join("data/skills-builtin-v1"),
            Path::new("skill-creator"),
            SkillDirectorySource::Builtin,
            None,
            crate::store::SkillSnapshotLimits::default().max_file_bytes,
        )
        .unwrap();
    creator
        .validate(
            &store.source_pool(),
            &captured.policies,
            crate::store::SkillSnapshotLimits::default().max_file_bytes,
        )
        .unwrap();
}

#[tokio::test]
async fn exhausted_root_capacity_does_not_evict_an_old_publication_consumer() {
    let temp = Fixture::new();
    let base = temp.path().join("skills");
    bundle(&base);
    let store = SkillStore::new_with_source_limits(
        SkillStoreConfig {
            skills_dir: base.clone(),
            ..Default::default()
        },
        SourceLimits {
            roots: 1,
            temporary: 8,
            index: 1,
        },
    );
    store.reload().await.unwrap();
    let old = store
        .source_bindings()
        .await
        .remove("source-fixture")
        .unwrap();
    std::fs::rename(&base, temp.path().join("old")).unwrap();
    bundle(&base);
    store.reload().await.unwrap();
    assert_eq!(row(&store).await.status, WorkflowStatus::Invalid);
    assert!(store.source_bindings().await.is_empty());
    assert_eq!(store.source_pool().counts().0, 1);
    drop(old);
    store.reload().await.unwrap();
    assert_eq!(row(&store).await.status, WorkflowStatus::Valid);
    assert!(store.source_bindings().await.contains_key("source-fixture"));
}

#[tokio::test]
async fn orchestration_optional_openai_read_error_keeps_the_old_workflow_boundary() {
    for (marker, limits) in [
        (false, SourceLimits::default()),
        (true, SourceLimits::default()),
        (
            false,
            SourceLimits {
                roots: 0,
                ..SourceLimits::default()
            },
        ),
        (
            false,
            SourceLimits {
                index: 0,
                ..SourceLimits::default()
            },
        ),
    ] {
        let temp = Fixture::new();
        let root = bundle(temp.path());
        std::fs::create_dir_all(root.join("agents/openai.yaml")).unwrap();
        if marker {
            std::fs::write(
                root.join("workflow.yaml"),
                r#"
workflow_schema: 1
id: source-fixture
revision: 1
input_schema:
  type: object
  additionalProperties: false
steps:
  - id: inspect
    type: tool
    tool: read_file
    args: {}
    capabilities: [read]
    output_schema:
      type: object
      additionalProperties: true
plan:
  type: step
  step: inspect
budgets:
  max_concurrency: 1
  max_agents: 0
  max_steps: 4
  max_retries: 1
  max_nesting_depth: 2
  wall_time_ms: 10000
"#,
            )
            .unwrap();
        }
        std::fs::write(
            root.join("agents/bamboo.yaml"),
            "composition: {}\ninvocation_policy:\n  explicit: false\n  automatic: false\n",
        )
        .unwrap();
        let store = SkillStore::new_with_source_limits(
            SkillStoreConfig {
                skills_dir: temp.path().into(),
                ..Default::default()
            },
            limits,
        );
        store.reload().await.unwrap();
        let workflow = store
            .workflow_catalog_snapshot()
            .await
            .entries
            .into_iter()
            .find(|entry| entry.id == "source-fixture")
            .unwrap();
        assert_eq!(workflow.kind, crate::WorkflowKind::Orchestration);
        assert_eq!(workflow.status, WorkflowStatus::Valid);
        assert_eq!(workflow.invocation_policy["explicit"], false);
        assert_eq!(workflow.invocation_policy["automatic"], false);
        assert!(!store.source_bindings().await.contains_key("source-fixture"));
        assert!(store.skill_catalog_snapshot().await.entries.is_empty());
        assert_eq!(store.source_pool().counts().0, 0);
    }
}

#[cfg(unix)]
#[tokio::test]
async fn linked_host_composition_keeps_orchestration_compatibility() {
    for deny in [false, true] {
        let temp = Fixture::new();
        let root = bundle(temp.path());
        std::fs::create_dir(root.join("agents")).unwrap();
        let outside = temp.path().parent().unwrap().join("host-composition.yaml");
        std::fs::write(
            &outside,
            if deny {
                "composition: {}\ninvocation_policy:\n  explicit: false\n  automatic: false\n"
            } else {
                "composition: {}"
            },
        )
        .unwrap();
        std::os::unix::fs::symlink(&outside, root.join("agents/bamboo.yaml")).unwrap();
        assert!(!root.join("workflow.yaml").exists());
        assert_eq!(
            crate::catalog::load_bundle_metadata(&root)
                .await
                .unwrap()
                .kind,
            crate::WorkflowKind::Orchestration
        );

        let store = store(temp.path());
        store.reload().await.unwrap();
        let workflow = store
            .workflow_catalog_snapshot()
            .await
            .entries
            .into_iter()
            .find(|entry| entry.id == "source-fixture")
            .expect("linked host composition must remain a Workflow");
        assert_eq!(workflow.kind, crate::WorkflowKind::Orchestration);
        assert_eq!(workflow.status, WorkflowStatus::Valid);
        assert_eq!(workflow.invocation_policy["explicit"], !deny);
        assert_eq!(workflow.invocation_policy["automatic"], false);
        assert_eq!(
            store.get_workflow_root("source-fixture").await.unwrap(),
            root
        );
        assert!(store.skill_catalog_snapshot().await.entries.is_empty());
        assert!(store.get_skill("source-fixture").await.is_err());
        assert!(!store.source_bindings().await.contains_key("source-fixture"));
    }
}

#[cfg(unix)]
#[tokio::test]
async fn ordinary_linked_host_metadata_cannot_grant_instruction_source() {
    for raw in [
        "invocation_policy:\n  explicit: true\n  automatic: true\n",
        "composition: [\n",
    ] {
        let temp = Fixture::new();
        let root = bundle(temp.path());
        std::fs::create_dir(root.join("agents")).unwrap();
        let outside = temp.path().parent().unwrap().join("host-policy.yaml");
        std::fs::write(&outside, raw).unwrap();
        std::os::unix::fs::symlink(&outside, root.join("agents/bamboo.yaml")).unwrap();
        let store = store(temp.path());
        store.reload().await.unwrap();
        let instruction = row(&store).await;
        assert_eq!(instruction.kind, crate::WorkflowKind::Instruction);
        assert_eq!(instruction.status, WorkflowStatus::Invalid);
        assert_eq!(instruction.invocation_policy["explicit"], false);
        assert_eq!(instruction.invocation_policy["automatic"], false);
        assert!(store.get_skill("source-fixture").await.is_err());
        assert!(store.workflow_catalog_snapshot().await.entries.is_empty());
        assert!(!store.source_bindings().await.contains_key("source-fixture"));
    }
}

#[tokio::test]
async fn main_bundle_and_root_replacements_during_capture_never_publish_new_authority() {
    for replacement in 0..3 {
        let temp = Fixture::new();
        let base = temp.path().join("skills");
        let root = bundle(&base);
        let store = store(&base);
        store.set_source_snapshot_hook(move || match replacement {
            0 => {
                std::fs::rename(root.join("SKILL.md"), root.join("previous")).unwrap();
                std::fs::write(root.join("SKILL.md"), MAIN).unwrap();
            }
            1 => {
                std::fs::rename(&root, base.join("previous")).unwrap();
                bundle(&base);
            }
            _ => {
                let previous = base.parent().unwrap().join("previous");
                std::fs::rename(&base, previous).unwrap();
                bundle(&base);
            }
        });
        store.reload().await.unwrap();
        assert_eq!(row(&store).await.status, WorkflowStatus::Invalid);
        assert!(!store.source_bindings().await.contains_key("source-fixture"));
    }
}

#[test]
fn captured_policy_rejects_a_different_aux_snapshot_without_reparsing_permissions() {
    let temp = Fixture::new();
    let root = bundle(temp.path());
    std::fs::create_dir(root.join("agents")).unwrap();
    let pool = SourcePool::default();
    for original in [true, false] {
        std::fs::write(
            root.join("agents/openai.yaml"),
            format!("policy:\n  allow_implicit_invocation: {original}\n"),
        )
        .unwrap();
        let captured = capture(&pool, temp.path());
        assert_eq!(captured.metadata.invocation_policy["automatic"], original);
        captured
            .binding
            .validate(&pool, &captured.policies, 4096)
            .unwrap();
        let mut mismatched = captured.policies.clone();
        mismatched.insert(
            "agents/openai.yaml".into(),
            Arc::new(format!("policy:\n  allow_implicit_invocation: {}\n", !original).into_bytes()),
        );
        assert!(captured.binding.validate(&pool, &mismatched, 4096).is_err());
        assert!(captured
            .binding
            .validate(&pool, &Default::default(), 4096)
            .is_err());
    }
}
