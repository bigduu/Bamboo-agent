use super::*;
use std::collections::BTreeSet;

fn entry(id: &str, description: &str, short: Option<&str>) -> SkillCatalogMetadata {
    SkillCatalogMetadata {
        package: id.into(),
        name: id.into(),
        description: description.into(),
        short_description: short.map(str::to_owned),
        main_resource: format!("/skills/{id}/SKILL.md"),
        source: crate::WorkflowSource::User,
        revision: 1,
        identity: id.into(),
        root: "/skills".into(),
        explicit: true,
        automatic: true,
    }
}

#[test]
fn authority_ceiling_and_invocation_are_independent() {
    let mut manual = entry("manual", "Manual", None);
    manual.automatic = false;
    let entries = vec![entry("auto", "Automatic", None), manual];
    let mut access = SkillCatalogEligibility {
        ceiling: None,
        explicit: BTreeSet::new(),
        disabled: BTreeSet::new(),
        deny_all: false,
    };
    assert_eq!(eligible_metadata(&entries, &access).unwrap().len(), 1);
    access.ceiling = Some(BTreeSet::new());
    access.explicit.insert("manual".into());
    assert!(eligible_metadata(&entries, &access).unwrap().is_empty());
    access.ceiling = Some(BTreeSet::from(["manual".into()]));
    assert_eq!(
        eligible_metadata(&entries, &access).unwrap()[0].package,
        "manual"
    );
    access.disabled.insert("manual".into());
    assert!(eligible_metadata(&entries, &access).unwrap().is_empty());
    access.disabled.clear();
    access.deny_all = true;
    assert!(eligible_metadata(&entries, &access).unwrap().is_empty());
    access.ceiling = Some(BTreeSet::from(["../escape".into()]));
    assert!(eligible_metadata(&entries, &access).is_err());
}

fn access(id: &str) -> SkillCatalogEligibility {
    SkillCatalogEligibility {
        ceiling: Some(BTreeSet::from([id.into()])),
        explicit: BTreeSet::new(),
        disabled: BTreeSet::new(),
        deny_all: false,
    }
}
fn write_skill(root: &std::path::Path, id: &str) {
    std::fs::create_dir_all(root.join(id)).unwrap();
    std::fs::write(
        root.join(id).join("SKILL.md"),
        format!("---\nname: {id}\ndescription: public summary\n---\nPRIVATE INSTRUCTIONS"),
    )
    .unwrap();
}

#[tokio::test]
async fn catalog_projection_rejects_invalid_lkg_failed_refresh_and_changed_raw_inputs() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("skills");
    write_skill(&root, "catalog-source");
    let store = crate::SkillStore::new(crate::SkillStoreConfig {
        skills_dir: root.clone(),
        ..Default::default()
    });
    store.initialize().await.unwrap();
    let first = store
        .progressive_catalog_for_mode(None, &access("catalog-source"))
        .await
        .unwrap();
    assert_eq!(first.entries.len(), 1);
    assert!(!serde_json::to_string(&first)
        .unwrap()
        .contains("PRIVATE INSTRUCTIONS"));
    let file = root.join("catalog-source/SKILL.md");
    let content = std::fs::read_to_string(&file).unwrap();
    std::fs::write(&file, format!("{content}\n ")).unwrap();
    let raw = store
        .progressive_catalog_for_mode(None, &access("catalog-source"))
        .await
        .unwrap();
    assert_ne!(first.identity, raw.identity);
    std::fs::create_dir_all(root.join("catalog-source/agents")).unwrap();
    std::fs::write(
        root.join("catalog-source/agents/openai.yaml"),
        "policy:\n  allow_implicit_invocation: false\n",
    )
    .unwrap();
    assert!(store
        .progressive_catalog_for_mode(None, &access("catalog-source"))
        .await
        .unwrap()
        .entries
        .is_empty());
    std::fs::write(&file, "---\nname: catalog-source\ndescription: summary\nallowed-tools: [broken\n---\nPRIVATE BROKEN").unwrap();
    assert!(store
        .progressive_catalog_for_mode(None, &access("catalog-source"))
        .await
        .unwrap()
        .entries
        .is_empty());
    assert!(
        store.get_skill("catalog-source").await.is_ok(),
        "management retains LKG"
    );
    std::fs::write(&file, content).unwrap();
    std::fs::write(
        root.join("catalog-source/too-large.txt"),
        vec![b'x'; crate::store::SkillSnapshotLimits::default().max_file_bytes + 1],
    )
    .unwrap();
    assert!(store
        .progressive_catalog_for_mode(None, &access("catalog-source"))
        .await
        .is_err());
    assert!(store.get_skill("catalog-source").await.is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn catalog_projection_holds_publication_read_lifetime_but_returns_no_retained_handles() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("skills");
    write_skill(&root, "catalog-lock");
    let store = std::sync::Arc::new(crate::SkillStore::new(crate::SkillStoreConfig {
        skills_dir: root.clone(),
        ..Default::default()
    }));
    store.initialize().await.unwrap();
    let pool = store.source_pool();
    let baseline = pool.counts().0;
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    store.set_projection_hook(move || {
        entered_tx.send(()).unwrap();
        release_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
    });
    let reader_store = store.clone();
    let reader = tokio::spawn(async move {
        reader_store
            .progressive_catalog_for_mode(None, &access("catalog-lock"))
            .await
            .unwrap()
    });
    entered_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    std::fs::rename(&root, directory.path().join("old-root")).unwrap();
    write_skill(&root, "catalog-lock");
    let (prepared_tx, prepared_rx) = std::sync::mpsc::channel();
    store.set_source_snapshot_hook(move || {
        prepared_tx.send(()).unwrap();
    });
    let writer_store = store.clone();
    let writer = tokio::spawn(async move { writer_store.reload().await.unwrap() });
    prepared_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    tokio::task::yield_now().await;
    assert!(
        !writer.is_finished(),
        "publication cannot replace the borrowed generation"
    );
    release_tx.send(()).unwrap();
    let owned = reader.await.unwrap();
    writer.await.unwrap();
    let current = store
        .progressive_catalog_for_mode(None, &access("catalog-lock"))
        .await
        .unwrap();
    assert_ne!(
        owned.identity, current.identity,
        "byte-identical physical replacement invalidates identity"
    );
    assert_eq!(
        pool.counts().0,
        baseline,
        "old owned metadata retains no root handles"
    );
}

#[tokio::test]
async fn catalog_projection_tracks_mode_and_current_workspace_winner() {
    let directory = tempfile::tempdir().unwrap();
    let global = directory.path().join("skills");
    let workspace = directory.path().join("workspace");
    let overlay = workspace.join(".bamboo/skills");
    write_skill(&global, "catalog-scope");
    write_skill(&overlay, "catalog-scope");
    let mode = workspace.join(".bamboo/skills-review");
    write_skill(&mode, "catalog-scope");
    let store = crate::SkillStore::new(crate::SkillStoreConfig {
        skills_dir: global,
        project_dir: Some(workspace),
        ..Default::default()
    });
    store.initialize().await.unwrap();
    let normal = store
        .progressive_catalog_for_mode(None, &access("catalog-scope"))
        .await
        .unwrap();
    assert_eq!(normal.entries[0].source, crate::WorkflowSource::Workspace);
    assert!(std::path::Path::new(&normal.entries[0].main_resource)
        .ends_with(overlay.join("catalog-scope/SKILL.md")));
    let review = store
        .progressive_catalog_for_mode(Some("review"), &access("catalog-scope"))
        .await
        .unwrap();
    assert_ne!(normal.identity, review.identity);
    assert!(std::path::Path::new(&review.entries[0].main_resource)
        .ends_with(mode.join("catalog-scope/SKILL.md")));
    std::fs::write(
        overlay.join("catalog-scope/SKILL.md"),
        "---\nname: catalog-scope\nallowed-tools: [\n---\nBAD",
    )
    .unwrap();
    assert!(
        store
            .progressive_catalog_for_mode(None, &access("catalog-scope"))
            .await
            .unwrap()
            .entries
            .is_empty(),
        "invalid winning overlay cannot fall back to valid global or LKG"
    );
}
