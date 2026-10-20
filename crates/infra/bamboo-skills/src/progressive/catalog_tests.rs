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

async fn input_store_fixture() -> (
    tempfile::TempDir,
    std::sync::Arc<crate::SkillStore>,
    crate::WorkflowSelection,
    SkillCatalogEligibility,
) {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("skills");
    write_skill(&root, "current-input");
    std::fs::write(root.join("current-input/aux.txt"), "PRIVATE AUXILIARY").unwrap();
    let store = std::sync::Arc::new(crate::SkillStore::new(crate::SkillStoreConfig {
        skills_dir: root,
        ..Default::default()
    }));
    store.initialize().await.unwrap();
    let catalog = store.skill_catalog_snapshot().await;
    let entry = catalog
        .entries
        .iter()
        .find(|entry| entry.id == "current-input")
        .unwrap();
    let selection = crate::WorkflowSelection {
        id: entry.id.clone(),
        source: entry.source,
        revision: entry.revision,
        args: serde_json::json!({}),
    };
    let mut access = access("current-input");
    access.explicit.insert("current-input".into());
    (directory, store, selection, access)
}

#[tokio::test]
async fn current_input_borrows_correlated_publication_and_releases_all_source_charges() {
    let (_directory, store, selection, access) = input_store_fixture().await;
    let pool = store.source_pool();
    let handles = pool.counts();
    let budget = store.selected_budget();
    let before = store.skill_catalog_snapshot().await;
    for _ in 0..3 {
        let prepared = store.prepare_current_input_store(None).await.unwrap();
        let expected_revision = before.revision;
        let result = prepared
            .with_current_inputs(
                &access,
                std::slice::from_ref(&selection),
                move |publication| {
                    Box::pin(async move {
                        publication.validate_current()?;
                        let input = &publication.inputs()[0];
                        assert_eq!(input.catalog_revision, expected_revision);
                        assert_eq!(input.mode, None);
                        assert_eq!(input.definition.id, input.selection.id);
                        assert_eq!(input.catalog_entry.revision, input.revision);
                        assert_eq!(input.catalog_entry.source, input.selection.source);
                        assert!(input.main_resource.ends_with("current-input/SKILL.md"));
                        Ok(input.definition.prompt.clone())
                    })
                },
            )
            .await
            .unwrap();
        assert!(result.contains("PRIVATE INSTRUCTIONS"));
        assert!(!result.contains("PRIVATE AUXILIARY"));
        assert_eq!(pool.counts(), handles);
        assert_eq!(budget.usage(), (0, 0, 0));
    }
    assert_eq!(
        store.skill_catalog_snapshot().await.revision,
        before.revision,
        "no-op refresh retains generation"
    );
}

#[tokio::test]
async fn current_input_final_source_check_denies_raw_physical_auxiliary_and_policy_changes() {
    for change in [
        "raw",
        "physical",
        "auxiliary",
        "auxiliary-physical",
        "policy",
    ] {
        let (directory, store, selection, access) = input_store_fixture().await;
        let prepared = store.prepare_current_input_store(None).await.unwrap();
        let path = directory.path().join("skills/current-input");
        let result = prepared
            .with_current_inputs(
                &access,
                std::slice::from_ref(&selection),
                move |publication| {
                    Box::pin(async move {
                        publication.validate_current()?;
                        match change {
                            "raw" => {
                                let main = path.join("SKILL.md");
                                let text = std::fs::read_to_string(&main).unwrap();
                                std::fs::write(main, format!("{text}\n ")).unwrap();
                            }
                            "physical" => {
                                let main = path.join("SKILL.md");
                                let text = std::fs::read_to_string(&main).unwrap();
                                std::fs::rename(&main, path.join("previous.txt")).unwrap();
                                std::fs::write(main, text).unwrap();
                            }
                            "auxiliary" => {
                                std::fs::write(path.join("aux.txt"), "OTHER RAW AUXILIARY").unwrap()
                            }
                            "auxiliary-physical" => {
                                let aux = path.join("aux.txt");
                                let bytes = std::fs::read(&aux).unwrap();
                                std::fs::rename(&aux, path.join("old-aux.txt")).unwrap();
                                std::fs::write(aux, bytes).unwrap();
                            }
                            "policy" => {
                                std::fs::create_dir_all(path.join("agents")).unwrap();
                                std::fs::write(
                                    path.join("agents/openai.yaml"),
                                    "policy:\n  allow_implicit_invocation: false\n",
                                )
                                .unwrap();
                            }
                            _ => unreachable!(),
                        }
                        Ok("must not escape")
                    })
                },
            )
            .await;
        assert!(
            result.is_err(),
            "{change}: final raw Source revalidation is mandatory"
        );
        assert_eq!(store.selected_budget().usage(), (0, 0, 0));
    }
}

#[tokio::test]
async fn current_input_all_selection_validation_denies_lkg_and_rejected_generation() {
    let (directory, store, selection, access) = input_store_fixture().await;
    let path = directory.path().join("skills/current-input/SKILL.md");
    std::fs::write(
        &path,
        "---\nname: current-input\nallowed-tools: [broken\n---\nBROKEN",
    )
    .unwrap();
    store.reload().await.unwrap();
    assert!(
        store.get_skill("current-input").await.is_ok(),
        "management retains LKG"
    );
    let prepared = store.prepare_current_input_store(None).await.unwrap();
    let called = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let callback_called = called.clone();
    assert!(prepared
        .with_current_inputs(
            &access,
            std::slice::from_ref(&selection),
            move |_| Box::pin(async move {
                callback_called.store(true, std::sync::atomic::Ordering::SeqCst);
                Ok(())
            })
        )
        .await
        .is_err());
    assert!(!called.load(std::sync::atomic::Ordering::SeqCst));
    write_skill(&directory.path().join("skills"), "current-input");
    std::fs::write(
        directory.path().join("skills/current-input/oversized.txt"),
        vec![b'x'; 8 * 1024 * 1024 + 1],
    )
    .unwrap();
    assert!(store.prepare_current_input_store(None).await.is_err());
    assert_eq!(store.selected_budget().usage(), (0, 0, 0));
}
