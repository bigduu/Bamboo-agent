use super::{
    read::{SelectedBudget, SelectedLimits},
    SkillCatalogEligibility,
};
use crate::{SkillManager, SkillStore, SkillStoreConfig};
use std::{collections::BTreeSet, path::PathBuf, sync::Arc};

const RAW: &str = "---\r\nname: chosen\r\ndescription: chosen raw\r\n---\r\nBody 界🦀\r\n\r\n";
struct Fixture {
    _directory: tempfile::TempDir,
    base: PathBuf,
    bundle: PathBuf,
    store: SkillStore,
}
impl Fixture {
    async fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let base = directory.path().join("skills");
        let bundle = base.join("chosen");
        std::fs::create_dir_all(bundle.join("references")).unwrap();
        std::fs::write(bundle.join("SKILL.md"), RAW).unwrap();
        std::fs::write(bundle.join("references/plain.txt"), "Exact\r\naux 界\0").unwrap();
        std::fs::write(bundle.join("references/empty.txt"), "").unwrap();
        let store = SkillStore::new(SkillStoreConfig {
            skills_dir: base.clone(),
            ..Default::default()
        });
        store.initialize().await.unwrap();
        Self {
            _directory: directory,
            base,
            bundle,
            store,
        }
    }
    fn access() -> SkillCatalogEligibility {
        SkillCatalogEligibility {
            ceiling: Some(BTreeSet::from(["chosen".into()])),
            explicit: BTreeSet::new(),
            disabled: BTreeSet::new(),
            deny_all: false,
        }
    }
    async fn catalog(&self) -> super::SkillCatalogSnapshot {
        self.store
            .progressive_catalog_for_mode(None, &Self::access())
            .await
            .unwrap()
    }
    async fn read(&self, path: &str) -> crate::SkillResult<Arc<super::SelectedSkillSnapshot>> {
        let catalog = self.catalog().await;
        self.store
            .selected_source_for_mode(None, &Self::access(), &catalog, "chosen", path)
            .await
    }
}

#[test]
fn ledger_precharge_bounds_allocation_inflight_and_last_real_owner() {
    let ledger = SelectedBudget::new(SelectedLimits {
        bytes: 9,
        owners: 1,
        inflight: 1,
    });
    assert!(ledger.buffer(10, true).is_err());
    assert_eq!(ledger.usage(), (0, 0, 0));
    let pending = ledger.operation().unwrap();
    assert!(ledger.operation().is_err());
    let buffer = ledger.buffer(9, true).unwrap();
    assert_eq!(ledger.usage(), (buffer.contents.capacity(), 1, 1));
    assert!(ledger.buffer(1, false).is_err());
    drop(pending);
    assert_eq!(ledger.usage().2, 0);
    drop(buffer);
    assert_eq!(ledger.usage(), (0, 0, 0));
}

#[tokio::test]
async fn raw_main_aux_empty_and_active_arc_keep_exact_capacity_charge() {
    let fixture = Fixture::new().await;
    let ledger = fixture.store.selected_budget();
    let main = fixture.read("SKILL.md").await.unwrap();
    assert_eq!(main.contents(), RAW);
    assert_eq!(ledger.usage(), (main.capacity(), 1, 0));
    let active = main.clone();
    drop(main);
    assert_eq!(ledger.usage().1, 1);
    let aux = fixture.read("references/plain.txt").await.unwrap();
    assert_eq!(aux.contents(), "Exact\r\naux 界\0");
    let empty = fixture.read("references/empty.txt").await.unwrap();
    assert_eq!(empty.contents(), "");
    assert_eq!(
        ledger.usage().0,
        active.capacity() + aux.capacity() + empty.capacity()
    );
    drop((active, aux, empty));
    assert_eq!(ledger.usage(), (0, 0, 0));
}

#[tokio::test]
async fn full_bad_utf8_suffix_and_error_paths_rollback_owned_and_inflight_charge() {
    let fixture = Fixture::new().await;
    let mut bytes = vec![b'x'; 30_000];
    bytes.push(0xff);
    std::fs::write(fixture.bundle.join("references/bad.txt"), bytes).unwrap();
    let ledger = fixture.store.selected_budget();
    assert!(fixture.read("references/bad.txt").await.is_err());
    for path in [
        "",
        "../SKILL.md",
        "/SKILL.md",
        "./SKILL.md",
        "references/missing.txt",
        "references",
        "SKILL.md/child",
    ] {
        assert!(fixture.read(path).await.is_err(), "{path}");
        assert_eq!(ledger.usage(), (0, 0, 0));
    }
}

#[tokio::test]
async fn shared_manager_scopes_and_teardown_do_not_make_ledger_owner_cycle() {
    let fixture = Fixture::new().await;
    let manager = SkillManager::with_config(SkillStoreConfig {
        skills_dir: fixture.base.clone(),
        ..Default::default()
    });
    let ledger = manager.selected_budget();
    let weak = ledger.weak_state();
    let workspace = fixture._directory.path().join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let scoped = manager
        .clone()
        .store_for_workspace(Some(&workspace))
        .await
        .unwrap();
    assert!(Arc::ptr_eq(&ledger, &scoped.selected_budget()));
    let project = bamboo_domain::ProjectId::parse("chosen-project").unwrap();
    let project_home = fixture._directory.path().join("project");
    std::fs::create_dir_all(&project_home).unwrap();
    let project_store = manager
        .store_for_project_workspace(&project, &project_home, Some(&workspace))
        .await
        .unwrap();
    assert!(Arc::ptr_eq(&ledger, &project_store.selected_budget()));
    let mode_budget = scoped.selected_mode_budget("reader-mode").await;
    assert!(Arc::ptr_eq(&ledger, &mode_budget));
    drop((project_store, mode_budget));

    let access = Fixture::access();
    let catalog = scoped
        .progressive_catalog_for_mode(None, &access)
        .await
        .unwrap();
    let bytes = scoped
        .selected_source_for_mode(None, &access, &catalog, "chosen", "SKILL.md")
        .await
        .unwrap();
    drop(scoped);
    drop(manager);
    drop(ledger);
    assert!(
        weak.upgrade().is_some(),
        "returned bytes really retain their charge"
    );
    drop(bytes);
    assert!(
        weak.upgrade().is_none(),
        "charge must not retain manager/cache/owner"
    );
}

#[tokio::test]
async fn warm_probe_needs_bounded_scratch_not_a_second_complete_chosen_buffer() {
    let fixture = Fixture::new().await;
    let large = format!("{RAW}{}", "x".repeat(128 * 1024));
    std::fs::write(fixture.bundle.join("SKILL.md"), &large).unwrap();
    let store = SkillStore::new_with_selected_limits(
        SkillStoreConfig {
            skills_dir: fixture.base.clone(),
            ..Default::default()
        },
        SelectedLimits {
            bytes: large.len() + 1 + 4096,
            owners: 1,
            inflight: 1,
        },
    );
    let access = Fixture::access();
    let catalog = store
        .progressive_catalog_for_mode(None, &access)
        .await
        .unwrap();
    let snapshot = store
        .selected_source_for_mode(None, &access, &catalog, "chosen", "SKILL.md")
        .await
        .unwrap();
    assert!(store
        .selected_source_for_mode(None, &access, &catalog, "chosen", "SKILL.md")
        .await
        .is_err());
    assert_eq!(
        store
            .probe_selected_source_for_mode(None, &access, &catalog, "chosen", "SKILL.md")
            .await
            .unwrap(),
        snapshot.identity
    );
    assert_eq!(store.selected_budget().usage(), (snapshot.capacity(), 1, 0));
}

#[tokio::test]
async fn published_aux_a_descriptor_b_and_same_bytes_new_leaf_are_observed() {
    let fixture = Fixture::new().await;
    let before = fixture.read("references/plain.txt").await.unwrap();
    let catalog = fixture.catalog().await;
    let changed = fixture.bundle.join("references/plain.txt");
    fixture
        .store
        .set_projection_hook(move || std::fs::write(changed, "changed after projection").unwrap());
    assert!(fixture
        .store
        .selected_source_for_mode(
            None,
            &Fixture::access(),
            &catalog,
            "chosen",
            "references/plain.txt"
        )
        .await
        .is_err());
    std::fs::write(
        fixture.bundle.join("references/plain.txt"),
        before.contents(),
    )
    .unwrap();
    std::fs::rename(
        fixture.bundle.join("references/plain.txt"),
        fixture.bundle.join("previous.txt"),
    )
    .unwrap();
    std::fs::write(
        fixture.bundle.join("references/plain.txt"),
        before.contents(),
    )
    .unwrap();
    let new = fixture.read("references/plain.txt").await.unwrap();
    assert_eq!(new.contents(), before.contents());
    assert_ne!(new.identity, before.identity);
}

#[tokio::test]
async fn root_main_policy_replacement_invalid_lkg_and_builtin_never_mix_authority() {
    let fixture = Fixture::new().await;
    let before = fixture.catalog().await;
    let bindings = fixture.store.source_bindings().await;
    let ledger = fixture.store.selected_budget();
    let pool = fixture.store.source_pool();
    assert_eq!(pool.counts().1, 0);
    std::fs::rename(
        fixture.bundle.join("SKILL.md"),
        fixture.bundle.join("old-main"),
    )
    .unwrap();
    std::fs::write(fixture.bundle.join("SKILL.md"), RAW).unwrap();
    assert!(fixture
        .store
        .selected_source_for_mode(None, &Fixture::access(), &before, "chosen", "SKILL.md")
        .await
        .is_err());
    let new = fixture.catalog().await;
    std::fs::create_dir_all(fixture.bundle.join("agents")).unwrap();
    std::fs::write(
        fixture.bundle.join("agents/bamboo.yaml"),
        "invocation_policy: malformed",
    )
    .unwrap();
    assert!(fixture
        .store
        .selected_source_for_mode(None, &Fixture::access(), &new, "chosen", "SKILL.md")
        .await
        .is_err());
    assert!(
        fixture.store.get_skill("chosen").await.is_ok(),
        "management LKG remains"
    );
    std::fs::remove_file(fixture.bundle.join("agents/bamboo.yaml")).unwrap();
    let restored = fixture.catalog().await;
    std::fs::rename(&fixture.base, fixture.base.with_extension("old")).unwrap();
    std::fs::create_dir_all(&fixture.bundle).unwrap();
    std::fs::write(fixture.bundle.join("SKILL.md"), RAW).unwrap();
    assert!(fixture
        .store
        .selected_source_for_mode(None, &Fixture::access(), &restored, "chosen", "SKILL.md")
        .await
        .is_err());
    drop(bindings);
    assert_eq!(pool.counts().1, 0, "no selected leaf/temp retained");
    assert_eq!(ledger.usage(), (0, 0, 0));
    let unrestricted = SkillCatalogEligibility {
        ceiling: None,
        ..Fixture::access()
    };
    let catalog = fixture
        .store
        .progressive_catalog_for_mode(None, &unrestricted)
        .await
        .unwrap();
    let builtin = catalog
        .entries
        .iter()
        .find(|entry| entry.source == crate::WorkflowSource::Builtin)
        .unwrap();
    let snapshot = fixture
        .store
        .selected_source_for_mode(None, &unrestricted, &catalog, &builtin.package, "SKILL.md")
        .await
        .unwrap();
    assert!(snapshot.contents().contains("---"));
    assert_eq!(snapshot.package, builtin.package);
    assert!(builtin.main_resource.contains("skills-builtin-v1"));
}

#[tokio::test]
async fn pending_materialization_cancellation_and_tiny_owner_capacity_release() {
    use std::future::Future;
    let fixture = Fixture::new().await;
    let catalog = fixture.catalog().await;
    let access = Fixture::access();
    let guard = fixture.store.hold_reload_for_selected_test().await;
    let mut pending = Box::pin(
        fixture
            .store
            .selected_source_for_mode(None, &access, &catalog, "chosen", "SKILL.md"),
    );
    let state = std::future::poll_fn(|cx| std::task::Poll::Ready(pending.as_mut().poll(cx))).await;
    assert!(
        state.is_pending(),
        "actual existing reload mutex holds the future"
    );
    assert_eq!(fixture.store.selected_budget().usage(), (0, 0, 1));
    drop(pending);
    assert_eq!(fixture.store.selected_budget().usage(), (0, 0, 0));
    drop(guard);
    let deny = SkillStore::new_with_selected_limits(
        SkillStoreConfig {
            skills_dir: fixture.base.clone(),
            ..Default::default()
        },
        SelectedLimits {
            bytes: 4096,
            owners: 0,
            inflight: 1,
        },
    );
    let catalog = deny
        .progressive_catalog_for_mode(None, &access)
        .await
        .unwrap();
    assert!(deny
        .selected_source_for_mode(None, &access, &catalog, "chosen", "SKILL.md")
        .await
        .is_err());
    assert_eq!(deny.selected_budget().usage(), (0, 0, 0));
}

#[tokio::test]
async fn eight_mib_aux_is_complete_and_oversize_rejection_preserves_existing_owner() {
    let fixture = Fixture::new().await;
    let path = fixture.bundle.join("references/large.txt");
    let exact = "x".repeat(8 * 1024 * 1024);
    std::fs::write(&path, &exact).unwrap();
    let snapshot = fixture.read("references/large.txt").await.unwrap();
    assert_eq!(snapshot.contents(), exact);
    let ledger = fixture.store.selected_budget();
    assert_eq!(ledger.usage(), (snapshot.capacity(), 1, 0));
    std::fs::write(&path, format!("{exact}x")).unwrap();
    let catalog = fixture
        .store
        .progressive_catalog_for_mode(None, &Fixture::access())
        .await;
    if let Ok(catalog) = catalog {
        assert!(fixture
            .store
            .selected_source_for_mode(
                None,
                &Fixture::access(),
                &catalog,
                "chosen",
                "references/large.txt"
            )
            .await
            .is_err());
    }
    assert_eq!(snapshot.contents().len(), 8 * 1024 * 1024);
    assert_eq!(ledger.usage(), (snapshot.capacity(), 1, 0));
    drop(snapshot);
    assert_eq!(ledger.usage(), (0, 0, 0));
}

#[tokio::test]
async fn policy_a_aux_b_capture_never_publishes_reader_authority_from_management_lkg() {
    let fixture = Fixture::new().await;
    std::fs::create_dir_all(fixture.bundle.join("agents")).unwrap();
    let policy = fixture.bundle.join("agents/bamboo.yaml");
    std::fs::write(
        &policy,
        "invocation_policy:\n  explicit: true\n  automatic: true\n",
    )
    .unwrap();
    let catalog = fixture.catalog().await;
    fixture.store.set_source_snapshot_hook(move || {
        std::fs::write(
            policy,
            "invocation_policy:\n  explicit: false\n  automatic: false\n",
        )
        .unwrap()
    });
    assert!(fixture
        .store
        .selected_source_for_mode(None, &Fixture::access(), &catalog, "chosen", "SKILL.md")
        .await
        .is_err());
    assert_eq!(
        fixture.store.get_skill("chosen").await.unwrap().prompt,
        "Body 界🦀",
        "management LKG keeps the old parsed body"
    );
    assert_eq!(fixture.store.selected_budget().usage(), (0, 0, 0));
}

#[cfg(unix)]
#[tokio::test]
async fn unix_file_directory_link_fifo_and_read_error_do_not_become_empty_or_absent() {
    use std::os::unix::fs::symlink;
    let fixture = Fixture::new().await;
    let outside = fixture._directory.path().join("outside");
    std::fs::create_dir(&outside).unwrap();
    std::fs::write(outside.join("secret"), "OUTSIDE").unwrap();
    symlink(
        outside.join("secret"),
        fixture.bundle.join("references/link"),
    )
    .unwrap();
    symlink(&outside, fixture.bundle.join("linked-directory")).unwrap();
    let fifo = fixture.bundle.join("references/fifo");
    let status = std::process::Command::new("mkfifo")
        .arg(&fifo)
        .status()
        .unwrap();
    assert!(status.success());
    for path in [
        "references/link",
        "linked-directory/secret",
        "references/fifo",
    ] {
        assert!(fixture.read(path).await.is_err());
    }
    let catalog = fixture.catalog().await;
    std::fs::create_dir_all(fixture.bundle.join("agents/openai.yaml")).unwrap();
    assert!(fixture
        .store
        .selected_source_for_mode(None, &Fixture::access(), &catalog, "chosen", "SKILL.md")
        .await
        .is_err());
    assert_eq!(fixture.store.selected_budget().usage(), (0, 0, 0));
}

#[cfg(windows)]
#[tokio::test]
async fn actual_windows_junction_is_created_and_rejected_without_reading_target() {
    let fixture = Fixture::new().await;
    let outside = fixture._directory.path().join("outside");
    std::fs::create_dir(&outside).unwrap();
    std::fs::write(outside.join("plain.txt"), "Exact\r\naux 界\0").unwrap();
    let link = fixture.bundle.join("references");
    let old = fixture.bundle.join("previous-references");
    let catalog = fixture.catalog().await;
    fixture.store.set_projection_hook(move || {
        std::fs::rename(&link, old).unwrap();
        let status = std::process::Command::new("cmd")
            .arg("/C")
            .arg("mklink")
            .arg("/J")
            .arg(&link)
            .arg(&outside)
            .status()
            .unwrap();
        assert!(
            status.success(),
            "actual Windows fixture requires junction creation"
        );
        assert!(
            std::fs::metadata(link.join("plain.txt")).is_ok(),
            "real junction points at matching target bytes"
        );
    });
    assert!(fixture
        .store
        .selected_source_for_mode(
            None,
            &Fixture::access(),
            &catalog,
            "chosen",
            "references/plain.txt"
        )
        .await
        .is_err());
    assert_eq!(fixture.store.selected_budget().usage(), (0, 0, 0));
    assert_eq!(fixture.store.source_pool().counts().1, 0);
}
