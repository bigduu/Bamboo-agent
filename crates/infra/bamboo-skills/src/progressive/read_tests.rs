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

// Bamboo-authored cache and upstream UTF-8 algorithm boundary goldens.
fn owned_snapshot(ledger: &SelectedBudget, contents: &str) -> Arc<super::SelectedSkillSnapshot> {
    let mut buffer = ledger.buffer(contents.len(), true).unwrap();
    buffer.contents.copy_from_slice(contents.as_bytes());
    Arc::new(
        buffer
            .snapshot("chosen".into(), "SKILL.md".into(), "raw-physical".into())
            .unwrap(),
    )
}
fn test_page(
    snapshot: &super::SelectedSkillSnapshot,
    start: usize,
    budget: usize,
) -> crate::SkillResult<super::SelectedSkillPage> {
    snapshot.page_response(
        start,
        budget,
        |text, offset| Ok(serde_json::to_vec(&(text, offset)).unwrap().len()),
        |text, offset, writer| {
            serde_json::to_writer(writer, &(text, offset))
                .map_err(|e| crate::SkillError::Validation(e.to_string()))
        },
    )
}

#[test]
fn chosen_cache_active_borrow_eviction_last_drop_and_teardown_are_charged() {
    let ledger = SelectedBudget::new(SelectedLimits {
        bytes: 2048,
        owners: 2,
        inflight: 1,
    });
    let weak = ledger.weak_state();
    let cache = super::SelectedSkillReadCache::default();
    let first = cache.admit(owned_snapshot(&ledger, "first 界"), "caller-A".into());
    let cursor = first.cursor(0);
    let active = cache.lookup(&cursor).unwrap().0;
    let second = cache.admit(owned_snapshot(&ledger, "second 🦀"), "caller-B".into());
    assert!(!cache.contains(&first));
    assert!(cache.lookup(&cursor).is_err());
    assert_eq!(
        ledger.usage().1,
        2,
        "eviction does not release an active owner"
    );
    assert!(ledger.buffer(0, true).is_err());
    drop(first);
    assert_eq!(
        ledger.usage().1,
        2,
        "the real last borrower still holds the charge"
    );
    drop(active);
    assert_eq!(ledger.usage().1, 1);
    cache.clear();
    assert_eq!(
        ledger.usage().1,
        1,
        "returned second admission is still a real owner"
    );
    drop(second);
    assert_eq!(ledger.usage(), (0, 0, 0));
    let entry = cache.admit(owned_snapshot(&ledger, "teardown"), "caller".into());
    drop(entry);
    drop(ledger);
    assert!(
        weak.upgrade().is_some(),
        "cache retains only data and its ledger charge"
    );
    drop(cache);
    assert!(
        weak.upgrade().is_none(),
        "no cache-to-charge-to-cache ownership cycle"
    );
}

#[test]
fn chosen_cursors_never_revive_on_identical_data_or_another_cache() {
    let ledger = SelectedBudget::default();
    let snapshot = owned_snapshot(&ledger, "a界🦀b");
    let cache = super::SelectedSkillReadCache::default();
    let old = cache.admit(snapshot.clone(), "same-current-source".into());
    let old_cursor = old.cursor(1);
    assert_eq!(cache.lookup(&old_cursor).unwrap().1, 1);
    for offset in [2, 3, 5, 6, 7, 100] {
        assert!(
            cache.lookup(&old.cursor(offset)).is_err(),
            "reject nonboundary/out-of-range {offset}"
        );
    }
    for malformed in [
        "",
        "1",
        "1:2",
        "1:2:3:4",
        "-1:2:3",
        "1:overflow:0",
        "1:2:+0",
    ] {
        assert!(cache.lookup(malformed).is_err(), "reject {malformed:?}");
    }
    cache.clear();
    let new = cache.admit(snapshot.clone(), "same-current-source".into());
    assert!(cache.lookup(&old_cursor).is_err());
    assert_ne!(old.cursor(1), new.cursor(1));
    let other = super::SelectedSkillReadCache::default();
    let elsewhere = other.admit(snapshot, "same-current-source".into());
    assert!(other.lookup(&new.cursor(1)).is_err());
    assert!(cache.lookup(&elsewhere.cursor(1)).is_err());
    // Empty resident state has no stale weak keys to accumulate.
    for _ in 0..100 {
        cache.clear();
        assert!(cache.lookup(&new.cursor(1)).is_err());
    }
}

#[test]
fn charged_pages_roll_back_all_failed_operations_and_keep_active_page_bytes() {
    let ledger = SelectedBudget::new(SelectedLimits {
        bytes: 400,
        owners: 1,
        inflight: 1,
    });
    let snapshot = owned_snapshot(&ledger, "body 界");
    let base = ledger.usage();
    assert!(
        test_page(&snapshot, 0, 300).is_err(),
        "scratch plus page must be precharged"
    );
    assert_eq!(ledger.usage(), base);
    for budget in [0, 1, 2, 3] {
        assert!(test_page(&snapshot, 0, budget).is_err());
        assert_eq!(ledger.usage(), base);
    }
    let pending = ledger.operation().unwrap();
    assert!(
        test_page(&snapshot, 0, 30).is_err(),
        "inflight is shared with materialization/probe"
    );
    drop(pending);
    assert_eq!(ledger.usage(), base);
    let failed = snapshot.page_response(
        0,
        30,
        |_, _| Ok(20),
        |_, _, _| Err(crate::SkillError::Validation("encoder failure".into())),
    );
    assert!(failed.is_err());
    assert_eq!(ledger.usage(), base);
    let oversized = snapshot.page_response(
        0,
        30,
        |_, _| Ok(20),
        |_, _, writer| {
            std::io::Write::write_all(writer, &[b'x'; 40])
                .map_err(|e| crate::SkillError::Validation(e.to_string()))
        },
    );
    assert!(
        oversized.is_err(),
        "fixed output buffer cannot grow behind the ledger"
    );
    assert_eq!(ledger.usage(), base);
    let page = test_page(&snapshot, 0, 30).unwrap();
    assert_eq!(
        ledger.usage().0,
        base.0 + 30,
        "page bytes remain charged during final async host checks"
    );
    assert_eq!(
        ledger.usage().2,
        0,
        "probe can acquire the same inflight lease"
    );
    drop(snapshot);
    assert_eq!(ledger.usage(), (30, 0, 0));
    drop(page);
    assert_eq!(ledger.usage(), (0, 0, 0));
}

#[test]
fn advancing_utf8_search_matches_bruteforce_and_preserves_complete_eof() {
    let ledger = SelectedBudget::default();
    for contents in ["", "a", "界", "🦀", "a界🦀\r\n\0\"\\tail"] {
        let snapshot = owned_snapshot(&ledger, contents);
        for start in (0..=contents.len()).filter(|offset| contents.is_char_boundary(*offset)) {
            for budget in 0..45 {
                let mut expected = None;
                for end in
                    (start..=contents.len()).filter(|offset| contents.is_char_boundary(*offset))
                {
                    if end == start && end < contents.len() {
                        continue;
                    }
                    let offset = (end < contents.len()).then_some(end);
                    if serde_json::to_vec(&(&contents[start..end], offset))
                        .unwrap()
                        .len()
                        <= budget
                    {
                        expected = Some(end);
                    }
                }
                match (test_page(&snapshot, start, budget), expected) {
                    (Ok(page), Some(end)) => {
                        let (text, next): (String, Option<usize>) = serde_json::from_str(page.as_str()).unwrap();
                        assert_eq!(text, contents[start..end]);
                        assert_eq!(next, (end < contents.len()).then_some(end));
                        assert!(page.as_str().len() <= budget);
                        assert!(next.is_none() || !text.is_empty());
                    },
                    (Err(_), None) => {},
                    (result, expected) => panic!("start={start} budget={budget} contents={contents:?} actual={result:?} expected={expected:?}"),
                }
                assert_eq!(ledger.usage().2, 0);
            }
        }
    }
    assert_eq!(ledger.usage(), (0, 0, 0));
}

#[test]
fn warm_probe_page_candidates_never_copy_the_whole_large_buffer() {
    let ledger = SelectedBudget::default();
    let raw = "界🦀\"\\\r\n".repeat(100_000);
    let snapshot = owned_snapshot(&ledger, &raw);
    let baseline = ledger.usage();
    let observed = std::cell::Cell::new(0usize);
    let page = snapshot
        .page_response(
            0,
            512 * 1024,
            |contents, next| {
                observed.set(observed.get().max(contents.len()));
                assert_eq!(
                    ledger.usage().0,
                    baseline.0 + 256,
                    "only bounded scratch exists during borrowed probes"
                );
                Ok(serde_json::to_vec(&(contents, next)).unwrap().len())
            },
            |contents, next, writer| {
                serde_json::to_writer(writer, &(contents, next))
                    .map_err(|e| crate::SkillError::Validation(e.to_string()))
            },
        )
        .unwrap();
    assert!(observed.get() <= 512 * 1024 && observed.get() < raw.len());
    assert_eq!(ledger.usage().0, baseline.0 + 512 * 1024);
    let (contents, next): (String, Option<usize>) = serde_json::from_str(page.as_str()).unwrap();
    assert_eq!(contents, raw[..next.unwrap()]);
    drop(page);
    assert_eq!(ledger.usage(), baseline);
}

#[test]
fn usage_guidance_is_atomic_budgeted_and_requires_complete_current_turn_reads() {
    use super::{render_skill_usage_instructions, SkillMetadataBudget};
    let guidance =
        render_skill_usage_instructions(SkillMetadataBudget::Characters(10_000)).unwrap();
    for required in [
        "stable package",
        "next_cursor",
        "EOF",
        "main agent",
        "Multiple mentions",
        "later turns",
        "references/guide.md",
    ] {
        assert!(guidance.contains(required));
    }
    let chars = guidance.chars().count();
    assert!(render_skill_usage_instructions(SkillMetadataBudget::Characters(chars)).is_some());
    assert!(render_skill_usage_instructions(SkillMetadataBudget::Characters(chars - 1)).is_none());
    let tokens = SkillMetadataBudget::Tokens(usize::MAX).cost(guidance);
    assert!(render_skill_usage_instructions(SkillMetadataBudget::Tokens(tokens)).is_some());
    assert!(render_skill_usage_instructions(SkillMetadataBudget::Tokens(tokens - 1)).is_none());
}
