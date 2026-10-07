//! Actual files and started blocking-job barriers; no fake reader authority.

use super::canonical_birth_census::{
    BirthObservation as Observation, CensusError as Error, Counts, Limits,
};
use super::*;
use bamboo_domain::{ActorDirectoryEntry, ActorDirectoryPort, ActorLogicalState, Message};
use std::collections::BTreeMap;
use std::sync::{Condvar, Mutex as StdMutex, Weak};

const WAIT: Duration = Duration::from_secs(10);

#[derive(Debug, Default)]
struct HookState {
    started: bool,
    released: bool,
    finished: bool,
    reads: Vec<(PathBuf, usize)>,
    counts: Counts,
    grow: Option<(PathBuf, Vec<u8>)>,
}
#[derive(Debug)]
pub(super) struct ReadHook {
    park: bool,
    state: StdMutex<HookState>,
    changed: Condvar,
    guard: StdMutex<Weak<DefaultWriterGuards>>,
}
impl ReadHook {
    fn install(store: &SessionStoreV2, park: bool) -> Arc<Self> {
        let hook = Arc::new(Self {
            park,
            state: StdMutex::new(HookState::default()),
            changed: Condvar::new(),
            guard: StdMutex::new(Weak::new()),
        });
        *store.census_read_hook.lock().unwrap() = Some(Arc::clone(&hook));
        hook
    }
    pub(super) fn attach(&self, guards: &Arc<DefaultWriterGuards>) {
        *self.guard.lock().unwrap() = Arc::downgrade(guards);
    }
    pub(super) fn started(&self) {
        let mut state = self.state.lock().unwrap();
        state.started = true;
        self.changed.notify_all();
        while self.park && !state.released {
            state = self.changed.wait(state).unwrap();
        }
    }
    pub(super) fn read(&self, path: &Path, count: usize) {
        let mut state = self.state.lock().unwrap();
        state.reads.push((path.to_path_buf(), count));
        if count > 0
            && state
                .grow
                .as_ref()
                .is_some_and(|(target, _)| target == path)
        {
            let (_, bytes) = state.grow.take().unwrap();
            use std::io::Write;
            std::fs::OpenOptions::new()
                .append(true)
                .open(path)
                .unwrap()
                .write_all(&bytes)
                .unwrap();
        }
    }
    pub(super) fn finished(&self, counts: Counts) {
        let mut state = self.state.lock().unwrap();
        state.finished = true;
        state.counts = counts;
        self.changed.notify_all();
    }
    fn wait(&self, finished: bool) {
        let state = self.state.lock().unwrap();
        let (state, timeout) = self
            .changed
            .wait_timeout_while(state, WAIT, |state| {
                if finished {
                    !state.finished
                } else {
                    !state.started
                }
            })
            .unwrap();
        assert!(!timeout.timed_out());
        assert!(if finished {
            state.finished
        } else {
            state.started
        });
    }
    fn release(&self) {
        self.state.lock().unwrap().released = true;
        self.changed.notify_all();
    }
}
struct Release(Arc<ReadHook>);
impl Drop for Release {
    fn drop(&mut self) {
        self.0.release();
    }
}

struct Fixture {
    _temp: tempfile::TempDir,
    home: PathBuf,
    store: Arc<SessionStoreV2>,
    other: Arc<SessionStoreV2>,
    root: Session,
    child: Session,
}
impl Fixture {
    async fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().canonicalize().unwrap();
        // Both Stores finish constructor recovery before test publications.
        let store = Arc::new(SessionStoreV2::new(home.clone()).await.unwrap());
        let other = Arc::new(SessionStoreV2::new(home.clone()).await.unwrap());
        let root = Session::new("census-root", "model");
        store.save_session(&root).await.unwrap();
        let child = Session::new_child_of("census-child", &root, "model", "Child");
        store.save_session(&child).await.unwrap();
        store.flush_search_index().await;
        Self {
            _temp: temp,
            home,
            store,
            other,
            root,
            child,
        }
    }
    fn directory(&self, session: &Session) -> PathBuf {
        if session.kind == SessionKind::Root {
            self.home.join("sessions").join(&session.id)
        } else {
            self.home
                .join("sessions")
                .join(&session.root_session_id)
                .join("children")
                .join(&session.id)
        }
    }
    // Synthetic physical fixture, never claimed to be a production publisher.
    fn pair(&self, session: &Session, compact: bool) {
        let dir = self.directory(session);
        std::fs::create_dir_all(dir.join("attachments")).unwrap();
        if session.kind == SessionKind::Root {
            std::fs::create_dir_all(dir.join("children")).unwrap();
        }
        let main = if compact {
            compact_main::serialize_main(session).unwrap()
        } else {
            serde_json::to_vec_pretty(session).unwrap()
        };
        std::fs::write(dir.join("session.json"), main).unwrap();
        std::fs::write(
            dir.join(RUNTIME_SIDECAR_FILE),
            serde_json::to_vec_pretty(&runtime_sidecar_snapshot(session)).unwrap(),
        )
        .unwrap();
    }
    fn edit_side(&self, edit: impl FnOnce(&mut serde_json::Value)) {
        let path = self.directory(&self.child).join(RUNTIME_SIDECAR_FILE);
        let mut value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        edit(&mut value);
        std::fs::write(path, serde_json::to_vec(&value).unwrap()).unwrap();
    }
    async fn unchanged(&self, id: &str, error: Error) {
        let before = business(&self.home);
        assert_eq!(self.store.canonical_birth_census(id).await, Err(error));
        assert_eq!(business(&self.home), before);
    }
}

fn business(home: &Path) -> BTreeMap<PathBuf, Option<Vec<u8>>> {
    fn visit(home: &Path, dir: &Path, result: &mut BTreeMap<PathBuf, Option<Vec<u8>>>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            let name = entry.file_name();
            if [
                SESSION_WRITE_LOCK_DIR,
                SESSION_LIFECYCLE_LOCK_FILE,
                SESSION_INDEX_LOCK_FILE,
                RUNTIME_TASK_TRANSACTION_LOCK_FILE,
            ]
            .contains(&name.to_str().unwrap())
            {
                continue;
            }
            let meta = std::fs::symlink_metadata(&path).unwrap();
            let bytes = if meta.is_file() {
                Some(std::fs::read(&path).unwrap())
            } else {
                None
            };
            result.insert(path.strip_prefix(home).unwrap().to_path_buf(), bytes);
            if meta.file_type().is_dir() {
                visit(home, &path, result);
            }
        }
    }
    let mut result = BTreeMap::new();
    visit(home, home, &mut result);
    result
}

async fn present(
    store: &SessionStoreV2,
    session: &Session,
) -> canonical_birth_census::BirthIdentity {
    let Observation::PresentIdentity(identity) =
        store.canonical_birth_census(&session.id).await.unwrap()
    else {
        panic!("expected full pair observation")
    };
    assert_eq!(identity.id, session.id);
    assert_eq!(identity.created_at, session.created_at);
    assert_eq!(identity.parent_id, session.parent_session_id);
    assert_eq!(identity.depth, session.spawn_depth);
    identity
}

#[tokio::test]
async fn census_actual_child_and_proofed_root_have_honest_distinct_outcomes() {
    let f = Fixture::new().await;
    f.unchanged(&f.root.id, Error::Unsupported).await;
    let dir = f.directory(&f.child);
    std::fs::write(dir.join(TOKEN_USAGE_FILE), b"PRIVATE inert non-JSON").unwrap();
    let hook = ReadHook::install(&f.other, false);
    let before = business(&f.home);
    let identity = present(&f.other, &f.child).await;
    assert!(!identity.actor_witnesses);
    assert_eq!(business(&f.home), before);
    {
        let state = hook.state.lock().unwrap();
        assert_eq!(
            state.counts.bytes,
            state.reads.iter().map(|(_, n)| n).sum::<usize>()
        );
        assert_eq!(
            state.counts.bytes,
            std::fs::metadata(dir.join("session.json")).unwrap().len() as usize
                + std::fs::metadata(dir.join(RUNTIME_SIDECAR_FILE))
                    .unwrap()
                    .len() as usize
        );
        assert!(!state
            .reads
            .iter()
            .any(|(path, _)| path.ends_with(TOKEN_USAGE_FILE)));
        assert_eq!(
            state
                .reads
                .iter()
                .filter(|(p, n)| p.ends_with("session.json") && *n == 0)
                .count(),
            1
        );
    }
    let legacy = Session::new("synthetic-legacy", "PRIVATE model");
    // After both constructors: migration must not add a proof to this synthetic setup.
    f.pair(&legacy, false);
    let before = business(&f.home);
    let identity = present(&f.store, &legacy).await;
    assert_eq!(identity.root_id, legacy.id);
    assert!(!format!("{identity:?}").contains("PRIVATE"));
    assert!(!f
        .directory(&legacy)
        .join(root_context::ROOT_TOOL_AUTHORITY_PROOF_FILE)
        .exists());
    assert_eq!(business(&f.home), before);
}

#[tokio::test]
async fn census_complete_physical_scan_ignores_index_and_rejects_any_second_slot() {
    let f = Fixture::new().await;
    let before = business(&f.home);
    present(&f.other, &f.child).await; // Other Store's in-memory index predates creation.
    assert_eq!(business(&f.home), before);
    let index = std::fs::read(&f.store.index_path).unwrap();
    std::fs::remove_file(&f.store.index_path).unwrap();
    let missing_index = business(&f.home);
    present(&f.other, &f.child).await;
    assert_eq!(business(&f.home), missing_index);
    for hint in [b"{ invalid index".as_slice(), b"{}".as_slice()] {
        std::fs::write(&f.store.index_path, hint).unwrap();
        let before = business(&f.home);
        present(&f.other, &f.child).await;
        assert_eq!(business(&f.home), before);
    }
    std::fs::write(&f.store.index_path, index).unwrap();
    let root_slot = f.home.join("sessions").join(&f.child.id);
    std::fs::create_dir(&root_slot).unwrap(); // Even empty is a candidate.
    f.unchanged(&f.child.id, Error::Conflict).await;
    std::fs::remove_dir(&root_slot).unwrap();
    let second = f
        .home
        .join("sessions/second-root/children")
        .join(&f.child.id);
    std::fs::create_dir_all(&second).unwrap();
    f.unchanged(&f.child.id, Error::Conflict).await;
}

#[tokio::test]
async fn census_empty_partial_unknown_and_nonregular_slots_never_become_vacant() {
    let f = Fixture::new().await;
    let synthetic = Session::new("partial", "model");
    for files in [
        vec![],
        vec!["session.json"],
        vec![RUNTIME_SIDECAR_FILE],
        vec!["actor-authority.json"],
        vec!["actor-authority.initialized.json"],
        vec![SEARCH_INDEX_REVISION_FILE],
    ] {
        let dir = f.directory(&synthetic);
        std::fs::create_dir_all(&dir).unwrap();
        for file in files {
            std::fs::write(dir.join(file), b"{}").unwrap();
        }
        f.unchanged(&synthetic.id, Error::Incomplete).await;
        std::fs::remove_dir_all(dir).unwrap();
    }
    assert_eq!(
        f.store.canonical_birth_census("vacant").await,
        Ok(Observation::VacantObserved)
    );
    assert_eq!(
        f.store.canonical_birth_census("../escape").await,
        Err(Error::InvalidInput)
    );
    let dir = f.directory(&synthetic);
    f.pair(&synthetic, false);
    for name in [
        "unknown-authority.json",
        "session.json.tmp.pending",
        "supervisor-authority.json",
    ] {
        std::fs::write(dir.join(name), b"PRIVATE").unwrap();
        f.unchanged(&synthetic.id, Error::Unsupported).await;
        std::fs::remove_file(dir.join(name)).unwrap();
    }
    std::fs::remove_file(dir.join(RUNTIME_SIDECAR_FILE)).unwrap();
    std::fs::create_dir(dir.join(RUNTIME_SIDECAR_FILE)).unwrap();
    f.unchanged(&synthetic.id, Error::Unsupported).await;
    #[cfg(unix)]
    {
        std::fs::remove_dir(dir.join(RUNTIME_SIDECAR_FILE)).unwrap();
        std::os::unix::fs::symlink(
            f.directory(&f.child).join(RUNTIME_SIDECAR_FILE),
            dir.join(RUNTIME_SIDECAR_FILE),
        )
        .unwrap();
        f.unchanged(&synthetic.id, Error::Unsupported).await;
    }
}

#[tokio::test]
async fn census_pair_identity_project_and_full_compact_validation_are_independent() {
    let f = Fixture::new().await;
    let original = std::fs::read(f.directory(&f.child).join(RUNTIME_SIDECAR_FILE)).unwrap();
    let variants = [
        ("id", serde_json::json!("another")),
        ("kind", serde_json::json!("root")),
        ("root_session_id", serde_json::json!("another-root")),
        ("parent_session_id", serde_json::Value::Null),
        ("spawn_depth", serde_json::json!(0)),
        (
            "created_at",
            serde_json::json!(f.child.created_at + chrono::Duration::seconds(1)),
        ),
        ("authority_identity", serde_json::json!({"kind":"unknown"})),
        (
            "authority_identity",
            serde_json::json!({"kind":"ordinary","unknown":1}),
        ),
        (
            "authority_identity",
            serde_json::json!({"kind":"supervisor","incarnation_id":Uuid::new_v4()}),
        ),
        ("root_orchestration_only", serde_json::json!(true)),
    ];
    for (key, value) in variants {
        f.edit_side(|side| side[key] = value);
        f.unchanged(&f.child.id, Error::Unsupported).await;
        std::fs::write(f.directory(&f.child).join(RUNTIME_SIDECAR_FILE), &original).unwrap();
    }
    for project in [
        serde_json::json!("different"),
        serde_json::json!(" bad "),
        serde_json::json!(3),
    ] {
        f.edit_side(|side| side["runtime_metadata"] = serde_json::json!({"project_id":project}));
        f.unchanged(&f.child.id, Error::Unsupported).await;
        std::fs::write(f.directory(&f.child).join(RUNTIME_SIDECAR_FILE), &original).unwrap();
    }
    let dir = f.directory(&f.child);
    let main = std::fs::read(dir.join("session.json")).unwrap();
    let mut value: serde_json::Value = serde_json::from_slice(&main).unwrap();
    value["created_at"] = serde_json::json!(Utc::now() + chrono::Duration::seconds(2));
    std::fs::write(
        dir.join("session.json"),
        serde_json::to_vec(&value).unwrap(),
    )
    .unwrap();
    f.unchanged(&f.child.id, Error::Unsupported).await; // Frame/flat cannot disagree.
    std::fs::write(dir.join("session.json"), b"{ not JSON").unwrap();
    f.unchanged(&f.child.id, Error::Unsupported).await;
    let mut project_pair = f.child.clone();
    project_pair.set_project_id_meta("exact-project");
    f.pair(&project_pair, true);
    assert_eq!(
        present(&f.store, &project_pair)
            .await
            .project
            .unwrap()
            .as_str(),
        "exact-project"
    );
    let side_path = dir.join(RUNTIME_SIDECAR_FILE);
    let side = String::from_utf8(std::fs::read(&side_path).unwrap()).unwrap();
    // Duplicate known Project survives no Map overwrite: pure raw decoder rejects.
    let duplicate = side.replacen(
        "\"project_id\": \"exact-project\"",
        "\"project_id\": \"exact-project\", \"project_id\": \"other\"",
        1,
    );
    assert_ne!(duplicate, side);
    std::fs::write(side_path, duplicate).unwrap();
    f.unchanged(&f.child.id, Error::Unsupported).await;
    f.pair(&f.child, true);
    let mut legacy = Session::new("legacy-normalized", "model");
    legacy.root_session_id.clear();
    f.pair(&legacy, false);
    assert_eq!(present(&f.store, &legacy).await.root_id, legacy.id);
    let nested = Session::new_child_of("nested", &f.child, "model", "Nested");
    f.store.save_session(&nested).await.unwrap();
    f.store.flush_search_index().await;
    assert_eq!(present(&f.store, &nested).await.depth, 2);
}

#[tokio::test]
async fn census_actor_marker_and_revocation_are_closed_observations_without_repair() {
    let f = Fixture::new().await;
    f.store.ensure_actor(&f.child.id).await.unwrap();
    let dir = f.directory(&f.child);
    assert!(present(&f.store, &f.child).await.actor_witnesses);
    let entry = std::fs::read(dir.join("actor-authority.json")).unwrap();
    let marker = std::fs::read(dir.join("actor-authority.initialized.json")).unwrap();
    let mut project_pair = f.child.clone();
    project_pair.set_project_id_meta("exact-project");
    f.pair(&project_pair, true);
    let mut project_entry: ActorDirectoryEntry = serde_json::from_slice(&entry).unwrap();
    project_entry.actor.project_id = Some("exact-project".into());
    std::fs::write(
        dir.join("actor-authority.json"),
        serde_json::to_vec(&project_entry).unwrap(),
    )
    .unwrap();
    assert!(present(&f.store, &project_pair).await.actor_witnesses);
    f.pair(&f.child, true);
    std::fs::write(dir.join("actor-authority.json"), &entry).unwrap();
    for (file, original) in [
        ("actor-authority.json", &entry),
        ("actor-authority.initialized.json", &marker),
    ] {
        std::fs::remove_file(dir.join(file)).unwrap();
        f.unchanged(&f.child.id, Error::Incomplete).await;
        std::fs::write(dir.join(file), original).unwrap();
        let mut value: serde_json::Value = serde_json::from_slice(original).unwrap();
        value["unknown"] = serde_json::json!(1);
        std::fs::write(dir.join(file), serde_json::to_vec(&value).unwrap()).unwrap();
        f.unchanged(&f.child.id, Error::Unsupported).await;
        std::fs::write(dir.join(file), original).unwrap();
    }
    for edit in ["project", "birth", "version", "state"] {
        let mut value: serde_json::Value = serde_json::from_slice(&entry).unwrap();
        match edit {
            "project" => value["actor"]["project_id"] = serde_json::json!("other"),
            "birth" => {
                value["actor"]["session_created_at"] =
                    serde_json::json!(Utc::now() + chrono::Duration::seconds(1))
            }
            "version" => value["schema_version"] = serde_json::json!(999),
            _ => value["actor"]["state"] = serde_json::json!("unknown"),
        }
        std::fs::write(
            dir.join("actor-authority.json"),
            serde_json::to_vec(&value).unwrap(),
        )
        .unwrap();
        f.unchanged(&f.child.id, Error::Unsupported).await;
        std::fs::write(dir.join("actor-authority.json"), &entry).unwrap();
    }
    let duplicate = String::from_utf8(marker.clone()).unwrap().replacen(
        "\"schema_version\":",
        "\"schema_version\": 999, \"schema_version\":",
        1,
    );
    std::fs::write(dir.join("actor-authority.initialized.json"), duplicate).unwrap();
    f.unchanged(&f.child.id, Error::Unsupported).await;
    std::fs::write(dir.join("actor-authority.initialized.json"), &marker).unwrap();
    let mut wrong_birth: serde_json::Value = serde_json::from_slice(&marker).unwrap();
    wrong_birth["session_created_at"] =
        serde_json::json!(f.child.created_at + chrono::Duration::seconds(1));
    std::fs::write(
        dir.join("actor-authority.initialized.json"),
        serde_json::to_vec(&wrong_birth).unwrap(),
    )
    .unwrap();
    f.unchanged(&f.child.id, Error::Unsupported).await;
    std::fs::write(dir.join("actor-authority.initialized.json"), &marker).unwrap();
    let revocations = f.home.join(root_lifetime::ROOT_REVOCATIONS_DIR);
    std::fs::create_dir_all(&revocations).unwrap();
    let path = revocations.join(format!("{}.json", f.child.id));
    let valid = serde_json::json!({"version":1,"session_id":f.child.id,"revoked_through":f.child.created_at});
    std::fs::write(&path, serde_json::to_vec(&valid).unwrap()).unwrap();
    assert!(present(&f.store, &f.child).await.revocation_seen); // No live-cutoff inference.
    for invalid in [
        serde_json::json!({"version":2,"session_id":f.child.id,"revoked_through":f.child.created_at}),
        serde_json::json!({"version":1,"session_id":"wrong","revoked_through":f.child.created_at}),
        serde_json::json!({"version":1,"session_id":f.child.id,"revoked_through":"invalid"}),
        serde_json::json!({"version":1,"session_id":f.child.id,"revoked_through":f.child.created_at,"unknown":1}),
    ] {
        std::fs::write(&path, serde_json::to_vec(&invalid).unwrap()).unwrap();
        f.unchanged(&f.child.id, Error::Unsupported).await;
    }
    std::fs::write(&path, b"{\"version\":1,\"version\":1}").unwrap();
    f.unchanged(&f.child.id, Error::Unsupported).await;
    std::fs::remove_file(&path).unwrap();
    std::fs::write(
        revocations.join("absent.json"),
        serde_json::to_vec(
            &serde_json::json!({"version":1,"session_id":"absent","revoked_through":Utc::now()}),
        )
        .unwrap(),
    )
    .unwrap();
    f.unchanged("absent", Error::Incomplete).await;
    // Valid inert records still cannot complete a missing initialized publication.
    let mut inert: ActorDirectoryEntry = serde_json::from_slice(&entry).unwrap();
    for state in [ActorLogicalState::Cold, ActorLogicalState::Retired] {
        inert.actor.state = state;
        assert_eq!(inert.actor.current_attempt, 0);
        inert.validate().unwrap();
        std::fs::write(
            dir.join("actor-authority.json"),
            serde_json::to_vec(&inert).unwrap(),
        )
        .unwrap();
        std::fs::remove_file(dir.join("actor-authority.initialized.json")).unwrap();
        f.unchanged(&f.child.id, Error::Incomplete).await;
        std::fs::write(dir.join("actor-authority.initialized.json"), &marker).unwrap();
    }
}

#[tokio::test]
async fn census_caps_charge_actual_reads_and_unrelated_entries_without_partial_success() {
    let f = Fixture::new().await;
    let dir = f.directory(&f.child);
    let hook = ReadHook::install(&f.store, false);
    let before = business(&f.home);
    assert_eq!(
        f.store
            .census_with_limits(
                &f.child.id,
                Limits {
                    main: 8,
                    ..Limits::default()
                }
            )
            .await,
        Err(Error::BudgetExceeded)
    );
    assert_eq!(hook.state.lock().unwrap().counts.bytes, 9); // Actual overflow byte charged once.
    assert_eq!(business(&f.home), before);
    let total = std::fs::metadata(dir.join("session.json")).unwrap().len() as usize + 11;
    assert_eq!(
        f.store
            .census_with_limits(
                &f.child.id,
                Limits {
                    total,
                    ..Limits::default()
                }
            )
            .await,
        Err(Error::BudgetExceeded)
    );
    assert_eq!(hook.state.lock().unwrap().counts.bytes, total);
    let side_path = dir.join(RUNTIME_SIDECAR_FILE);
    let original = std::fs::read(&side_path).unwrap();
    let mut padded = original.clone();
    padded.extend(vec![b' '; 8 * 1024 * 1024]);
    std::fs::write(&side_path, padded).unwrap();
    f.unchanged(&f.child.id, Error::BudgetExceeded).await;
    assert_eq!(
        hook.state.lock().unwrap().counts.bytes,
        total - 11 + 8 * 1024 * 1024 + 1
    );
    std::fs::write(side_path, original).unwrap();
    for n in 0..3 {
        std::fs::write(
            f.home.join("sessions").join(format!("unrelated-{n}")),
            b"ignored",
        )
        .unwrap();
    }
    assert_eq!(
        f.store
            .census_with_limits(
                &f.child.id,
                Limits {
                    roots: 1,
                    ..Limits::default()
                }
            )
            .await,
        Err(Error::BudgetExceeded)
    );
    assert_eq!(
        f.store
            .census_with_limits(
                &f.child.id,
                Limits {
                    probes: 1,
                    ..Limits::default()
                }
            )
            .await,
        Err(Error::BudgetExceeded)
    );
    std::fs::write(dir.join("actor-authority.json"), vec![b'x'; 20]).unwrap();
    std::fs::write(dir.join("actor-authority.initialized.json"), b"{}").unwrap();
    let before = business(&f.home);
    assert_eq!(
        f.store
            .census_with_limits(
                &f.child.id,
                Limits {
                    small: 5,
                    ..Limits::default()
                }
            )
            .await,
        Err(Error::BudgetExceeded)
    );
    assert_eq!(business(&f.home), before);
    for name in ["actor-authority.json", "actor-authority.initialized.json"] {
        std::fs::remove_file(dir.join(name)).unwrap();
    }
    let mut large = f.child.clone();
    large
        .messages
        .push(Message::user("PRIVATE ".repeat(128 * 1024)));
    f.pair(&large, true);
    assert!(!format!("{:?}", present(&f.store, &large).await).contains("PRIVATE"));
    large.messages = vec![Message::user("x".repeat(8 * 1024 * 1024 + 32))];
    f.pair(&large, true);
    f.unchanged(&large.id, Error::BudgetExceeded).await;
    assert_eq!(hook.state.lock().unwrap().counts.bytes, 8 * 1024 * 1024 + 1);
}

#[tokio::test]
async fn census_growth_uses_the_real_file_stream_not_stat_size_or_a_second_loader() {
    let f = Fixture::new().await;
    let dir = f.directory(&f.child);
    std::fs::remove_file(dir.join("session.json")).unwrap();
    std::fs::write(dir.join("session.json"), b"{}").unwrap();
    let hook = ReadHook::install(&f.store, false);
    hook.state.lock().unwrap().grow = Some((dir.join("session.json"), vec![b' '; 200]));
    assert_eq!(
        f.store
            .census_with_limits(
                &f.child.id,
                Limits {
                    main: 32,
                    ..Limits::default()
                }
            )
            .await,
        Err(Error::BudgetExceeded)
    );
    let state = hook.state.lock().unwrap();
    assert_eq!(state.counts.bytes, 33);
    assert_eq!(
        state.reads.iter().map(|(_, bytes)| bytes).sum::<usize>(),
        33
    );
    assert!(std::fs::metadata(dir.join("session.json")).unwrap().len() > 32);
}

fn held(store: &SessionStoreV2, id: &str) {
    for path in [
        store.bamboo_home_dir.join(SESSION_LIFECYCLE_LOCK_FILE),
        store
            .bamboo_home_dir
            .join(RUNTIME_TASK_TRANSACTION_LOCK_FILE),
        store.session_write_lock_path(id),
    ] {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        assert_eq!(
            FileExt::try_lock_exclusive(&file).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
    }
}
fn completed(store: &SessionStoreV2, id: &str, hook: &ReadHook) {
    hook.wait(true);
    let deadline = Instant::now() + WAIT;
    while hook.guard.lock().unwrap().upgrade().is_some() {
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(1));
    }
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(store.session_write_lock_path(id))
        .unwrap();
    FileExt::try_lock_exclusive(&file).unwrap();
    FileExt::unlock(&file).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn census_started_job_keeps_actual_guards_after_caller_abort_until_completion() {
    let f = Fixture::new().await;
    let hook = ReadHook::install(&f.store, true);
    let _release = Release(Arc::clone(&hook));
    let reader = {
        let store = Arc::clone(&f.store);
        let id = f.child.id.clone();
        tokio::spawn(async move { store.canonical_birth_census(&id).await })
    };
    let wait = Arc::clone(&hook);
    tokio::task::spawn_blocking(move || wait.wait(false))
        .await
        .unwrap();
    held(&f.store, &f.child.id);
    reader.abort();
    assert!(reader.await.unwrap_err().is_cancelled());
    held(&f.store, &f.child.id);
    assert!(hook.guard.lock().unwrap().upgrade().is_some());
    let before = business(&f.home);
    let mut changed = f.child.clone();
    changed.title = "later writer".into();
    let writer = {
        let store = Arc::clone(&f.other);
        let changed = changed.clone();
        tokio::spawn(async move { store.save_session(&changed).await })
    };
    assert!(tokio::time::timeout(Duration::from_millis(25), async {
        while !writer.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_err());
    assert_eq!(business(&f.home), before);
    hook.release();
    writer.await.unwrap().unwrap();
    completed(&f.store, &f.child.id, &hook);
    let saved: Session =
        serde_json::from_slice(&std::fs::read(f.directory(&changed).join("session.json")).unwrap())
            .unwrap();
    assert_eq!(saved.title, changed.title);
}

#[test]
fn census_started_job_outlives_whole_runtime_shutdown_without_releasing_physical_locks() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let f = runtime.block_on(Fixture::new());
    let hook = ReadHook::install(&f.store, true);
    let _release = Release(Arc::clone(&hook));
    let (tx, rx) = std::sync::mpsc::channel();
    struct Cancel(std::sync::mpsc::Sender<()>);
    impl Drop for Cancel {
        fn drop(&mut self) {
            let _ = self.0.send(());
        }
    }
    let reader = {
        let store = Arc::clone(&f.store);
        let id = f.child.id.clone();
        runtime.spawn(async move {
            let _cancel = Cancel(tx);
            store.canonical_birth_census(&id).await
        })
    };
    hook.wait(false);
    held(&f.store, &f.child.id);
    runtime.shutdown_timeout(Duration::from_millis(20));
    rx.recv_timeout(WAIT).unwrap();
    assert!(hook.guard.lock().unwrap().upgrade().is_some());
    held(&f.store, &f.child.id);
    let before = business(&f.home);
    hook.release();
    completed(&f.store, &f.child.id, &hook);
    assert_eq!(business(&f.home), before);
    let fresh = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    fresh.block_on(async {
        assert!(reader.await.unwrap_err().is_cancelled());
        let mut changed = f.child.clone();
        changed.title = "after shutdown".into();
        f.other.save_session(&changed).await.unwrap();
        let saved: Session = serde_json::from_slice(
            &std::fs::read(f.directory(&changed).join("session.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(saved.title, changed.title);
    });
}
