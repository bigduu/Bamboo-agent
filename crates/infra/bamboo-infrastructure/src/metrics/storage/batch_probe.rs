//! Per-database test instrumentation; no production state or callbacks.
use std::sync::{mpsc, Arc, Mutex, OnceLock, Weak};

use super::*;

#[derive(Clone, Debug, Default)]
pub(super) struct Stats {
    pub tasks: usize,
    pub attempts: usize,
    pub opens: usize,
    pub commits: usize,
    pub cache_reuses: usize,
    pub events: Vec<String>,
}

#[derive(Default)]
struct State {
    stats: Stats,
    successes: usize,
    fail_opens: usize,
    panic_round: Option<String>,
    pause: Option<(usize, mpsc::Sender<()>, mpsc::Receiver<()>)>,
}

#[derive(Default)]
pub(super) struct Probe(Mutex<State>);

fn registry() -> &'static Mutex<HashMap<PathBuf, Weak<Probe>>> {
    static REGISTRY: OnceLock<Mutex<HashMap<PathBuf, Weak<Probe>>>> = OnceLock::new();
    REGISTRY.get_or_init(Default::default)
}

fn lookup(path: &Path) -> Option<Arc<Probe>> {
    let path = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    registry()
        .lock()
        .unwrap()
        .get(&path)
        .and_then(Weak::upgrade)
}

fn for_connection(connection: &Connection) -> Option<Arc<Probe>> {
    connection.path().and_then(|path| lookup(Path::new(path)))
}

impl Probe {
    pub(super) fn install(path: &Path) -> Arc<Self> {
        let probe = Arc::new(Self::default());
        registry().lock().unwrap().insert(
            std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf()),
            Arc::downgrade(&probe),
        );
        probe
    }

    pub(super) fn stats(&self) -> Stats {
        self.0.lock().unwrap().stats.clone()
    }
    pub(super) fn reset(&self) {
        *self.0.lock().unwrap() = State::default();
    }
    pub(super) fn fail_next_open(&self) {
        self.0.lock().unwrap().fail_opens = 1;
    }
    pub(super) fn panic_after_round_update(&self, round: &str) {
        self.0.lock().unwrap().panic_round = Some(round.into());
    }
    pub(super) fn pause_after(&self, successes: usize) -> (mpsc::Receiver<()>, mpsc::Sender<()>) {
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        self.0.lock().unwrap().pause = Some((successes, entered_tx, release_rx));
        (entered_rx, release_tx)
    }
}

pub(super) fn task_started(path: &Path) {
    if let Some(probe) = lookup(path) {
        probe.0.lock().unwrap().stats.tasks += 1;
    }
}

pub(super) fn before_open(path: &Path) -> MetricsResult<()> {
    if let Some(probe) = lookup(path) {
        let mut state = probe.0.lock().unwrap();
        state.stats.attempts += 1;
        if state.fail_opens != 0 {
            state.fail_opens -= 1;
            return Err(std::io::Error::other("injected metrics open failure").into());
        }
    }
    Ok(())
}

pub(super) fn opened(path: &Path) {
    if let Some(probe) = lookup(path) {
        probe.0.lock().unwrap().stats.opens += 1;
    }
}

pub(super) fn committed(connection: &Connection) {
    if let Some(probe) = for_connection(connection) {
        let mut state = probe.0.lock().unwrap();
        state.stats.commits += 1;
        state.stats.events.push("commit".into());
    }
}

pub(super) fn aggregated(connection: &Connection, session_id: &str) {
    if let Some(probe) = for_connection(connection) {
        probe
            .0
            .lock()
            .unwrap()
            .stats
            .events
            .push(format!("aggregate:{session_id}"));
    }
}

pub(super) fn cached_statement(connection: &Connection, previous_runs: i32) {
    if previous_runs > 0 {
        if let Some(probe) = for_connection(connection) {
            probe.0.lock().unwrap().stats.cache_reuses += 1;
        }
    }
}

pub(super) fn after_round_update(connection: &Connection, round_id: &str) {
    let panic_now = for_connection(connection).is_some_and(|probe| {
        let mut state = probe.0.lock().unwrap();
        if state.panic_round.as_deref() == Some(round_id) {
            state.panic_round.take();
            true
        } else {
            false
        }
    });
    assert!(!panic_now, "injected panic after metrics child update");
}

pub(super) fn after_item(connection: &Connection) {
    let pause = for_connection(connection).and_then(|probe| {
        let mut state = probe.0.lock().unwrap();
        state.successes += 1;
        state.stats.events.push("success".into());
        if state
            .pause
            .as_ref()
            .is_some_and(|(after, _, _)| *after == state.successes)
        {
            state.pause.take()
        } else {
            None
        }
    });
    if let Some((_, entered, release)) = pause {
        entered.send(()).unwrap();
        release.recv().unwrap();
    }
}
