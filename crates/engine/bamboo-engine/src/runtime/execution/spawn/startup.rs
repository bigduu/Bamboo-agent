use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use bamboo_agent_core::Session;
use chrono::{DateTime, Utc};
use tokio::sync::watch;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use super::{NativeTaskStartup, SpawnContext, SpawnJob};

const STARTUP_CHECK_DELAY: Duration = Duration::from_secs(60);
const STARTUP_INSPECTION_LIMIT: Duration = Duration::from_secs(3);
const PREPARATION_DELAYED: &str = "Startup preparation has not completed after 60 seconds.";
const SETUP_FAILED: &str = "Startup failed before the task could enter execution.";

#[derive(Clone)]
enum Progress {
    Queued,
    Preparing(Instant),
    Native {
        progress: watch::Receiver<NativeTaskStartup>,
        cancel: CancellationToken,
    },
}

#[derive(Clone)]
pub(crate) struct ChildStartup(Arc<Launch>);

struct Launch {
    child_id: String,
    birth: DateTime<Utc>,
    generation: u64,
    progress: watch::Sender<Progress>,
    finished: CancellationToken,
    retried: AtomicBool,
    reported: AtomicBool,
    failed_run: OnceLock<String>,
}

impl ChildStartup {
    pub(super) fn new(child: &Session) -> Option<Self> {
        let generation = child.recoverable_child_launch_generation()?;
        if child
            .metadata
            .contains_key(crate::ticket_runtime::TICKET_DISPATCH_KEY)
        {
            return None;
        }
        Some(Self(Arc::new(Launch {
            child_id: child.id.clone(),
            birth: child.created_at,
            generation,
            progress: watch::channel(Progress::Queued).0,
            finished: CancellationToken::new(),
            retried: AtomicBool::new(false),
            reported: AtomicBool::new(false),
            failed_run: OnceLock::new(),
        })))
    }

    pub(crate) fn matches_child(&self, child: &Session) -> bool {
        child.id == self.0.child_id
            && child.created_at == self.0.birth
            && child.child_launch_generation() == self.0.generation
            && !child.is_child_launch_cancelled(self.0.generation)
    }

    pub(super) fn preparing(&self) {
        self.0
            .progress
            .send_replace(Progress::Preparing(Instant::now()));
    }

    pub(crate) fn observe_native(
        &self,
        progress: watch::Receiver<NativeTaskStartup>,
        cancel: CancellationToken,
    ) {
        self.0
            .progress
            .send_replace(Progress::Native { progress, cancel });
    }

    pub(super) fn finish(&self) {
        self.0.finished.cancel();
    }

    /// The native backend proved that this Run stopped before Task entry and
    /// committed an error checkpoint for it.
    pub(crate) fn record_startup_failure(&self, run_id: &str) {
        let _ = self.0.failed_run.set(run_id.to_owned());
    }

    pub(super) fn finish_unless_native(&self) {
        if !matches!(*self.0.progress.borrow(), Progress::Native { .. }) {
            self.finish();
        }
    }

    /// Retry a returned transient I/O error in place. The caller still holds
    /// the launch fence and repeats the operation with the same run identity.
    /// An uncertain owner, dropped future or authority rejection never qualifies.
    pub(crate) fn retry_io(&self, job: &SpawnJob, step: &str, error: &io::Error) -> bool {
        let mut source: Option<&(dyn std::error::Error + 'static)> = Some(error);
        let mut transient = false;
        while let Some(cause) = source {
            if cause.is::<bamboo_domain::SessionAuthorityConflict>() {
                return false;
            }
            if let Some(io) = cause.downcast_ref::<io::Error>() {
                transient |= matches!(
                    io.kind(),
                    io::ErrorKind::Interrupted
                        | io::ErrorKind::TimedOut
                        | io::ErrorKind::WouldBlock
                );
                source = io
                    .get_ref()
                    .map(|error| error as &(dyn std::error::Error + 'static));
            } else {
                source = cause.source();
            }
        }
        if !transient || self.0.retried.swap(true, Ordering::AcqRel) {
            return false;
        }
        tracing::warn!(
            parent_session_id = %job.parent_session_id,
            child_session_id = %job.child_session_id,
            generation = self.0.generation,
            step,
            %error,
            "retrying the same Child startup after transient I/O"
        );
        true
    }

    pub(super) fn watch(self, ctx: SpawnContext, job: SpawnJob) {
        tokio::spawn(async move {
            if self.preparation_delayed().await {
                self.notify(&ctx, &job, PREPARATION_DELAYED).await;
            }
        });
    }

    async fn preparation_delayed(&self) -> bool {
        let mut progress = self.0.progress.subscribe();
        loop {
            let stage = progress.borrow_and_update().clone();
            let started_at = match stage {
                Progress::Queued => None,
                Progress::Preparing(started_at) => Some(started_at),
                Progress::Native { progress, cancel } => {
                    return self.native_preparation_delayed(progress, cancel).await;
                }
            };
            tokio::select! {
                biased;
                _ = self.0.finished.cancelled() => return false,
                changed = progress.changed() => {
                    if changed.is_err() {
                        return false;
                    }
                }
                _ = async {
                    match started_at {
                        Some(started_at) => tokio::time::sleep_until(started_at + STARTUP_CHECK_DELAY).await,
                        None => std::future::pending().await,
                    }
                } => {
                    if matches!(*progress.borrow(), Progress::Preparing(current) if Some(current) == started_at) {
                        return true;
                    }
                }
            }
        }
    }

    async fn native_preparation_delayed(
        &self,
        mut progress: watch::Receiver<NativeTaskStartup>,
        cancel: CancellationToken,
    ) -> bool {
        let wait = async {
            loop {
                let stage = *progress.borrow_and_update();
                match stage {
                    NativeTaskStartup::Started => return false,
                    NativeTaskStartup::WaitingForCapacity => {
                        if progress.changed().await.is_err() {
                            return false;
                        }
                    }
                    NativeTaskStartup::Preparing => {
                        return tokio::time::timeout(STARTUP_CHECK_DELAY, progress.changed())
                            .await
                            .is_err()
                            && *progress.borrow() == NativeTaskStartup::Preparing;
                    }
                }
            }
        };
        tokio::select! {
            biased;
            _ = self.0.finished.cancelled() => false,
            _ = cancel.cancelled() => false,
            delayed = wait => delayed,
        }
    }

    pub(super) fn report(self, ctx: SpawnContext, job: SpawnJob) {
        tokio::spawn(async move { self.notify(&ctx, &job, SETUP_FAILED).await });
    }

    async fn notify(&self, ctx: &SpawnContext, job: &SpawnJob, reason: &'static str) {
        let storage = ctx.agent.storage();
        let observation = tokio::time::timeout(STARTUP_INSPECTION_LIMIT, async {
            let parent_events = ctx
                .session_event_senders
                .read()
                .await
                .get(&job.parent_session_id)
                .cloned();
            let child = storage
                .load_runtime_control_plane(&job.child_session_id)
                .await?;
            Ok::<_, io::Error>((child, parent_events))
        })
        .await;
        let Ok(Ok((Some(child), parent_events))) = observation else {
            tracing::warn!(child_session_id = %job.child_session_id, reason,
                "could not confirm Child launch state for startup notice");
            return;
        };
        let status = child.last_run_status();
        let confirmed_failure = reason == SETUP_FAILED
            && status.as_deref() == Some("error")
            && self
                .0
                .failed_run
                .get()
                .is_some_and(|run| child.host_run_id() == Some(run.as_str()));
        if !self.matches_child(&child)
            || child.parent_session_id.as_deref() != Some(job.parent_session_id.as_str())
            || !(confirmed_failure || matches!(status.as_deref(), Some("pending" | "running")))
            || (reason == PREPARATION_DELAYED && !self.still_preparing())
            || self.0.reported.swap(true, Ordering::AcqRel)
        {
            return;
        }
        tracing::warn!(
            parent_session_id = %job.parent_session_id,
            child_session_id = %job.child_session_id,
            generation = self.0.generation,
            reason,
            "Child startup needs attention"
        );
        if let (Some(hook), Some(parent_events)) = (&ctx.child_run_launch_hook, parent_events) {
            hook.on_child_startup_blocked(job, parent_events, reason);
        }
    }

    fn still_preparing(&self) -> bool {
        !self.0.finished.is_cancelled()
            && match &*self.0.progress.borrow() {
                Progress::Queued => false,
                Progress::Preparing(_) => true,
                Progress::Native { progress, cancel } => {
                    !cancel.is_cancelled() && *progress.borrow() == NativeTaskStartup::Preparing
                }
            }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn startup() -> ChildStartup {
        let mut child = Session::new_child("child", "parent", "model", "Title");
        child.set_last_run_status("pending");
        child.advance_child_launch_generation().unwrap();
        child.mark_child_auto_run_launch_intent();
        ChildStartup::new(&child).unwrap()
    }

    #[test]
    fn retry_budget_is_shared_by_all_steps_of_the_launch() {
        let startup = startup();
        let job = SpawnJob {
            parent_session_id: "parent".into(),
            child_session_id: "child".into(),
            model: "model".into(),
            disabled_tools: None,
        };
        let conflict =
            io::Error::other(bamboo_domain::SessionAuthorityConflict("stale Run".into()));
        assert!(!startup.retry_io(&job, "bind", &conflict));
        let transient = io::Error::other(io::Error::from(io::ErrorKind::Interrupted));
        assert!(startup.retry_io(&job, "read", &transient));
        assert!(!startup.clone().retry_io(&job, "bind", &transient));
    }

    #[tokio::test(start_paused = true)]
    async fn queue_wait_does_not_report_and_dequeue_starts_a_finite_check() {
        let startup = startup();
        let started = Instant::now();
        let check = startup.preparation_delayed();
        tokio::pin!(check);
        assert!(futures::poll!(check.as_mut()).is_pending());
        tokio::time::advance(Duration::from_secs(61)).await;
        assert!(futures::poll!(check.as_mut()).is_pending());
        startup.preparing();
        assert!(futures::poll!(check.as_mut()).is_pending());
        assert!(check.await);
        assert!(started.elapsed() >= Duration::from_secs(121));
    }

    #[tokio::test(start_paused = true)]
    async fn late_dequeue_gets_a_full_preparation_window() {
        let startup = startup();
        let check = startup.preparation_delayed();
        tokio::pin!(check);
        assert!(futures::poll!(check.as_mut()).is_pending());

        tokio::time::advance(Duration::from_secs(59)).await;
        startup.preparing();
        let prepared_at = Instant::now();

        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(futures::poll!(check.as_mut()).is_pending());
        tokio::time::advance(Duration::from_secs(58)).await;
        assert!(futures::poll!(check.as_mut()).is_pending());
        assert!(check.await);
        assert!(prepared_at.elapsed() >= STARTUP_CHECK_DELAY);
        assert!(prepared_at.elapsed() < STARTUP_CHECK_DELAY + Duration::from_secs(1));
    }

    #[tokio::test(start_paused = true)]
    async fn native_capacity_wait_sleeps_until_entry_or_cancellation() {
        for enter in [true, false] {
            let startup = startup();
            let (native, progress) = watch::channel(NativeTaskStartup::WaitingForCapacity);
            let cancel = CancellationToken::new();
            startup.observe_native(progress, cancel.clone());
            let check = startup.preparation_delayed();
            tokio::pin!(check);
            assert!(futures::poll!(check.as_mut()).is_pending());
            tokio::time::advance(Duration::from_secs(120)).await;
            assert!(futures::poll!(check.as_mut()).is_pending());
            native.send_replace(NativeTaskStartup::Preparing);
            assert!(futures::poll!(check.as_mut()).is_pending());
            if enter {
                native.send_replace(NativeTaskStartup::Started);
            } else {
                cancel.cancel();
            }
            assert!(!check.await);
        }
    }
}
