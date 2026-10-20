//! Host-owned resource lifetime for one in-process execution.
//!
//! Cancellation requests are never receipts. A receipt is produced only after
//! every registered task has been joined. This covers registered tool invocations
//! and Bash processes, not arbitrary tasks spawned by third-party tool code.
use chrono::{DateTime, Utc};
use futures::FutureExt;
use std::{
    future::Future,
    io,
    process::ExitStatus,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};
use tokio::{
    process::{Child, ChildStderr, ChildStdin, ChildStdout, Command},
    sync::{oneshot, watch},
    task::JoinHandle,
};

#[cfg(target_os = "macos")]
mod darwin;

#[cfg(windows)]
mod windows;

mod initial;
pub(crate) use initial::InitialHandoff;

pub(crate) enum InitialWait {
    NaturalPending,
    NeedWait,
    Reaped(io::Result<ExitStatus>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalTaskResourceIdentity {
    pub session_id: String,
    pub run_id: String,
    pub child_birth: DateTime<Utc>,
    pub owner_id: uuid::Uuid,
}

/// Only constructed by successfully joining the exact resource owner.
#[derive(Debug)]
pub struct DrainedLocalTaskResources {
    identity: LocalTaskResourceIdentity,
}
impl DrainedLocalTaskResources {
    pub fn identity(&self) -> &LocalTaskResourceIdentity {
        &self.identity
    }
}

struct TrackedTask {
    task: JoinHandle<Result<(), String>>,
    cancellation_safe: bool,
    relay_abort: Option<tokio::task::AbortHandle>,
}

// Bound concurrently retained resources even when a single Agent opens many
// background processes. Finished tasks are actually joined before reuse.
const MAX_TRACKED_RESOURCE_TASKS: usize = 1024;

#[derive(Default)]
struct State {
    closed: bool,
    tasks: Vec<TrackedTask>,
    failure: Option<String>,
}
impl State {
    fn admit_task(&mut self) -> io::Result<()> {
        if self.closed {
            return Err(io::Error::other("local execution resource scope is closed"));
        }
        let mut index = 0;
        while index < self.tasks.len() {
            if !self.tasks[index].task.is_finished() {
                index += 1;
                continue;
            }
            let mut task = self.tasks.swap_remove(index);
            // is_finished is only a readiness hint, never a receipt. Poll the
            // real JoinHandle and preserve any panic/finalization failure.
            let Some(joined) = (&mut task.task).now_or_never() else {
                self.tasks.insert(index, task);
                index += 1;
                continue;
            };
            let error = match joined {
                Ok(Ok(())) => None,
                Ok(Err(error)) => Some(error),
                Err(error) if task.cancellation_safe && error.is_cancelled() => None,
                Err(error) => Some(format!("owned task did not finalize: {error}")),
            };
            if let Some(error) = error {
                self.failure.get_or_insert(error);
            }
        }
        if self.tasks.len() >= MAX_TRACKED_RESOURCE_TASKS {
            return Err(io::Error::other(
                "local execution resource task capacity exhausted",
            ));
        }
        Ok(())
    }
}
struct Inner {
    identity: LocalTaskResourceIdentity,
    state: Mutex<State>,
    stop: watch::Sender<bool>,
    drained: watch::Sender<Option<Result<(), String>>>,
}
#[derive(Clone)]
pub struct LocalTaskResources(Arc<Inner>);
tokio::task_local! {
    static RESOURCES: LocalTaskResources;
    static INLINE_INVOCATION_SAFE: Arc<AtomicBool>;
}

// Called only at the provenance-checked BuiltinToolExecutor dispatch after
// its actual invocation has been registered in the resource domain.
pub(crate) fn mark_current_invocation_owned() {
    let _ = INLINE_INVOCATION_SAFE.try_with(|safe| safe.store(true, Ordering::Release));
}

struct InlineInvocationGuard {
    resources: LocalTaskResources,
    safe: Arc<AtomicBool>,
}
impl Drop for InlineInvocationGuard {
    fn drop(&mut self) {
        if !self.safe.load(Ordering::Acquire) {
            self.resources.record_unconfirmed("inline tool invocation was interrupted; external or blocking work stop is unconfirmed");
        }
    }
}

impl LocalTaskResources {
    pub fn new(
        session_id: impl Into<String>,
        run_id: impl Into<String>,
        child_birth: DateTime<Utc>,
    ) -> Self {
        Self(Arc::new(Inner {
            identity: LocalTaskResourceIdentity {
                session_id: session_id.into(),
                run_id: run_id.into(),
                child_birth,
                owner_id: uuid::Uuid::new_v4(),
            },
            state: Mutex::new(State::default()),
            stop: watch::channel(false).0,
            drained: watch::channel(None).0,
        }))
    }
    pub fn identity(&self) -> &LocalTaskResourceIdentity {
        &self.0.identity
    }
    pub fn current() -> Option<Self> {
        RESOURCES.try_with(Clone::clone).ok()
    }
    pub async fn scope<F: Future>(&self, future: F) -> F::Output {
        RESOURCES.scope(self.clone(), future).await
    }
    /// Poison this resource domain when external/blocking work cannot be
    /// proved stopped. This can only deny a receipt, never manufacture one.
    pub fn record_unconfirmed(&self, error: impl Into<String>) {
        let mut state = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
        let failure = state.failure.get_or_insert(error.into()).clone();
        let already_drained = self.0.drained.borrow().is_some();
        if already_drained {
            self.0.drained.send_replace(Some(Err(failure)));
        }
    }
    /// Preserve inline/custom tool task-local capabilities. A normal return
    /// keeps the existing tool completion contract; cancellation or unwind
    /// denies a receipt unless the framework registered the real invocation.
    pub async fn guard_tool_invocation<F: Future>(&self, future: F) -> F::Output {
        let safe = Arc::new(AtomicBool::new(false));
        let guard = InlineInvocationGuard {
            resources: self.clone(),
            safe: safe.clone(),
        };
        let output = INLINE_INVOCATION_SAFE.scope(safe.clone(), future).await;
        safe.store(true, Ordering::Release);
        drop(guard);
        output
    }

    /// The registry, not the awaiting caller, owns the task's JoinHandle.
    /// A dropped/aborted awaiter cannot cancel a started filesystem write.
    pub fn spawn_owned<F, T>(&self, future: F) -> io::Result<oneshot::Receiver<T>>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let mut state = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
        state.admit_task()?;
        let (tx, rx) = oneshot::channel();
        let scope = self.clone();
        state.tasks.push(TrackedTask {
            cancellation_safe: false,
            relay_abort: None,
            task: tokio::spawn(async move {
                match std::panic::AssertUnwindSafe(scope.scope(future))
                    .catch_unwind()
                    .await
                {
                    Ok(result) => {
                        let _ = tx.send(result);
                        Ok(())
                    }
                    Err(_) => {
                        // A panic can have dropped a Tokio filesystem future with
                        // blocking work still outstanding. Never certify this scope.
                        let error =
                            "owned tool/resource task panicked; resource stop is unconfirmed"
                                .to_string();
                        scope.record_unconfirmed(error.clone());
                        Err(error)
                    }
                }
            }),
        });
        Ok(rx)
    }
    /// Close admission and request process cleanup once. Joining is owned by an
    /// independent finalizer, so cancellation of this wait is safely retryable.
    /// Call outside the invocation being drained, after execution has returned.
    pub async fn close_and_drain(&self) -> io::Result<DrainedLocalTaskResources> {
        let mut completed = self.0.drained.subscribe();
        {
            let mut state = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
            if !state.closed {
                state.closed = true;
                self.0.stop.send_replace(true);
                let tasks = std::mem::take(&mut state.tasks);
                for task in &tasks {
                    if task.cancellation_safe {
                        if let Some(relay) = &task.relay_abort {
                            relay.abort();
                        }
                        task.task.abort();
                    }
                }
                let scope = self.clone();
                tokio::spawn(async move {
                    for task in tasks {
                        match task.task.await {
                            Ok(Ok(())) => {}
                            Ok(Err(error)) => scope.record_unconfirmed(error),
                            Err(error) if task.cancellation_safe && error.is_cancelled() => {}
                            Err(error) => scope.record_unconfirmed(format!(
                                "owned task did not finalize: {error}"
                            )),
                        }
                    }
                    // Serialize the final publication with poisoning; a
                    // concurrent record_unconfirmed must never be overwritten
                    // by a stale successful drain snapshot.
                    let state = scope.0.state.lock().unwrap_or_else(|e| e.into_inner());
                    let result = state.failure.clone().map_or(Ok(()), Err);
                    scope.0.drained.send_replace(Some(result));
                });
            }
        }
        loop {
            if let Some(result) = completed.borrow().clone() {
                result.map_err(io::Error::other)?;
                return Ok(DrainedLocalTaskResources {
                    identity: self.identity().clone(),
                });
            }
            completed
                .changed()
                .await
                .map_err(|_| io::Error::other("resource finalizer disappeared"))?;
        }
    }
}

/// Spawn a cancellation-safe helper such as an auxiliary provider evaluation.
/// Never use this for filesystem writes: those must finish via `spawn_owned`.
/// The returned handle is a relay; the scope always joins the actual task.
pub fn spawn_cancellable<F, T>(future: F) -> JoinHandle<T>
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let Some(scope) = LocalTaskResources::current() else {
        return tokio::spawn(future);
    };
    let mut state = scope.0.state.lock().unwrap_or_else(|e| e.into_inner());
    if let Err(error) = state.admit_task() {
        return tokio::spawn(async move { panic!("{error}") });
    }
    let (tx, rx) = oneshot::channel();
    let owned_scope = scope.clone();
    let task = tokio::spawn(async move {
        let value = owned_scope.scope(future).await;
        let _ = tx.send(value);
        Ok(())
    });
    let abort = task.abort_handle();

    // Place the guard in the future before its first poll: an immediately
    // aborted relay must still request cancellation of the actual helper.
    struct AbortInner(tokio::task::AbortHandle);
    impl Drop for AbortInner {
        fn drop(&mut self) {
            self.0.abort();
        }
    }
    let guard = AbortInner(abort);
    let relay = tokio::spawn(async move {
        let _guard = guard;
        rx.await.expect("owned auxiliary task ended without result")
    });
    state.tasks.push(TrackedTask {
        task,
        cancellation_safe: true,
        relay_abort: Some(relay.abort_handle()),
    });
    relay
}

pub(crate) enum ResourceTask<T> {
    Legacy(JoinHandle<T>),
    Owned(oneshot::Receiver<T>),
}
impl<T: Send + 'static> ResourceTask<T> {
    pub(crate) fn spawn(future: impl Future<Output = T> + Send + 'static) -> io::Result<Self> {
        match LocalTaskResources::current() {
            Some(scope) => scope.spawn_owned(future).map(Self::Owned),
            None => Ok(Self::Legacy(tokio::spawn(future))),
        }
    }
    pub(crate) async fn join(self) -> io::Result<T> {
        match self {
            Self::Legacy(task) => task.await.map_err(io::Error::other),
            Self::Owned(task) => task.await.map_err(io::Error::other),
        }
    }
}

/// A process owned by the current local resource domain. Framework hook
/// executors use the same process containment and drain as Bash. Construction
/// requires a bound domain; callers cannot manufacture an owned process.
pub struct LocalTaskChild(ManagedBashChild);

impl LocalTaskChild {
    pub fn spawn(command: &mut Command) -> io::Result<Self> {
        if LocalTaskResources::current().is_none() {
            return Err(io::Error::other("local child requires a resource scope"));
        }
        ManagedBashChild::spawn(command).map(Self)
    }
    pub fn take_stdout(&mut self) -> Option<ChildStdout> {
        self.0.take_stdout()
    }
    pub fn take_stderr(&mut self) -> Option<ChildStderr> {
        self.0.take_stderr()
    }
    pub fn take_stdin(&mut self) -> Option<ChildStdin> {
        self.0.take_stdin()
    }
    pub async fn wait(&mut self) -> io::Result<ExitStatus> {
        self.0.wait().await
    }
    pub fn start_kill(&mut self) -> io::Result<()> {
        self.0.start_kill()
    }
    pub async fn kill(&mut self) -> io::Result<()> {
        self.0.kill().await
    }
}

/// Legacy Child behavior is untouched when no local execution scope is bound.
pub(crate) enum ManagedBashChild {
    Legacy(Child),
    Initial(Arc<InitialHandoff>),
    Owned {
        stdout: Option<ChildStdout>,
        stderr: Option<ChildStderr>,
        stdin: Option<ChildStdin>,
        kill: watch::Sender<bool>,
        result: watch::Receiver<Option<Result<ExitStatus, String>>>,
    },
}
impl From<Child> for ManagedBashChild {
    fn from(child: Child) -> Self {
        Self::Legacy(child)
    }
}
impl ManagedBashChild {
    pub(crate) fn spawn_polled(command: &mut Command) -> io::Result<Self> {
        if LocalTaskResources::current().is_some() {
            return Self::spawn(command);
        }
        let (owner, setup) = InitialHandoff::spawn(command)?;
        if let Err(error) = setup {
            owner.cancel();
            // Setup already created a real child. Keep that exact owner until
            // its cleanup completes, even though no tool handle is returned.
            tokio::spawn(async move {
                if let Err(cleanup) = owner.wait().await {
                    tracing::warn!(%cleanup, "initial exec setup cleanup failed");
                }
            });
            return Err(error);
        }
        Ok(Self::Initial(owner))
    }

    pub(crate) fn initial_handoff(&self) -> Option<Arc<InitialHandoff>> {
        match self {
            Self::Initial(owner) => Some(owner.clone()),
            _ => None,
        }
    }

    pub(crate) async fn prepare_initial_wait(&mut self) -> io::Result<InitialWait> {
        match self {
            Self::Initial(owner) => Ok(if owner.prepare_wait().await? {
                InitialWait::NaturalPending
            } else {
                InitialWait::NeedWait
            }),
            _ => Ok(InitialWait::Reaped(self.wait().await)),
        }
    }

    pub(crate) fn mark_initial_capture_ready(&self) {
        if let Self::Initial(owner) = self {
            owner.mark_capture_ready();
        }
    }

    pub(crate) fn spawn(command: &mut Command) -> io::Result<Self> {
        let Some(scope) = LocalTaskResources::current() else {
            return command.spawn().map(Self::Legacy);
        };
        #[cfg(windows)]
        {
            return windows::spawn(command, &scope);
        }
        #[cfg(not(any(unix, windows)))]
        {
            let _ = (scope, command);
            return Err(io::Error::new(io::ErrorKind::Unsupported, "local task Bash resource ownership requires Unix process groups or Windows Job Objects; legacy Bash remains available"));
        }
        #[cfg(unix)]
        {
            // Admission and spawn share the registry lock: close cannot certify
            // an empty registry while a process is being created.
            let mut state = scope.0.state.lock().unwrap_or_else(|e| e.into_inner());
            state.admit_task()?;
            command.process_group(0).kill_on_drop(true);
            let mut child = command.spawn()?;
            let pid = child
                .id()
                .ok_or_else(|| io::Error::other("owned process has no PID"))?;
            let stdout = child.stdout.take();
            let stderr = child.stderr.take();
            let stdin = child.stdin.take();
            let mut child = GroupLeader { child, pid };
            let (kill, mut requested) = watch::channel(false);
            let (result_tx, result) = watch::channel(None);
            let mut stop = scope.0.stop.subscribe();
            state.tasks.push(TrackedTask {
                cancellation_safe: false,
                relay_abort: None,
                task: tokio::spawn(async move {
                    let outcome = async {
                        tokio::select! {
                            result = wait_without_reaping(pid) => result.map_err(|error| owned_group_error(pid, "WNOWAIT leader observation", error))?,
                            _ = requested.changed() => {},
                            _ = stop.changed() => {},
                        }
                        // Keep the leader unreaped until signalling its group.
                        // Never signal a group after its leader PID can be
                        // recycled for an unrelated process.
                        signal_owned_group(pid)
                            .await
                            .map_err(|error| owned_group_error(pid, "pre-reap group signal", error))?;
                        let status = child
                            .child
                            .wait()
                            .await
                            .map_err(|error| owned_group_error(pid, "leader reap", error))?;
                        // A shell may exit before background descendants.
                        // Verify without sending any post-reap signals.
                        drain_group(pid)
                            .await
                            .map_err(|error| owned_group_error(pid, "post-reap group drain", error))?;
                        Ok::<_, io::Error>(status)
                    }
                    .await
                    .map_err(|error| error.to_string());
                    let receipt = outcome.as_ref().map(|_| ()).map_err(Clone::clone);
                    result_tx.send_replace(Some(outcome));
                    receipt
                }),
            });
            Ok(Self::Owned {
                stdout,
                stderr,
                stdin,
                kill,
                result,
            })
        }
    }
    pub(crate) fn take_stdout(&mut self) -> Option<ChildStdout> {
        match self {
            Self::Legacy(c) => c.stdout.take(),
            Self::Initial(owner) => owner.take_stdout(),
            Self::Owned { stdout, .. } => stdout.take(),
        }
    }
    pub(crate) fn take_stderr(&mut self) -> Option<ChildStderr> {
        match self {
            Self::Legacy(c) => c.stderr.take(),
            Self::Initial(owner) => owner.take_stderr(),
            Self::Owned { stderr, .. } => stderr.take(),
        }
    }
    pub(crate) fn take_stdin(&mut self) -> Option<ChildStdin> {
        match self {
            Self::Legacy(c) => c.stdin.take(),
            Self::Initial(owner) => owner.take_stdin(),
            Self::Owned { stdin, .. } => stdin.take(),
        }
    }
    pub(crate) async fn wait(&mut self) -> io::Result<ExitStatus> {
        match self {
            Self::Legacy(child) => child.wait().await,
            Self::Initial(owner) => owner.wait().await,
            Self::Owned { result, .. } => loop {
                if let Some(outcome) = result.borrow().clone() {
                    return outcome.map_err(io::Error::other);
                }
                result
                    .changed()
                    .await
                    .map_err(|_| io::Error::other("owned process finalizer disappeared"))?;
            },
        }
    }
    pub(crate) fn start_kill(&mut self) -> io::Result<()> {
        match self {
            Self::Legacy(child) => child.start_kill(),
            Self::Initial(owner) => owner.start_kill(),
            Self::Owned { kill, .. } => {
                kill.send_replace(true);
                Ok(())
            }
        }
    }
    pub(crate) async fn kill(&mut self) -> io::Result<()> {
        self.start_kill()?;
        self.wait().await.map(|_| ())
    }
}
impl Drop for ManagedBashChild {
    fn drop(&mut self) {
        if let Self::Initial(owner) = self {
            owner.cancel();
            owner.retain_cancelled_cleanup();
        }
        if let Self::Owned { kill, .. } = self {
            kill.send_replace(true);
        }
    }
}

/// Synchronous fallback for runtime shutdown or an aborted process supervisor.
/// It does not produce proof; only the async reaper can certify termination.
#[cfg(unix)]
struct GroupLeader {
    child: Child,
    pid: u32,
}
#[cfg(unix)]
impl Drop for GroupLeader {
    fn drop(&mut self) {
        if self.child.id().is_some() {
            let _ = signal_group(self.pid);
            let _ = self.child.start_kill();
        }
    }
}

#[cfg(unix)]
async fn wait_without_reaping(pid: u32) -> io::Result<()> {
    loop {
        if leader_has_exited(pid)? {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}

#[cfg(unix)]
fn leader_has_exited(pid: u32) -> io::Result<bool> {
    loop {
        // SAFETY: waitid initializes siginfo; WNOWAIT retains the owned leader
        // as a zombie, preventing PID/PGID reuse before the group signal.
        let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
        let result = unsafe {
            libc::waitid(
                libc::P_PID,
                pid as libc::id_t,
                &mut info,
                libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
            )
        };
        if result != 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(error);
        }
        // SAFETY: a successful waitid initialized the SIGCHLD fields. Zero
        // PID means no state change with WNOHANG.
        return Ok(unsafe { info.si_pid() != 0 });
    }
}

#[cfg(unix)]
fn signal_group(pid: u32) -> io::Result<()> {
    // SAFETY: this immutable PID was obtained from a Child created with its own
    // process group. Never derived from a session registry or model output.
    if unsafe { libc::kill(-(pid as libc::pid_t), libc::SIGKILL) } == 0 {
        return Ok(());
    }
    let error = io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ESRCH) {
        Ok(())
    } else {
        Err(error)
    }
}
#[cfg(unix)]
fn owned_group_error(pid: u32, phase: &str, error: io::Error) -> io::Error {
    #[cfg(target_os = "macos")]
    {
        io::Error::new(
            error.kind(),
            format!("Darwin owned process group {pid} {phase}: {error}"),
        )
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = (pid, phase);
        error
    }
}

#[cfg(unix)]
async fn signal_owned_group(pid: u32) -> io::Result<()> {
    #[cfg(not(target_os = "macos"))]
    {
        signal_group(pid)
    }
    #[cfg(target_os = "macos")]
    {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            match signal_group(pid) {
                Ok(()) => return Ok(()),
                Err(error) if error.raw_os_error() == Some(libc::EPERM) => {
                    // Darwin excludes zombies from killpg's permission scan.
                    // EPERM is only a reason to observe, never a stop receipt.
                    // The caller retains the leader, so retries cannot target
                    // a new owner's recycled PID or process group.
                    if !darwin::has_running_members(pid, true).map_err(|error| {
                        owned_group_error(pid, "pre-reap observation after SIGKILL EPERM", error)
                    })? {
                        return Ok(());
                    }
                    if tokio::time::Instant::now() >= deadline {
                        return Err(io::Error::new(
                            io::ErrorKind::TimedOut,
                            format!("Darwin owned process group {pid} stop remains unconfirmed after SIGKILL EPERM"),
                        ));
                    }
                }
                Err(error) => return Err(error),
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }
}

#[cfg(unix)]
async fn drain_group(pid: u32) -> io::Result<()> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        if !group_has_running_members(pid)? {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "owned Bash process group did not stop",
            ));
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}
// Only an individual proc stat observation may disappear this way. Other
// errors and every other cleanup phase must still fail closed.
#[cfg(any(target_os = "linux", all(test, unix)))]
fn proc_stat_task_vanished(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::NotFound
}

#[cfg(unix)]
fn group_has_running_members(pid: u32) -> io::Result<bool> {
    #[cfg(target_os = "linux")]
    {
        // Unreaped orphan zombies cannot execute/write, but kill(0) sees them
        // until init reaps them (which need not be prompt inside containers).
        for entry in std::fs::read_dir("/proc")? {
            let entry = entry?;
            if entry.file_name().to_string_lossy().parse::<u32>().is_err() {
                continue;
            }
            let stat = match std::fs::read_to_string(entry.path().join("stat")) {
                Ok(stat) => stat,
                Err(error) if proc_stat_task_vanished(&error) => continue,
                Err(error) => return Err(error),
            };
            let Some((_, tail)) = stat.rsplit_once(')') else {
                return Err(io::Error::other("invalid process stat"));
            };
            let fields: Vec<_> = tail.split_whitespace().collect();
            if fields.get(2).and_then(|group| group.parse::<u32>().ok()) == Some(pid)
                && fields
                    .first()
                    .is_some_and(|state| !matches!(*state, "Z" | "X"))
            {
                return Ok(true);
            }
        }
        Ok(false)
    }
    #[cfg(target_os = "macos")]
    {
        // Read-only after reaping: never signal a possibly recycled group.
        darwin::has_running_members(pid, false)
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        // SAFETY: signal zero only checks for the owned group.
        if unsafe { libc::kill(-(pid as libc::pid_t), 0) } == 0 {
            return Ok(true);
        }
        let error = io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::ESRCH) {
            Ok(false)
        } else {
            Err(error)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn resources(run: &str) -> LocalTaskResources {
        LocalTaskResources::new("same-session", run, Utc::now())
    }

    #[tokio::test]
    async fn local_task_resources_poison_blocks_repeated_receipts() {
        let scope = resources("late-poison");
        scope.close_and_drain().await.unwrap();
        scope.record_unconfirmed("late external resource failure");
        assert!(scope.close_and_drain().await.is_err());
    }

    #[tokio::test]
    async fn local_task_resources_inline_completion_and_interruption_differ() {
        let complete = resources("inline-complete");
        assert_eq!(complete.guard_tool_invocation(async { 42 }).await, 42);
        complete.close_and_drain().await.unwrap();

        let interrupted = resources("inline-interrupted");
        let mut invoke = Box::pin(interrupted.guard_tool_invocation(std::future::pending::<()>()));
        assert!(futures::poll!(invoke.as_mut()).is_pending());
        drop(invoke);
        assert!(interrupted.close_and_drain().await.is_err());
    }

    #[tokio::test]
    async fn local_task_resources_harvest_actually_joins_completed_tasks() {
        let scope = resources("harvest");
        for _ in 0..(MAX_TRACKED_RESOURCE_TASKS + 8) {
            scope.spawn_owned(async {}).unwrap().await.unwrap();
            loop {
                let finished = scope
                    .0
                    .state
                    .lock()
                    .unwrap()
                    .tasks
                    .iter()
                    .all(|task| task.task.is_finished());
                if finished {
                    break;
                }
                tokio::task::yield_now().await;
            }
            assert!(scope.0.state.lock().unwrap().tasks.len() <= 1);
        }
        scope.close_and_drain().await.unwrap();
    }

    #[tokio::test]
    async fn local_task_resources_harvest_preserves_auxiliary_panic() {
        let scope = resources("harvest-panic");
        let task = scope
            .scope(async { spawn_cancellable(async { panic!("controlled auxiliary panic") }) })
            .await;
        assert!(task.await.is_err());
        scope.spawn_owned(async {}).unwrap().await.unwrap();
        assert!(scope.close_and_drain().await.is_err());
    }

    #[tokio::test]
    async fn local_task_resources_capacity_refuses_before_work_starts() {
        let scope = resources("capacity");
        let (release, wait) = watch::channel(false);
        for _ in 0..MAX_TRACKED_RESOURCE_TASKS {
            let mut wait = wait.clone();
            drop(
                scope
                    .spawn_owned(async move {
                        wait.changed().await.unwrap();
                    })
                    .unwrap(),
            );
        }
        assert!(scope
            .spawn_owned(async {})
            .unwrap_err()
            .to_string()
            .contains("capacity"));
        release.send_replace(true);
        scope.close_and_drain().await.unwrap();
    }

    #[tokio::test]
    async fn local_task_resources_wait_for_owned_work_after_awaiter_is_dropped() {
        let scope = resources("run");
        let (release, wait) = oneshot::channel();
        let receiver = scope
            .spawn_owned(async move {
                wait.await.unwrap();
            })
            .unwrap();
        drop(receiver);
        let mut drain = Box::pin(scope.close_and_drain());
        assert!(tokio::time::timeout(Duration::from_millis(20), &mut drain)
            .await
            .is_err());
        assert!(scope.spawn_owned(async {}).is_err());
        release.send(()).unwrap();
        let receipt = drain.await.unwrap();
        assert_eq!(receipt.identity(), scope.identity());
    }

    #[tokio::test]
    async fn local_task_resources_cancelled_drain_can_be_retried() {
        let scope = resources("run");
        let (release, wait) = oneshot::channel();
        drop(
            scope
                .spawn_owned(async move {
                    wait.await.unwrap();
                })
                .unwrap(),
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(20), scope.close_and_drain())
                .await
                .is_err()
        );
        release.send(()).unwrap();
        scope.close_and_drain().await.unwrap();
    }

    #[tokio::test]
    async fn local_task_resources_panic_never_produces_a_stop_receipt() {
        let scope = resources("run");
        let receiver = scope
            .spawn_owned(async { panic!("simulated write panic") })
            .unwrap();
        assert!(receiver.await.is_err());
        assert!(scope.close_and_drain().await.is_err());
        assert!(scope.close_and_drain().await.is_err());
    }

    #[tokio::test]
    async fn local_task_resources_close_joins_the_real_auxiliary_task() {
        struct Dropped(Arc<std::sync::atomic::AtomicBool>);
        impl Drop for Dropped {
            fn drop(&mut self) {
                self.0.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        }
        let scope = resources("run");
        let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let guard = Dropped(dropped.clone());
        let relay = scope
            .scope(async {
                spawn_cancellable(async move {
                    let _guard = guard;
                    std::future::pending::<()>().await;
                })
            })
            .await;
        // Dropping a plain Tokio handle does not abort the relay. Scope close
        // still owns and joins the real task rather than trusting this handle.
        drop(relay);
        scope.close_and_drain().await.unwrap();
        assert!(dropped.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[cfg(unix)]
    #[test]
    fn local_task_resources_proc_stat_disappearance_is_narrow() {
        for code in [libc::ENOENT, libc::ESRCH] {
            assert!(
                proc_stat_task_vanished(&io::Error::from_raw_os_error(code)),
                "a vanished proc entry must not poison cleanup: errno {code}"
            );
        }
        for code in [libc::EACCES, libc::EPERM, libc::EIO, libc::EINTR] {
            assert!(
                !proc_stat_task_vanished(&io::Error::from_raw_os_error(code)),
                "non-disappearance errors must remain failures: errno {code}"
            );
        }
        assert!(!proc_stat_task_vanished(&io::Error::other("unconfirmed")));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn local_task_resources_proc_stat_held_fd_reports_vanished_task() {
        use std::io::Read;

        let mut child = std::process::Command::new("sleep")
            .arg("30")
            .spawn()
            .unwrap();
        // Open while the owned child still exists, then deterministically
        // reap it before read: proc_single_show returns ESRCH, not ENOENT.
        let file = std::fs::File::open(format!("/proc/{}/stat", child.id()));
        let killed = child.kill();
        let waited = child.wait();
        killed.unwrap();
        waited.unwrap();
        let mut stat = String::new();
        let error = file.unwrap().read_to_string(&mut stat).unwrap_err();
        assert_eq!(error.raw_os_error(), Some(libc::ESRCH));
        assert!(proc_stat_task_vanished(&error));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn local_task_resources_drop_foreground_reaps_owned_process_group() {
        let scope = resources("run");
        let dir = tempfile::tempdir().unwrap();
        let pid_file = dir.path().join("descendant");
        let marker = dir.path().join("late-write");
        let mut command = Command::new("sh");
        command
            .arg("-c")
            .arg(format!(
                "(sleep 1; echo forbidden > '{}') & echo $! > '{}'; wait",
                marker.display(),
                pid_file.display()
            ))
            .kill_on_drop(true);
        let child = scope
            .scope(async { ManagedBashChild::spawn(&mut command).unwrap() })
            .await;
        for _ in 0..100 {
            if pid_file.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(pid_file.exists());
        drop(child);
        tokio::time::timeout(Duration::from_secs(6), scope.close_and_drain())
            .await
            .unwrap()
            .unwrap();
        tokio::time::sleep(Duration::from_millis(1100)).await;
        assert!(!marker.exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn local_task_resources_background_cleanup_is_exact_execution_scoped() {
        let a = resources("old-run");
        let b = resources("new-run");
        let first = a
            .scope(crate::tools::bash_runtime::spawn_background(
                "sleep 30",
                None,
                None,
                Some("same-session".into()),
                false,
                None,
            ))
            .await
            .unwrap();
        let second = b
            .scope(crate::tools::bash_runtime::spawn_background(
                "sleep 30",
                None,
                None,
                Some("same-session".into()),
                false,
                None,
            ))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(6), a.close_and_drain())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.status(), "completed");
        assert_eq!(
            second.status(),
            "running",
            "predecessor cleanup must not touch successor's shell"
        );
        tokio::time::timeout(Duration::from_secs(6), b.close_and_drain())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(second.status(), "completed");
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn local_task_resources_darwin_held_zombie_group_can_finalize() {
        let mut command = Command::new("/bin/sh");
        command
            .arg("-c")
            .arg("exit 0")
            .process_group(0)
            .kill_on_drop(true);
        let child = command.spawn().unwrap();
        let pid = child.id().unwrap();
        let mut leader = GroupLeader { child, pid };
        tokio::time::timeout(Duration::from_secs(5), wait_without_reaping(pid))
            .await
            .unwrap()
            .unwrap();
        // The owned, unreaped leader pins this exact PGID. Darwin excludes
        // zombies from killpg permission checks, even for their real parent.
        let raw_result = unsafe { libc::kill(-(pid as libc::pid_t), libc::SIGKILL) };
        let raw_error = io::Error::last_os_error();
        let signal_result = signal_owned_group(pid).await;
        let status = leader.child.wait().await.unwrap();
        eprintln!("owned held-zombie PGID={pid} raw_signal={raw_result} raw_errno={:?} owner_signal={signal_result:?} reaped={status}", raw_error.raw_os_error());
        assert_eq!(raw_result, -1);
        assert_eq!(raw_error.raw_os_error(), Some(libc::EPERM));
        assert!(status.success());
        signal_result.expect("a positively verified zombie-only owned group is already stopped");
        drain_group(pid).await.unwrap();
    }
    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn local_task_resources_darwin_full_window_live_group_is_killed_and_drained() {
        let scope = resources("darwin-large-group");
        let dir = tempfile::tempdir().unwrap();
        let mut command = Command::new("/bin/sh");
        command.current_dir(dir.path()).arg("-c").arg(
            "i=0; while [ \"$i\" -lt 24 ]; do sleep 30 & printf '%s\\n' \"$!\" >> pids; i=$((i + 1)); done; : > ready; wait",
        );
        let mut child = scope
            .scope(async { ManagedBashChild::spawn(&mut command).unwrap() })
            .await;
        let pid = match &child {
            ManagedBashChild::Owned { .. } => {
                // Each child PID is recorded by this owned shell before ready.
                tokio::time::timeout(Duration::from_secs(30), async {
                    while !dir.path().join("ready").exists() {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                })
                .await
                .expect("owned large-group fixture did not report ready");
                let child_pid = std::fs::read_to_string(dir.path().join("pids"))
                    .unwrap()
                    .lines()
                    .next()
                    .unwrap()
                    .parse::<u32>()
                    .unwrap();
                let mut info: libc::proc_bsdinfo = unsafe { std::mem::zeroed() };
                let bytes = unsafe {
                    libc::proc_pidinfo(
                        child_pid as libc::pid_t,
                        libc::PROC_PIDTBSDINFO,
                        1,
                        (&mut info as *mut libc::proc_bsdinfo).cast(),
                        std::mem::size_of::<libc::proc_bsdinfo>() as libc::c_int,
                    )
                };
                assert_eq!(bytes as usize, std::mem::size_of::<libc::proc_bsdinfo>());
                assert_eq!(info.pbi_pid, child_pid);
                assert_eq!(info.pbi_ppid, info.pbi_pgid);
                info.pbi_pgid
            }
            _ => panic!("large-group fixture must use owned process containment"),
        };
        let recorded: Vec<u32> = std::fs::read_to_string(dir.path().join("pids"))
            .unwrap()
            .lines()
            .map(|line| line.parse().unwrap())
            .collect();
        assert_eq!(recorded.len(), 24);
        let mut unique = recorded.clone();
        unique.sort_unstable();
        unique.dedup();
        assert_eq!(unique.len(), 24);
        let mut raw = [0 as libc::pid_t; 20];
        let (count, error) = unsafe {
            *libc::__error() = 0;
            let count = libc::proc_listpgrppids(
                pid as libc::pid_t,
                raw.as_mut_ptr().cast(),
                std::mem::size_of_val(&raw) as libc::c_int,
            );
            (count, *libc::__error())
        };
        assert_eq!(error, 0);
        assert_eq!(count, 20, "fixture must actually fill the fixed PID window");
        assert!(darwin::has_running_members(pid, true).unwrap());
        eprintln!(
            "owned large live PGID={pid} recorded_children={} raw_count={count}",
            recorded.len()
        );

        tokio::time::timeout(Duration::from_secs(6), child.kill())
            .await
            .expect("owned large-group kill did not finalize")
            .unwrap();
        tokio::time::timeout(Duration::from_secs(6), scope.close_and_drain())
            .await
            .unwrap()
            .unwrap();
        assert!(!group_has_running_members(pid).unwrap());
        for child_pid in recorded {
            let mut info: libc::proc_bsdinfo = unsafe { std::mem::zeroed() };
            let (bytes, error) = unsafe {
                *libc::__error() = 0;
                let bytes = libc::proc_pidinfo(
                    child_pid as libc::pid_t,
                    libc::PROC_PIDTBSDINFO,
                    1,
                    (&mut info as *mut libc::proc_bsdinfo).cast(),
                    std::mem::size_of::<libc::proc_bsdinfo>() as libc::c_int,
                );
                (bytes, *libc::__error())
            };
            if bytes == 0 && error == libc::ESRCH {
                continue;
            }
            assert_eq!(bytes as usize, std::mem::size_of::<libc::proc_bsdinfo>());
            assert!(
                info.pbi_pgid != pid || info.pbi_status == libc::SZOMB,
                "recorded PID {child_pid} remained live in owned group {pid}: status={}",
                info.pbi_status
            );
        }
    }
}
