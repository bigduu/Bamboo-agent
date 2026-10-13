//! One polled command's ownership before its first result is delivered.
//!
//! The completion task remains the reaper. Delivery only borrows its retained
//! Child synchronously, so a terminal handoff cannot reap and then suspend
//! while cancellation still has process-group authority.
use std::{
    future::{poll_fn, Future},
    io,
    pin::pin,
    process::ExitStatus,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    process::{Child, ChildStderr, ChildStdin, ChildStdout, Command},
    sync::watch,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Pending,
    Delivered,
    Cancelled,
}

struct State {
    phase: Phase,
    process: Option<Process>,
    capture_ready: bool,
    outcome: Option<Result<ExitStatus, String>>,
}

pub(crate) struct InitialHandoff {
    state: Mutex<State>,
    changed: watch::Sender<u64>,
}

impl std::fmt::Debug for InitialHandoff {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InitialHandoff")
            .field(
                "phase",
                &self.state.lock().unwrap_or_else(|e| e.into_inner()).phase,
            )
            .finish_non_exhaustive()
    }
}

impl InitialHandoff {
    pub(super) fn spawn(command: &mut Command) -> io::Result<(Arc<Self>, io::Result<()>)> {
        let (process, setup) = Process::spawn(command)?;
        Ok((
            Arc::new(Self {
                state: Mutex::new(State {
                    phase: Phase::Pending,
                    process: Some(process),
                    capture_ready: false,
                    outcome: None,
                }),
                changed: watch::channel(0).0,
            }),
            setup,
        ))
    }

    fn changed(&self) {
        self.changed
            .send_modify(|generation| *generation = generation.wrapping_add(1));
    }

    pub(crate) fn cancel(&self) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.phase == Phase::Pending {
            state.phase = Phase::Cancelled;
            drop(state);
            self.changed();
        }
    }

    pub(super) fn retain_cancelled_cleanup(self: &Arc<Self>) {
        let unfinished = {
            let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            state.phase == Phase::Cancelled && state.outcome.is_none()
        };
        if unfinished {
            let owner = self.clone();
            // Covers a synchronous post-spawn setup/pump-registration error.
            // The actual Child stays owned; Drop itself claims no stop proof.
            tokio::spawn(async move {
                if let Err(error) = owner.wait().await {
                    tracing::warn!(%error, "initial exec retained cleanup failed");
                }
            });
        }
    }

    pub(crate) fn start_kill(&self) -> io::Result<()> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.phase == Phase::Pending {
            state.phase = Phase::Cancelled;
            drop(state);
            self.changed();
            Ok(())
        } else if state.phase == Phase::Delivered {
            // Initial authority has ended. Preserve legacy later-handle kill.
            state
                .process
                .as_mut()
                .map_or(Ok(()), |p| p.child_mut().start_kill())
        } else {
            Ok(())
        }
    }

    pub(super) fn take_stdout(&self) -> Option<ChildStdout> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .process
            .as_mut()?
            .child_mut()
            .stdout
            .take()
    }

    pub(super) fn take_stderr(&self) -> Option<ChildStderr> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .process
            .as_mut()?
            .child_mut()
            .stderr
            .take()
    }

    pub(super) fn take_stdin(&self) -> Option<ChildStdin> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .process
            .as_mut()?
            .child_mut()
            .stdin
            .take()
    }

    /// Observe natural exit without releasing the pending group/job authority.
    /// `true` is readiness for capture, never a wait/reap receipt.
    pub(super) async fn prepare_wait(&self) -> io::Result<bool> {
        let mut changed = self.changed.subscribe();
        loop {
            {
                let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
                if state.phase != Phase::Pending {
                    return Ok(false);
                }
                if state
                    .process
                    .as_mut()
                    .ok_or_else(|| io::Error::other("initial process owner disappeared"))?
                    .leader_has_exited()?
                {
                    return Ok(true);
                }
            }
            tokio::select! {
                _ = changed.changed() => {},
                _ = tokio::time::sleep(Duration::from_millis(25)) => {},
            }
        }
    }

    pub(super) fn mark_capture_ready(&self) {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .capture_ready = true;
        self.changed();
    }

    pub(crate) async fn capture_ready(&self) {
        let mut changed = self.changed.subscribe();
        loop {
            {
                let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
                if state.capture_ready || state.outcome.is_some() {
                    return;
                }
            }
            if changed.changed().await.is_err() {
                return;
            }
        }
    }

    pub(crate) async fn resolved(&self) {
        let mut changed = self.changed.subscribe();
        loop {
            if self.state.lock().unwrap_or_else(|e| e.into_inner()).phase != Phase::Pending {
                return;
            }
            if changed.changed().await.is_err() {
                return;
            }
        }
    }

    #[cfg(all(test, unix))]
    pub(crate) fn retained_leader_has_exited(&self) -> io::Result<bool> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.phase != Phase::Pending {
            return Err(io::Error::other("test requires an undelivered owner"));
        }
        state
            .process
            .as_mut()
            .ok_or_else(|| io::Error::other("initial process owner disappeared"))?
            .leader_has_exited()
    }

    /// Last synchronous step of the invoking Future, after its final await.
    /// A natural terminal proposal is returned only after an actual wait.
    pub(crate) fn deliver(&self) -> io::Result<Option<ExitStatus>> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.phase != Phase::Pending {
            return Err(io::Error::other(
                "initial exec ownership was cancelled before delivery",
            ));
        }
        let ready = state.capture_ready;
        let process = state
            .process
            .as_mut()
            .ok_or_else(|| io::Error::other("initial process owner disappeared"))?;
        let status = if ready {
            Some(process.try_wait()?.ok_or_else(|| {
                io::Error::other("terminal capture became ready without a leader exit")
            })?)
        } else {
            None
        };
        process.disarm()?;
        state.phase = Phase::Delivered;
        drop(state);
        self.changed();
        Ok(status)
    }

    pub(super) async fn wait(&self) -> io::Result<ExitStatus> {
        let mut changed = self.changed.subscribe();
        loop {
            let phase = {
                let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
                if let Some(outcome) = &state.outcome {
                    return outcome.clone().map_err(io::Error::other);
                }
                state.phase
            };
            let outcome = match phase {
                Phase::Pending => {
                    changed
                        .changed()
                        .await
                        .map_err(|_| io::Error::other("initial ownership signal disappeared"))?;
                    continue;
                }
                Phase::Cancelled => {
                    let process = self
                        .state
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .process
                        .take()
                        .ok_or_else(|| io::Error::other("initial process owner disappeared"))?;
                    process.stop_and_reap().await
                }
                Phase::Delivered => {
                    // Child::wait is cancellation safe. Only poll it while the
                    // short synchronous lock is held, never across an await.
                    poll_fn(|cx| {
                        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
                        let process = state
                            .process
                            .as_mut()
                            .expect("delivered child remains owned");
                        pin!(process.child_mut().wait()).poll(cx)
                    })
                    .await
                }
            };
            self.state.lock().unwrap_or_else(|e| e.into_inner()).outcome =
                Some(outcome.as_ref().copied().map_err(ToString::to_string));
            self.changed();
            return outcome;
        }
    }
}

#[cfg(unix)]
struct Process {
    child: Child,
    pid: u32,
    armed: bool,
}

#[cfg(unix)]
impl Process {
    fn spawn(command: &mut Command) -> io::Result<(Self, io::Result<()>)> {
        command.process_group(0).kill_on_drop(true);
        let child = command.spawn()?;
        let pid = child
            .id()
            .ok_or_else(|| io::Error::other("initial process has no PID"))?;
        Ok((
            Self {
                child,
                pid,
                armed: true,
            },
            Ok(()),
        ))
    }
    fn child_mut(&mut self) -> &mut Child {
        &mut self.child
    }
    fn leader_has_exited(&mut self) -> io::Result<bool> {
        match super::leader_has_exited(self.pid) {
            Err(error) if error.raw_os_error() == Some(libc::ECHILD) => {
                self.armed = false;
                Err(error)
            }
            result => result,
        }
    }
    fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        match self.child.try_wait() {
            Ok(Some(status)) => {
                self.armed = false;
                Ok(Some(status))
            }
            Err(error) if error.raw_os_error() == Some(libc::ECHILD) => {
                self.armed = false;
                Err(error)
            }
            result => result,
        }
    }
    fn disarm(&mut self) -> io::Result<()> {
        self.armed = false;
        Ok(())
    }
    async fn stop_and_reap(mut self) -> io::Result<ExitStatus> {
        if !self.armed {
            return Err(io::Error::other(
                "initial group leader identity is no longer retained",
            ));
        }
        super::signal_owned_group(self.pid).await?;
        let status = self.child.wait().await?;
        self.armed = false;
        super::drain_group(self.pid).await?;
        Ok(status)
    }
}

#[cfg(unix)]
impl Drop for Process {
    fn drop(&mut self) {
        if self.armed && self.child.id().is_some() {
            // Runtime-shutdown fallback only. Async wait/drain produces proof.
            let _ = super::signal_group(self.pid);
            let _ = self.child.start_kill();
        }
    }
}

#[cfg(windows)]
type Process = super::windows::InitialWindowsProcess;

#[cfg(windows)]
impl Process {
    fn spawn(command: &mut Command) -> io::Result<(Self, io::Result<()>)> {
        super::windows::spawn_initial(command)
    }
    fn child_mut(&mut self) -> &mut Child {
        &mut self.child
    }
    fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        self.child.try_wait()
    }
}

#[cfg(not(any(unix, windows)))]
struct Process;

#[cfg(not(any(unix, windows)))]
impl Process {
    fn spawn(_: &mut Command) -> io::Result<(Self, io::Result<()>)> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "initial exec ownership requires Unix process groups or Windows Job Objects",
        ))
    }
    fn child_mut(&mut self) -> &mut Child {
        unreachable!("unsupported initial process cannot be spawned")
    }
    fn leader_has_exited(&mut self) -> io::Result<bool> {
        unreachable!("unsupported initial process cannot be spawned")
    }
    fn disarm(&mut self) -> io::Result<()> {
        unreachable!("unsupported initial process cannot be spawned")
    }
    fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        unreachable!("unsupported initial process cannot be spawned")
    }
    async fn stop_and_reap(self) -> io::Result<ExitStatus> {
        unreachable!("unsupported initial process cannot be spawned")
    }
}
