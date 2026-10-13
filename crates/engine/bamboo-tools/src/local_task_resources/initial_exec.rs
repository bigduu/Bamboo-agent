//! Per-spawn containment until an ordinary exec_command result is delivered.
//! This is not a Root resource scope: successful delivery restores legacy
//! direct-child lifetime, including descendants that retain output pipes.
use super::ManagedBashChild;
use std::{io, process::ExitStatus};
use tokio::{process::Command, sync::watch};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Disposition {
    Pending,
    Delivered,
    Cancelled,
}

#[derive(Clone)]
pub(crate) struct InitialExecHandle {
    disposition: watch::Sender<Disposition>,
    observed: watch::Receiver<Option<ExitStatus>>,
    #[cfg(windows)]
    job: std::sync::Arc<super::windows::WindowsJob>,
}

impl std::fmt::Debug for InitialExecHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InitialExecHandle")
            .field("disposition", &*self.disposition.borrow())
            .finish_non_exhaustive()
    }
}

impl InitialExecHandle {
    pub(super) fn observed(&self) -> watch::Receiver<Option<ExitStatus>> {
        self.observed.clone()
    }

    pub(crate) fn cancel(&self) {
        self.disposition.send_if_modified(|state| {
            if *state == Disposition::Pending {
                *state = Disposition::Cancelled;
                true
            } else {
                false
            }
        });
    }

    /// No await may separate this commit from returning the initial result.
    /// A failed Windows disarm stays pending so the invoking guard cleans up.
    pub(crate) fn deliver(&self) -> io::Result<()> {
        let mut result = Ok(());
        self.disposition.send_if_modified(|state| match state {
            Disposition::Pending => {
                #[cfg(windows)]
                if let Err(error) = self.job.disarm_kill_on_close() {
                    result = Err(error);
                    return false;
                }
                *state = Disposition::Delivered;
                true
            }
            Disposition::Delivered => false,
            Disposition::Cancelled => {
                result = Err(io::Error::other(
                    "initial exec was cancelled before delivery",
                ));
                false
            }
        });
        result
    }
}

pub(super) fn spawn(command: &mut Command) -> io::Result<ManagedBashChild> {
    #[cfg(unix)]
    {
        spawn_unix(command)
    }
    #[cfg(windows)]
    {
        spawn_windows(command)
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = command;
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "initial exec containment requires Unix process groups or Windows Job Objects",
        ))
    }
}

#[cfg(unix)]
fn spawn_unix(command: &mut Command) -> io::Result<ManagedBashChild> {
    use super::{drain_group, observe_without_reaping, signal_owned_group, GroupLeader};
    command.process_group(0).kill_on_drop(true);
    let mut child = command.spawn()?;
    let pid = child
        .id()
        .ok_or_else(|| io::Error::other("initial exec has no PID"))?;
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();
    let stdin = child.stdin.take();
    // Construct the fallback before spawning its supervisor: even cancellation
    // before the supervisor's first poll retains the exact unreaped leader.
    let (disposition, mut decision) = watch::channel(Disposition::Pending);
    struct InitialLeader {
        leader: GroupLeader,
        disposition: watch::Receiver<Disposition>,
    }
    impl Drop for InitialLeader {
        fn drop(&mut self) {
            // Delivery can commit before this supervisor is ever polled.
            if *self.disposition.borrow() == Disposition::Delivered {
                self.leader.kill_group_on_drop = false;
            }
        }
    }
    let mut owner = InitialLeader {
        leader: GroupLeader {
            child,
            pid,
            kill_group_on_drop: true,
        },
        disposition: decision.clone(),
    };
    let (observed_tx, observed) = watch::channel(None);
    let (kill, mut requested) = watch::channel(false);
    let (result_tx, result) = watch::channel(None);
    let initial = InitialExecHandle {
        disposition,
        observed,
    };
    tokio::spawn(async move {
        let outcome = async {
            let mut exited = false;
            let disposition = loop {
                let disposition = *decision.borrow();
                if disposition != Disposition::Pending {
                    break disposition;
                }
                if *requested.borrow() {
                    break Disposition::Cancelled;
                }
                tokio::select! {
                    status = observe_without_reaping(pid), if !exited => {
                        observed_tx.send_replace(Some(status?));
                        exited = true;
                    }
                    _ = decision.changed() => {},
                    _ = requested.changed() => {},
                }
            };
            if disposition == Disposition::Cancelled {
                // No wait/try_wait has released the PID/PGID anchor, including
                // when the leader exited before its initial result was ready.
                signal_owned_group(pid).await?;
                let status = owner.leader.child.wait().await?;
                drain_group(pid).await?;
                Ok(status)
            } else {
                // Delivered handles keep ordinary direct-child behavior. In
                // particular, natural leader exit must not kill descendants.
                owner.leader.kill_group_on_drop = false;
                if *requested.borrow() {
                    let _ = owner.leader.child.start_kill();
                }
                tokio::select! {
                    status = owner.leader.child.wait() => status,
                    _ = requested.changed() => {
                        let _ = owner.leader.child.start_kill();
                        owner.leader.child.wait().await
                    }
                }
            }
        }
        .await
        .map_err(|error: io::Error| error.to_string());
        result_tx.send_replace(Some(outcome));
    });
    Ok(ManagedBashChild::Owned {
        stdout,
        stderr,
        stdin,
        kill,
        result,
        initial: Some(initial),
    })
}

#[cfg(windows)]
fn spawn_windows(command: &mut Command) -> io::Result<ManagedBashChild> {
    use super::windows::{stop_and_reap, WindowsJob};
    use std::sync::Arc;
    use windows_sys::Win32::System::Threading::{CREATE_NO_WINDOW, CREATE_SUSPENDED};

    struct InitialJob {
        child: Option<tokio::process::Child>,
        job: Arc<WindowsJob>,
        disposition: watch::Receiver<Disposition>,
    }
    impl Drop for InitialJob {
        fn drop(&mut self) {
            if *self.disposition.borrow() != Disposition::Delivered {
                let _ = self.job.terminate();
            }
        }
    }
    let job = Arc::new(WindowsJob::new_kill_on_close()?);
    command
        .creation_flags(CREATE_SUSPENDED | CREATE_NO_WINDOW)
        .kill_on_drop(true);
    let mut child = command.spawn()?;
    if let Err(error) = job.assign_and_resume(&child) {
        // No uncontained fallback, including nested-job admission failures.
        tokio::spawn(async move {
            let _ = stop_and_reap(child, &job).await;
        });
        return Err(error);
    }
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();
    let stdin = child.stdin.take();
    let (disposition, mut decision) = watch::channel(Disposition::Pending);
    let (observed_tx, observed) = watch::channel(None);
    let (kill, mut requested) = watch::channel(false);
    let (result_tx, result) = watch::channel(None);
    let initial = InitialExecHandle {
        disposition,
        observed,
        job: job.clone(),
    };
    let mut owner = InitialJob {
        child: Some(child),
        job,
        disposition: decision.clone(),
    };
    tokio::spawn(async move {
        let outcome = async {
            let mut exited = false;
            let disposition = loop {
                let disposition = *decision.borrow();
                if disposition != Disposition::Pending {
                    break disposition;
                }
                if *requested.borrow() {
                    break Disposition::Cancelled;
                }
                tokio::select! {
                    status = owner.child.as_mut().unwrap().wait(), if !exited => {
                        observed_tx.send_replace(Some(status?));
                        exited = true;
                    }
                    _ = decision.changed() => {},
                    _ = requested.changed() => {},
                }
            };
            if disposition == Disposition::Cancelled {
                stop_and_reap(owner.child.take().unwrap(), &owner.job).await
            } else {
                // deliver() already disarmed kill-on-close synchronously.
                // Job association remains, but it no longer ends descendants.
                let child = owner.child.as_mut().unwrap();
                if *requested.borrow() {
                    let _ = child.start_kill();
                }
                tokio::select! {
                    status = child.wait() => status,
                    _ = requested.changed() => { let _ = child.start_kill(); child.wait().await }
                }
            }
        }
        .await
        .map_err(|error: io::Error| error.to_string());
        result_tx.send_replace(Some(outcome));
    });
    Ok(ManagedBashChild::Owned {
        stdout,
        stderr,
        stdin,
        kill,
        result,
        initial: Some(initial),
    })
}
