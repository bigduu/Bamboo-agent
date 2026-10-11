//! Windows process ownership. The job handle, never a PID lookup, is the stop
//! authority. The child starts suspended so no user code can escape before job
//! assignment. The stable Rust/Tokio API does not expose its primary thread,
//! so locate the unique initial thread and validate its still-owned process.
use super::{LocalTaskResources, ManagedBashChild, TrackedTask};
use std::{
    io,
    mem::size_of,
    os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle},
    process::ExitStatus,
    ptr,
    time::Duration,
};
use tokio::{
    process::{Child, Command},
    sync::watch,
};
use windows_sys::Win32::{
    Foundation::{ERROR_NO_MORE_FILES, HANDLE, INVALID_HANDLE_VALUE},
    System::{
        Diagnostics::ToolHelp::{
            CreateToolhelp32Snapshot, Thread32First, Thread32Next, TH32CS_SNAPTHREAD, THREADENTRY32,
        },
        JobObjects::{
            AssignProcessToJobObject, CreateJobObjectW, JobObjectBasicAccountingInformation,
            JobObjectExtendedLimitInformation, QueryInformationJobObject, SetInformationJobObject,
            TerminateJobObject, JOBOBJECT_BASIC_ACCOUNTING_INFORMATION,
            JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
        },
        Threading::{
            GetProcessIdOfThread, OpenThread, ResumeThread, CREATE_NO_WINDOW, CREATE_SUSPENDED,
            THREAD_QUERY_LIMITED_INFORMATION, THREAD_SUSPEND_RESUME,
        },
    },
};

pub(super) fn spawn(
    command: &mut Command,
    scope: &LocalTaskResources,
) -> io::Result<ManagedBashChild> {
    spawn_with_setup(command, scope, WindowsJob::assign_and_resume)
}

fn spawn_with_setup(
    command: &mut Command,
    scope: &LocalTaskResources,
    setup: impl FnOnce(&WindowsJob, &Child) -> io::Result<()>,
) -> io::Result<ManagedBashChild> {
    // Admission, creation, assignment and supervisor registration are atomic
    // against close_and_drain. No await or caller cancellation gap here.
    let mut state = scope
        .0
        .state
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    state.admit_task()?;
    let job = WindowsJob::new_kill_on_close()?;
    command
        .creation_flags(CREATE_SUSPENDED | CREATE_NO_WINDOW)
        .kill_on_drop(true);
    let mut child = command.spawn()?;
    if let Err(error) = setup(&job, &child) {
        let failure = format!("owned Windows Bash setup failed: {error}");
        state.failure.get_or_insert_with(|| failure.clone());
        // Even assignment failure leaves a real suspended child to reap.
        // The registry retains that cleanup; kill_on_drop alone is not proof.
        state.tasks.push(TrackedTask {
            cancellation_safe: false,
            relay_abort: None,
            task: tokio::spawn(async move {
                let cleanup = stop_and_reap(child, job).await;
                Err(match cleanup {
                    Ok(_) => failure,
                    Err(error) => format!("{failure}; cleanup failed: {error}"),
                })
            }),
        });
        return Err(error);
    }
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();
    let stdin = child.stdin.take();
    let (kill, mut requested) = watch::channel(false);
    let (result_tx, result) = watch::channel(None);
    let mut stop = scope.0.stop.subscribe();
    state.tasks.push(TrackedTask {
        cancellation_safe: false,
        relay_abort: None,
        task: tokio::spawn(async move {
            let first_wait = tokio::select! {
                result = child.wait() => Some(result),
                _ = requested.changed() => None,
                _ = stop.changed() => None,
            };
            // A leader exiting does not imply its descendants exited. Keep
            // the exact job handle and terminate/drain it on every path.
            let cleanup = stop_and_reap(child, job).await;
            let outcome = match first_wait {
                Some(Err(error)) => Err(error),
                _ => cleanup,
            }
            .map_err(|error| error.to_string());
            let receipt = outcome.as_ref().map(|_| ()).map_err(Clone::clone);
            result_tx.send_replace(Some(outcome));
            receipt
        }),
    });
    Ok(ManagedBashChild::Owned {
        stdout,
        stderr,
        stdin,
        kill,
        result,
    })
}

struct WindowsJob {
    handle: OwnedHandle,
}

impl WindowsJob {
    fn new_kill_on_close() -> io::Result<Self> {
        // SAFETY: unnamed, non-inheritable job; OwnedHandle closes it exactly
        // once. No breakaway flags are set, so descendants stay in this job.
        let raw = unsafe { CreateJobObjectW(ptr::null(), ptr::null()) };
        if raw.is_null() {
            return Err(io::Error::last_os_error());
        }
        let job = Self {
            handle: unsafe { OwnedHandle::from_raw_handle(raw.cast()) },
        };
        let mut limits = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
        limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        // SAFETY: the pointer and length describe the required initialized
        // extended-limit structure for this information class.
        if unsafe {
            SetInformationJobObject(
                job.raw(),
                JobObjectExtendedLimitInformation,
                (&limits as *const JOBOBJECT_EXTENDED_LIMIT_INFORMATION).cast(),
                size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(job)
    }

    fn raw(&self) -> HANDLE {
        self.handle.as_raw_handle().cast()
    }

    fn assign(&self, child: &Child) -> io::Result<()> {
        let process = child
            .raw_handle()
            .ok_or_else(|| io::Error::other("owned suspended process has no handle"))?;
        // SAFETY: process and job handles are retained through the call. Nested
        // job incompatibility is an error, never an uncontained fallback.
        if unsafe { AssignProcessToJobObject(self.raw(), process.cast()) } == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    fn assign_and_resume(&self, child: &Child) -> io::Result<()> {
        self.assign(child)?;
        let pid = child
            .id()
            .ok_or_else(|| io::Error::other("owned suspended process has no PID"))?;
        resume_unique_initial_thread(pid)
    }

    fn terminate(&self) -> io::Result<()> {
        // SAFETY: this is the retained per-spawn job, not a name/PID lookup.
        if unsafe { TerminateJobObject(self.raw(), 1) } == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    fn active_processes(&self) -> io::Result<u32> {
        let mut accounting = JOBOBJECT_BASIC_ACCOUNTING_INFORMATION::default();
        // SAFETY: writable initialized buffer matches the information class.
        if unsafe {
            QueryInformationJobObject(
                self.raw(),
                JobObjectBasicAccountingInformation,
                (&mut accounting as *mut JOBOBJECT_BASIC_ACCOUNTING_INFORMATION).cast(),
                size_of::<JOBOBJECT_BASIC_ACCOUNTING_INFORMATION>() as u32,
                ptr::null_mut(),
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(accounting.ActiveProcesses)
    }

    async fn wait_empty(&self) -> io::Result<()> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if self.active_processes()? == 0 {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "owned Windows Bash job did not empty",
                ));
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

async fn stop_and_reap(mut child: Child, job: WindowsJob) -> io::Result<ExitStatus> {
    let terminated = job.terminate();
    // This also covers a child whose job assignment failed. It targets the
    // actual Child handle. A failed request is not substituted for a wait.
    let _ = child.start_kill();
    let waited = child.wait().await;
    // Release our process handle before checking the job's active count.
    drop(child);
    let emptied = job.wait_empty().await;
    terminated?;
    let status = waited?;
    emptied?;
    Ok(status)
}

fn resume_unique_initial_thread(process_id: u32) -> io::Result<()> {
    // SAFETY: snapshot is owned locally and closed on all paths.
    let raw = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0) };
    if raw == INVALID_HANDLE_VALUE {
        return Err(io::Error::last_os_error());
    }
    let snapshot = unsafe { OwnedHandle::from_raw_handle(raw.cast()) };
    let mut entry = THREADENTRY32 {
        dwSize: size_of::<THREADENTRY32>() as u32,
        ..Default::default()
    };
    let mut thread_id = None;
    let mut available = unsafe { Thread32First(snapshot.as_raw_handle().cast(), &mut entry) } != 0;
    loop {
        if !available {
            let error = io::Error::last_os_error();
            if error.raw_os_error() != Some(ERROR_NO_MORE_FILES as i32) {
                return Err(error);
            }
            break;
        }
        if entry.th32OwnerProcessID == process_id {
            if thread_id.replace(entry.th32ThreadID).is_some() {
                return Err(io::Error::other(
                    "suspended owned process has more than one initial thread",
                ));
            }
        }
        entry.dwSize = size_of::<THREADENTRY32>() as u32;
        available = unsafe { Thread32Next(snapshot.as_raw_handle().cast(), &mut entry) } != 0;
    }
    let thread_id = thread_id
        .ok_or_else(|| io::Error::other("suspended owned process has no initial thread"))?;
    let raw = unsafe {
        OpenThread(
            THREAD_SUSPEND_RESUME | THREAD_QUERY_LIMITED_INFORMATION,
            0,
            thread_id,
        )
    };
    if raw.is_null() {
        return Err(io::Error::last_os_error());
    }
    let thread = unsafe { OwnedHandle::from_raw_handle(raw.cast()) };
    // Reject a stale thread ID rather than resuming an unrelated thread. The
    // Child handle keeps the process identity alive until this check finishes.
    if unsafe { GetProcessIdOfThread(thread.as_raw_handle().cast()) } != process_id {
        return Err(io::Error::other(
            "suspended thread no longer belongs to the owned process",
        ));
    }
    match unsafe { ResumeThread(thread.as_raw_handle().cast()) } {
        1 => Ok(()),
        u32::MAX => Err(io::Error::last_os_error()),
        count => Err(io::Error::other(format!(
            "unexpected owned process suspend count: {count}"
        ))),
    }
}

#[cfg(test)]
mod tests;
