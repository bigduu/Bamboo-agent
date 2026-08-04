//! Process-owner guards shared by the desktop sidecar and local sub-agent workers.
//!
//! This is deliberately an OS lifecycle primitive, not a child-session state
//! machine. Durable parent/child `lost` reconciliation remains owned by the
//! runtime; the guard only ensures a local subprocess cannot survive the Bamboo
//! process that physically spawned it.

fn owner_lost_from_observation(
    initial_parent: u32,
    current_parent: u32,
    expected_direct_parent: Option<u32>,
    watched_process_exists: bool,
) -> bool {
    expected_direct_parent.is_some_and(|owner_pid| initial_parent != owner_pid)
        || current_parent != initial_parent
        || current_parent <= 1
        || !watched_process_exists
}

/// Exit this process when `owner_pid` terminates, including abrupt termination
/// paths that cannot drop a `tokio::process::Child` handle.
///
/// `owner_instance_id` and `owner_session_id` are diagnostic identities only;
/// a warm worker may serve later sessions, so they never mutate durable child
/// lifecycle state.
#[cfg(unix)]
fn spawn_owner_guard(
    owner_pid: u32,
    component: &'static str,
    owner_instance_id: Option<String>,
    owner_session_id: Option<String>,
    require_direct_parent: bool,
) {
    let guard_started = std::time::Instant::now();
    std::thread::spawn(move || {
        let initial_parent = unsafe { libc::getppid() } as u32;
        loop {
            let current_parent = unsafe { libc::getppid() } as u32;
            // Signal 0 probes existence without delivering a signal. EPERM means
            // the process exists but is not signalable; only ESRCH is owner loss.
            let owner_exists = {
                let result = unsafe { libc::kill(owner_pid as libc::pid_t, 0) };
                result == 0 || std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
            };
            let expected_direct_parent = require_direct_parent.then_some(owner_pid);
            if owner_lost_from_observation(
                initial_parent,
                current_parent,
                expected_direct_parent,
                owner_exists,
            ) {
                tracing::warn!(
                    component,
                    owner_pid,
                    owner_instance_id = owner_instance_id.as_deref().unwrap_or("unknown"),
                    owner_session_id = owner_session_id.as_deref().unwrap_or("none"),
                    worker_age_ms = guard_started.elapsed().as_millis() as u64,
                    shutdown_reason = "owner_lost",
                    "owned Bamboo subprocess detected owner loss"
                );
                std::process::exit(0);
            }
            std::thread::sleep(std::time::Duration::from_millis(300));
        }
    });
}

#[cfg(windows)]
fn spawn_owner_guard(
    owner_pid: u32,
    component: &'static str,
    owner_instance_id: Option<String>,
    owner_session_id: Option<String>,
    _require_direct_parent: bool,
) {
    use windows_sys::Win32::Foundation::CloseHandle;
    use windows_sys::Win32::System::Threading::{
        OpenProcess, WaitForSingleObject, PROCESS_SYNCHRONIZE,
    };

    let guard_started = std::time::Instant::now();
    std::thread::spawn(move || unsafe {
        // Holding the handle pins the exact process object, so PID reuse cannot
        // make a replacement process look like the original owner.
        let handle = OpenProcess(PROCESS_SYNCHRONIZE, 0, owner_pid);
        if !handle.is_null() {
            WaitForSingleObject(handle, u32::MAX);
            let _ = CloseHandle(handle);
        }
        tracing::warn!(
            component,
            owner_pid,
            owner_instance_id = owner_instance_id.as_deref().unwrap_or("unknown"),
            owner_session_id = owner_session_id.as_deref().unwrap_or("none"),
            worker_age_ms = guard_started.elapsed().as_millis() as u64,
            shutdown_reason = "owner_lost",
            "owned Bamboo subprocess detected owner loss"
        );
        std::process::exit(0);
    });
}

/// Watch an owner that may be an indirect ancestor (the desktop sidecar case).
pub fn spawn_orphan_guard(
    owner_pid: u32,
    component: &'static str,
    owner_instance_id: Option<String>,
    owner_session_id: Option<String>,
) {
    spawn_owner_guard(
        owner_pid,
        component,
        owner_instance_id,
        owner_session_id,
        false,
    );
}

/// Watch the process that directly spawned this subprocess.
///
/// The startup parent check closes the narrow race where the owner terminates
/// after writing the provision document but before the guard thread captures
/// its initial parent. An already-reparented worker exits immediately instead
/// of accepting init/launchd as its legitimate owner.
pub fn spawn_direct_owner_guard(
    owner_pid: u32,
    component: &'static str,
    owner_instance_id: Option<String>,
    owner_session_id: Option<String>,
) {
    spawn_owner_guard(
        owner_pid,
        component,
        owner_instance_id,
        owner_session_id,
        true,
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owner_loss_detects_reparenting_even_while_recorded_pid_still_exists() {
        assert!(owner_lost_from_observation(42, 1, None, true));
        assert!(owner_lost_from_observation(42, 99, None, true));
    }

    #[test]
    fn owner_loss_detects_disappeared_pid_and_keeps_stable_owner() {
        assert!(owner_lost_from_observation(42, 42, None, false));
        assert!(!owner_lost_from_observation(42, 42, None, true));
    }

    #[test]
    fn direct_owner_loss_detects_worker_already_reparented_before_guard_start() {
        assert!(owner_lost_from_observation(1, 1, Some(42), true));
        assert!(!owner_lost_from_observation(42, 42, Some(42), true));
    }
}
