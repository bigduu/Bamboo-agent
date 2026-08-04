//! Process-owner guards shared by the desktop sidecar and local sub-agent workers.
//!
//! This is deliberately an OS lifecycle primitive, not a child-session state
//! machine. Durable parent/child `lost` reconciliation remains owned by the
//! runtime; the guard only ensures a local subprocess cannot survive the Bamboo
//! process that physically spawned it.

#[cfg(any(unix, test))]
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

#[cfg(any(windows, test))]
fn owner_process_identity_matches(
    require_exact_identity: bool,
    expected_start_id: Option<u64>,
    observed_start_id: Option<u64>,
) -> bool {
    !require_exact_identity
        || matches!(
            (expected_start_id, observed_start_id),
            (Some(expected), Some(observed)) if expected == observed
        )
}

#[cfg(windows)]
unsafe fn windows_process_start_id(handle: windows_sys::Win32::Foundation::HANDLE) -> Option<u64> {
    use windows_sys::Win32::Foundation::FILETIME;
    use windows_sys::Win32::System::Threading::GetProcessTimes;

    let mut creation: FILETIME = unsafe { std::mem::zeroed() };
    let mut exit: FILETIME = unsafe { std::mem::zeroed() };
    let mut kernel: FILETIME = unsafe { std::mem::zeroed() };
    let mut user: FILETIME = unsafe { std::mem::zeroed() };
    if unsafe { GetProcessTimes(handle, &mut creation, &mut exit, &mut kernel, &mut user) } == 0 {
        return None;
    }
    Some(((creation.dwHighDateTime as u64) << 32) | creation.dwLowDateTime as u64)
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
    _owner_process_start_id: Option<u64>,
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
    require_direct_parent: bool,
    owner_process_start_id: Option<u64>,
) {
    use windows_sys::Win32::Foundation::CloseHandle;
    use windows_sys::Win32::System::Threading::{
        OpenProcess, WaitForSingleObject, PROCESS_QUERY_LIMITED_INFORMATION, PROCESS_SYNCHRONIZE,
    };

    let guard_started = std::time::Instant::now();
    std::thread::spawn(move || unsafe {
        // The creation identity closes the startup window before this thread
        // opens its handle: a reused PID cannot impersonate the stamped owner.
        // Once validated, holding the handle pins that exact process object.
        let handle = OpenProcess(
            PROCESS_SYNCHRONIZE | PROCESS_QUERY_LIMITED_INFORMATION,
            0,
            owner_pid,
        );
        let observed_start_id = if handle.is_null() {
            None
        } else {
            windows_process_start_id(handle)
        };
        let identity_matches = owner_process_identity_matches(
            require_direct_parent,
            owner_process_start_id,
            observed_start_id,
        );
        if !handle.is_null() && identity_matches {
            WaitForSingleObject(handle, u32::MAX);
            let _ = CloseHandle(handle);
        } else if !handle.is_null() {
            let _ = CloseHandle(handle);
        }
        tracing::warn!(
            component,
            owner_pid,
            owner_instance_id = owner_instance_id.as_deref().unwrap_or("unknown"),
            owner_session_id = owner_session_id.as_deref().unwrap_or("none"),
            owner_process_start_id,
            observed_owner_process_start_id = observed_start_id,
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
        None,
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
    owner_process_start_id: Option<u64>,
) {
    spawn_owner_guard(
        owner_pid,
        component,
        owner_instance_id,
        owner_session_id,
        true,
        owner_process_start_id,
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

    #[test]
    fn exact_owner_identity_requires_matching_start_ids() {
        assert!(owner_process_identity_matches(true, Some(123), Some(123)));
        assert!(!owner_process_identity_matches(true, Some(123), Some(456)));
        assert!(!owner_process_identity_matches(true, Some(123), None));
        assert!(!owner_process_identity_matches(true, None, Some(123)));
        assert!(owner_process_identity_matches(false, None, None));
    }

    #[cfg(windows)]
    #[test]
    fn windows_stamped_owner_identity_matches_open_process_object() {
        use windows_sys::Win32::Foundation::CloseHandle;
        use windows_sys::Win32::System::Threading::{
            OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION, PROCESS_SYNCHRONIZE,
        };

        let owner = bamboo_subagent::provision::WorkerOwner::for_current_process(
            "windows-owner-test".to_string(),
            None,
        );
        let handle = unsafe {
            OpenProcess(
                PROCESS_SYNCHRONIZE | PROCESS_QUERY_LIMITED_INFORMATION,
                0,
                owner.process_id,
            )
        };
        assert!(!handle.is_null());
        let observed_start_id = unsafe { windows_process_start_id(handle) };
        assert!(owner_process_identity_matches(
            true,
            owner.process_start_id,
            observed_start_id
        ));
        let _ = unsafe { CloseHandle(handle) };
    }
}
