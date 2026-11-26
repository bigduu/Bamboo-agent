use std::time::Duration;

#[cfg(any(target_os = "macos", target_os = "linux"))]
pub(super) fn time() -> Option<Duration> {
    let mut value = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: value is writable storage for the operating system's clock result.
    if unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut value) } != 0 {
        return None;
    }
    Some(Duration::new(
        u64::try_from(value.tv_sec).ok()?,
        u32::try_from(value.tv_nsec).ok()?,
    ))
}

#[cfg(windows)]
pub(super) fn time() -> Option<Duration> {
    use windows_sys::Win32::Foundation::FILETIME;
    use windows_sys::Win32::System::Threading::{GetCurrentThread, GetThreadTimes};

    let mut creation = FILETIME::default();
    let mut exit = FILETIME::default();
    let mut kernel = FILETIME::default();
    let mut user = FILETIME::default();
    // SAFETY: the pseudo-handle needs no ownership transfer, and each output
    // is backed by its own writable FILETIME.
    if unsafe {
        GetThreadTimes(
            GetCurrentThread(),
            &mut creation,
            &mut exit,
            &mut kernel,
            &mut user,
        )
    } == 0
    {
        return None;
    }
    let ticks =
        |value: FILETIME| (u64::from(value.dwHighDateTime) << 32) | u64::from(value.dwLowDateTime);
    let total_ticks = ticks(kernel).checked_add(ticks(user))?;
    Some(Duration::from_nanos(total_ticks.checked_mul(100)?))
}

#[cfg(not(any(target_os = "macos", target_os = "linux", windows)))]
pub(super) fn time() -> Option<Duration> {
    None
}
