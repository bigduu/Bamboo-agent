//! A linker regression must remain observable as a failed assertion, not abort.
use std::process::{Command, Stdio};

const PROBE_ENV: &str = "BAMBOO_SERVER_ASSERTION_UNWIND_PROBE";
const TEST: &str = "test_unwinding::failed_assertion_retains_backtrace_and_harness_exit";

#[test]
fn failed_assertion_retains_backtrace_and_harness_exit() {
    if std::env::var(PROBE_ENV).as_deref() == Ok("1") {
        let observed_arguments = std::env::args_os().count();
        assert_eq!(
            observed_arguments,
            usize::MAX,
            "macOS assertion unwind probe"
        );
    }
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", TEST, "--nocapture", "--test-threads=1"])
        .env(PROBE_ENV, "1")
        .env("RUST_BACKTRACE", "1")
        .stdin(Stdio::null())
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(101), "{stderr}");
    assert!(stderr.contains("macOS assertion unwind probe"), "{stderr}");
    assert!(stderr.contains("stack backtrace:"), "{stderr}");
    assert!(stderr.contains("test_unwinding.rs:"), "{stderr}");
    assert!(!stderr.contains("failed to initiate panic"), "{stderr}");
}
