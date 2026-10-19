//! A conforming consumer may treat the opt-in stdout pipe as frames only.
//! No apply frame, user confirmation, identity, provider or credentials are used.
use std::{
    io::{BufRead, BufReader, Write},
    process::{Child, Command, Stdio},
    sync::mpsc,
    time::Duration,
};

const PREFIX: &str = "BAMBOO_SUPERVISOR_HOST_V1 ";

struct OwnedChild(Child);
impl Drop for OwnedChild {
    fn drop(&mut self) {
        drop(self.0.stdin.take());
        // This guard owns exactly the child it just spawned, including failures.
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn assert_native_stdout_is_frames_only(block_file_logging: bool) {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("data");
    std::fs::create_dir(&home).unwrap();
    if block_file_logging {
        std::fs::write(home.join("logs"), b"own test fixture blocks directory creation").unwrap();
    }
    let mut child = OwnedChild(
        Command::new(env!("CARGO_BIN_EXE_bamboo"))
            .args([
                "serve",
                "--supervisor-scope-native-confirmation",
                "--bind",
                "127.0.0.1",
                "--port",
                "0",
                "--workers",
                "1",
                "--data-dir",
            ])
            .arg(&home)
            .env("BAMBOO_DATA_DIR", &home)
            .env("BAMBOO_JIANDU_DATA_DIR", root.path().join("jiandu"))
            .env("RUST_LOG", "info")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let stdout = child.0.stdout.take().unwrap();
    let (send, receive) = mpsc::channel();
    std::thread::spawn(move || {
        let mut input = BufReader::new(stdout);
        for _ in 0..2 {
            let mut line = String::new();
            let result = input.read_line(&mut line).map(|_| line);
            if send.send(result).is_err() {
                return;
            }
        }
    });
    let next_frame = || {
        let line = receive
            .recv_timeout(Duration::from_secs(20))
            .expect("bounded owned-child stdout read")
            .unwrap();
        assert!(line.len() <= 16384, "native frame must remain bounded");
        let payload = line
            .strip_prefix(PREFIX)
            .unwrap_or_else(|| panic!("unframed native stdout: {line:?}"));
        serde_json::from_str::<serde_json::Value>(payload).unwrap()
    };
    let hello = next_frame();
    assert_eq!(hello["kind"], "hello");
    let ack = serde_json::json!({
        "kind": "hello_ack",
        "version": 1,
        "launch_id": hello["launch_id"],
    });
    let stdin = child.0.stdin.as_mut().unwrap();
    writeln!(stdin, "{PREFIX}{ack}").unwrap();
    stdin.flush().unwrap();
    let ready = next_frame();
    assert_eq!(ready["kind"], "ready");
    assert_eq!(ready["launch_id"], hello["launch_id"]);
    assert_eq!(ready["transport_ready"], true);
}

#[test]
fn native_stdio_keeps_startup_banner_and_tracing_off_stdout() {
    assert_native_stdout_is_frames_only(false);
}

#[test]
fn native_stdio_file_logging_failure_still_keeps_stdout_framed() {
    assert_native_stdout_is_frames_only(true);
}
