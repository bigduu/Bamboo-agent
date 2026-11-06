//! Exercise the actual removal scope in its own process: a deliberate fork
//! must not inherit unrelated concurrently running test owners.
use super::*;
use std::cell::RefCell;
use std::io::Read;
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::rc::Rc;

const CASE: &str = "browser::profile::removal_fork_tests::profile_removal_releases_its_lock_while_inherited_fork_is_alive";
const REENTRY: &str = "BAMBOO_PROFILE_REMOVAL_FORK_REPRO";

struct ForkHolder(libc::pid_t);
impl ForkHolder {
    fn start() -> Self {
        let mut descriptors = [-1; 2];
        assert_eq!(unsafe { libc::pipe(descriptors.as_mut_ptr()) }, 0);
        let mut reader = unsafe { std::fs::File::from_raw_fd(descriptors[0]) };
        let writer = unsafe { std::fs::File::from_raw_fd(descriptors[1]) };
        let pid = unsafe { libc::fork() };
        assert!(pid >= 0);
        if pid == 0 {
            // Only async-signal-safe syscalls in the post-fork child. The
            // owning parent always reaps this exact, otherwise idle helper.
            unsafe {
                libc::close(reader.as_raw_fd());
                let ready = [1u8];
                libc::write(writer.as_raw_fd(), ready.as_ptr().cast(), 1);
                libc::close(writer.as_raw_fd());
                loop {
                    libc::pause();
                }
            }
        }
        let holder = Self(pid);
        drop(writer);
        let mut poll = libc::pollfd {
            fd: reader.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        assert_eq!(unsafe { libc::poll(&mut poll, 1, 2000) }, 1);
        let mut ready = [0];
        reader.read_exact(&mut ready).unwrap();
        assert_eq!(ready, [1]);
        holder
    }
}
impl Drop for ForkHolder {
    fn drop(&mut self) {
        // This unreaped PID belongs exclusively to this short-lived test;
        // no application or user process is signalled.
        unsafe {
            libc::kill(self.0, libc::SIGKILL);
            libc::waitpid(self.0, std::ptr::null_mut(), 0);
        }
    }
}

#[test]
fn profile_removal_releases_its_lock_while_inherited_fork_is_alive() {
    if std::env::var(REENTRY).as_deref() != Ok("child") {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", CASE, "--nocapture", "--test-threads=1"])
            .env(REENTRY, "child")
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&output.stdout);
        eprintln!("{stdout}\n{}", String::from_utf8_lossy(&output.stderr));
        assert!(
            output.status.success(),
            "isolated removal/fork contract failed"
        );
        assert!(stdout.contains("running 1 test") && stdout.contains("REMOVAL_FORK_EXERCISED"));
        return;
    }
    let mut results = Vec::new();
    for barrier_error in [false, true] {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("profiles");
        let lease = acquire(&root, "chat").unwrap();
        let path = lease.path.clone();
        std::fs::write(path.join("Cookies"), b"fixture state").unwrap();
        drop(lease);
        let lock_path = root.join(LEASES).join(format!("{}.lock", key("chat")));
        let inode = std::fs::metadata(&lock_path).unwrap().ino();
        let holder = Rc::new(RefCell::new(None));
        let capture = holder.clone();
        REMOVAL_BARRIER_PROBE.with(|slot| {
            *slot.borrow_mut() = Some((
                root.clone(),
                Box::new(move || {
                    *capture.borrow_mut() = Some(ForkHolder::start());
                }),
            ))
        });
        if barrier_error {
            std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o300)).unwrap();
        }
        let first = remove(&root, "chat");
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
        assert_eq!(
            first.is_err(),
            barrier_error,
            "original directory-barrier contract"
        );
        assert!(!path.exists());
        assert!(holder.borrow().is_some());
        assert_eq!(
            unsafe { libc::kill(holder.borrow().as_ref().unwrap().0, 0) },
            0
        );
        let retry = remove(&root, "chat");
        println!("barrier_error={barrier_error}; fork_alive=true; retry={retry:?}");
        assert_eq!(std::fs::metadata(&lock_path).unwrap().ino(), inode);
        assert_eq!(std::fs::read(&lock_path).unwrap(), RETIRED);
        results.push(retry.is_ok());
        drop(holder);
        remove(&root, "chat").expect("helper exit is the unchanged positive control");
        assert!(matches!(acquire(&root, "chat"), Err(BrowserError::NotOpen)));
    }
    assert!(results.iter().all(|ok| *ok), "the finished removal owner must release its own lock even while a fork retains the descriptor: {results:?}");
    println!("REMOVAL_FORK_EXERCISED");
}
