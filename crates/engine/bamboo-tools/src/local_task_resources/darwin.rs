//! Read-only Darwin group observations; a kill request alone is not a receipt.
use std::{io, mem::size_of};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Process {
    pid: u32,
    group: u32,
    status: u32,
    started: (u64, u64),
}

trait ProcessTable {
    fn members(&self, group: u32) -> io::Result<Vec<u32>>;
    fn process(&self, pid: u32) -> io::Result<Process>;
}
struct Libproc;

const PID_WINDOW: usize = 20;

fn decode_pid_list(
    pids: &[libc::pid_t; PID_WINDOW],
    count: libc::c_int,
    error: libc::c_int,
) -> io::Result<Vec<u32>> {
    // libproc masks a syscall error as zero, so errno is captured immediately
    // after clearing it before the call. Never turn a failed list into empty.
    if error != 0 || count < 0 {
        return Err(io::Error::from_raw_os_error(if error == 0 {
            libc::EIO
        } else {
            error
        }));
    }
    let count = count as usize;
    if count > PID_WINDOW {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Darwin process-group list exceeded its buffer",
        ));
    }
    if count == PID_WINDOW {
        return Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            "Darwin process-group observation window is full",
        ));
    }
    let pids = &pids[..count];
    if pids.iter().any(|pid| *pid <= 0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Darwin process-group list contains an invalid PID",
        ));
    }
    let mut pids: Vec<_> = pids.iter().map(|pid| *pid as u32).collect();
    pids.sort_unstable();
    if pids.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Darwin process-group list contains a duplicate PID",
        ));
    }
    Ok(pids)
}

impl ProcessTable for Libproc {
    fn members(&self, group: u32) -> io::Result<Vec<u32>> {
        // XNU bounds its internal list buffer by nprocs + 20 before locking
        // the process lists. A larger caller buffer can hide truncation. Keep
        // this window at 20, and never certify a full window as complete.
        // Apple XNU f6217f89: bsd/kern/proc_info.c::proc_listpids.
        // A large group is retried by the existing bounded drain while its
        // orphan zombies are reaped; a full window is not a stop certificate.
        let mut pids = [0 as libc::pid_t; PID_WINDOW];
        // SAFETY: this buffer holds exactly the supplied writable byte count.
        // The libproc pgrp wrapper returns a PID count, preserving errno on
        // failure. Its locked scan includes both allproc (including SIDL) and
        // zombproc; proc_pidinfo below verifies each returned identity/state.
        let (count, error) = unsafe {
            *libc::__error() = 0;
            let count = libc::proc_listpgrppids(
                group as libc::pid_t,
                pids.as_mut_ptr().cast(),
                std::mem::size_of_val(&pids) as libc::c_int,
            );
            (count, *libc::__error())
        };
        decode_pid_list(&pids, count, error)
    }

    fn process(&self, pid: u32) -> io::Result<Process> {
        // SAFETY: proc_bsdinfo is libc's typed Darwin ABI. arg=1 includes a
        // zombie; arg=0 would report ESRCH for the held WNOWAIT leader.
        let mut info: libc::proc_bsdinfo = unsafe { std::mem::zeroed() };
        let (count, error) = unsafe {
            *libc::__error() = 0;
            let count = libc::proc_pidinfo(
                pid as libc::pid_t,
                libc::PROC_PIDTBSDINFO,
                1,
                (&mut info as *mut libc::proc_bsdinfo).cast(),
                size_of::<libc::proc_bsdinfo>() as libc::c_int,
            );
            (count, *libc::__error())
        };
        if count != size_of::<libc::proc_bsdinfo>() as libc::c_int {
            return Err(if error != 0 {
                io::Error::from_raw_os_error(error)
            } else {
                io::Error::other("Darwin process identity observation was incomplete")
            });
        }
        Ok(Process {
            pid: info.pbi_pid,
            group: info.pbi_pgid,
            status: info.pbi_status,
            started: (info.pbi_start_tvsec, info.pbi_start_tvusec),
        })
    }
}

/// True means live members or incomplete evidence; only false certifies stop.
pub(super) fn has_running_members(group: u32, require_held_leader: bool) -> io::Result<bool> {
    observe_group(&Libproc, group, require_held_leader)
}

fn observe_group(
    table: &impl ProcessTable,
    group: u32,
    require_held_leader: bool,
) -> io::Result<bool> {
    'observation: for _ in 0..3 {
        let members = match table.members(group) {
            Ok(members) => members,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(true),
            Err(error) => return Err(observation_error(group, "initial PID list", error)),
        };
        if require_held_leader && !members.contains(&group) {
            return Err(io::Error::other(
                "Darwin owned group observation lost its held leader",
            ));
        }
        let mut identities = Vec::with_capacity(members.len());
        for pid in &members {
            let info = match table.process(*pid) {
                Ok(info) => info,
                Err(error) if error.raw_os_error() == Some(libc::ESRCH) => {
                    // A disappearing member is not proof. Start a new complete
                    // observation; only a stable later list may certify stop.
                    continue 'observation;
                }
                Err(error) => {
                    return Err(observation_error(
                        group,
                        &format!("initial process {pid} identity"),
                        error,
                    ))
                }
            };
            if info.pid != *pid || info.group != group || info.started.0 == 0 {
                return Err(io::Error::other(format!(
                    "Darwin owned process {pid} identity could not be verified for group {group}: {info:?}",
                )));
            }
            if info.status != libc::SZOMB {
                return Ok(true);
            }
            identities.push(info);
        }
        // Once every member is a zombie, none can fork. A second complete list
        // fences a descendant added while earlier members were being read.
        match table.members(group) {
            Ok(current) if current == members => {}
            Ok(_) => continue,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(true),
            Err(error) => return Err(observation_error(group, "confirming PID list", error)),
        }
        // Refuse PID reuse or changed membership/identity during observation.
        for old in identities {
            let current = match table.process(old.pid) {
                Ok(info) => info,
                Err(error) if error.raw_os_error() == Some(libc::ESRCH) => continue 'observation,
                Err(error) => {
                    return Err(observation_error(
                        group,
                        &format!("confirming process {} identity", old.pid),
                        error,
                    ))
                }
            };
            if current != old {
                return Err(io::Error::other(format!(
                    "Darwin owned process {} identity changed during observation for group {group}: {old:?} -> {current:?}",
                    old.pid,
                )));
            }
        }
        return Ok(false);
    }
    // Unstable visibility is pending evidence, never proof that a group stopped.
    Ok(true)
}

fn observation_error(group: u32, phase: &str, error: io::Error) -> io::Error {
    io::Error::new(
        error.kind(),
        format!("Darwin owned process group {group} {phase}: {error}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{cell::RefCell, collections::VecDeque};

    struct Table {
        lists: RefCell<VecDeque<io::Result<Vec<u32>>>>,
        processes: RefCell<VecDeque<io::Result<Process>>>,
    }
    impl ProcessTable for Table {
        fn members(&self, _: u32) -> io::Result<Vec<u32>> {
            self.lists
                .borrow_mut()
                .pop_front()
                .expect("unexpected group read")
        }
        fn process(&self, _: u32) -> io::Result<Process> {
            self.processes
                .borrow_mut()
                .pop_front()
                .expect("unexpected identity read")
        }
    }
    fn zombie(pid: u32) -> Process {
        Process {
            pid,
            group: 7,
            status: libc::SZOMB,
            started: (123, pid as u64),
        }
    }
    fn table(lists: Vec<Vec<u32>>, processes: Vec<io::Result<Process>>) -> Table {
        Table {
            lists: RefCell::new(lists.into_iter().map(Ok).collect()),
            processes: RefCell::new(processes.into()),
        }
    }

    #[test]
    fn raw_pid_list_short_reply_is_complete_and_sorted() {
        let mut raw = [0; PID_WINDOW];
        assert!(decode_pid_list(&raw, 0, 0).unwrap().is_empty());
        raw[0] = 8;
        raw[1] = 7;
        assert_eq!(decode_pid_list(&raw, 2, 0).unwrap(), vec![7, 8]);
        for (index, pid) in raw.iter_mut().enumerate() {
            *pid = index as libc::pid_t + 1;
        }
        assert_eq!(decode_pid_list(&raw, 19, 0).unwrap().len(), 19);
    }

    #[test]
    fn raw_pid_list_full_or_oversized_reply_never_proves_completeness() {
        let raw = std::array::from_fn(|index| index as libc::pid_t + 1);
        assert_eq!(
            decode_pid_list(&raw, PID_WINDOW as libc::c_int, 0)
                .unwrap_err()
                .kind(),
            io::ErrorKind::WouldBlock
        );
        assert_eq!(
            decode_pid_list(&raw, PID_WINDOW as libc::c_int + 1, 0)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn raw_pid_list_errno_cannot_become_an_empty_or_partial_success() {
        let raw = [7; PID_WINDOW];
        for count in [-1, 0, 1, PID_WINDOW as libc::c_int] {
            for errno in [libc::EPERM, libc::EIO] {
                assert_eq!(
                    decode_pid_list(&raw, count, errno)
                        .unwrap_err()
                        .raw_os_error(),
                    Some(errno)
                );
            }
        }
        assert_eq!(
            decode_pid_list(&raw, -1, 0).unwrap_err().raw_os_error(),
            Some(libc::EIO)
        );
    }

    #[test]
    fn raw_pid_list_invalid_or_duplicate_pid_is_not_a_snapshot() {
        for invalid in [0, -1] {
            let mut raw = [0; PID_WINDOW];
            raw[0] = invalid;
            assert_eq!(
                decode_pid_list(&raw, 1, 0).unwrap_err().kind(),
                io::ErrorKind::InvalidData
            );
        }
        let raw = [7; PID_WINDOW];
        assert_eq!(
            decode_pid_list(&raw, 2, 0).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn complete_zombie_group_and_empty_reaped_group_are_stopped() {
        let t = table(
            vec![vec![7, 8], vec![7, 8]],
            vec![Ok(zombie(7)), Ok(zombie(8)), Ok(zombie(7)), Ok(zombie(8))],
        );
        assert!(!observe_group(&t, 7, true).unwrap());
        assert!(!observe_group(&table(vec![vec![], vec![]], vec![]), 7, false).unwrap());
    }

    #[test]
    fn live_or_unreadable_process_never_proves_stopped() {
        let live = Process {
            status: libc::SRUN,
            ..zombie(8)
        };
        assert!(observe_group(
            &table(vec![vec![7, 8]], vec![Ok(zombie(7)), Ok(live)]),
            7,
            true
        )
        .unwrap());
        for errno in [libc::EPERM, libc::EIO] {
            assert!(observe_group(
                &table(
                    vec![vec![7, 8]],
                    vec![Ok(zombie(7)), Err(io::Error::from_raw_os_error(errno))]
                ),
                7,
                true
            )
            .is_err());
        }
        let t = Table {
            lists: RefCell::new([Err(io::Error::from_raw_os_error(libc::EPERM))].into()),
            processes: RefCell::new(VecDeque::new()),
        };
        assert!(observe_group(&t, 7, true).is_err());
    }

    #[test]
    fn confirming_identity_error_or_change_never_proves_stopped() {
        for errno in [libc::EPERM, libc::EIO] {
            let t = table(
                vec![vec![7], vec![7]],
                vec![Ok(zombie(7)), Err(io::Error::from_raw_os_error(errno))],
            );
            assert!(observe_group(&t, 7, true).is_err());
        }
        let t = table(
            vec![vec![7], vec![7]],
            vec![
                Ok(zombie(7)),
                Ok(Process {
                    status: libc::SRUN,
                    ..zombie(7)
                }),
            ],
        );
        assert!(observe_group(&t, 7, true).is_err());
    }

    #[test]
    fn missing_leader_wrong_group_and_reused_pid_deny_proof() {
        assert!(observe_group(&table(vec![vec![]], vec![]), 7, true).is_err());
        for bad in [
            Process {
                group: 9,
                ..zombie(7)
            },
            Process {
                pid: 9,
                ..zombie(7)
            },
            Process {
                started: (0, 0),
                ..zombie(7)
            },
        ] {
            assert!(observe_group(&table(vec![vec![7]], vec![Ok(bad)]), 7, true).is_err());
        }
        let reused = Process {
            started: (124, 7),
            ..zombie(7)
        };
        assert!(observe_group(
            &table(vec![vec![7], vec![7]], vec![Ok(zombie(7)), Ok(reused)]),
            7,
            true
        )
        .is_err());
    }

    #[test]
    fn disappeared_member_needs_a_new_complete_observation() {
        let t = table(
            vec![vec![7, 8], vec![7], vec![7]],
            vec![
                Ok(zombie(7)),
                Err(io::Error::from_raw_os_error(libc::ESRCH)),
                Ok(zombie(7)),
                Ok(zombie(7)),
            ],
        );
        assert!(!observe_group(&t, 7, true).unwrap());
        let t = table(
            vec![vec![7], vec![7], vec![7]],
            vec![
                Err(io::Error::from_raw_os_error(libc::ESRCH)),
                Err(io::Error::from_raw_os_error(libc::ESRCH)),
                Err(io::Error::from_raw_os_error(libc::ESRCH)),
            ],
        );
        assert!(observe_group(&t, 7, true).unwrap());
    }

    #[test]
    fn full_observation_window_never_proves_stopped() {
        let t = Table {
            lists: RefCell::new(
                [Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "full window",
                ))]
                .into(),
            ),
            processes: RefCell::new(VecDeque::new()),
        };
        assert!(observe_group(&t, 7, true).unwrap());
        let t = Table {
            lists: RefCell::new(
                [
                    Ok(vec![7]),
                    Err(io::Error::new(
                        io::ErrorKind::WouldBlock,
                        "full confirmation window",
                    )),
                ]
                .into(),
            ),
            processes: RefCell::new([Ok(zombie(7))].into()),
        };
        assert!(observe_group(&t, 7, true).unwrap());
        for status in [
            0,
            libc::SIDL,
            libc::SRUN,
            libc::SSLEEP,
            libc::SSTOP,
            u32::MAX,
        ] {
            let t = table(
                vec![vec![7]],
                vec![Ok(Process {
                    status,
                    ..zombie(7)
                })],
            );
            assert!(observe_group(&t, 7, true).unwrap());
        }
    }

    #[test]
    fn newly_listed_live_descendant_prevents_stale_zombie_proof() {
        let live = Process {
            status: libc::SRUN,
            ..zombie(8)
        };
        let t = table(
            vec![vec![7], vec![7, 8], vec![7, 8]],
            vec![Ok(zombie(7)), Ok(zombie(7)), Ok(live)],
        );
        assert!(observe_group(&t, 7, true).unwrap());
        let t = table(
            vec![
                vec![7],
                vec![7, 8],
                vec![7],
                vec![7, 8],
                vec![7],
                vec![7, 8],
            ],
            vec![Ok(zombie(7)), Ok(zombie(7)), Ok(zombie(7))],
        );
        assert!(observe_group(&t, 7, true).unwrap());
    }
}
