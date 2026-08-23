//! Best-effort startup adjustment for the process file-descriptor limit.
//!
//! This module deliberately does not use `tracing`: the binary invokes it before
//! constructing the Tokio runtime or initializing logging, so failures must be
//! visible directly on stderr without preventing startup.

#[cfg(any(unix, test))]
use std::fmt;

#[cfg(unix)]
type LimitValue = libc::rlim_t;
#[cfg(all(test, not(unix)))]
type LimitValue = u64;

#[cfg(any(unix, test))]
const TARGET_SOFT_LIMIT: LimitValue = 65_536;

/// Raise the process `RLIMIT_NOFILE` soft limit where Unix supports it.
///
/// This is intentionally a no-op on non-Unix targets. On Unix every failure is
/// reported to stderr and ignored so Bamboo can continue with the operator's
/// existing limit.
pub(super) fn raise_nofile_limit_best_effort() {
    #[cfg(unix)]
    {
        use std::io::Write as _;

        let mut backend = UnixNofileLimit;
        if let Err(error) = try_raise_soft_limit(&mut backend, TARGET_SOFT_LIMIT) {
            // Ignore stderr write failures as well: inability to raise this
            // optional limit must never prevent Bamboo from starting.
            let _ = writeln!(
                std::io::stderr().lock(),
                "warning: failed to raise RLIMIT_NOFILE soft limit: {error}; \
                 continuing with the existing limit"
            );
        }
    }
}

#[cfg(any(unix, test))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct NofileLimit {
    soft: LimitValue,
    hard: LimitValue,
}

#[cfg(any(unix, test))]
trait NofileLimitBackend {
    type Error;

    fn get(&mut self) -> Result<NofileLimit, Self::Error>;
    fn set(&mut self, limit: NofileLimit) -> Result<(), Self::Error>;
}

#[cfg(any(unix, test))]
#[derive(Debug, PartialEq, Eq)]
enum RaiseOutcome {
    Unchanged {
        soft: LimitValue,
        hard: LimitValue,
    },
    Raised {
        previous_soft: LimitValue,
        soft: LimitValue,
        hard: LimitValue,
    },
}

#[cfg(any(unix, test))]
#[derive(Debug, PartialEq, Eq)]
enum RaiseError<E> {
    Read(E),
    Set(E),
    Verify(E),
    DidNotAdvance {
        previous_soft: LimitValue,
        requested_soft: LimitValue,
        actual_soft: LimitValue,
    },
}

#[cfg(any(unix, test))]
impl<E: fmt::Display> fmt::Display for RaiseError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Read(error) => write!(formatter, "could not read the current limit: {error}"),
            Self::Set(error) => write!(formatter, "could not set the requested limit: {error}"),
            Self::Verify(error) => {
                write!(formatter, "could not verify the updated limit: {error}")
            }
            Self::DidNotAdvance {
                previous_soft,
                requested_soft,
                actual_soft,
            } => write!(
                formatter,
                "setrlimit returned success but the soft limit did not reach the requested value \
                 (previous {previous_soft}, requested {requested_soft}, actual {actual_soft})"
            ),
        }
    }
}

/// Raise only the soft limit, never beyond the hard limit and never downward.
///
/// A second read verifies that a successful syscall actually advanced the
/// process limit. Keeping the syscall behind a backend makes all policy branches
/// testable without mutating the test runner's real process limit.
#[cfg(any(unix, test))]
fn try_raise_soft_limit<B>(
    backend: &mut B,
    target_soft: LimitValue,
) -> Result<RaiseOutcome, RaiseError<B::Error>>
where
    B: NofileLimitBackend,
{
    let current = backend.get().map_err(RaiseError::Read)?;
    let requested_soft = target_soft.min(current.hard);

    if current.soft >= requested_soft {
        return Ok(RaiseOutcome::Unchanged {
            soft: current.soft,
            hard: current.hard,
        });
    }

    backend
        .set(NofileLimit {
            soft: requested_soft,
            hard: current.hard,
        })
        .map_err(RaiseError::Set)?;

    let updated = backend.get().map_err(RaiseError::Verify)?;
    if updated.soft < requested_soft {
        return Err(RaiseError::DidNotAdvance {
            previous_soft: current.soft,
            requested_soft,
            actual_soft: updated.soft,
        });
    }

    Ok(RaiseOutcome::Raised {
        previous_soft: current.soft,
        soft: updated.soft,
        hard: updated.hard,
    })
}

#[cfg(unix)]
struct UnixNofileLimit;

#[cfg(unix)]
impl NofileLimitBackend for UnixNofileLimit {
    type Error = std::io::Error;

    fn get(&mut self) -> Result<NofileLimit, Self::Error> {
        let mut raw = std::mem::MaybeUninit::<libc::rlimit>::uninit();
        // SAFETY: `raw` points to valid writable storage and is read only after
        // `getrlimit` reports success.
        let result = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, raw.as_mut_ptr()) };
        if result != 0 {
            return Err(std::io::Error::last_os_error());
        }
        // SAFETY: a successful `getrlimit` initialized the entire `rlimit` value.
        let raw = unsafe { raw.assume_init() };
        Ok(NofileLimit {
            soft: raw.rlim_cur,
            hard: raw.rlim_max,
        })
    }

    fn set(&mut self, limit: NofileLimit) -> Result<(), Self::Error> {
        let raw = libc::rlimit {
            rlim_cur: limit.soft,
            rlim_max: limit.hard,
        };
        // SAFETY: `raw` is fully initialized and remains alive for the duration
        // of the syscall. Both values came from this platform's `rlim_t`, except
        // the soft target (65,536), which fits every supported Unix `rlim_t`.
        let result = unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &raw) };
        if result == 0 {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        try_raise_soft_limit, NofileLimit, NofileLimitBackend, RaiseError, RaiseOutcome,
        TARGET_SOFT_LIMIT,
    };
    use std::collections::VecDeque;
    use std::fmt;

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct FakeError(&'static str);

    impl fmt::Display for FakeError {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str(self.0)
        }
    }

    struct FakeBackend {
        reads: VecDeque<Result<NofileLimit, FakeError>>,
        set_result: Result<(), FakeError>,
        set_calls: Vec<NofileLimit>,
    }

    impl FakeBackend {
        fn new(reads: impl IntoIterator<Item = Result<NofileLimit, FakeError>>) -> Self {
            Self {
                reads: reads.into_iter().collect(),
                set_result: Ok(()),
                set_calls: Vec::new(),
            }
        }
    }

    impl NofileLimitBackend for FakeBackend {
        type Error = FakeError;

        fn get(&mut self) -> Result<NofileLimit, Self::Error> {
            self.reads
                .pop_front()
                .expect("test backend should provide every expected read")
        }

        fn set(&mut self, limit: NofileLimit) -> Result<(), Self::Error> {
            self.set_calls.push(limit);
            self.set_result.clone()
        }
    }

    fn limit(soft: super::LimitValue, hard: super::LimitValue) -> NofileLimit {
        NofileLimit { soft, hard }
    }

    #[test]
    fn raises_low_soft_limit_to_target_without_changing_hard_limit() {
        let mut backend = FakeBackend::new([
            Ok(limit(1_024, 131_072)),
            Ok(limit(TARGET_SOFT_LIMIT, 131_072)),
        ]);

        let outcome = try_raise_soft_limit(&mut backend, TARGET_SOFT_LIMIT).unwrap();

        assert_eq!(
            outcome,
            RaiseOutcome::Raised {
                previous_soft: 1_024,
                soft: TARGET_SOFT_LIMIT,
                hard: 131_072,
            }
        );
        assert_eq!(backend.set_calls, [limit(TARGET_SOFT_LIMIT, 131_072)]);
    }

    #[test]
    fn caps_requested_soft_limit_at_a_lower_hard_limit() {
        let mut backend = FakeBackend::new([Ok(limit(1_024, 4_096)), Ok(limit(4_096, 4_096))]);

        let outcome = try_raise_soft_limit(&mut backend, TARGET_SOFT_LIMIT).unwrap();

        assert_eq!(
            outcome,
            RaiseOutcome::Raised {
                previous_soft: 1_024,
                soft: 4_096,
                hard: 4_096,
            }
        );
        assert_eq!(backend.set_calls, [limit(4_096, 4_096)]);
    }

    #[test]
    fn leaves_an_already_sufficient_soft_limit_unchanged() {
        let mut backend = FakeBackend::new([Ok(limit(100_000, 200_000))]);

        let outcome = try_raise_soft_limit(&mut backend, TARGET_SOFT_LIMIT).unwrap();

        assert_eq!(
            outcome,
            RaiseOutcome::Unchanged {
                soft: 100_000,
                hard: 200_000,
            }
        );
        assert!(backend.set_calls.is_empty());
    }

    #[test]
    fn reports_an_initial_read_failure_without_attempting_a_write() {
        let mut backend = FakeBackend::new([Err(FakeError("get failed"))]);

        let error = try_raise_soft_limit(&mut backend, TARGET_SOFT_LIMIT).unwrap_err();

        assert_eq!(error, RaiseError::Read(FakeError("get failed")));
        assert!(backend.set_calls.is_empty());
    }

    #[test]
    fn reports_a_write_failure_and_preserves_the_requested_hard_limit() {
        let mut backend = FakeBackend::new([Ok(limit(1_024, 131_072))]);
        backend.set_result = Err(FakeError("set failed"));

        let error = try_raise_soft_limit(&mut backend, TARGET_SOFT_LIMIT).unwrap_err();

        assert_eq!(error, RaiseError::Set(FakeError("set failed")));
        assert_eq!(backend.set_calls, [limit(TARGET_SOFT_LIMIT, 131_072)]);
    }

    #[test]
    fn reports_a_verification_read_failure() {
        let mut backend =
            FakeBackend::new([Ok(limit(1_024, 131_072)), Err(FakeError("verify failed"))]);

        let error = try_raise_soft_limit(&mut backend, TARGET_SOFT_LIMIT).unwrap_err();

        assert_eq!(error, RaiseError::Verify(FakeError("verify failed")));
    }

    #[test]
    fn reports_successful_write_that_did_not_advance_the_soft_limit() {
        let mut backend = FakeBackend::new([Ok(limit(1_024, 131_072)), Ok(limit(1_024, 131_072))]);

        let error = try_raise_soft_limit(&mut backend, TARGET_SOFT_LIMIT).unwrap_err();

        assert_eq!(
            error,
            RaiseError::DidNotAdvance {
                previous_soft: 1_024,
                requested_soft: TARGET_SOFT_LIMIT,
                actual_soft: 1_024,
            }
        );
    }
}
