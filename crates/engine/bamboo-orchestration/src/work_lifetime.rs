//! A managed Work owns its execution lifetime; a generic Child default is not
//! an implicit Work quota. The Host must prove the current Native execution.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChildTotalLimit {
    Default(i64),
    Explicit(i64),
}

impl ChildTotalLimit {
    /// Explicit owner limits remain effective even when equal to the default.
    /// Missing/stale/remote execution evidence retains ordinary Child policy.
    pub fn enforced_seconds(self, current_native_work: bool) -> Option<i64> {
        match self {
            Self::Default(_) if current_native_work => None,
            Self::Default(seconds) | Self::Explicit(seconds) => Some(seconds),
        }
    }
}
