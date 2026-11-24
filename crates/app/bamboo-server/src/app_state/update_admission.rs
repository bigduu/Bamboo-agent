use std::{io, sync::Arc};

use tokio::sync::{Mutex, OwnedMutexGuard};

/// Close a live route whose detached work has no process replacement
/// certificate. Once it dispatches, the process cannot prove its tails ended.
#[derive(Clone, Default)]
pub(crate) struct UntrackedUpdateAdmission(Arc<Mutex<DispatchState>>);

#[derive(Clone, Copy, Default, PartialEq, Eq)]
enum DispatchState {
    #[default]
    Unused,
    Dispatched,
    Closed,
}

impl UntrackedUpdateAdmission {
    pub(crate) async fn record_dispatch(&self) -> io::Result<()> {
        let mut state = self.0.lock().await;
        if *state == DispatchState::Closed {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "Host dispatch is closed for a planned update",
            ));
        }
        *state = DispatchState::Dispatched;
        Ok(())
    }

    pub(crate) fn try_hold(
        &self,
        busy_message: &str,
        unsupported_message: &str,
    ) -> io::Result<DispatchHold> {
        let guard = self
            .0
            .clone()
            .try_lock_owned()
            .map_err(|_| io::Error::new(io::ErrorKind::WouldBlock, busy_message))?;
        if *guard != DispatchState::Unused {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                unsupported_message,
            ));
        }
        Ok(DispatchHold(guard))
    }
}

pub(crate) struct DispatchHold(OwnedMutexGuard<DispatchState>);

impl DispatchHold {
    pub(crate) fn close(mut self) {
        *self.0 = DispatchState::Closed;
    }
}
