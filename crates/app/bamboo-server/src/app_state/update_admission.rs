use std::{io, sync::Arc};

use tokio::sync::{Mutex, MutexGuard, OwnedMutexGuard};

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
    /// Let a periodic producer skip its tick while admission is held.
    pub(crate) fn try_enter(&self) -> Option<DispatchPermit<'_>> {
        let guard = self.0.try_lock().ok()?;
        (*guard != DispatchState::Closed).then_some(DispatchPermit(guard))
    }

    pub(crate) async fn record_dispatch(&self) -> io::Result<()> {
        self.enter().await?.record_dispatch();
        Ok(())
    }

    pub(crate) async fn enter(&self) -> io::Result<DispatchPermit<'_>> {
        let state = self.0.lock().await;
        if *state == DispatchState::Closed {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "Host dispatch is closed for a planned update",
            ));
        }
        Ok(DispatchPermit(state))
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

pub(crate) struct DispatchPermit<'a>(MutexGuard<'a, DispatchState>);

impl DispatchPermit<'_> {
    pub(crate) fn record_dispatch(&mut self) {
        *self.0 = DispatchState::Dispatched;
    }
}

pub(crate) struct DispatchHold(OwnedMutexGuard<DispatchState>);

impl DispatchHold {
    pub(crate) fn close(mut self) {
        *self.0 = DispatchState::Closed;
    }
}
