//! One-shot provider effects for live staged patrol submissions. Only the
//! existing checkpoint and Work receipt survive a process exit.

use std::{collections::BTreeMap, sync::Mutex, time::Duration};

use bamboo_domain::TokenUsage;
use tokio::{
    runtime::Handle,
    sync::oneshot,
    task::AbortHandle,
    time::{timeout, timeout_at, Instant},
};

use super::{
    staged::{Reconciled, Submission},
    BudgetPort, Diagnosis, DiagnosticRequest, DiagnosticStatus, OperationId, PortError,
    Reservation, Usage,
};

const MAX_CALLS: usize = 16;
const MAX_DEADLINE_MS: u64 = 86_400_000;
const ACCOUNTING_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_DIAGNOSIS_BYTES: usize = 64 * 1024;

/// Bounded provider reply projected by a protocol adapter. Two exact counts
/// enable settlement. Independently observed overruns still halt accounting
/// when the complete count or advisory output is unavailable.
pub struct ProviderReply {
    pub diagnosis: Option<String>,
    pub exact_usage: Option<TokenUsage>,
    pub observed_input_tokens: Option<u64>,
    pub observed_output_tokens: Option<u64>,
}

impl ProviderReply {
    fn status(self, request: &DiagnosticRequest) -> DiagnosticStatus {
        let input_tokens = self
            .observed_input_tokens
            .into_iter()
            .chain(self.exact_usage.map(|usage| usage.prompt_tokens))
            .max();
        let output_tokens = self
            .observed_output_tokens
            .into_iter()
            .chain(self.exact_usage.map(|usage| usage.completion_tokens))
            .max();
        if input_tokens.is_some_and(|input| input > u64::from(request.model.max_input_tokens))
            || output_tokens
                .is_some_and(|output| output > u64::from(request.model.max_output_tokens))
        {
            return DiagnosticStatus::Failed {
                usage: Usage::OverCap {
                    input_tokens,
                    output_tokens,
                },
            };
        }
        let Some(exact) = self.exact_usage.filter(|usage| {
            input_tokens == Some(usage.prompt_tokens)
                && output_tokens == Some(usage.completion_tokens)
                && usage.prompt_tokens >= u64::from(request.input.input_tokens)
                && usage.prompt_tokens.checked_add(usage.completion_tokens)
                    == Some(usage.total_tokens)
        }) else {
            return DiagnosticStatus::Failed {
                usage: Usage::Unknown,
            };
        };
        let usage = Usage::Known {
            input_tokens: exact.prompt_tokens as u32,
            output_tokens: exact.completion_tokens as u32,
        };
        match self.diagnosis.as_deref().and_then(|text| {
            if text.len() > MAX_DIAGNOSIS_BYTES {
                return None;
            }
            serde_json::from_str::<Diagnosis>(text).ok()
        }) {
            Some(diagnosis) => DiagnosticStatus::Finished { diagnosis, usage },
            None => DiagnosticStatus::Failed { usage },
        }
    }
}

/// Concrete Host I/O. The final check must obtain fresh authority and verify
/// the exact receipt. Completing may send once. Settlement may reconcile a
/// receipt even when current admission has been revoked.
#[async_trait::async_trait]
pub trait CallIo: Send + Sync {
    async fn admit(&self, reservation: &Reservation) -> Result<(), PortError>;
    async fn complete(&mut self, remaining: Duration) -> Result<ProviderReply, PortError>;
    async fn settle(&self, reservation: &Reservation, usage: Usage) -> Result<(), PortError>;
}

/// Only volatile delivery and status, with a fixed bound and no retry loop.
#[derive(Default)]
pub struct Calls {
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    entries: BTreeMap<OperationId, Entry>,
    closed: bool,
}

struct Entry {
    receipt: Reservation,
    expires: Instant,
    task: AbortHandle,
    status: CallStatus,
    checkpoint_ack: bool,
}

impl Drop for Entry {
    fn drop(&mut self) {
        self.task.abort();
    }
}

enum CallStatus {
    Receiving(oneshot::Receiver<Delivery>),
    Complete(Delivery),
}

struct Delivery {
    status: DiagnosticStatus,
    accounted: bool,
}

impl Delivery {
    fn usage(&self) -> Usage {
        match self.status {
            DiagnosticStatus::Finished { usage, .. } | DiagnosticStatus::Failed { usage } => usage,
            _ => Usage::Unknown,
        }
    }
}

impl Entry {
    fn delivery(&mut self) -> Option<&Delivery> {
        if let CallStatus::Receiving(receiver) = &mut self.status {
            let delivery = match receiver.try_recv() {
                Ok(delivery) => delivery,
                Err(oneshot::error::TryRecvError::Empty) => return None,
                Err(oneshot::error::TryRecvError::Closed) => Delivery {
                    status: DiagnosticStatus::OutcomeUnknown,
                    accounted: false,
                },
            };
            self.status = CallStatus::Complete(delivery);
        }
        match &self.status {
            CallStatus::Complete(delivery) => Some(delivery),
            CallStatus::Receiving(_) => None,
        }
    }

    fn retain(&mut self, now: Instant) -> bool {
        let checkpoint_ack = self.checkpoint_ack;
        let within_deadline = self.expires > now;
        self.delivery()
            .is_none_or(|delivery| !delivery.accounted || (!checkpoint_ack && within_deadline))
    }
}

impl Calls {
    pub fn launch(
        &self,
        submission: Submission,
        now_ms: u64,
        mut io: impl CallIo + 'static,
    ) -> Result<(), PortError> {
        let receipt = submission.into_reservation();
        let remaining = receipt
            .request
            .deadline_ms
            .checked_sub(now_ms)
            .filter(|remaining| (1..=MAX_DEADLINE_MS).contains(remaining))
            .ok_or(PortError::Denied)?;
        let runtime = Handle::try_current().map_err(|_| PortError::Unavailable)?;
        let mut state = self.state.lock().map_err(|_| PortError::Unavailable)?;
        let now = Instant::now();
        state.entries.retain(|_, entry| entry.retain(now));
        if state.closed
            || state.entries.len() >= MAX_CALLS
            || state.entries.contains_key(&receipt.operation)
        {
            return Err(PortError::Unavailable);
        }
        let deadline = now + Duration::from_millis(remaining);
        let (send, receive) = oneshot::channel();
        let operation = receipt.operation.clone();
        let exact_receipt = receipt.clone();
        let task = runtime.spawn(async move {
            let status = timeout_at(deadline, async {
                io.admit(&receipt).await?;
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return Err(PortError::Denied);
                }
                Ok(io.complete(remaining).await?.status(&receipt.request))
            })
            .await
            .ok()
            .and_then(Result::ok)
            .unwrap_or(DiagnosticStatus::OutcomeUnknown);
            let usage = match status {
                DiagnosticStatus::Finished { usage, .. } | DiagnosticStatus::Failed { usage } => {
                    usage
                }
                _ => Usage::Unknown,
            };
            // Prefer writing through before reporting. A ledger outage keeps
            // the exact result available to the checkpoint's settlement path.
            let accounted = matches!(
                timeout(ACCOUNTING_TIMEOUT, io.settle(&receipt, usage)).await,
                Ok(Ok(()))
            );
            let _ = send.send(Delivery { status, accounted });
        });
        state.entries.insert(
            operation,
            Entry {
                receipt: exact_receipt,
                expires: deadline + ACCOUNTING_TIMEOUT,
                task: task.abort_handle(),
                status: CallStatus::Receiving(receive),
                checkpoint_ack: false,
            },
        );
        Ok(())
    }

    pub fn status(&self, operation: &OperationId) -> Result<DiagnosticStatus, PortError> {
        let mut state = self.state.lock().map_err(|_| PortError::Unavailable)?;
        let Some(entry) = state.entries.get_mut(operation) else {
            return Ok(DiagnosticStatus::OutcomeUnknown);
        };
        Ok(entry
            .delivery()
            .map_or(DiagnosticStatus::Pending, |delivery| {
                delivery.status.clone()
            }))
    }

    pub fn acknowledge(&self, reconciled: Reconciled) {
        if let Ok(mut state) = self.state.lock() {
            if let Some(entry) = state.entries.get_mut(&reconciled.0) {
                entry.checkpoint_ack = true;
                if entry.delivery().is_some_and(|delivery| delivery.accounted) {
                    state.entries.remove(&reconciled.0);
                }
            }
        }
    }

    /// Reconcile completed receipts without observing the original Work. A
    /// fresh Runtime budget port still authorizes the exact accounting write.
    pub fn needs_accounting(&self) -> bool {
        self.state.lock().is_ok_and(|mut state| {
            state
                .entries
                .values_mut()
                .any(|entry| entry.delivery().is_some_and(|delivery| !delivery.accounted))
        })
    }

    /// Reconcile the currently completed receipts once.
    pub fn reconcile_accounting(&self, budget: &mut dyn BudgetPort) -> Result<usize, PortError> {
        let due: Vec<_> = self
            .state
            .lock()
            .map_err(|_| PortError::Unavailable)?
            .entries
            .values_mut()
            .filter_map(|entry| {
                let usage = entry
                    .delivery()
                    .filter(|delivery| !delivery.accounted)?
                    .usage();
                Some((entry.receipt.clone(), usage))
            })
            .collect();
        let mut settled = 0;
        for (receipt, usage) in due {
            if budget.settle(&receipt, usage).is_err() {
                continue;
            }
            settled += 1;
            let mut state = self.state.lock().map_err(|_| PortError::Unavailable)?;
            if let Some(entry) = state
                .entries
                .get_mut(&receipt.operation)
                .filter(|entry| entry.receipt == receipt)
            {
                if let CallStatus::Complete(delivery) = &mut entry.status {
                    delivery.accounted = true;
                }
                if entry.checkpoint_ack {
                    state.entries.remove(&receipt.operation);
                }
            }
        }
        Ok(settled)
    }

    pub fn close(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.closed = true;
            state.entries.clear();
        }
    }
}
