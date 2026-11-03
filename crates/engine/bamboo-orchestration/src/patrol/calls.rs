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
    staged::Submission, Diagnosis, DiagnosticRequest, DiagnosticStatus, OperationId, PortError,
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
    entries: Mutex<BTreeMap<OperationId, Entry>>,
}

struct Entry {
    expires: Instant,
    task: AbortHandle,
    status: CallStatus,
}

impl Drop for Entry {
    fn drop(&mut self) {
        self.task.abort();
    }
}

enum CallStatus {
    Receiving(oneshot::Receiver<DiagnosticStatus>),
    Complete(DiagnosticStatus),
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
        let mut entries = self.entries.lock().map_err(|_| PortError::Unavailable)?;
        let now = Instant::now();
        entries.retain(|_, entry| entry.expires > now);
        if entries.len() >= MAX_CALLS || entries.contains_key(&receipt.operation) {
            return Err(PortError::Unavailable);
        }
        let deadline = now + Duration::from_millis(remaining);
        let (send, receive) = oneshot::channel();
        let operation = receipt.operation.clone();
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
            let _ = timeout(ACCOUNTING_TIMEOUT, io.settle(&receipt, usage)).await;
            let _ = send.send(status);
        });
        entries.insert(
            operation,
            Entry {
                expires: deadline + ACCOUNTING_TIMEOUT,
                task: task.abort_handle(),
                status: CallStatus::Receiving(receive),
            },
        );
        Ok(())
    }

    pub fn status(&self, operation: &OperationId) -> Result<DiagnosticStatus, PortError> {
        let mut entries = self.entries.lock().map_err(|_| PortError::Unavailable)?;
        let Some(entry) = entries.get_mut(operation) else {
            return Ok(DiagnosticStatus::OutcomeUnknown);
        };
        if let CallStatus::Receiving(receiver) = &mut entry.status {
            let status = match receiver.try_recv() {
                Ok(status) => status,
                Err(oneshot::error::TryRecvError::Empty) => return Ok(DiagnosticStatus::Pending),
                Err(oneshot::error::TryRecvError::Closed) => DiagnosticStatus::OutcomeUnknown,
            };
            entry.status = CallStatus::Complete(status);
        }
        match &entry.status {
            CallStatus::Complete(status) => Ok(status.clone()),
            CallStatus::Receiving(_) => unreachable!("receive either returned or completed"),
        }
    }
}
