//! Generation-bound, non-blocking stdin delivery for supervised services.
//!
//! A public [`ServiceInputSender`] is a capability for exactly one spawned
//! process. It never follows a restart: retiring that process first marks all
//! clones stale/stopped, then cancels and joins the sole writer task that owns
//! its `ChildStdin`. A replacement process gets a fresh channel, writer, and
//! monotonically increasing generation.

use std::io::Write as StdWrite;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;

use bamboo_plugin::manifest::ServiceInputProtocol;
use serde::Serialize;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::process::Child;
use tokio::sync::{mpsc, RwLock};
use tokio_util::sync::CancellationToken;

/// The fixed second-stage queue between a live service-generation handle and
/// its stdin writer. Event sinks have their own independently validated queue
/// in #905; this small bound prevents even an in-process caller of the service
/// input API from accumulating unbounded writes behind a blocked child.
pub const DEFAULT_SERVICE_INPUT_QUEUE_CAPACITY: usize = 64;
/// Hard cap for one physical NDJSON line, including its trailing newline.
/// Enforced during streaming serialization before queue admission,
/// independently of any router/event-specific payload limit.
pub const MAX_SERVICE_INPUT_LINE_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ServiceInputHealth {
    /// Protocol declared, but no process generation is currently writable
    /// (startup, restart backoff, or a crashed service).
    Waiting,
    Ready,
    BrokenStdin,
    Stopped,
}

/// Payload-free diagnostics for one supervised service's NDJSON input across
/// all of its process generations. Only bounded counters and protocol state
/// are exposed: no serialized values, OS error strings, environment, or paths.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ServiceInputStatusSnapshot {
    pub protocol: ServiceInputProtocol,
    /// The currently bound generation. `None` means there is no writable
    /// child right now; handles from prior generations remain invalid.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub generation: Option<u64>,
    pub health: ServiceInputHealth,
    pub queue_capacity: usize,
    pub max_line_bytes: usize,
    pub accepted_lines: u64,
    pub written_lines: u64,
    pub dropped_queue_full: u64,
    pub dropped_stale_generation: u64,
    pub dropped_stopped: u64,
    pub dropped_broken_stdin: u64,
    pub serialization_failures: u64,
    pub oversize_lines: u64,
    pub write_failures: u64,
}

/// Immediate producer-side outcomes. None contains the payload or an
/// underlying serde/OS error, so callers may safely expose or aggregate it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ServiceInputSendError {
    #[error("service input generation {generation} is stale")]
    StaleGeneration { generation: u64 },
    #[error("service input generation {generation} is stopped")]
    Stopped { generation: u64 },
    #[error("service input generation {generation} has broken stdin")]
    BrokenStdin { generation: u64 },
    #[error("service input generation {generation} queue is full")]
    QueueFull { generation: u64 },
    #[error("service input value could not be serialized as JSON")]
    Serialization,
    #[error("service input line exceeds the {max_bytes}-byte limit")]
    Oversize { max_bytes: usize },
}

#[derive(Default)]
struct ServiceInputCounters {
    accepted_lines: AtomicU64,
    written_lines: AtomicU64,
    dropped_queue_full: AtomicU64,
    dropped_stale_generation: AtomicU64,
    dropped_stopped: AtomicU64,
    dropped_broken_stdin: AtomicU64,
    serialization_failures: AtomicU64,
    oversize_lines: AtomicU64,
    write_failures: AtomicU64,
}

fn increment(counter: &AtomicU64) {
    // Diagnostics must remain monotonic even at the integer boundary rather
    // than wrapping to zero and misrepresenting a heavily degraded service.
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
        Some(value.saturating_add(1))
    });
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum GenerationState {
    Active = 0,
    Stale = 1,
    Stopped = 2,
    BrokenStdin = 3,
}

impl GenerationState {
    fn from_u8(value: u8) -> Self {
        match value {
            0 => Self::Active,
            1 => Self::Stale,
            2 => Self::Stopped,
            3 => Self::BrokenStdin,
            _ => Self::BrokenStdin,
        }
    }
}

struct GenerationMeta {
    generation: u64,
    state: AtomicU8,
    counters: Arc<ServiceInputCounters>,
}

impl GenerationMeta {
    fn state(&self) -> GenerationState {
        GenerationState::from_u8(self.state.load(Ordering::SeqCst))
    }

    /// Writer ownership is narrow: it may report only Active -> BrokenStdin.
    fn mark_broken_stdin(&self) {
        let _ = self.state.compare_exchange(
            GenerationState::Active as u8,
            GenerationState::BrokenStdin as u8,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
    }

    /// Process exit/restart retires Active OR Broken as stale, but may not
    /// downgrade the higher-priority intentional Stopped terminal state.
    fn mark_stale(&self) {
        let _ = self
            .state
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                (GenerationState::from_u8(current) != GenerationState::Stopped)
                    .then_some(GenerationState::Stale as u8)
            });
    }

    /// Intentional stop/upgrade/uninstall has highest priority and overrides
    /// Active, Broken, or a concurrently published Stale state.
    fn mark_stopped(&self) {
        let _ = self
            .state
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                (GenerationState::from_u8(current) != GenerationState::Stopped)
                    .then_some(GenerationState::Stopped as u8)
            });
    }
}

struct CappedJsonLineWriter {
    bytes: Vec<u8>,
    max_json_bytes: usize,
    exceeded: bool,
}

impl CappedJsonLineWriter {
    fn new() -> Self {
        Self {
            // Keep one spare byte throughout serialization so appending the
            // final newline never triggers an amortized growth beyond the
            // declared line bound.
            bytes: Vec::with_capacity(1),
            max_json_bytes: MAX_SERVICE_INPUT_LINE_BYTES.saturating_sub(1),
            exceeded: false,
        }
    }

    fn finish(mut self) -> Vec<u8> {
        // One byte was reserved from the cap specifically for the delimiter.
        self.bytes.push(b'\n');
        self.bytes
    }
}

impl StdWrite for CappedJsonLineWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let remaining = self.max_json_bytes.saturating_sub(self.bytes.len());
        if buf.len() > remaining {
            self.exceeded = true;
            return Err(std::io::Error::other("service input line exceeds limit"));
        }
        let spare = self.bytes.capacity().saturating_sub(self.bytes.len());
        let needed_spare = buf.len().saturating_add(1);
        if needed_spare > spare {
            self.bytes
                .try_reserve_exact(needed_spare - spare)
                .map_err(|_| std::io::Error::other("service input allocation failed"))?;
        }
        self.bytes.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Cloneable, non-blocking send capability for one exact process generation.
///
/// Serialization happens on the caller and the bounded queue is entered only
/// with `try_send`; this API never awaits service I/O or queue capacity.
/// A successful send means queue admission, not durable delivery: stopping or
/// restarting the generation may discard records that the writer has not yet
/// completed. `written_lines` advances only after a full write and flush.
#[derive(Clone)]
pub struct ServiceInputSender {
    meta: Arc<GenerationMeta>,
    tx: mpsc::Sender<Vec<u8>>,
}

impl ServiceInputSender {
    pub fn generation(&self) -> u64 {
        self.meta.generation
    }

    fn reject_for_state(&self) -> Result<(), ServiceInputSendError> {
        let generation = self.generation();
        match self.meta.state() {
            GenerationState::Active => Ok(()),
            GenerationState::Stale => {
                increment(&self.meta.counters.dropped_stale_generation);
                Err(ServiceInputSendError::StaleGeneration { generation })
            }
            GenerationState::Stopped => {
                increment(&self.meta.counters.dropped_stopped);
                Err(ServiceInputSendError::Stopped { generation })
            }
            GenerationState::BrokenStdin => {
                increment(&self.meta.counters.dropped_broken_stdin);
                Err(ServiceInputSendError::BrokenStdin { generation })
            }
        }
    }

    /// Serialize one value and enqueue exactly one newline-terminated JSON
    /// record. Newlines inside strings remain JSON escapes, so one accepted
    /// call always corresponds to one physical NDJSON line.
    pub fn try_send<T>(&self, value: &T) -> Result<(), ServiceInputSendError>
    where
        T: Serialize + ?Sized,
    {
        self.reject_for_state()?;
        let mut writer = CappedJsonLineWriter::new();
        if serde_json::to_writer(&mut writer, value).is_err() {
            if writer.exceeded {
                increment(&self.meta.counters.oversize_lines);
                return Err(ServiceInputSendError::Oversize {
                    max_bytes: MAX_SERVICE_INPUT_LINE_BYTES,
                });
            }
            increment(&self.meta.counters.serialization_failures);
            return Err(ServiceInputSendError::Serialization);
        }
        let line = writer.finish();

        // Serialization can invoke arbitrary user code. Re-check the lease
        // afterwards so a generation retired during serialization cannot be
        // enqueued into its now-closing writer.
        self.reject_for_state()?;
        match self.tx.try_send(line) {
            Ok(()) => {
                increment(&self.meta.counters.accepted_lines);
                Ok(())
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                increment(&self.meta.counters.dropped_queue_full);
                Err(ServiceInputSendError::QueueFull {
                    generation: self.generation(),
                })
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                // A closed receiver while the lease still said Active means
                // the sole writer ended unexpectedly. Publish the safe state
                // before classifying this and subsequent producer attempts.
                self.meta.mark_broken_stdin();
                self.reject_for_state()
            }
        }
    }

    #[cfg(test)]
    pub(super) fn remaining_capacity(&self) -> usize {
        self.tx.capacity()
    }
}

struct ActiveServiceInput {
    sender: ServiceInputSender,
    cancel: CancellationToken,
}

/// Runtime-wide input registry/counters. The active slot contains at most one
/// generation and is consulted only while reconciling lifecycle/status, never
/// by the producer hot path after it has acquired a `ServiceInputSender`.
pub(super) struct ServiceInputRuntime {
    service_id: String,
    next_generation: Arc<AtomicU64>,
    counters: Arc<ServiceInputCounters>,
    active: RwLock<Option<ActiveServiceInput>>,
}

impl ServiceInputRuntime {
    pub(super) fn new(service_id: String, next_generation: Arc<AtomicU64>) -> Self {
        Self {
            service_id,
            next_generation,
            counters: Arc::new(ServiceInputCounters::default()),
            active: RwLock::new(None),
        }
    }

    pub(super) async fn sender(&self) -> Option<ServiceInputSender> {
        self.active
            .read()
            .await
            .as_ref()
            .map(|active| active.sender.clone())
    }

    pub(super) async fn bind_child(
        &self,
        child: &mut Child,
    ) -> Result<BoundServiceInput, ServiceInputBindError> {
        let stdin = child
            .stdin
            .take()
            .ok_or(ServiceInputBindError::MissingStdinPipe)?;
        self.bind_writer(stdin, DEFAULT_SERVICE_INPUT_QUEUE_CAPACITY)
            .await
    }

    async fn bind_writer<W>(
        &self,
        writer: W,
        queue_capacity: usize,
    ) -> Result<BoundServiceInput, ServiceInputBindError>
    where
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let mut active = self.active.write().await;
        if active.is_some() {
            return Err(ServiceInputBindError::GenerationAlreadyBound);
        }

        let generation = self
            .next_generation
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                current.checked_add(1)
            })
            .map(|previous| previous + 1)
            .map_err(|_| ServiceInputBindError::GenerationExhausted)?;
        let meta = Arc::new(GenerationMeta {
            generation,
            state: AtomicU8::new(GenerationState::Active as u8),
            counters: self.counters.clone(),
        });
        let cancel = CancellationToken::new();
        let (tx, rx) = mpsc::channel(queue_capacity.max(1));
        let sender = ServiceInputSender {
            meta: meta.clone(),
            tx,
        };
        let writer_cancel = cancel.clone();
        let service_id = self.service_id.clone();
        let task = tokio::spawn(run_writer(service_id, meta, writer_cancel, writer, rx));

        *active = Some(ActiveServiceInput {
            sender: sender.clone(),
            cancel: cancel.clone(),
        });
        Ok(BoundServiceInput {
            sender,
            cancel,
            task: Some(task),
        })
    }

    /// Invalidate the public generation before the supervisor is awakened.
    /// `stop_service` uses this ordering so upgrade/uninstall cannot enqueue
    /// more input after they have begun stopping the old binary.
    pub(super) async fn stop_active(&self) {
        let active = self.active.write().await.take();
        if let Some(active) = active {
            active.sender.meta.mark_stopped();
            active.cancel.cancel();
        }
    }

    async fn retire_generation(&self, generation: u64, stopped: bool) {
        let mut active = self.active.write().await;
        if active
            .as_ref()
            .is_some_and(|active| active.sender.generation() == generation)
        {
            let active = active.take().expect("checked active generation");
            if stopped {
                active.sender.meta.mark_stopped();
            } else {
                active.sender.meta.mark_stale();
            }
            active.cancel.cancel();
        }
    }

    pub(super) async fn snapshot(&self, stopped: bool) -> ServiceInputStatusSnapshot {
        let active = self.active.read().await;
        let (generation, health) = match active.as_ref() {
            Some(active) => (
                Some(active.sender.generation()),
                match active.sender.meta.state() {
                    GenerationState::Active => ServiceInputHealth::Ready,
                    GenerationState::BrokenStdin => ServiceInputHealth::BrokenStdin,
                    GenerationState::Stale => ServiceInputHealth::Waiting,
                    GenerationState::Stopped => ServiceInputHealth::Stopped,
                },
            ),
            None if stopped => (None, ServiceInputHealth::Stopped),
            None => (None, ServiceInputHealth::Waiting),
        };
        ServiceInputStatusSnapshot {
            protocol: ServiceInputProtocol::NdjsonV1,
            generation,
            health,
            queue_capacity: DEFAULT_SERVICE_INPUT_QUEUE_CAPACITY,
            max_line_bytes: MAX_SERVICE_INPUT_LINE_BYTES,
            accepted_lines: self.counters.accepted_lines.load(Ordering::Relaxed),
            written_lines: self.counters.written_lines.load(Ordering::Relaxed),
            dropped_queue_full: self.counters.dropped_queue_full.load(Ordering::Relaxed),
            dropped_stale_generation: self
                .counters
                .dropped_stale_generation
                .load(Ordering::Relaxed),
            dropped_stopped: self.counters.dropped_stopped.load(Ordering::Relaxed),
            dropped_broken_stdin: self.counters.dropped_broken_stdin.load(Ordering::Relaxed),
            serialization_failures: self.counters.serialization_failures.load(Ordering::Relaxed),
            oversize_lines: self.counters.oversize_lines.load(Ordering::Relaxed),
            write_failures: self.counters.write_failures.load(Ordering::Relaxed),
        }
    }

    #[cfg(test)]
    pub(super) async fn bind_writer_for_test<W>(
        &self,
        writer: W,
        queue_capacity: usize,
    ) -> Result<BoundServiceInput, ServiceInputBindError>
    where
        W: AsyncWrite + Unpin + Send + 'static,
    {
        self.bind_writer(writer, queue_capacity).await
    }

    #[cfg(test)]
    pub(super) async fn bind_child_for_test(
        &self,
        child: &mut Child,
        queue_capacity: usize,
    ) -> Result<BoundServiceInput, ServiceInputBindError> {
        let stdin = child
            .stdin
            .take()
            .ok_or(ServiceInputBindError::MissingStdinPipe)?;
        self.bind_writer(stdin, queue_capacity).await
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(super) enum ServiceInputBindError {
    #[error("spawned NDJSON service has no stdin pipe")]
    MissingStdinPipe,
    #[error("service input generation is already bound")]
    GenerationAlreadyBound,
    #[error("service input generation space is exhausted")]
    GenerationExhausted,
}

pub(super) struct BoundServiceInput {
    sender: ServiceInputSender,
    cancel: CancellationToken,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl BoundServiceInput {
    pub(super) fn generation(&self) -> u64 {
        self.sender.generation()
    }

    /// Unpublish this exact generation, cancel any blocked write, and await
    /// task exit so its stdin is closed before process shutdown/replacement.
    pub(super) async fn close(mut self, runtime: &ServiceInputRuntime, stopped: bool) {
        runtime.retire_generation(self.generation(), stopped).await;
        // `stop_service` may already have removed the active slot; these are
        // deliberately idempotent and still cover that ordering.
        if stopped {
            self.sender.meta.mark_stopped();
        } else {
            self.sender.meta.mark_stale();
        }
        self.cancel.cancel();
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for BoundServiceInput {
    fn drop(&mut self) {
        // Covers supervisor abort/panic: never detach the sole ChildStdin
        // owner. `mark_stale` respects an already-published Stopped state.
        self.sender.meta.mark_stale();
        self.cancel.cancel();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

async fn run_writer<W>(
    service_id: String,
    meta: Arc<GenerationMeta>,
    cancel: CancellationToken,
    mut writer: W,
    mut rx: mpsc::Receiver<Vec<u8>>,
) where
    W: AsyncWrite + Unpin,
{
    loop {
        let line = tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
            line = rx.recv() => match line {
                Some(line) => line,
                None => break,
            },
        };

        let result = tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
            result = async {
                writer.write_all(&line).await?;
                writer.flush().await
            } => result,
        };
        match result {
            Ok(()) => increment(&meta.counters.written_lines),
            Err(error) => {
                increment(&meta.counters.write_failures);
                meta.mark_broken_stdin();
                // Only the coarse error kind is retained in logs; values and
                // platform-specific error strings can contain sensitive paths.
                tracing::warn!(
                    service_id = %service_id,
                    generation = meta.generation,
                    error_kind = ?error.kind(),
                    "service NDJSON stdin writer stopped"
                );
                break;
            }
        }
    }
    rx.close();
    // `writer` (and therefore ChildStdin) drops here, delivering EOF before
    // the supervisor signals or replaces the process.
}
