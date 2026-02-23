use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::sync::mpsc;

use crate::events::{MetricsEvent, SystemEvent};

/// A bounded channel-based metrics event bus
///
/// Uses `try_send` for non-blocking emission. If the channel is full,
/// events are dropped and counted in the `dropped` counter.
pub struct MetricsBus {
    tx: mpsc::Sender<MetricsEvent>,
    dropped: Arc<AtomicU64>,
}

impl MetricsBus {
    /// Create a new MetricsBus with the specified channel capacity
    ///
    /// Returns the bus (for emitting events) and the receiver (for consuming events)
    pub fn new(capacity: usize) -> (Self, mpsc::Receiver<MetricsEvent>) {
        let (tx, rx) = mpsc::channel(capacity);
        (
            Self {
                tx,
                dropped: Arc::new(AtomicU64::new(0)),
            },
            rx,
        )
    }

    /// Emit a metrics event
    ///
    /// This is non-blocking - if the channel is full, the event is dropped
    /// and the drop counter is incremented.
    pub fn emit(&self, event: MetricsEvent) {
        if self.tx.try_send(event).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Emit a system event indicating metrics were dropped
    pub fn emit_dropped_notification(&self, count: u64, reason: &str) {
        // We use try_send here too, but for system events we don't count drops
        // to avoid infinite recursion
        let _ = self.tx.try_send(MetricsEvent::System(SystemEvent::MetricsDropped {
            count,
            reason: reason.to_string(),
        }));
    }

    /// Get the number of dropped events since the bus was created
    pub fn dropped_count(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// Get a clone of the dropped counter for monitoring
    pub fn dropped_counter(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.dropped)
    }

    /// Check if the channel is closed (receiver dropped)
    pub fn is_closed(&self) -> bool {
        self.tx.is_closed()
    }
}

impl Clone for MetricsBus {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            dropped: Arc::clone(&self.dropped),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::ChatEvent;
    use tokio::time::{timeout, Duration};

    #[tokio::test]
    async fn test_bus_creates_channel() {
        let (bus, mut rx) = MetricsBus::new(10);

        let event = MetricsEvent::Chat(ChatEvent::SessionStarted {
            meta: crate::events::EventMeta::new(),
            session_id: "test".to_string(),
            model: "gpt-4".to_string(),
        });

        bus.emit(event);

        let received = timeout(Duration::from_millis(100), rx.recv())
            .await
            .expect("Should receive event")
            .expect("Event should exist");

        match received {
            MetricsEvent::Chat(ChatEvent::SessionStarted { session_id, .. }) => {
                assert_eq!(session_id, "test");
            }
            _ => panic!("Wrong event type"),
        }
    }

    #[tokio::test]
    async fn test_bus_drops_when_full() {
        let (bus, _rx) = MetricsBus::new(1); // Capacity of 1

        // Fill the channel
        let event1 = MetricsEvent::Chat(ChatEvent::SessionStarted {
            meta: crate::events::EventMeta::new(),
            session_id: "1".to_string(),
            model: "gpt-4".to_string(),
        });
        bus.emit(event1);

        // This should be dropped
        let event2 = MetricsEvent::Chat(ChatEvent::SessionStarted {
            meta: crate::events::EventMeta::new(),
            session_id: "2".to_string(),
            model: "gpt-4".to_string(),
        });
        bus.emit(event2);

        // Give a small delay for the counter to update
        tokio::time::sleep(Duration::from_millis(10)).await;

        assert_eq!(bus.dropped_count(), 1);
    }

    #[test]
    fn test_bus_clone_shares_dropped_counter() {
        let (bus1, _rx) = MetricsBus::new(10);
        let bus2 = bus1.clone();

        // Emit through bus1
        let event = MetricsEvent::Chat(ChatEvent::SessionStarted {
            meta: crate::events::EventMeta::new(),
            session_id: "1".to_string(),
            model: "gpt-4".to_string(),
        });
        bus1.emit(event);

        // Check through bus2
        assert_eq!(bus2.dropped_count(), 0);
    }
}
