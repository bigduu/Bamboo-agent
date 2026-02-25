use std::sync::Arc;

use crate::agent::metrics::MetricsCollector;
use crate::server::metrics_service::MetricsService;

/// Unified metrics infrastructure supporting both MetricsBus and MetricsService
///
/// This provides backward compatibility during migration from the dual-metrics system
/// to a unified approach. Eventually MetricsService will be the single source of truth.
pub struct MetricsInfrastructure {
    /// Service-based metrics (agent/server style) - primary
    pub service: Arc<MetricsService>,
    /// Bus-based metrics (web_service style) - secondary, for backward compatibility
    pub bus: Option<crate::agent::metrics::MetricsBus>,
}

impl MetricsInfrastructure {
    /// Create new metrics infrastructure with both service and optional bus
    pub async fn new(
        db_path: impl AsRef<std::path::Path>,
        enable_bus: bool,
        bus_capacity: usize,
    ) -> Result<Self, crate::agent::metrics::MetricsError> {
        // Initialize MetricsService (the future)
        let service = Arc::new(MetricsService::new(db_path).await?);

        // Optionally initialize MetricsBus (the past, for compatibility)
        let bus = if enable_bus {
            let (bus, _receiver) = crate::agent::metrics::MetricsBus::new(bus_capacity);
            Some(bus)
        } else {
            None
        };

        Ok(Self { service, bus })
    }

    /// Get the metrics collector for emitting events
    pub fn collector(&self) -> MetricsCollector {
        self.service.collector()
    }

    /// Get a reference to the metrics service
    pub fn service(&self) -> &Arc<MetricsService> {
        &self.service
    }

    /// Get a reference to the optional metrics bus
    pub fn bus(&self) -> Option<&crate::agent::metrics::MetricsBus> {
        self.bus.as_ref()
    }
}

impl Clone for MetricsInfrastructure {
    fn clone(&self) -> Self {
        Self {
            service: Arc::clone(&self.service),
            bus: self.bus.clone(),
        }
    }
}
