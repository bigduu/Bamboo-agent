//! Host-facing Supervisor identity bootstrap over the canonical Storage port.

use std::{io, sync::Arc};

use bamboo_domain::{Storage, SupervisorBootstrapReceipt};

/// Trusted host service; not a model tool or an automatic Session startup hook.
/// Storage owns atomic publication and identity validation. This service never
/// creates an ordinary Session, changes a cache, or launches a run.
#[derive(Clone)]
pub struct SupervisorSessionService {
    storage: Arc<dyn Storage>,
}

impl SupervisorSessionService {
    pub fn new(storage: Arc<dyn Storage>) -> Self {
        Self { storage }
    }

    /// Get the stable identity, using `initial_model` only on first creation.
    pub async fn get_or_create_default(
        &self,
        initial_model: &str,
    ) -> io::Result<SupervisorBootstrapReceipt> {
        self.storage
            .get_or_create_default_supervisor(initial_model)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_domain::Session;

    struct UnsupportedStore;

    #[async_trait::async_trait]
    impl Storage for UnsupportedStore {
        async fn save_session(&self, _: &Session) -> io::Result<()> {
            panic!("bootstrap must not fall back to ordinary save")
        }
        async fn load_session(&self, _: &str) -> io::Result<Option<Session>> {
            panic!("authority must not fall back to ordinary load")
        }
        async fn delete_session(&self, _: &str) -> io::Result<bool> {
            panic!("bootstrap must not delete sessions")
        }
    }

    #[tokio::test]
    async fn unsupported_authority_ports_fail_without_ordinary_fallback() {
        let storage: Arc<dyn Storage> = Arc::new(UnsupportedStore);
        let service = SupervisorSessionService::new(storage.clone());
        assert_eq!(
            service
                .get_or_create_default("model")
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::Unsupported
        );
        assert_eq!(
            storage
                .load_root_authority("root")
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::Unsupported
        );
    }
}
