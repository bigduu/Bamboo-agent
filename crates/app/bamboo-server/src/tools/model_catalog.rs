//! Server-side `ModelCatalogPort`: lists models per configured provider so the
//! parent agent can pin a child session to an explicit model
//! (`SubAgent` tool `action=list_models` / `create.model`).

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use bamboo_engine::session_app::child_session::{ModelCatalogPort, ProviderModelList};
use bamboo_llm::ProviderRegistry;

/// How long one provider's `list_models` call may take before we report it as
/// timed out (the listing is best-effort; a slow provider must not stall the
/// tool call).
const PER_PROVIDER_TIMEOUT: Duration = Duration::from_secs(8);

/// Model catalog backed by the live provider registry.
pub struct RegistryModelCatalog {
    registry: Arc<ProviderRegistry>,
}

impl RegistryModelCatalog {
    pub fn new(registry: Arc<ProviderRegistry>) -> Self {
        Self { registry }
    }
}

#[async_trait]
impl ModelCatalogPort for RegistryModelCatalog {
    async fn list_models(&self) -> Vec<ProviderModelList> {
        let mut names = self.registry.provider_names();
        names.sort();

        // Query all providers concurrently; tolerate individual failures.
        let futures = names.into_iter().map(|name| {
            let provider = self.registry.get(&name);
            async move {
                let Some(provider) = provider else {
                    return ProviderModelList {
                        provider: name,
                        models: Vec::new(),
                        error: Some("provider not initialized".to_string()),
                    };
                };
                match tokio::time::timeout(PER_PROVIDER_TIMEOUT, provider.list_models()).await {
                    Ok(Ok(mut models)) => {
                        models.sort();
                        ProviderModelList {
                            provider: name,
                            models,
                            error: None,
                        }
                    }
                    Ok(Err(e)) => ProviderModelList {
                        provider: name,
                        models: Vec::new(),
                        error: Some(e.to_string()),
                    },
                    Err(_) => ProviderModelList {
                        provider: name,
                        models: Vec::new(),
                        error: Some(format!(
                            "timed out after {}s",
                            PER_PROVIDER_TIMEOUT.as_secs()
                        )),
                    },
                }
            }
        });

        futures::future::join_all(futures).await
    }

    fn default_provider(&self) -> String {
        self.registry.default_provider_name()
    }
}
