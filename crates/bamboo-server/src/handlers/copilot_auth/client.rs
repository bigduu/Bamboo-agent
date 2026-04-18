use std::{path::PathBuf, sync::Arc, time::Duration};

use anyhow::Result;
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};

use bamboo_infrastructure_llm::providers::{copilot::auth::CopilotAuthHandler, CopilotProvider};
use bamboo_infrastructure_config::Config;

pub(super) fn resolve_headless_auth(config: &Config) -> bool {
    config
        .providers
        .copilot
        .as_ref()
        .map(|copilot| copilot.headless_auth)
        .unwrap_or(config.headless_auth)
}

pub(super) fn build_auth_handler(
    config: &Config,
    app_data_dir: PathBuf,
) -> Result<CopilotAuthHandler> {
    let retry_policy = ExponentialBackoff::builder()
        .retry_bounds(Duration::from_millis(100), Duration::from_secs(5))
        .build_with_max_retries(3);

    let http_client = bamboo_infrastructure_llm::http_client::build_http_client(config)?;
    let client_with_middleware: Arc<ClientWithMiddleware> = Arc::new(
        ClientBuilder::new(http_client)
            .with(RetryTransientMiddleware::new_with_policy(retry_policy))
            .build(),
    );

    Ok(CopilotAuthHandler::new(
        client_with_middleware,
        app_data_dir,
        resolve_headless_auth(config),
    ))
}

pub(super) fn build_provider_with_auth_handler(
    config: &Config,
    app_data_dir: PathBuf,
) -> Result<CopilotProvider> {
    let http_client = bamboo_infrastructure_llm::http_client::build_http_client(config)?;
    Ok(CopilotProvider::with_auth_handler(
        http_client,
        app_data_dir,
        resolve_headless_auth(config),
    ))
}
