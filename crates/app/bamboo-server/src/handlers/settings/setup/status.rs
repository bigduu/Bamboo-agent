use serde::Serialize;

use bamboo_llm::Config;

/// Setup status response.
#[derive(Serialize)]
pub(super) struct SetupStatus {
    /// Whether setup is complete.
    pub(super) is_complete: bool,
    /// Whether proxy config exists in config.json.
    pub(super) has_proxy_config: bool,
    /// Whether proxy env vars are detected.
    pub(super) has_proxy_env: bool,
    /// Status message.
    pub(super) message: String,
}

pub(super) fn build_setup_status(config: &Config, proxy_environment_flags: &[&str]) -> SetupStatus {
    let has_proxy_config = has_proxy_config(config);
    let has_proxy_env = !proxy_environment_flags.is_empty();
    let setup_completed = is_setup_completed(config);
    let is_complete = !should_show_setup(setup_completed, has_proxy_config, has_proxy_env);
    let message = setup_status_message(setup_completed, has_proxy_config, proxy_environment_flags);

    SetupStatus {
        is_complete,
        has_proxy_config,
        has_proxy_env,
        message,
    }
}

pub(super) fn collect_proxy_environment_flags() -> Vec<&'static str> {
    ["HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"]
        .iter()
        .copied()
        .filter(|key| {
            std::env::var(key)
                .ok()
                .map(|value| !value.trim().is_empty())
                .unwrap_or(false)
        })
        .collect()
}

fn has_proxy_config(config: &Config) -> bool {
    !config.http_proxy.trim().is_empty() || !config.https_proxy.trim().is_empty()
}

fn is_setup_completed(config: &Config) -> bool {
    config
        .extra
        .get("setup")
        .and_then(|setup| setup.get("completed"))
        .and_then(|value| value.as_bool())
        .unwrap_or(false)
}

fn should_show_setup(setup_completed: bool, _has_proxy_config: bool, _has_proxy_env: bool) -> bool {
    !setup_completed
}

fn setup_status_message(
    setup_completed: bool,
    has_proxy_config: bool,
    proxy_environment_flags: &[&str],
) -> String {
    if setup_completed {
        return "Setup has already been completed in config.json.".to_string();
    }

    if has_proxy_config {
        return "Proxy configuration already exists in config.json. Setup is not required."
            .to_string();
    }

    if !proxy_environment_flags.is_empty() {
        return format!(
            "Detected proxy environment variables: {}. Please confirm proxy settings in setup.",
            proxy_environment_flags.join(", ")
        );
    }

    "No proxy configuration or proxy environment variables detected. Setup is not required."
        .to_string()
}
