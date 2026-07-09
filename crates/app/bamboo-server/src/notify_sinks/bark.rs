//! [Bark](https://github.com/Finb/Bark) iOS push notification sink.
//!
//! POSTs a JSON payload to `{base_url}/push` — see
//! <https://github.com/Finb/Bark#push-parameters>.

use std::sync::OnceLock;

use bamboo_config::BarkChannelConfig;
use serde::Serialize;

use super::{NotificationSink, SinkNotification};

/// One shared `reqwest::Client` for every Bark delivery. Reuses the
/// workspace's pinned (native-tls) `reqwest` — never construct a second
/// client/connector for this sink.
fn http_client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(reqwest::Client::new)
}

/// Maps [`SinkNotification::priority`] to Bark's `level` parameter. Bark's
/// other levels (`passive`, `critical`) have no equivalent in our two-value
/// priority model, so this is a simple two-way split.
fn bark_level(priority: &str) -> &'static str {
    if priority == "high" {
        "timeSensitive"
    } else {
        "active"
    }
}

#[derive(Serialize)]
struct BarkPush<'a> {
    device_key: &'a str,
    title: &'a str,
    body: &'a str,
    group: &'a str,
    level: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    url: Option<&'a str>,
}

pub(super) struct BarkSink {
    base_url: String,
    device_key: Option<String>,
}

impl BarkSink {
    pub(super) fn from_config(config: &BarkChannelConfig) -> Self {
        Self {
            base_url: config.base_url.clone(),
            device_key: config.device_key.clone(),
        }
    }
}

impl NotificationSink for BarkSink {
    fn name(&self) -> &'static str {
        "bark"
    }

    fn deliver(&self, n: &SinkNotification) {
        let Some(device_key) = self.device_key.as_deref().filter(|key| !key.is_empty()) else {
            tracing::warn!("bark sink: enabled with no device key; skipping delivery");
            return;
        };
        let url = format!("{}/push", self.base_url.trim_end_matches('/'));
        let payload = BarkPush {
            device_key,
            title: &n.title,
            body: &n.body,
            group: "bamboo",
            level: bark_level(&n.priority),
            url: n.click_url.as_deref(),
        };
        let request = http_client().post(url).json(&payload);
        tokio::spawn(async move {
            if let Err(error) = request.send().await {
                tracing::warn!(%error, "bark sink: delivery failed");
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bark_level_maps_high_to_time_sensitive() {
        assert_eq!(bark_level("high"), "timeSensitive");
        assert_eq!(bark_level("normal"), "active");
        assert_eq!(bark_level("low"), "active");
    }

    #[test]
    fn from_config_copies_fields() {
        let config = BarkChannelConfig {
            enabled: true,
            base_url: "https://bark.example.com".to_string(),
            device_key: Some("dk".to_string()),
            device_key_encrypted: None,
        };
        let sink = BarkSink::from_config(&config);
        assert_eq!(sink.base_url, "https://bark.example.com");
        assert_eq!(sink.device_key.as_deref(), Some("dk"));
    }

    fn sample_notification() -> SinkNotification {
        SinkNotification {
            title: "Approval needed".to_string(),
            body: "please approve".to_string(),
            category: "needs_approval".to_string(),
            priority: "high".to_string(),
            session_id: "sess-1".to_string(),
            click_url: Some("bamboo://session/sess-1".to_string()),
        }
    }

    /// Polls the mock server's recorded requests until `expected` have
    /// arrived or a bounded timeout elapses. `deliver()` is fire-and-forget
    /// (it spawns its own task), so the test has no join handle to await.
    async fn wait_for_requests(
        server: &wiremock::MockServer,
        expected: usize,
    ) -> Vec<wiremock::Request> {
        for _ in 0..50 {
            if let Some(requests) = server.received_requests().await {
                if requests.len() >= expected {
                    return requests;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        server.received_requests().await.unwrap_or_default()
    }

    #[tokio::test]
    async fn deliver_posts_expected_method_path_and_json_body() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .respond_with(wiremock::ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let sink = BarkSink {
            base_url: server.uri(),
            device_key: Some("dk-secret".to_string()),
        };
        sink.deliver(&sample_notification());

        let requests = wait_for_requests(&server, 1).await;
        assert_eq!(requests.len(), 1, "expected exactly one delivered request");
        let request = &requests[0];
        assert_eq!(request.method.as_str(), "POST");
        assert_eq!(request.url.path(), "/push");
        assert_eq!(
            request
                .headers
                .get("content-type")
                .and_then(|v| v.to_str().ok()),
            Some("application/json")
        );
        let body: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
        assert_eq!(body["device_key"], "dk-secret");
        assert_eq!(body["title"], "Approval needed");
        assert_eq!(body["body"], "please approve");
        assert_eq!(body["group"], "bamboo");
        assert_eq!(body["level"], "timeSensitive");
        assert_eq!(body["url"], "bamboo://session/sess-1");
    }

    #[tokio::test]
    async fn deliver_skips_missing_device_key_without_sending() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .respond_with(wiremock::ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let sink = BarkSink {
            base_url: server.uri(),
            device_key: None,
        };
        sink.deliver(&sample_notification());

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert_eq!(server.received_requests().await.map(|r| r.len()), Some(0));
    }
}
