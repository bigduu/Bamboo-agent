//! [ntfy.sh](https://ntfy.sh) push notification sink (self-hostable).
//!
//! POSTs the notification body to `{base_url}/{topic}` using ntfy's
//! header-based publish API — see <https://docs.ntfy.sh/publish/>.

use std::sync::OnceLock;

use bamboo_config::NtfyChannelConfig;

use super::{NotificationSink, SinkNotification};

/// One shared `reqwest::Client` for every ntfy delivery. Reuses the
/// workspace's pinned (native-tls) `reqwest` — never construct a second
/// client/connector for this sink.
fn http_client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(reqwest::Client::new)
}

/// Maps [`SinkNotification::priority`] to ntfy's `Priority` header value.
/// ntfy accepts `max`/`urgent`/`high`/`default`/`low`/`min`; we only ever
/// mint `"high"` or `"normal"` from the policy layer, so the mapping is a
/// simple two-way split (see <https://docs.ntfy.sh/publish/#message-priority>).
fn ntfy_priority(priority: &str) -> &'static str {
    if priority == "high" {
        "high"
    } else {
        "default"
    }
}

pub(super) struct NtfySink {
    base_url: String,
    topic: String,
    token: Option<String>,
}

impl NtfySink {
    pub(super) fn from_config(config: &NtfyChannelConfig) -> Self {
        Self {
            base_url: config.base_url.clone(),
            topic: config.topic.clone(),
            token: config.token.clone(),
        }
    }
}

impl NotificationSink for NtfySink {
    fn name(&self) -> &'static str {
        "ntfy"
    }

    fn deliver(&self, n: &SinkNotification) {
        if self.topic.trim().is_empty() {
            tracing::warn!("ntfy sink: enabled with an empty topic; skipping delivery");
            return;
        }
        let url = format!("{}/{}", self.base_url.trim_end_matches('/'), self.topic);
        let mut request = http_client()
            .post(url)
            .body(n.body.clone())
            .header("Title", n.title.clone())
            .header("Priority", ntfy_priority(&n.priority))
            .header("Tags", n.category.clone());
        if let Some(token) = self.token.as_deref().filter(|t| !t.is_empty()) {
            request = request.bearer_auth(token);
        }
        if let Some(click_url) = &n.click_url {
            request = request.header("Click", click_url.clone());
        }
        tokio::spawn(async move {
            if let Err(error) = request.send().await {
                tracing::warn!(%error, "ntfy sink: delivery failed");
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ntfy_priority_maps_high_and_defaults_everything_else() {
        assert_eq!(ntfy_priority("high"), "high");
        assert_eq!(ntfy_priority("normal"), "default");
        assert_eq!(ntfy_priority("low"), "default");
    }

    #[test]
    fn from_config_copies_fields() {
        let config = NtfyChannelConfig {
            enabled: true,
            base_url: "https://ntfy.example.com".to_string(),
            topic: "bamboo-alerts".to_string(),
            token: Some("tk".to_string()),
            token_encrypted: None,
        };
        let sink = NtfySink::from_config(&config);
        assert_eq!(sink.base_url, "https://ntfy.example.com");
        assert_eq!(sink.topic, "bamboo-alerts");
        assert_eq!(sink.token.as_deref(), Some("tk"));
    }

    fn sample_notification() -> SinkNotification {
        SinkNotification {
            title: "Approval needed".to_string(),
            body: "please approve".to_string(),
            category: "needs_approval".to_string(),
            priority: "high".to_string(),
            session_id: "sess-1".to_string(),
            click_url: None,
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
    async fn deliver_posts_expected_method_path_headers_and_body() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .respond_with(wiremock::ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let sink = NtfySink {
            base_url: server.uri(),
            topic: "bamboo-alerts".to_string(),
            token: Some("secret-token".to_string()),
        };
        sink.deliver(&sample_notification());

        let requests = wait_for_requests(&server, 1).await;
        assert_eq!(requests.len(), 1, "expected exactly one delivered request");
        let request = &requests[0];
        assert_eq!(request.method.as_str(), "POST");
        assert_eq!(request.url.path(), "/bamboo-alerts");
        assert_eq!(
            request.headers.get("title").and_then(|v| v.to_str().ok()),
            Some("Approval needed")
        );
        assert_eq!(
            request
                .headers
                .get("priority")
                .and_then(|v| v.to_str().ok()),
            Some("high")
        );
        assert_eq!(
            request.headers.get("tags").and_then(|v| v.to_str().ok()),
            Some("needs_approval")
        );
        assert_eq!(
            request
                .headers
                .get("authorization")
                .and_then(|v| v.to_str().ok()),
            Some("Bearer secret-token")
        );
        assert_eq!(
            std::str::from_utf8(&request.body).unwrap(),
            "please approve"
        );
    }

    #[tokio::test]
    async fn deliver_skips_empty_topic_without_sending() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .respond_with(wiremock::ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let sink = NtfySink {
            base_url: server.uri(),
            topic: String::new(),
            token: None,
        };
        sink.deliver(&sample_notification());

        // Give any (incorrect) delivery a chance to land, then confirm none did.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert_eq!(server.received_requests().await.map(|r| r.len()), Some(0));
    }
}
