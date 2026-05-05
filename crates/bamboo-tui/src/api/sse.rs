use anyhow::Result;
use futures::StreamExt;
use tokio::sync::mpsc;

use super::types::AgentEvent;

pub struct SseStream;

impl SseStream {
    /// Connect to SSE endpoint and forward parsed AgentEvents to a channel.
    ///
    /// Bamboo server format: `data: {json}\n\n` with `: heartbeat\n\n` comments.
    pub fn start(
        base_url: &str,
        session_id: &str,
        tx: mpsc::UnboundedSender<AgentEvent>,
    ) -> Result<()> {
        let url = format!("{}/api/v1/events/{}", base_url, session_id);
        let client = reqwest::Client::new();

        tokio::spawn(async move {
            let response = client.get(&url).send().await;
            match response {
                Ok(resp) => {
                    if !resp.status().is_success() {
                        let status = resp.status();
                        let body = resp.text().await.unwrap_or_default();
                        tx.send(AgentEvent::Error {
                            message: format!("SSE connection failed: {} - {}", status, body),
                        })
                        .ok();
                        return;
                    }

                    let mut stream = resp.bytes_stream();
                    let mut buffer = String::new();

                    while let Some(chunk) = stream.next().await {
                        let chunk = match chunk {
                            Ok(c) => c,
                            Err(e) => {
                                tx.send(AgentEvent::Error {
                                    message: format!("SSE stream error: {}", e),
                                })
                                .ok();
                                return;
                            }
                        };

                        buffer.push_str(&String::from_utf8_lossy(&chunk));

                        // Process complete SSE events.
                        // SSE spec: events are separated by blank lines (\n\n or \r\n\r\n).
                        while let Some(sep_pos) = buffer.find("\n\n") {
                            let event_text = buffer[..sep_pos].to_string();
                            buffer = buffer[sep_pos + 2..].to_string();
                            Self::parse_sse_block(&event_text, &tx);
                        }
                    }
                }
                Err(e) => {
                    tx.send(AgentEvent::Error {
                        message: format!("SSE connect error: {}", e),
                    })
                    .ok();
                }
            }
        });

        Ok(())
    }

    fn parse_sse_block(block: &str, tx: &mpsc::UnboundedSender<AgentEvent>) {
        for line in block.lines() {
            // Skip comments (heartbeat, etc.)
            if line.starts_with(':') {
                continue;
            }
            if let Some(data) = line.strip_prefix("data: ") {
                if data == "[DONE]" || data == "[KEEPALIVE]" {
                    return;
                }
                if let Ok(event) = serde_json::from_str::<AgentEvent>(data) {
                    let is_terminal = matches!(
                        &event,
                        AgentEvent::Complete { .. }
                            | AgentEvent::Cancelled { .. }
                            | AgentEvent::Error { .. }
                    );
                    if tx.send(event).is_err() {
                        return;
                    }
                    if is_terminal {
                        return;
                    }
                }
            }
        }
    }
}
