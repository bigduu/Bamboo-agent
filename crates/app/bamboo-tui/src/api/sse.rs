use anyhow::Result;
use futures::StreamExt;
use tokio::sync::mpsc;

use super::types::AgentEvent;

pub struct SseStream;

impl SseStream {
    /// Max reconnect attempts after an unexpected drop (before a terminal event).
    const MAX_RETRIES: u32 = 6;

    /// Connect to the SSE endpoint and forward parsed AgentEvents to a channel,
    /// reconnecting on an unexpected drop until a terminal event.
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
            // Reconnect with capped exponential backoff on an unexpected drop
            // (network blip, EOF before a terminal event). The server's
            // per-session feed re-sends only cached critical events on reconnect
            // and then live-tails, so this does NOT replay the whole token
            // history — at worst a few tokens emitted during the gap are missed.
            let mut attempt: u32 = 0;
            loop {
                let terminal_seen = Self::consume_once(&client, &url, &tx).await;
                if terminal_seen {
                    return; // run completed / cancelled / hard error — done.
                }
                // The UI dropped the receiver ⇒ nothing to reconnect for.
                if tx.is_closed() {
                    return;
                }
                attempt += 1;
                if attempt > Self::MAX_RETRIES {
                    tx.send(AgentEvent::Error {
                        message: "SSE stream lost and reconnect gave up after retries".to_string(),
                    })
                    .ok();
                    return;
                }
                let backoff_ms = 200u64.saturating_mul(1u64 << (attempt - 1)).min(5_000);
                tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
            }
        });

        Ok(())
    }

    /// One connect + consume cycle. Returns `true` when a terminal event (or a
    /// non-retryable client error) was seen — the caller should then stop.
    /// Returns `false` on a retryable outcome (connect error, 5xx, or the stream
    /// ending without a terminal event), so the caller reconnects.
    async fn consume_once(
        client: &reqwest::Client,
        url: &str,
        tx: &mpsc::UnboundedSender<AgentEvent>,
    ) -> bool {
        let resp = match client.get(url).send().await {
            Ok(resp) => resp,
            Err(_) => return false, // connect error → retry
        };

        if !resp.status().is_success() {
            let status = resp.status();
            // A client error (e.g. 404 session gone, 401) will not fix itself —
            // report and stop. A 5xx is transient → retry.
            if status.is_client_error() {
                let body = resp.text().await.unwrap_or_default();
                tx.send(AgentEvent::Error {
                    message: format!("SSE connection failed: {} - {}", status, body),
                })
                .ok();
                return true;
            }
            return false;
        }

        let mut stream = resp.bytes_stream();
        let mut buffer = String::new();
        while let Some(chunk) = stream.next().await {
            let chunk = match chunk {
                Ok(c) => c,
                Err(_) => return false, // mid-stream error → reconnect
            };
            buffer.push_str(&String::from_utf8_lossy(&chunk));
            // SSE events are separated by blank lines.
            while let Some(sep_pos) = buffer.find("\n\n") {
                let event_text = buffer[..sep_pos].to_string();
                buffer = buffer[sep_pos + 2..].to_string();
                if Self::parse_sse_block(&event_text, tx) {
                    return true; // terminal event delivered
                }
            }
        }
        // Stream ended without a terminal event (unexpected EOF) → reconnect.
        false
    }

    /// Parse one SSE block and forward its events. Returns `true` if a terminal
    /// event (Complete / Cancelled / Error) or a closed receiver was seen.
    fn parse_sse_block(block: &str, tx: &mpsc::UnboundedSender<AgentEvent>) -> bool {
        for line in block.lines() {
            // Skip comments (heartbeat, etc.)
            if line.starts_with(':') {
                continue;
            }
            if let Some(data) = line.strip_prefix("data: ") {
                if data == "[DONE]" || data == "[KEEPALIVE]" {
                    continue;
                }
                if let Ok(event) = serde_json::from_str::<AgentEvent>(data) {
                    let is_terminal = matches!(
                        &event,
                        AgentEvent::Complete { .. }
                            | AgentEvent::Cancelled { .. }
                            | AgentEvent::Error { .. }
                    );
                    if tx.send(event).is_err() {
                        return true; // receiver gone — stop
                    }
                    if is_terminal {
                        return true;
                    }
                }
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_block_flags_terminal_events_only() {
        let (tx, mut rx) = mpsc::unbounded_channel();

        // A token is not terminal.
        let terminal = SseStream::parse_sse_block(r#"data: {"type":"token","content":"hi"}"#, &tx);
        assert!(!terminal);
        assert!(rx.try_recv().is_ok());

        // A heartbeat comment / keepalive is skipped, not terminal.
        assert!(!SseStream::parse_sse_block(": heartbeat", &tx));
        assert!(!SseStream::parse_sse_block("data: [KEEPALIVE]", &tx));
        assert!(rx.try_recv().is_err());

        // Complete is terminal.
        let terminal = SseStream::parse_sse_block(
            r#"data: {"type":"complete","usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}"#,
            &tx,
        );
        assert!(terminal);
    }

    #[test]
    fn closed_receiver_is_reported_terminal() {
        let (tx, rx) = mpsc::unbounded_channel::<AgentEvent>();
        drop(rx);
        // With no receiver, sending fails and the block reports "stop".
        assert!(SseStream::parse_sse_block(
            r#"data: {"type":"token","content":"x"}"#,
            &tx
        ));
    }
}
