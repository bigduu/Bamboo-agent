use anyhow::Result;
use futures::StreamExt;
use tokio::sync::{mpsc, watch};

use super::types::AgentEvent;

pub struct SseStream;

#[derive(Debug)]
pub enum SessionSseEvent {
    Event {
        session_id: String,
        stream_epoch: u64,
        event: Box<AgentEvent>,
    },
    /// An initial connection or retry reached a successful SSE response. A
    /// question may have been persisted before the server subscribed this
    /// connection, so every handshake reconciles authoritative pending state.
    Connected {
        session_id: String,
        stream_epoch: u64,
        reconnecting: bool,
    },
    /// The stream cannot continue (non-retryable HTTP error or retry budget
    /// exhausted). This is transport state, not an agent terminal event.
    TransportFailed {
        session_id: String,
        stream_epoch: u64,
        message: String,
    },
}

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
        stream_epoch: u64,
        tx: mpsc::UnboundedSender<SessionSseEvent>,
    ) -> Result<(tokio::task::JoinHandle<()>, watch::Receiver<bool>)> {
        let url = format!("{}/api/v1/events/{}", base_url, session_id);
        let session_id = session_id.to_string();
        let client = reqwest::Client::new();
        let (ready_tx, ready_rx) = watch::channel(false);

        let task = tokio::spawn(async move {
            // Reconnect with capped exponential backoff on an unexpected drop
            // (network blip, EOF before a terminal event). Critical-event replay
            // does not include every stateful event, and the initial subscribe
            // can race execution startup, so every successful handshake tells
            // the app to reconcile the pending-question endpoint.
            let mut attempt: u32 = 0;
            loop {
                let terminal_seen = Self::consume_once(
                    &client,
                    &url,
                    &session_id,
                    stream_epoch,
                    attempt > 0,
                    &tx,
                    &ready_tx,
                )
                .await;
                if terminal_seen {
                    return; // protocol [DONE] / non-retryable error — done.
                }
                ready_tx.send(false).ok();
                // The UI dropped the receiver ⇒ nothing to reconnect for.
                if tx.is_closed() {
                    return;
                }
                attempt += 1;
                if attempt > Self::MAX_RETRIES {
                    tx.send(SessionSseEvent::TransportFailed {
                        session_id: session_id.clone(),
                        stream_epoch,
                        message: "SSE stream lost and reconnect gave up after retries".to_string(),
                    })
                    .ok();
                    return;
                }
                let backoff_ms = 200u64.saturating_mul(1u64 << (attempt - 1)).min(5_000);
                tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
            }
        });

        Ok((task, ready_rx))
    }

    /// One connect + consume cycle. Returns `true` when the protocol `[DONE]`
    /// sentinel (or a non-retryable client error) was seen — the caller should
    /// then stop. Returns `false` on a retryable outcome (connect error, 5xx,
    /// or the stream ending without `[DONE]`), so the caller reconnects.
    async fn consume_once(
        client: &reqwest::Client,
        url: &str,
        session_id: &str,
        stream_epoch: u64,
        reconnecting: bool,
        tx: &mpsc::UnboundedSender<SessionSseEvent>,
        ready_tx: &watch::Sender<bool>,
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
                tx.send(SessionSseEvent::TransportFailed {
                    session_id: session_id.to_string(),
                    stream_epoch,
                    message: format!("SSE connection failed: {} - {}", status, body),
                })
                .ok();
                return true;
            }
            return false;
        }

        // A successful HTTP response means the server has installed this SSE
        // subscription. Answer submission waits on this signal so a resumed
        // run cannot emit its first token before the TUI is listening.
        ready_tx.send(true).ok();
        if tx
            .send(SessionSseEvent::Connected {
                session_id: session_id.to_string(),
                stream_epoch,
                reconnecting,
            })
            .is_err()
        {
            return true;
        }

        let mut stream = resp.bytes_stream();
        let mut buffer = Vec::new();
        while let Some(chunk) = stream.next().await {
            if tx.is_closed() {
                return true;
            }
            let chunk = match chunk {
                Ok(c) => c,
                Err(_) => return false, // mid-stream error → reconnect
            };
            // Preserve raw bytes across transport chunks. A UTF-8 scalar may
            // be split between chunks; lossy-decoding each chunk separately
            // would silently replace it with U+FFFD.
            buffer.extend_from_slice(&chunk);
            // SSE events are separated by blank lines.
            while let Some((sep_pos, sep_len)) = Self::find_event_separator(&buffer) {
                let remainder = buffer.split_off(sep_pos + sep_len);
                let mut event_bytes = std::mem::replace(&mut buffer, remainder);
                event_bytes.truncate(sep_pos);
                let Ok(event_text) = std::str::from_utf8(&event_bytes) else {
                    // SSE is UTF-8 by contract. Skip a malformed complete
                    // frame, but never corrupt a valid scalar split across
                    // network chunks.
                    continue;
                };
                if Self::parse_sse_block(event_text, session_id, stream_epoch, tx) {
                    return true; // protocol sentinel delivered
                }
            }
        }
        // Stream ended without [DONE] (unexpected EOF) → reconnect.
        false
    }

    /// Parse one SSE block and forward its events. Agent terminal events do
    /// not necessarily close the transport: the server keeps the session SSE
    /// alive while background children emit lifecycle events. Only the
    /// protocol `[DONE]` sentinel (or a closed receiver) ends this consumer.
    fn parse_sse_block(
        block: &str,
        session_id: &str,
        stream_epoch: u64,
        tx: &mpsc::UnboundedSender<SessionSseEvent>,
    ) -> bool {
        for line in block.lines() {
            // Skip comments (heartbeat, etc.)
            if line.starts_with(':') {
                continue;
            }
            if let Some(data) = line.strip_prefix("data: ") {
                if data == "[DONE]" {
                    return true;
                }
                if data == "[KEEPALIVE]" {
                    continue;
                }
                if let Ok(event) = serde_json::from_str::<AgentEvent>(data) {
                    if tx
                        .send(SessionSseEvent::Event {
                            session_id: session_id.to_string(),
                            stream_epoch,
                            event: Box::new(event),
                        })
                        .is_err()
                    {
                        return true; // receiver gone — stop
                    }
                }
            }
        }
        false
    }

    fn find_event_separator(buffer: &[u8]) -> Option<(usize, usize)> {
        buffer
            .windows(2)
            .position(|window| window == b"\n\n")
            .map(|position| (position, 2))
            .or_else(|| {
                buffer
                    .windows(4)
                    .position(|window| window == b"\r\n\r\n")
                    .map(|position| (position, 4))
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn parse_block_only_closes_on_done_sentinel() {
        let (tx, mut rx) = mpsc::unbounded_channel();

        // A token is not terminal.
        let terminal =
            SseStream::parse_sse_block(r#"data: {"type":"token","content":"hi"}"#, "s1", 7, &tx);
        assert!(!terminal);
        let event = rx.try_recv().unwrap();
        assert!(matches!(
            event,
            SessionSseEvent::Event {
                session_id,
                stream_epoch: 7,
                event,
            } if session_id == "s1" && matches!(event.as_ref(), AgentEvent::Token { .. })
        ));

        // A heartbeat comment / keepalive is skipped, not terminal.
        assert!(!SseStream::parse_sse_block(": heartbeat", "s1", 7, &tx));
        assert!(!SseStream::parse_sse_block(
            "data: [KEEPALIVE]",
            "s1",
            7,
            &tx
        ));
        assert!(rx.try_recv().is_err());

        // Parent completion is forwarded but the transport remains open for
        // late background-child lifecycle events.
        let terminal = SseStream::parse_sse_block(
            r#"data: {"type":"complete","usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}"#,
            "s1",
            7,
            &tx,
        );
        assert!(!terminal);
        assert!(matches!(
            rx.try_recv().unwrap(),
            SessionSseEvent::Event {
                event,
                ..
            } if matches!(event.as_ref(), AgentEvent::Complete { .. })
        ));

        assert!(!SseStream::parse_sse_block(
            r#"data: {"type":"sub_agent_completed","child_session_id":"child-1","status":"completed"}"#,
            "s1",
            7,
            &tx,
        ));
        assert!(matches!(
            rx.try_recv().unwrap(),
            SessionSseEvent::Event {
                event,
                ..
            } if matches!(event.as_ref(), AgentEvent::SubAgentCompleted { .. })
        ));

        assert!(SseStream::parse_sse_block("data: [DONE]", "s1", 7, &tx));
    }

    #[test]
    fn closed_receiver_is_reported_terminal() {
        let (tx, rx) = mpsc::unbounded_channel::<SessionSseEvent>();
        drop(rx);
        // With no receiver, sending fails and the block reports "stop".
        assert!(SseStream::parse_sse_block(
            r#"data: {"type":"token","content":"x"}"#,
            "s1",
            1,
            &tx
        ));
    }

    #[tokio::test]
    async fn split_utf8_scalar_across_http_chunks_is_lossless() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut byte = [0u8; 1];
            while !request.ends_with(b"\r\n\r\n") {
                socket.read_exact(&mut byte).await.unwrap();
                request.push(byte[0]);
            }
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\nConnection: keep-alive\r\n\r\n",
                )
                .await
                .unwrap();

            let frame = "data: {\"type\":\"token\",\"content\":\"你好🙂\"}\n\n";
            let emoji = frame.find('🙂').unwrap();
            // Split halfway through the four-byte emoji scalar. Per-chunk
            // lossy decoding would produce replacement characters here.
            let parts = [
                &frame.as_bytes()[..emoji + 2],
                &frame.as_bytes()[emoji + 2..],
            ];
            for part in parts {
                socket
                    .write_all(format!("{:X}\r\n", part.len()).as_bytes())
                    .await
                    .unwrap();
                socket.write_all(part).await.unwrap();
                socket.write_all(b"\r\n").await.unwrap();
                socket.flush().await.unwrap();
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
            std::future::pending::<()>().await;
        });

        let (tx, mut rx) = mpsc::unbounded_channel();
        let (task, _ready) = SseStream::start(&base_url, "unicode", 42, tx).unwrap();
        assert!(matches!(
            tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
                .await
                .unwrap()
                .unwrap(),
            SessionSseEvent::Connected {
                stream_epoch: 42,
                ..
            }
        ));
        let event = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            event,
            SessionSseEvent::Event {
                stream_epoch: 42,
                event,
                ..
            } if matches!(
                event.as_ref(),
                AgentEvent::Token { content } if content == "你好🙂"
            )
        ));

        task.abort();
        server.abort();
    }
}
