//! Bounded HTTP and current-launch log evidence for ordinary Root test fixtures.
use std::{
    fmt,
    io::{Read, Seek, SeekFrom},
    path::Path,
};

pub(crate) const LOG_TAIL_BYTES: u64 = 16 * 1024;
const HTTP_BODY_BYTES: usize = 1024;
const HTTP_HEADER_BYTES: usize = 256;
const HTTP_ERROR_BYTES: usize = 512;

#[derive(Default)]
pub(crate) struct HttpEvidence {
    summary: String,
    body: Vec<u8>,
    body_read: String,
}

impl fmt::Display for HttpEvidence {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.summary.is_empty() {
            return output.write_str("no HTTP result captured");
        }
        output.write_str(&self.summary)?;
        if !self.body_read.is_empty() {
            write!(
                output,
                " body={:?} body_read={}",
                String::from_utf8_lossy(&self.body),
                self.body_read
            )?;
        }
        Ok(())
    }
}

fn error_summary(error: &reqwest::Error) -> String {
    let mut summary = format!("{error:?}");
    if summary.len() > HTTP_ERROR_BYTES {
        summary.truncate(summary.floor_char_boundary(HTTP_ERROR_BYTES));
        summary.push_str(" (truncated)");
    }
    summary
}

pub(crate) async fn observe_http(
    response: Result<reqwest::Response, reqwest::Error>,
    evidence: &mut HttpEvidence,
) -> bool {
    *evidence = HttpEvidence::default();
    let mut response = match response {
        Ok(response) => response,
        Err(error) => {
            evidence.summary = format!("transport_error={}", error_summary(&error));
            return false;
        }
    };
    let status = response.status();
    let headers: Vec<_> = [
        "content-type",
        "content-length",
        "retry-after",
        "server",
        "x-request-id",
    ]
    .into_iter()
    .filter_map(|name| {
        let value = response.headers().get(name)?.as_bytes();
        let prefix = String::from_utf8_lossy(&value[..value.len().min(HTTP_HEADER_BYTES)]);
        let suffix = if value.len() > HTTP_HEADER_BYTES {
            " (truncated)"
        } else {
            ""
        };
        Some(format!("{name}={prefix:?}{suffix}"))
    })
    .collect();
    evidence.summary = format!(
        "status={status} remote_addr={:?} headers=[{}]",
        response.remote_addr(),
        headers.join(", ")
    );
    if status.is_success() {
        return true;
    }

    // Update the receipt as chunks arrive. Readiness may cancel the body read
    // at the next polling instant while retaining the status and prefix.
    evidence.body_read = "incomplete".into();
    loop {
        match response.chunk().await {
            Ok(Some(chunk)) => {
                let remaining = HTTP_BODY_BYTES - evidence.body.len();
                evidence
                    .body
                    .extend_from_slice(&chunk[..chunk.len().min(remaining)]);
                if chunk.len() > remaining {
                    evidence.body_read = format!("truncated at {HTTP_BODY_BYTES} bytes");
                    break;
                }
            }
            Ok(None) => {
                evidence.body_read = "complete".into();
                break;
            }
            Err(error) => {
                evidence.body_read = format!("error: {}", error_summary(&error));
                break;
            }
        }
    }
    false
}

pub(crate) fn current_log_tail(path: &Path, start: u64) -> std::io::Result<Vec<u8>> {
    let mut file = std::fs::File::open(path)?;
    let length = file.metadata()?.len();
    if length < start {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Host log was truncated after this launch started",
        ));
    }
    file.seek(SeekFrom::Start(
        start.max(length.saturating_sub(LOG_TAIL_BYTES)),
    ))?;
    let mut bytes = Vec::new();
    file.take(LOG_TAIL_BYTES).read_to_end(&mut bytes)?;
    Ok(bytes)
}
