use actix_web::http::header;
use actix_web::{web, HttpResponse, Responder};
use tokio::sync::broadcast;

use crate::server::app_state::AppState;

use self::frame::serialize_event_frame;
use self::terminal::{is_terminal_event, terminal_event_from_status};

mod frame;
mod terminal;

fn sse_response(
    stream: impl futures::Stream<Item = Result<web::Bytes, actix_web::Error>> + 'static,
) -> HttpResponse {
    HttpResponse::Ok()
        .append_header((header::CONTENT_TYPE, "text/event-stream"))
        .append_header((header::CACHE_CONTROL, "no-cache"))
        .append_header((header::CONNECTION, "keep-alive"))
        .streaming(stream)
}

/// Streams Claude session events via SSE.
pub async fn claude_events(state: web::Data<AppState>, path: web::Path<String>) -> impl Responder {
    let session_id = path.into_inner();

    let (runner_status, mut receiver) = {
        let runners = state.claude_runners.read().await;
        let Some(runner) = runners.get(&session_id) else {
            return HttpResponse::NotFound().json(serde_json::json!({
                "error": "Claude session not running",
                "session_id": session_id,
            }));
        };
        (runner.status.clone(), runner.event_sender.subscribe())
    };

    if let Some(terminal_event) = terminal_event_from_status(&runner_status) {
        return sse_response(async_stream::stream! {
            if let Some(frame) = serialize_event_frame(&terminal_event) {
                yield Ok::<_, actix_web::Error>(frame);
            }
        });
    }

    sse_response(async_stream::stream! {
        loop {
            match receiver.recv().await {
                Ok(event) => {
                    let terminal = is_terminal_event(&event);
                    if let Some(frame) = serialize_event_frame(&event) {
                        yield Ok::<_, actix_web::Error>(frame);
                    }
                    if terminal {
                        break;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    })
}
