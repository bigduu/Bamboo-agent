use serde_json::Value;

pub(crate) fn format_sse_event(event: &str, data: Value) -> String {
    format!("event: {}\ndata: {}\n\n", event, data)
}

pub(crate) fn format_sse_data(data: Value) -> String {
    format!("data: {}\n\n", data)
}
