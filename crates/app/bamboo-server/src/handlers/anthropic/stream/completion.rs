use bamboo_llm::api::models::ChatCompletionStreamChunk;
use serde_json::{json, Value};

use super::super::conversion::map_stop_reason_complete;
use super::format::format_sse_data;

pub(crate) fn map_completion_stream_chunk(
    chunk: &ChatCompletionStreamChunk,
    model: &str,
) -> String {
    let mut output = String::new();
    let model_name = if model.is_empty() {
        chunk.model.clone().unwrap_or_else(|| model.to_string())
    } else {
        model.to_string()
    };

    for choice in &chunk.choices {
        let mut completion = String::new();
        if let Some(content) = &choice.delta.content {
            completion.push_str(content);
        }

        if !completion.is_empty() {
            let data = json!({
                "type": "completion",
                "completion": completion,
                "model": model_name.clone(),
                "stop_reason": Value::Null
            });
            output.push_str(&format_sse_data(data));
        }

        if let Some(reason) = &choice.finish_reason {
            let data = json!({
                "type": "completion",
                "completion": "",
                "model": model_name.clone(),
                "stop_reason": map_stop_reason_complete(Some(reason.as_str()))
            });
            output.push_str(&format_sse_data(data));
        }
    }

    output
}
