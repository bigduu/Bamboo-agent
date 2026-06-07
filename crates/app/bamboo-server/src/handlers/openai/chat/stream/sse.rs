use bytes::Bytes;

use bamboo_llm::types::LLMChunk;

use super::super::super::helpers::convert_chunk_to_openai;

pub(super) fn openai_chunk_bytes(chunk: LLMChunk, model: &str) -> Option<Bytes> {
    convert_chunk_to_openai(chunk, model)
        .map(|openai_chunk| serde_json::to_string(&openai_chunk).unwrap_or_default())
        .map(Bytes::from)
}

pub(super) fn done_marker_bytes() -> Bytes {
    Bytes::from_static(b"[DONE]")
}

pub(super) fn wrap_sse_data(bytes: Bytes) -> Bytes {
    let data = format!("data: {}\n\n", String::from_utf8_lossy(&bytes));
    Bytes::from(data)
}
