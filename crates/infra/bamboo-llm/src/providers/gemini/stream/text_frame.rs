//! Borrow the common single-text frame. Other wire shapes use the full decoder.

use std::borrow::Cow;

use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Frame<'a> {
    #[serde(borrow)]
    candidates: [Candidate<'a>; 1],
    #[serde(rename = "modelVersion", borrow)]
    _model_version: Option<Cow<'a, str>>,
    #[serde(rename = "responseId", borrow)]
    _response_id: Option<Cow<'a, str>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Candidate<'a> {
    #[serde(borrow)]
    content: Content<'a>,
    #[serde(rename = "index")]
    _index: Option<u32>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Content<'a> {
    #[serde(borrow)]
    parts: [Part<'a>; 1],
    #[serde(rename = "role", borrow)]
    _role: Option<Cow<'a, str>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Part<'a> {
    #[serde(borrow)]
    text: Cow<'a, str>,
}

pub(super) fn decode(data: &str) -> Option<Cow<'_, str>> {
    let Frame {
        candidates:
            [Candidate {
                content: Content { parts: [part], .. },
                ..
            }],
        ..
    } = serde_json::from_str(data).ok()?;
    Some(part.text)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_frames_borrow_plain_text_and_own_escaped_text() {
        let plain = r#"{"candidates":[{"content":{"parts":[{"text":"中文"}],"role":"model"},"index":0}],"modelVersion":"gemini-test","responseId":"fixture"}"#;
        assert!(matches!(decode(plain), Some(Cow::Borrowed("中文"))));
        let escaped = r#"{"candidates":[{"content":{"parts":[{"text":"中文\nnext"}]}}]}"#;
        assert!(matches!(decode(escaped), Some(Cow::Owned(text)) if text == "中文\nnext"));
        let duplicate = r#"{"candidates":[{"content":{"parts":[{"text":"old","text":"new"}]}}]}"#;
        assert!(decode(duplicate).is_none());
    }
}
