//! Borrow the common single-text frame. Other wire shapes use the full decoder.

use std::borrow::Cow;
use std::fmt;
use std::marker::PhantomData;

use serde::de::{value::MapAccessDeserializer, MapAccess, Visitor};
use serde::{Deserialize, Deserializer};

// Serde's derived structs also accept positional arrays. Gemini's full
// decoder reads named fields from JSON objects at each of these levels.
struct Object<T>(T);

impl<'de, T: Deserialize<'de>> Deserialize<'de> for Object<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ObjectVisitor<T>(PhantomData<T>);

        impl<'de, T: Deserialize<'de>> Visitor<'de> for ObjectVisitor<T> {
            type Value = Object<T>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a JSON object")
            }

            fn visit_map<M: MapAccess<'de>>(self, map: M) -> Result<Self::Value, M::Error> {
                T::deserialize(MapAccessDeserializer::new(map)).map(Object)
            }
        }

        deserializer.deserialize_map(ObjectVisitor(PhantomData))
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Frame<'a> {
    #[serde(borrow)]
    candidates: [Object<Candidate<'a>>; 1],
    #[serde(rename = "modelVersion", borrow)]
    _model_version: Option<Cow<'a, str>>,
    #[serde(rename = "responseId", borrow)]
    _response_id: Option<Cow<'a, str>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Candidate<'a> {
    #[serde(borrow)]
    content: Object<Content<'a>>,
    #[serde(rename = "index")]
    _index: Option<u32>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Content<'a> {
    #[serde(borrow)]
    parts: [Object<Part<'a>>; 1],
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
    let Object(frame) = serde_json::from_str::<Object<Frame<'_>>>(data).ok()?;
    let [Object(candidate)] = frame.candidates;
    let [Object(part)] = candidate.content.0.parts;
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
