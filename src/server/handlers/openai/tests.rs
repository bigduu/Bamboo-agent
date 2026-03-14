use crate::agent::llm::api::models::{Content, ContentPart, Role};

use super::helpers::responses_input_to_chat_messages;

#[test]
fn responses_input_string_becomes_single_user_message() {
    let msgs = responses_input_to_chat_messages(serde_json::json!("hi")).unwrap();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].role, Role::User);
    match &msgs[0].content {
        Content::Text(t) => assert_eq!(t, "hi"),
        _ => panic!("expected text content"),
    }
}

#[test]
fn responses_input_array_parses_role_and_content_string() {
    let msgs = responses_input_to_chat_messages(serde_json::json!([
        { "role": "system", "content": "s" },
        { "role": "user", "content": "u" },
        { "role": "assistant", "content": "a" }
    ]))
    .unwrap();
    assert_eq!(msgs.len(), 3);
    assert_eq!(msgs[0].role, Role::System);
    assert_eq!(msgs[1].role, Role::User);
    assert_eq!(msgs[2].role, Role::Assistant);
}

#[test]
fn responses_input_parts_support_input_text() {
    let msgs = responses_input_to_chat_messages(serde_json::json!([
        {
          "role": "user",
          "content": [{ "type": "input_text", "text": "hello" }]
        }
    ]))
    .unwrap();
    assert_eq!(msgs.len(), 1);
    match &msgs[0].content {
        Content::Parts(parts) => {
            assert_eq!(parts.len(), 1);
            match &parts[0] {
                ContentPart::Text { text } => assert_eq!(text, "hello"),
                _ => panic!("expected text part"),
            }
        }
        _ => panic!("expected parts content"),
    }
}
