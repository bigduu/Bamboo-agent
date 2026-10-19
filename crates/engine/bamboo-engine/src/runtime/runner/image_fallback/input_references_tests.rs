use super::*;
use bamboo_agent_core::storage::Storage;
use bamboo_agent_core::{Tool, ToolCtx, ToolOutcome};
use bamboo_tools::tools::view_image::{with_image_reference_reader, ViewImageTool};
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};

const PNG: &[u8] = b"\x89PNG\r\n\x1a\nreference-fixture";

fn message(url: &str) -> Message {
    Message::user_with_parts(
        "inspect retained image",
        vec![
            MessagePart::Text {
                text: "inspect retained image".into(),
            },
            MessagePart::ImageUrl {
                image_url: bamboo_domain::ImageUrlRef {
                    url: url.into(),
                    detail: None,
                },
            },
        ],
    )
}

fn tool_ctx(session_id: &str) -> ToolCtx {
    let mut ctx = ToolCtx::none("reference-call");
    ctx.session_id = Some(Arc::from(session_id));
    ctx
}

#[tokio::test]
async fn uploaded_and_inline_history_images_remain_durable_and_resolve_without_temp_paths() {
    for inline_history in [false, true] {
        let home = tempfile::tempdir().unwrap();
        let store = Arc::new(
            bamboo_storage::SessionStoreV2::new(home.path().to_path_buf())
                .await
                .unwrap(),
        );
        let mut session = Session::new("reference-session", "text-only");
        store.save_session(&session).await.unwrap();
        let encoded = base64::engine::general_purpose::STANDARD.encode(PNG);
        let (attachment_id, url) = if inline_history {
            (None, format!("data:image/png;base64,{encoded}"))
        } else {
            let (id, url) = store
                .write_image_attachment(&session, &encoded, Some("image/png"))
                .await
                .unwrap();
            (Some(id), url)
        };
        let image = message(&url);
        let reference = conversation_image_reference(&session.id, &image.id, 1, &url);
        session.add_message(image);
        if inline_history {
            session.add_message(Message::assistant("previous response", None));
            session.add_message(Message::user("now inspect the earlier image"));
        }
        store.save_session(&session).await.unwrap();
        let original = serde_json::to_vec(&session.messages).unwrap();
        let session_file = store
            .sessions_root_dir()
            .join(&session.id)
            .join("session.json");
        let original_record_bytes = tokio::fs::read(&session_file).await.unwrap();
        let stored_original = serde_json::to_vec(
            &store
                .load_session(&session.id)
                .await
                .unwrap()
                .unwrap()
                .messages,
        )
        .unwrap();

        let mut prepared = session.messages.clone();
        project_nonvision_images(&mut prepared, &session.id);
        assert!(prepared.iter().all(|m| m.content_parts.is_none()));
        assert!(prepared[0].content.contains(&reference));
        assert!(prepared[0].content.contains("not inspected"));
        assert!(prepared[0].content.contains("defaults.vision"));
        assert!(!prepared[0].content.contains(&encoded));
        assert!(reference.starts_with("bamboo-"));
        assert!(!reference.starts_with('/'));
        if inline_history {
            assert_eq!(
                prepared.last().unwrap().content,
                "now inspect the earlier image"
            );
        }
        assert_eq!(serde_json::to_vec(&session.messages).unwrap(), original);

        let reader = image_reference_reader(&session, Some(store.clone()));
        let result = with_image_reference_reader(
            &session.id,
            reader,
            ViewImageTool::new().invoke(json!({"path": reference}), tool_ctx(&session.id)),
        )
        .await
        .unwrap();
        let ToolOutcome::Completed(result) = result else {
            panic!("completed image load")
        };
        assert!(result.success);
        assert_eq!(result.images[0].data, encoded);
        assert_eq!(result.images[0].mime_type, "image/png");
        assert_eq!(serde_json::to_vec(&session.messages).unwrap(), original);
        assert_eq!(
            serde_json::to_vec(
                &store
                    .load_session(&session.id)
                    .await
                    .unwrap()
                    .unwrap()
                    .messages
            )
            .unwrap(),
            stored_original,
        );
        assert_eq!(
            tokio::fs::read(&session_file).await.unwrap(),
            original_record_bytes,
            "the durable session record bytes are unchanged",
        );
        if let Some(id) = attachment_id {
            assert_eq!(
                store
                    .read_attachment(&session.id, &id)
                    .await
                    .unwrap()
                    .unwrap()
                    .0,
                PNG
            );
        }
        assert!(
            ViewImageTool::new()
                .invoke(json!({"path": reference}), tool_ctx(&session.id))
                .await
                .is_err(),
            "a completed dispatch leaves no ambient reference authority"
        );
    }
}

struct CountingReader(Arc<AtomicUsize>);

#[async_trait::async_trait]
impl AttachmentReader for CountingReader {
    async fn read_attachment(
        &self,
        _: &str,
        _: &str,
    ) -> std::io::Result<Option<(Vec<u8>, String)>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(Some((PNG.to_vec(), "image/png".into())))
    }
}

#[tokio::test]
async fn forged_cross_session_and_malformed_references_deny_without_reader_or_file_fallback() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut session = Session::new("current", "text-only");
    session.add_message(message("bamboo-attachment://current/known"));
    // Even a foreign/malformed URI already present in raw history grants no access.
    session.add_message(message("bamboo-attachment://other/known"));
    session.add_message(message("bamboo-attachment://current/../known"));
    let original = serde_json::to_vec(&session.messages).unwrap();
    for reference in [
        "bamboo-attachment://other/known",
        "bamboo-attachment://current/../known",
        "bamboo-attachment://current/unknown",
        "bamboo-attachment://current/",
        "bamboo-image://other/message/1",
        "bamboo-image://current/missing/0",
    ] {
        let reader =
            image_reference_reader(&session, Some(Arc::new(CountingReader(calls.clone()))));
        let error = with_image_reference_reader(
            &session.id,
            reader,
            ViewImageTool::new().invoke(json!({"path": reference}), tool_ctx(&session.id)),
        )
        .await
        .expect_err("forged reference rejected");
        assert!(!error.to_string().contains("Failed to inspect image"));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
    let reader = image_reference_reader(&session, Some(Arc::new(CountingReader(calls.clone()))));
    assert!(
        with_image_reference_reader(
            &session.id,
            reader,
            ViewImageTool::new().invoke(
                json!({"path": "bamboo-attachment://current/known"}),
                tool_ctx("other"),
            ),
        )
        .await
        .is_err(),
        "same URI cannot be borrowed by another executing Session"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(serde_json::to_vec(&session.messages).unwrap(), original);
}

#[test]
fn image_reference_scope_is_only_prepared_for_callable_reference_dispatch() {
    use bamboo_agent_core::tools::ToolExecutor;

    let schemas = bamboo_tools::BuiltinToolExecutor::new().list_tools();
    let callable = crate::runtime::runner::tool_execution::legacy_effective_callable_set(&schemas);
    let core_only = bamboo_domain::EffectiveCallableSet::from_catalog(
        &schemas
            .into_iter()
            .filter_map(bamboo_domain::ClassifiedToolSchema::new)
            .collect::<Vec<_>>(),
        bamboo_domain::CapabilityLoadingMode::Progressive,
        std::iter::empty::<&str>(),
    );
    let call = |name: &str, args: serde_json::Value| {
        serde_json::from_value::<bamboo_agent_core::ToolCall>(json!({
            "id": "scoped-reference", "type": "function",
            "function": {"name": name, "arguments": args.to_string()}
        }))
        .unwrap()
    };
    for name in ["ViewImage", "view_image", "default::viewImage"] {
        let reference = call(name, json!({"path": "bamboo-image://current/message/0"}));
        assert!(round_needs_image_references(
            std::slice::from_ref(&reference),
            &callable,
            false
        ));
        assert!(!round_needs_image_references(
            &[reference],
            &core_only,
            false
        ));
    }
    let mut repaired = call("view_image", json!({}));
    repaired.function.arguments = r#"{"path":"bamboo-image://current/message/0""#.into();
    assert!(
        round_needs_image_references(&[repaired], &callable, false),
        "match the existing executor's best-effort argument repair"
    );
    assert!(!round_needs_image_references(
        &[call(
            "Read",
            json!({"path": "bamboo-image://current/message/0"})
        )],
        &callable,
        false,
    ));
    let local = call("ViewImage", json!({"path": "/tmp/local-image.png"}));
    assert!(!round_needs_image_references(
        std::slice::from_ref(&local),
        &callable,
        false
    ));
    assert!(
        round_needs_image_references(&[local], &callable, true),
        "pre-tool hooks can rewrite this admitted ViewImage path"
    );
    assert!(!round_needs_image_references(&[], &callable, true));
    assert!(
        !round_needs_image_references(
            &[call(
                "execute_tool",
                json!({"tool_name": "ViewImage", "arguments": {
                    "path": "bamboo-image://current/message/0"
                }})
            )],
            &callable,
            false,
        ),
        "an unsupported literal wrapper is not new execution authority"
    );
}
