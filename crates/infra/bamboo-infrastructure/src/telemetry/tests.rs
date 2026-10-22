use super::*;
use opentelemetry::trace::SpanId;
use opentelemetry::Value;
use opentelemetry_sdk::trace::SpanData;

fn attr<'a>(span: &'a SpanData, key: &str) -> Option<&'a Value> {
    span.attributes
        .iter()
        .find(|kv| kv.key.as_str() == key)
        .map(|kv| &kv.value)
}

#[tokio::test]
async fn finite_invocations_parent_real_operations_and_preserve_usage_presence() {
    let test = test_support::TraceTest::default();
    test.scope(async {
        let agent = Observation::agent("session-1", "root-1");
        agent
            .scope(async {
                record_execution_identity("execution-1", "run-1");
                let llm = Observation::llm(Some("openai"), "gpt-test", "session-1");
                llm.usage(Some(100), Some(0));
                llm.finish(Outcome::Success);
                Observation::tool("Read", "call-1", "session-1", "round-1")
                    .finish(Outcome::Success);
            })
            .await;
        agent.finish(Outcome::Success);
        // Continuing a persisted conversation is a new finite invocation.
        Observation::agent("session-1", "root-1").finish(Outcome::Success);
        Observation::llm(None, "test-model", "session-1").finish(Outcome::Success);
    })
    .await;
    let spans = test.spans();
    assert_eq!(spans.len(), 5);
    let agent = &spans[2];
    assert_eq!(agent.name, "invoke_agent");
    assert_eq!(agent.span_kind, SpanKind::Internal);
    assert_eq!(
        attr(agent, "bamboo.execution.id"),
        Some(&Value::from("execution-1"))
    );
    assert_eq!(attr(agent, "bamboo.run.id"), Some(&Value::from("run-1")));
    for span in &spans[..2] {
        assert_eq!(span.parent_span_id, agent.span_context.span_id());
        assert_eq!(span.span_context.trace_id(), agent.span_context.trace_id());
        assert!(span.events.is_empty());
        assert!(span.links.is_empty());
        assert!(span.end_time >= span.start_time);
    }
    assert_eq!(
        attr(&spans[0], "gen_ai.usage.input_tokens"),
        Some(&Value::I64(100))
    );
    assert_eq!(
        attr(&spans[0], "gen_ai.usage.output_tokens"),
        Some(&Value::I64(0))
    );
    assert!(attr(&spans[4], "gen_ai.usage.input_tokens").is_none());
    assert!(attr(&spans[4], "gen_ai.usage.output_tokens").is_none());
    assert_eq!(spans[3].parent_span_id, SpanId::INVALID);
    assert_ne!(
        spans[3].span_context.trace_id(),
        agent.span_context.trace_id()
    );
    assert_eq!(agent.instrumentation_scope.name(), INSTRUMENTATION_NAME);
    assert_eq!(agent.instrumentation_scope.schema_url(), None);
    assert!(agent
        .instrumentation_scope
        .attributes()
        .any(|kv| kv.key.as_str() == "bamboo.gen_ai.conventions_revision"
            && kv.value == Value::from(GEN_AI_CONVENTIONS_REVISION)));
}

#[tokio::test]
async fn context_isolated_between_parallel_and_explicitly_spawned_futures() {
    let test = test_support::TraceTest::default();
    test.scope(async {
        let first = Observation::agent("first", "first");
        let second = Observation::agent("second", "second");
        tokio::join!(
            first.scope(async {
                tokio::task::yield_now().await;
                tokio::spawn(with_current_context(async {
                    tokio::task::yield_now().await;
                    Observation::tool("Read", "first-call", "first", "r1").finish(Outcome::Success);
                }))
                .await
                .unwrap();
            }),
            second.scope(async {
                tokio::task::yield_now().await;
                Observation::tool("Read", "second-call", "second", "r2").finish(Outcome::Success);
            }),
        );
        first.finish(Outcome::Success);
        second.finish(Outcome::Success);
    })
    .await;
    let spans = test.spans();
    for session in ["first", "second"] {
        let matching: Vec<_> = spans
            .iter()
            .filter(|span| attr(span, "gen_ai.conversation.id") == Some(&Value::from(session)))
            .collect();
        assert_eq!(matching.len(), 2);
        let root = matching.iter().find(|s| s.name == "invoke_agent").unwrap();
        let tool = matching
            .iter()
            .find(|s| s.name.starts_with("execute_tool"))
            .unwrap();
        assert_eq!(tool.parent_span_id, root.span_context.span_id());
        assert_eq!(tool.span_context.trace_id(), root.span_context.trace_id());
    }
    assert!(!Context::current().span().span_context().is_valid());
}

#[tokio::test]
async fn fixed_error_categories_and_drop_end_spans_without_error_payloads() {
    let test = test_support::TraceTest::default();
    test.scope(async {
        Observation::llm(Some("anthropic"), "model", "s1").finish(Outcome::ProviderError);
        Observation::tool("Read", "c1", "s1", "r1").finish(Outcome::ToolError);
        let dropped = Observation::agent("cancelled", "cancelled");
        drop(dropped);
        assert!(std::panic::catch_unwind(|| {
            let _observation = Observation::agent("panicked", "panicked");
            panic!("PRIVATE_PANIC_SENTINEL");
        })
        .is_err());
    })
    .await;
    let spans = test.spans();
    assert_eq!(spans.len(), 4);
    for (span, expected) in spans
        .iter()
        .zip(["provider_error", "tool_error", "cancelled", "panic"])
    {
        assert_eq!(attr(span, "error.type"), Some(&Value::from(expected)));
        assert_eq!(span.status, Status::error(expected));
    }
    assert!(!format!("{spans:?}").contains("PRIVATE_PANIC_SENTINEL"));
}

#[tokio::test]
async fn payload_shaped_identifiers_and_ambient_logs_are_never_exported() {
    let test = test_support::TraceTest::default();
    test.scope(async {
        tracing::warn!(
            prompt = "PRIVATE_PROMPT_SENTINEL",
            path = "/Users/private/secret",
            "private ordinary log"
        );
        let agent = Observation::agent(
            "/Users/private/secret",
            "https://secret.example/path?token=secret",
        );
        agent
            .scope(async {
                record_execution_identity(
                    "C:\\private\\secret",
                    "credential=PRIVATE_CREDENTIAL_SENTINEL",
                );
                Observation::llm(
                    Some("PRIVATE_PROVIDER_SENTINEL"),
                    "prompt with PRIVATE_MODEL_SENTINEL",
                    "\nPRIVATE_SESSION_SENTINEL",
                )
                .finish(Outcome::ProviderError);
                Observation::tool(
                    "../PRIVATE_TOOL_SENTINEL",
                    "{\"arguments\":\"PRIVATE_ARG_SENTINEL\"}",
                    " ",
                    &"x".repeat(129),
                )
                .finish(Outcome::ToolError);
            })
            .await;
        agent.finish(Outcome::Success);
    })
    .await;
    let spans = test.spans();
    let rendered = format!("{spans:?}");
    for secret in ["PRIVATE_", "/Users/", "C:\\private", "https://secret"] {
        assert!(!rendered.contains(secret), "leaked {secret}");
    }
    for span in spans {
        assert!(span.events.is_empty());
        assert!(span.links.is_empty());
        assert!(span.attributes.iter().all(|kv| matches!(
            kv.key.as_str(),
            "gen_ai.operation.name" | "bamboo.tool.observation" | "error.type" | "bamboo.outcome"
        )));
    }
}

#[tokio::test]
async fn w3c_carrier_roundtrips_remote_parent_and_rejects_non_context_data() {
    let parent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    let carrier = TraceContext::new(parent.into(), Some("vendor=opaque".into())).unwrap();
    let encoded = serde_json::to_string(&carrier).unwrap();
    let decoded: TraceContext = serde_json::from_str(&encoded).unwrap();
    assert_eq!(carrier, decoded);
    assert_eq!(decoded.traceparent(), parent);
    assert_eq!(decoded.tracestate(), Some("vendor=opaque"));
    let test = test_support::TraceTest::default();
    decoded
        .scope(async {
            test.scope(async {
                let agent = Observation::agent("remote-child", "root");
                agent
                    .scope(async {
                        let captured = TraceContext::capture().unwrap();
                        assert!(captured
                            .traceparent()
                            .starts_with("00-4bf92f3577b34da6a3ce929d0e0e4736-"));
                    })
                    .await;
                agent.finish(Outcome::Success);
            })
            .await;
        })
        .await;
    let spans = test.spans();
    assert_eq!(
        spans.last().unwrap().parent_span_id.to_string(),
        "00f067aa0ba902b7"
    );
    assert_eq!(
        spans.last().unwrap().span_context.trace_id().to_string(),
        "4bf92f3577b34da6a3ce929d0e0e4736"
    );
    for invalid in [
        "",
        "secret",
        "00-00000000000000000000000000000000-00f067aa0ba902b7-01",
        "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",
    ] {
        assert!(TraceContext::new(invalid.into(), None).is_none());
    }
    assert!(TraceContext::new("x".repeat(513), None).is_none());
    assert!(TraceContext::new(parent.into(), Some("x".repeat(513))).is_none());
    assert!(serde_json::from_value::<TraceContext>(serde_json::json!({
        "traceparent": parent, "baggage": "PRIVATE_SECRET"
    }))
    .is_err());
    assert!(TraceContext::capture().is_none());
}

#[cfg(feature = "otel-otlp")]
mod otlp {
    use super::*;
    use opentelemetry::trace::TracerProvider;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::time::{Duration, Instant};

    fn config(endpoint: String) -> TelemetryConfig {
        TelemetryConfig {
            enabled: true,
            endpoint: Some(endpoint),
            sample_ratio: 1.0,
        }
    }

    #[test]
    fn local_otlp_collector_receives_allowlisted_trace_bytes() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}/v1/traces", listener.local_addr().unwrap());
        listener.set_nonblocking(true).unwrap();
        let collector = std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(5);
            let mut socket = loop {
                match listener.accept() {
                    Ok((socket, _)) => break socket,
                    Err(error)
                        if error.kind() == std::io::ErrorKind::WouldBlock
                            && Instant::now() < deadline =>
                    {
                        std::thread::sleep(Duration::from_millis(10))
                    }
                    Err(error) => panic!("collector accept: {error}"),
                }
            };
            socket
                .set_read_timeout(Some(Duration::from_secs(3)))
                .unwrap();
            let mut request = Vec::new();
            let header_end = loop {
                let mut byte = [0];
                socket.read_exact(&mut byte).unwrap();
                request.push(byte[0]);
                assert!(request.len() < 8192);
                if request.ends_with(b"\r\n\r\n") {
                    break request.len();
                }
            };
            let headers = String::from_utf8(request.clone()).unwrap();
            let content_length: usize = headers
                .lines()
                .find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("content-length: ")
                        .map(|value| value.parse().unwrap())
                })
                .unwrap();
            assert!(content_length < 64 * 1024);
            request.resize(header_end + content_length, 0);
            socket.read_exact(&mut request[header_end..]).unwrap();
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .unwrap();
            request
        });
        let provider = config::build_provider(&config(endpoint)).unwrap();
        let tracer = provider.tracer_with_scope(scope());
        let agent = Observation::start_with(
            &tracer,
            "invoke_agent",
            SpanKind::Internal,
            vec![KeyValue::new("gen_ai.operation.name", "invoke_agent")],
        );
        agent.finish(Outcome::Success);
        provider.force_flush().unwrap();
        provider
            .shutdown_with_timeout(Duration::from_secs(2))
            .unwrap();
        let request = collector.join().unwrap();
        let request = String::from_utf8_lossy(&request);
        for required in [
            "POST /v1/traces",
            "application/x-protobuf",
            "invoke_agent",
            "gen_ai.operation.name",
            "service.name",
            GEN_AI_CONVENTIONS_REVISION,
        ] {
            assert!(request.contains(required), "missing {required}");
        }
        for denied in [
            "authorization:",
            "baggage:",
            "process.command",
            "host.name",
            "telemetry.sdk",
            "PRIVATE_",
        ] {
            assert!(!request.contains(denied), "leaked {denied}");
        }
    }

    #[test]
    fn ambient_otel_headers_and_resources_cannot_leak_credentials() {
        // A subprocess gives this test private environment variables without
        // racing unrelated tests or mutating the embedding host's environment.
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "telemetry::tests::otlp::local_otlp_collector_receives_allowlisted_trace_bytes",
                "--nocapture",
            ])
            .env(
                "OTEL_EXPORTER_OTLP_HEADERS",
                "authorization=Bearer%20PRIVATE_GENERAL_SECRET",
            )
            .env(
                "OTEL_EXPORTER_OTLP_TRACES_HEADERS",
                "authorization=Bearer%20PRIVATE_TRACE_SECRET,baggage=PRIVATE_BAGGAGE",
            )
            .env(
                "OTEL_RESOURCE_ATTRIBUTES",
                "host.name=PRIVATE_HOST,process.command_line=PRIVATE_COMMAND",
            )
            .env("OTEL_SERVICE_NAME", "PRIVATE_SERVICE")
            .env(
                "OTEL_EXPORTER_OTLP_ENDPOINT",
                "http://127.0.0.1:1/WRONG_ENDPOINT",
            )
            .env(
                "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
                "http://127.0.0.1:1/WRONG_TRACE_ENDPOINT",
            )
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "collector subprocess failed: {} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"));
    }

    #[test]
    fn unavailable_collector_never_backpressures_spans_and_shutdown_is_bounded() {
        // A listener that accepts a connection but never sends response headers.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let provider = config::build_provider(&config(format!(
            "http://{}/v1/traces",
            listener.local_addr().unwrap()
        )))
        .unwrap();
        let tracer = provider.tracer_with_scope(scope());
        let started = Instant::now();
        for _ in 0..10_000 {
            Observation::start_with(&tracer, "invoke_agent", SpanKind::Internal, vec![])
                .finish(Outcome::Success);
        }
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "span production was backpressured"
        );
        let started = Instant::now();
        let _ = provider.shutdown_with_timeout(Duration::from_secs(2));
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "shutdown exceeded timeout tolerance"
        );
    }

    #[test]
    fn zero_sample_ratio_exports_no_roots() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let mut config = config(format!(
            "http://{}/v1/traces",
            listener.local_addr().unwrap()
        ));
        config.sample_ratio = 0.0;
        let provider = config::build_provider(&config).unwrap();
        let tracer = provider.tracer_with_scope(scope());
        Observation::start_with(&tracer, "invoke_agent", SpanKind::Internal, vec![])
            .finish(Outcome::Success);
        provider.force_flush().unwrap();
        provider
            .shutdown_with_timeout(Duration::from_secs(2))
            .unwrap();
        assert_eq!(
            listener.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
    }
}
