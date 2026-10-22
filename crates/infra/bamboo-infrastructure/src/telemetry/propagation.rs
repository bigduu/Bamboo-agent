use std::collections::HashMap;
use std::future::Future;

use opentelemetry::context::FutureExt;
use opentelemetry::propagation::TextMapPropagator;
use opentelemetry::trace::TraceContextExt;
use opentelemetry::Context;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use serde::{Deserialize, Serialize};

/// W3C trace context only: no baggage, identity, credentials or business data.
///
/// Incoming carriers are untrusted observability hints, never actor/attempt
/// authority. Use only after authenticating a transport. SDK users can roundtrip
/// this type without configuring an exporter. Remote-worker wiring is separate.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "WireContext")]
pub struct TraceContext {
    traceparent: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    tracestate: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WireContext {
    traceparent: String,
    #[serde(default)]
    tracestate: Option<String>,
}

impl TryFrom<WireContext> for TraceContext {
    type Error = &'static str;
    fn try_from(wire: WireContext) -> Result<Self, Self::Error> {
        Self::new(wire.traceparent, wire.tracestate).ok_or("invalid W3C trace context")
    }
}

impl TraceContext {
    pub fn new(traceparent: String, tracestate: Option<String>) -> Option<Self> {
        // W3C traceparent future versions may carry additional fields. Bound
        // transport input before giving it to the SDK's standards-aware parser.
        if traceparent.len() > 512 || tracestate.as_ref().is_some_and(|v| v.len() > 512) {
            return None;
        }
        let carrier = Self {
            traceparent,
            tracestate,
        };
        let context = carrier.extract();
        if !context.span().span_context().is_valid() {
            return None;
        }
        // Normalize the parsed context. Invalid optional tracestate is discarded
        // per W3C behavior; no arbitrary carrier fields are retained.
        Self::from_context(&context)
    }

    pub fn capture() -> Option<Self> {
        Self::from_context(&Context::current())
    }

    fn from_context(context: &Context) -> Option<Self> {
        let mut headers = HashMap::new();
        TraceContextPropagator::new().inject_context(context, &mut headers);
        Some(Self {
            traceparent: headers.remove("traceparent")?,
            tracestate: headers
                .remove("tracestate")
                .filter(|value| !value.is_empty()),
        })
    }

    fn extract(&self) -> Context {
        let mut headers = HashMap::from([("traceparent".to_owned(), self.traceparent.clone())]);
        if let Some(state) = &self.tracestate {
            headers.insert("tracestate".to_owned(), state.clone());
        }
        TraceContextPropagator::new().extract_with_context(&Context::new(), &headers)
    }

    /// Poll a future under the validated remote parent, including inside spawn.
    /// Does not leave a thread-local guard installed across an await.
    pub fn scope<F: Future>(&self, future: F) -> opentelemetry::context::WithContext<F> {
        future.with_context(self.extract())
    }

    pub fn traceparent(&self) -> &str {
        &self.traceparent
    }
    pub fn tracestate(&self) -> Option<&str> {
        self.tracestate.as_deref()
    }
}
