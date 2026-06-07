//! Shared helpers for the LLM-compatibility surfaces (anthropic / openai /
//! gemini). These three HTTP front-ends all translate through the OpenAI
//! `ChatCompletionRequest` canonical form, so the token-usage estimators and
//! reasoning-effort parser are factored here instead of being copied per
//! surface. The heavy schema translation itself lives one crate down in
//! `bamboo-llm`; this module is only the thin per-request glue.

pub(crate) mod reasoning;
pub(crate) mod usage;

pub(crate) use reasoning::parse_reasoning_effort;
