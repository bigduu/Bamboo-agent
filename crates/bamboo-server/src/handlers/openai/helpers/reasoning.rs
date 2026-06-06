//! Reasoning-effort parsing now lives in `handlers::llm_compat` (shared by all
//! three compat surfaces). Re-exported here so OpenAI's own helpers keep their
//! `reasoning::parse_reasoning_effort` call path.

pub(super) use crate::handlers::llm_compat::parse_reasoning_effort;
