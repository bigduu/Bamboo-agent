//! Ergonomic top-level Agent SDK facade for the Bamboo agent framework.
//!
//! This crate is the thin, ergonomic entry point over the engine runtime. It
//! depends only on the lower layers (`bamboo-engine`, `bamboo-infrastructure`,
//! `bamboo-tools`, `bamboo-agent-core`, `bamboo-domain`) and **never** on
//! `bamboo-server`, so it can be reused freely by server/engine code without a
//! circular dependency.
//!
//! See [`agent`] for the full surface; the most common entry points are
//! [`Agent`] and [`AgentBuilder`].

#![allow(clippy::module_inception)]

pub mod agent;

// Mirror the public facade re-exports the root crate previously exposed.
pub use agent::ExecuteRequestBuilder;
pub use agent::{
    Agent, AgentBuilder, AgentHook, AgentHookPoint, HookPayload, HookResult, HookRunner,
    HookToolOutcome, ShellCommandHook, ShellHookEvent,
};

// Tool catalog surfaced by `agent::mod`.
pub use agent::{
    builtin_tool_names, builtin_tool_specs, BuiltinTool, ToolSpec, CANONICAL_TOOL_NAMES,
};
