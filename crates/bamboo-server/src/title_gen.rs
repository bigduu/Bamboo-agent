//! Server-side re-export shim for the engine title-generation service.
//!
//! The title-generation service now lives in `bamboo_engine::title_gen`. These
//! re-exports preserve the historical `crate::title_gen::…` paths used in the
//! server crate.

pub use bamboo_engine::title_gen::{
    build_title_messages, heuristic_title, is_untitled, spawn_title_generation,
    spawn_title_generation_force,
};
