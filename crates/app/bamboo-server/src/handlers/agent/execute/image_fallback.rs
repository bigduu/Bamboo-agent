//! Server-side re-export of the engine image-fallback resolver.
//!
//! The resolver now lives in `bamboo_engine::model_config_helper`. This
//! re-export preserves the historical
//! `crate::handlers::agent::execute::image_fallback::resolve_image_fallback`
//! path used across the server crate.

pub(crate) use bamboo_engine::model_config_helper::resolve_image_fallback;
