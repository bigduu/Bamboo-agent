//! Regression tests for Actix-web `Data<T>` wiring.
//!
//! These tests ensure handlers only extract the unified `AppState` registered by the server.
//! If a handler accidentally adds an extra `web::Data<...>` (e.g. a legacy AppState),
//! Actix will return 500 before reaching JSON extraction/validation.

use actix_web::{test, App};
use bamboo_agent::server::configure_routes;

mod app_state;
