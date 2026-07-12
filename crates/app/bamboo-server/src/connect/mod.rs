//! bamboo-connect: drive bamboo sessions from IM platforms (Telegram first).
//!
//! Issue #452 (MVP, phase 1 of epic #447). Sibling module of
//! `schedule_app`/`notify_sinks` — the closest precedents: `notify_sinks` is
//! one-way outbound, `schedule_app::manager` is the canonical
//! background-execution pattern this module's [`bridge::ConnectBridge`]
//! reuses verbatim (`spawn_session_execution`, event-forwarder, runner
//! reservation).
//!
//! ```text
//! connect/
//!   platform.rs      — the Platform trait + Capabilities + message types
//!   bridge.rs         — chat ⇄ bamboo-session routing, busy lock, queueing
//!   render.rs         — AgentEvent stream → platform messages
//!   platforms/telegram.rs — long-poll adapter
//! ```
//!
//! [`ConnectManager`] is constructed once at server startup (mirrors
//! `ScheduleManager` / the notification relay — see
//! `app_state::init::build_connect_manager`) and is FULLY INERT when
//! `config.connect.platforms` is empty: no `[connect]` section in
//! `config.json` means zero background tasks are spawned.

pub mod bridge;
pub mod platform;
pub mod platforms;
pub mod render;

pub use bridge::{ConnectBridge, ConnectContext, SessionKey};
pub use platform::{
    Capabilities, InboundMessage, MessageRef, OutboundMessage, Platform, PlatformError,
    PlatformResult, ReplyCtx,
};

use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::mpsc;

use bamboo_llm::Config;

/// Owns every configured platform's long-poll/dispatch background task plus
/// the shared [`ConnectBridge`]. All tasks are aborted when the manager
/// drops (mirrors `app_state::builder::EmbeddedBroker`/`HealthMonitor`'s
/// Drop-based stop — the closest server-lifecycle precedent for a
/// fire-and-forget background subsystem).
pub struct ConnectManager {
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl ConnectManager {
    /// Builds the bridge, loads its persisted session map, and starts one
    /// long-poll + one dispatch task per recognized platform entry in
    /// `config_snapshot.connect.platforms`. An empty (or absent) `[connect]`
    /// config starts zero tasks — fully inert by default, per #452.
    pub async fn start(
        ctx: ConnectContext,
        config_snapshot: &Config,
        data_dir: Option<PathBuf>,
    ) -> Self {
        let map_path = data_dir.map(|dir| dir.join("connect_sessions.json"));
        let bridge = Arc::new(ConnectBridge::new(ctx, map_path));
        bridge.load_session_map().await;

        let mut tasks = Vec::new();
        for platform_cfg in &config_snapshot.connect.platforms {
            match platform_cfg.platform_type.as_str() {
                "telegram" => {
                    let token = platform_cfg.token.clone().unwrap_or_default();
                    if token.trim().is_empty() {
                        tracing::warn!(
                            "connect: telegram platform configured without a token; skipping"
                        );
                        continue;
                    }
                    if platform_cfg.allow_from.is_empty() {
                        tracing::warn!(
                            "connect: telegram platform has an EMPTY allow_from list — every \
                             inbound message will be denied until you add allowed user ids to \
                             connect.platforms[].allow_from"
                        );
                    }

                    let platform: Arc<dyn Platform> =
                        Arc::new(platforms::telegram::TelegramPlatform::new(token));
                    let allow_from = platform_cfg.allow_from.clone();
                    let (tx, rx) = mpsc::channel(64);

                    let platform_for_start = platform.clone();
                    tasks.push(tokio::spawn(async move {
                        if let Err(error) = platform_for_start.start(tx).await {
                            tracing::warn!("connect: telegram platform loop exited: {error}");
                        }
                    }));

                    let bridge_for_dispatch = bridge.clone();
                    let platform_for_dispatch = platform.clone();
                    tasks.push(tokio::spawn(dispatch_loop(
                        bridge_for_dispatch,
                        platform_for_dispatch,
                        allow_from,
                        rx,
                    )));

                    tracing::info!("connect: started telegram platform");
                }
                other => {
                    tracing::warn!("connect: unknown platform type '{other}'; skipping");
                }
            }
        }

        Self { tasks }
    }
}

impl Drop for ConnectManager {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

/// Pulls inbound messages off a single platform's channel and hands each to
/// the bridge. Kept as its OWN task per platform (not merged into the
/// platform's `start()` loop) so a slow/queued chat can never stall the next
/// `getUpdates` poll — `ConnectBridge::handle_inbound` itself returns quickly
/// (it spawns the actual run), so this loop only ever blocks briefly.
async fn dispatch_loop(
    bridge: Arc<ConnectBridge>,
    platform: Arc<dyn Platform>,
    allow_from: Vec<String>,
    mut rx: mpsc::Receiver<InboundMessage>,
) {
    while let Some(msg) = rx.recv().await {
        ConnectBridge::handle_inbound(bridge.clone(), platform.clone(), allow_from.clone(), msg)
            .await;
    }
}
