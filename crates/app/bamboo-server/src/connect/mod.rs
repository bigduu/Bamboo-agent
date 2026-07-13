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

pub mod approvals;
pub mod bridge;
pub mod platform;
pub mod platforms;
pub mod render;

pub use bridge::{ConnectBridge, ConnectContext, SessionKey};
pub use platform::{
    Button, CallbackQuery, Capabilities, Inbound, InboundMessage, MessageRef, OutboundMessage,
    Platform, PlatformError, PlatformResult, ReplyCtx,
};

use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::mpsc;

use bamboo_config::ConnectPlatformConfig;
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
        let telegram_start_ok = telegram_multi_bot_guard(&config_snapshot.connect.platforms);
        for (index, platform_cfg) in config_snapshot.connect.platforms.iter().enumerate() {
            match platform_cfg.platform_type.as_str() {
                "telegram" => {
                    let token = platform_cfg.token.clone().unwrap_or_default();
                    if token.trim().is_empty() {
                        tracing::warn!(
                            "connect: telegram platform configured without a token; skipping"
                        );
                        continue;
                    }
                    // Issue #454 follow-up: `SessionKey`/`InboundMessage.platform`
                    // hardcode `"telegram"` for every adapter instance, so two
                    // live telegram bots on one server would collide on
                    // `telegram:<chat_id>:<user_id>` — a private chat's
                    // `chat_id` equals the Telegram user id, so a user who
                    // messages BOTH bots would silently share one bamboo
                    // session across them. Until per-bot session keys are
                    // supported, start at most the first validly-configured
                    // telegram entry and reject the rest with a clear warning
                    // rather than let them collide.
                    if !telegram_start_ok[index] {
                        tracing::warn!(
                            "connect: multiple telegram platform entries are configured; only \
                             the FIRST is started. A second telegram bot on this instance would \
                             collide with the first on the same session-routing key \
                             (`telegram:<chat_id>:<user_id>`), silently mixing sessions for any \
                             user who messages both bots. Remove the extra entry, or track \
                             issue #454 for per-bot session keys."
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

/// Issue #454 follow-up (multi-bot session-key collision): for each entry in
/// `platforms` (same order/length as the input), returns whether
/// [`ConnectManager::start`] is allowed to start it as far as the
/// "at most one live telegram bot" guard is concerned.
///
/// `SessionKey`/`InboundMessage.platform` hardcode `"telegram"` — there is no
/// per-bot/config-index component in the session-routing key — so two
/// telegram entries running at once would route messages from the SAME
/// Telegram user (private-chat `chat_id` == user id) to different bots into
/// the SAME bamboo session key, mixing their conversations. Rather than the
/// more invasive fix of threading a bot identity through `SessionKey`
/// (touching the routing key, the persisted session map's key format, and
/// every call site that builds one), this rejects the collision at the
/// source: only the FIRST validly-configured (`platform_type == "telegram"`
/// and a non-empty token) entry is ever started; every other telegram entry
/// is guarded off here regardless of how many are configured.
///
/// Entries with an empty/absent token are left `true` — they're handled by
/// [`ConnectManager::start`]'s pre-existing "no token configured" skip, which
/// doesn't count as a "started" telegram bot for this guard's purposes (so a
/// blank placeholder entry followed by one real entry still starts the real
/// one). Non-telegram entries are always `true` — this guard doesn't apply to
/// them.
fn telegram_multi_bot_guard(platforms: &[ConnectPlatformConfig]) -> Vec<bool> {
    let mut seen_valid_telegram = false;
    platforms
        .iter()
        .map(|platform_cfg| {
            if platform_cfg.platform_type != "telegram" {
                return true;
            }
            let has_token = platform_cfg
                .token
                .as_deref()
                .is_some_and(|token| !token.trim().is_empty());
            if !has_token {
                return true;
            }
            if seen_valid_telegram {
                false
            } else {
                seen_valid_telegram = true;
                true
            }
        })
        .collect()
}

/// Pulls inbound events (messages and button-press callbacks, issue #458)
/// off a single platform's channel and hands each to the bridge. Kept as its
/// OWN task per platform (not merged into the platform's `start()` loop) so
/// a slow/queued chat can never stall the next `getUpdates` poll —
/// `ConnectBridge::handle_inbound`/`handle_callback` themselves return
/// quickly (a message spawns the actual run; a callback only ever
/// acks+resolves), so this loop only ever blocks briefly.
async fn dispatch_loop(
    bridge: Arc<ConnectBridge>,
    platform: Arc<dyn Platform>,
    allow_from: Vec<String>,
    mut rx: mpsc::Receiver<Inbound>,
) {
    while let Some(event) = rx.recv().await {
        match event {
            Inbound::Message(msg) => {
                ConnectBridge::handle_inbound(
                    bridge.clone(),
                    platform.clone(),
                    allow_from.clone(),
                    msg,
                )
                .await;
            }
            Inbound::Callback(callback) => {
                ConnectBridge::handle_callback(
                    bridge.clone(),
                    platform.clone(),
                    allow_from.clone(),
                    callback,
                )
                .await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn platform(platform_type: &str, token: Option<&str>) -> ConnectPlatformConfig {
        ConnectPlatformConfig {
            platform_type: platform_type.to_string(),
            token: token.map(str::to_string),
            token_encrypted: None,
            allow_from: Vec::new(),
            admin_from: Vec::new(),
        }
    }

    #[test]
    fn telegram_multi_bot_guard_allows_a_single_telegram_entry() {
        let platforms = vec![platform("telegram", Some("tok-1"))];
        assert_eq!(telegram_multi_bot_guard(&platforms), vec![true]);
    }

    #[test]
    fn telegram_multi_bot_guard_rejects_every_telegram_entry_after_the_first() {
        let platforms = vec![
            platform("telegram", Some("tok-1")),
            platform("telegram", Some("tok-2")),
            platform("telegram", Some("tok-3")),
        ];
        assert_eq!(
            telegram_multi_bot_guard(&platforms),
            vec![true, false, false]
        );
    }

    #[test]
    fn telegram_multi_bot_guard_ignores_non_telegram_entries() {
        let platforms = vec![
            platform("telegram", Some("tok-1")),
            platform("feishu", Some("tok-x")),
            platform("telegram", Some("tok-2")),
        ];
        assert_eq!(
            telegram_multi_bot_guard(&platforms),
            vec![true, true, false]
        );
    }

    /// A blank/absent-token entry doesn't count as a "started" telegram bot
    /// for this guard — `ConnectManager::start`'s pre-existing empty-token
    /// check skips it separately, so the NEXT (real) telegram entry must
    /// still be allowed to start.
    #[test]
    fn telegram_multi_bot_guard_does_not_count_a_tokenless_entry_against_the_budget() {
        let platforms = vec![
            platform("telegram", None),
            platform("telegram", Some("")),
            platform("telegram", Some("tok-real")),
        ];
        assert_eq!(telegram_multi_bot_guard(&platforms), vec![true, true, true]);
    }

    #[test]
    fn telegram_multi_bot_guard_handles_an_empty_platform_list() {
        assert_eq!(telegram_multi_bot_guard(&[]), Vec::<bool>::new());
    }
}
