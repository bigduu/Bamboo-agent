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
//!   platforms/feishu/     — Feishu/Lark WS long-connection adapter (phase 3)
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
        let start_ok = multi_bot_guard(&config_snapshot.connect.platforms);
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
                    if !start_ok[index] {
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
                    spawn_platform_tasks(
                        &mut tasks,
                        &bridge,
                        platform,
                        platform_cfg.allow_from.clone(),
                    );
                }
                "feishu" => {
                    let app_id = platform_cfg.app_id.clone().unwrap_or_default();
                    let app_secret = platform_cfg.app_secret.clone().unwrap_or_default();
                    if app_id.trim().is_empty() || app_secret.trim().is_empty() {
                        tracing::warn!(
                            "connect: feishu platform configured without app_id/app_secret; \
                             skipping"
                        );
                        continue;
                    }
                    // Same session-key collision as telegram's guard above:
                    // `SessionKey` hardcodes "feishu", and two apps in one
                    // group chat would also dedup-eat each other's copy of
                    // the same message_id. At most one live feishu entry.
                    if !start_ok[index] {
                        tracing::warn!(
                            "connect: multiple feishu platform entries are configured; only the \
                             FIRST is started. A second feishu app on this instance would \
                             collide with the first on the same session-routing key \
                             (`feishu:<chat_id>:<open_id>`) and on inbound message dedup. \
                             Remove the extra entry, or track issue #454 for per-bot session \
                             keys."
                        );
                        continue;
                    }
                    let Some(base_url) = resolve_feishu_base_url(platform_cfg.domain.as_deref())
                    else {
                        tracing::warn!(
                            domain = platform_cfg.domain.as_deref().unwrap_or_default(),
                            "connect: feishu platform has an invalid domain (expected \"feishu\", \
                             \"lark\", or an https:// base URL); skipping"
                        );
                        continue;
                    };
                    if platform_cfg.allow_from.is_empty() {
                        tracing::warn!(
                            "connect: feishu platform has an EMPTY allow_from list — every \
                             inbound message will be denied until you add allowed open_ids to \
                             connect.platforms[].allow_from"
                        );
                    }

                    let platform: Arc<dyn Platform> = Arc::new(
                        platforms::feishu::FeishuPlatform::new(app_id, app_secret, base_url),
                    );
                    spawn_platform_tasks(
                        &mut tasks,
                        &bridge,
                        platform,
                        platform_cfg.allow_from.clone(),
                    );
                }
                other => {
                    tracing::warn!("connect: unknown platform type '{other}'; skipping");
                }
            }
        }

        Self { tasks }
    }
}

/// Spawns the pair of background tasks every platform entry needs — the
/// adapter's own `start()` loop and its [`dispatch_loop`] — and logs the
/// startup. Factored out of the per-platform match arms, which only differ in
/// how they validate config and construct the adapter.
fn spawn_platform_tasks(
    tasks: &mut Vec<tokio::task::JoinHandle<()>>,
    bridge: &Arc<ConnectBridge>,
    platform: Arc<dyn Platform>,
    allow_from: Vec<String>,
) {
    let name = platform.name().to_string();
    let (tx, rx) = mpsc::channel(64);

    let platform_for_start = platform.clone();
    let name_for_start = name.clone();
    tasks.push(tokio::spawn(async move {
        if let Err(error) = platform_for_start.start(tx).await {
            tracing::warn!("connect: {name_for_start} platform loop exited: {error}");
        }
    }));

    tasks.push(tokio::spawn(dispatch_loop(
        bridge.clone(),
        platform,
        allow_from,
        rx,
    )));

    tracing::info!("connect: started {name} platform");
}

/// Resolves the `domain` config field of a feishu platform entry to an API
/// base URL: absent/`"feishu"` → open.feishu.cn, `"lark"` → open.larksuite.com
/// (Lark international), any `https://` value → private-deployment base used
/// as-is (trailing slash trimmed). Anything else is invalid — the caller
/// warns and skips the entry.
fn resolve_feishu_base_url(domain: Option<&str>) -> Option<String> {
    match domain.map(str::trim).filter(|d| !d.is_empty()) {
        None | Some("feishu") => Some("https://open.feishu.cn".to_string()),
        Some("lark") => Some("https://open.larksuite.com".to_string()),
        Some(custom) if custom.starts_with("https://") => {
            Some(custom.trim_end_matches('/').to_string())
        }
        Some(_) => None,
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
/// [`ConnectManager::start`] is allowed to start it as far as the "at most
/// one live bot PER PLATFORM TYPE" guard is concerned.
///
/// `SessionKey`/`InboundMessage.platform` hardcode the platform name — there
/// is no per-bot/config-index component in the session-routing key — so two
/// entries of the same type running at once would route messages from the
/// SAME user to different bots into the SAME bamboo session key, mixing
/// their conversations (and, since the inbound dedup key is
/// `platform:message_id`, two feishu apps in one group chat would even eat
/// each other's copy of the same message). Rather than the more invasive fix
/// of threading a bot identity through `SessionKey` (touching the routing
/// key, the persisted session map's key format, and every call site that
/// builds one), this rejects the collision at the source: only the FIRST
/// validly-configured entry of each platform type is ever started; every
/// later same-type entry is guarded off regardless of how many are
/// configured.
///
/// Entries with empty/absent credentials are left `true` — they're handled
/// by [`ConnectManager::start`]'s pre-existing "not configured" skip, which
/// doesn't count as a "started" bot for this guard's purposes (so a blank
/// placeholder entry followed by one real entry still starts the real one).
/// Unknown platform types are always `true` — `start` skips them anyway.
fn multi_bot_guard(platforms: &[ConnectPlatformConfig]) -> Vec<bool> {
    let mut seen_valid: std::collections::HashSet<&str> = std::collections::HashSet::new();
    platforms
        .iter()
        .map(|platform_cfg| {
            let non_empty =
                |field: &Option<String>| field.as_deref().is_some_and(|v| !v.trim().is_empty());
            let valid = match platform_cfg.platform_type.as_str() {
                "telegram" => non_empty(&platform_cfg.token),
                "feishu" => non_empty(&platform_cfg.app_id) && non_empty(&platform_cfg.app_secret),
                _ => return true,
            };
            if !valid {
                return true;
            }
            seen_valid.insert(platform_cfg.platform_type.as_str())
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
            id: None,
            platform_type: platform_type.to_string(),
            token: token.map(str::to_string),
            token_encrypted: None,
            app_id: None,
            app_secret: None,
            app_secret_encrypted: None,
            domain: None,
            allow_from: Vec::new(),
            admin_from: Vec::new(),
        }
    }

    fn feishu_platform(app_id: Option<&str>, app_secret: Option<&str>) -> ConnectPlatformConfig {
        ConnectPlatformConfig {
            app_id: app_id.map(str::to_string),
            app_secret: app_secret.map(str::to_string),
            ..platform("feishu", None)
        }
    }

    #[test]
    fn multi_bot_guard_allows_a_single_telegram_entry() {
        let platforms = vec![platform("telegram", Some("tok-1"))];
        assert_eq!(multi_bot_guard(&platforms), vec![true]);
    }

    #[test]
    fn multi_bot_guard_rejects_every_telegram_entry_after_the_first() {
        let platforms = vec![
            platform("telegram", Some("tok-1")),
            platform("telegram", Some("tok-2")),
            platform("telegram", Some("tok-3")),
        ];
        assert_eq!(multi_bot_guard(&platforms), vec![true, false, false]);
    }

    /// The guard is PER platform type: one telegram and one feishu entry can
    /// coexist, but a second live entry of the SAME type is rejected.
    #[test]
    fn multi_bot_guard_is_scoped_per_platform_type() {
        let platforms = vec![
            platform("telegram", Some("tok-1")),
            feishu_platform(Some("cli_a"), Some("secret-a")),
            platform("telegram", Some("tok-2")),
            feishu_platform(Some("cli_b"), Some("secret-b")),
        ];
        assert_eq!(multi_bot_guard(&platforms), vec![true, true, false, false]);
    }

    /// A blank/absent-credential entry doesn't count as a "started" bot for
    /// this guard — `ConnectManager::start`'s pre-existing empty-credential
    /// check skips it separately, so the NEXT (real) entry of the same type
    /// must still be allowed to start.
    #[test]
    fn multi_bot_guard_does_not_count_a_credentialless_entry_against_the_budget() {
        let platforms = vec![
            platform("telegram", None),
            platform("telegram", Some("")),
            platform("telegram", Some("tok-real")),
            feishu_platform(Some("cli_a"), None),
            feishu_platform(Some("cli_b"), Some("secret-real")),
        ];
        assert_eq!(
            multi_bot_guard(&platforms),
            vec![true, true, true, true, true]
        );
    }

    #[test]
    fn multi_bot_guard_leaves_unknown_platform_types_alone() {
        let platforms = vec![
            platform("dingtalk", Some("x")),
            platform("dingtalk", Some("y")),
        ];
        assert_eq!(multi_bot_guard(&platforms), vec![true, true]);
    }

    #[test]
    fn multi_bot_guard_handles_an_empty_platform_list() {
        assert_eq!(multi_bot_guard(&[]), Vec::<bool>::new());
    }

    #[test]
    fn resolve_feishu_base_url_covers_the_three_domain_forms() {
        assert_eq!(
            resolve_feishu_base_url(None).as_deref(),
            Some("https://open.feishu.cn")
        );
        assert_eq!(
            resolve_feishu_base_url(Some("feishu")).as_deref(),
            Some("https://open.feishu.cn")
        );
        assert_eq!(
            resolve_feishu_base_url(Some("")).as_deref(),
            Some("https://open.feishu.cn")
        );
        assert_eq!(
            resolve_feishu_base_url(Some("lark")).as_deref(),
            Some("https://open.larksuite.com")
        );
        assert_eq!(
            resolve_feishu_base_url(Some("https://feishu.example.corp/")).as_deref(),
            Some("https://feishu.example.corp")
        );
        assert_eq!(
            resolve_feishu_base_url(Some("http://insecure.example")),
            None
        );
        assert_eq!(resolve_feishu_base_url(Some("dingtalk")), None);
    }
}
