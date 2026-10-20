//! OS-native desktop notification sink.
//!
//! Delivery is best-effort: the synchronous notification call drives D-Bus
//! or native macOS/Windows APIs, so it always runs on `spawn_blocking`, and every failure is
//! logged and swallowed — a missing D-Bus session (headless Linux) or a
//! transient mac API error must never break a run.

use std::sync::atomic::{AtomicBool, Ordering};
// `Once` is only referenced inside the macOS-gated `warm_notification_center`
// below; an unconditional import is an unused-import ERROR (`-D warnings`) on
// the Linux CI lint runner.
#[cfg(target_os = "macos")]
use std::sync::Once;

use super::{NotificationSink, SinkNotification};

/// Process-wide: true when this server was started as a sidecar
/// (`bamboo serve --parent-pid <pid>`) — a native shell (e.g. Bodhi) owns
/// notification UX in that mode, so the desktop sink's default posture flips
/// to off (see [`desktop_enabled`]). Set once at startup from
/// `src/bin/bamboo.rs`'s `Serve` arm, before the Actix runtime starts (main
/// thread, no concurrent readers yet), so a `Relaxed` load/store is enough.
static IS_SIDECAR: AtomicBool = AtomicBool::new(false);

/// Records whether this process was launched as a sidecar. Call once, before
/// serving, from the CLI entry point.
pub fn set_sidecar_mode(is_sidecar: bool) {
    IS_SIDECAR.store(is_sidecar, Ordering::Relaxed);
}

/// Whether this process is running as a sidecar (see [`set_sidecar_mode`]).
pub fn is_sidecar_mode() -> bool {
    IS_SIDECAR.load(Ordering::Relaxed)
}

/// Pure desktop-enable rule, factored out for unit testing.
///
/// `configured` mirrors `DesktopChannelConfig::enabled`: `None` (auto) is on
/// for a standalone `bamboo serve` and off for a sidecar; `Some(_)` is an
/// explicit user override in either direction.
pub(crate) fn desktop_enabled(configured: Option<bool>, is_sidecar: bool) -> bool {
    configured.unwrap_or(!is_sidecar)
}

/// Whether the desktop sink should stay silent for `category` because the
/// session already has a live client watching it — the UI already surfaces
/// this, so an OS popup would be redundant noise. Only the "the app already
/// shows this" categories are suppressed; failures / background completions /
/// context-pressure / subagent-completed still fire regardless of a watcher.
pub(crate) fn suppressed_by_watcher(category: &str, has_watcher: bool) -> bool {
    has_watcher && (category == "run_completed" || category.starts_with("needs_"))
}

/// Sets the macOS application identity notifications are attributed to.
///
/// `notify_rust::set_application` errors if called twice (`AlreadySet`), so
/// this is guarded by a process-wide [`Once`] — later calls are no-ops.
/// Compiled only for macOS: the underlying `mac-notification-sys` call
/// doesn't exist on other platforms.
#[cfg(target_os = "macos")]
fn ensure_mac_application_set() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        if let Err(error) = notify_rust::set_application("com.apple.Terminal") {
            tracing::warn!(
                %error,
                "desktop notification sink: failed to set macOS application identity"
            );
        }
    });
}

#[cfg(target_os = "windows")]
fn ensure_mac_application_set() {}

/// Maps [`SinkNotification::priority`] to Windows' native urgency. `notify-rust`
/// 4.18 has no urgency mapping on macOS (that needs
/// the `preview-macos-un` feature, which this crate doesn't enable) — a
/// no-op there.
#[cfg(target_os = "windows")]
fn apply_urgency(notification: &mut notify_rust::Notification, priority: &str) {
    let urgency = if priority == "high" {
        notify_rust::Urgency::Critical
    } else {
        notify_rust::Urgency::Normal
    };
    notification.urgency(urgency);
}

#[cfg(target_os = "macos")]
fn apply_urgency(_notification: &mut notify_rust::Notification, _priority: &str) {}

/// Rate-limits the failure log: the first failure in this process's lifetime
/// warns (surfaces a genuinely broken notification backend), every
/// subsequent failure only debugs (a headless Linux box with no D-Bus session
/// would otherwise warn on every single notification, forever).
static WARNED_ONCE: AtomicBool = AtomicBool::new(false);

fn log_failure(error: &impl std::fmt::Display) {
    if !WARNED_ONCE.swap(true, Ordering::Relaxed) {
        tracing::warn!(
            %error,
            "desktop notification sink: failed to show notification (further failures logged at debug)"
        );
    } else {
        tracing::debug!(%error, "desktop notification sink: failed to show notification");
    }
}

/// The synchronous `notify-rust` call, factored out so the gating/mapping
/// logic above can be unit-tested without ever firing a real OS notification.
#[cfg(any(target_os = "macos", target_os = "windows"))]
fn fire_desktop_notification(title: &str, body: &str, priority: &str) {
    ensure_mac_application_set();
    let mut notification = notify_rust::Notification::new();
    notification.summary(title).body(body);
    apply_urgency(&mut notification, priority);
    if let Err(error) = notification.show() {
        log_failure(&error);
    }
}

#[cfg(all(unix, any(not(target_os = "macos"), test)))]
type XdgNotifyPayload<'a> = (
    String,
    u32,
    &'static str,
    &'a str,
    &'a str,
    Vec<String>,
    std::collections::HashMap<&'static str, zbus::Variant<'static>>,
    i32,
);

/// Preserve notify-rust's existing XDG Notify fields while using maintained
/// zbus 6 wire types. No actions, icon, replacement ID or custom timeout were
/// configured by this sink; only high priority maps to critical urgency.
#[cfg(all(unix, any(not(target_os = "macos"), test)))]
fn xdg_notify_payload<'a>(
    app_name: String,
    title: &'a str,
    body: &'a str,
    priority: &str,
) -> XdgNotifyPayload<'a> {
    (
        app_name,
        0,
        "",
        title,
        body,
        Vec::new(),
        std::collections::HashMap::from([(
            "urgency",
            zbus::Variant::U8(if priority == "high" { 2 } else { 1 }),
        )]),
        -1,
    )
}

#[cfg(all(unix, not(target_os = "macos")))]
fn show_xdg_notification(title: &str, body: &str, priority: &str) -> zbus::Result<()> {
    // Match notify-rust's executable basename, independently of argv[0].
    let executable = std::env::current_exe()?;
    let app_name = executable
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "desktop notification executable has no UTF-8 basename",
            )
        })?
        .to_owned();
    let payload = xdg_notify_payload(app_name, title, body, priority);
    zbus::block_on(async {
        let connection = zbus::Connection::session().await?;
        let reply = connection
            .call_method(
                Some("org.freedesktop.Notifications"),
                "/org/freedesktop/Notifications",
                Some("org.freedesktop.Notifications"),
                "Notify",
                &payload,
            )
            .await?;
        // The previous sink also required an unsigned notification ID and
        // immediately dropped its handle, leaving the desktop's timeout intact.
        let _notification_id: u32 = reply.body().deserialize()?;
        Ok(())
    })
}

#[cfg(all(unix, not(target_os = "macos")))]
fn fire_desktop_notification(title: &str, body: &str, priority: &str) {
    if let Err(error) = show_xdg_notification(title, body, priority) {
        log_failure(&error);
    }
}

/// OS-native desktop notification sink.
pub(crate) struct DesktopSink;

impl NotificationSink for DesktopSink {
    fn name(&self) -> &'static str {
        "desktop"
    }

    fn deliver(&self, n: &SinkNotification) {
        let title = n.title.clone();
        let body = n.body.clone();
        let priority = n.priority.clone();
        tokio::spawn(async move {
            // The notification call blocks the calling thread (it drives
            // D-Bus/mac APIs synchronously internally) — never call it
            // directly on a tokio worker thread.
            if let Err(error) = tokio::task::spawn_blocking(move || {
                fire_desktop_notification(&title, &body, &priority)
            })
            .await
            {
                tracing::warn!(%error, "desktop notification sink: blocking task panicked/was cancelled");
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn desktop_enabled_auto_is_on_standalone_off_sidecar() {
        assert!(desktop_enabled(None, false));
        assert!(!desktop_enabled(None, true));
    }

    #[test]
    fn desktop_enabled_explicit_override_wins_either_direction() {
        assert!(desktop_enabled(Some(true), true));
        assert!(!desktop_enabled(Some(false), false));
    }

    #[test]
    fn sidecar_mode_round_trips_through_the_process_static() {
        set_sidecar_mode(true);
        assert!(is_sidecar_mode());
        set_sidecar_mode(false);
        assert!(!is_sidecar_mode());
    }

    #[test]
    fn suppressed_by_watcher_only_for_run_completed_and_needs_categories() {
        assert!(suppressed_by_watcher("run_completed", true));
        assert!(suppressed_by_watcher("needs_approval", true));
        assert!(suppressed_by_watcher("needs_clarification", true));
        assert!(!suppressed_by_watcher("run_completed", false));
        assert!(!suppressed_by_watcher("needs_approval", false));
        assert!(!suppressed_by_watcher("run_failed", true));
        assert!(!suppressed_by_watcher("context_critical", true));
        assert!(!suppressed_by_watcher("subagent_completed", true));
        assert!(!suppressed_by_watcher("background_task_completed", true));
        assert!(!suppressed_by_watcher("custom", true));
    }

    #[cfg(unix)]
    #[test]
    fn xdg_notify_wire_preserves_original_desktop_contract() {
        type DecodedPayload = (
            String,
            u32,
            String,
            String,
            String,
            Vec<String>,
            std::collections::HashMap<String, zbus::OwnedVariant>,
            i32,
        );
        for (priority, urgency) in [("high", 2), ("normal", 1), ("low", 1), ("HIGH", 1), ("", 1)] {
            let payload = xdg_notify_payload("bamboo".into(), "任务完成", "body\n<text>", priority);
            use zbus::DynamicType;
            let context = zbus::wire::serialized::Context::new(zbus::wire::LE, 0);
            let encoded = zbus::wire::to_bytes(context, &payload).unwrap();
            assert_eq!(payload.signature().to_string_no_parens(), "susssasa{sv}i");
            let (decoded, consumed): (DecodedPayload, usize) = encoded.deserialize().unwrap();
            assert_eq!(consumed, encoded.bytes().len());
            assert_eq!(decoded.0, "bamboo");
            assert_eq!(decoded.1, 0);
            assert!(decoded.2.is_empty());
            assert_eq!(decoded.3, "任务完成");
            assert_eq!(decoded.4, "body\n<text>");
            assert!(decoded.5.is_empty());
            assert_eq!(decoded.6.len(), 1);
            assert_eq!(u8::try_from(&decoded.6["urgency"]).unwrap(), urgency);
            assert_eq!(decoded.7, -1);
        }
    }
}
