//! OS-native desktop notification sink (`notify-rust`).
//!
//! Delivery is best-effort: `notify_rust::Notification::show` is a
//! synchronous, potentially-blocking call (it drives D-Bus/mac APIs
//! internally), so it always runs on `spawn_blocking`, and every failure is
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

#[cfg(not(target_os = "macos"))]
fn ensure_mac_application_set() {}

/// Maps [`SinkNotification::priority`] to `notify_rust::Urgency` on the
/// platforms that support it (Linux/BSD via XDG, Windows). `notify-rust`
/// 4.18 with default features has no urgency mapping on macOS (that needs
/// the `preview-macos-un` feature, which this crate doesn't enable) — a
/// no-op there.
#[cfg(any(all(unix, not(target_os = "macos")), target_os = "windows"))]
fn apply_urgency(notification: &mut notify_rust::Notification, priority: &str) {
    let urgency = if priority == "high" {
        notify_rust::Urgency::Critical
    } else {
        notify_rust::Urgency::Normal
    };
    notification.urgency(urgency);
}

#[cfg(not(any(all(unix, not(target_os = "macos")), target_os = "windows")))]
fn apply_urgency(_notification: &mut notify_rust::Notification, _priority: &str) {}

/// Rate-limits the failure log: the first failure in this process's lifetime
/// warns (surfaces a genuinely broken notification backend), every
/// subsequent failure only debugs (a headless Linux box with no D-Bus session
/// would otherwise warn on every single notification, forever).
static WARNED_ONCE: AtomicBool = AtomicBool::new(false);

fn log_failure(error: &notify_rust::error::Error) {
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
fn fire_desktop_notification(title: &str, body: &str, priority: &str) {
    ensure_mac_application_set();
    let mut notification = notify_rust::Notification::new();
    notification.summary(title).body(body);
    apply_urgency(&mut notification, priority);
    if let Err(error) = notification.show() {
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
            // `Notification::show()` blocks the calling thread (it drives
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
}
