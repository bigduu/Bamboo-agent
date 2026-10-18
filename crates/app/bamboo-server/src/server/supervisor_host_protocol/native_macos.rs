//! AppKit is confined to the initial thread of Bamboo's own UI-only helper.
//! A local process identity, parent PID or IPC handshake is not user consent.
use super::confirmation::{HelperDecision, Unavailable};

#[cfg(target_os = "macos")]
mod platform {
    use super::*;
    use objc2::{
        msg_send,
        rc::{autoreleasepool, Allocated, Retained},
        runtime::{AnyClass, AnyObject, Bool},
        MainThreadMarker,
    };
    use objc2_foundation::{NSPoint, NSRect, NSSize, NSString};
    use std::{ffi::c_void, ptr};

    #[link(name = "AppKit", kind = "framework")]
    unsafe extern "C" {}
    #[link(name = "Security", kind = "framework")]
    unsafe extern "C" {
        fn SessionGetInfo(session: u32, actual: *mut u32, attributes: *mut u32) -> i32;
    }
    #[link(name = "SystemConfiguration", kind = "framework")]
    unsafe extern "C" {
        fn SCDynamicStoreCopyConsoleUser(
            store: *const c_void,
            uid: *mut u32,
            gid: *mut u32,
        ) -> *const c_void;
    }
    #[link(name = "CoreFoundation", kind = "framework")]
    unsafe extern "C" {
        fn CFRelease(value: *const c_void);
    }

    pub(super) fn readiness() -> Result<(), Unavailable> {
        // Graphic-session membership plus the active console user's uid prevent
        // SSH, loginwindow and a background/root session from claiming readiness.
        // These checks permit asking once; they are never an approval decision.
        let mut attributes = 0_u32;
        let mut uid = u32::MAX;
        let mut gid = u32::MAX;
        // SAFETY: documented Security/SystemConfiguration C ABI, valid outputs;
        // the returned CoreFoundation object is released exactly once.
        unsafe {
            if SessionGetInfo(u32::MAX, ptr::null_mut(), &mut attributes) != 0
                || attributes & 0x10 == 0
            {
                return Err(Unavailable::NoGui);
            }
            let user = SCDynamicStoreCopyConsoleUser(ptr::null(), &mut uid, &mut gid);
            if user.is_null() {
                return Err(Unavailable::NoGui);
            }
            CFRelease(user);
            if uid == 0 || uid != libc::geteuid() {
                return Err(Unavailable::NoGui);
            }
        }
        Ok(())
    }

    pub(super) fn show(preview: &str) -> HelperDecision {
        if MainThreadMarker::new().is_none() {
            return HelperDecision::Unavailable {
                code: Unavailable::ThreadUnavailable,
            };
        }
        if let Err(code) = readiness() {
            return HelperDecision::Unavailable { code };
        }
        autoreleasepool(|_| {
            let Some(app_class) = AnyClass::get(c"NSApplication") else {
                return unavailable();
            };
            let Some(alert_class) = AnyClass::get(c"NSAlert") else {
                return unavailable();
            };
            let Some(scroll_class) = AnyClass::get(c"NSScrollView") else {
                return unavailable();
            };
            let Some(text_class) = AnyClass::get(c"NSTextView") else {
                return unavailable();
            };
            // SAFETY: AppKit is linked and all messages use its SDK-declared ABI.
            // The marker above pins all NSView/window work to the initial main
            // thread, inside an autorelease pool; retained objects outlive runModal.
            unsafe {
                let app: Retained<AnyObject> = msg_send![app_class, sharedApplication];
                let policy: Bool = msg_send![&*app, setActivationPolicy: 1_isize]; // accessory
                if !policy.as_bool() {
                    return unavailable();
                }
                let _: () = msg_send![&*app, finishLaunching];
                let alert: Retained<AnyObject> = msg_send![alert_class, new];
                let title = NSString::from_str("Bamboo: change Supervisor Project scope?");
                let info = NSString::from_str("Review the full change below. Allow applies this request once. Cancel is the default; no approval is remembered.");
                let _: () = msg_send![&*alert, setMessageText: &*title];
                let _: () = msg_send![&*alert, setInformativeText: &*info];
                let _: () = msg_send![&*alert, setAlertStyle: 0_usize];
                let _: () = msg_send![&*alert, setShowsSuppressionButton: false];
                let cancel_title = NSString::from_str("Cancel");
                let allow_title = NSString::from_str("Allow this change once");
                let cancel: Retained<AnyObject> =
                    msg_send![&*alert, addButtonWithTitle: &*cancel_title];
                let allow: Retained<AnyObject> =
                    msg_send![&*alert, addButtonWithTitle: &*allow_title];
                let enter = NSString::from_str("\r");
                let no_key = NSString::from_str("");
                let _: () = msg_send![&*cancel, setKeyEquivalent: &*enter];
                let _: () = msg_send![&*allow, setKeyEquivalent: &*no_key];
                let rect = NSRect::new(NSPoint::new(0.0, 0.0), NSSize::new(640.0, 360.0));
                let scroll: Allocated<AnyObject> = msg_send![scroll_class, alloc];
                let scroll: Retained<AnyObject> = msg_send![scroll, initWithFrame: rect];
                let text: Allocated<AnyObject> = msg_send![text_class, alloc];
                let text: Retained<AnyObject> = msg_send![text, initWithFrame: rect];
                let value = NSString::from_str(preview);
                let _: () = msg_send![&*text, setEditable: false];
                let _: () = msg_send![&*text, setSelectable: true];
                let _: () = msg_send![&*text, setRichText: false];
                let _: () = msg_send![&*text, setVerticallyResizable: true];
                let _: () = msg_send![&*text, setHorizontallyResizable: false];
                let _: () = msg_send![&*text, setAutoresizingMask: 2_usize]; // width sizable
                let _: () = msg_send![&*text, setMaxSize: NSSize::new(640.0, f64::MAX)];
                let container: Retained<AnyObject> = msg_send![&*text, textContainer];
                let _: () = msg_send![&*container, setContainerSize: NSSize::new(640.0, f64::MAX)];
                let _: () = msg_send![&*container, setWidthTracksTextView: true];
                let _: () = msg_send![&*text, setString: &*value];
                let _: () = msg_send![&*scroll, setHasVerticalScroller: true];
                let _: () = msg_send![&*scroll, setDocumentView: &*text];
                let _: () = msg_send![&*alert, setAccessoryView: &*scroll];
                let _: () = msg_send![&*app, activateIgnoringOtherApps: true];
                // Only the second, explicit Allow button is approval. Close,
                // escape, unknown modal codes and interrupted helpers deny.
                let response: isize = msg_send![&*alert, runModal];
                modal_decision(response)
            }
        })
    }
    fn unavailable() -> HelperDecision {
        HelperDecision::Unavailable {
            code: Unavailable::UiUnavailable,
        }
    }
}

pub(super) fn readiness() -> Result<(), Unavailable> {
    #[cfg(target_os = "macos")]
    {
        platform::readiness()
    }
    #[cfg(not(target_os = "macos"))]
    {
        Err(Unavailable::UnsupportedPlatform)
    }
}

pub(super) fn show(preview: &str) -> HelperDecision {
    #[cfg(target_os = "macos")]
    {
        platform::show(preview)
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = preview;
        HelperDecision::Unavailable {
            code: Unavailable::UnsupportedPlatform,
        }
    }
}

#[cfg(any(target_os = "macos", test))]
fn modal_decision(response: isize) -> HelperDecision {
    match response {
        1001 => HelperDecision::Approve,
        1000 => HelperDecision::Cancel,
        -1000 | -1001 => HelperDecision::WindowClosed,
        _ => HelperDecision::Unavailable {
            code: Unavailable::UiUnavailable,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn only_explicit_second_button_allows_default_cancel_and_unknown_deny() {
        assert!(matches!(modal_decision(1001), HelperDecision::Approve));
        assert!(matches!(modal_decision(1000), HelperDecision::Cancel));
        for response in [-1, 0, 1, 1002, isize::MAX] {
            assert!(matches!(
                modal_decision(response),
                HelperDecision::Unavailable {
                    code: Unavailable::UiUnavailable
                }
            ));
        }
        for response in [-1001, -1000] {
            assert!(matches!(
                modal_decision(response),
                HelperDecision::WindowClosed
            ));
        }
    }
    #[cfg(target_os = "macos")]
    #[test]
    fn non_main_test_thread_denies_before_any_appkit_window_is_created() {
        assert!(matches!(
            show("must not be displayed"),
            HelperDecision::Unavailable {
                code: Unavailable::ThreadUnavailable
            }
        ));
    }
}
