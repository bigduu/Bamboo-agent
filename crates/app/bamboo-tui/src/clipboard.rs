use std::sync::Arc;

use anyhow::Result;
use bamboo_client_core::ChatImage;
use base64::prelude::{Engine as _, BASE64_STANDARD};

// 18 MiB of PNG becomes 24 MiB of base64, leaving room in Bamboo's
// 25 MiB JSON request limit for text and metadata.
pub(crate) const MAX_IMAGE_BYTES: usize = 18 * 1024 * 1024;

#[derive(Default)]
pub(crate) struct ClipboardContents {
    pub(crate) image: Option<Arc<ClipboardImage>>,
    pub(crate) text: Option<String>,
}

pub(crate) struct ClipboardImage {
    pub(crate) label: String,
    png: Vec<u8>,
}

impl std::fmt::Debug for ClipboardImage {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ClipboardImage")
            .field("label", &self.label)
            .field("size", &self.png.len())
            .finish()
    }
}

impl ClipboardImage {
    #[cfg(any(target_os = "macos", test))]
    pub(crate) fn new(png: Vec<u8>, width: usize, height: usize) -> Self {
        Self {
            label: format!("Screenshot · {width}×{height}"),
            png,
        }
    }

    pub(crate) fn size(&self) -> usize {
        self.png.len()
    }

    pub(crate) fn to_chat_image(&self) -> ChatImage {
        let mut data_url = String::from("data:image/png;base64,");
        BASE64_STANDARD.encode_string(&self.png, &mut data_url);
        ChatImage {
            base64: data_url,
            name: "screenshot.png".to_string(),
            size: self.png.len(),
            mime_type: "image/png".to_string(),
        }
    }
}

#[cfg(not(target_os = "macos"))]
pub(crate) fn read() -> Result<ClipboardContents> {
    anyhow::bail!("System screenshot paste is available on macOS")
}

#[cfg(target_os = "macos")]
pub(crate) use macos::read;

#[cfg(target_os = "macos")]
mod macos {
    use anyhow::{anyhow, ensure, Context};
    use objc2::rc::{autoreleasepool, Retained};
    use objc2::{msg_send, ClassType};
    use objc2_app_kit::{
        NSBitmapImageFileType, NSBitmapImageRep, NSPasteboard, NSPasteboardTypePNG,
        NSPasteboardTypeString, NSPasteboardTypeTIFF,
    };
    use objc2_foundation::{NSData, NSDictionary};

    use super::*;

    const MAX_BITMAP_PIXELS: usize = 64 * 1024 * 1024;

    pub(crate) fn read() -> Result<ClipboardContents> {
        autoreleasepool(|_| {
            // AppKit can return nil when a process has no pasteboard service.
            // SAFETY: generalPasteboard returns a retained-compatible object
            // reference. Option permits the nil return in headless sessions.
            let pasteboard: Option<Retained<NSPasteboard>> =
                unsafe { msg_send![NSPasteboard::class(), generalPasteboard] };
            let pasteboard = pasteboard.context("System clipboard is unavailable")?;
            read_pasteboard(&pasteboard)
        })
    }

    fn read_pasteboard(pasteboard: &NSPasteboard) -> Result<ClipboardContents> {
        // SAFETY: AppKit exposes these immutable type identifiers for NSPasteboard.
        let (png_type, tiff_type, string_type) = unsafe {
            (
                NSPasteboardTypePNG,
                NSPasteboardTypeTIFF,
                NSPasteboardTypeString,
            )
        };
        if let Some(png) = pasteboard.dataForType(png_type) {
            ensure!(
                png.len() <= MAX_IMAGE_BYTES,
                "Screenshot exceeds the 18 MiB image limit"
            );
            return image_contents(&png, true);
        }
        if let Some(tiff) = pasteboard.dataForType(tiff_type) {
            return image_contents(&tiff, false);
        }
        Ok(ClipboardContents {
            image: None,
            text: pasteboard
                .stringForType(string_type)
                .map(|text| text.to_string()),
        })
    }

    fn image_contents(data: &NSData, is_png: bool) -> Result<ClipboardContents> {
        ensure!(
            data.len() <= MAX_BITMAP_PIXELS * 4,
            "Clipboard screenshot source is too large"
        );
        let bitmap = NSBitmapImageRep::imageRepWithData(data)
            .context("Clipboard screenshot could not be decoded")?;
        let width = usize::try_from(bitmap.pixelsWide())?;
        let height = usize::try_from(bitmap.pixelsHigh())?;
        ensure!(
            width > 0
                && height > 0
                && width
                    .checked_mul(height)
                    .is_some_and(|pixels| pixels <= MAX_BITMAP_PIXELS),
            "Clipboard screenshot dimensions are unsupported"
        );
        let png = if is_png {
            data.to_vec()
        } else {
            // SAFETY: An empty property dictionary requests AppKit's default
            // PNG encoding and contains no values with mismatched types.
            let data = unsafe {
                bitmap.representationUsingType_properties(
                    NSBitmapImageFileType::PNG,
                    &NSDictionary::new(),
                )
            }
            .ok_or_else(|| anyhow!("Clipboard screenshot could not be encoded as PNG"))?;
            ensure!(
                data.len() <= MAX_IMAGE_BYTES,
                "Screenshot exceeds the 18 MiB image limit"
            );
            data.to_vec()
        };
        Ok(ClipboardContents {
            image: Some(Arc::new(ClipboardImage::new(png, width, height))),
            text: None,
        })
    }

    #[cfg(test)]
    #[test]
    fn native_pasteboard_reads_png_tiff_and_text() {
        use objc2_foundation::ns_string;

        autoreleasepool(|_| {
            let pasteboard = NSPasteboard::pasteboardWithUniqueName();
            let png_bytes = BASE64_STANDARD.decode(super::TEST_PNG).unwrap();
            let source = NSData::with_bytes(&png_bytes);
            let bitmap = NSBitmapImageRep::imageRepWithData(&source).unwrap();
            // SAFETY: Native type identifiers and the default TIFF properties.
            let (png_type, tiff_type, string_type, tiff) = unsafe {
                (
                    NSPasteboardTypePNG,
                    NSPasteboardTypeTIFF,
                    NSPasteboardTypeString,
                    bitmap
                        .representationUsingType_properties(
                            NSBitmapImageFileType::TIFF,
                            &NSDictionary::new(),
                        )
                        .unwrap(),
                )
            };
            for (format, data) in [(png_type, &*source), (tiff_type, &*tiff)] {
                pasteboard.clearContents();
                assert!(pasteboard.setData_forType(Some(data), format));
                let contents = read_pasteboard(&pasteboard).unwrap();
                let screenshot = contents.image.unwrap();
                assert_eq!(screenshot.label, "Screenshot · 2×3");
                assert!(screenshot.png.starts_with(b"\x89PNG\r\n\x1a\n"));
                let encoded = NSData::with_bytes(&screenshot.png);
                let png = NSBitmapImageRep::imageRepWithData(&encoded).unwrap();
                assert_eq!((png.pixelsWide(), png.pixelsHigh()), (2, 3));
                if format == png_type {
                    assert_eq!(screenshot.png, png_bytes);
                }
            }
            pasteboard.clearContents();
            assert!(pasteboard.setString_forType(ns_string!("ordinary text"), string_type));
            let contents = read_pasteboard(&pasteboard).unwrap();
            assert!(contents.image.is_none());
            assert_eq!(contents.text.as_deref(), Some("ordinary text"));
            pasteboard.clearContents();
        });
    }
}

#[cfg(test)]
pub(crate) const TEST_PNG: &str =
    "iVBORw0KGgoAAAANSUhEUgAAAAIAAAADCAIAAAA2iEnWAAAAEElEQVR4nGP4zwAE/xlQKAA+1gX7XtdWVQAAAABJRU5ErkJggg==";
