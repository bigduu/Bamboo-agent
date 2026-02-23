/// Copy text to clipboard
///
/// Platform-specific implementation that uses arboard on non-Linux systems.
/// On Linux (including Docker), returns an error suggesting to use Web API.
pub fn copy_to_clipboard(text: String) -> Result<(), String> {
    #[cfg(not(target_os = "linux"))]
    {
        use arboard::Clipboard;
        let mut clipboard = Clipboard::new().map_err(|e| e.to_string())?;
        clipboard.set_text(text).map_err(|e| e.to_string())?;
        Ok(())
    }

    #[cfg(target_os = "linux")]
    {
        // On Linux (including Docker), suggest using Web API
        // The frontend should handle this gracefully
        Err("Clipboard not available on Linux - use Web API".to_string())
    }
}
