pub mod colors {
    use ratatui::style::Color;

    pub const BRAND: Color = Color::Rgb(215, 119, 87);
    pub const USER_PREFIX: Color = Color::Rgb(122, 180, 232);
    pub const TOOL_RUNNING: Color = Color::Rgb(147, 165, 255);
    pub const TOOL_DONE: Color = Color::Rgb(78, 186, 101);
    pub const TOOL_ERROR: Color = Color::Rgb(255, 107, 128);
    pub const INACTIVE: Color = Color::Rgb(153, 153, 153);
    pub const SUBTLE: Color = Color::Rgb(80, 80, 80);
    pub const THINKING: Color = Color::Rgb(147, 165, 255);
    pub const CODE_BORDER: Color = Color::Rgb(80, 80, 80);
    pub const SUCCESS: Color = Color::Rgb(78, 186, 101);
    pub const ERROR: Color = Color::Rgb(255, 107, 128);
    pub const WARNING: Color = Color::Rgb(255, 193, 7);
}

pub const BRAILLE_SPINNER: &[&str] = &["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];
