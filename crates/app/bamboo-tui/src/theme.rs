//! Terminal colour palette selection.
//!
//! Renderers resolve semantic roles through [`color`] (or the convenience
//! functions in [`colors`]) so the palette selected for one frame is applied
//! consistently without hard-coded colours leaking into widgets.

use std::cell::Cell;
use std::fmt;
use std::str::FromStr;

use ratatui::style::Color;

/// The terminal colour strategy used by the TUI.
///
/// `System` deliberately uses named ANSI colours rather than fixed RGB values,
/// allowing the terminal's own light/dark palette to provide the contrast.
/// `NoColor` maps every role to [`Color::Reset`]; callers must therefore keep
/// semantic text and glyphs (for example `error`/`!`/`x`) alongside colour.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ThemePalette {
    /// Bamboo's existing 24-bit RGB palette.
    #[default]
    TrueColor = 0,
    /// Named ANSI colours supplied by the terminal theme.
    System = 1,
    /// No foreground colour overrides.
    NoColor = 2,
}

impl ThemePalette {
    /// Canonical CLI/config spelling.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TrueColor => "truecolor",
            Self::System => "system",
            Self::NoColor => "no-color",
        }
    }

    /// Whether this palette uses colour to enhance the UI.
    pub const fn uses_color(self) -> bool {
        !matches!(self, Self::NoColor)
    }

    /// Resolve one semantic role without consulting global state.
    pub const fn color(self, role: ColorRole) -> Color {
        match self {
            Self::TrueColor => role.truecolor(),
            Self::System => role.system_color(),
            Self::NoColor => Color::Reset,
        }
    }
}

impl fmt::Display for ThemePalette {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Error returned when a CLI/config palette value is not recognised.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThemePaletteParseError {
    value: String,
}

impl fmt::Display for ThemePaletteParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "unknown theme palette {:?}; expected truecolor, system, or no-color",
            self.value
        )
    }
}

impl std::error::Error for ThemePaletteParseError {}

impl FromStr for ThemePalette {
    type Err = ThemePaletteParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let normalized = value.trim().to_ascii_lowercase();
        match normalized.as_str() {
            "truecolor" | "true-color" | "24bit" | "24-bit" => Ok(Self::TrueColor),
            "system" | "ansi" => Ok(Self::System),
            "no-color" | "nocolor" | "none" => Ok(Self::NoColor),
            _ => Err(ThemePaletteParseError {
                value: value.to_string(),
            }),
        }
    }
}

/// Semantic foreground roles shared by all palettes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColorRole {
    Brand,
    UserPrefix,
    ToolRunning,
    ToolDone,
    ToolError,
    Inactive,
    Subtle,
    Thinking,
    CodeBorder,
    Success,
    Error,
    Warning,
}

impl ColorRole {
    const fn truecolor(self) -> Color {
        match self {
            Self::Brand => Color::Rgb(215, 119, 87),
            Self::UserPrefix => Color::Rgb(122, 180, 232),
            Self::ToolRunning | Self::Thinking => Color::Rgb(147, 165, 255),
            Self::ToolDone | Self::Success => Color::Rgb(78, 186, 101),
            Self::ToolError | Self::Error => Color::Rgb(255, 107, 128),
            Self::Inactive => Color::Rgb(153, 153, 153),
            Self::Subtle | Self::CodeBorder => Color::Rgb(80, 80, 80),
            Self::Warning => Color::Rgb(255, 193, 7),
        }
    }

    const fn system_color(self) -> Color {
        match self {
            Self::Brand => Color::Magenta,
            Self::UserPrefix => Color::Cyan,
            Self::ToolRunning | Self::Thinking => Color::Blue,
            Self::ToolDone | Self::Success => Color::Green,
            Self::ToolError | Self::Error => Color::Red,
            // Reset inherits the terminal's foreground, which remains legible
            // on both light and dark user-selected palettes.
            Self::Inactive | Self::Subtle | Self::CodeBorder => Color::Reset,
            Self::Warning => Color::Yellow,
        }
    }
}

thread_local! {
    static ACTIVE_PALETTE: Cell<ThemePalette> = const { Cell::new(ThemePalette::TrueColor) };
}

/// Return the currently selected process-wide palette.
pub fn active_palette() -> ThemePalette {
    ACTIVE_PALETTE.get()
}

/// Resolve colours under `palette` for one synchronous render operation.
/// The previous value is restored even when renderers are nested. A
/// thread-local scope keeps parallel TestBackend snapshots independent.
pub fn with_palette<T>(palette: ThemePalette, render: impl FnOnce() -> T) -> T {
    struct Restore(ThemePalette);
    impl Drop for Restore {
        fn drop(&mut self) {
            ACTIVE_PALETTE.set(self.0);
        }
    }

    let restore = Restore(ACTIVE_PALETTE.replace(palette));
    let result = render();
    drop(restore);
    result
}

/// Resolve the initial palette from an explicit CLI/config selection and the
/// conventional `NO_COLOR` environment signal.
///
/// An explicit selection wins, allowing `--theme truecolor` to override an
/// inherited `NO_COLOR`. Without an explicit selection, the presence of
/// `NO_COLOR` selects [`ThemePalette::NoColor`], including when its value is
/// empty. Otherwise the existing true-colour appearance is preserved.
pub const fn resolve_initial_palette(
    explicit: Option<ThemePalette>,
    no_color_present: bool,
) -> ThemePalette {
    match explicit {
        Some(palette) => palette,
        None if no_color_present => ThemePalette::NoColor,
        None => ThemePalette::TrueColor,
    }
}

/// Resolve a semantic colour using the current process-wide palette.
pub fn color(role: ColorRole) -> Color {
    active_palette().color(role)
}

pub mod colors {
    use super::{color, ColorRole};
    use ratatui::style::Color;

    macro_rules! palette_color {
        ($name:ident, $role:ident) => {
            #[doc = concat!("Return the current palette's `", stringify!($role), "` colour.")]
            pub fn $name() -> Color {
                color(ColorRole::$role)
            }
        };
    }

    palette_color!(brand, Brand);
    palette_color!(user_prefix, UserPrefix);
    palette_color!(tool_running, ToolRunning);
    palette_color!(tool_done, ToolDone);
    palette_color!(tool_error, ToolError);
    palette_color!(inactive, Inactive);
    palette_color!(subtle, Subtle);
    palette_color!(thinking, Thinking);
    palette_color!(code_border, CodeBorder);
    palette_color!(success, Success);
    palette_color!(error, Error);
    palette_color!(warning, Warning);
}

pub const BRAILLE_SPINNER: &[&str] = &["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];

#[cfg(test)]
mod tests {
    use super::*;

    const ALL_ROLES: [ColorRole; 12] = [
        ColorRole::Brand,
        ColorRole::UserPrefix,
        ColorRole::ToolRunning,
        ColorRole::ToolDone,
        ColorRole::ToolError,
        ColorRole::Inactive,
        ColorRole::Subtle,
        ColorRole::Thinking,
        ColorRole::CodeBorder,
        ColorRole::Success,
        ColorRole::Error,
        ColorRole::Warning,
    ];

    #[test]
    fn parses_cli_palette_names_and_aliases() {
        for (value, expected) in [
            ("truecolor", ThemePalette::TrueColor),
            ("TRUE-COLOR", ThemePalette::TrueColor),
            ("24-bit", ThemePalette::TrueColor),
            ("system", ThemePalette::System),
            ("ansi", ThemePalette::System),
            ("no-color", ThemePalette::NoColor),
            ("none", ThemePalette::NoColor),
        ] {
            assert_eq!(value.parse::<ThemePalette>().unwrap(), expected);
        }

        let error = "rainbow".parse::<ThemePalette>().unwrap_err().to_string();
        assert!(error.contains("rainbow"));
        assert!(error.contains("truecolor, system, or no-color"));
    }

    #[test]
    fn displays_canonical_cli_names() {
        assert_eq!(ThemePalette::TrueColor.to_string(), "truecolor");
        assert_eq!(ThemePalette::System.to_string(), "system");
        assert_eq!(ThemePalette::NoColor.to_string(), "no-color");
    }

    #[test]
    fn explicit_selection_precedes_no_color_and_default_is_truecolor() {
        assert_eq!(
            resolve_initial_palette(Some(ThemePalette::System), true),
            ThemePalette::System
        );
        assert_eq!(resolve_initial_palette(None, true), ThemePalette::NoColor);
        assert_eq!(
            resolve_initial_palette(None, false),
            ThemePalette::TrueColor
        );
    }

    #[test]
    fn no_color_resets_every_semantic_role() {
        assert!(!ThemePalette::NoColor.uses_color());
        for role in ALL_ROLES {
            assert_eq!(ThemePalette::NoColor.color(role), Color::Reset);
        }
    }

    #[test]
    fn system_palette_uses_terminal_colors_and_reset_for_neutral_roles() {
        assert_eq!(ThemePalette::System.color(ColorRole::Brand), Color::Magenta);
        assert_eq!(ThemePalette::System.color(ColorRole::Success), Color::Green);
        assert_eq!(ThemePalette::System.color(ColorRole::Error), Color::Red);
        assert_eq!(
            ThemePalette::System.color(ColorRole::Warning),
            Color::Yellow
        );
        assert_eq!(
            ThemePalette::System.color(ColorRole::Inactive),
            Color::Reset
        );
        assert!(ALL_ROLES
            .into_iter()
            .all(|role| !matches!(ThemePalette::System.color(role), Color::Rgb(_, _, _))));
    }
}
