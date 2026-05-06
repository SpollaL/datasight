use ratatui::style::Color;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

pub struct Base16Scheme {
    pub name: &'static str,
    pub display_name: &'static str,
    pub base: [[u8; 3]; 16],
}

#[derive(Debug)]
pub struct Theme {
    pub name: &'static str,
    /// Human-friendly name shown in the in-app theme picker.
    pub display_name: &'static str,
    pub bg: Color,
    pub bg_alt: Color,
    pub bg_sel: Color,
    pub border_idle: Color,
    pub fg_muted: Color,
    pub fg_dim: Color,
    pub fg: Color,
    pub accent: Color,
    pub error: Color,
    pub warn: Color,
    pub success: Color,
    pub info: Color,
    pub series: [Color; 6],
}

const fn rgb(c: [u8; 3]) -> Color {
    Color::Rgb(c[0], c[1], c[2])
}

impl Theme {
    pub const fn from_scheme(s: &Base16Scheme) -> Self {
        Self {
            name: s.name,
            display_name: s.display_name,
            bg: rgb(s.base[0x00]),
            bg_alt: rgb(s.base[0x01]),
            bg_sel: rgb(s.base[0x02]),
            border_idle: rgb(s.base[0x03]),
            fg_muted: rgb(s.base[0x03]),
            fg_dim: rgb(s.base[0x04]),
            fg: rgb(s.base[0x05]),
            accent: rgb(s.base[0x0D]),
            error: rgb(s.base[0x08]),
            warn: rgb(s.base[0x0A]),
            success: rgb(s.base[0x0B]),
            info: rgb(s.base[0x0C]),
            series: [
                rgb(s.base[0x0D]),
                rgb(s.base[0x0B]),
                rgb(s.base[0x09]),
                rgb(s.base[0x0E]),
                rgb(s.base[0x0C]),
                rgb(s.base[0x0A]),
            ],
        }
    }
}

pub static MOCHA: Base16Scheme = Base16Scheme {
    name: "mocha",
    display_name: "Catppuccin Mocha",
    base: [
        [0x1e, 0x1e, 0x2e],
        [0x18, 0x18, 0x25],
        [0x31, 0x32, 0x44],
        [0x45, 0x47, 0x5a],
        [0x58, 0x5b, 0x70],
        [0xcd, 0xd6, 0xf4],
        [0xf5, 0xe0, 0xdc],
        [0xb4, 0xbe, 0xfe],
        [0xf3, 0x8b, 0xa8],
        [0xfa, 0xb3, 0x87],
        [0xf9, 0xe2, 0xaf],
        [0xa6, 0xe3, 0xa1],
        [0x94, 0xe2, 0xd5],
        [0x89, 0xb4, 0xfa],
        [0xcb, 0xa6, 0xf7],
        [0xf2, 0xcd, 0xcd],
    ],
};

pub static LATTE: Base16Scheme = Base16Scheme {
    name: "latte",
    display_name: "Catppuccin Latte",
    base: [
        [0xef, 0xf1, 0xf5],
        [0xe6, 0xe9, 0xef],
        [0xcc, 0xd0, 0xda],
        [0xbc, 0xc0, 0xcc],
        [0xac, 0xb0, 0xbe],
        [0x4c, 0x4f, 0x69],
        [0xdc, 0x8a, 0x78],
        [0x7c, 0x7f, 0x93],
        [0xd2, 0x0f, 0x39],
        [0xfe, 0x64, 0x0b],
        [0xdf, 0x8e, 0x1d],
        [0x40, 0xa0, 0x2b],
        [0x17, 0x91, 0x99],
        [0x1e, 0x66, 0xf5],
        [0x88, 0x39, 0xef],
        [0xdd, 0x76, 0x78],
    ],
};

pub static FRAPPE: Base16Scheme = Base16Scheme {
    name: "frappe",
    display_name: "Catppuccin Frappé",
    base: [
        [0x30, 0x33, 0x46],
        [0x29, 0x2c, 0x3c],
        [0x41, 0x45, 0x59],
        [0x51, 0x57, 0x6d],
        [0x62, 0x68, 0x80],
        [0xc6, 0xd0, 0xf5],
        [0xf2, 0xd5, 0xcf],
        [0xba, 0xbb, 0xf1],
        [0xe7, 0x82, 0x84],
        [0xef, 0x9f, 0x76],
        [0xe5, 0xc8, 0x90],
        [0xa6, 0xd1, 0x89],
        [0x81, 0xc8, 0xbe],
        [0x8c, 0xaa, 0xee],
        [0xca, 0x9e, 0xe6],
        [0xee, 0xbe, 0xbe],
    ],
};

pub static MACCHIATO: Base16Scheme = Base16Scheme {
    name: "macchiato",
    display_name: "Catppuccin Macchiato",
    base: [
        [0x24, 0x27, 0x3a],
        [0x1e, 0x20, 0x30],
        [0x36, 0x3a, 0x4f],
        [0x49, 0x4d, 0x64],
        [0x5b, 0x60, 0x78],
        [0xca, 0xd3, 0xf5],
        [0xf4, 0xdb, 0xd6],
        [0xb7, 0xbd, 0xf8],
        [0xed, 0x87, 0x96],
        [0xf5, 0xa9, 0x7f],
        [0xee, 0xd4, 0x9f],
        [0xa6, 0xda, 0x95],
        [0x8b, 0xd5, 0xca],
        [0x8a, 0xad, 0xf4],
        [0xc6, 0xa0, 0xf6],
        [0xf0, 0xc6, 0xc6],
    ],
};

pub static GRUVBOX_DARK: Base16Scheme = Base16Scheme {
    name: "gruvbox-dark",
    display_name: "Gruvbox Dark",
    base: [
        [0x28, 0x28, 0x28],
        [0x3c, 0x38, 0x36],
        [0x50, 0x49, 0x45],
        [0x66, 0x5c, 0x54],
        [0xbd, 0xae, 0x93],
        [0xd5, 0xc4, 0xa1],
        [0xeb, 0xdb, 0xb2],
        [0xfb, 0xf1, 0xc7],
        [0xfb, 0x49, 0x34],
        [0xfe, 0x80, 0x19],
        [0xfa, 0xbd, 0x2f],
        [0xb8, 0xbb, 0x26],
        [0x8e, 0xc0, 0x7c],
        [0x83, 0xa5, 0x98],
        [0xd3, 0x86, 0x9b],
        [0xd6, 0x5d, 0x0e],
    ],
};

pub static NORD: Base16Scheme = Base16Scheme {
    name: "nord",
    display_name: "Nord",
    base: [
        [0x2e, 0x34, 0x40],
        [0x3b, 0x42, 0x52],
        [0x43, 0x4c, 0x5e],
        [0x4c, 0x56, 0x6a],
        [0xd8, 0xde, 0xe9],
        [0xe5, 0xe9, 0xf0],
        [0xec, 0xef, 0xf4],
        [0x8f, 0xbc, 0xbb],
        [0xbf, 0x61, 0x6a],
        [0xd0, 0x87, 0x70],
        [0xeb, 0xcb, 0x8b],
        [0xa3, 0xbe, 0x8c],
        [0x88, 0xc0, 0xd0],
        [0x81, 0xa1, 0xc1],
        [0xb4, 0x8e, 0xad],
        [0x5e, 0x81, 0xac],
    ],
};

pub static DRACULA: Base16Scheme = Base16Scheme {
    name: "dracula",
    display_name: "Dracula",
    base: [
        [0x28, 0x29, 0x36],
        [0x3a, 0x3c, 0x4e],
        [0x4d, 0x4f, 0x68],
        [0x62, 0x64, 0x83],
        [0x62, 0x64, 0x83],
        [0xe9, 0xe9, 0xf4],
        [0xf1, 0xf2, 0xf8],
        [0xf7, 0xf7, 0xfb],
        [0xea, 0x51, 0xb2],
        [0xb4, 0x5b, 0xcf],
        [0x00, 0xf7, 0x69],
        [0xeb, 0xff, 0x87],
        [0xa1, 0xef, 0xe4],
        [0x62, 0xd6, 0xe8],
        [0xb4, 0x5b, 0xcf],
        [0x00, 0xf7, 0x69],
    ],
};

pub static SOLARIZED_DARK: Base16Scheme = Base16Scheme {
    name: "solarized-dark",
    display_name: "Solarized Dark",
    base: [
        [0x00, 0x2b, 0x36],
        [0x07, 0x36, 0x42],
        [0x58, 0x6e, 0x75],
        [0x65, 0x7b, 0x83],
        [0x83, 0x94, 0x96],
        [0x93, 0xa1, 0xa1],
        [0xee, 0xe8, 0xd5],
        [0xfd, 0xf6, 0xe3],
        [0xdc, 0x32, 0x2f],
        [0xcb, 0x4b, 0x16],
        [0xb5, 0x89, 0x00],
        [0x85, 0x99, 0x00],
        [0x2a, 0xa1, 0x98],
        [0x26, 0x8b, 0xd2],
        [0x6c, 0x71, 0xc4],
        [0xd3, 0x36, 0x82],
    ],
};

pub static TOKYO_NIGHT: Base16Scheme = Base16Scheme {
    name: "tokyo-night",
    display_name: "Tokyo Night",
    base: [
        [0x1a, 0x1b, 0x26],
        [0x16, 0x16, 0x1e],
        [0x2f, 0x33, 0x49],
        [0x44, 0x4b, 0x6a],
        [0x78, 0x7c, 0x99],
        [0xa9, 0xb1, 0xd6],
        [0xcb, 0xcc, 0xd1],
        [0xd5, 0xd6, 0xdb],
        [0xc0, 0xca, 0xf5],
        [0xa9, 0xb1, 0xd6],
        [0x0d, 0xb9, 0xd7],
        [0x9e, 0xce, 0x6a],
        [0xb4, 0xf9, 0xf8],
        [0x2a, 0xc3, 0xde],
        [0xbb, 0x9a, 0xf7],
        [0xf7, 0x76, 0x8e],
    ],
};

fn all_themes_init() -> Vec<Theme> {
    [
        &MOCHA,
        &LATTE,
        &FRAPPE,
        &MACCHIATO,
        &GRUVBOX_DARK,
        &NORD,
        &DRACULA,
        &SOLARIZED_DARK,
        &TOKYO_NIGHT,
    ]
    .iter()
    .map(|s| Theme::from_scheme(s))
    .collect()
}

pub fn list_themes() -> &'static [Theme] {
    static THEMES: OnceLock<Vec<Theme>> = OnceLock::new();
    THEMES.get_or_init(all_themes_init)
}

pub fn theme_by_name(name: &str) -> Option<&'static Theme> {
    list_themes().iter().find(|t| t.name == name)
}

pub fn default_theme() -> &'static Theme {
    theme_by_name("mocha").expect("mocha is always present")
}

pub fn theme_names_csv() -> String {
    list_themes()
        .iter()
        .map(|t| t.name)
        .collect::<Vec<_>>()
        .join(", ")
}

#[derive(serde::Deserialize, serde::Serialize)]
struct StateFile {
    theme: String,
}

pub fn state_path() -> Option<PathBuf> {
    dirs::config_dir().map(|p| p.join("datasight").join("state.toml"))
}

pub fn read_state_theme_at(path: &Path) -> Option<String> {
    let contents = std::fs::read_to_string(path).ok()?;
    let parsed: StateFile = toml::from_str(&contents).ok()?;
    Some(parsed.theme)
}

/// Persist the chosen theme name to `state.toml`.
pub fn write_state_theme_at(path: &Path, name: &str) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let state = StateFile {
        theme: name.to_string(),
    };
    let s = toml::to_string(&state).map_err(std::io::Error::other)?;
    std::fs::write(path, s)
}

pub fn resolve_theme(
    cli: Option<&str>,
    env: Option<String>,
    state: Option<String>,
) -> Result<&'static Theme, String> {
    if let Some(name) = cli {
        return theme_by_name(name)
            .ok_or_else(|| format!("unknown theme: {} (try: {})", name, theme_names_csv()));
    }
    if let Some(name) = env {
        return theme_by_name(&name)
            .ok_or_else(|| format!("unknown theme: {} (try: {})", name, theme_names_csv()));
    }
    if let Some(name) = state {
        if let Some(t) = theme_by_name(&name) {
            return Ok(t);
        }
    }
    Ok(default_theme())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mocha_loads_with_all_slots_populated() {
        let t = Theme::from_scheme(&MOCHA);
        assert_eq!(t.name, "mocha");
        assert_eq!(t.display_name, "Catppuccin Mocha");
        assert_eq!(t.bg, Color::Rgb(0x1e, 0x1e, 0x2e));
        assert_eq!(t.fg, Color::Rgb(0xcd, 0xd6, 0xf4));
        assert_eq!(t.accent, Color::Rgb(0x89, 0xb4, 0xfa));
        assert_eq!(t.error, Color::Rgb(0xf3, 0x8b, 0xa8));
        for s in t.series.iter() {
            assert!(matches!(s, Color::Rgb(_, _, _)));
        }
    }

    #[test]
    fn all_nine_builtins_load() {
        let names = [
            "mocha",
            "latte",
            "frappe",
            "macchiato",
            "gruvbox-dark",
            "nord",
            "dracula",
            "solarized-dark",
            "tokyo-night",
        ];
        for &n in &names {
            let scheme = match n {
                "mocha" => &MOCHA,
                "latte" => &LATTE,
                "frappe" => &FRAPPE,
                "macchiato" => &MACCHIATO,
                "gruvbox-dark" => &GRUVBOX_DARK,
                "nord" => &NORD,
                "dracula" => &DRACULA,
                "solarized-dark" => &SOLARIZED_DARK,
                "tokyo-night" => &TOKYO_NIGHT,
                _ => unreachable!(),
            };
            let t = Theme::from_scheme(scheme);
            assert_eq!(t.name, n, "name field for {}", n);
            assert_ne!(t.bg, t.fg, "bg == fg for {}", n);
        }
    }

    #[test]
    fn theme_by_name_returns_known() {
        assert_eq!(theme_by_name("mocha").map(|t| t.name), Some("mocha"));
        assert_eq!(theme_by_name("nord").map(|t| t.name), Some("nord"));
        assert_eq!(
            theme_by_name("gruvbox-dark").map(|t| t.name),
            Some("gruvbox-dark")
        );
    }

    #[test]
    fn theme_by_name_returns_none_for_unknown() {
        assert!(theme_by_name("does-not-exist").is_none());
        assert!(theme_by_name("").is_none());
    }

    #[test]
    fn list_themes_returns_all_nine() {
        let names: Vec<&str> = list_themes().iter().map(|t| t.name).collect();
        assert_eq!(names.len(), 9);
        assert!(names.contains(&"mocha"));
        assert!(names.contains(&"tokyo-night"));
    }

    #[test]
    fn theme_names_alphabetical_helper_returns_csv() {
        let csv = theme_names_csv();
        assert!(csv.contains("mocha"));
        assert!(csv.contains("nord"));
        assert!(csv.contains(", "));
    }

    #[test]
    fn default_theme_is_mocha() {
        assert_eq!(default_theme().name, "mocha");
    }

    #[test]
    fn read_state_returns_none_for_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.toml");
        assert!(read_state_theme_at(&path).is_none());
    }

    #[test]
    fn read_state_returns_none_for_malformed_toml() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.toml");
        std::fs::write(&path, "this is not valid toml ::: !!!").unwrap();
        assert!(read_state_theme_at(&path).is_none());
    }

    #[test]
    fn write_then_read_state_roundtrips() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.toml");
        write_state_theme_at(&path, "nord").unwrap();
        assert_eq!(read_state_theme_at(&path).as_deref(), Some("nord"));
    }

    #[test]
    fn write_state_creates_parent_directory() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nested").join("dir").join("state.toml");
        write_state_theme_at(&path, "dracula").unwrap();
        assert!(path.exists());
        assert_eq!(read_state_theme_at(&path).as_deref(), Some("dracula"));
    }

    #[test]
    fn resolve_uses_cli_arg_when_provided() {
        let result = resolve_theme(Some("nord"), Some("dracula".into()), Some("latte".into()));
        assert!(result.is_ok());
        assert_eq!(result.unwrap().name, "nord");
    }

    #[test]
    fn resolve_falls_back_to_env_when_no_cli() {
        let result = resolve_theme(None, Some("dracula".into()), Some("latte".into()));
        assert!(result.is_ok());
        assert_eq!(result.unwrap().name, "dracula");
    }

    #[test]
    fn resolve_falls_back_to_state_when_no_cli_or_env() {
        let result = resolve_theme(None, None, Some("latte".into()));
        assert!(result.is_ok());
        assert_eq!(result.unwrap().name, "latte");
    }

    #[test]
    fn resolve_falls_back_to_default_when_nothing_provided() {
        let result = resolve_theme(None, None, None);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().name, "mocha");
    }

    #[test]
    fn resolve_unknown_cli_returns_error() {
        let result = resolve_theme(Some("nonsense"), None, None);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.contains("unknown theme: nonsense"));
        assert!(
            err.contains("mocha"),
            "error should list valid themes, got: {}",
            err
        );
    }

    #[test]
    fn resolve_unknown_env_returns_error() {
        let result = resolve_theme(None, Some("nonsense".into()), None);
        assert!(result.is_err());
    }

    #[test]
    fn resolve_unknown_state_falls_back_to_default() {
        let result = resolve_theme(None, None, Some("stale-name".into()));
        assert!(result.is_ok());
        assert_eq!(result.unwrap().name, "mocha");
    }
}
