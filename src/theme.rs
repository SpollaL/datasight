use ratatui::style::Color;

pub struct Base16Scheme {
    pub name: &'static str,
    pub display_name: &'static str,
    pub base: [[u8; 3]; 16],
}

pub struct Theme {
    pub name: &'static str,
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
}
