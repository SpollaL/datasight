# Themes

datasight comes with 9 colour themes:

| Name | Theme |
|---|---|
| `mocha` *(default)* | Catppuccin Mocha |
| `latte` | Catppuccin Latte (light) |
| `frappe` | Catppuccin Frappé |
| `macchiato` | Catppuccin Macchiato |
| `gruvbox-dark` | Gruvbox Dark |
| `nord` | Nord |
| `dracula` | Dracula |
| `solarized-dark` | Solarized Dark |
| `tokyo-night` | Tokyo Night |

## Try them live

Press <kbd>T</kbd>. Move with <kbd>j</kbd> / <kbd>k</kbd> and the screen changes as you
go. <kbd>Enter</kbd> keeps the theme for next time too. <kbd>Esc</kbd> puts back the
one you had.

## Set one from the command line

```bash
datasight --theme nord sales.csv
DATASIGHT_THEME=dracula datasight sales.csv
```

## Which one wins

If you set more than one, the first in this list is used:

1. `--theme`
2. `DATASIGHT_THEME`
3. The theme you last kept with <kbd>T</kbd>, saved in `~/.config/datasight/state.toml`
4. `mocha`
