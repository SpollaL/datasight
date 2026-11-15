# Copy and export

## Copy to the clipboard

| Key | Copies |
|---|---|
| <kbd>y</kbd> | The cell under the cursor |
| <kbd>Y</kbd> | The whole row, tab-separated, so it pastes straight into a spreadsheet |

Your terminal does the copying, so it works over SSH too. It works in Windows
Terminal, WezTerm, kitty, iTerm2, Alacritty, Ghostty and Konsole. A few need a
setting turned on:

- **GNOME Terminal** has it switched off by default.
- **tmux** needs `set -g set-clipboard on` in your config.

If your terminal doesn't support it, nothing happens. There's no error.

## Save the view as CSV

Press <kbd>w</kbd>. You'll be asked for a file name, already filled in for you:
`orders.csv` becomes `orders.export.csv`. Edit it if you like, then press
<kbd>Enter</kbd>. <kbd>Esc</kbd> cancels.

**You get what you see.** The file has your filters, sorts and grouping applied, not
the original data.

Good to know:

- The file is saved in the current directory unless you type a path. `~/exports/`
  and `/tmp/x.csv` both work.
- `.csv` is added if you leave it off.
- Folders that don't exist yet are created.
- If the file already exists you get a warning first. A failed save never damages
  the file it was going to replace.
- Empty cells are saved as empty fields, not `∅`.
- Saving always goes to your machine. When you're viewing a file from Azure or S3,
  the name is suggested locally, and typing an `az://` or `s3://` destination is
  refused.
