# datasight — a fast terminal viewer for CSV, Parquet, and JSON

**A vim-keybinded TUI for exploring tabular data from the command line.** Browse, filter, sort, group, and plot CSV, TSV, Parquet, JSON, and NDJSON files directly in your terminal — no spreadsheet, no notebook, no web UI required. Built in Rust on [ratatui](https://ratatui.rs), with 9 built-in [Base16](https://github.com/chriskempson/base16) color themes.

[![CI](https://github.com/SpollaL/datasight/actions/workflows/ci.yml/badge.svg)](https://github.com/SpollaL/datasight/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

![demo](.github/assets/demo.gif)

## Features

- Vim-style navigation (`hjkl`, `g`/`G`, `PageUp`/`PageDown`)
- Search within a column (`/`, `n`/`N`)
- Multi-column filtering with comparison operators — `> 30`, `= Engineering`, `!= 0` (`f`, `F`)
- Unique values popup — browse and filter by distinct values instantly (`u`)
- Hierarchical multi-column sort — `s` cycles ascending → descending → off, additional columns append as secondary priorities (`s`, `S` to clear)
- Group-by with per-column aggregations; filters and sorts survive the aggregation switch (`b`, `a`, `B`)
- Multi-series plots — select multiple Y columns with `Space` for side-by-side comparison, or a single Y for line/bar/histogram (`p`, `t`)
- Column Inspector — schema and stats for every column at a glance (`i`)
- Column stats popup (`e`)
- Export the current view to CSV — filters, sorts and group-by included (`w`)
- Automatic date detection for ISO (`YYYY-MM-DD`) and common non-ISO formats (`MM/DD/YYYY`, `DD-Mon-YYYY`), with an ambiguity guard for slash-dates
- In-app help popup (`?`)
- 9 built-in Base16 themes — `--theme nord`, `gruvbox-dark`, `dracula`, etc., switchable live with `T` and persisted across runs
- Supports CSV, TSV, Parquet, JSON (`[{...}]`), and NDJSON/JSON Lines (`.ndjson`, `.jsonl`) files
- Custom delimiter support via `-d` flag — works with pipe-separated, semicolon-separated, and any single-character delimiter
- Pipe-friendly — reads from stdin with automatic format detection (CSV, JSON, NDJSON)
- Viewport-windowed rendering — stays fast on large files
- File browser (`browse`) — navigate local directories and Azure Blob / S3 buckets, open files directly into the viewer, download cloud objects locally (`d`)

## Install

### Pre-built binaries (recommended)

Download the latest binary for your platform from the [GitHub Releases](https://github.com/SpollaL/datasight/releases) page.

### Build from source

Requires Rust 1.75 or higher.

```
cargo install datasight
```

Or clone and run locally:

```
cargo run -- tests/fixtures/orders.csv
cargo run -- tests/fixtures/orders.tsv
cargo run -- tests/fixtures/orders.parquet
cargo run -- tests/fixtures/orders.json
cargo run -- tests/fixtures/orders.ndjson
cargo run -- -d '|' <path-to-file.psv>   # pipe-separated
cargo run -- -d ';' <path-to-file.csv>   # semicolon-separated
```

Sample fixtures for all supported formats are in `tests/fixtures/`.

### Pipe from stdin

datasight reads from stdin when no file is given. Format is detected automatically from content:

```bash
# CSV
cat data.csv | datasight

# TSV
cat data.tsv | datasight

# Pipe-separated via flag
cat data.psv | datasight -d '|'

# JSON array of objects
curl https://api.example.com/records | datasight

# Newline-delimited JSON
kubectl get pods -o jsonlines | datasight

# From a database
psql -c "\copy (SELECT * FROM orders) TO STDOUT CSV HEADER" | datasight
```

## File Browser

Browse local directories or cloud storage and open files directly into the viewer:

```bash
datasight browse .                          # current directory
datasight browse /data/exports              # local path
datasight browse az://my-container         # Azure Blob Storage
datasight browse s3://my-bucket            # AWS S3
```

Cloud backends require building with the relevant feature:

```bash
cargo install --path . --features azure     # Azure support
cargo install --path . --features aws       # S3 support
```

Azure reads `AZURE_STORAGE_CONNECTION_STRING` or individual `AZURE_STORAGE_ACCOUNT_NAME` / `AZURE_STORAGE_ACCOUNT_KEY` env vars.

### Browse keybindings

| Key | Action |
|-----|--------|
| `j` / `k` | Navigate list |
| `. / Enter` | Open file or enter directory |
| `Esc` | Go up to parent |
| `Tab` | Switch focus browser ↔ viewer |
| `ctrl-e` | Toggle browser sidebar |
| `d` | Download the selected cloud file locally (`az://` / `s3://` only) |
| `T` | Open theme picker |
| `q` | Quit (when no file is open) |

`d` prompts for a destination, pre-filled with the remote file name. `~` expands to
your home directory, and a destination ending in `/` (or an existing directory) keeps
the remote name — so `~/Downloads/` saves `sales.csv` as `~/Downloads/sales.csv`. The
prompt shows the resolved path and warns before overwriting an existing file; `Enter`
saves, `Esc` cancels. Missing directories in the destination are created for you.

## Themes

datasight ships with 9 Base16 color themes:

| Name | Display |
|---|---|
| `mocha` *(default)* | Catppuccin Mocha |
| `latte` | Catppuccin Latte |
| `frappe` | Catppuccin Frappé |
| `macchiato` | Catppuccin Macchiato |
| `gruvbox-dark` | Gruvbox Dark |
| `nord` | Nord |
| `dracula` | Dracula |
| `solarized-dark` | Solarized Dark |
| `tokyo-night` | Tokyo Night |

Pick one at startup:

```
datasight --theme nord file.csv
DATASIGHT_THEME=dracula datasight file.csv
```

Or switch live: press **`T`** to open the theme picker, `j`/`k` to browse with
live preview, `Enter` to confirm (saves to `~/.config/datasight/state.toml`),
`Esc` to revert.

Precedence: `--theme` > `DATASIGHT_THEME` > saved state file > `mocha` (default).

## Keybindings

### Navigation

| Key | Action |
|-----|--------|
| `j` / `Down` | Move down |
| `k` / `Up` | Move up |
| `h` / `Left` | Move left |
| `l` / `Right` | Move right |
| `g` / `Home` | Jump to first row |
| `G` / `End` | Jump to last row |
| `PageDown` | Scroll down 20 rows |
| `PageUp` | Scroll up 20 rows |

### Search

| Key | Action |
|-----|--------|
| `/` | Enter search mode (searches in current column) |
| `Enter` | Confirm search and jump to first match |
| `n` | Next match |
| `N` | Previous match |
| `Esc` | Exit search and clear results |

### Filter

| Key | Action |
|-----|--------|
| `f` | Enter filter mode (filters rows by current column) |
| `Enter` | Confirm filter and return to normal mode |
| `F` | Clear all filters |
| `Esc` | Discard input |

Supports comparison operators for numeric columns: `> 30`, `< 100`, `>= 0`, `<= 50`, `= 42`, `!= 0`.
Use `= text` or `!= text` for exact string matching. Plain text falls back to substring search.

### Unique Values

| Key | Action |
|-----|--------|
| `u` | Open unique values popup for current column (sorted by frequency) |
| type | Search / filter the list live |
| `j` / `k` | Navigate the list |
| `Enter` | Apply selected value as a filter and close |
| `Esc` | Close without filtering |

### Sort

| Key | Action |
|-----|--------|
| `s` | Cycle current column: ascending → descending → off |
| `s` on a new column | Append as next sort priority (primary stays in place) |
| `S` | Clear all sorts |

Active sorts are shown in the header with `①▲` / `②▼` glyphs and summarised in the status bar.

### Group By

| Key | Action |
|-----|--------|
| `b` | Toggle group-by key for current column |
| `a` | Cycle aggregation for current column (Σ μ # ↓ ↑) |
| `B` | Execute group-by / clear and return to full view |

### Plot

| Key | Context | Action |
|-----|---------|--------|
| `p` | Normal | Enter pick-Y mode |
| `h` / `←` / `l` / `→` | Pick-Y / Pick-X | Move between columns |
| `Space` | Pick-Y | Toggle the current column as a Y series (select one or many) |
| `Enter` | Pick-Y | Confirm Y selection and advance to pick-X |
| `i` | Pick-Y | Use row index as X and render the chart immediately (skips pick-X) |
| `Enter` | Pick-X | Confirm X column and render the chart |
| `Esc` | Pick-Y / Pick-X | Cancel (pick-X goes back to pick-Y; pick-Y returns to normal) |
| `t` | Plot | Cycle chart type (line → bar → histogram for single-Y; line ↔ bar for multi-Y) |
| `Esc` / `p` | Plot | Close the chart and return to normal mode |
| `q` | Plot | Quit |

Selecting multiple Y columns draws them as overlaid series with a shared X axis — great for comparing `actual` vs `fit` vs `residual` over a date column.

Numeric X columns are plotted directly. String or date X columns use row indices as data points and render the actual values as rotated (vertical) labels below the chart — all labels are shown when they fit, otherwise they are sampled evenly.

For histogram, the Y column is binned automatically — no X column selection needed.

### Column Inspector

| Key | Action |
|-----|--------|
| `i` | Open Column Inspector (type, count, nulls, unique, min, max, mean, median) |
| `j` / `k` | Navigate rows |
| `Enter` | Jump to the selected column and return to data view |
| `Esc` / `i` | Close and return to data view |

### Column Stats

| Key | Action |
|-----|--------|
| `e` | Toggle stats popup for current column (count, min, max, mean, median) |

### Copy

| Key | Action |
|-----|--------|
| `y` | Copy the selected cell to the clipboard |
| `Y` | Copy the current row to the clipboard, tab-separated |

Copying uses the OSC 52 escape sequence, so the terminal performs the copy — which means
it also works over SSH. Supported by Windows Terminal, WezTerm, kitty, iTerm2, Alacritty,
Ghostty and Konsole. GNOME Terminal disables OSC 52 by default, and inside tmux you need
`set-clipboard on`. Where it is unsupported the sequence is ignored silently.

### Export

| Key | Action |
|-----|--------|
| `w` | Write the current view to a CSV file |
| `Enter` | Confirm and write |
| `Esc` | Cancel without writing |

The prompt opens prefilled with a name derived from the source (`orders.csv` →
`orders.export.csv`; piped input becomes `export.csv`) and writes to the current
directory unless you type a path. Absolute paths, relative paths and a leading `~` all
work, and `.csv` is appended when it is missing so the extension always matches the
contents. The status line reports the resolved path, so you can see where the file
actually went.

The file contains the rows exactly as filtered, sorted and grouped on screen — not the
original file. Null values are written as empty fields: the `∅` shown on screen and the
`(null)` filter sentinel are display conventions and never reach the file.

A warning appears in the prompt before an existing file is overwritten, and the write
goes to a temporary file that is renamed into place only on success — so a failed export
(a column CSV cannot represent, a full disk) leaves the existing file untouched.
Directories are never created for you: exporting into a path whose parent does not exist
fails without writing anything.

Exports are always local. Viewing a file from `browse az://…` or `s3://…` suggests a local
name from the basename (`az://container/data/orders.parquet` → `orders.export.csv`) and
writes to the current directory — the rows are already in memory, so nothing goes back over
the network. Typing a remote destination is flagged in the prompt and refused before any
file is touched; writing back to object storage is not supported.

### Other

| Key | Action |
|-----|--------|
| `i` | Open Column Inspector |
| `_` | Autofit current column width (press again to reset) |
| `=` | Autofit all columns |
| `-` | Narrow current column |
| `+` | Widen current column |
| `e` | Toggle column stats popup |
| `?` | Toggle help popup |
| `T` | Open theme picker |
| `q` | Quit |

## Troubleshooting

**The display looks garbled or misaligned**

Your terminal may not support 256 colors or Unicode box-drawing characters. Try a modern terminal emulator (kitty, alacritty, iTerm2, Windows Terminal) and make sure `TERM` is set to `xterm-256color`.

**Columns are too narrow or too wide**

Press `=` to autofit all columns to their content, or `_` to autofit only the current
column; press `_` again to return that column to the default width.

Autofit grows a column to fit its longest value, capped at the width of the table pane —
a column wider than the pane cannot be scrolled into, so fitting beyond that point would
only push other columns off-screen. For values longer than your terminal is wide, widen
the terminal and autofit again, or use `-` and `+` to size the column by hand.

**Filtering with `>`, `<`, `>=`, `<=` shows an error**

These operators only work on numeric columns. Use `= value` or `!= value` for exact string matching, or plain text for substring search.

**The unique values popup shows fewer results than expected**

The popup is capped at the 500 most frequent values. If your column has more than 500 distinct values, the title will say `[top 500]`.

**Export fails with "No such file or directory"**

The parent directory has to exist already — export never creates directories. Check the
path you typed, or create the directory first.

**Large files are slow to open**

datasight reads the entire file into memory on startup using Polars. In practice, load times are fast: a 42 MB / 1M-row CSV loads in under 0.1s using ~115 MB RAM. Multi-GB files will use proportionally more memory. Once loaded, navigation and filtering are fast regardless of row count.

**Parquet file fails to open**

Make sure the file is a valid Parquet file. Compressed or encrypted Parquet variants are not supported.
