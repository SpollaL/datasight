# datasight — a fast terminal viewer for CSV, Parquet, and JSON

**A vim-keybinded TUI for exploring tabular data from the command line.** Browse, filter, sort, group, and plot CSV, TSV, Parquet, JSON, and NDJSON files directly in your terminal — no spreadsheet, no notebook, no web UI required. Built in Rust on [ratatui](https://ratatui.rs), with 9 built-in [Base16](https://github.com/chriskempson/base16) color themes.

[![CI](https://github.com/SpollaL/datasight/actions/workflows/ci.yml/badge.svg)](https://github.com/SpollaL/datasight/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

![demo](.github/assets/demo.gif)

**[Read the manual →](https://spollal.github.io/datasight/)**

## Highlights

- Search, filter (`> 500`, `= North`, `!= (null)`), multi-column sort and group-by, all from the keyboard
- Charts in the terminal: line, bar and histogram, several series at once
- Reads files or pipes: `curl … | datasight`, with the format detected for you
- A file browser for local folders, Azure Blob Storage and S3
- Save exactly what you see to CSV, or copy a cell or row to the clipboard, even over SSH
- An agent or a script in another terminal can read and drive the table you have open (`datasight ctl`), with a ready-made Claude Code skill (`--skill`)

## Install

Download a binary from [GitHub Releases](https://github.com/SpollaL/datasight/releases), or build it with Rust 1.75+:

```bash
cargo install datasight                     # add --features azure,aws for cloud storage
```

Then:

```bash
datasight orders.csv                        # press ? for every key
```

The [manual](https://spollal.github.io/datasight/) covers every feature, the full
[cheat sheet](https://spollal.github.io/datasight/cheat-sheet.html), and
[troubleshooting](https://spollal.github.io/datasight/troubleshooting.html).
