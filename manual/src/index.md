# Welcome

datasight lets you look at a data file without leaving the terminal. Point it at a
CSV, TSV, Parquet, JSON or NDJSON file and you get a fast, scrollable table: search it,
filter it, sort it, group it, chart it, and save what you end up with.

![datasight in action](https://raw.githubusercontent.com/SpollaL/datasight/main/.github/assets/demo.gif)

It's for the moment when you want to *see* the data. Opening a spreadsheet or
starting a notebook is overkill for that.

## Try it in 30 seconds

```bash
datasight orders.csv
```

Then:

- <kbd>j</kbd> <kbd>k</kbd> <kbd>h</kbd> <kbd>l</kbd> move around, the same keys as vim.
- <kbd>/</kbd> searches the current column.
- <kbd>f</kbd> filters it, so typing `> 100` keeps the rows over 100.
- <kbd>?</kbd> shows every key.
- <kbd>q</kbd> quits.

That covers the basics. Each chapter in the sidebar covers one thing you can do, and
the [cheat sheet](cheat-sheet.md) lists every key on one page.

## More

- [Changelog](https://github.com/SpollaL/datasight/blob/main/CHANGELOG.md): what's new in each release
- [Contributing](https://github.com/SpollaL/datasight/blob/main/CONTRIBUTING.md): building from source and sending a fix
- [Roadmap](https://github.com/SpollaL/datasight/blob/main/ROADMAP.md): what's planned
- [Issues](https://github.com/SpollaL/datasight/issues): report a bug or ask for a feature
