# Troubleshooting

## The screen looks garbled

Your terminal may not handle colours or box-drawing characters. Use a modern
terminal (kitty, Alacritty, WezTerm, iTerm2, Windows Terminal) and make sure `TERM` is
set to `xterm-256color`.

## "Unsupported file format"

datasight doesn't recognise the extension. Give it the separator and it reads the
file as delimited text: `datasight -d ',' export.dat`.

## Everything is in one column

The separator is wrong. Tell datasight which one it is: `-d ';'` for semicolons,
`-d '\t'` for tabs. Piped tab-separated data always needs `-d '\t'`.

## A date column stayed as text

Either some values don't match a [supported format](opening-data.md#dates), or every
value works as both day-first and month-first (`03/04/2024`), and datasight won't
guess which one you meant.

## `> 100` says it only works on numbers

`>`, `<`, `>=` and `<=` need a number column. For text, use `= value`, `!= value`, or
just type part of the value.

## A filter finds nothing, but the value is there

Filters are case-sensitive: `north` won't match `North`. Use `(?i)north`, or press
<kbd>u</kbd> and pick the value from the list.

## The unique values list seems short

It shows the 500 most common values. The title says `[top 500]` when there are more.

## Columns are too narrow or too wide

<kbd>=</kbd> fits every column. <kbd>_</kbd> fits the current one, and pressing it
again resets it. <kbd>-</kbd> and <kbd>+</kbd> adjust by hand. A column is never fitted
wider than the screen. For very long values, widen the terminal and fit again.

## Copy does nothing

Your terminal doesn't support clipboard access, or has it switched off. See
[Copy and export](copy-and-export.md#copy-to-the-clipboard).

## `az://` or `s3://` says it needs a feature

Downloaded binaries don't include cloud support. Build it in:
`cargo install datasight --features azure,aws`.

## `datasight ctl` says "no running datasight found"

Nothing is open to drive, or it's running as another user. Start datasight in another
terminal first. On Windows, `ctl` isn't available.

## A Parquet file won't open

Make sure it's a valid Parquet file. Encrypted Parquet isn't supported.

## Still stuck?

[Open an issue](https://github.com/SpollaL/datasight/issues) with your datasight
version (`datasight --version`), your OS and terminal, and a small file that shows the
problem.
