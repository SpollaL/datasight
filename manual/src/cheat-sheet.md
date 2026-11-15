# Cheat sheet

Every key on one page. Press <kbd>?</kbd> inside datasight for the same list.

## Table

| Key | Does |
|---|---|
| <kbd>j</kbd> <kbd>k</kbd> <kbd>h</kbd> <kbd>l</kbd> / arrows | Move |
| <kbd>g</kbd> / <kbd>G</kbd> | First / last row |
| <kbd>PageDown</kbd> / <kbd>PageUp</kbd> | 20 rows down / up |
| <kbd>/</kbd> | Search the current column |
| <kbd>n</kbd> / <kbd>N</kbd> | Next / previous match |
| <kbd>f</kbd> | Filter the current column |
| <kbd>F</kbd> | Clear all filters |
| <kbd>u</kbd> | Unique values, then <kbd>Enter</kbd> to filter |
| <kbd>s</kbd> | Sort: ▲ → ▼ → off. On another column: add a tie-breaker |
| <kbd>S</kbd> | Clear all sorts |
| <kbd>b</kbd> | Make the column a group key |
| <kbd>a</kbd> | Summarise the column: Σ μ # ↓ ↑ |
| <kbd>B</kbd> | Group / ungroup |
| <kbd>p</kbd> | Start a chart |
| <kbd>e</kbd> | Column stats |
| <kbd>i</kbd> | Column inspector |
| <kbd>=</kbd> | Fit all columns |
| <kbd>_</kbd> | Fit this column (again: reset) |
| <kbd>-</kbd> / <kbd>+</kbd> | Narrower / wider |
| <kbd>y</kbd> / <kbd>Y</kbd> | Copy cell / row |
| <kbd>w</kbd> | Save the view as CSV |
| <kbd>T</kbd> | Change theme |
| <kbd>?</kbd> | Help |
| <kbd>q</kbd> | Quit |

## Unique values and column inspector

| Key | Does |
|---|---|
| <kbd>j</kbd> / <kbd>k</kbd>, <kbd>g</kbd> / <kbd>G</kbd> | Move |
| <kbd>/</kbd> | Type to narrow the list (<kbd>Esc</kbd> stops typing) |
| <kbd>Enter</kbd> | Filter to the value / jump to the column |
| <kbd>Esc</kbd> | Close |

## Charts

| Key | Does |
|---|---|
| <kbd>h</kbd> / <kbd>l</kbd> | Move between columns |
| <kbd>Space</kbd> | Add or remove a Y column |
| <kbd>2</kbd> | Add or remove a Y column in a second chart |
| <kbd>Enter</kbd> | Done with Y, then pick X / Done with X, show the chart |
| <kbd>i</kbd> | Plot against row number (no X) |
| <kbd>t</kbd> | Change chart type |
| <kbd>Esc</kbd> | Back one step / close the chart |

## File browser

| Key | Does |
|---|---|
| <kbd>j</kbd> / <kbd>k</kbd> | Move |
| <kbd>Enter</kbd> / <kbd>.</kbd> | Open file or folder |
| <kbd>Esc</kbd> | Parent folder |
| <kbd>Tab</kbd> | Switch between list and table |
| <kbd>ctrl</kbd>+<kbd>e</kbd> | Hide / show the list |
| <kbd>/</kbd> | Find a file by name |
| <kbd>d</kbd> | Download a cloud file |
| <kbd>q</kbd> | Quit |

## Text viewer

| Key | Does |
|---|---|
| <kbd>j</kbd> / <kbd>k</kbd> | Scroll |
| <kbd>g</kbd><kbd>g</kbd> / <kbd>G</kbd> | Top / bottom |
| <kbd>w</kbd> | Wrap lines |
| <kbd>L</kbd> | Line numbers |
| <kbd>/</kbd>, <kbd>n</kbd> / <kbd>N</kbd> | Search |

## Command line

```text
datasight [FILE]                 open a file, or read from a pipe
datasight -d ';' FILE            use another separator
datasight --theme nord FILE      pick a theme
datasight browse [PATH]          file browser: a folder, az://… or s3://…
datasight ctl METHOD [JSON]      drive a running datasight (see Agents and scripts)
datasight --skill                print the Claude Code skill
datasight --version
```
