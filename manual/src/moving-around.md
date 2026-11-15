# Moving around

## Getting around the table

| Key | Moves |
|---|---|
| <kbd>j</kbd> / <kbd>↓</kbd> | Down a row |
| <kbd>k</kbd> / <kbd>↑</kbd> | Up a row |
| <kbd>h</kbd> / <kbd>←</kbd> | Left a column |
| <kbd>l</kbd> / <kbd>→</kbd> | Right a column |
| <kbd>g</kbd> / <kbd>Home</kbd> | First row |
| <kbd>G</kbd> / <kbd>End</kbd> | Last row |
| <kbd>PageDown</kbd> / <kbd>PageUp</kbd> | 20 rows at a time |

Most keys act on the **current column**, the one under the cursor. Searching,
filtering, sorting and the popups below all use it.

## Column widths

| Key | Does |
|---|---|
| <kbd>=</kbd> | Fit every column to its content |
| <kbd>_</kbd> | Fit the current column. Press again to put it back |
| <kbd>-</kbd> / <kbd>+</kbd> | Make the current column narrower / wider |

A column never grows wider than the screen, so it can't push everything else
off to the side.

## A quick look at a column

<kbd>e</kbd> opens a small box with the current column's count, min, max, mean and
median. Press <kbd>e</kbd> again to close it.

## Every column at once

<kbd>i</kbd> opens the **column inspector**: one line per column with its type, how many
values are filled in, how many are empty, how many are distinct, and min, max, mean
and median. Move with <kbd>j</kbd> / <kbd>k</kbd>, press <kbd>Enter</kbd> to jump to
that column in the table, or <kbd>Esc</kbd> to go back. In a wide file, press
<kbd>/</kbd> and type part of a column's name to find it.

It's the fastest way to get to know a file you've never seen.

## Help

<kbd>?</kbd> shows every key without leaving the app. <kbd>q</kbd> quits.

Empty cells show as `∅`.
