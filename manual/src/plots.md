# Plots

datasight draws charts right in the terminal. A chart takes two choices: what goes
**up the side** (Y) and what goes **along the bottom** (X).

## Make a chart

1. Go to the column you want to chart and press <kbd>p</kbd>. It's already picked as
   a Y column.
2. To compare, move with <kbd>h</kbd> / <kbd>l</kbd> and press <kbd>Space</kbd> on
   each extra column. <kbd>Space</kbd> on a picked column un-picks it.
3. Press <kbd>Enter</kbd>, move to the X column (a date, say), and press
   <kbd>Enter</kbd> again.

There's a shortcut for step 3: press <kbd>i</kbd> instead of <kbd>Enter</kbd> to plot
against the row number and skip choosing an X.

<kbd>Esc</kbd> backs out one step. Once the chart is up, <kbd>Esc</kbd> or <kbd>p</kbd>
closes it.

## Chart types

<kbd>t</kbd> switches the chart type:

- **One Y column:** line → bar → histogram.
- **Several Y columns:** line ↔ bar.

A histogram shows how a single column's values are spread out, so it doesn't use an
X column.

## Two charts, one above the other

When two series have very different scales (revenue in millions, order count in
hundreds), one shared chart squashes the smaller one flat. While choosing Y columns,
press <kbd>2</kbd> instead of <kbd>Space</kbd> on a column to put it in a second
chart below the first. Both charts share the same X. <kbd>2</kbd> won't move the top
chart's last column, and <kbd>Enter</kbd> needs at least one column in the top chart.

## X columns that aren't numbers

Numbers along the bottom are plotted to scale. Text and dates are placed evenly, in
row order, and their values are printed sideways under the chart. When there are too
many to fit, an even sample is shown. Sort by the date column first if you want the
line to read left to right in time.
