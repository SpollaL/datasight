# Sort and group

## Sort

Press <kbd>s</kbd> on a column to sort by it. Press again to reverse, and a third
time to turn it off: ▲ → ▼ → off.

To sort by more than one column, press <kbd>s</kbd> on another column. It becomes the
tie-breaker. Sorting by `region` and then `amount` puts the regions in order and,
within each region, the amounts in order. The header shows the order with `①▲` and
`②▼`.

<kbd>S</kbd> clears every sort.

## Group

Grouping turns rows into a summary, like a pivot table. To get total sales per
region:

1. Go to `region` and press <kbd>b</kbd>. It's now a **group key**.
2. Go to `amount` and press <kbd>a</kbd> until it shows **Σ** (sum).
3. Press <kbd>B</kbd>.

You get one row per region, with a column called `amount_sum`.

<kbd>a</kbd> cycles through these:

| Symbol | Means |
|---|---|
| Σ | Sum |
| μ | Average |
| # | Count |
| ↓ | Smallest |
| ↑ | Largest |

You can group by several keys, and summarise several columns at once. Columns that
are neither a key nor summarised are left out of the result.

Filters and sorts still apply once the rows are grouped. Press <kbd>B</kbd> again to
go back to the full table.
