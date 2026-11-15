# Finding rows

There are three ways to find rows. **Search** jumps between matches and keeps every row.
**Filter** hides the rows that don't match. **Unique values** lets you pick a value
from a list.

## Search: jump to a match

Press <kbd>/</kbd>, type, and press <kbd>Enter</kbd> to jump to the first row in the
current column that contains it. Case doesn't matter. <kbd>n</kbd> goes to the next
match and <kbd>N</kbd> to the previous one. <kbd>Esc</kbd> clears the search.

## Filter: keep only matching rows

Press <kbd>f</kbd> on a column and type a condition. The table updates as you type.
<kbd>Enter</kbd> keeps the filter and <kbd>Esc</kbd> throws it away.

| You type | Keeps rows where the column… |
|---|---|
| `> 500` | is over 500 (also `<`, `>=`, `<=`) |
| `= 500` | is exactly 500 |
| `= North` | is exactly `North` |
| `!= North` | is anything but `North` |
| `= (null)` | is empty |
| `!= (null)` | is not empty |
| `Lap` | contains `Lap` somewhere |

Some details:

- `>`, `<`, `>=` and `<=` only work on number columns.
- Plain text is **case-sensitive**: `lap` won't find `Laptop`. Put `(?i)` in front to
  ignore case: `(?i)lap`.
- Plain text is actually a regular expression, so `^North` means "starts with North".
  A `.` matches any character, which is why `3.5` also finds `305`.

Filters add up. Filter `region` to `= North`, then move to `amount` and filter
`> 500`, and you see northern orders over 500. <kbd>F</kbd> removes every filter.

## Unique values: pick from a list

<kbd>u</kbd> lists every distinct value in the current column, the most common first,
with a count next to each. Move with <kbd>j</kbd> / <kbd>k</kbd> and press
<kbd>Enter</kbd> to filter the table to that value. <kbd>Esc</kbd> closes the list.

Long list? Press <kbd>/</kbd> and type to narrow it, then <kbd>↑</kbd> / <kbd>↓</kbd>
and <kbd>Enter</kbd> as before. <kbd>Esc</kbd> stops typing.

It's the easy way to filter when you don't know exactly how a value is spelled.
Columns with more than 500 distinct values show the top 500, and the title says
`[top 500]`.
