# Roadmap

Suggestions captured from code review. Each item lists the type
(bug / feature / refactor / perf), the affected files, and a sketch of the fix.

---

## 1. Bug — substring filter treats input as a regex

**Type:** bug
**Files:** `src/app.rs:292`, `src/app.rs:323` (and test coverage in `src/app_tests.rs`)

`FilterQuery::build_expr` and `build_committed_filter_expr` use
`str().contains(pat, false)`. With the `regex` feature enabled in `Cargo.toml`,
this is regex-based. The `strict: false` flag only swallows invalid-regex
errors — it does not switch to literal matching.

Symptoms a user hits today:

- `.` matches every non-null row.
- `a.b` matches `axb`, `a-b`, `a1b`.
- `a|b` is regex alternation, not a literal pipe.
- `[abc` is invalid regex and silently returns zero matches.
- Filter is case-sensitive, while `/`-search is case-insensitive — inconsistent.

**Fix sketch:** use `contains_literal` for the substring fallback and lowercase
both sides for case-insensitive matching. Optionally introduce a `re:` prefix
to opt into regex explicitly (visidata-style).

**Tests to add:** filter on `.`, `a|b`, `[abc`, mixed-case substrings —
asserting the literal-match semantics. Update `qa.sh` if any new key is added.

---

## 2. Bug — `eprintln!` corrupts the alternate screen

**Type:** bug
**Files:** `src/events.rs:127`, and the silent-failure path in
`src/app.rs:781-785`

The theme-picker Enter handler does `eprintln!("warning: could not save theme to {:?}: {}", path, e);`
while ratatui owns the terminal in alternate-screen mode. The line either
corrupts the next frame or is lost entirely when the alt-screen tears down on
exit — the user never sees it.

**Fix sketch:** add a transient status channel to `App` (mirror
`BrowserApp.status: Option<String>`), render it in the status bar for one
frame, and route warnings through it:

```rust
pub struct App {
    ...
    pub transient_message: Option<String>,
}

if let Err(e) = crate::theme::write_state_theme_at(&path, app.theme.name) {
    app.transient_message = Some(format!("Could not save theme: {}", e));
}
```

Clear it on the next keystroke (or on the next event loop tick if a redraw
budget is preferred).

**Bonus targets for the same channel:**
- `App::apply_groupby` (`src/app.rs:781-785`) swallows aggregation errors and
  silently returns — the user gets no feedback when `B` fails because of an
  incompatible agg/key combo.
- Anywhere else that currently has no UI affordance for "this didn't work".

---

## 3. Feature — hide and pin columns

**Type:** feature
**Files:** `src/app.rs` (state + ops), `src/ui.rs` (visible-column derivation,
render order), `src/events.rs` (key bindings), `src/app_tests.rs`, `qa.sh`,
README keybinding table

Wide datasets (the repo already ships `tests/fixtures/wide.csv` with 200
columns) currently only have horizontal scrolling. visidata users expect to
hide noise columns and pin key columns to the left as a frozen pane.

**Proposed keys (visidata-compatible):**

| Key | Action |
|---|---|
| `-` | Hide current column |
| `+` | Unhide all columns |
| `[` | Pin current column to the left |
| `]` | Unpin current column |

**Implementation sketch:**

```rust
pub struct App {
    ...
    pub hidden_cols: HashSet<usize>,
    pub pinned_cols: Vec<usize>,  // ordered: pin order is display order
}
```

The viewport derives a `Vec<usize>` of visible column indices each frame:
pinned columns first (in pin order), then the windowed slice of the
remaining-and-not-hidden columns starting at `viewport.col`. Pinned cells
consume from `available_w` before `count_visible_from` runs on the
scrollable slice.

**Edge cases to respect:**

- `apply_groupby` rebuilds `headers` — clear hidden/pinned sets and restore
  them on `clear_groupby`, mirroring `column_widths` save/restore at
  `app.rs:774-775` / `app.rs:971-972`.
- Filter / sort / groupby reference columns by *name*, so they're unaffected.
- Column Inspector (`i`) should mark hidden columns and let `Enter` unhide
  and jump.
- Don't persist to `state.toml` — visidata behaviour is session-local.

**Tests + QA:** add cases for `-`, `+`, `[`, `]` against `wide.csv` and a
normal-width file in `qa.sh`; unit tests for the visible-column derivation
helper.

---

## 4. Perf — replace hand-rolled unique-value counter with `Series::value_counts`

**Type:** perf
**Files:** `src/app.rs:813-852` (`build_unique_values`); related cleanup in
`src/app.rs:367-396` (`update_search`)

`build_unique_values` runs on every `u` keypress. It casts the full column to
`String`, allocates a `HashMap<Option<String>, usize>` with one `String` key
per distinct value, builds a `Vec`, sorts the whole thing, then truncates to
`MAX_UNIQUE` (500). On a 1M-row column with ~100K uniques that's roughly 1M
`String` allocations + 100K hashmap keys to keep 500 rows.

**Fix sketch:**

```rust
let counts = series
    .value_counts(true /*sort*/, true /*parallel*/, "count".into(), false)?
    .head(Some(config::MAX_UNIQUE + 1));
// cast only the truncated value column to String for display
```

The result is already sorted by count desc; nulls survive naturally and map
to `"(null)"` at the display layer only.

**Caveats:** some dtypes (Decimal, exotic Categorical variants) may need a
pre-cast — fall back to the existing hand-rolled path on `value_counts`
error.

**Related cleanup (same file, `update_search`):** the search loop also does a
full column cast + per-row `to_lowercase().contains()` in Rust on every
keystroke. Could be a single lazy expression returning row indices; benefit
is smaller (only one column is scanned and only when typing), but the
symmetry is nice and it opens the door to `re:` regex search if desired.

---

## 5. Feature — export the current view (`w` write)

**Type:** feature
**Files:** `src/app.rs` (new `Mode::Write` + write helper), `src/events.rs`
(dispatch), `src/ui.rs` (modal), `qa.sh`, README

datasight is read-only today. A user who filters/sorts/groups to the rows
they want has no way to get them out except re-running the pipeline
elsewhere. `w` would close the explore → act loop visidata users expect.

**UX:** `w` in Normal mode opens a small modal — `Tab` cycles format
(`csv` / `tsv` / `parquet` / `json` / `ndjson`), `Enter` writes `app.view`
(post filter/sort/groupby) to the path, `Esc` cancels. Default path:
`${original_basename}.filtered.${ext}` next to the source; for stdin input,
`./datasight-export.${ext}`.

**Writer:** all formats are already enabled in `Cargo.toml` — `CsvWriter`,
`ParquetWriter`, `JsonWriter` against `app.view.clone()`. No new deps.

**Safety:**

- Refuse to write to `app.file_path` (don't corrupt the source).
- Confirm-on-overwrite: first `Enter` on an existing path shows
  `"file exists — Enter again to overwrite"`; second `Enter` proceeds.
- Cloud paths (`az://`, `s3://`) out of scope for v1 — error early.
- Expand `~` via `dirs::home_dir()`.

**Status feedback:** route success/error through the `transient_message`
channel proposed in suggestion #2 (`"Wrote 1,237 rows to foo.filtered.csv"`).

**Tests:** round-trip each format (load fixture → filter → write to tempdir
→ reload → equality assert). `tempfile` is already a dev-dep. Add a `qa.sh`
case.

**Future hook:** once `w` exists, `y` (yank cell to clipboard) via `arboard`
is the natural follow-up.

---

## 6. Refactor — collapse popup/picker booleans into `Mode`

**Type:** refactor
**Files:** `src/app.rs` (state + `is_typing`), `src/events.rs` (whole
dispatch), `src/ui.rs` (mode branches)

Today there are five sources of truth for "what input state is active":
`mode`, `show_help`, `show_stats`, `unique_values.searching`,
`columns_view.searching`, `picker: Option<_>`. Symptoms:

- `events.rs:35` uses match guards (`Mode::Normal if app.show_help`),
  which aren't exhaustive — the compiler can't catch a missed combination.
- `App::is_typing()` at `app.rs:948` checks four fields by hand; adding a
  new typing mode (e.g. `Mode::Write` from #5) requires remembering to
  update this function — no compiler push.
- `Mode::ThemePicker` is gated by `app.picker.as_mut()` even though the
  invariant "ThemePicker mode ⇒ picker is Some" should be type-enforced.

**Proposed shape (either works):**

```rust
// option A — overlays as their own field, dispatched first
pub overlay: Option<Overlay>,
pub mode: Mode,

// option B — overlays as a Mode variant carrying the underlying mode
Mode::Overlay(Overlay, Box<Mode>),
```

Either way:

- `Mode::Inspector { searching: bool }` / `Mode::UniqueValues { searching: bool }`
  fold the per-state `searching` flag in.
- `Mode::ThemePicker(ThemePicker)` owns its data — drop the separate
  `picker` field.
- `Mode::Plot(PlotStage)` (PickY / PickX / Render) collapses the three
  current Plot* variants.

**Win:** `is_typing` becomes a single `matches!` against `mode`, dispatch
gets compiler-enforced exhaustiveness, defensive `if let Some(picker) = ...`
goes away.

**Order:** land *after* the new `Mode::Write` (suggestion #5) so the new
mode arrives into the cleaner shape rather than perpetuating the current
pattern. ~200-line mechanical diff, no behaviour change.

---

## 7. Perf — autofit scans entire column to find a max that's capped at 40

**Type:** perf
**Files:** `src/app.rs:576-596` (`compute_column_width`),
`src/app.rs:649-659` (`autofit_*` callers)

`compute_column_width` casts the whole column to `String`, iterates every
row in Rust, runs `.chars().count()` per row (Unicode-aware, slow), takes
max, then clamps to `MAX_COLUMN_WIDTH = 40` (see `config.rs:8`). The first
row ≥ 40 chars already settles the answer — everything after is wasted.

For a 1M-row × 50-col dataset, `=` (autofit all) triggers ~50M `String`
allocations + 50M Unicode iterations. Felt as a multi-second freeze.

**Two compounding wins:**

1. **Early exit at the cap** — manual loop that bails the moment running
   max reaches `MAX_COLUMN_WIDTH`. Usually a few hundred rows, not millions.
2. **Push to Polars** — `Utf8Chunked::str_len_chars()` (verify exact method
   name in 0.46) is a single SIMD-friendly pass with no per-row Rust
   allocation. Combines well with #1's `min(cap+1)` cap.

**Bonus UX (visidata-style two-speed autofit):**

| Key | Action |
|---|---|
| `_`  | Autofit current column to **visible** rows (instant) |
| `g_` | Autofit current column to **all** rows (current behaviour) |
| `=`  | Autofit all columns to visible rows |
| `g=` | Autofit all columns to all rows |

Mirrors vim's `gg` prefix convention and aligns with visidata. Fitting to
visible rows is also typically *more useful* — a single outlier shouldn't
balloon every column.

---

## 8. Bug — European `DD/MM/YYYY` dates never auto-detected

**Type:** bug
**Files:** `src/main.rs:141-148` (`DATE_FORMATS`),
`src/main.rs:213-268` (`pick_date_format`)

`DATE_FORMATS` contains `%m/%d/%Y` but not `%d/%m/%Y`. The latter only
appears inside the ambiguity guard. A column with unambiguous European
dates (at least one value with day > 12) flows through every entry in
`DATE_FORMATS`, matches none, and stays as `String` — sortable
lexicographically only, no date comparisons in filters.

**Fix:**

1. Add `%d/%m/%Y` to `DATE_FORMATS` immediately after `%m/%d/%Y` (US first
   so ambiguous all-≤12 columns get the documented "ambiguous → string"
   treatment via the existing guard).
2. Extend the ambiguity-skip clause to also skip `%d/%m/%Y`:
   `if ambiguous_slash && matches!(*fmt, "%m/%d/%Y" | "%d/%m/%Y") { continue; }`.
3. Optional follow-up: `--date-format=us|eu|auto` CLI override for columns
   that are all-days-≤-12 (still kept as string by the ambiguity rule).

**Regression test (fails today; passes after the fix):**

```rust
#[test]
fn test_try_parse_date_columns_dd_mm_yyyy_unambiguous() {
    let csv = b"id,ts\n1,15/01/2023\n2,30/06/2023\n3,25/12/2023\n".to_vec();
    let df = parse_buf(csv, Some(b',')).expect("load");
    assert_eq!(df.column("ts").unwrap().dtype(), &DataType::Date);
}
```

---

## 9. Perf — Column Inspector recomputes 1,400 full-column passes on wide files

**Type:** perf
**Files:** `src/app.rs:875-924` (`build_columns_profile`),
`src/app.rs:692-722` (`compute_stats` — same shape)

Each column in `build_columns_profile` independently runs `n_unique`,
`min_reduce`, `max_reduce`, `sum`, `mean`, `median`. On `wide.csv` (200
columns) with 1M rows that's ~1,400 full passes per `i` press, and median
sorts every column. Inspector becomes unusable on the exact data shape it
was designed for.

**Two viable approaches:**

1. **Fuse into one lazy plan:**

   ```rust
   let exprs: Vec<Expr> = headers.iter().flat_map(|name| [
       col(name).count().alias(format!("{}__count", name)),
       col(name).null_count().alias(format!("{}__null", name)),
       col(name).n_unique().alias(format!("{}__unique", name)),
       col(name).min().alias(format!("{}__min", name)),
       col(name).max().alias(format!("{}__max", name)),
       col(name).mean().alias(format!("{}__mean", name)),
       col(name).median().alias(format!("{}__median", name)),
   ]).collect();
   let stats = self.view.clone().lazy().select(exprs).collect()?;
   ```

   Polars fuses count/null_count/min/max into a single scan per column and
   parallelises across columns.

2. **Two-tier UX (visidata-style):** grid shows the cheap stats only —
   `dtype`, `count`, `null_count`, `min`, `max`, `mean`. `Enter` (or `x`)
   on a row drills into a one-column detail view that also computes
   `n_unique` and `median`. Instant grid + on-demand depth.

Combine both for best results.

**`compute_stats` has the same shape** (six independent passes per call,
runs on every column move when `show_stats` is on) — same lazy-fuse fix.

**Regression test:** model on the perf test at `src/main.rs:603` —
build the profile against `wide.csv`, eprintln wall-time, assert under a
budget (e.g. 500ms). Catches future regressions.

---

## 10. Refactor — split `src/ui.rs` (2,130 lines) into a `ui/` module

**Type:** refactor
**Files:** `src/ui.rs` → `src/ui/{mod,table,popup,inspector,plot,status,help}.rs`

`ui.rs` is the largest non-test file and mixes six concerns: main table
render, popups, status / shortcut bars, Column Inspector, plotting (~750
lines on its own), and a chunk of `help_text` data. The `_pub` shims at
`ui.rs:1618-1626` exist only because the test module is in the same file
and can't reach private plot internals otherwise — a smell.

**Proposed layout:**

```
src/ui/
    mod.rs       // pub fn ui(), centered_rect, format_cell, NULL_GLYPH
    table.rs     // main table render, count_visible_from
    popup.rs     // stats / help / unique-values popups
    inspector.rs // render_columns_view, profile_row
    plot.rs      // histogram + line/bar/multi-series + axis + data extraction
    status.rs    // shortcut_bar, get_bar
    help.rs      // help_text() — pure data
```

Each submodule re-exports its public entry; `mod.rs` stays small.
`extract_plot_data` becomes `pub(super)` and the `_pub` shims disappear.

**Order:** land *after* the `Mode` refactor (#6). Once dispatch is
exhaustive, each `Mode` variant maps to one submodule and the split is
mechanical. Zero behaviour change. Don't do this during in-flight UI work
— merge conflicts will be miserable.

**Not in scope:** `text_viewer.rs` (766 lines) is also large but is its
own coherent feature, not a multi-concern monolith.

---

## 11. Feature — reorder columns (`H` / `L`)

**Type:** feature
**Files:** `src/app.rs` (state + `move_column_*` + `current_raw_col`
accessor + audit of `selected_column()` call sites), `src/ui.rs` (visible
column iteration), `src/events.rs`, tests, `qa.sh`

`h`/`l` already navigate columns; uppercase `H`/`L` are unbound and
visidata uses them for left/right swap. Closes the "these columns are 50
apart, I want them adjacent" gap on wide files.

**Don't mutate the DataFrame.** Add a display-order indirection so all
filter/sort/groupby state (keyed by name or raw index) is unaffected:

```rust
pub struct App {
    ...
    pub column_order: Vec<usize>, // identity by default; column_order[slot] = raw col
}
pub fn current_raw_col(&self) -> Option<usize> {
    self.state.selected_column().and_then(|i| self.column_order.get(i).copied())
}
```

Then iterate `column_order[viewport.col..]` in `ui.rs` where the table
visible-column list is built today. Audit the ~15 `state.selected_column()`
call sites that actually want the raw index and route them through
`current_raw_col`. Mechanical.

**Interactions:**
- `apply_groupby` / `clear_groupby` reset `column_order` to
  `(0..width).collect()` and stash/restore alongside `column_widths`.
- Pinned columns from #3 ignore `column_order` (already in their own
  list); hidden columns are filtered *after* applying `column_order`.

**Tests:** boundary no-ops; sort still works after reorder; reorder
survives groupby round-trip; `qa.sh` cases on `orders.csv` and
`wide.csv`.

---

## 12. Perf/UX — filter input rebuilds the full pipeline on every keystroke

**Type:** perf
**Files:** `src/app.rs:405-494` (`update_filter`), invalidation call sites
in `src/events.rs:372,408,449` and `src/app.rs:768,965`

`update_filter` rebuilds `self.df` → committed filters → optional
groupby on every keystroke in `f` mode, even though only the in-progress
filter changes between keys. For a multi-million-row file with committed
filters and an active groupby, that's visible lag per keystroke.

**Fix sketch — cache the committed-and-grouped intermediate:**

```rust
pub struct App {
    ...
    committed_base: Option<DataFrame>, // df → committed filters → optional groupby
}
```

`update_filter` clones the cache (cheap — `DataFrame::clone` is mostly
arc-bumps for the columns), applies only the in-progress filter + sort.
Invalidate from five call sites: `to_normal_mode_with_filter`,
`clear_filters`, `apply_groupby`, `clear_groupby`,
`apply_unique_value_filter`.

Optional safer shape: wrap as `CommittedPipeline { cache, invalidate(),
get(builder) }` so invalidation discipline lives in one place.

**Bonus:** same caching unlocks faster `update_search` — memoize the
active column's String cast for the duration of the search session.

**Pre-flight:** confirm typing is actually slow today via a perf test (10
keystrokes against a 1M-row fixture). If Polars' lazy plan is already
short-circuiting internally, this is moot.

---

## 13. Bug — text-viewer search panics on non-ASCII content

**Type:** bug
**Files:** `src/text_viewer.rs:214-235` (`recompute_matches`),
`src/text_viewer.rs:441-472` (`highlight_spans`)

Both functions lowercase the input then use the lowercase byte offsets to
slice the *original* `text`. `str::to_lowercase` is not
length-preserving — e.g. Turkish `İ` (2 bytes) → `i̇` (3 bytes). Slicing
the original with offsets from the lowercase string lands inside a UTF-8
sequence and panics in `Str::index`.

Separately, `recompute_matches` advances with `start = abs + 1` after a
match — fine for ASCII, but `+1` byte into a multibyte char in the
lowercase string causes the next `lower[start..]` call to panic.

**Fix sketch:**

```rust
// advance by one full char, not one byte
start = abs + lower[abs..].chars().next().map_or(1, |c| c.len_utf8());
```

For `highlight_spans` the cleanest fix is to stop slicing the original
text with lowercase offsets: build a parallel `lower_byte → text_byte`
offset map once per `text`, or use a case-insensitive search crate
(`caseless`/`unicase`). Document the limitation if you'd rather just fix
the panic and live with ASCII-correct case folding.

**Regression tests (in `text_viewer.rs::tests`):**

```rust
#[test] fn search_with_non_ascii_does_not_panic() { ... "İstanbul\n" ... }
#[test] fn search_advance_through_multibyte_char_does_not_panic() {
    // 'a' in "aña" — must find 2 hits, must not panic on advance.
}
```

The second test panics today.

---

## 14. Bug — line plots zigzag when X isn't monotonic (no scatter mode)

**Type:** bug + feature
**Files:** `src/ui.rs:1746-1796` (`extract_plot_data`),
`src/events.rs:208-222` (plot-type cycle), `src/app.rs:46-52` (`PlotType`)

`extract_plot_data` returns points in dataframe row order, no sort by X.
Feeding them to `GraphType::Line` produces a zigzag for any column where
the data isn't already sorted by X — confusing, and the obvious workaround
("sort by X first with `s`") only works for the *plot* phase if the user
remembers.

**Two-part fix:**

1. **Sort by X inside `extract_plot_data` when `plot.plot_type ==
   PlotType::Line`** (10 lines):

   ```rust
   if matches!(app.plot.plot_type, PlotType::Line) {
       points.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(Equal));
   }
   ```

   That's a pure bug fix — "Line" plot type implies monotonic X.

2. **Add `PlotType::Scatter`** for users who actually want the data-order
   view. `ratatui::widgets::GraphType::Scatter` exists. Insert into the
   type-cycle in `events.rs`:
   - single Y: `Line → Bar → Histogram → Scatter → Line`
   - multi Y: `Line → Bar → Scatter → Line`

**Tests:**

```rust
#[test]
fn line_plot_sorts_by_x() {
    let df = df! { "x" => [5.0, 1.0, 8.0, 3.0], "y" => [10.0, 20.0, 30.0, 40.0] }.unwrap();
    let app = make_plot_app(df, PlotType::Line, 0, 1);
    let (pts, _) = extract_plot_data_pub(&app, 0, 1);
    assert!(pts.windows(2).all(|w| w[0].0 <= w[1].0));
}
```

**Side notes:** `extract_plot_data` has an `.unwrap()` on `.f64()` after
a `series_to_f64` round-trip — swap for `.expect("series_to_f64 returns f64")`
for a more informative panic if the invariant ever changes. And confirm
`downsample` preserves first/last points; losing endpoints visibly bends
a plot's edges.

---

## 15. Feature: Yank current cell / row / column header to clipboard (`y`)

**Type:** Feature
**Files:** `src/app.rs`, `src/events.rs`, `Cargo.toml`

**Problem:** A TUI for poking at data files is incomplete without
"grab that value and paste it into Slack/SQL/an issue." Right now the
only way to extract a cell is to squint at it and retype — and wide
columns get truncated in the render, so what you see isn't even always
what's there.

**Proposed UX (visidata-flavored):**

| Keystroke | Yanks |
|---|---|
| `y` | current cell as a plain string |
| `yr` | current row as TSV (one line, tab-separated) |
| `yc` | current column name |
| `yC` | entire current column as one value per line (capped at e.g. 100k rows; warn if truncated) |

Show a transient message via the channel proposed in #2:
`"yanked cell: 42.0"`, `"yanked row (8 fields)"`, etc.

**Implementation sketch:**

1. Add `arboard = "3"` to `Cargo.toml`. It's the de-facto Rust clipboard
   crate, MIT-licensed, ~works on linux/mac/win. On linux/wayland it falls
   back to xclip/wl-copy if present — degrade gracefully with a transient
   "no clipboard available" message rather than panic.
2. New module `src/yank.rs` with `fn copy(text: &str) -> Result<(), String>`
   that lazily constructs `arboard::Clipboard` on first use and caches it
   (clipboard handle is moderately expensive on linux).
3. New `App` method `current_cell_string()` that pulls
   `view.column(name)?.get(row_idx)?` and formats it via the same path
   the renderer uses, so what you yank matches what you see.
4. Two-key sequence handled in `events.rs` similar to how `g`/`G` could be
   done: set `app.pending_key = Some('y')` on first `y`, consume the next
   key, clear pending on any non-`y*` key.

**Tests:**

- `current_cell_string` returns the same string the renderer would draw
  for ints, floats, strings, nulls, and dates.
- Row-yank produces N-1 tabs for an N-column row, no trailing newline.
- Column-yank truncates at the cap and reports the original length.

**Estimated effort:** small. ~80 lines total across `app.rs`, `events.rs`,
`Cargo.toml`. The cell-extraction logic is already there.

---

## 16. Feature: Derived / computed columns (`=` expression)

**Type:** Feature (headline visidata gap)
**Files:** `src/app.rs`, `src/events.rs`, `src/ui.rs`, new `src/expr.rs`

**Problem:** This is the biggest visidata capability you don't have. In
visidata, `=` opens a Python expression bar that creates a new column
from the others — `=price * quantity`, `=name.upper()`,
`=duration / 60` — and that column becomes first-class: sortable,
filterable, groupable, plottable. Without it, "load the parquet,
add a margin column, sort by margin desc" requires bouncing out to a
Jupyter notebook. With it, datasight covers maybe 70% more day-to-day
analyst use.

**Polars makes this almost trivial** because it already has an
expression engine. You don't need a parser — you can lean on polars
itself.

**Proposed UX:**

1. Press `=` in Normal mode → opens an input bar:
   `expr [name]: ___`
2. User types: `price * 1.2 as margin_with_tax` (the `as <name>` is
   optional; default to `expr_1`, `expr_2`, ...).
3. On Enter:
   - Parse `<expression> [as <name>]`.
   - Build a polars expression. Two options:
     - **(a) Restricted DSL:** parse a tiny grammar — column refs by
       name, `+ - * /`, numeric literals, a handful of functions
       (`abs`, `log`, `upper`, `lower`, `len`). Safe, predictable,
       ~150 lines with `nom` or hand-rolled.
     - **(b) `polars-sql`:** evaluate
       `SELECT *, (<expr>) as <name> FROM df`. Polars already has a
       SQL parser exposed via the `sql` feature. ~10 lines but adds a
       feature flag and pulls in `sqlparser`.
4. New column appears at the right of `df`; `view` is recomputed.
5. Derived columns are persisted alongside the file in a sidecar
   (see #11) so they survive reopens — or kept session-only by default
   with a "save derived columns?" prompt on quit.

**Recommendation: go with (a) restricted DSL.** Reasons:
- Predictable: users learn one small grammar that maps 1:1 to polars,
  no SQL-vs-Rust-vs-Python ambiguity.
- No new heavy dep (`sqlparser` is 200+ KB).
- The grammar is small enough to specify in `:help`.

**New state on `App`:**

```rust
pub struct DerivedColumn {
    pub name: String,
    pub source: String,
    pub expr: polars::lazy::dsl::Expr,
}

pub derived: Vec<DerivedColumn>,
```

`App::recompute_view` (or wherever you currently rebuild `view` from
`df` + filters + sort) gains a step:

```rust
let mut lf = df.clone().lazy();
for d in &self.derived {
    lf = lf.with_column(d.expr.clone().alias(&d.name));
}
```

That means derived columns participate naturally in everything
downstream.

**New mode:** `Mode::DerivedColumnInput`. New keybindings:

| Key | Action |
|---|---|
| `=` (Normal mode) | open expression input |
| `D` (Normal mode) | open "manage derived columns" popup — list, delete, edit |

**Tests:**

```rust
#[test]
fn derived_column_arithmetic() {
    let df = df! { "a" => [1, 2, 3], "b" => [10, 20, 30] }.unwrap();
    let mut app = App::new(df);
    app.add_derived("a + b as sum").unwrap();
    let col = app.view.column("sum").unwrap().i64().unwrap();
    assert_eq!(col.into_no_null_iter().collect::<Vec<_>>(), vec![11, 22, 33]);
}

#[test]
fn derived_column_survives_sort() {
    let df = df! { "x" => [3, 1, 2] }.unwrap();
    let mut app = App::new(df);
    app.add_derived("x * 10 as ten_x").unwrap();
    app.sort_by("ten_x", SortDirection::Desc);
    assert_eq!(app.view.column("ten_x").unwrap().i64().unwrap().get(0), Some(30));
}

#[test]
fn invalid_expr_does_not_corrupt_state() {
    let mut app = make_app();
    let r = app.add_derived("not_a_column + 1");
    assert!(r.is_err());
    assert!(app.derived.is_empty());
}
```

**Estimated effort:** medium. ~400 lines counting the mini-parser,
mode wiring, popup UI, and tests. The polars side is genuinely a
one-liner.

**Why this is the right next feature:** it multiplies the value of
*every* other feature (filter, sort, group, plot) without rewriting
any of them. Adding it after the perf and bug-fix tier (#1, #4, #9,
#13) is the natural ordering — a fast, correct base earns the right
to add expressive power on top.

---

## 17. Feature: Jump-to-row (`<N>G`, `gg`, `G`)

**Type:** Feature (vim-ergonomic)
**Files:** `src/app.rs`, `src/events.rs`, `src/ui.rs` (status bar)

**Problem:** A 2-million-row parquet, you want row 1,400,000. Today
either page-down 70,000 times or filter — neither is right. visidata
has `<N>g` for this; vim users instinctively type `1400000G`.

**Proposed UX (vim-faithful):**

| Keystroke | Action |
|---|---|
| `gg` | jump to first row |
| `G` | jump to last row |
| `<N>G` | jump to row N (1-indexed) |
| `<N>gg` | same as `<N>G` |

The numeric prefix is the new thing — none of the current
keybindings consume a number prefix. The same buffer can later power
`<N>j`/`<N>k` for free.

**Implementation sketch:**

1. Add `count_buffer: String` (or `count: Option<usize>`) to `App`.
2. In Normal-mode dispatch, intercept digit keys before the main match:
   ```rust
   if let KeyCode::Char(c @ '0'..='9') = key.code {
       app.count_buffer.push(c);
       return;
   }
   ```
   Edge case: leading `0` with an empty buffer should act as the
   "jump to column 0" operator (vim-consistent), not as a digit.
3. On `G`/`gg`, consume the buffer:
   ```rust
   let target = match app.count_buffer.parse::<usize>() {
       Ok(n) if n > 0 => n.saturating_sub(1).min(app.view.height() - 1),
       _ => /* G = last, gg = first */
   };
   ```
4. Render the pending count in the status bar so accumulating digits
   are visible.

**Edge cases / tests:**

- `<N>` larger than `view.height()` clamps to last row.
- `<N>G` on a 0-row view is a no-op and clears buffer.
- Mode switch (`/`, `:`, `f`, etc.) clears the buffer.
- `Esc` clears the buffer (vim-conformant).
- Cap digits at 12 chars to prevent runaway input.

```rust
#[test]
fn count_prefix_jumps_to_row() {
    let mut app = make_app_with_rows(1000);
    feed_keys(&mut app, "500G");
    assert_eq!(app.cursor_row(), 499);
}

#[test]
fn count_prefix_clamps_to_last_row() {
    let mut app = make_app_with_rows(50);
    feed_keys(&mut app, "9999G");
    assert_eq!(app.cursor_row(), 49);
}

#[test]
fn esc_clears_pending_count() {
    let mut app = make_app_with_rows(100);
    feed_keys(&mut app, "42");
    feed_keys(&mut app, "\x1b");
    feed_keys(&mut app, "G");
    assert_eq!(app.cursor_row(), 99);
}
```

**Estimated effort:** small. ~60 lines plus tests. Reusable infra —
once the buffer exists, `<N>j`/`<N>k`/`<N>l`/`<N>h` are nearly free.

**Why now:** cheapest feature on the list with the highest per-day
return for vim users. Also a prerequisite for "feels native" — every
other vim TUI accepts count prefixes, and the absence is noticeable.

---

## 18. Feature: Frozen / pinned columns (`Ctrl-f` on current column)

**Type:** Feature
**Files:** `src/app.rs`, `src/ui.rs`, `src/events.rs`

**Problem:** Wide tables (40+ columns is common in business CSVs) are
unusable past column 8 because as you scroll right, the identifying
column on the left (`id`, `user_email`, `order_id`) disappears.
You're staring at numbers with no anchor. Excel "freeze panes" is the
feature that makes wide sheets usable. visidata has `_` for this.

**Proposed UX:**

| Key | Action |
|---|---|
| `Ctrl-f` (Normal) | toggle "frozen" on the column under the cursor |
| `:freeze N` | freeze the first N columns wholesale |
| `:unfreeze` | clear all freezes |

Frozen columns render with the `accent` theme slot as a right border,
and stay visible while `l`/`h`/PgRight scrolls the non-frozen region.
Horizontal viewport math only applies to non-frozen columns.

**State on `App`:**

```rust
pub frozen_cols: BTreeSet<String>,
```

By name (not index) so freezes survive column reorder and derived
columns (#16).

**Rendering changes in `ui.rs`:**

`count_visible_from` becomes two-stage:

1. Lay out frozen columns from the left edge, accumulating
   `frozen_width`.
2. Run existing viewport windowing on `(area_width - frozen_width)`
   over the non-frozen columns, starting from `viewport.col`.

Helper to factor out:

```rust
fn split_columns<'a>(app: &'a App) -> (Vec<&'a str>, Vec<&'a str>) {
    let names = app.view.get_column_names();
    names.into_iter().partition(|n| app.frozen_cols.contains(*n))
}
```

`viewport.col` now indexes into the non-frozen subview — a subtle
shift worth documenting on `ViewportState::col`.

**Edge cases / tests:**

- Cursor on a column that gets frozen: keep cursor on it; `l` from
  that cursor jumps into the non-frozen region.
- Frozen total width > `area_width / 2`: cap and show a transient
  warning rather than truncate columns into illegibility.
- Group-by mode: respect frozen — the group-key columns are usually
  exactly what you want frozen.

```rust
#[test]
fn frozen_column_stays_visible_after_scroll() {
    let df = wide_df(20);
    let mut app = App::new(df);
    app.frozen_cols.insert("col_0".to_string());
    app.viewport.col = 15;
    let visible = visible_column_names(&app, 80);
    assert!(visible.contains(&"col_0".to_string()));
}
```

**Estimated effort:** small-to-medium. ~150 lines mostly in `ui.rs`
plus ~30 lines of state/events. The trickiest part is auditing every
column-index math site to use the non-frozen subview.

**Why this:** combined with #16 (derived columns) and #11 (saved
per-file state), this is the trio that makes wide tables genuinely
usable — open a familiar CSV with frozen columns and widths restored
automatically.

---

## 19. Refactor: Centralize column-index ↔ column-name conversions

**Type:** Refactor (small but high-leverage)
**Files:** `src/app.rs`, `src/events.rs`, `src/ui.rs`

**Problem:** The pattern

```rust
let col_name = app.view.get_column_names()[app.cursor_col].to_string();
```

is repeated across `app.rs`, `events.rs`, `ui.rs` with subtle
variations — some sites `.clone()`, some go through
`.get(i).unwrap_or(default)`, some index unsafely. Each is a panic
site if the cursor and view drift out of sync, which happens when
filter / group-by / derived columns reshape view column count.

It's also a future-bug magnet: #16, #18, sort, group-by all
re-derive this mapping, and not all apply the same clamping rules.

**Proposed accessors on `App`:**

```rust
impl App {
    pub fn current_col_name(&self) -> Option<&str> {
        self.view.get_column_names().get(self.cursor_col).copied()
    }

    pub fn current_col_name_owned(&self) -> Option<String> {
        self.current_col_name().map(str::to_string)
    }

    pub fn col_index(&self, name: &str) -> Option<usize> {
        self.view.get_column_names().iter().position(|n| *n == name)
    }

    pub fn clamp_cursor_col(&mut self) {
        let last = self.view.width().saturating_sub(1);
        self.cursor_col = self.cursor_col.min(last);
    }
}
```

**Migration steps:**

1. Land the new accessors.
2. Grep `get_column_names()[` and replace each hit.
3. Grep `.cursor_col` assignments; audit each for the new
   `clamp_cursor_col()` call. Candidates: `apply_filter`,
   `recompute_view`, `exit_group_by`.
4. Add a regression test that builds the cursor-drift scenario.

**Tests:**

```rust
#[test]
fn cursor_clamps_after_view_shrinks() {
    let df = df! { "a" => [1], "b" => [2], "c" => [3] }.unwrap();
    let mut app = App::new(df);
    app.cursor_col = 2;
    app.view = df!{ "a" => [1], "b" => [2] }.unwrap();
    app.clamp_cursor_col();
    assert_eq!(app.cursor_col, 1);
    assert_eq!(app.current_col_name(), Some("b"));
}

#[test]
fn current_col_name_on_empty_view_returns_none() {
    let app = App::new(DataFrame::empty());
    assert!(app.current_col_name().is_none());
}
```

**Estimated effort:** small. ~50 lines of new code, ~30 line
migrations across the codebase, near-zero behavior change for users.

**Why mid-feature-list:** #16 (derived) and #18 (frozen) both
introduce states where cursor's effective column index is
ambiguous. Landing this refactor *before* them costs nothing;
landing it *after* means rewriting both features.
