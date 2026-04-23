#!/usr/bin/env bash
# qa.sh — TUI smoke-test suite for datasight
# Requires: tmux, a built binary at ./target/debug/datasight
# Usage: bash qa.sh
# Run from the repo root.

set -uo pipefail

BINARY="./target/debug/datasight"
SESSION="qa"
APP_PANE="$SESSION:0.0"
PASS=0
FAIL=0
FAILURES=()

# ── tmux setup ────────────────────────────────────────────────────────────────

tmux kill-session -t "$SESSION" 2>/dev/null || true
tmux new-session -d -s "$SESSION" -x 220 -y 50
sleep 0.3

# Helpers
# send: send literal keys (no special key interpretation)
send()   { tmux send-keys -t "$APP_PANE" -l "$1"; sleep "${2:-0.10}"; }
# key: send a named tmux key (Enter, Escape, PgDn, PgUp, etc.)
key()    { tmux send-keys -t "$APP_PANE" "$1"; sleep "${2:-0.15}"; }
esc()    { key Escape 0.15; }
enter()  { key Enter "${1:-0.20}"; }
pgdn()   { key PgDn  0.15; }
pgup()   { key PgUp  0.15; }
cap()    { tmux capture-pane -t "$APP_PANE" -p 2>/dev/null || true; }

start_app() {
  # Kill any running app cleanly; clear shell input line
  tmux send-keys -t "$APP_PANE" C-c; sleep 0.15
  tmux send-keys -t "$APP_PANE" C-u; sleep 0.10
  tmux send-keys -t "$APP_PANE" "$BINARY $*" Enter
  sleep 0.6
}

quit() {
  send "q" 0.20
}

assert_contains() {
  local label="$1" pattern="$2"
  if cap | grep -q "$pattern"; then
    echo "  PASS [$label]"
    PASS=$((PASS + 1))
  else
    echo "  FAIL [$label] — expected: '$pattern'"
    FAIL=$((FAIL + 1))
    FAILURES+=("[$label] expected '$pattern'")
  fi
}

assert_not_contains() {
  local label="$1" pattern="$2"
  if ! cap | grep -q "$pattern"; then
    echo "  PASS [$label]"
    PASS=$((PASS + 1))
  else
    echo "  FAIL [$label] — did NOT expect: '$pattern'"
    FAIL=$((FAIL + 1))
    FAILURES+=("[$label] did not expect '$pattern'")
  fi
}

# ── Suite A: File format loading ───────────────────────────────────────────────
echo ""
echo "=== Suite A: File format loading ==="

for fmt in csv tsv json ndjson; do
  start_app "tests/fixtures/orders.$fmt"
  assert_contains "A/$fmt-header" "order_id"
  assert_contains "A/$fmt-col"    "region"
  quit
done

# parquet fixture has a different schema (id, name, age, city, score, active)
start_app "tests/fixtures/orders.parquet"
assert_contains "A/parquet-header" "id"
assert_contains "A/parquet-col"    "city"
quit

start_app "tests/fixtures/wide.csv"
assert_contains "A/wide-header" "very_long"
send "llllllllll"
assert_contains "A/wide-hscroll" "col"
quit

# stdin CSV
tmux send-keys -t "$APP_PANE" "cat tests/fixtures/orders.csv | $BINARY" Enter
sleep 0.6
assert_contains "A/stdin-csv" "order_id"
quit

# stdin JSON
tmux send-keys -t "$APP_PANE" "cat tests/fixtures/orders.json | $BINARY" Enter
sleep 0.6
assert_contains "A/stdin-json" "order_id"
quit

# ── Suite B: Normal mode navigation ───────────────────────────────────────────
echo ""
echo "=== Suite B: Normal mode navigation ==="

start_app "tests/fixtures/orders.csv"
assert_contains "B/start" "order_id"

send "jjjjj"
send "kkk"
send "G" 0.30
assert_contains "B/last-row" "1100"   # order_id 1100 is last row

send "g" 0.25
assert_contains "B/first-row" "1001"  # back to top

pgdn
pgup
send "llll"
assert_contains "B/col-right" "region"
send "hh"
send "_"  0.15
send "="  0.2
assert_contains "B/autofit" "order_id"
quit

# ── Suite C: Search mode ───────────────────────────────────────────────────────
echo ""
echo "=== Suite C: Search mode ==="

start_app "tests/fixtures/orders.csv"
send "/"
assert_contains "C/search-mode" "/_"
send "Alice"
enter 0.25
assert_contains "C/found-alice" "Alice"
send "n" 0.1
send "N" 0.1
# exit search
send "/"
esc
# no-match search
send "/"
send "zzznomatch"
enter 0.2
assert_contains "C/no-crash" "order_id"
esc
quit

# ── Suite D: Filter mode ───────────────────────────────────────────────────────
echo ""
echo "=== Suite D: Filter mode ==="

start_app "tests/fixtures/orders.csv"

# D1: filter region=North (col index 4 — press l 4 times)
send "llll"
send "f"
send "North"
enter 0.25
assert_contains     "D/filter-north"    "North"
assert_not_contains "D/no-south"        "South"

# D2: chain filter quantity > 1 (3 more rights to quantity col)
send "lll"
send "f"
send "> 1"
enter 0.25
assert_contains "D/chained-filter" "North"

# D3: clear all filters
send "F" 0.25
assert_contains "D/clear-filters" "South"

# D4: invalid operator on string col
send "llll"  # region col
send "f"
send "> abc"
enter 0.15
assert_contains "D/filter-error" "requires a number"
esc

# D5: Esc discards filter
send "f"
send "Pending"
esc
assert_contains "D/esc-no-filter" "South"

# D6: Fix 4 — filter column is locked when f is pressed, not at Enter time
# Note: after all prior navigation we're on the status col (string), so "> 0" produces an
# error and Enter stays in Filter mode.  Add esc to cleanly return to Normal before cleanup.
send "lllllll"  # status col (col 11, clamped from wherever we are)
send "f"
send "> 0"
enter 0.25
assert_contains "D/fix4-no-crash" "total_amount"
esc             # exit Filter mode (Enter kept us here because of the type error on string col)
send "F" 0.25   # clear all filters now that we're back in Normal mode

quit

# ── Suite E: Unique values mode ────────────────────────────────────────────────
echo ""
echo "=== Suite E: Unique values mode ==="

start_app "tests/fixtures/orders.csv"
send "llll"   # region col
send "u" 0.3
assert_contains "E/popup-open" "Unique"
send "/"
send "Nor"
sleep 0.15
assert_contains "E/filter-narrow" "North"
enter 0.25
assert_contains     "E/filter-applied" "North"
assert_not_contains "E/no-south"       "South"
send "F" 0.25

# Esc without applying
send "u" 0.3
esc
assert_contains "E/esc-no-filter" "South"
quit

# E6-E7: null fixture
start_app "tests/fixtures/orders_nulls.csv"
send "lll"   # customer_name col
send "u" 0.4
assert_contains "E/null-popup" "Unique"
assert_contains "E/null-shown" "(null)"
esc
quit

# ── Suite F: Sort ──────────────────────────────────────────────────────────────
echo ""
echo "=== Suite F: Sort ==="

start_app "tests/fixtures/orders.csv"
send "lllllllll"   # total_amount col
send "s" 0.25
assert_contains "F/sort-asc" "order_id"
send "s" 0.25
assert_contains "F/sort-desc" "order_id"
send "hhhhhh"       # customer_name col (6 left from total_amount)
send "s" 0.25
assert_contains "F/sort-str" "Alice"
quit

# ── Suite G: Stats popup ───────────────────────────────────────────────────────
echo ""
echo "=== Suite G: Stats popup ==="

start_app "tests/fixtures/orders.csv"
send "lllllllll"   # total_amount col
send "e" 0.25
assert_contains "G/stats-open" "Count"
assert_contains "G/stats-mean" "Mean"
send "e" 0.25
assert_not_contains "G/stats-closed" "Count"

# non-numeric col
send "hhhhhh"
send "e" 0.25
assert_contains "G/stats-na" "N/A"
send "e" 0.25
quit

# null fixture stats
start_app "tests/fixtures/orders_nulls.csv"
send "lllllllll"
send "e" 0.25
assert_contains "G/null-stats" "Count"
send "e" 0.25
quit

# ── Suite H: Column Inspector ──────────────────────────────────────────────────
echo ""
echo "=== Suite H: Column Inspector ==="

start_app "tests/fixtures/orders.csv"
send "i" 0.3
assert_contains "H/inspector-open" "Column"
send "jjj"
send "kkk"
send "g" 0.1
send "G" 0.1
enter 0.25
assert_contains "H/inspector-select" "status"  # G selected last col (status); order_id may scroll off
send "i" 0.3
assert_contains "H/inspector-toggle" "order_id"  # inspector lists all cols; order_id is in the list
send "i" 0.3
esc
assert_contains "H/esc-close" "status"  # still at col 11 after close
quit

# ── Suite I: Group-by ─────────────────────────────────────────────────────────
echo ""
echo "=== Suite I: Group-by ==="

start_app "tests/fixtures/orders.csv"
send "llll"   # region col
send "b" 0.15
assert_contains "I/key-marked" "region"

send "lllll"  # total_amount col
send "a" 0.1
send "a" 0.1
send "a" 0.1
send "B" 0.4
assert_contains "I/groupby-applied" "region"

send "s" 0.25
assert_contains "I/grouped-sort" "region"

send "B" 0.4
assert_contains "I/groupby-cleared" "order_id"

# no key/agg — no crash
send "B" 0.25
assert_contains "I/no-key-no-crash" "order_id"
quit

# ── Suite J: Plot mode ─────────────────────────────────────────────────────────
echo ""
echo "=== Suite J: Plot mode ==="

start_app "tests/fixtures/orders.csv"

# J1: single-Y plot — PlotPickY → PlotPickX → Plot
send "lllllllll"          # total_amount (col 9)
send "p" 0.25
assert_contains "J/picky-mode"   "Space toggle"   # in PlotPickY
assert_contains "J/picky-presel" "total_amount"   # pre-selected in status bar

enter 0.25                # confirm single Y, move to PlotPickX
assert_contains "J/pickx-prompt" "navigate to X"

send "hhhhhhhhh"          # 9 left → order_id (col 0)
enter 0.4
assert_contains "J/plot-rendered"  "total_amount"
send "t" 0.25
assert_contains "J/plot-bar"       "Bar"
send "t" 0.25
assert_contains "J/plot-hist"      "Histogram"   # single-Y: histogram available
send "t" 0.25
esc
sleep 0.2
assert_contains "J/plot-exit"      "order_id"

# J2: multi-Y plot — two columns, legend, histogram disabled
send "lllllllll"          # total_amount (col 9)
send "p" 0.25             # PlotPickY, total_amount pre-selected
key Left 0.15
key Left 0.15             # navigate to quantity (col 7)
send " " 0.25             # Space: toggle quantity into Y cols
assert_contains "J/picky-two-y"   "quantity"     # both cols now in status bar

enter 0.25                # confirm Y cols, move to PlotPickX
assert_contains "J/pickx-two-y"   "navigate to X"

key Left 0.15
key Left 0.15
key Left 0.15
key Left 0.15
key Left 0.15
key Left 0.15
key Left 0.15             # 7 left → order_id (col 0)
enter 0.4
assert_contains     "J/multi-rendered"  "total_amount"
assert_contains     "J/multi-legend"    "●"          # legend marker
send "t" 0.25
assert_contains     "J/multi-bar"       "Bar"
assert_not_contains "J/multi-no-hist"   "Histogram"  # histogram disabled for multi-Y
send "t" 0.25
assert_contains     "J/multi-line"      "Line"
esc
sleep 0.2
assert_contains "J/multi-exit" "order_id"

# J3: Esc from PlotPickY cancels entirely
send "lllllllll"
send "p" 0.25
esc
sleep 0.15
assert_contains "J/picky-esc" "order_id"

# J4: Esc from PlotPickX goes back to PlotPickY
send "lllllllll"
send "p" 0.25
enter 0.25                # go to PlotPickX
esc
sleep 0.15
assert_contains "J/pickx-esc-back" "Space toggle"  # back in PlotPickY
esc
sleep 0.15
assert_not_contains "J/picky-esc2"  "Space toggle"

quit

# ── Suite K: Help popup ────────────────────────────────────────────────────────
echo ""
echo "=== Suite K: Help popup ==="

start_app "tests/fixtures/orders.csv"
send "?" 0.25
assert_contains "K/help-open" "Navigation"
send "jjj"
send "kkk"
pgdn
pgup
send "?" 0.25
assert_not_contains "K/help-closed" "Navigation"
send "?" 0.25
esc
assert_not_contains "K/esc-close" "Navigation"
quit

# ── Suite L: Edge cases ────────────────────────────────────────────────────────
echo ""
echo "=== Suite L: Edge cases ==="

start_app "tests/fixtures/orders.csv"

# L1: filter to 0 rows, then search
send "llll"   # region col
send "f"
send "= zzznomatch"
enter 0.25
send "/"
send "Alice"
enter 0.2
assert_contains "L/zero-search" "order_id"
send "F" 0.25

# L2: filter to 0 rows, then sort
send "f"
send "= zzznomatch"
enter 0.25
send "s" 0.2
assert_contains "L/zero-sort" "order_id"
send "F" 0.25

# L3: filter to 0 rows, then group-by
send "f"
send "= zzznomatch"
enter 0.25
send "b" 0.1
send "lll"
send "a" 0.1
send "B" 0.25
assert_contains "L/zero-groupby" "order_id"
send "B" 0.25
send "F" 0.25

# L4: filter to 0 rows, then plot
send "f"
send "= zzznomatch"
enter 0.25
send "lllllllll"
send "p" 0.25
esc
sleep 0.15
assert_contains "L/zero-plot" "order_id"
send "F" 0.25

quit

# ── Suite M: Row navigation clamping ──────────────────────────────────────────
echo ""
echo "=== Suite M: Row navigation clamping ==="

start_app "tests/fixtures/orders.csv"

# M1: spam Down past end, then Up once — cursor must visually move up
# With the bug (unclamped): G sets internal to usize::MAX or 99, then 50 Down
# accumulates to 149; 1 Up → 148; status bar still shows "Row 100/100".
# With the fix (clamped): Down clamps at last row (99); 1 Up → 98 → "Row 99/100".
key G 0.30
for _ in $(seq 1 50); do key Down 0.03; done
key Up 0.20
assert_contains     "M/down-spam-up" "Row 99/100"
assert_not_contains "M/down-spam-stuck" "Row 100/100"

# M2: same for j/k aliases
key G 0.30
for _ in $(seq 1 50); do send "j" 0.03; done
send "k" 0.20
assert_contains     "M/jk-spam-up" "Row 99/100"
assert_not_contains "M/jk-spam-stuck" "Row 100/100"

quit

# wide.csv edge cases
start_app "tests/fixtures/wide.csv"

# L6: horizontal scroll to far right
for _ in $(seq 1 30); do send "l" 0.03; done
assert_contains "L/wide-hscroll" "col"

# L7: autofit all on wide
send "=" 0.4
assert_contains "L/wide-autofit" "col"

# L8: rapid keystrokes
tmux send-keys -t "$APP_PANE" "jjjjjjjjjjkkkkkkkkkk" ""
sleep 0.4
assert_contains "L/rapid-keys" "col"

quit

# ── Summary ────────────────────────────────────────────────────────────────────
echo ""
echo "════════════════════════════════════════"
echo "  QA Results: $PASS passed, $FAIL failed"
echo "════════════════════════════════════════"

if [ "${#FAILURES[@]}" -gt 0 ]; then
  echo ""
  echo "Failures:"
  for f in "${FAILURES[@]}"; do
    echo "  • $f"
  done
  echo ""
  exit 1
fi

echo ""
echo "All checks passed. Safe to release."
exit 0
