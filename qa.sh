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
# Suite O asserts on real clipboard content: with set-clipboard on, tmux accepts
# the app's OSC 52 sequence and stores it in a paste buffer we can read back.
tmux set-option -g set-clipboard on
sleep 0.3

# Scratch fixtures generated at run time. These deliberately do NOT live in
# tests/fixtures/ — Suite Z navigates that directory by hardcoded index, so an
# extra file there would shift the cursor and break the browse suites.
QA_TMP="$(mktemp -d)"
trap 'rm -rf "$QA_TMP"' EXIT

# Long values (issue #23): the widest cell far exceeds the old 40-char clamp.
cat > "$QA_TMP/long_values.csv" <<'CSV'
KeyName,LnkName,KeyLastWriteTimestamp
windows powershe|6d2a715ad3bf3395,C:\ProgramData\Microsoft\Windows\Start Menu\Programs\Windows PowerShell\Windows PowerShell ISE (x86).lnk,2025-05-23 10:52:37
wordpad.lnk|b071df8746c2a535,C:\ProgramData\Microsoft\Windows\Start Menu\Programs\Accessories\Wordpad.lnk,2025-05-23 10:52:34
wps office.lnk|1c3aad1926983f46,c:\users\otello.j\Desktop\WPS Office.lnk,2025-05-23 11:46:14
CSV

# Single data row: polars backs length-1 columns with a Scalar, which the
# renderer once mistook for "no data" and painted as null glyphs.
cat > "$QA_TMP/single_row.csv" <<'CSV'
name,qty
solitary,7
CSV

# A list-typed column: CsvWriter rejects nested dtypes mid-write, which is the
# failure path that must not destroy an existing destination file.
cat > "$QA_TMP/nested.ndjson" <<'JSON'
{"a": 1, "b": [1, 2]}
{"a": 2, "b": [3]}
JSON

# Helpers
# send: send literal keys (no special key interpretation).
# The `--` is required: without it tmux parses a payload like "----" as flags.
send()   { tmux send-keys -t "$APP_PANE" -l -- "$1"; sleep "${2:-0.10}"; }
# key: send a named tmux key (Enter, Escape, PgDn, PgUp, etc.)
key()    { tmux send-keys -t "$APP_PANE" "$1"; sleep "${2:-0.15}"; }
esc()    { key Escape 0.15; }
enter()  { key Enter "${1:-0.20}"; }
pgdn()   { key PgDn  0.15; }
pgup()   { key PgUp  0.15; }
cap()    { tmux capture-pane -t "$APP_PANE" -p 2>/dev/null || true; }

# await_pane: poll until PATTERN shows up in the pane. Returns 1 on timeout.
await_pane() {
  local pattern="$1" deadline=$((SECONDS + ${2:-10}))
  while [ "$SECONDS" -le "$deadline" ]; do
    cap | grep -qF -- "$pattern" && return 0
    sleep 0.05
  done
  return 1
}

# await_app: poll until the launched TUI owns the screen — a box corner is
# present and MARKER (the shell token printed just before launching) is gone.
#
# Waiting for the corner alone is not enough: it would also match the frame of a
# previous app, or a shell prompt drawn with box characters. The marker
# disappearing is what proves we are looking at the new app's alternate screen.
await_app() {
  local marker="$1" deadline=$((SECONDS + ${2:-20})) pane
  while [ "$SECONDS" -le "$deadline" ]; do
    pane="$(cap)"
    if ! printf '%s' "$pane" | grep -qF -- "$marker" &&
       printf '%s' "$pane" | grep -qE '[╭┌]'; then
      sleep 0.15   # let the first full paint finish before any key is sent
      return 0
    fi
    sleep 0.05
  done
  return 1
}

LAUNCHES=0

# launch: run CMD in the app pane and block until its TUI has painted.
#
# The pane is respawned rather than interrupted. datasight ignores ctrl-c, and in
# browse mode `q` is ignored while a file is open (browser/events.rs), so neither
# reliably dismisses the previous app — it can still be running and repainting
# when the next one is launched.
#
# Both waits poll rather than sleeping a fixed amount. The fixed sleep this
# replaces was the cause of intermittent failures under load: keystrokes arrived
# before the app existed and reached the shell instead, where a filter query like
# "> 0" is a redirection that silently created junk files in the repo root.
launch() {
  local cmd="$1" marker
  LAUNCHES=$((LAUNCHES + 1))
  marker="qa-shell-ready-$LAUNCHES"
  tmux respawn-pane -k -t "$APP_PANE"; sleep 0.20
  tmux send-keys -t "$APP_PANE" "clear; echo $marker" Enter
  if ! await_pane "$marker" 10; then
    echo "  FAIL [launch] shell never became ready for: $cmd"
    FAIL=$((FAIL + 1)); FAILURES+=("[launch] shell not ready for '$cmd'")
    return 1
  fi
  tmux send-keys -t "$APP_PANE" "$cmd" Enter
  if ! await_app "$marker" 20; then
    echo "  FAIL [launch] no frame painted for: $cmd"
    FAIL=$((FAIL + 1)); FAILURES+=("[launch] no frame for '$cmd'")
    return 1
  fi
}

start_app() { launch "$BINARY $*"; }

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

# Clipboard helpers. drop_buffers clears tmux's paste-buffer stack so a stale
# buffer cannot satisfy the next assertion.
drop_buffers() { while tmux delete-buffer 2>/dev/null; do :; done; }

assert_buffer_contains() {
  local label="$1" pattern="$2"
  if tmux show-buffer 2>/dev/null | grep -qF -- "$pattern"; then
    echo "  PASS [$label]"
    PASS=$((PASS + 1))
  else
    echo "  FAIL [$label] — clipboard buffer missing: '$pattern'"
    FAIL=$((FAIL + 1))
    FAILURES+=("[$label] clipboard buffer missing '$pattern'")
  fi
}

# Export helpers. clear_line empties a prompt that opens prefilled (the export
# prompt suggests a filename), so a test can type a $QA_TMP destination instead of
# writing into the repo working tree.
clear_line() {
  local n="${1:-40}"
  for _ in $(seq 1 "$n"); do tmux send-keys -t "$APP_PANE" BSpace; done
  sleep 0.20
}

assert_file_lines() {
  local label="$1" file="$2" expected="$3" actual
  actual="$(wc -l < "$file" 2>/dev/null || echo missing)"
  if [ "$actual" = "$expected" ]; then
    echo "  PASS [$label]"
    PASS=$((PASS + 1))
  else
    echo "  FAIL [$label] — expected $expected lines, got $actual"
    FAIL=$((FAIL + 1))
    FAILURES+=("[$label] expected $expected lines in $file, got $actual")
  fi
}

assert_file_head() {
  local label="$1" file="$2" pattern="$3"
  if head -1 "$file" 2>/dev/null | grep -qF -- "$pattern"; then
    echo "  PASS [$label]"
    PASS=$((PASS + 1))
  else
    echo "  FAIL [$label] — first line of $file missing: '$pattern'"
    FAIL=$((FAIL + 1))
    FAILURES+=("[$label] first line of $file missing '$pattern'")
  fi
}

# assert_exited: the app left the alternate screen — no TUI frame is on the pane
# any more. Polls, because restoring the terminal takes a beat. Asserting a quit
# needs this shape: every other assertion reads a frame that is still up.
assert_exited() {
  local label="$1" deadline=$((SECONDS + 5))
  while [ "$SECONDS" -le "$deadline" ]; do
    if ! cap | grep -qE '[╭┌]'; then
      echo "  PASS [$label]"
      PASS=$((PASS + 1))
      return
    fi
    sleep 0.05
  done
  echo "  FAIL [$label] — the TUI is still on screen"
  FAIL=$((FAIL + 1))
  FAILURES+=("[$label] TUI still on screen")
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
launch "cat tests/fixtures/orders.csv | $BINARY"
assert_contains "A/stdin-csv" "order_id"
quit

# stdin JSON
launch "cat tests/fixtures/orders.json | $BINARY"
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
assert_contains "J/picky-mode"   "Toggle Y"   # in PlotPickY
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
assert_contains "J/pickx-esc-back" "Toggle Y"  # back in PlotPickY
esc
sleep 0.15
assert_not_contains "J/picky-esc2"  "Toggle Y"

quit

# ── Suite J2: Dual-panel plot mode ─────────────────────────────────────────────
echo ""
echo "=== Suite J2: Dual-panel plot mode ==="

start_app "tests/fixtures/orders.csv"

# J2-1: assign total_amount to panel 1 (pre-selected by p), quantity to panel 2
send "lllllllll"          # total_amount (col 9)
send "p" 0.25             # PlotPickY: total_amount pre-selected in y_cols
assert_contains "J2/picky-mode" "P1"  # status bar shows P1/P2 labels

key Left 0.15
key Left 0.15             # navigate to quantity (col 7)
send "2" 0.25             # toggle quantity into y2_cols (panel 2)
assert_contains "J2/p2-assigned" "quantity"  # quantity appears in P2 status

# J2-2: guard: pressing 2 again on quantity removes it from y2 (toggle off)
# navigate back to total_amount — pressing 2 on it (the only y1 col) must be a no-op
send "ll"                 # back to total_amount (col 9)
send "2" 0.15             # this would drain y_cols — must be refused
assert_contains "J2/drain-guard" "total_amount"  # total_amount still in P1

# J2-3: re-navigate to quantity, ensure it is still in P2 (guard didn't corrupt state)
key Left 0.15
key Left 0.15             # back to quantity
assert_contains "J2/p2-intact" "quantity"

# J2-4: press i to plot against row index
send "i" 0.40
assert_contains "J2/panel1-title" "total_amount"   # panel 1 chart title
assert_contains "J2/panel2-title" "quantity"       # panel 2 chart title

# J2-5: t cycles Line → Bar only (no Histogram) in dual mode
send "t" 0.25
assert_contains     "J2/dual-bar"     "Bar"
assert_not_contains "J2/dual-no-hist" "Histogram"
send "t" 0.25
assert_contains     "J2/dual-line"    "Line"

esc
sleep 0.15
assert_contains "J2/dual-exit" "order_id"

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

# ── Suite N: Column widths (issue #23) ────────────────────────────────────────
echo ""
echo "=== Suite N: Column widths ==="

start_app "$QA_TMP/long_values.csv"

# N1: default widths truncate the long path column
assert_not_contains "N/default-truncates" "Wordpad.lnk"

# N2: '=' autofits every column — the full path is now on screen.
# The old MAX_COLUMN_WIDTH=40 clamp made this impossible at any terminal size.
send "=" 0.4
assert_contains "N/autofit-all-long" "Accessories.Wordpad.lnk"
assert_contains "N/autofit-all-header" "KeyLastWriteTimestamp"

# N3: '_' on the selected column fits it, and pressing again resets it
send "hh" 0.15          # ensure cursor is on the first column
send "_" 0.25           # column 0 is already fitted from N2 → resets to default
assert_not_contains "N/autofit-toggle-reset" "windows powershe|6d2a715ad3bf3395"
send "_" 0.25           # fit it again
assert_contains "N/autofit-toggle-refit" "windows powershe|6d2a715ad3bf3395"

# N4: '-' narrows the current column, '+' widens it back
send "----" 0.30
assert_not_contains "N/shrink" "windows powershe|6d2a715ad3bf3395"
send "++++" 0.30
assert_contains "N/grow" "windows powershe|6d2a715ad3bf3395"

# N5: shrinking past the floor must not wedge the column at zero width.
# MIN_COLUMN_WIDTH is 6, so the 7-char header renders clipped to "KeyNam" —
# present at width 6, absent entirely at width 0.
send "----------------" 0.40
assert_contains     "N/shrink-floor"      "KeyNam"
assert_not_contains "N/shrink-floor-min"  "KeyName"

quit

# N6: a single-row file renders its values, not null glyphs.
# Regression for Column::as_series() returning None on Scalar-backed columns.
start_app "$QA_TMP/single_row.csv"
assert_contains     "N/single-row-value" "solitary"
assert_not_contains "N/single-row-not-null" "∅"
quit

# ── Suite O: Clipboard copy (issue #24) ───────────────────────────────────────
echo ""
echo "=== Suite O: Clipboard copy ==="

# O1: y copies the selected cell — confirmed on screen and in tmux's paste buffer.
start_app "tests/fixtures/orders.csv"
drop_buffers
send "y" 0.25
assert_contains        "O/cell-status"  "Copied cell"
assert_buffer_contains "O/cell-buffer"  "1001"

# O2: the confirmation lasts exactly one keystroke.
send "j" 0.20
assert_not_contains "O/status-transient" "Copied cell"

# O3: Y copies the whole row, tab-separated.
drop_buffers
send "Y" 0.25
assert_contains        "O/row-status" "Copied row"
assert_buffer_contains "O/row-buffer" "Bob Smith"
if tmux show-buffer 2>/dev/null | grep -q "$(printf '\t')"; then
  echo "  PASS [O/row-tab-separated]"
  PASS=$((PASS + 1))
else
  echo "  FAIL [O/row-tab-separated] — no tab in clipboard buffer"
  FAIL=$((FAIL + 1))
  FAILURES+=("[O/row-tab-separated] no tab in clipboard buffer")
fi

# O4: both keys are discoverable in the help popup ("Other" section, two pages down).
send "?" 0.30
pgdn
pgdn
assert_contains "O/help-cell" "Copy cell to clipboard"
assert_contains "O/help-row"  "Copy row to clipboard"
esc
quit

# O5: copying a null cell copies nothing without panicking, and the app stays live.
# Row 2 / column total_amount is empty in the null fixture.
start_app "tests/fixtures/orders_nulls.csv"
send "j" 0.15
send "lllllllll" 0.25
drop_buffers
send "y" 0.25
assert_contains "O/null-cell-status" "Copied cell"
send "k" 0.20
assert_contains "O/null-cell-app-alive" "order_id"
quit

# ── Suite P: CSV export ───────────────────────────────────────────────────────
echo ""
echo "=== Suite P: CSV export ==="

# P1: w opens a prompt prefilled with a name derived from the source file.
start_app "tests/fixtures/orders.csv"
send "w" 0.30
assert_contains "P/prompt-default-name" "orders.export.csv"
assert_contains "P/prompt-hint"         "writes CSV"
assert_contains "P/prompt-shortcut-bar" "Write"

# P2: Esc closes the prompt without writing; the key stays advertised in Normal mode.
esc
assert_not_contains "P/esc-closes-prompt" "writes CSV"
assert_contains     "P/normal-bar-has-w"  "Export"

# P3: the full view exports every row, header included (fixture: 100 rows).
send "w" 0.25
clear_line
send "$QA_TMP/all.csv" 0.20
enter 0.50
assert_contains    "P/write-status"  "Wrote 100 rows"
assert_file_lines  "P/write-lines"   "$QA_TMP/all.csv" 101
assert_file_head   "P/write-header"  "$QA_TMP/all.csv" "order_id,order_date"

# P4: the confirmation is transient, like the clipboard one.
send "j" 0.20
assert_not_contains "P/status-transient" "Wrote 100 rows"

# P5: a filtered view exports only the rows on screen (region North = 25 rows).
send "llll" 0.20   # order_id → region
send "f" 0.20
send "North" 0.20
enter 0.35
send "w" 0.25
clear_line
send "$QA_TMP/north.csv" 0.20
enter 0.50
assert_contains   "P/filtered-status" "Wrote 25 rows"
assert_file_lines "P/filtered-lines"  "$QA_TMP/north.csv" 26
assert_file_head  "P/filtered-header" "$QA_TMP/north.csv" "order_id,order_date"

# P6: a path with no extension gets .csv appended, and the status reports the real name.
send "w" 0.25
clear_line
send "$QA_TMP/no_ext" 0.20
enter 0.50
assert_contains   "P/appends-extension" "no_ext.csv"
assert_file_lines "P/appends-lines"     "$QA_TMP/no_ext.csv" 26

# P7: re-typing an existing destination warns before Enter is pressed.
send "w" 0.25
clear_line
send "$QA_TMP/north.csv" 0.30
assert_contains "P/overwrite-warning" "overwrites"
esc

# P7b: a remote destination is refused, and the prompt says so before Enter rather
# than letting the user commit to a write that cannot succeed.
send "w" 0.25
clear_line
send "az://container/out.csv" 0.30
assert_contains "P/remote-warning-az" "az:// is not writable"
enter 0.50
assert_contains "P/remote-refused-az" "export writes local files only"
send "w" 0.25
clear_line
send "s3://bucket/out" 0.30
assert_contains "P/remote-warning-s3" "s3:// is not writable"
esc

# P8: the key is discoverable in the help popup.
send "?" 0.30
assert_contains "P/help-export" "Write the current view to a CSV file"
esc
quit

# P9: the shortcut bar still advertises Export in the states the feature exists for —
# a filtered view and a sorted one, both of which take an earlier match arm.
start_app "tests/fixtures/orders.csv"
send "llll" 0.20   # order_id → region
send "f" 0.20
send "North" 0.20
enter 0.35
assert_contains "P/filtered-bar-has-w" "Export"
send "s" 0.30      # add a sort on top
assert_contains "P/sorted-bar-has-w" "Export"
quit

# P10: a failed export must not destroy the file it was overwriting. CsvWriter emits
# the header before rejecting a nested column, so an in-place write would truncate.
printf 'keep,me\n1,2\n' > "$QA_TMP/precious.csv"
start_app "$QA_TMP/nested.ndjson"
send "w" 0.25
clear_line
send "$QA_TMP/precious.csv" 0.30
assert_contains "P/nested-overwrite-warning" "overwrites"
enter 0.50
assert_contains  "P/nested-write-fails"     "Export failed"
assert_file_head "P/nested-target-survives" "$QA_TMP/precious.csv" "keep,me"
assert_file_lines "P/nested-target-intact"  "$QA_TMP/precious.csv" 2
quit

# P11: export works from the browse viewer pane. A capital T in the path must reach
# the prompt instead of opening the theme picker — browse mode intercepts T unless
# the viewer reports it is taking text input.
start_app "browse tests/fixtures/"
enter 0.45   # opens orders.csv; focus lands on the viewer
send "w" 0.30
assert_contains "P/browse-prompt" "orders.export.csv"
clear_line
send "$QA_TMP/Totals.csv" 0.30
assert_contains     "P/browse-typed-capital-T" "Totals.csv"
assert_not_contains "P/browse-no-theme-picker" "nord"
enter 0.55
assert_contains   "P/browse-write-status" "Wrote 100 rows"
assert_file_lines "P/browse-write-lines"  "$QA_TMP/Totals.csv" 101
send "q" 0.20

# ── Suite X: browse subcommand ────────────────────────────────────────────────
echo ""
echo "=== Suite X: browse subcommand ==="

# X1: browse opens with fixture directory — browser pane visible
start_app "browse tests/fixtures/"
assert_contains "X1/browser-pane-visible" "orders.csv"
quit

# X2: open a file from the browser — viewer pane appears
start_app "browse tests/fixtures/"
# Press Enter to open whichever file is selected at cursor
enter 0.4
assert_contains "X2/viewer-loads-file" "order_id"
send "q" 0.20

# X3: ctrl-e toggles the browser pane off
# Use "wide.csv" as the sentinel — it lives in the directory listing but is NOT
# the file that opens (orders.csv is first alphabetically), so it only appears
# when the browser pane is actually visible.
start_app "browse tests/fixtures/"
enter 0.4  # open a file first
tmux send-keys -t "$APP_PANE" C-e; sleep 0.30
assert_not_contains "X3/browser-hidden" "wide.csv"
send "q" 0.20

# X4: ctrl-e toggles back on
# Use "wide.csv" — present only in the browser listing, not in viewer title.
start_app "browse tests/fixtures/"
enter 0.4
tmux send-keys -t "$APP_PANE" C-e; sleep 0.30
tmux send-keys -t "$APP_PANE" C-e; sleep 0.30
assert_contains "X4/browser-shown-again" "wide.csv"
send "q" 0.20

# X5: tab brings focus back to the browser.
#
# "Find" is on the shortcut bar only while the browser pane holds focus, so it is
# what proves the switch happened. This case used to send ctrl-h and assert on
# "wide.csv", which passed for the wrong reason twice over: there is no ctrl-h
# binding (tab is the toggle), and the listing is on screen under either focus.
start_app "browse tests/fixtures/"
enter 0.4  # open file, focus moves to the viewer
assert_not_contains "X5/viewer-focused" "Find"
key Tab 0.20
assert_contains "X5/browser-focused" "Find"
send "q" 0.20

# X6: browse with no path arg opens current dir
# The cd is part of the launched command, not bare keystrokes: `launch` only sends
# it once the shell has proved it is reading input, and every launch respawns the
# pane back to the repo root afterwards, so no cleanup cd is needed.
REPO_ROOT=$(git rev-parse --show-toplevel)
# $BINARY is relative, so it cannot survive the cd — resolve it against the root.
launch "cd $REPO_ROOT/tests/fixtures && $REPO_ROOT/target/debug/datasight browse"
assert_contains "X6/browse-cwd-default" "orders.csv"
send "q" 0.20

# Reset cwd to repo root — X6 leaves it wherever its last `cd` landed, and $BINARY
# is a relative path.
tmux send-keys -t "$APP_PANE" C-c; sleep 0.10
tmux send-keys -t "$APP_PANE" C-u; sleep 0.05
tmux send-keys -t "$APP_PANE" "cd $REPO_ROOT" Enter; sleep 0.20

# X7: d on a local file is refused — it is already on disk, so no prompt opens.
# (The cloud download itself needs az://|s3:// credentials and is covered by the
# unit tests in src/browser/download.rs against a stub backend.)
start_app "browse tests/fixtures/"
send "d" 0.30
assert_contains "X7/local-download-refused" "already a local file"
assert_contains "X7/shortcut-bar-intact" "Navigate"
send "q" 0.20

# X8: the Download hint is offered only for cloud listings. Assert the bar is on
# screen first — "no Download" is also true of a pane with no app running.
start_app "browse tests/fixtures/"
assert_contains "X8/shortcut-bar-present" "ctrl-e"
assert_not_contains "X8/no-download-hint-local" "Download"
send "q" 0.20

# X9: d on a directory is refused — the repo root lists dirs first, so the cursor
# starts on one.
start_app "browse ."
send "d" 0.30
assert_contains "X9/dir-download-refused" "not directories"
send "q" 0.20

# X10: / opens the find prompt — it takes over the bottom bar, so the prompt's
# own hints are the only exit sign on screen.
start_app "browse tests/fixtures/"
send "/" 0.20
assert_contains "X10/find-prompt-open" "Esc cancel"
esc
send "q" 0.20

# X11: typing narrows the listing to the matches. "wide" is a subsequence of no
# other fixture name.
start_app "browse tests/fixtures/"
send "/" 0.20
send "wide" 0.30
assert_contains     "X11/match-kept"    "wide.csv"
assert_not_contains "X11/rest-dropped"  "orders.csv"

# X12: Esc closes the prompt and restores the full listing (same session — this
# is what the user does after finding the file they wanted).
esc
assert_contains "X12/listing-restored" "orders.csv"
assert_contains "X12/shortcut-bar-back" "Find"
send "q" 0.20

# X13: Enter opens the top match straight out of the prompt.
start_app "browse tests/fixtures/"
send "/" 0.20
send "wide" 0.30
enter 0.6
# The header is clipped to the width of the viewer sub-pane, so match the stem
# only — it is still unique to wide.csv, which is what the case is proving.
assert_contains "X13/enter-opens-the-match" "very_long_colum"
send "q" 0.20

# X14: while the prompt is open the browse bindings are query text — q must not
# quit, d must not act, T must not open the theme picker. None of the three is a
# subsequence of any fixture name, so the prompt reports no matches instead.
start_app "browse tests/fixtures/"
send "/" 0.20
send "qdT" 0.30
assert_contains     "X14/prompt-survives-qdT" "no matches"
assert_not_contains "X14/d-not-handled"       "already a local file"
assert_not_contains "X14/T-not-handled"       "nord"
esc
assert_contains "X14/listing-restored" "orders.csv"
send "q" 0.20

# X15: q quits from the browser pane with a file already open — the bug this
# case exists to pin. The binding used to be gated on "no viewer loaded", which
# left the browser pane with no way out at all.
start_app "browse tests/fixtures/"
enter 0.5      # open a file; focus moves to the viewer
key Tab 0.20   # back to the browser pane
send "q" 0.30
assert_exited "X15/q-quits-from-the-browser-pane"

# X16: ctrl-n walks the matches — j/k are query text in the prompt. "sample"
# scores sample.txt, sample.json and sample_binary.png equally (one consecutive
# run from the start), so the shorter-name tie-break puts sample.txt on top and
# one step down lands on sample.json.
start_app "browse tests/fixtures/"
send "/" 0.20
send "sample" 0.30
tmux send-keys -t "$APP_PANE" C-n; sleep 0.20
enter 0.6
assert_contains "X16/second-match-opened" "pretty JSON"
send "q" 0.20

# ── Suite Y: Theme picker ─────────────────────────────────────────────────────
echo ""
echo "=== Suite Y: Theme picker ==="

# Start clean — remove any persisted state from a prior run.
STATE_FILE="${HOME}/.config/datasight/state.toml"
rm -f "$STATE_FILE"

# Reset cwd to repo root in case earlier suites left it elsewhere.
tmux send-keys -t "$APP_PANE" C-c; sleep 0.10
tmux send-keys -t "$APP_PANE" C-u; sleep 0.05
tmux send-keys -t "$APP_PANE" "cd $REPO_ROOT" Enter; sleep 0.20

# Y1: T opens the picker; the popup lists Base16 themes
start_app "tests/fixtures/orders.csv"
send "T" 0.60
assert_contains "Y1/picker-title" "Theme"
assert_contains "Y1/picker-list"  "nord"

# Y2: Esc cancels the picker — popup gone, no state file written
esc
assert_not_contains "Y2/picker-closed" "nord"
if [ ! -f "$STATE_FILE" ]; then
  echo "  PASS [Y2/no-state-after-cancel]"
  PASS=$((PASS + 1))
else
  echo "  FAIL [Y2/no-state-after-cancel] — state.toml written despite Esc"
  FAIL=$((FAIL + 1))
  FAILURES+=("[Y2/no-state-after-cancel] state.toml exists after cancel")
fi
quit

# Y3: Re-open picker, navigate down once, Enter persists the choice
start_app "tests/fixtures/orders.csv"
send "T" 0.60
send "j" 0.15
enter 0.30
assert_contains "Y3/picker-closed" "order_id"
quit

# Y4: State file exists and contains a theme name
if [ -f "$STATE_FILE" ] && grep -q '^theme *=' "$STATE_FILE"; then
  echo "  PASS [Y4/state-persisted]"
  PASS=$((PASS + 1))
else
  echo "  FAIL [Y4/state-persisted] — expected theme= in $STATE_FILE"
  FAIL=$((FAIL + 1))
  FAILURES+=("[Y4/state-persisted] state file missing or malformed")
fi

# Y5: T also opens the picker in browse mode
start_app "browse tests/fixtures/"
send "T" 0.60
assert_contains "Y5/browse-picker-open" "Theme"
esc
send "q" 0.20

# Reset theme state so the next QA run (and the dev's normal use) starts fresh.
rm -f "$STATE_FILE"

# ── Suite Z: Text viewer ──────────────────────────────────────────────────────
echo ""
echo "=== Suite Z: Text viewer ==="

# Browser listing in tests/fixtures/ (alphabetical):
#   0 orders.csv  1 orders.json  2 orders.ndjson  3 orders.parquet
#   4 orders.tsv  5 orders_nulls.csv  6 sample.json  7 sample.txt
#   8 sample_binary.png  9 wide.csv

# Z1: open sample.txt — text viewer renders content with UTF-8 title marker
start_app "browse tests/fixtures/"
send "jjjjjjj" 0.30   # cursor → sample.txt
enter 0.40
assert_contains "Z1/text-content"   "Datasight text viewer fixture"
assert_contains "Z1/title-utf8"     "UTF-8"
assert_contains "Z1/title-lines"    "lines"
quit

# Z2: j scrolls down, k scrolls back up — content remains rendered
start_app "browse tests/fixtures/"
send "jjjjjjj" 0.30
enter 0.40
send "jjj" 0.20
send "kkk" 0.20
assert_contains "Z2/scroll-roundtrip" "Datasight text viewer fixture"
quit

# Z3: G then gg returns to top of file (covered by unit tests; this is a
# smoke check that the keys don't crash the viewer)
start_app "browse tests/fixtures/"
send "jjjjjjj" 0.30
enter 0.40
send "G" 0.30
send "gg" 0.30
assert_contains "Z3/back-to-top" "Datasight text viewer fixture"
quit

# Z4: / opens search; typing a query shows match count; Enter jumps; n cycles
start_app "browse tests/fixtures/"
send "jjjjjjj" 0.30
enter 0.40
send "/" 0.20
send "needle" 0.30
# Still in Search mode — bottom status row reports match count.
assert_contains "Z4/match-count" "match"
key Enter 0.30
# Confirm scrolled to first occurrence of the token.
assert_contains "Z4/jumped-to-needle" "needle"
send "n" 0.20
assert_contains "Z4/cycle-still-on-match" "needle"
quit

# Z5: L toggles line numbers (rendering still succeeds, content still visible)
start_app "browse tests/fixtures/"
send "jjjjjjj" 0.30
enter 0.40
send "L" 0.20
assert_contains "Z5/text-still-visible" "Datasight text viewer"
send "L" 0.20
assert_contains "Z5/text-after-retoggle" "Datasight text viewer"
quit

# Z6: w toggles word wrap (rendering survives the toggle)
start_app "browse tests/fixtures/"
send "jjjjjjj" 0.30
enter 0.40
send "w" 0.20
assert_contains "Z6/wrap-off-content" "Datasight text viewer"
send "w" 0.20
assert_contains "Z6/wrap-on-content"  "Datasight text viewer"
quit

# Z7: non-tabular .json falls through to text viewer with pretty-print
start_app "browse tests/fixtures/"
send "jjjjjj" 0.30   # cursor → sample.json
enter 0.50
# Pretty-printed JSON spans multiple lines; the title flags it as such.
assert_contains "Z7/json-pretty-title"   "pretty JSON"
assert_contains "Z7/json-pretty-content" "version"
quit

# Z8: opening a binary file shows a status message and does not load a viewer
start_app "browse tests/fixtures/"
send "jjjjjjjj" 0.30  # cursor → sample_binary.png
enter 0.40
assert_contains "Z8/binary-status" "Cannot preview"
# Browser still has focus and no viewer loaded — q quits cleanly.
send "q" 0.20

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
