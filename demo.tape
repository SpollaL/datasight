Output .github/assets/demo.gif

Set FontSize 13
Set Width 1400
Set Height 700
Set Theme "Catppuccin Mocha"
Set PlaybackSpeed 0.75
Set TypingSpeed 60ms
Set Shell bash
Env PATH "/home/spolla-l/workspace/datasight/target/release:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

# cd silently so only clean command is visible
Hide
Type "cd /home/spolla-l/workspace/datasight"
Enter
Sleep 300ms
Ctrl+L
Sleep 100ms
Show

# ── Phase 1: viewer ───────────────────────────────────────────────────────────

Type "datasight tests/fixtures/orders.csv"
Sleep 500ms
Enter
Sleep 2s

# Autofit all columns
Type "="
Sleep 800ms

# Navigate down a few rows
Down
Sleep 150ms
Down
Sleep 150ms
Down
Sleep 150ms
Down
Sleep 150ms
Down
Sleep 300ms

# Move to customer_name column (col 3)
Right
Sleep 100ms
Right
Sleep 100ms
Right
Sleep 200ms

# Search
Type "/"
Sleep 400ms
Type "Alice"
Sleep 1s
Enter
Sleep 600ms
Type "n"
Sleep 600ms
Type "n"
Sleep 600ms
Escape
Sleep 600ms

# Navigate right to status column (col 11, 8 more rights from col 3)
Right
Sleep 100ms
Right
Sleep 100ms
Right
Sleep 100ms
Right
Sleep 100ms
Right
Sleep 100ms
Right
Sleep 100ms
Right
Sleep 100ms
Right
Sleep 200ms

# Filter on status column
Type "f"
Sleep 400ms
Type "= Completed"
Sleep 800ms
Enter
Sleep 1s

# Sort ascending then descending
Type "s"
Sleep 600ms
Type "s"
Sleep 600ms

# Show stats bar
Type "e"
Sleep 1s

# Clear filter, sort, stats
Type "F"
Sleep 400ms
Type "S"
Sleep 400ms
Type "e"
Sleep 600ms

# ── NEW: Theme picker ─────────────────────────────────────────────────────────

Type "T"
Sleep 600ms
Down
Sleep 220ms
Down
Sleep 220ms
Down
Sleep 220ms
Down
Sleep 220ms
Down
Sleep 220ms
Down
Sleep 700ms
Enter
Sleep 900ms

# ── Plot with dual Y axis ─────────────────────────────────────────────────────

# Navigate to total_amount (col 9, 2 lefts from col 11)
Left
Sleep 100ms
Left
Sleep 200ms

# Enter PlotPickY — total_amount pre-selected as Y1
Type "p"
Sleep 600ms

# Navigate left 2 to quantity (col 7), add to Y2 axis
Left
Sleep 150ms
Left
Sleep 300ms
Type "2"
Sleep 700ms

# Confirm Y selection, move to PlotPickX
Enter
Sleep 500ms

# Navigate left 6 to order_date (col 1)
Left
Sleep 100ms
Left
Sleep 100ms
Left
Sleep 100ms
Left
Sleep 100ms
Left
Sleep 100ms
Left
Sleep 300ms

# Confirm X — renders dual-panel plot
Enter
Sleep 1200ms

# Toggle plot type
Type "t"
Sleep 900ms
Type "t"
Sleep 900ms

Escape
Sleep 800ms

# Quit viewer, clear screen
Hide
Type "q"
Enter
Sleep 300ms
Ctrl+L
Sleep 100ms
Show

# ── Phase 2: browse mode ──────────────────────────────────────────────────────

Type "datasight browse tests/fixtures"
Sleep 500ms
Enter
Sleep 2s

# Navigate down to orders.parquet (index 3)
Down
Sleep 220ms
Down
Sleep 220ms
Down
Sleep 300ms

# Open orders.parquet — viewer pane appears and auto-focuses
Enter
Sleep 2500ms

# Autofit in viewer pane
Type "="
Sleep 800ms

# Navigate a few rows
Down
Sleep 200ms
Down
Sleep 200ms
Down
Sleep 300ms

# Switch back to browser pane
Tab
Sleep 700ms

# Hide sidebar — full-width viewer
Ctrl+E
Sleep 1400ms

# Bring sidebar back
Ctrl+E
Sleep 800ms

# Switch to viewer and quit
Tab
Sleep 300ms
Hide
Type "q"
