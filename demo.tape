Output .github/assets/demo.gif

Set FontSize 13
Set Width 1400
Set Height 700
Set Theme "Catppuccin Mocha"
Set PlaybackSpeed 1
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

# Open the app
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

# Move to customer_name column before searching
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

# Navigate right to the "status" column (8 more rights, already at col 3)
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

# Sort descending
Type "s"
Sleep 600ms
Type "s"
Sleep 600ms

# Show stats bar
Type "S"
Sleep 1s

# Clear filter and stats
Type "F"
Sleep 500ms
Type "S"
Sleep 800ms

# Column info view
Type "i"
Sleep 1s
Down
Sleep 300ms
Down
Sleep 300ms
Down
Sleep 300ms
Escape
Sleep 600ms

# Unique values on current column
Type "u"
Sleep 1s
Down
Sleep 300ms
Down
Sleep 300ms
Escape
Sleep 600ms

# Plot: pick y=total_amount, x=order_date
# navigate to total_amount col first (col 9)
Left
Sleep 100ms
Left
Sleep 100ms
Left
Sleep 200ms
Type "p"
Sleep 600ms
# pick x column (go left to order_date at col 1)
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
Sleep 100ms
Left
Sleep 100ms
Left
Sleep 200ms
Enter
Sleep 1s
Type "t"
Sleep 800ms
Type "t"
Sleep 800ms
Escape
Sleep 1s

# End on the app view — pause then quit
Sleep 1s
Hide
Type "q"
