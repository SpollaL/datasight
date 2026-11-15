---
name: datasight
description: "Read and drive a datasight table the user has open in another terminal pane. Use when the user mentions datasight, or asks about the table, rows or filters they are looking at in it."
---

# datasight

The user is looking at a table in datasight. `datasight ctl` reads and changes what they see.

1. Run `datasight ctl state` first. It gives the columns and their types; use those exact
   names. "no running datasight found" means there is nothing to drive: say so and stop.
   With several sessions, pick one with `--socket`: the socket is named `<pid>.sock`, and
   `herdr pane process-info --pane ID` lists datasight's pid under `foreground_processes`
   (not `shell_pid`).
2. Run `datasight ctl --help` for the methods and the filter syntax before the first filter.
3. It is the user's screen: change filters, sorts or the cursor only when they asked.
   Reading (`state`, `rows`) is always fine.
4. `busy` means the user is typing in a prompt: wait a second and retry once, then tell them.
5. For more than 1,000 rows, `export` to an absolute path and read that file instead of
   paging.
