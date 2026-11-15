# Agents and scripts

datasight can be driven from another terminal. While you look at a table, an AI agent
or a script can read what's on your screen and change it: add a filter, sort a column,
move the cursor, save the view. You see each change as it happens.

> [!NOTE]
> This is new and ships in the release after v0.10.1. Until then, build from the
> latest code: `cargo install --git https://github.com/SpollaL/datasight`

## Try it

Open a file in one terminal:

```bash
datasight orders.csv
```

and in a second terminal, run:

```bash
datasight ctl state
datasight ctl filter '{"column":"region","query":"= North"}'
datasight ctl rows '{"limit":5}'
```

The first command prints the columns and what's currently applied. The second
filters the table in the first terminal, and the status bar there says
`✓ ctl: filter region = North`. The third prints the first five rows that are left,
as JSON.

## Use it with Claude Code

datasight comes with a skill that teaches Claude Code how to use `ctl`. Install it once:

```bash
mkdir -p ~/.claude/skills/datasight
datasight --skill > ~/.claude/skills/datasight/SKILL.md
```

Now keep datasight open in one pane and Claude Code in another, and ask about what
you're looking at:

> *Which region has the most orders over 500? Filter the table so I can see them.*

Claude reads the columns, filters your table, and answers from the rows it finds.
The skill tells it to change your view only when you ask. Reading the table is
always allowed.

Re-run the `--skill` command after upgrading datasight to pick up any changes.

## What it can do

| Command | Does |
|---|---|
| `state` | Show the file, columns and their types, filters, sorts, and cursor |
| `rows '{"offset":0,"limit":50}'` | Return rows of the current view, up to 1,000 at a time |
| `filter '{"column":"region","query":"= North"}'` | Add a filter, like pressing <kbd>f</kbd>. Filters add up |
| `clear_filters` | Remove every filter |
| `sort '{"column":"amount","direction":"desc"}'` | Sort `asc` or `desc`, or `none` to drop that column's sort |
| `goto '{"row":0,"column":"region"}'` | Move the cursor. Either part can be left out |
| `export '{"path":"/tmp/view.csv"}'` | Save the current view as CSV (needs a full path) |

Filters use the same conditions as the <kbd>f</kbd> key: see
[Finding rows](finding-rows.md#filter-keep-only-matching-rows). For the full
reference, run `datasight ctl --help`.

## Answers and errors

A successful command prints JSON and exits with 0. Commands that change the table
print the new `state`.

A failed command prints `{"code": …, "message": …}` to stderr and exits with 1, so
scripts can check it. The code you're most likely to see is **`busy`**: you were in
the middle of typing (a filter, a search, a file name), and datasight won't change
the table under you. Wait a second and try again.

## More than one datasight open

With one datasight running, `ctl` finds it by itself. With several, say which one:

```bash
datasight ctl --socket /run/user/1000/datasight/48213.sock state
# or
export DATASIGHT_SOCKET=/run/user/1000/datasight/48213.sock
```

Each running datasight has a socket named after its process ID, `<pid>.sock`. It
lives in `$XDG_RUNTIME_DIR/datasight/` on Linux and `~/Library/Caches/datasight/` on
macOS. Only your user can open it.

Driving datasight works on Linux and macOS. On Windows, datasight runs normally but
can't be driven this way.
