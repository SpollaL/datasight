# Files and the cloud

`datasight browse` opens a file browser beside the table. Pick a file and it opens on
the right. Pick another and it replaces the first. No need to quit and restart.

```bash
datasight browse                    # the current directory
datasight browse ~/data/exports     # a local folder
datasight browse az://my-container  # Azure Blob Storage
datasight browse s3://my-bucket     # Amazon S3
```

## Getting around

| Key | Does |
|---|---|
| <kbd>j</kbd> / <kbd>k</kbd> | Move through the list |
| <kbd>Enter</kbd> / <kbd>.</kbd> | Open the file, or go into the folder |
| <kbd>Esc</kbd> | Up to the parent folder |
| <kbd>Tab</kbd> | Switch between the list and the open file |
| <kbd>ctrl</kbd>+<kbd>e</kbd> | Hide or show the list, to give the table the full width |
| <kbd>/</kbd> | Find a file by name |
| <kbd>d</kbd> | Download the selected cloud file |
| <kbd>T</kbd> | Change theme |
| <kbd>q</kbd> | Quit |

Once you <kbd>Tab</kbd> into an open table, every key from the rest of this manual works
as usual.

## Find a file

Press <kbd>/</kbd> and start typing. The letters only have to appear in order, so
`ordcsv` finds `orders.csv`. Best matches come first, and the matching letters are
highlighted.

- Lowercase ignores case. One capital letter makes the whole search case-sensitive.
- <kbd>↑</kbd> / <kbd>↓</kbd> (or <kbd>ctrl</kbd>+<kbd>p</kbd> / <kbd>ctrl</kbd>+<kbd>n</kbd>)
  move through the matches.
- <kbd>Enter</kbd> opens the highlighted match.
- <kbd>ctrl</kbd>+<kbd>u</kbd> clears what you typed. <kbd>Esc</kbd> brings the full
  list back.

## What opens where

- **Data files** (`.csv`, `.tsv`, `.parquet`, `.json`, `.ndjson`, `.jsonl`) open as a
  table.
- **Binary files** (images, archives, executables, PDFs and the like) are greyed out
  and won't open.
- **Everything else** (`.txt`, `.md`, `.log`, `.yaml`, files with no extension) opens
  in a plain text viewer.

The text viewer has its own small set of keys:

| Key | Does |
|---|---|
| <kbd>j</kbd> / <kbd>k</kbd>, <kbd>PageDown</kbd> / <kbd>PageUp</kbd> | Scroll |
| <kbd>g</kbd><kbd>g</kbd> / <kbd>G</kbd> | Top / bottom |
| <kbd>h</kbd> / <kbd>l</kbd> | Scroll sideways (when wrapping is off) |
| <kbd>w</kbd> | Wrap long lines on or off |
| <kbd>L</kbd> | Line numbers on or off |
| <kbd>/</kbd>, <kbd>n</kbd> / <kbd>N</kbd> | Search, next, previous |

## Download a cloud file

On a file in an `az://` or `s3://` listing, press <kbd>d</kbd>. You'll be asked where to
save it, with the file's own name already filled in.

- Type a folder ending in `/`, like `~/Downloads/`, to keep the original name.
- The full destination is shown as you type, with a warning if a file is already
  there.
- Missing folders are created.
- <kbd>Enter</kbd> saves and <kbd>Esc</kbd> cancels.

If a download fails, no half-written file is left behind. <kbd>d</kbd> does nothing on
local files, since they're already on your disk.

## Cloud storage

Cloud support has to be built in. See [Install](install.md#with-cloud-storage). Then
datasight logs in using environment variables.

**Azure**: either a connection string:

```bash
export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=…;AccountKey=…"
```

or an account name and key:

```bash
export AZURE_STORAGE_ACCOUNT_NAME=myaccount
export AZURE_STORAGE_ACCOUNT_KEY=…
```

**Amazon S3**: the standard AWS variables:

```bash
export AWS_ACCESS_KEY_ID=…
export AWS_SECRET_ACCESS_KEY=…
export AWS_REGION=eu-west-1
```

Cloud files are downloaded into memory when you open them. Nothing is written to
disk unless you press <kbd>d</kbd>.
