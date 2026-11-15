# Opening data

## A file

```bash
datasight sales.csv
datasight events.parquet
```

datasight goes by the file extension:

| Extension | Read as |
|---|---|
| `.csv` | Comma-separated |
| `.tsv` | Tab-separated |
| `.parquet` | Parquet |
| `.json` | A JSON array of objects: `[{"a": 1}, {"a": 2}]` |
| `.ndjson`, `.jsonl` | One JSON object per line |

A file with no extension is read like piped input (below).

## Other separators

Use `-d` for anything that isn't a comma. It also opens files with any other
extension as delimited text:

```bash
datasight -d ';' european.csv
datasight -d '|' export.psv
datasight -d '\t' data.txt
```

## Piping data in

Leave the file name off and datasight reads whatever you pipe to it:

```bash
curl -s https://api.example.com/orders | datasight
gzip -dc sales.csv.gz | datasight
psql -c "\copy (SELECT * FROM orders) TO STDOUT CSV HEADER" | datasight
```

It looks at the first character to work out the format: `[` means JSON, `{` means
NDJSON, and anything else is treated as CSV. Piped tab-separated data needs `-d '\t'`,
because there's no extension to say so.

You can also pass a command's output as if it were a file:

```bash
datasight <(head -n 1000 huge.csv)
```

## Dates

Text columns that hold dates are turned into real dates automatically, so they sort
and chart properly. Recognised formats:

| Example | Format |
|---|---|
| `2024-03-15` | ISO |
| `2024/03/15` | Year first, slashes |
| `03/15/2024` | US style |
| `03-15-2024` | US style, dashes |
| `15-Mar-2024` | Day, month name, year |
| `15 Mar 2024` | Same, with spaces |

A column is only converted when *every* value matches. Something like `03/04/2024`
could mean 3 April or 4 March. If every value in a column could be read either way,
datasight leaves it as text rather than guess.

## How big a file?

The whole file is loaded into memory. A 1-million-row, 42 MB CSV opens in under a
tenth of a second and uses about 115 MB of RAM. After that, scrolling and filtering
stay fast no matter how many rows there are.
