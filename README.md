# science.me-sources

Raw captures of public data sources for [science.me](https://github.com/queelius/science.me), kept with their provenance so that later revisions never overwrite what was published earlier.

science.me scores frozen programs only on data published after they were frozen, so it needs to know what a source said and when. Publishers revise their numbers, and most keep only the latest version. This repository keeps every version it sees.

## What is captured

| Source | Dataset | Released |
|---|---|---|
| `nhsn-hrd` | [CDC NHSN Weekly Hospital Respiratory Data by jurisdiction](https://data.cdc.gov/Public-Health-Surveillance/Weekly-Hospital-Respiratory-Data-HRD-Metrics-by-Ju/ua7e-t2fy) | Fridays |
| `nhsn-hrd-preliminary` | [The same metrics, preliminary](https://data.cdc.gov/Public-Health-Surveillance/Weekly-Hospital-Respiratory-Data-HRD-Metrics-by-Ju/mpgq-jmmr) | Wednesdays |

Both keep the same columns: week, jurisdiction, influenza, COVID-19 and RSV admissions, and how many hospitals reported each. The full list is in `sources.json`. Capture started on 2026-10-29. Earlier vintages of the same data are archived by [Delphi Epidata](https://cmu-delphi.github.io/delphi-epidata/api/v5-signals/nhsn.html).

## How it works

`capture.py` runs every hour from GitHub Actions. For each source it reads the dataset's metadata, its rows, its published row count, then its metadata again. It gives up until the next run if:

- the source changed in between,
- the server marks a response out of date,
- the rows and the count were served from different versions,
- the rows are not well-formed CSV with one field per declared column, or
- there are not exactly as many rows as the count says.

If the rows are the same as the last capture, it stores nothing. Otherwise it stores the rows and the metadata gzipped and untouched under `captures/<source>/`, and appends one line to `captures/<source>/log.jsonl` with:

- the request URLs and the response headers,
- the SHA-256 of each response,
- `retrieved_at`, when this puller first saw the version,
- `rows_updated_at`, when the publisher says the rows changed,
- `data_last_modified`, the version the rows were served from.

If the push fails, the workflow keeps the new captures as an artifact of the run for 90 days.

The last two are kept apart on purpose. A program is eligible for an origin only if it was frozen before the publisher released the data, not before this puller happened to fetch it.

Files are written once and never replaced. The log is append-only. Git history, pushed to GitHub, timestamps each commit.

## What this is not

There are no transformations here. Turning a capture into a table that programs can see is a versioned program that lives with science.me, so it can be reviewed like any other.

## Running it

```
uv sync
uv run pytest
python -I capture.py          # captures into captures/
```

It has no dependencies beyond the Python standard library.

## License

The code is MIT. The NHSN data is a work of the U.S. government and in the public domain.
