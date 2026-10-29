"""Raw captures of public data sources, with provenance, for science.me.

Each source is a Socrata dataset declared in `sources.json`: a domain, a dataset id, the columns kept and
the order that makes the response deterministic.  A capture fetches the dataset's metadata, its rows, its
published row count, then its metadata again.  It is refused, and the next run tries again, when the source
updated in between, when the server marks a response out of date, when the rows and the count come from
different versions, or when the rows are not a well-formed table of exactly that count.  When the rows hash
to what was last stored, nothing is written.  Otherwise both responses are stored
gzipped, untouched, under `captures/<source id>/<retrieved at>.*.gz`, and one line is appended to that
directory's `log.jsonl`: the URLs, the response headers, the hashes, the retrieval time, and the publisher's
own `rowsUpdatedAt`.  Retrieval time is when this puller first saw a vintage; `rowsUpdatedAt` is when the
publisher says it changed.  The two are kept apart because eligibility is decided by the second.

Files are written once and never replaced, and the log is only appended to.
"""

from __future__ import annotations

import argparse
import csv
import email.utils
import gzip
import hashlib
import io
import json
import sys
import urllib.parse
import urllib.request
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

REPO = Path(__file__).resolve().parent
SOURCES_FILE = REPO / "sources.json"
CAPTURES_DIR = REPO / "captures"
LOG_FILE = "log.jsonl"
# The most rows one request asks for.  A response that reaches it may have been cut short, so it is refused.
ROW_LIMIT = 1_000_000
TIMEOUT_S = 300
USER_AGENT = "science.me-sources (https://github.com/queelius/science.me-sources)"


class CaptureError(RuntimeError):
    """A source could not be captured consistently, or is declared wrongly."""


@dataclass(frozen=True)
class Source:
    """A Socrata dataset: `dataset` on `domain`, its `columns` kept, its rows sorted by `order`."""

    id: str
    domain: str
    dataset: str
    columns: tuple[str, ...]
    order: tuple[str, ...]


@dataclass(frozen=True)
class Response:
    """An HTTP response body and its headers, in the order served."""

    body: bytes
    headers: tuple[tuple[str, str], ...]


Fetch = Callable[[str], Response]


def load_sources(path: Path) -> list[Source]:
    """The sources declared in `path`.  CaptureError when a source orders by a column it does not keep."""
    sources = []
    for item in json.loads(path.read_text(encoding="utf-8"))["sources"]:
        source = Source(
            id=item["id"],
            domain=item["domain"],
            dataset=item["dataset"],
            columns=tuple(item["columns"]),
            order=tuple(item["order"]),
        )
        if not set(source.order) <= set(source.columns):
            raise CaptureError(f"{source.id}: its order {source.order} names columns it does not keep")
        sources.append(source)
    return sources


def data_url(source: Source) -> str:
    query = urllib.parse.urlencode(
        {"$select": ",".join(source.columns), "$order": ",".join(source.order), "$limit": ROW_LIMIT}
    )
    return f"https://{source.domain}/resource/{source.dataset}.csv?{query}"


def count_url(source: Source) -> str:
    return f"https://{source.domain}/resource/{source.dataset}.json?" + urllib.parse.urlencode({"$select": "count(*) as n"})


def metadata_url(source: Source) -> str:
    return f"https://{source.domain}/api/views/{source.dataset}.json"


def fetch(url: str) -> Response:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=TIMEOUT_S) as response:
        return Response(body=response.read(), headers=tuple(response.headers.items()))


def sha256(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


def _iso(moment: datetime) -> str:
    return moment.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _header(source: Source, response: Response, name: str) -> str:
    """The value of the header `name` (any case).  CaptureError when the response lacks it."""
    for key, value in response.headers:
        if key.lower() == name.lower():
            return value
    raise CaptureError(f"{source.id}: the response has no {name} header")


def _version(source: Source, response: Response) -> datetime:
    """The version of the data a Socrata response was served from.  CaptureError when the server marks it
    out of date, or does not say."""
    if _header(source, response, "X-SODA2-Data-Out-Of-Date").lower() != "false":
        raise CaptureError(f"{source.id}: the server marks the response out of date")
    return email.utils.parsedate_to_datetime(_header(source, response, "X-SODA2-Truth-Last-Modified"))


def _rows(source: Source, body: bytes) -> int:
    """The number of data rows in the CSV `body`.  CaptureError unless it parses strictly, its header is the
    declared columns, every row has one field per column, and it stops short of the row limit."""
    reader = csv.reader(io.StringIO(body.decode("utf-8")), strict=True)
    try:
        if next(reader, None) != list(source.columns):
            raise CaptureError(f"{source.id}: the response's header is not the declared columns {source.columns}")
        rows = 0
        for row in reader:
            if len(row) != len(source.columns):
                raise CaptureError(f"{source.id}: row {rows + 1} has {len(row)} fields, not {len(source.columns)}")
            rows += 1
    except csv.Error as e:
        raise CaptureError(f"{source.id}: the response is not well-formed CSV: {e}") from e
    if rows >= ROW_LIMIT:
        raise CaptureError(f"{source.id}: {rows} rows reaches the row limit {ROW_LIMIT}; it may be cut short")
    return rows


def _last_entry(log: Path) -> dict | None:
    if not log.exists():
        return None
    lines = log.read_text(encoding="utf-8").splitlines()
    return json.loads(lines[-1]) if lines else None


def _store(path: Path, body: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as f:
        f.write(gzip.compress(body, mtime=0))


def capture_source(source: Source, root: Path, now: datetime, fetch: Fetch = fetch) -> dict | None:
    """Capture `source` into `root/<source.id>/` and return the log entry, or None when its rows are unchanged.

    CaptureError when the source updated while it was read, or its response is not the declared table.
    FileExistsError when a capture at the same second already exists.
    """
    url, meta_url, n_url = data_url(source), metadata_url(source), count_url(source)
    before = fetch(meta_url)
    data = fetch(url)
    counted = fetch(n_url)
    after = fetch(meta_url)
    updated = json.loads(after.body)["rowsUpdatedAt"]
    if json.loads(before.body)["rowsUpdatedAt"] != updated:
        raise CaptureError(f"{source.id}: the source updated while it was read; the next run tries again")
    version = _version(source, data)
    if _version(source, counted) != version:
        raise CaptureError(f"{source.id}: the rows and their count were served from different versions")
    rows = _rows(source, data.body)
    published = int(json.loads(counted.body)[0]["n"])
    if rows != published:
        raise CaptureError(f"{source.id}: {rows} rows received, but the published count is {published}")

    directory = root / source.id
    log = directory / LOG_FILE
    last = _last_entry(log)
    digest = sha256(data.body)
    if last is not None and last["data"]["sha256"] == digest:
        return None

    stamp = now.astimezone(UTC).strftime("%Y-%m-%dT%H%M%SZ")
    data_file, meta_file = f"{stamp}.csv.gz", f"{stamp}.metadata.json.gz"
    _store(directory / data_file, data.body)
    _store(directory / meta_file, after.body)
    entry = {
        "retrieved_at": _iso(now),
        "rows_updated_at": _iso(datetime.fromtimestamp(updated, UTC)),
        "rows": rows,
        "data_last_modified": _iso(version),
        "count_url": n_url,
        "data": {"url": url, "file": data_file, "sha256": digest, "headers": [list(h) for h in data.headers]},
        "metadata": {
            "url": meta_url, "file": meta_file, "sha256": sha256(after.body), "headers": [list(h) for h in after.headers]
        },
    }
    with log.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry, sort_keys=True) + "\n")
    return entry


def main(argv: Sequence[str] | None = None, fetch: Fetch = fetch, now: datetime | None = None) -> int:
    """Capture every declared source; report each new vintage on stdout and each failure on stderr.

    Exits 1 when any source failed, after trying all of them.
    """
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--root", type=Path, default=CAPTURES_DIR)
    args = parser.parse_args(argv)
    now = now or datetime.now(UTC)
    failed = False
    for source in load_sources(SOURCES_FILE):
        try:
            entry = capture_source(source, args.root, now, fetch)
        except Exception as e:  # one source failing, however it fails, must not stop the others
            print(f"{source.id}: {type(e).__name__}: {e}", file=sys.stderr)
            failed = True
            continue
        if entry is not None:
            print(f"{source.id}: {entry['rows']} rows, updated {entry['rows_updated_at']}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
