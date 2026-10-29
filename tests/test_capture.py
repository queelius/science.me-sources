import gzip
import http.client
import json
from datetime import UTC, datetime

import pytest

import capture
from capture import CaptureError, Response, Source, capture_source, count_url, data_url, load_sources, metadata_url

NOW = datetime(2026, 10, 29, 14, 5, 12, tzinfo=UTC)
LATER = datetime(2026, 10, 30, 14, 5, 12, tzinfo=UTC)
SOURCE = Source(
    id="nhsn-hrd", domain="data.example.gov", dataset="abcd-1234", columns=("week", "place", "count"), order=("week", "place")
)
CSV = b'"week","place","count"\n"2026-10-17","AK","3"\n"2026-10-17","AL","40"\n'


def metadata(rows_updated_at=1792765664, view_last_modified=1792765614) -> bytes:
    return json.dumps({"rowsUpdatedAt": rows_updated_at, "viewLastModified": view_last_modified, "columns": []}).encode()


TRUTH = "Fri, 23 Oct 2026 14:26:57 GMT"


def soda(truth=TRUTH, stale="false"):
    """The headers Socrata serves on a data or count response."""
    return (("X-SODA2-Data-Out-Of-Date", stale), ("X-SODA2-Truth-Last-Modified", truth))


class FakeWeb:
    """Serves `pages[url]`, a (body, headers) pair or a list of them consumed one per fetch."""

    def __init__(self, pages):
        self.pages = {url: list(page) if isinstance(page, list) else [page] for url, page in pages.items()}

    def __call__(self, url):
        pages = self.pages[url]
        body, headers = pages.pop(0) if len(pages) > 1 else pages[0]
        return Response(body=body, headers=headers)


def count(n):
    return json.dumps([{"n": str(n)}]).encode()


def web(data=CSV, meta=None, rows=2, data_headers=None, count_headers=None):
    return FakeWeb({
        data_url(SOURCE): (data, soda() if data_headers is None else data_headers),
        count_url(SOURCE): (count(rows), soda() if count_headers is None else count_headers),
        metadata_url(SOURCE): (meta if meta is not None else metadata(), ()),
    })


def entries(root):
    return [json.loads(line) for line in (root / SOURCE.id / "log.jsonl").read_text().splitlines()]


def test_data_url_selects_and_orders_the_declared_columns():
    assert data_url(SOURCE) == (
        "https://data.example.gov/resource/abcd-1234.csv?"
        "%24select=week%2Cplace%2Ccount&%24order=week%2Cplace&%24limit=1000000"
    )
    assert metadata_url(SOURCE) == "https://data.example.gov/api/views/abcd-1234.json"
    assert count_url(SOURCE) == "https://data.example.gov/resource/abcd-1234.json?%24select=count%28%2A%29+as+n"


def test_a_first_capture_stores_the_payloads_and_logs_their_provenance(tmp_path):
    entry = capture_source(SOURCE, tmp_path, NOW, web())
    directory = tmp_path / SOURCE.id
    assert gzip.decompress((directory / entry["data"]["file"]).read_bytes()) == CSV
    assert gzip.decompress((directory / entry["metadata"]["file"]).read_bytes()) == metadata()
    assert entry["retrieved_at"] == "2026-10-29T14:05:12Z"
    assert entry["data"]["url"] == data_url(SOURCE)
    assert entry["data"]["sha256"] == capture.sha256(CSV)
    assert entry["data"]["headers"] == [list(h) for h in soda()]
    assert entry["rows"] == 2
    assert entry["rows_updated_at"] == "2026-10-23T14:27:44Z"
    assert entry["data_last_modified"] == "2026-10-23T14:26:57Z"
    assert entry["count_url"] == count_url(SOURCE)
    assert entries(tmp_path) == [entry]


def test_an_unchanged_payload_is_not_stored_again(tmp_path):
    capture_source(SOURCE, tmp_path, NOW, web())
    assert capture_source(SOURCE, tmp_path, LATER, web()) is None
    assert len(entries(tmp_path)) == 1
    assert len(list((tmp_path / SOURCE.id).glob("*.gz"))) == 2


def test_a_revision_is_stored_beside_the_earlier_vintage(tmp_path):
    first = capture_source(SOURCE, tmp_path, NOW, web())
    revised = CSV.replace(b'"40"', b'"41"')
    second = capture_source(SOURCE, tmp_path, LATER, web(revised, metadata(rows_updated_at=1792852064)))
    assert [e["data"]["sha256"] for e in entries(tmp_path)] == [capture.sha256(CSV), capture.sha256(revised)]
    assert gzip.decompress((tmp_path / SOURCE.id / first["data"]["file"]).read_bytes()) == CSV
    assert second["rows_updated_at"] == "2026-10-24T14:27:44Z"


def test_a_capture_is_refused_when_the_source_updated_while_it_was_read(tmp_path):
    fake = web()
    fake.pages[metadata_url(SOURCE)] = [(metadata(1), ()), (metadata(2), ())]
    with pytest.raises(CaptureError, match="updated while"):
        capture_source(SOURCE, tmp_path, NOW, fake)
    assert not (tmp_path / SOURCE.id).exists()


def test_a_payload_at_the_row_limit_is_refused_as_possibly_truncated(tmp_path, monkeypatch):
    monkeypatch.setattr(capture, "ROW_LIMIT", 2)
    with pytest.raises(CaptureError, match="row limit"):
        capture_source(SOURCE, tmp_path, NOW, web())


def test_a_response_the_server_marks_out_of_date_is_refused(tmp_path):
    with pytest.raises(CaptureError, match="out of date"):
        capture_source(SOURCE, tmp_path, NOW, web(data_headers=soda(stale="true")))


def test_a_response_without_its_version_headers_is_refused(tmp_path):
    with pytest.raises(CaptureError, match="X-SODA2"):
        capture_source(SOURCE, tmp_path, NOW, web(data_headers=()))


def test_rows_and_count_from_different_versions_are_refused(tmp_path):
    with pytest.raises(CaptureError, match="different versions"):
        capture_source(SOURCE, tmp_path, NOW, web(count_headers=soda(truth="Wed, 21 Oct 2026 14:00:00 GMT")))


def test_rows_short_of_the_published_count_are_refused(tmp_path):
    with pytest.raises(CaptureError, match="count"):
        capture_source(SOURCE, tmp_path, NOW, web(rows=3))


@pytest.mark.parametrize(
    "data",
    [CSV[:-3], CSV.replace(b',"40"', b"")],
    ids=["unterminated-quote", "short-row"],
)
def test_malformed_csv_is_refused(tmp_path, data):
    with pytest.raises(CaptureError):
        capture_source(SOURCE, tmp_path, NOW, web(data=data))


def test_a_payload_without_a_header_row_is_refused(tmp_path):
    with pytest.raises(CaptureError, match="columns"):
        capture_source(SOURCE, tmp_path, NOW, web(data=b'"2026-10-17","AK","3"\n'))


def test_stored_files_are_never_replaced(tmp_path):
    capture_source(SOURCE, tmp_path, NOW, web())
    with pytest.raises(FileExistsError):
        capture_source(SOURCE, tmp_path, NOW, web(CSV + b'"2026-10-17","AZ","5"\n', rows=3))


def test_load_sources_reads_the_declared_sources(tmp_path):
    path = tmp_path / "sources.json"
    path.write_text(json.dumps({"sources": [
        {"id": "a", "domain": "d", "dataset": "x", "columns": ["c"], "order": ["c"]},
    ]}))
    assert load_sources(path) == [Source(id="a", domain="d", dataset="x", columns=("c",), order=("c",))]


def test_load_sources_refuses_an_order_outside_the_columns(tmp_path):
    path = tmp_path / "sources.json"
    path.write_text(json.dumps({"sources": [{"id": "a", "domain": "d", "dataset": "x", "columns": ["c"], "order": ["z"]}]}))
    with pytest.raises(CaptureError, match="order"):
        load_sources(path)


def test_main_captures_every_source_and_reports_failures(tmp_path, monkeypatch, capsys):
    bad = Source(id="bad", domain="d", dataset="y", columns=("c",), order=("c",))
    fake = web()

    def fetch(url):
        if "/y" in url:
            raise http.client.IncompleteRead(b"partial")
        return fake(url)

    monkeypatch.setattr(capture, "load_sources", lambda path: [bad, SOURCE])
    assert capture.main(["--root", str(tmp_path)], fetch=fetch, now=NOW) == 1
    assert (tmp_path / SOURCE.id / "log.jsonl").exists()
    assert "bad: IncompleteRead" in capsys.readouterr().err


def test_the_declared_sources_are_valid():
    sources = load_sources(capture.SOURCES_FILE)
    assert {s.dataset for s in sources} == {"ua7e-t2fy", "mpgq-jmmr"}
