"""Snapshot tests: consistent NDJSON dump, verifiable SHA-256, atomic
visibility, failure hygiene, and restart cleanup.

Run:  python -m pytest -q   (from a venv with the collector requirements)
"""
from __future__ import annotations

import hashlib
import json
import os
import tempfile
import threading

import httpx
import pytest

# Point the collector at throwaway paths before importing the app.
_TMP = tempfile.TemporaryDirectory()
os.environ["COLLECTOR_DB"] = os.path.join(_TMP.name, "test.db")
os.environ["SOURCE_URL"] = "http://fake-source"
os.environ["SNAPSHOT_DIR"] = os.path.join(_TMP.name, "snapshots")

from fastapi.testclient import TestClient  # noqa: E402

from collector.app import db, snapshots, sync  # noqa: E402
from collector.app.main import app  # noqa: E402
from test_sync import FakeSource  # noqa: E402  (shared fake change feed)


@pytest.fixture
def source():
    return FakeSource()


@pytest.fixture
def client(source, monkeypatch):
    transport = httpx.MockTransport(source.handler)

    real_client = httpx.Client

    def patched_client(*args, **kwargs):
        kwargs["transport"] = transport
        return real_client(*args, **kwargs)

    monkeypatch.setattr(httpx, "Client", patched_client)
    # Fresh DB and snapshot directory per test.
    db_path = os.path.join(_TMP.name, f"test_{id(source)}.db")
    snap_dir = os.path.join(_TMP.name, f"snaps_{id(source)}")
    monkeypatch.setattr("collector.app.main.DB_PATH", db_path)
    monkeypatch.setattr("collector.app.main.SNAPSHOT_DIR", snap_dir)
    # Do not spawn API-level background threads; tests drive the real worker
    # synchronously via sync.run_sync (same code path).
    monkeypatch.setattr(
        "collector.app.main.sync.start_background", lambda *a, **k: True
    )
    db.init_db(db_path)
    with TestClient(app) as c:
        c.db_path = db_path
        c.snap_dir = snap_dir
        yield c
        for thread in threading.enumerate():
            if thread.name.startswith("sync-job-") and thread.is_alive():
                thread.join(timeout=10)
        assert not sync._running.locked(), "worker run lock leaked across tests"


def _publish(db_path, *papers):
    """Apply rows to the published library exactly like a sync commit does."""
    conn = db.connect(db_path)
    try:
        for p in papers:
            db.upsert_paper(conn, p)
        conn.commit()
    finally:
        conn.close()


def _paper(paper_id, rev, retracted, changed_at, title=None, authors="author"):
    return {
        "paper_id": paper_id, "rev": rev,
        "title": title or f"title-{paper_id}-{rev}",
        "authors": authors,
        "is_retracted": 1 if retracted else 0,
        "changed_at": changed_at,
    }


def _download_rows(client, snapshot_id):
    resp = client.get(f"/snapshots/{snapshot_id}/download")
    assert resp.status_code == 200
    return resp, [json.loads(line) for line in resp.content.decode("utf-8").splitlines()]


# --------------------------------------------------------------------------- #
# Tests
# --------------------------------------------------------------------------- #
def test_snapshot_dumps_all_papers_incl_retracted_with_verifiable_sha(client):
    _publish(
        client.db_path,
        _paper("P1", 3, True, "2026-11-03T08:00:00Z", title="Retracted work"),
        _paper("P2", 1, False, "2026-11-01T09:00:00Z", title="Ünïcode tïtle"),
    )

    resp = client.post("/snapshots")
    assert resp.status_code == 201
    meta = resp.json()
    assert meta["status"] == "completed"
    assert meta["record_count"] == 2
    assert len(meta["sha256"]) == 64
    assert meta["download_url"] == f"/snapshots/{meta['id']}/download"
    assert meta["completed_at"]

    # The completed registration is listable and fetchable.
    listing = client.get("/snapshots").json()
    assert [s["id"] for s in listing] == [meta["id"]]
    fetched = client.get(f"/snapshots/{meta['id']}").json()
    assert fetched["sha256"] == meta["sha256"]
    assert fetched["record_count"] == 2

    # Download: NDJSON, one record per line, retracted paper included.
    dl, rows = _download_rows(client, meta["id"])
    assert dl.headers["content-type"].startswith("application/x-ndjson")
    assert dl.headers["x-snapshot-sha256"] == meta["sha256"]
    assert dl.headers["x-snapshot-record-count"] == "2"
    # The registered SHA-256 verifies against the delivered bytes.
    assert hashlib.sha256(dl.content).hexdigest() == meta["sha256"]

    assert [r["paper_id"] for r in rows] == ["P1", "P2"]  # deterministic order
    assert rows[0]["is_retracted"] == 1 and rows[0]["rev"] == 3
    assert rows[1]["title"] == "Ünïcode tïtle"  # non-ASCII preserved
    assert set(rows[0]) == {
        "paper_id", "rev", "title", "authors", "is_retracted",
        "changed_at", "updated_at",
    }


def test_empty_library_snapshot(client):
    meta = client.post("/snapshots").json()
    assert meta["record_count"] == 0
    assert meta["sha256"] == hashlib.sha256(b"").hexdigest()
    dl = client.get(f"/snapshots/{meta['id']}/download")
    assert dl.status_code == 200
    assert dl.content == b""


def test_snapshot_pinned_to_read_start_despite_concurrent_commit(
    client, source, monkeypatch
):
    # Batch 1 is published by a first sync.
    source.add("P1", 1, False, "2026-11-01T08:00:00Z")
    source.add("P2", 1, False, "2026-11-02T08:00:00Z")
    job = client.post("/sync/jobs", json={"end": "2026-11-05T00:00:00Z"}).json()
    sync.run_sync(job["id"], client.db_path)
    assert client.get("/papers-count").json()["count"] == 2

    # Batch 2 waits in the source; its job exists but has not run yet.
    source.add("P3", 1, False, "2026-11-06T08:00:00Z")
    source.add("P1", 2, False, "2026-11-06T09:00:00Z")
    job2 = client.post("/sync/jobs", json={"end": "2026-11-10T00:00:00Z"}).json()

    # Run the incremental publish *while* the snapshot dump is streaming.
    real_stream = snapshots._stream_papers
    committed = {"done": False}

    def interleaved_stream(conn):
        for index, row in enumerate(real_stream(conn)):
            if index == 1 and not committed["done"]:
                sync.run_sync(job2["id"], client.db_path)  # publishes mid-dump
                committed["done"] = True
            yield row

    monkeypatch.setattr(snapshots, "_stream_papers", interleaved_stream)

    resp = client.post("/snapshots")
    assert resp.status_code == 201
    meta = resp.json()
    assert committed["done"], "the incremental sync must commit mid-dump"

    # The file holds only the library version as of the read start: no P3,
    # and P1 still at rev 1 — old and new revisions are never mixed.
    assert meta["record_count"] == 2
    _, rows = _download_rows(client, meta["id"])
    assert [r["paper_id"] for r in rows] == ["P1", "P2"]
    assert rows[0]["rev"] == 1

    # The live library really did move on underneath the dump.
    assert client.get("/papers-count").json()["count"] == 3
    assert client.get("/papers/P1").json()["rev"] == 2

    # A later snapshot sees the new state.
    meta2 = client.post("/snapshots").json()
    assert meta2["record_count"] == 3
    _, rows2 = _download_rows(client, meta2["id"])
    assert [r["paper_id"] for r in rows2] == ["P1", "P2", "P3"]
    assert rows2[0]["rev"] == 2


def test_failed_generation_leaves_no_residue_and_library_unaffected(
    client, monkeypatch
):
    _publish(client.db_path, _paper("P1", 1, False, "2026-11-01T08:00:00Z"))

    real_stream = snapshots._stream_papers

    def broken_stream(conn):
        for row in real_stream(conn):
            yield row
            raise OSError("simulated I/O failure mid-dump")

    monkeypatch.setattr(snapshots, "_stream_papers", broken_stream)

    resp = client.post("/snapshots")
    assert resp.status_code == 500
    assert "simulated I/O failure" in resp.json()["detail"]

    # Nothing visible, nothing downloadable, no stray files.
    assert client.get("/snapshots").json() == []
    assert client.get("/snapshots/1").status_code == 404
    assert client.get("/snapshots/1/download").status_code == 404
    assert os.listdir(client.snap_dir) == []

    # The failed registration is kept for audit but never exposed.
    conn = db.connect(client.db_path)
    try:
        row = db.get_snapshot(conn, 1)
        assert row["status"] == "failed"
        assert "simulated I/O failure" in row["error"]
    finally:
        conn.close()

    # The library is unaffected and a retry succeeds.
    monkeypatch.setattr(snapshots, "_stream_papers", real_stream)
    assert client.get("/papers/P1").status_code == 200
    retry = client.post("/snapshots")
    assert retry.status_code == 201
    assert retry.json()["record_count"] == 1


def test_restart_cleans_residue_and_keeps_completed_snapshots(client):
    _publish(client.db_path, _paper("P1", 1, False, "2026-11-01T08:00:00Z"))
    meta = client.post("/snapshots").json()
    final_path = os.path.join(client.snap_dir, meta["file_name"])
    assert os.path.isfile(final_path)
    with open(final_path, "rb") as fh:
        before = fh.read()

    # Plant residue: a temp part file, an orphan final-looking file, and a
    # registration stuck at 'generating' (as a crash mid-dump would leave).
    part_path = os.path.join(client.snap_dir, snapshots.part_name(99))
    with open(part_path, "wb") as fh:
        fh.write(b'{"partial":')
    orphan_path = os.path.join(client.snap_dir, snapshots.final_name(88))
    with open(orphan_path, "wb") as fh:
        fh.write(b"{}\n")
    conn = db.connect(client.db_path)
    try:
        stuck_id = db.create_snapshot(conn)  # status 'generating'
        conn.commit()
    finally:
        conn.close()

    # Simulate process restart: startup re-runs init + residue cleanup.
    db.init_db(client.db_path)
    removed = snapshots.cleanup_residue(client.db_path, client.snap_dir)

    assert snapshots.part_name(99) in removed
    assert snapshots.final_name(88) in removed
    assert not os.path.exists(part_path)
    assert not os.path.exists(orphan_path)
    conn = db.connect(client.db_path)
    try:
        assert db.get_snapshot(conn, stuck_id) is None
    finally:
        conn.close()

    # The completed snapshot survived, byte-identical and still downloadable.
    assert os.path.isfile(final_path)
    with open(final_path, "rb") as fh:
        assert fh.read() == before
    dl = client.get(f"/snapshots/{meta['id']}/download")
    assert dl.status_code == 200
    assert dl.content == before
    assert client.get(f"/snapshots/{meta['id']}").json()["sha256"] == meta["sha256"]
    assert [s["id"] for s in client.get("/snapshots").json()] == [meta["id"]]


def test_unknown_or_incomplete_snapshot_is_not_downloadable(client):
    assert client.get("/snapshots/123").status_code == 404
    assert client.get("/snapshots/123/download").status_code == 404

    # A completed registration whose file vanished is not downloadable either.
    _publish(client.db_path, _paper("P1", 1, False, "2026-11-01T08:00:00Z"))
    meta = client.post("/snapshots").json()
    os.remove(os.path.join(client.snap_dir, meta["file_name"]))
    assert client.get(f"/snapshots/{meta['id']}/download").status_code == 404
