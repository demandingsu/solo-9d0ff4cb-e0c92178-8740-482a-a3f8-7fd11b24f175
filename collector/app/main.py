"""Collector API: trigger/resume sync jobs and query the paper library.

Endpoints
---------
POST   /sync/jobs              create a sync job (full or incremental)
GET    /sync/jobs              list jobs
GET    /sync/jobs/{id}         job status incl. durable checkpoint details
POST   /sync/jobs/{id}/resume  resume a failed/interrupted job
GET    /sync/state             last completed interval and completion time
GET    /papers                 query library (retracted hidden by default)
GET    /papers/{paper_id}      lookup one paper (410 when retracted)
POST   /snapshots              dump the published library to NDJSON (consistent,
                               retracted included) and return count + SHA-256
GET    /snapshots              list completed snapshots
GET    /snapshots/{id}         snapshot metadata (record count, SHA-256)
GET    /snapshots/{id}/download  download the snapshot NDJSON file
"""
from __future__ import annotations

import os
import sqlite3
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse

from . import db, snapshots, sync
from .schemas import (
    CreateJobRequest,
    JobResponse,
    PaperResponse,
    SnapshotResponse,
    StateResponse,
)

DB_PATH = os.getenv("COLLECTOR_DB", "/data/collector.db")
SNAPSHOT_DIR = os.getenv("SNAPSHOT_DIR", "/data/snapshots")
DEFAULT_SOURCE_URL = os.getenv("SOURCE_URL", "http://source:8000")


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init_db(DB_PATH)
    # Reclaim half-finished snapshot residue from a previous run; completed
    # snapshots keep working across restarts.
    snapshots.cleanup_residue(DB_PATH, SNAPSHOT_DIR)
    yield


app = FastAPI(
    title="Institutional Repository Sync Collector",
    version="1.0.0",
    lifespan=lifespan,
)


def _conn() -> sqlite3.Connection:
    return db.connect(DB_PATH)


def _job_response(conn: sqlite3.Connection, row: sqlite3.Row) -> JobResponse:
    return JobResponse(
        id=row["id"],
        source_url=row["source_url"],
        interval_start=row["interval_start"],
        interval_end=row["interval_end"],
        status=row["status"],
        cursor=row["cursor"],
        next_offset=row["next_offset"],
        pages_fetched=row["pages_fetched"],
        fetch_done=bool(row["fetch_done"]),
        cursor_resets=row["cursor_resets"],
        staged_count=db.staged_count(conn, row["id"]),
        error=row["error"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


# --------------------------------------------------------------------------- #
# sync jobs
# --------------------------------------------------------------------------- #
@app.post("/sync/jobs", status_code=201, response_model=JobResponse)
def create_job(req: CreateJobRequest) -> JobResponse:
    end = sync._normalize_iso(req.end)
    source_url = (req.source_url or DEFAULT_SOURCE_URL).rstrip("/")
    if not source_url.startswith(("http://", "https://")):
        raise HTTPException(status_code=422, detail="source_url must be http(s) URL")

    conn = _conn()
    try:
        active = db.has_active_job(conn)
        if active is not None:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"job {active['id']} is {active['status']}; resume it before "
                    "starting another job (overlapping intervals are rejected)"
                ),
            )

        state = db.get_state(conn)
        if req.start is not None:
            start = sync._normalize_iso(req.start)
        elif state["completed_at"]:
            # Incremental: begin at the last completed time. Intervals are
            # closed on both sides, so the boundary event is re-fetched and
            # deduped by revision — no change at the boundary is ever missed.
            start = state["completed_at"]
        else:
            # First run: full fetch from the epoch.
            start = "1970-01-01T00:00:00Z"

        if start > end:
            raise HTTPException(status_code=422, detail="start must be <= end")

        job_id = db.create_job(conn, source_url, start, end)
        conn.commit()
        row = db.get_job(conn, job_id)
        response = _job_response(conn, row)
    finally:
        conn.close()

    if not sync.start_background(job_id, DB_PATH):
        raise HTTPException(status_code=409, detail="a sync worker is already running")
    return response


@app.get("/sync/jobs", response_model=list[JobResponse])
def list_jobs() -> list[JobResponse]:
    conn = _conn()
    try:
        return [_job_response(conn, row) for row in db.list_jobs(conn)]
    finally:
        conn.close()


@app.get("/sync/jobs/{job_id}", response_model=JobResponse)
def get_job(job_id: int) -> JobResponse:
    conn = _conn()
    try:
        row = db.get_job(conn, job_id)
        if row is None:
            raise HTTPException(status_code=404, detail="job not found")
        return _job_response(conn, row)
    finally:
        conn.close()


@app.post("/sync/jobs/{job_id}/resume", response_model=JobResponse)
def resume_job(job_id: int) -> JobResponse:
    conn = _conn()
    try:
        row = db.get_job(conn, job_id)
        if row is None:
            raise HTTPException(status_code=404, detail="job not found")
        if row["status"] not in ("failed", "interrupted"):
            raise HTTPException(
                status_code=409,
                detail=f"job is {row['status']}; only failed/interrupted jobs can resume",
            )
        response = _job_response(conn, row)
    finally:
        conn.close()

    # Same original closed interval is reused; the durable cursor determines
    # where fetching continues (or re-starts from offset 0 after cursor expiry).
    if not sync.start_background(job_id, DB_PATH):
        raise HTTPException(status_code=409, detail="a sync worker is already running")
    return response


@app.get("/sync/state", response_model=StateResponse)
def get_sync_state() -> StateResponse:
    conn = _conn()
    try:
        row = db.get_state(conn)
        return StateResponse(
            source_url=row["source_url"],
            last_start=row["last_start"],
            last_end=row["last_end"],
            completed_at=row["completed_at"],
        )
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# paper library (query side)
# --------------------------------------------------------------------------- #
@app.get("/papers", response_model=list[PaperResponse])
def list_papers(include_retracted: bool = Query(False)) -> list[PaperResponse]:
    conn = _conn()
    try:
        rows = db.list_papers(conn, include_retracted)
        return [PaperResponse(**dict(r)) for r in rows]
    finally:
        conn.close()


@app.get("/papers/{paper_id}", response_model=PaperResponse)
def get_paper(
    paper_id: str, include_retracted: bool = Query(False)
) -> PaperResponse:
    conn = _conn()
    try:
        row = db.get_paper(conn, paper_id)
        if row is None:
            raise HTTPException(status_code=404, detail="paper not found")
        if row["is_retracted"] and not include_retracted:
            raise HTTPException(
                status_code=410,
                detail="paper has been retracted and is hidden from retrieval; "
                       "use ?include_retracted=true to inspect it",
            )
        return PaperResponse(**dict(row))
    finally:
        conn.close()


@app.get("/papers-count")
def papers_count(include_retracted: bool = Query(False)) -> dict:
    conn = _conn()
    try:
        return {"count": db.count_papers(conn, include_retracted)}
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# collection snapshots (handover to the offline preservation archive)
# --------------------------------------------------------------------------- #
def _snapshot_response(row: sqlite3.Row) -> SnapshotResponse:
    return SnapshotResponse(
        id=row["id"],
        status=row["status"],
        record_count=row["record_count"],
        sha256=row["sha256"],
        file_name=row["file_name"],
        download_url=f"/snapshots/{row['id']}/download",
        created_at=row["created_at"],
        completed_at=row["completed_at"],
    )


@app.post("/snapshots", status_code=201, response_model=SnapshotResponse)
def create_snapshot() -> SnapshotResponse:
    """Dump the whole published library (retracted papers included) to NDJSON.

    The dump reads one consistent WAL snapshot of the library: an incremental
    sync committing mid-dump never leaks newer revisions into the file. Rows
    stream to disk one at a time; the file and its registration become
    visible only after both are fully written.
    """
    try:
        row = snapshots.generate(DB_PATH, SNAPSHOT_DIR)
    except Exception as exc:
        raise HTTPException(
            status_code=500, detail=f"snapshot generation failed: {exc}"
        )
    return _snapshot_response(row)


@app.get("/snapshots", response_model=list[SnapshotResponse])
def list_snapshots() -> list[SnapshotResponse]:
    conn = _conn()
    try:
        return [_snapshot_response(r) for r in db.list_completed_snapshots(conn)]
    finally:
        conn.close()


@app.get("/snapshots/{snapshot_id}", response_model=SnapshotResponse)
def get_snapshot(snapshot_id: int) -> SnapshotResponse:
    conn = _conn()
    try:
        row = db.get_snapshot(conn, snapshot_id)
        if row is None or row["status"] != "completed":
            # Incomplete registrations are never externally visible.
            raise HTTPException(status_code=404, detail="snapshot not found")
        return _snapshot_response(row)
    finally:
        conn.close()


@app.get("/snapshots/{snapshot_id}/download")
def download_snapshot(snapshot_id: int) -> FileResponse:
    conn = _conn()
    try:
        row = db.get_snapshot(conn, snapshot_id)
        if row is None or row["status"] != "completed":
            raise HTTPException(status_code=404, detail="snapshot not found")
        file_name = row["file_name"]
        sha256 = row["sha256"]
        record_count = row["record_count"]
    finally:
        conn.close()

    path = os.path.join(SNAPSHOT_DIR, file_name)
    if not os.path.isfile(path):
        raise HTTPException(
            status_code=404, detail="snapshot file is missing from storage"
        )
    return FileResponse(
        path,
        media_type="application/x-ndjson",
        filename=file_name,
        headers={
            "X-Snapshot-SHA256": sha256,
            "X-Snapshot-Record-Count": str(record_count),
        },
    )
