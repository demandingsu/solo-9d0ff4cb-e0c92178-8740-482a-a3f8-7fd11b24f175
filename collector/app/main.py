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
"""
from __future__ import annotations

import os
import sqlite3
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query

from . import db, sync
from .schemas import CreateJobRequest, JobResponse, PaperResponse, StateResponse

DB_PATH = os.getenv("COLLECTOR_DB", "/data/collector.db")
DEFAULT_SOURCE_URL = os.getenv("SOURCE_URL", "http://source:8000")


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init_db(DB_PATH)
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
