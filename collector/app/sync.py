"""Sync worker: paginated fetch with durable checkpoints and atomic publish."""
from __future__ import annotations

import threading
from datetime import datetime, timezone

import httpx

from . import db

# In-memory guard so a single process never runs two workers at once.
_running = threading.Lock()


class SyncError(RuntimeError):
    """Any fetch/transport failure; the job stays resumable."""


def _normalize_iso(value: str) -> str:
    """Accept ...Z / +00:00 / naive; emit canonical UTC 'YYYY-MM-DDTHH:MM:SSZ'."""
    text = value.strip()
    try:
        if text.endswith("Z"):
            dt = datetime.fromisoformat(text[:-1]).replace(tzinfo=timezone.utc)
        else:
            dt = datetime.fromisoformat(text)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            dt = dt.astimezone(timezone.utc)
    except ValueError as exc:
        raise SyncError(f"invalid ISO-8601 timestamp: {value!r}") from exc
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _fetch_page(client: httpx.Client, source_url: str, start: str, end: str,
                cursor: str | None) -> dict:
    params = {"start": start, "end": end}
    if cursor:
        params["cursor"] = cursor
    try:
        resp = client.get(f"{source_url.rstrip('/')}/changes", params=params)
    except httpx.HTTPError as exc:
        raise SyncError(f"source request failed: {exc}") from exc
    if resp.status_code != 200:
        raise SyncError(
            f"source returned HTTP {resp.status_code}: {resp.text[:300]}"
        )
    return resp.json()


def run_sync(job_id: int, db_path: str | None = None) -> None:
    """Execute (or resume) one sync job until completion or failure.

    On every page: persist rows + next cursor in ONE transaction before the
    next request. A 410 (expired cursor) discards this job's staging and
    re-fetches the *original* interval — never a newer cutoff.
    """
    if not _running.acquire(blocking=False):
        return  # another worker is active in this process

    conn = db.connect(db_path)
    try:
        job = db.get_job(conn, job_id)
        if job is None:
            return
        if job["status"] not in ("collecting", "interrupted", "failed"):
            return
        db.mark_status(conn, "collecting", job_id)
        conn.commit()

        source_url = job["source_url"]
        start = job["interval_start"]
        end = job["interval_end"]
        cursor = job["cursor"]
        pages = job["pages_fetched"]
        fetch_done = bool(job["fetch_done"])

        with httpx.Client(timeout=10.0) as client:
            while not fetch_done:
                try:
                    page = _fetch_page(client, source_url, start, end, cursor)
                except SyncError as exc:
                    msg = str(exc)
                    if "HTTP 410" in msg:
                        # Cursor expired/invalid: restart the ORIGINAL interval.
                        with db.transaction(conn) as tx:
                            db.clear_staged(tx, job_id)
                            resets = db.increment_cursor_resets(tx, job_id)
                            db.mark_status(
                                tx, job_id, "failed",
                                f"cursor expired; interval re-fetch required "
                                f"(reset #{resets})",
                            )
                        return
                    with db.transaction(conn) as tx:
                        db.mark_status(tx, job_id, "failed", msg)
                    return

                # Durable checkpoint: page rows and continuation cursor commit
                # together, so resume never loses or skips a page.
                with db.transaction(conn) as tx:
                    for event in page["changes"]:
                        db.upsert_staged(tx, job_id, event)
                    cursor = page["next_cursor"]
                    pages += 1
                    fetch_done = bool(page["done"])
                    db.update_cursor(
                        tx, job_id, cursor, page["next_offset"],
                        pages, fetch_done,
                    )

        # Whole interval collected: publish staging and completed time in one
        # atomic transaction. Any error rolls back; old library stays queryable
        # and staging remains private until a successful resume.
        with db.transaction(conn) as tx:
            db.mark_status(tx, job_id, "committing")
            published_at = db.utc_now_iso()
            for row in db.list_staged(tx, job_id):
                db.upsert_paper(tx, row)
            # completed_at is the change-time baseline for the next incremental
            # fetch, so it must equal this interval's closed end (not wall-clock
            # time); otherwise a historical cutoff would never be re-fetchable.
            tx.execute(
                "UPDATE sync_state SET source_url=?, last_start=?, last_end=?, "
                "completed_at=? WHERE id=1",
                (source_url, start, end, end),
            )
            tx.execute(
                "UPDATE jobs SET updated_at=? WHERE id=?", (published_at, job_id)
            )
            db.mark_status(tx, job_id, "completed")
    except Exception as exc:  # pragma: no cover - defensive guard
        conn.rollback()
        with db.transaction(conn) as tx:
            db.mark_status(tx, job_id, "failed", f"unexpected error: {exc}")
    finally:
        conn.close()
        _running.release()


def start_background(job_id: int, db_path: str | None = None) -> bool:
    """Spawn a daemon worker thread. Returns False if one is already running."""
    if _running.locked():
        return False
    thread = threading.Thread(
        target=run_sync, args=(job_id, db_path), daemon=True,
        name=f"sync-job-{job_id}",
    )
    thread.start()
    return True
