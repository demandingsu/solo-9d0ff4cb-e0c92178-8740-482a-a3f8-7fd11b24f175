"""SQLite persistence: sync state, resumable jobs, staged changes, papers.

Design points:

* Every fetched page is persisted together with its resumption cursor in the
  same transaction, so a crash/restart can resume from the last durable page.
* Staged rows of one job stay invisible to query clients; they only become
  visible when the *whole* closed interval is complete and a single atomic
  transaction publishes them together with the new completed time.
* Retraction is sticky in the committed ``papers`` table: once a paper is
  retracted, an older non-retracted revision can never undo it.
"""
from __future__ import annotations

import os
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone

SCHEMA = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS sync_state (
    id          INTEGER PRIMARY KEY CHECK (id = 1),
    source_url  TEXT NOT NULL,
    last_start  TEXT,
    last_end    TEXT,
    completed_at TEXT
);

CREATE TABLE IF NOT EXISTS jobs (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    source_url       TEXT NOT NULL,
    interval_start   TEXT NOT NULL,
    interval_end     TEXT NOT NULL,
    status           TEXT NOT NULL CHECK (status IN
                       ('collecting','committing','completed','failed','interrupted')),
    cursor           TEXT,
    next_offset      INTEGER NOT NULL DEFAULT 0,
    pages_fetched    INTEGER NOT NULL DEFAULT 0,
    fetch_done       INTEGER NOT NULL DEFAULT 0,
    cursor_resets    INTEGER NOT NULL DEFAULT 0,
    error            TEXT,
    created_at       TEXT NOT NULL,
    updated_at       TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS staged_changes (
    job_id        INTEGER NOT NULL REFERENCES jobs(id),
    paper_id      TEXT NOT NULL,
    rev           INTEGER NOT NULL,
    title         TEXT NOT NULL,
    authors       TEXT NOT NULL,
    is_retracted  INTEGER NOT NULL,
    changed_at    TEXT NOT NULL,
    PRIMARY KEY (job_id, paper_id)
);

CREATE TABLE IF NOT EXISTS papers (
    paper_id      TEXT PRIMARY KEY,
    rev           INTEGER NOT NULL,
    title         TEXT NOT NULL,
    authors       TEXT NOT NULL,
    is_retracted  INTEGER NOT NULL,
    changed_at    TEXT NOT NULL,
    updated_at    TEXT NOT NULL
);
"""

_lock = threading.Lock()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def connect(db_path: str | None = None) -> sqlite3.Connection:
    path = db_path or os.getenv("COLLECTOR_DB", "/data/collector.db")
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    conn = sqlite3.connect(path, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def init_db(db_path: str | None = None) -> None:
    with _lock:
        conn = connect(db_path)
        try:
            conn.executescript(SCHEMA)
            conn.execute(
                "INSERT OR IGNORE INTO sync_state (id, source_url, completed_at) "
                "VALUES (1, '', NULL)"
            )
            # A process killed mid-sync leaves jobs marked collecting: on
            # restart they become explicitly resumable as 'interrupted'.
            conn.execute(
                "UPDATE jobs SET status='interrupted', "
                "error='process restarted before completion', updated_at=? "
                "WHERE status IN ('collecting','committing')",
                (utc_now_iso(),),
            )
            conn.commit()
        finally:
            conn.close()


@contextmanager
def transaction(conn: sqlite3.Connection):
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise


# --------------------------------------------------------------------------- #
# sync_state
# --------------------------------------------------------------------------- #
def get_state(conn: sqlite3.Connection) -> sqlite3.Row:
    return conn.execute("SELECT * FROM sync_state WHERE id=1").fetchone()


# --------------------------------------------------------------------------- #
# jobs
# --------------------------------------------------------------------------- #
def create_job(conn: sqlite3.Connection, source_url: str,
               interval_start: str, interval_end: str) -> int:
    now = utc_now_iso()
    cur = conn.execute(
        "INSERT INTO jobs (source_url, interval_start, interval_end, status, created_at, updated_at) "
        "VALUES (?, ?, ?, 'collecting', ?, ?)",
        (source_url, interval_start, interval_end, now, now),
    )
    return int(cur.lastrowid)


def get_job(conn: sqlite3.Connection, job_id: int) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()


def list_jobs(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return list(conn.execute("SELECT * FROM jobs ORDER BY id DESC"))


def has_active_job(conn: sqlite3.Connection) -> sqlite3.Row | None:
    """A collecting/committing/interrupted job blocks new overlapping jobs."""
    return conn.execute(
        "SELECT * FROM jobs WHERE status IN ('collecting','committing','interrupted') "
        "ORDER BY id LIMIT 1"
    ).fetchone()


def update_cursor(conn: sqlite3.Connection, job_id: int, cursor: str | None,
                  next_offset: int, pages_fetched: int, fetch_done: bool) -> None:
    conn.execute(
        "UPDATE jobs SET cursor=?, next_offset=?, pages_fetched=?, fetch_done=?, updated_at=? "
        "WHERE id=?",
        (cursor, next_offset, pages_fetched, int(fetch_done), utc_now_iso(), job_id),
    )


def mark_status(conn: sqlite3.Connection, job_id: int, status: str,
                error: str | None = None) -> None:
    conn.execute(
        "UPDATE jobs SET status=?, error=?, updated_at=? WHERE id=?",
        (status, error, utc_now_iso(), job_id),
    )


def increment_cursor_resets(conn: sqlite3.Connection, job_id: int) -> int:
    conn.execute(
        "UPDATE jobs SET cursor_resets=cursor_resets+1, cursor=NULL, next_offset=0, "
        "pages_fetched=0, fetch_done=0, updated_at=? WHERE id=?",
        (utc_now_iso(), job_id),
    )
    row = conn.execute("SELECT cursor_resets FROM jobs WHERE id=?", (job_id,)).fetchone()
    return int(row["cursor_resets"])


# --------------------------------------------------------------------------- #
# staging
# --------------------------------------------------------------------------- #
def upsert_staged(conn: sqlite3.Connection, job_id: int, event: dict) -> None:
    """Keep the highest revision per paper; retraction sticks on equal rev."""
    conn.execute(
        """
        INSERT INTO staged_changes
            (job_id, paper_id, rev, title, authors, is_retracted, changed_at)
        VALUES (:job_id, :paper_id, :rev, :title, :authors, :is_retracted, :changed_at)
        ON CONFLICT(job_id, paper_id) DO UPDATE SET
            rev          = CASE
                             WHEN excluded.rev > staged_changes.rev THEN excluded.rev
                             ELSE staged_changes.rev
                           END,
            title        = CASE
                             WHEN excluded.rev > staged_changes.rev THEN excluded.title
                             ELSE staged_changes.title
                           END,
            authors      = CASE
                             WHEN excluded.rev > staged_changes.rev THEN excluded.authors
                             ELSE staged_changes.authors
                           END,
            is_retracted = CASE
                             WHEN excluded.rev > staged_changes.rev THEN excluded.is_retracted
                             WHEN excluded.rev = staged_changes.rev
                                 THEN MAX(staged_changes.is_retracted, excluded.is_retracted)
                             ELSE staged_changes.is_retracted
                           END,
            changed_at   = CASE
                             WHEN excluded.rev > staged_changes.rev THEN excluded.changed_at
                             ELSE staged_changes.changed_at
                           END
        """,
        {
            "job_id": job_id,
            "paper_id": event["paper_id"],
            "rev": event["rev"],
            "title": event["title"],
            "authors": event["authors"],
            "is_retracted": event["is_retracted"],
            "changed_at": event["changed_at"],
        },
    )


def staged_count(conn: sqlite3.Connection, job_id: int) -> int:
    row = conn.execute(
        "SELECT COUNT(*) AS c FROM staged_changes WHERE job_id=?", (job_id,)
    ).fetchone()
    return int(row["c"])


def list_staged(conn: sqlite3.Connection, job_id: int) -> list[sqlite3.Row]:
    return list(conn.execute(
        "SELECT * FROM staged_changes WHERE job_id=? ORDER BY paper_id", (job_id,)
    ))


def clear_staged(conn: sqlite3.Connection, job_id: int) -> None:
    conn.execute("DELETE FROM staged_changes WHERE job_id=?", (job_id,))


# --------------------------------------------------------------------------- #
# committed papers
# --------------------------------------------------------------------------- #
def upsert_paper(conn: sqlite3.Connection, row: sqlite3.Row) -> None:
    """Publish one staged paper. Old revisions and un-retracting old rows lose."""
    conn.execute(
        """
        INSERT INTO papers (paper_id, rev, title, authors, is_retracted, changed_at, updated_at)
        VALUES (:paper_id, :rev, :title, :authors, :is_retracted, :changed_at, :updated_at)
        ON CONFLICT(paper_id) DO UPDATE SET
            rev          = CASE
                             WHEN :rev > papers.rev THEN :rev
                             ELSE papers.rev
                           END,
            title        = CASE
                             WHEN :rev > papers.rev THEN :title
                             ELSE papers.title
                           END,
            authors      = CASE
                             WHEN :rev > papers.rev THEN :authors
                             ELSE papers.authors
                           END,
            is_retracted = CASE
                             WHEN :rev > papers.rev THEN :is_retracted
                             WHEN :rev = papers.rev
                                 THEN MAX(papers.is_retracted, :is_retracted)
                             ELSE papers.is_retracted
                           END,
            changed_at   = CASE
                             WHEN :rev > papers.rev THEN :changed_at
                             ELSE papers.changed_at
                           END,
            updated_at   = :updated_at
        """,
        {
            "paper_id": row["paper_id"],
            "rev": row["rev"],
            "title": row["title"],
            "authors": row["authors"],
            "is_retracted": row["is_retracted"],
            "changed_at": row["changed_at"],
            "updated_at": utc_now_iso(),
        },
    )


def get_paper(conn: sqlite3.Connection, paper_id: str) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM papers WHERE paper_id=?", (paper_id,)).fetchone()


def list_papers(conn: sqlite3.Connection, include_retracted: bool) -> list[sqlite3.Row]:
    sql = "SELECT * FROM papers"
    if not include_retracted:
        sql += " WHERE is_retracted=0"
    return list(conn.execute(sql + " ORDER BY paper_id"))


def count_papers(conn: sqlite3.Connection, include_retracted: bool) -> int:
    sql = "SELECT COUNT(*) AS c FROM papers"
    if not include_retracted:
        sql += " WHERE is_retracted=0"
    return int(conn.execute(sql).fetchone()["c"])
