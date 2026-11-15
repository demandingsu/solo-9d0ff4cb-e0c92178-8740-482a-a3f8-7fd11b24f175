"""Collection snapshots: a consistent NDJSON dump of the published library.

A snapshot hands the *entire* published paper catalog (retracted papers and
their status included) to the offline preservation archive as one NDJSON
file, plus its record count and SHA-256 for later verification.

Guarantees:

* Consistency: the dump runs inside a single WAL read transaction, so a sync
  commit landing mid-dump cannot leak newer revisions into the file — every
  line comes from the library version as of the first read. New and old
  revisions are never mixed.
* Streaming: rows are fetched from SQLite and written one at a time; the
  library is never materialized in memory.
* Atomic visibility: the NDJSON is written to a temp ``*.part`` file, fsynced,
  then atomically renamed to its final name, and only then is the registry
  row flipped to ``completed``. The API exposes completed snapshots only, so
  a partial file or registration is never downloadable.
* Restart safety: :func:`cleanup_residue` drops incomplete registrations and
  stray files at startup; completed snapshots are never touched.
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3

from . import db

FINAL_SUFFIX = ".ndjson"
PART_SUFFIX = ".ndjson.part"


def final_name(snapshot_id: int) -> str:
    return f"snapshot-{snapshot_id}{FINAL_SUFFIX}"


def part_name(snapshot_id: int) -> str:
    # Dot-prefixed so a temp file can never be mistaken for a final one.
    return f".snapshot-{snapshot_id}{PART_SUFFIX}"


def _stream_papers(conn: sqlite3.Connection) -> sqlite3.Cursor:
    """Lazy cursor over the published library.

    Rows are produced one at a time as the caller iterates, so the table is
    never loaded into memory. Must run inside an open read transaction.
    """
    return conn.execute(
        "SELECT paper_id, rev, title, authors, is_retracted, changed_at, "
        "updated_at FROM papers ORDER BY paper_id"
    )


def _row_to_line(row: sqlite3.Row) -> bytes:
    """Canonical NDJSON line (sorted keys, UTF-8) for one paper record."""
    line = json.dumps(
        dict(row), ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    return (line + "\n").encode("utf-8")


def _dump(conn: sqlite3.Connection, part_path: str) -> tuple[int, str]:
    """Stream the published library to ``part_path``; return (count, sha256).

    ``conn`` must already sit inside the read transaction that pins the
    library version for the whole dump.
    """
    hasher = hashlib.sha256()
    count = 0
    with open(part_path, "wb") as fh:
        for row in _stream_papers(conn):
            line = _row_to_line(row)
            hasher.update(line)
            fh.write(line)
            count += 1
        fh.flush()
        os.fsync(fh.fileno())
    return count, hasher.hexdigest()


def _fsync_dir(directory: str) -> None:
    """Best-effort fsync of the directory so the rename itself is durable."""
    try:
        fd = os.open(directory, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def _remove_quietly(path: str) -> None:
    try:
        os.remove(path)
    except OSError:
        pass


def generate(db_path: str, directory: str) -> sqlite3.Row:
    """Generate one snapshot and return its completed registration row.

    Raises on any read/write/registry failure; in that case no partial file
    is left behind and the registration is marked failed (never exposed).
    """
    os.makedirs(directory, exist_ok=True)

    # Reserve the snapshot number. The row stays invisible to the API until
    # it reaches 'completed'.
    reg = db.connect(db_path)
    try:
        snapshot_id = db.create_snapshot(reg)
        reg.commit()
    finally:
        reg.close()

    part_path = os.path.join(directory, part_name(snapshot_id))
    final_path = os.path.join(directory, final_name(snapshot_id))
    try:
        reader = db.connect(db_path)
        reader.isolation_level = None  # manual transaction control
        try:
            reader.execute("PRAGMA query_only = ON")
            # One deferred read transaction pins the library version: in WAL
            # mode every row streamed below comes from this version even if
            # an incremental sync commits while the dump is running.
            reader.execute("BEGIN")
            try:
                count, digest = _dump(reader, part_path)
            finally:
                try:
                    reader.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
        finally:
            reader.close()

        # Publish the file atomically, then publish the registration.
        os.replace(part_path, final_path)
        _fsync_dir(directory)

        reg = db.connect(db_path)
        try:
            db.complete_snapshot(
                reg, snapshot_id, count, digest, final_name(snapshot_id)
            )
            reg.commit()
            return db.get_snapshot(reg, snapshot_id)
        finally:
            reg.close()
    except Exception as exc:
        # No half-products: remove any partial/renamed file and record the
        # failure. The failed row is audit-only; the API never shows it.
        _remove_quietly(part_path)
        _remove_quietly(final_path)
        try:
            reg = db.connect(db_path)
            try:
                db.fail_snapshot(reg, snapshot_id, str(exc)[:500])
                reg.commit()
            finally:
                reg.close()
        except sqlite3.Error:
            pass
        raise


def cleanup_residue(db_path: str, directory: str) -> list[str]:
    """Reclaim incomplete snapshot residue after a restart.

    Drops registrations stuck at 'generating' (the process died mid-dump),
    then deletes every temp ``*.part`` file and every final-looking file no
    completed registration points to. Completed snapshots — row and file —
    are never touched. Returns the removed file names.
    """
    conn = db.connect(db_path)
    try:
        conn.execute("DELETE FROM snapshots WHERE status='generating'")
        conn.commit()
        keep = db.completed_snapshot_filenames(conn)
    finally:
        conn.close()

    removed: list[str] = []
    if not os.path.isdir(directory):
        return removed
    for name in sorted(os.listdir(directory)):
        path = os.path.join(directory, name)
        if not os.path.isfile(path):
            continue
        if name.endswith(PART_SUFFIX):
            stale = True
        elif name.startswith("snapshot-") and name.endswith(FINAL_SUFFIX):
            stale = name not in keep
        else:
            stale = False
        if stale:
            _remove_quietly(path)
            removed.append(name)
    return removed
