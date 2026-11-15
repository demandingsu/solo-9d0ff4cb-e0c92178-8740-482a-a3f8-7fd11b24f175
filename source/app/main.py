"""Mock institutional-repository source service.

No authentication is required (local demo source). It serves immutable change
events over a closed [start, end] interval, paginated with opaque resumption
cursors. Cursors have a short TTL and can also be invalidated globally via the
control API, letting the collector demonstrate re-fetching the *original*
interval instead of jumping to a newer cutoff.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import threading
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

from .data import APPENDED_EVENTS, SEED_EVENTS

PAGE_SIZE = int(os.getenv("SOURCE_PAGE_SIZE", "3"))
CURSOR_TTL_SECONDS = int(os.getenv("CURSOR_TTL_SECONDS", "60"))
SECRET = os.getenv("CURSOR_SECRET", "local-demo-secret").encode()

app = FastAPI(title="Mock Institutional Repository Source", version="1.0.0")
_lock = threading.Lock()


# --------------------------------------------------------------------------- #
# In-memory runtime state
# --------------------------------------------------------------------------- #
class _State:
    def __init__(self) -> None:
        self.appended: list[dict] = []
        # Bumped on forced cursor expiry; cursors signed under an older
        # generation are rejected even while inside their TTL.
        self.cursor_generation = 0
        self.fail_next = 0  # number of upcoming /changes requests that must fail
        # When > 0, decremented after every successful /changes response; once
        # it reaches 0 the cursor generation is bumped (post-response) so the
        # cursor just handed out is rejected (410) on the next request.
        # Gives a deterministic, race-free cursor-expiry demo.
        self.expire_after_requests = 0
        self.changes_calls = 0


STATE = _State()


def _all_events() -> list[dict]:
    with _lock:
        return _events_locked()


def _events_locked() -> list[dict]:
    """Caller must already hold ``_lock`` (the lock is non-reentrant)."""
    return list(SEED_EVENTS) + list(STATE.appended)


# --------------------------------------------------------------------------- #
# Opaque signed cursor: base64url(payload).hmac
# --------------------------------------------------------------------------- #
def _sign(payload_b64: str, generation: int) -> str:
    mac = hmac.new(SECRET, f"{generation}:{payload_b64}".encode(), hashlib.sha256)
    return f"{generation}.{payload_b64}.{mac.hexdigest()}"


def make_cursor(start: str, end: str, offset: int, page_size: int) -> str:
    payload = {
        "start": start,
        "end": end,
        "offset": offset,
        "page_size": page_size,
        "exp": datetime.now(timezone.utc).timestamp() + CURSOR_TTL_SECONDS,
    }
    raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()
    payload_b64 = base64.urlsafe_b64encode(raw).decode().rstrip("=")
    return _sign(payload_b64, STATE.cursor_generation)


def parse_cursor(cursor: str) -> dict:
    """Validate signature, generation and TTL. Raises HTTPException(410)."""
    try:
        gen_str, payload_b64, sig = cursor.split(".", 2)
        generation = int(gen_str)
    except (ValueError, AttributeError):
        raise HTTPException(status_code=410, detail="cursor invalid or expired")
    expected = _sign(payload_b64, generation)
    if not hmac.compare_digest(expected, cursor):
        raise HTTPException(status_code=410, detail="cursor invalid or expired")
    if generation != STATE.cursor_generation:
        raise HTTPException(status_code=410, detail="cursor invalid or expired")
    try:
        padded = payload_b64 + "=" * (-len(payload_b64) % 4)
        payload = json.loads(base64.urlsafe_b64decode(padded.encode()))
    except Exception:
        raise HTTPException(status_code=410, detail="cursor invalid or expired")
    if float(payload.get("exp", 0)) < datetime.now(timezone.utc).timestamp():
        raise HTTPException(status_code=410, detail="cursor invalid or expired")
    return payload


# --------------------------------------------------------------------------- #
# Schemas
# --------------------------------------------------------------------------- #
class ChangeEvent(BaseModel):
    paper_id: str
    rev: int
    title: str
    authors: str
    is_retracted: int = Field(ge=0, le=1)
    changed_at: str


class ChangesResponse(BaseModel):
    changes: list[ChangeEvent]
    next_cursor: str | None
    next_offset: int
    page: int
    page_size: int
    total_matched: int
    done: bool


class AppendRequest(BaseModel):
    events: list[ChangeEvent]


class FailRequest(BaseModel):
    times: int = Field(ge=0)


class ExpireAfterRequest(BaseModel):
    requests: int = Field(ge=0, description="invalidate cursors after this many /changes calls; 0 disables")


# --------------------------------------------------------------------------- #
# Core change feed
# --------------------------------------------------------------------------- #
@app.get("/health")
def health() -> dict:
    return {"status": "ok", "events": len(_all_events()), "page_size": PAGE_SIZE}


@app.get("/changes", response_model=ChangesResponse)
def get_changes(
    start: str = Query(..., description="closed interval start, ISO-8611 UTC"),
    end: str = Query(..., description="closed interval end, ISO-8611 UTC"),
    cursor: str | None = Query(None),
) -> ChangesResponse:
    # Deterministic fault injection for the disconnect demo.
    with _lock:
        STATE.changes_calls += 1
        if STATE.fail_next > 0:
            STATE.fail_next -= 1
            raise HTTPException(status_code=503, detail="injected transient source failure")

    if start > end:
        raise HTTPException(status_code=400, detail="start must be <= end (ISO-8601 UTC)")

    page_size = PAGE_SIZE
    if cursor is None:
        offset = 0
    else:
        payload = parse_cursor(cursor)  # raises 410 when expired/invalid
        if payload["start"] != start or payload["end"] != end:
            # A cursor may only be resumed inside its original interval.
            raise HTTPException(status_code=410, detail="cursor invalid or expired")
        offset = int(payload["offset"])
        page_size = int(payload["page_size"])

    matched = [e for e in _all_events() if start <= e["changed_at"] <= end]
    page_rows = matched[offset:offset + page_size]
    next_offset = offset + len(page_rows)
    done = next_offset >= len(matched)
    next_cursor = None if done else make_cursor(start, end, next_offset, page_size)
    page = offset // page_size + 1 if page_size else 1

    response = ChangesResponse(
        changes=[ChangeEvent(**row) for row in page_rows],
        next_cursor=next_cursor,
        next_offset=next_offset,
        page=page,
        page_size=page_size,
        total_matched=len(matched),
        done=done,
    )

    # Deterministic cursor-expiry hook. The bump happens AFTER the response
    # (and its cursor) has been built under the current generation, so the
    # cursor handed out on this request is rejected only on the NEXT request:
    # the collector durably checkpointed a page, then gets 410 and must
    # re-fetch the original interval from offset 0.
    with _lock:
        if STATE.expire_after_requests > 0:
            STATE.expire_after_requests -= 1
            if STATE.expire_after_requests == 0:
                STATE.cursor_generation += 1

    return response


# --------------------------------------------------------------------------- #
# Control API (demo helpers; the collector never calls these)
# --------------------------------------------------------------------------- #
@app.post("/control/fail")
def control_fail(req: FailRequest) -> dict:
    with _lock:
        if req.times == 0:
            # Explicit clear of any queued fault injections.
            STATE.fail_next = 0
        else:
            STATE.fail_next += req.times
        remaining = STATE.fail_next
    return {"fail_next": remaining}


@app.post("/control/expire-after")
def control_expire_after(req: ExpireAfterRequest) -> dict:
    """Schedule a deterministic cursor invalidation after N /changes calls."""
    with _lock:
        STATE.expire_after_requests = req.requests
        return {
            "expire_after_requests": STATE.expire_after_requests,
            "cursor_generation": STATE.cursor_generation,
        }


@app.post("/control/expire-cursors")
def control_expire_cursors() -> dict:
    with _lock:
        STATE.cursor_generation += 1
    return {"cursor_generation": STATE.cursor_generation, "status": "all cursors invalidated"}


@app.post("/control/append-batch")
def control_append_batch() -> dict:
    """Append the prepared incremental batch (idempotent per demo run)."""
    with _lock:
        if not STATE.appended:
            STATE.appended = [dict(e) for e in APPENDED_EVENTS]
            added = len(STATE.appended)
        else:
            added = 0
        total = len(_events_locked())
    return {"added": added, "total_events": total}


@app.post("/control/append")
def control_append(req: AppendRequest) -> dict:
    with _lock:
        STATE.appended.extend(e.model_dump() for e in req.events)
        total = len(_events_locked())
    return {"total_events": total}


@app.post("/control/reset")
def control_reset() -> dict:
    with _lock:
        STATE.appended = []
        STATE.fail_next = 0
        STATE.cursor_generation = 0
        STATE.expire_after_requests = 0
        STATE.changes_calls = 0
        total = len(_events_locked())
    return {"status": "reset", "total_events": total}


@app.get("/control/state")
def control_state() -> dict:
    with _lock:
        return {
            "total_events": len(_events_locked()),
            "appended": len(STATE.appended),
            "fail_next": STATE.fail_next,
            "cursor_generation": STATE.cursor_generation,
            "expire_after_requests": STATE.expire_after_requests,
            "changes_calls": STATE.changes_calls,
            "cursor_ttl_seconds": CURSOR_TTL_SECONDS,
        }


@app.get("/events")
def list_events() -> dict:
    return {"events": _all_events()}
