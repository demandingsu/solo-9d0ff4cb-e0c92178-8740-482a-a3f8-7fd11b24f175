"""Source service tests: paging, cursor expiry, and the non-reentrant lock.

The lock regression test matters: control endpoints must not call another
lock-acquiring helper while already holding the global state lock.
"""
from __future__ import annotations

import threading

import pytest
from fastapi.testclient import TestClient

from source.app.main import STATE, _all_events, _lock, app


@pytest.fixture
def client():
    with TestClient(app) as c:
        yield c


def test_lock_is_never_reentered(client):
    # Every control endpoint must return promptly even though the state lock is
    # non-reentrant. A self-deadlock would make these time out.
    done = threading.Event()
    errors: list[str] = []

    def exercise():
        try:
            for call in (
                lambda: client.get("/health"),
                lambda: client.get("/control/state"),
                lambda: client.post("/control/reset"),
                lambda: client.post(
                    "/control/append",
                    json={"events": [{
                        "paper_id": "X1", "rev": 1, "title": "t",
                        "authors": "a", "is_retracted": 0,
                        "changed_at": "2026-11-09T00:00:00Z",
                    }]},
                ),
                lambda: client.post("/control/append-batch"),
                lambda: client.get("/events"),
            ):
                r = call()
                assert r.status_code == 200, r.text
        except Exception as exc:  # pragma: no cover
            errors.append(repr(exc))
        finally:
            done.set()

    t = threading.Thread(target=exercise, daemon=True)
    t.start()
    if not done.wait(timeout=10):
        pytest.fail("source endpoint self-deadlocked on the non-reentrant lock")
    assert not errors


def test_closed_interval_paging_and_dedup_inputs(client):
    client.post("/control/reset")
    r = client.get(
        "/changes",
        params={"start": "2026-11-01T00:00:00Z", "end": "2026-11-04T00:00:00Z"},
    )
    assert r.status_code == 200
    first = r.json()
    assert first["page_size"] == 3
    assert first["done"] is False and first["next_cursor"]
    cursor = first["next_cursor"]

    r2 = client.get(
        "/changes",
        params={"start": "2026-11-01T00:00:00Z",
                "end": "2026-11-04T00:00:00Z", "cursor": cursor},
    )
    assert r2.status_code == 200
    assert r2.json()["page"] == 2


def test_cursor_rejected_after_generation_bump(client):
    client.post("/control/reset")
    r = client.get(
        "/changes",
        params={"start": "2026-11-01T00:00:00Z", "end": "2026-11-08T00:00:00Z"},
    )
    cursor = r.json()["next_cursor"]
    client.post("/control/expire-cursors")
    r2 = client.get(
        "/changes",
        params={"start": "2026-11-01T00:00:00Z",
                "end": "2026-11-08T00:00:00Z", "cursor": cursor},
    )
    assert r2.status_code == 410


def test_cursor_bound_to_original_interval(client):
    client.post("/control/reset")
    r = client.get(
        "/changes",
        params={"start": "2026-11-01T00:00:00Z", "end": "2026-11-04T00:00:00Z"},
    )
    cursor = r.json()["next_cursor"]
    # Reuse the cursor against a different (newer) interval -> 410, never a
    # silent jump to the newer cutoff.
    r2 = client.get(
        "/changes",
        params={"start": "2026-11-01T00:00:00Z",
                "end": "2026-11-09T00:00:00Z", "cursor": cursor},
    )
    assert r2.status_code == 410


def test_injected_failure_then_recovery(client):
    client.post("/control/reset")
    client.post("/control/fail", json={"times": 1})
    r1 = client.get(
        "/changes",
        params={"start": "2026-11-01T00:00:00Z", "end": "2026-11-04T00:00:00Z"},
    )
    assert r1.status_code == 503
    r2 = client.get(
        "/changes",
        params={"start": "2026-11-01T00:00:00Z", "end": "2026-11-04T00:00:00Z"},
    )
    assert r2.status_code == 200
    assert _lock.acquire(blocking=False)  # lock must be free
    _lock.release()
    assert STATE.fail_next == 0


def test_expire_after_hook_invalidates_next_cursor(client):
    client.post("/control/reset")
    # Wide window covers all seed events (multiple pages), so the first call
    # always returns a continuation cursor.
    params = {"start": "1970-01-01T00:00:00Z", "end": "2026-12-31T23:59:59Z"}
    # First /changes call succeeds; afterwards the generation is bumped, so the
    # cursor returned by that call is rejected on the following request.
    client.post("/control/expire-after", json={"requests": 1})
    r1 = client.get("/changes", params=params)
    assert r1.status_code == 200
    cursor = r1.json()["next_cursor"]
    assert cursor is not None  # multi-page window guarantees a cursor
    r2 = client.get("/changes", params={**params, "cursor": cursor})
    assert r2.status_code == 410
    # A request without a cursor still works (re-fetch from offset 0 allowed).
    r3 = client.get("/changes", params=params)
    assert r3.status_code == 200 and r3.json()["page"] == 1
