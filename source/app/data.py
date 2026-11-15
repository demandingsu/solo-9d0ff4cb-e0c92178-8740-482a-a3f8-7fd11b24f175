"""Immutable change-event dataset of the mock institutional repository.

Each event is an append-only, never-rewritten change record. The source
returns these records from a closed [start, end] interval in pages; the same
event may appear more than once and page order is not globally sorted, so
collectors must dedupe and keep the highest revision per paper.
"""
from __future__ import annotations

# Static seed events. `changed_at` is the repository-side immutable change
# time; `rev` is the monotonically increasing revision number of the paper.
SEED_EVENTS: list[dict] = [
    # --- P0001: created, then revised, then retracted ---
    {"paper_id": "P0001", "rev": 1, "title": "Graph Neural Networks for Citation Ranking",
     "authors": "A. Chen; B. Liu", "is_retracted": 0, "changed_at": "2026-11-01T08:00:00Z"},
    {"paper_id": "P0001", "rev": 2, "title": "Graph Neural Networks for Citation Ranking (v2)",
     "authors": "A. Chen; B. Liu; C. Wang", "is_retracted": 0, "changed_at": "2026-11-02T09:30:00Z"},
    {"paper_id": "P0001", "rev": 3, "title": "Graph Neural Networks for Citation Ranking (v2)",
     "authors": "A. Chen; B. Liu; C. Wang", "is_retracted": 1, "changed_at": "2026-11-05T14:10:00Z"},
    # --- P0002: created, revised twice ---
    {"paper_id": "P0002", "rev": 1, "title": "Open Access Discovery Interfaces",
     "authors": "D. Zhao", "is_retracted": 0, "changed_at": "2026-11-01T10:05:00Z"},
    {"paper_id": "P0002", "rev": 2, "title": "Open Access Discovery Interfaces",
     "authors": "D. Zhao; E. Sun", "is_retracted": 0, "changed_at": "2026-11-03T11:20:00Z"},
    {"paper_id": "P0002", "rev": 3, "title": "Open Access Discovery Interfaces: Extended Evaluation",
     "authors": "D. Zhao; E. Sun", "is_retracted": 0, "changed_at": "2026-11-06T16:45:00Z"},
    # --- P0003: created then retracted ---
    {"paper_id": "P0003", "rev": 1, "title": "Citation Graph Anomalies in Preprint Corpora",
     "authors": "F. Garcia", "is_retracted": 0, "changed_at": "2026-11-02T15:00:00Z"},
    {"paper_id": "P0003", "rev": 2, "title": "Citation Graph Anomalies in Preprint Corpora",
     "authors": "F. Garcia", "is_retracted": 1, "changed_at": "2026-11-07T09:00:00Z"},
    # --- P0004: single revision, plus a duplicated out-of-order delivery ---
    {"paper_id": "P0004", "rev": 1, "title": "Long-Term Preservation of Dataset Metadata",
     "authors": "G. Kim; H. Ito", "is_retracted": 0, "changed_at": "2026-11-04T12:00:00Z"},
    # Exact duplicate of the P0004 row (same revision, same payload).
    {"paper_id": "P0004", "rev": 1, "title": "Long-Term Preservation of Dataset Metadata",
     "authors": "G. Kim; H. Ito", "is_retracted": 0, "changed_at": "2026-11-04T12:00:00Z"},
    # Stale revision of P0002 redelivered late; must not overwrite rev 3.
    {"paper_id": "P0002", "rev": 1, "title": "Open Access Discovery Interfaces",
     "authors": "D. Zhao", "is_retracted": 0, "changed_at": "2026-11-01T10:05:00Z"},
    # --- P0005: created, later revised without retraction ---
    {"paper_id": "P0005", "rev": 1, "title": "Subject Heading Drift Across Decades",
     "authors": "I. Novak", "is_retracted": 0, "changed_at": "2026-11-05T08:40:00Z"},
    {"paper_id": "P0005", "rev": 2, "title": "Subject Heading Drift Across Decades",
     "authors": "I. Novak; J. Meyer", "is_retracted": 0, "changed_at": "2026-11-08T13:15:00Z"},
    # --- P0006: created ---
    {"paper_id": "P0006", "rev": 1, "title": "Link Resolver Accuracy in Discovery Layers",
     "authors": "K. Rossi", "is_retracted": 0, "changed_at": "2026-11-08T17:30:00Z"},
]

# Events appended at runtime by the demo control API (incremental sync batch).
APPENDED_EVENTS: list[dict] = [
    # P0007 new paper (two pages with page_size=3: 4 rows -> 2 pages)
    {"paper_id": "P0007", "rev": 1, "title": "Authority Control for Multilingual Repositories",
     "authors": "L. Tan", "is_retracted": 0, "changed_at": "2026-11-09T08:00:00Z"},
    # P0005 revised again (highest revision must win)
    {"paper_id": "P0005", "rev": 3, "title": "Subject Heading Drift Across Decades",
     "authors": "I. Novak; J. Meyer; L. Tan", "is_retracted": 0, "changed_at": "2026-11-10T09:20:00Z"},
    # P0002 retracted; an old non-retracted row of P0002 follows to test
    # that retraction is sticky and cannot be overwritten by an old revision.
    {"paper_id": "P0002", "rev": 4, "title": "Open Access Discovery Interfaces: Extended Evaluation",
     "authors": "D. Zhao; E. Sun", "is_retracted": 1, "changed_at": "2026-11-10T15:00:00Z"},
    {"paper_id": "P0002", "rev": 2, "title": "Open Access Discovery Interfaces",
     "authors": "D. Zhao; E. Sun", "is_retracted": 0, "changed_at": "2026-11-03T11:20:00Z"},
]
