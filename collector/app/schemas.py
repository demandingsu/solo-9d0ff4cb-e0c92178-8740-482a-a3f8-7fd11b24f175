"""Pydantic request/response schemas."""
from __future__ import annotations

from pydantic import BaseModel, Field


class CreateJobRequest(BaseModel):
    source_url: str | None = Field(
        None, description="Change-feed base URL; defaults to SOURCE_URL env var"
    )
    end: str = Field(..., description="sync cutoff time, ISO-8601 UTC (closed interval end)")
    start: str | None = Field(
        None, description="interval start; defaults to last completed time, else full fetch"
    )


class JobResponse(BaseModel):
    id: int
    source_url: str
    interval_start: str
    interval_end: str
    status: str
    cursor: str | None
    next_offset: int
    pages_fetched: int
    fetch_done: bool
    cursor_resets: int
    staged_count: int
    error: str | None
    created_at: str
    updated_at: str


class StateResponse(BaseModel):
    source_url: str
    last_start: str | None
    last_end: str | None
    completed_at: str | None


class PaperResponse(BaseModel):
    paper_id: str
    rev: int
    title: str
    authors: str
    is_retracted: int
    changed_at: str
    updated_at: str
