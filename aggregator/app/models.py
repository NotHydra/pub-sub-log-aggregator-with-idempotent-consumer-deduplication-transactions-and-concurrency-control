from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class EventIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    topic: str = Field(min_length=1, max_length=200)
    event_id: str = Field(min_length=1, max_length=200)
    timestamp: datetime
    source: str = Field(min_length=1, max_length=200)
    payload: dict[str, Any]

    @field_validator("topic", "event_id", "source")
    @classmethod
    def no_blank(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError("must not be blank")
        return stripped

    @field_validator("timestamp")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("timestamp must include timezone")
        return value.astimezone(timezone.utc)


class PublishAccepted(BaseModel):
    accepted: int
    queued: int


class EventOut(BaseModel):
    topic: str
    event_id: str
    timestamp: datetime
    source: str
    payload: dict[str, Any]
    processed_at: datetime


class StatsOut(BaseModel):
    received: int
    unique_processed: int
    duplicate_dropped: int
    topics: dict[str, int]
    uptime_seconds: float
    workers: int


def parse_publish_body(body: Any) -> list[EventIn]:
    if isinstance(body, dict) and "events" in body:
        events = body["events"]
        if not isinstance(events, list):
            raise ValueError("events must be a list")
        return [EventIn.model_validate(item) for item in events]
    return [EventIn.model_validate(body)]
