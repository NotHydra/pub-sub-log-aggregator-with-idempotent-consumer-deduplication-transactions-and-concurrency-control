from __future__ import annotations

import json
from typing import Protocol

try:
    import redis.asyncio as redis
except ModuleNotFoundError:  # pragma: no cover - exercised only without runtime deps
    redis = None

from .models import EventIn


class EventQueue(Protocol):
    async def init(self) -> None: ...
    async def close(self) -> None: ...
    async def enqueue(self, event: EventIn) -> None: ...
    async def dequeue(self, timeout: int = 1) -> EventIn | None: ...


class RedisEventQueue:
    def __init__(self, redis_url: str, queue_name: str = "events") -> None:
        self.redis_url = redis_url
        self.queue_name = queue_name
        self.client = None

    async def init(self) -> None:
        if redis is None:
            raise RuntimeError("redis package is required for RedisEventQueue")
        self.client = redis.from_url(self.redis_url, decode_responses=True)
        await self.client.ping()

    async def close(self) -> None:
        if self.client is not None:
            await self.client.aclose()

    async def enqueue(self, event: EventIn) -> None:
        assert self.client is not None
        await self.client.rpush(self.queue_name, event.model_dump_json())

    async def dequeue(self, timeout: int = 1) -> EventIn | None:
        assert self.client is not None
        item = await self.client.blpop(self.queue_name, timeout=timeout)
        if item is None:
            return None
        _, raw = item
        return EventIn.model_validate(json.loads(raw))
