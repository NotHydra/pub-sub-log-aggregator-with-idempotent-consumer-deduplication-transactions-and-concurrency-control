from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from app.models import EventIn, EventOut


class InMemoryStorage:
    def __init__(self) -> None:
        self.lock = asyncio.Lock()
        self.events: dict[tuple[str, str], EventOut] = {}
        self.counters = {
            "received": 0,
            "unique_processed": 0,
            "duplicate_dropped": 0,
        }

    async def init(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def record_received(self, count: int) -> None:
        async with self.lock:
            self.counters["received"] += count

    async def process_event(self, event: EventIn) -> bool:
        async with self.lock:
            key = (event.topic, event.event_id)
            if key in self.events:
                self.counters["duplicate_dropped"] += 1
                return False
            self.events[key] = EventOut(
                topic=event.topic,
                event_id=event.event_id,
                timestamp=event.timestamp,
                source=event.source,
                payload=event.payload,
                processed_at=datetime.now(timezone.utc),
            )
            self.counters["unique_processed"] += 1
            return True

    async def list_events(self, topic: str | None = None) -> list[EventOut]:
        values = list(self.events.values())
        if topic:
            values = [event for event in values if event.topic == topic]
        return sorted(values, key=lambda event: (event.timestamp, event.event_id))

    async def stats(self) -> dict[str, int]:
        return dict(self.counters)

    async def topics(self) -> dict[str, int]:
        result: dict[str, int] = {}
        for topic, _ in self.events:
            result[topic] = result.get(topic, 0) + 1
        return result


class InMemoryQueue:
    def __init__(self) -> None:
        self.queue: asyncio.Queue[EventIn] = asyncio.Queue()

    async def init(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def enqueue(self, event: EventIn) -> None:
        await self.queue.put(event)

    async def dequeue(self, timeout: int = 1) -> EventIn | None:
        try:
            if timeout == 0:
                return self.queue.get_nowait()
            return await asyncio.wait_for(self.queue.get(), timeout=timeout)
        except (asyncio.QueueEmpty, TimeoutError):
            return None
