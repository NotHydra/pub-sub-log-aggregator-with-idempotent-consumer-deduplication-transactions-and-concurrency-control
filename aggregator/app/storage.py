from __future__ import annotations

import json
from typing import Protocol

try:
    import asyncpg
except ModuleNotFoundError:  # pragma: no cover - exercised only without runtime deps
    asyncpg = None

from .models import EventIn, EventOut


def _decode_payload(value):
    if isinstance(value, dict):
        return value
    return json.loads(value)


class Storage(Protocol):
    async def init(self) -> None: ...
    async def close(self) -> None: ...
    async def record_received(self, count: int) -> None: ...
    async def process_event(self, event: EventIn) -> bool: ...
    async def list_events(self, topic: str | None = None) -> list[EventOut]: ...
    async def stats(self) -> dict[str, int]: ...
    async def topics(self) -> dict[str, int]: ...


class PostgresStorage:
    def __init__(self, database_url: str) -> None:
        self.database_url = database_url
        self.pool = None

    async def init(self) -> None:
        if asyncpg is None:
            raise RuntimeError("asyncpg package is required for PostgresStorage")
        self.pool = await asyncpg.create_pool(self.database_url, min_size=1, max_size=10)
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS processed_events (
                    id BIGSERIAL PRIMARY KEY,
                    topic TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    event_timestamp TIMESTAMPTZ NOT NULL,
                    source TEXT NOT NULL,
                    payload JSONB NOT NULL,
                    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE (topic, event_id)
                );
                CREATE TABLE IF NOT EXISTS app_stats (
                    key TEXT PRIMARY KEY,
                    value BIGINT NOT NULL DEFAULT 0
                );
                INSERT INTO app_stats(key, value)
                VALUES ('received', 0), ('unique_processed', 0), ('duplicate_dropped', 0)
                ON CONFLICT (key) DO NOTHING;
                """
            )

    async def close(self) -> None:
        if self.pool is not None:
            await self.pool.close()

    async def record_received(self, count: int) -> None:
        await self._execute_counter("received", count)

    async def process_event(self, event: EventIn) -> bool:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            async with conn.transaction(isolation="read_committed"):
                inserted = await conn.fetchval(
                    """
                    INSERT INTO processed_events(topic, event_id, event_timestamp, source, payload)
                    VALUES ($1, $2, $3, $4, $5::jsonb)
                    ON CONFLICT (topic, event_id) DO NOTHING
                    RETURNING id
                    """,
                    event.topic,
                    event.event_id,
                    event.timestamp,
                    event.source,
                    json.dumps(event.payload),
                )
                key = "unique_processed" if inserted is not None else "duplicate_dropped"
                await conn.execute(
                    "UPDATE app_stats SET value = value + 1 WHERE key = $1",
                    key,
                )
                return inserted is not None

    async def list_events(self, topic: str | None = None) -> list[EventOut]:
        assert self.pool is not None
        query = """
            SELECT topic, event_id, event_timestamp AS timestamp, source, payload, processed_at
            FROM processed_events
        """
        params: tuple[str, ...] = ()
        if topic:
            query += " WHERE topic = $1"
            params = (topic,)
        query += " ORDER BY event_timestamp ASC, id ASC"
        rows = await self.pool.fetch(query, *params)
        return [
            EventOut(
                topic=row["topic"],
                event_id=row["event_id"],
                timestamp=row["timestamp"],
                source=row["source"],
                payload=_decode_payload(row["payload"]),
                processed_at=row["processed_at"],
            )
            for row in rows
        ]

    async def stats(self) -> dict[str, int]:
        assert self.pool is not None
        rows = await self.pool.fetch("SELECT key, value FROM app_stats")
        return {row["key"]: int(row["value"]) for row in rows}

    async def topics(self) -> dict[str, int]:
        assert self.pool is not None
        rows = await self.pool.fetch(
            "SELECT topic, count(*) AS count FROM processed_events GROUP BY topic ORDER BY topic"
        )
        return {row["topic"]: int(row["count"]) for row in rows}

    async def _execute_counter(self, key: str, delta: int) -> None:
        assert self.pool is not None
        await self.pool.execute(
            "UPDATE app_stats SET value = value + $1 WHERE key = $2",
            delta,
            key,
        )
