from __future__ import annotations

import os
import time
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Request
from pydantic import ValidationError

from .models import PublishAccepted, StatsOut, parse_publish_body
from .processor import WorkerManager
from .queue import EventQueue, RedisEventQueue
from .storage import PostgresStorage, Storage


def create_app(
    storage: Storage | None = None,
    queue: EventQueue | None = None,
    start_workers: bool = True,
) -> FastAPI:
    worker_count = int(os.getenv("WORKER_COUNT", "4"))

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.started_at = time.monotonic()
        app.state.storage = storage or PostgresStorage(os.environ["DATABASE_URL"])
        app.state.queue = queue or RedisEventQueue(os.environ["BROKER_URL"])
        app.state.worker_manager = WorkerManager(
            app.state.storage,
            app.state.queue,
            worker_count,
        )
        await app.state.storage.init()
        await app.state.queue.init()
        if start_workers:
            app.state.worker_manager.start()
        try:
            yield
        finally:
            if start_workers:
                await app.state.worker_manager.stop()
            await app.state.queue.close()
            await app.state.storage.close()

    app = FastAPI(title="Distributed Pub-Sub Log Aggregator", lifespan=lifespan)

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/publish", response_model=PublishAccepted)
    async def publish(request: Request) -> PublishAccepted:
        body: Any = await request.json()
        try:
            events = parse_publish_body(body)
        except (ValidationError, ValueError) as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        await request.app.state.storage.record_received(len(events))
        for event in events:
            await request.app.state.queue.enqueue(event)
        return PublishAccepted(accepted=len(events), queued=len(events))

    @app.get("/events")
    async def events(
        request: Request,
        topic: str | None = Query(default=None, min_length=1),
    ):
        return await request.app.state.storage.list_events(topic)

    @app.get("/stats", response_model=StatsOut)
    async def stats(request: Request) -> StatsOut:
        counters = await request.app.state.storage.stats()
        topics = await request.app.state.storage.topics()
        return StatsOut(
            received=counters.get("received", 0),
            unique_processed=counters.get("unique_processed", 0),
            duplicate_dropped=counters.get("duplicate_dropped", 0),
            topics=topics,
            uptime_seconds=time.monotonic() - request.app.state.started_at,
            workers=worker_count,
        )

    return app
