from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import pytest
from httpx import ASGITransport, AsyncClient
from pydantic import ValidationError

from app.main import create_app
from app.models import EventIn, parse_publish_body
from app.processor import WorkerManager
from tests.fakes import InMemoryQueue, InMemoryStorage


def event(event_id: str, topic: str = "app.logs", sequence: int = 1) -> dict:
    return {
        "topic": topic,
        "event_id": event_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "test",
        "payload": {"sequence": sequence},
    }


@pytest.fixture
async def client_context():
    storage = InMemoryStorage()
    queue = InMemoryQueue()
    app = create_app(storage=storage, queue=queue, start_workers=False)
    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            yield client, storage, queue, WorkerManager(storage, queue, worker_count=4)


async def drain_all(manager: WorkerManager) -> None:
    while await manager.drain_once():
        pass


def test_valid_event_schema_accepts_iso_timestamp() -> None:
    parsed = EventIn.model_validate(event("a"))
    assert parsed.topic == "app.logs"
    assert parsed.timestamp.tzinfo is not None


def test_event_schema_rejects_blank_topic() -> None:
    body = event("a")
    body["topic"] = " "
    with pytest.raises(ValidationError):
        EventIn.model_validate(body)


def test_event_schema_rejects_missing_timezone() -> None:
    body = event("a")
    body["timestamp"] = "2026-06-18T10:00:00"
    with pytest.raises(ValidationError):
        EventIn.model_validate(body)


def test_parse_single_publish_body() -> None:
    assert len(parse_publish_body(event("a"))) == 1


def test_parse_batch_publish_body() -> None:
    assert len(parse_publish_body({"events": [event("a"), event("b")]})) == 2


async def test_publish_single_event_returns_accepted(client_context) -> None:
    client, _, _, _ = client_context
    response = await client.post("/publish", json=event("a"))
    assert response.status_code == 200
    assert response.json() == {"accepted": 1, "queued": 1}


async def test_publish_invalid_event_returns_422(client_context) -> None:
    client, _, _, _ = client_context
    response = await client.post("/publish", json={"topic": "app.logs"})
    assert response.status_code == 422


async def test_dedup_processes_duplicate_once(client_context) -> None:
    client, _, _, manager = client_context
    await client.post("/publish", json={"events": [event("same"), event("same")]})
    await drain_all(manager)
    response = await client.get("/events")
    assert len(response.json()) == 1


async def test_same_event_id_on_different_topic_is_unique(client_context) -> None:
    client, _, _, manager = client_context
    await client.post("/publish", json={"events": [event("same", "a"), event("same", "b")]})
    await drain_all(manager)
    response = await client.get("/events")
    assert len(response.json()) == 2


async def test_events_filter_by_topic(client_context) -> None:
    client, _, _, manager = client_context
    await client.post("/publish", json={"events": [event("1", "a"), event("2", "b")]})
    await drain_all(manager)
    response = await client.get("/events", params={"topic": "a"})
    assert [item["topic"] for item in response.json()] == ["a"]


async def test_stats_after_unique_and_duplicate_events(client_context) -> None:
    client, _, _, manager = client_context
    await client.post("/publish", json={"events": [event("1"), event("1"), event("2")]})
    await drain_all(manager)
    stats = (await client.get("/stats")).json()
    assert stats["received"] == 3
    assert stats["unique_processed"] == 2
    assert stats["duplicate_dropped"] == 1
    assert stats["topics"] == {"app.logs": 2}


async def test_concurrent_workers_do_not_double_process_same_event() -> None:
    storage = InMemoryStorage()
    queue = InMemoryQueue()
    duplicate = EventIn.model_validate(event("race"))
    for _ in range(100):
        await queue.enqueue(duplicate)
    manager = WorkerManager(storage, queue, worker_count=8)
    await asyncio.gather(*(manager.drain_once() for _ in range(100)))
    stats = await storage.stats()
    assert stats["unique_processed"] == 1
    assert stats["duplicate_dropped"] == 99


async def test_counter_updates_are_consistent_under_parallel_processing() -> None:
    storage = InMemoryStorage()
    events = [EventIn.model_validate(event(str(index))) for index in range(100)]
    await asyncio.gather(*(storage.process_event(item) for item in events))
    stats = await storage.stats()
    assert stats["unique_processed"] == 100


async def test_batch_stress_small_completes_with_expected_duplicate_rate(client_context) -> None:
    client, _, _, manager = client_context
    unique = [event(str(index), sequence=index) for index in range(70)]
    duplicate = unique[:30]
    await client.post("/publish", json={"events": unique + duplicate})
    await drain_all(manager)
    stats = (await client.get("/stats")).json()
    assert stats["received"] == 100
    assert stats["unique_processed"] == 70
    assert stats["duplicate_dropped"] == 30


async def test_health_endpoint(client_context) -> None:
    client, _, _, _ = client_context
    response = await client.get("/health")
    assert response.json() == {"status": "ok"}
