from __future__ import annotations

import os
import random
import time
import uuid
from datetime import datetime, timezone

import requests


TARGET_URL = os.getenv("TARGET_URL", "http://aggregator:8080/publish")
TOTAL_EVENTS = int(os.getenv("TOTAL_EVENTS", "20000"))
DUPLICATE_RATE = float(os.getenv("DUPLICATE_RATE", "0.30"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "250"))
TOPICS = [topic.strip() for topic in os.getenv("TOPICS", "app.logs,audit.logs").split(",")]
SOURCE = os.getenv("SOURCE", "publisher-1")


def make_event(topic: str, event_id: str, sequence: int) -> dict:
    return {
        "topic": topic,
        "event_id": event_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": SOURCE,
        "payload": {
            "sequence": sequence,
            "level": random.choice(["INFO", "WARN", "ERROR"]),
            "message": f"log event {sequence}",
        },
    }


def generate_events() -> list[dict]:
    unique_target = max(1, int(TOTAL_EVENTS * (1 - DUPLICATE_RATE)))
    uniques: list[dict] = []
    for index in range(unique_target):
        topic = TOPICS[index % len(TOPICS)]
        uniques.append(make_event(topic, str(uuid.uuid4()), index))

    events = list(uniques)
    while len(events) < TOTAL_EVENTS:
        events.append(random.choice(uniques).copy())
    random.shuffle(events)
    return events


def wait_for_aggregator() -> None:
    base = TARGET_URL.rsplit("/", 1)[0]
    for _ in range(60):
        try:
            if requests.get(f"{base}/health", timeout=2).ok:
                return
        except requests.RequestException:
            time.sleep(1)
    raise RuntimeError("aggregator did not become healthy")


def main() -> None:
    wait_for_aggregator()
    events = generate_events()
    started = time.perf_counter()
    sent = 0
    for offset in range(0, len(events), BATCH_SIZE):
        batch = events[offset : offset + BATCH_SIZE]
        response = requests.post(TARGET_URL, json={"events": batch}, timeout=30)
        response.raise_for_status()
        sent += len(batch)
        print(f"sent={sent}/{len(events)} response={response.json()}", flush=True)
    elapsed = time.perf_counter() - started
    print(f"done sent={sent} elapsed_seconds={elapsed:.2f}", flush=True)


if __name__ == "__main__":
    main()
