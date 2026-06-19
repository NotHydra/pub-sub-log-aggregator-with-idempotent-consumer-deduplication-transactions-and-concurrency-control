from __future__ import annotations

import asyncio
import logging

from .queue import EventQueue
from .storage import Storage

logger = logging.getLogger(__name__)


class WorkerManager:
    def __init__(self, storage: Storage, queue: EventQueue, worker_count: int) -> None:
        self.storage = storage
        self.queue = queue
        self.worker_count = worker_count
        self._stop = asyncio.Event()
        self._tasks: list[asyncio.Task[None]] = []

    def start(self) -> None:
        for index in range(self.worker_count):
            self._tasks.append(asyncio.create_task(self._run(index), name=f"worker-{index}"))

    async def stop(self) -> None:
        self._stop.set()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)

    async def drain_once(self, timeout: int = 0) -> bool:
        event = await self.queue.dequeue(timeout=timeout)
        if event is None:
            return False
        inserted = await self.storage.process_event(event)
        if inserted:
            logger.info("processed topic=%s event_id=%s", event.topic, event.event_id)
        else:
            logger.info("duplicate_dropped topic=%s event_id=%s", event.topic, event.event_id)
        return True

    async def _run(self, index: int) -> None:
        logger.info("worker_started index=%s", index)
        while not self._stop.is_set():
            try:
                await self.drain_once(timeout=1)
            except Exception:
                logger.exception("worker_error index=%s", index)
                await asyncio.sleep(0.5)
        logger.info("worker_stopped index=%s", index)
