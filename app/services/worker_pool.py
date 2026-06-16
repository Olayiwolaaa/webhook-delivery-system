import asyncio
import hashlib
import logging
from datetime import datetime, timezone

import httpx

from app.core.config import INITIAL_TIMEOUT
from app.core.database import SessionLocal
from app.models.event import EventStatus, OutboxEvent

log = logging.getLogger(__name__)


class WorkerPool:
    def __init__(self, size: int):
        self.size = size
        self._queues: list[asyncio.Queue] = []
        self._tasks: list[asyncio.Task] = []

    def pick_worker(self, target_url: str) -> int:
        h = int(hashlib.sha256(target_url.encode()).hexdigest(), 16)
        return h % self.size

    async def start(self):
        for wid in range(self.size):
            q: asyncio.Queue = asyncio.Queue(maxsize=2000)
            self._queues.append(q)
            self._tasks.append(asyncio.create_task(self._loop(wid, q)))

    async def enqueue(self, worker_id: int, event_data: dict):
        await self._queues[worker_id].put(event_data)

    async def _loop(self, wid: int, q: asyncio.Queue):
        log.info("Worker-%d started", wid)
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(INITIAL_TIMEOUT, connect=5.0)
        ) as client:
            while True:
                data = await q.get()
                await self._deliver(wid, client, data)

    async def _deliver(self, wid: int, client: httpx.AsyncClient, data: dict):
        event_id = data["event_id"]
        try:
            resp = await client.post(
                data["target_url"],
                json={
                    "event_id": event_id,
                    "event_type": data["event_type"],
                    "payload": data["payload"],
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                },
                headers={
                    "Content-Type": "application/json",
                    "X-Webhook-ID": event_id,
                },
            )
            ok = resp.status_code == 200
            detail = "200 OK" if ok else f"HTTP {resp.status_code}"
        except httpx.TimeoutException:
            ok, detail = False, "timeout"
        except httpx.RequestError as exc:
            ok, detail = False, str(exc)

        self._update_db(event_id, ok, detail)
        level = logging.INFO if ok else logging.WARNING
        log.log(level, "Worker-%d  %s  %s", wid, event_id[:8], detail)

    @staticmethod
    def _update_db(event_id: str, ok: bool, detail: str):
        db = SessionLocal()
        try:
            row = db.query(OutboxEvent).filter(OutboxEvent.id == event_id).first()
            if not row:
                return
            now = datetime.now(timezone.utc)
            if ok:
                row.status = EventStatus.DELIVERED
                row.delivered_at = now
            else:
                row.status = EventStatus.FAILED
                row.last_error = detail
            row.updated_at = now
            db.commit()
        finally:
            db.close()

    def stop(self):
        for t in self._tasks:
            t.cancel()
