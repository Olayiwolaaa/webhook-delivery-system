import asyncio
import hashlib
import logging
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import httpx

from app.core.config import BACKOFF_BASE, BACKOFF_MAX_SECONDS, INITIAL_TIMEOUT, PERMANENT_ERROR_CODES
from app.core.database import SessionLocal
from app.models.event import DeadLetterEvent, EventStatus, OutboxEvent

log = logging.getLogger(__name__)

def _timeout_for_attempt(attempt: int) -> float:
    return INITIAL_TIMEOUT + (attempt * 2)

def _backoff_seconds(attempt: int) -> float:
    return min(BACKOFF_BASE**attempt, BACKOFF_MAX_SECONDS)

class WorkerPool:
    def __init__(self, size: int):
        self.size = size
        self._queues: list[asyncio.Queue] = []
        self._tasks: list[asyncio.Task] = []

    def pick_worker(self, target_url: str) -> int:
        h = int(hashlib.sha256(target_url.encode()).hexdigest(), 16)
        return h % self.size

    async def start(self):
        if self._tasks:
            return
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
                await self._attempt_delivery(wid, client, data)
    
    async def _attempt_delivery(self, wid: int, client: httpx.AsyncClient, data: dict):
        event_id = data["event_id"]
        attempt = data.get("retry_count", 0)
        timeout = _timeout_for_attempt(attempt)
        backoff = _backoff_seconds(attempt + 1)
        
        try:
            resp = await client.post(
                data["target_url"],
                json={
                    "event_id": event_id,
                    "event_type": data["event_type"],
                    "payload": data["payload"],
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "attempt": attempt + 1,
                },
                headers={
                    "Content-Type": "application/json",
                    "X-Webhook-ID": event_id,
                    "X-Webhook-Attempt": str(attempt + 1),
                },
                timeout=httpx.Timeout(timeout, connect=5.0),
            )
            status_code = resp.status_code
        except httpx.TimeoutException:
            status_code = None
            error_detail = f"timeout after {timeout}s"
        except httpx.RequestError as exc:
            status_code = None
            error_detail = str(exc)
        else:
            error_detail = f"HTTP {status_code}" if status_code != 200 else None
        
        if status_code == 200:
            await asyncio.to_thread(self._mark_delivered, event_id)
            log.info("Worker-%d %s delivered (attempt %d)", wid, event_id[:8], attempt + 1)
            return
        
        if status_code and status_code in PERMANENT_ERROR_CODES:
            await asyncio.to_thread(self._send_to_dlq, event_id, f"Permanent error: HTTP {status_code}", status_code, attempt + 1)
            log.warning("Worker-%d %s permanent failure HTTP %d → DLQ", wid, event_id[:8], status_code)
            return
        
        if attempt + 1 >= data["max_retries"]:
            reason = error_detail or f"Max retries ({data['max_retries']}) exhausted"
            await asyncio.to_thread(self._send_to_dlq, event_id, reason, status_code, attempt + 1)
            log.warning("Worker-%d %s retries exhausted → DLQ", wid, event_id[:8])
            return

        await asyncio.to_thread(self._schedule_retry, event_id, attempt + 1, error_detail, status_code)
        log.info("Worker-%d %s attempt %d failed (%s) — retry in %ds",
                 wid, event_id[:8], attempt + 1, error_detail,
                 int(backoff))

    @staticmethod
    def _mark_delivered(event_id: str):
        db = SessionLocal()
        try:
            row = db.query(OutboxEvent).filter(OutboxEvent.id == event_id).first()
            if not row:
                log.warning("_mark_delivered: Event %s not found in DB when marking delivered", event_id[:8])
                return
            now = datetime.now(timezone.utc)
            row.status = EventStatus.DELIVERED
            row.delivered_at = now
            row.updated_at = now
            db.commit()
        finally:
            db.close()

    @staticmethod
    def _schedule_retry(event_id: str, attempt: int, error_detail: str, status_code: int | None):
        db = SessionLocal()
        try:
            row = db.query(OutboxEvent).filter(OutboxEvent.id == event_id).first()
            if not row:
                log.warning("_schedule_retry: Event %s not found in DB when scheduling retry", event_id[:8])
                return
            now = datetime.now(timezone.utc)
            row.status = EventStatus.RETRYING
            row.retry_count = attempt
            row.last_error = error_detail
            row.last_http_status = status_code
            row.next_retry_at = now + timedelta(seconds=_backoff_seconds(attempt))
            row.updated_at = now
            db.commit()
        finally:
            db.close()
    
    @staticmethod
    def _send_to_dlq(event_id: str, reason: str, status_code: int | None, attempt: int):
        db = SessionLocal()
        try:
            row = db.query(OutboxEvent).filter(OutboxEvent.id == event_id).first()
            if not row:
                log.warning("_send_to_dlq: Event %s not found in DB when sending to DLQ", event_id[:8])
                return
            
            now = datetime.now(timezone.utc)
            row.status = EventStatus.DEAD
            row.last_error = reason
            row.last_http_status = status_code
            row.updated_at = now

            dlq_entry = DeadLetterEvent(
                id=uuid4(),
                original_event_id=row.id,
                event_type=row.event_type,
                payload=row.payload,
                target_url=row.target_url,
                failure_reason=reason,
                total_attempts=attempt,
                last_http_status=status_code,
            )
            db.add(dlq_entry)
            db.commit()
        finally:
            db.close()

    async def stop(self):
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)