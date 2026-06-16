import asyncio
import logging
from datetime import datetime, timezone

from app.core.config import BATCH_SIZE, POLL_INTERVAL, WORKER_COUNT
from app.core.database import SessionLocal
from app.models.event import EventStatus, OutboxEvent
from app.services.worker_pool import WorkerPool

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s"
)
log = logging.getLogger(__name__)


class Dispatcher:
    def __init__(self, worker_count: int = WORKER_COUNT):
        self.pool = WorkerPool(worker_count)
        self._running = False

    async def run(self):
        self._running = True
        await self.pool.start()
        log.info("Dispatcher up — %d workers", self.pool.size)

        while self._running:
            dispatched = await self._dispatch_batch()
            if dispatched == 0:
                await asyncio.sleep(POLL_INTERVAL)

    async def _dispatch_batch(self) -> int:
        db = SessionLocal()
        try:
            rows = (
                db.query(OutboxEvent)
                .filter(OutboxEvent.status == EventStatus.PENDING)
                .order_by(OutboxEvent.created_at)
                .limit(BATCH_SIZE)
                .with_for_update(skip_locked=True)
                .all()
            )
            if not rows:
                return 0

            for row in rows:
                worker_id = self.pool.pick_worker(row.target_url)
                row.status = EventStatus.DISPATCHED
                row.assigned_worker = f"worker-{worker_id}"
                row.updated_at = datetime.now(timezone.utc)

                await self.pool.enqueue(
                    worker_id,
                    {
                        "event_id": str(row.id),
                        "event_type": row.event_type,
                        "payload": row.payload,
                        "target_url": row.target_url,
                    },
                )

            db.commit()
            log.info("Dispatched %d events", len(rows))
            return len(rows)

        except Exception:
            db.rollback()
            log.exception("Dispatch batch failed")
            return 0
        finally:
            db.close()

    def stop(self):
        self._running = False
        self.pool.stop()


async def main():
    d = Dispatcher()
    try:
        await d.run()
    except KeyboardInterrupt:
        d.stop()


if __name__ == "__main__":
    asyncio.run(main())
