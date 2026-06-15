import logging
import time
from datetime import datetime, timezone

import httpx
from sqlalchemy import select

from app.core.config import BATCH_SIZE, POLL_INTERVAL
from app.core.database import SessionLocal
from app.models.event import EventStatus, OutboxEvent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
log = logging.getLogger(__name__)


def claim_batch(db) -> list[OutboxEvent]:
    """SELECT … FOR UPDATE SKIP LOCKED — safe for multiple workers."""
    rows = (
        db.query(OutboxEvent)
        .filter(OutboxEvent.status == EventStatus.PENDING)
        .order_by(OutboxEvent.created_at)
        .limit(BATCH_SIZE)
        .with_for_update(skip_locked=True)
        .all()
    )
    for r in rows:
        r.status = EventStatus.PROCESSING
        r.updated_at = datetime.now(timezone.utc)
    db.commit()
    return rows


def deliver_one(event: OutboxEvent) -> tuple[bool, str]:
    """Synchronous POST to the customer.  Returns (ok, detail)."""
    try:
        with httpx.Client(timeout=30.0) as client:
            resp = client.post(
                event.target_url,
                json={
                    "event_id": str(event.id),
                    "event_type": event.event_type,
                    "payload": event.payload,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                },
                headers={
                    "Content-Type": "application/json",
                    "X-Webhook-ID": str(event.id),
                },
            )
        if resp.status_code == 200:
            return True, "200 OK"
        return False, f"HTTP {resp.status_code}"
    except httpx.TimeoutException:
        return False, "timeout"
    except httpx.RequestError as exc:
        return False, str(exc)


def run_loop():
    log.info("Outbox worker started  (batch=%d  poll=%.1fs)", BATCH_SIZE, POLL_INTERVAL)
    while True:
        db = SessionLocal()
        try:
            rows = claim_batch(db)
            if not rows:
                time.sleep(POLL_INTERVAL)
                continue

            delivered = 0
            for event in rows:
                ok, detail = deliver_one(event)
                now = datetime.now(timezone.utc)
                if ok:
                    event.status = EventStatus.DELIVERED
                    event.delivered_at = now
                    delivered += 1
                else:
                    event.status = EventStatus.FAILED
                    event.last_error = detail
                event.updated_at = now

            db.commit()
            log.info("Batch done: %d/%d delivered", delivered, len(rows))

        except Exception:
            log.exception("Worker loop error")
            db.rollback()
            time.sleep(POLL_INTERVAL)
        finally:
            db.close()


if __name__ == "__main__":
    run_loop()
