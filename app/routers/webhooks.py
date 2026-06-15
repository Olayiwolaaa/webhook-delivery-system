from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.models.event import EventStatus, OutboxEvent

router = APIRouter(prefix="/webhooks", tags=["webhooks"])

class WebhookEventIn(BaseModel):
    event_type: str
    payload: dict
    target_url: str
    idempotency_key: str | None = None


class EventOut(BaseModel):
    event_id: str
    status: str
    message: str


@router.post("/send", response_model=EventOut, status_code=202)
def enqueue_webhook(body: WebhookEventIn, db: Session = Depends(get_db)):
    if body.idempotency_key:
        existing = (
            db.query(OutboxEvent)
            .filter(OutboxEvent.idempotency_key == body.idempotency_key)
            .first()
        )
        if existing:
            return EventOut(
                event_id=str(existing.id),
                status=existing.status,
                message="Duplicate — already enqueued",
            )

    row = OutboxEvent(
        id=uuid4(),
        event_type=body.event_type,
        payload=body.payload,
        target_url=body.target_url,
        status=EventStatus.PENDING,
        idempotency_key=body.idempotency_key,
    )
    db.add(row)
    db.commit()
    db.refresh(row)

    return EventOut(
        event_id=str(row.id),
        status="queued",
        message="Event written to outbox",
    )


@router.get("/{event_id}")
def get_status(event_id: str, db: Session = Depends(get_db)):
    row = db.query(OutboxEvent).filter(OutboxEvent.id == event_id).first()
    if not row:
        raise HTTPException(404, "Event not found")
    return {
        "event_id": str(row.id),
        "status": row.status,
        "created_at": row.created_at.isoformat(),
        "delivered_at": row.delivered_at.isoformat() if row.delivered_at else None,
        "last_error": row.last_error,
    }
