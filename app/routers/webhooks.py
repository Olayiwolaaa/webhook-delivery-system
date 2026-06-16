from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.models.event import DeadLetterEvent, EventStatus, OutboxEvent

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

@router.get("/")
def list_events(db: Session = Depends(get_db)):
    events = db.query(OutboxEvent).order_by(OutboxEvent.created_at.desc()).all()
    return [
        {
            "event_id": str(event.id),
            "event_type": event.event_type,
            "status": event.status,
            "created_at": event.created_at.isoformat(),
            "delivered_at": event.delivered_at.isoformat() if event.delivered_at else None,
            "last_error": event.last_error,
        }
        for event in events
    ]

@router.delete("/{event_id}", status_code=204)
def delete_event(event_id: str, db: Session = Depends(get_db)): 
    row = db.query(OutboxEvent).filter(OutboxEvent.id == event_id).first()
    if not row:
        raise HTTPException(404, "Event not found")
    db.delete(row)
    db.commit()
    return

@router.delete("/", status_code=204)
def delete_all_events(db: Session = Depends(get_db)):
    db.query(OutboxEvent).delete()
    db.commit()
    return

@router.get("dlq")
def list_dlq(limit: int = 50, db: Session = Depends(get_db)):
    rows = (
        db.query(DeadLetterEvent)
        .order_by(DeadLetterEvent.created_at.desc())
        .limit(limit)
        .all()
    )
    return [
        {
            "id": str(r.id),
            "original_event_id": str(r.original_event_id),
            "event_type": r.event_type,
            "target_url": r.target_url,
            "failure_reason": r.failure_reason,
            "total_attempts": r.total_attempts,
            "last_http_status": r.last_http_status,
            "created_at": r.created_at.isoformat(),
        }
        for r in rows
    ]