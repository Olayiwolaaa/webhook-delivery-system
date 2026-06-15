import enum
import uuid
from datetime import datetime, timezone

from sqlalchemy import Column, DateTime, Integer, String, Text
from sqlalchemy.dialects.postgresql import JSONB, UUID

from app.core.database import Base

class EventStatus(str, enum.Enum):
    PENDING = "pending"
    DISPATCHED = "dispatched"
    PROCESSING = "processing"
    DELIVERED = "delivered"
    FAILED = "failed"


class OutboxEvent(Base):
    __tablename__ = "outbox_events"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    event_type = Column(String(128), nullable=False, index=True)
    payload = Column(JSONB, nullable=False)
    target_url = Column(String(2048), nullable=False)
    status = Column(
        String(32), default=EventStatus.PENDING, nullable=False, index=True
    )
    assigned_worker = Column(String(64), nullable=True)
    created_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    updated_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )
    delivered_at = Column(DateTime(timezone=True), nullable=True)
    last_error = Column(Text, nullable=True)
    idempotency_key = Column(String(256), unique=True, nullable=True, index=True)
