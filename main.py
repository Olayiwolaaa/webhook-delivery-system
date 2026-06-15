from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import httpx
import uuid
from datetime import datetime, timezone

app = FastAPI(title="Webhook Delivery System — Naive Approach")


class WebhookEvent(BaseModel):
    event_type: str
    payload: dict
    target_url: str


class DeliveryResult(BaseModel):
    event_id: str
    status: str
    status_code: int | None = None
    error: str | None = None
    delivered_at: str | None = None


@app.post("/webhooks/send", response_model=DeliveryResult)
async def send_webhook(event: WebhookEvent):
    event_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)

    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            resp = await client.post(
                event.target_url,
                json={
                    "event_id": event_id,
                    "event_type": event.event_type,
                    "payload": event.payload,
                    "timestamp": now.isoformat(),
                },
                headers={
                    "Content-Type": "application/json",
                    "X-Webhook-ID": event_id,
                },
            )
        except httpx.TimeoutException:
            raise HTTPException(status_code=504, detail="Customer endpoint timed out")
        except httpx.RequestError as exc:
            raise HTTPException(
                status_code=502, detail=f"Could not reach customer: {exc}"
            )

    if resp.status_code == 200:
        return DeliveryResult(
            event_id=event_id,
            status="delivered",
            status_code=200,
            delivered_at=now.isoformat(),
        )

    raise HTTPException(
        status_code=502,
        detail=f"Customer returned HTTP {resp.status_code}",
    )


@app.get("/health")
async def health():
    return {"status": "ok", "approach": "naive"}
