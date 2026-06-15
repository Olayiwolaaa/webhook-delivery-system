from contextlib import asynccontextmanager

from fastapi import FastAPI

from core.database import init_db
from routers import webhooks

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield

app = FastAPI(title="Webhook Delivery System — Outbox", lifespan=lifespan)

app.include_router(webhooks.router)

@app.get("/health")
def health():
    return {"status": "ok", "approach": "outbox"}
