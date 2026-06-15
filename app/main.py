from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.core.database import init_db
from app.routers import webhooks

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield

app = FastAPI(title="Webhook Delivery System — Dispatch", lifespan=lifespan)

app.include_router(webhooks.router)

@app.get("/health")
def health():
    return {"status": "ok", "approach": "dispatch"}
