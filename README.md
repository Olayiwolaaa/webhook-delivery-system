# Webhook Delivery System — Approach 2: Outbox Pattern

Instead of delivering webhooks inline, write every event to a Postgres outbox table and return immediately. A background worker polls the table and handles delivery. Your API never blocks on a customer's endpoint.

## Why this approach?

The naive approach ties your API's availability to your customer's uptime. If their server is slow, yours is slow. If they're down, you lose the event.

The outbox pattern breaks that dependency. Your API writes to a local database — fast, reliable, under your control — and a separate worker handles the messy business of HTTP delivery. Events survive crashes because they're in Postgres before anyone tries to deliver them.

## Project Structure

```
├── core/
│   ├── config.py          # Environment-based configuration
│   └── database.py        # SQLAlchemy engine and session
├── models/
│   └── event.py           # OutboxEvent SQLAlchemy model
├── routers/
│   └── webhooks.py        # API endpoints
├── services/
│   └── worker.py          # Background worker that polls and delivers
├── main.py                # FastAPI app entry point
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md
```

## How it works

1. Your backend sends a POST to `/webhooks/send` with an event type, payload, and customer URL
2. The API writes the event to the `outbox_events` table in Postgres and returns `202 Accepted`
3. A background worker polls the table every second using `SELECT ... FOR UPDATE SKIP LOCKED`
4. The worker POSTs each event to the customer's URL
5. If the customer returns 200 → event marked as `delivered`
6. If it fails → event marked as `failed` with the error

The API and the worker are separate processes. The database is the coordination point between them.

## Installation

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Or with uv:

```bash
uv venv
source .venv/bin/activate
cp .env.example .env
uv pip install -r requirements.txt
```

## Run

### With Docker (recommended)

```bash
docker compose up --build
```

This starts Postgres, the API, and the worker.

### Without Docker

You need Postgres running locally. Then open two terminals:

```bash
# Terminal 1: API
uvicorn main:app --reload --port 8000

# Terminal 2: Worker
python -m services.worker
```

## Test

```bash
curl -X POST http://localhost:8000/webhooks/send \
  -H "Content-Type: application/json" \
  -d '{"event_type": "order.created", "payload": {"order_id": 123}, "target_url": "https://httpbin.org/post"}'
```

Check delivery status:

```bash
curl http://localhost:8000/webhooks/{event_id}
```

API docs at: `http://localhost:8000/docs`

## What this solves over Approach 1

- **Non-blocking** — API returns 202 immediately, never waits on the customer
- **Persistent** — events are in Postgres before delivery is attempted, so crashes don't lose data
- **Idempotent** — duplicate events are caught via `idempotency_key`

## What's still wrong?

- **No retries** — a failed event stays failed forever
- **Single worker** — one process doing all deliveries is a bottleneck at scale
- **No backoff** — if a customer is down, we fail immediately with no second chance

These problems are exactly what Approach 3 ([Dispatch + Worker Pool](https://github.com/Olayiwolaaa/webhook-delivery-system/tree/dispatch)) solves.