# Webhook Delivery System — Approach 3: Dispatch

Instead of a single worker grinding through events one by one, a dispatcher fans work out to a pool of async workers. Each customer endpoint is pinned to a specific worker via consistent hashing — so deliveries to the same URL are always sequential, but different URLs are processed in parallel.

## Why this approach?

The outbox worker from Approach 2 is a single process. At 10M events/day (~115/sec), one worker can't keep up — especially when customer endpoints are slow. You need parallelism, but naive parallelism creates race conditions: two workers hitting the same customer simultaneously can cause out-of-order delivery and duplicate processing.

The dispatcher solves this by assigning each target URL to exactly one worker, every time.

## Project Structure

```
├── app/
│   ├── core/
│   │   ├── config.py          # Environment-based configuration
│   │   └── database.py        # SQLAlchemy engine and session
│   ├── models/
│   │   └── event.py           # OutboxEvent with assigned_worker tracking
│   ├── routers/
│   │   └── webhooks.py        # API endpoints
│   ├── services/
│   │   ├── dispatch.py        # Reads outbox, assigns events to workers
│   │   └── worker.py          # Async worker pool with consistent hashing
│   └── main.py                # FastAPI app entry point
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md
```

## How it works

1. Your backend sends a POST to `/webhooks/send` — event is written to the outbox table
2. The dispatcher polls the outbox every second for pending events
3. Each event is assigned to a worker using a SHA-256 hash of the `target_url`
4. The worker POSTs the event to the customer's URL
5. If the customer returns 200 → event marked as `delivered`
6. If it fails → event marked as `failed`

Same URL always lands on the same worker. Different URLs fan out across the pool.

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

This starts Postgres, the API, and the dispatcher with its worker pool.

### Without Docker

You need Postgres running locally. Then open two terminals:

```bash
# Terminal 1: API
uvicorn app.main:app --reload --port 8000

# Terminal 2: Dispatcher + workers
python -m app.services.dispatch
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

## What this solves over Approach 2

- **Parallel delivery** — multiple workers process events simultaneously
- **Per-customer ordering** — consistent hashing prevents race conditions
- **Scalable** — add more workers via `WORKER_COUNT` env var

## What's still missing

- **No retries** — a failed event stays failed forever
- **No backoff** — if a customer is down, we don't wait and try again
- **No dead letter queue** — permanently failed events have nowhere to go

These problems are exactly what Approach 4 (Retry + Exponential Backoff + DLQ) solves.