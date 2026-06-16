# Webhook Delivery System — Approach 4: Retry + Exponential Backoff + DLQ

Building on the dispatcher from Approach 3, this adds retry logic with exponential backoff and a dead letter queue. Failed deliveries are retried with increasing delays, and events that exhaust all retries are moved to a DLQ for investigation.

## Why this approach?

Approach 3 fans events across workers but treats every failure as final — one timeout and the event is dead. In reality, customer endpoints go down temporarily, deploy, or rate-limit. You need a system that retries with patience, backs off to avoid hammering struggling endpoints, and has a clear path for events that truly can't be delivered.

## Project Structure

```
├── app/
│   ├── core/
│   │   ├── config.py            # Backoff, timeout, and retry configuration
│   │   └── database.py          # SQLAlchemy engine and session
│   ├── models/
│   │   └── event.py             # OutboxEvent, DeadLetterEvent, EventStatus
│   ├── routers/
│   │   └── webhooks.py          # API endpoints
│   ├── services/
│   │   ├── dispatcher.py        # Polls outbox, assigns events to workers
│   │   └── worker_pool.py       # Async worker pool with retry + backoff
│   └── main.py                  # FastAPI app entry point
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md
```

## How it works

1. Your backend sends a POST to `/webhooks/send` — event is written to the outbox table
2. The dispatcher polls the outbox for `PENDING` events and `RETRYING` events whose `next_retry_at` has passed
3. Each event is assigned to a worker using a SHA-256 hash of the `target_url`
4. The worker POSTs the event to the customer's URL with a per-attempt timeout
5. On success (HTTP 200) → event marked as `DELIVERED`
6. On permanent error (4xx) → event sent directly to the dead letter queue
7. On transient failure (timeout, 5xx, network error) → event marked as `RETRYING` with a backoff delay
8. If all retries are exhausted → event moved to the dead letter queue

Same URL always lands on the same worker. Backoff increases exponentially per attempt.

## Retry and backoff behavior

- **Exponential backoff**: delay = `BACKOFF_BASE ^ attempt`, capped at `BACKOFF_MAX_SECONDS`
- **Per-attempt timeout**: increases with each attempt (`INITIAL_TIMEOUT + attempt * 2`)
- **Permanent errors**: HTTP status codes in `PERMANENT_ERROR_CODES` (e.g., 400, 401, 403, 404, 422) skip retries entirely
- **Dead letter queue**: events that exhaust `max_retries` or hit permanent errors are stored in `dead_letter_events` with full failure context

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

This starts Postgres, runs migrations, then launches the API and dispatcher with its worker pool.

### Without Docker

You need Postgres running locally. Then open two terminals:

```bash
# Terminal 1: API
uvicorn app.main:app --reload --port 8000

# Terminal 2: Dispatcher + workers
python -m app.services.dispatcher
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

## What this solves over Approach 3

- **Automatic retries** — transient failures are retried instead of abandoned
- **Exponential backoff** — struggling endpoints aren't hammered with immediate retries
- **Permanent error detection** — 4xx errors skip retries and go straight to the DLQ
- **Dead letter queue** — permanently failed events are preserved with failure context for debugging
- **Adaptive timeouts** — later attempts get more time, accommodating slow-recovering endpoints
