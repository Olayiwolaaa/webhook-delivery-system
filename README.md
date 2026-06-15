# Webhook Delivery System — Approach 1: Naive

The simplest possible webhook delivery: receive an event, immediately POST it to the customer's URL, wait for a response, and return the result.

## Why start here?

This approach exists to expose what breaks at scale. There's no queue, no persistence, no retries. If the customer's server is slow, your API blocks. If it's down, the event is lost forever. At 10M events/day (~115/sec), this falls apart fast — but understanding *why* it fails is the foundation for every approach that follows.

## Project Structure

```
├── main.py             # FastAPI app — single POST endpoint
├── shared/
│   ├── config.py       # Environment-based configuration
│   └── database.py     # Postgres setup (unused in this approach)
├── requirements.txt
├── .gitignore
├── .env.example
└── README.md
```

## How it works

1. Your backend sends a POST to `/webhooks/send` with an event type, payload, and customer URL
2. The server immediately forwards that payload to the customer's URL
3. If the customer returns 200 → success
4. If the customer is slow or down → your API caller gets an error

That's it. No safety net.

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

```bash
uvicorn main:app --reload --port 8000
```

## Test

```bash
curl -X POST http://localhost:8000/webhooks/send \
  -H "Content-Type: application/json" \
  -d '{"event_type": "order.created", "payload": {"order_id": 123}, "target_url": "https://httpbin.org/post"}'
```

API docs at: `http://localhost:8000/docs`

## What's wrong with this approach?

- **Blocking** — your API waits for the customer to respond before replying to the caller
- **No persistence** — if your server crashes mid-delivery, the event is gone
- **No retries** — one failure and the event is lost
- **No backpressure** — a slow customer slows down your entire API

These problems are exactly what Approach 2 (Outbox Pattern) solves.
