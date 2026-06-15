# Webhook Delivery System

A progressive exploration of webhook delivery architecture in Python, built with FastAPI and PostgreSQL. Each branch implements a different approach — starting from the simplest possible solution and evolving toward a production-grade system capable of handling 10M+ events/day.

## The Problem

You need to notify customers when events happen in your system. Sounds simple: send an HTTP POST to their URL. But at scale, everything breaks. Customers go down. Networks flake. Endpoints are slow. Events get lost. Retries pile up.

This project walks through five approaches to solving that problem, each one fixing what the previous one got wrong.

## Approaches

| Branch | Approach | What It Teaches |
|--------|----------|-----------------|
| [`naive`](https://github.com/Olayiwolaaa/webhook-delivery-system/tree/naive-approach) | Direct POST | What breaks when you deliver webhooks inline |
| [`outbox`](https://github.com/Olayiwolaaa/webhook-delivery-system/tree/outbox) | Transactional Outbox | Decoupling acceptance from delivery using Postgres |
| [`dispatch`](https://github.com/Olayiwolaaa/webhook-delivery-system/tree/dispatch) | Dispatcher + Worker Pool | Parallel delivery with consistent hashing |
| [`retry`](https://github.com/Olayiwolaaa/webhook-delivery-system/tree/retry) | Retry + Exponential Backoff | Resilience, timeout escalation, and dead letter queue |
| [`dlq-replay`](https://github.com/Olayiwolaaa/webhook-delivery-system/tree/dlq-replay) | DLQ + Replay | Recovery system with single and bulk replay |

## How to Read This Project

Start at `naive` and work your way down. Each branch builds on the previous one, and each README explains what the approach solves and what it still gets wrong. The progression is designed so you understand *why* each piece of infrastructure exists before you see it introduced.

### 1. [Naive](https://github.com/Olayiwolaaa/webhook-delivery-system/tree/naive)

Receive an event, immediately POST it to the customer, block until you get a response. No persistence, no retries. This is the baseline — it shows you exactly what fails at scale.

### 2. [Outbox](https://github.com/Olayiwolaaa/webhook-delivery-system/tree/outbox)

Write events to a Postgres outbox table, return `202 Accepted` immediately. A background worker polls the table and delivers. Events survive crashes, and your API never blocks on a customer's endpoint.

### 3. [Dispatch](https://github.com/Olayiwolaaa/webhook-delivery-system/tree/dispatch)

A dispatcher fans events out to a pool of async workers. Consistent hashing on `target_url` ensures the same customer always hits the same worker — parallel delivery without race conditions.

### 4. [Retry](https://github.com/Olayiwolaaa/webhook-delivery-system/tree/retry)

Workers retry failed deliveries with exponential backoff and escalating timeouts (10s, 12s, 14s...). Permanent errors (4xx) go straight to the dead letter queue. Transient errors (5xx, timeouts) get up to 5 retries.

### 5. [DLQ + Replay](https://github.com/Olayiwolaaa/webhook-delivery-system/tree/dlq-replay)

A full dead letter queue management API. Inspect failed events, replay them one at a time or in bulk, purge delivered replays. The replay system re-injects events into the outbox so they flow through the same pipeline with the same retry logic.

## Tech Stack

- **FastAPI** — async API framework
- **PostgreSQL** — event persistence and coordination
- **SQLAlchemy** — ORM and query builder
- **httpx** — async HTTP client for delivery
- **Docker** — containerized deployment

## Quick Start

Pick any branch and follow its README:

```bash
git clone https://github.com/Olayiwolaaa/webhook-delivery-system.git
cd webhook-delivery-system
git checkout outbox  # or naive-approach, dispatch, retry, dlq-replay
docker compose up --build
```

## License

MIT