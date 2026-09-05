# Distributed Event Streaming Broker

A Kafka-style event streaming broker built in Go: topic/partition management, a real on-disk
append-only log (not an in-memory slice), pull-based consumer groups with durable offset
tracking, a dead-letter queue, time-based retention, WebSocket live-tail, Prometheus/Grafana
metrics, and a React monitoring dashboard.

Built from `01_Backend_Engineering/03_Event_Streaming_System` in a portfolio-project pipeline.
See **Design Decisions & Trade-offs** below for where and why this deliberately deviates from the
PRD's own reference sketch.

## Architecture

```
                    ┌─────────────┐
   producers ─────► │             │ ─────► on-disk segment logs (per topic/partition)
                    │   Broker    │
   consumers ◄───── │  (Go/chi)   │ ◄───── Postgres (topics, partitions, message index,
                    │             │         consumer offsets, DLQ)
                    └──────┬──────┘
                           │ WebSocket tail
                           ▼
                    React dashboard ◄──── Prometheus ◄──── /metrics
                                              │
                                          Grafana
```

Each topic-partition's messages live in length-prefixed, append-only segment files on disk
(`{DATA_DIR}/{topicID}/{partition}/{baseOffset}.log`), exactly like Kafka's own storage model.
Postgres holds metadata and a durable index mapping `(topic, partition, offset)` to
`(segment file, byte position, length)` — not the message bytes themselves. This is what lets a
restarted broker recover every partition's exact byte layout and next-offset counter from
Postgres alone, without re-scanning gigabytes of log files.

## Design Decisions & Trade-offs

- **Single broker, no Raft/replication.** The PRD's schema includes `replicas`/`isr`/
  `leader_broker` columns implying multi-broker replication ("3x replication factor"). Building
  real Raft-based replica sync is a multi-week distributed-systems project on its own and out of
  scope here — this is intentionally a single-broker system. The schema columns are kept (so the
  data model matches the PRD) but every partition's leader is hardcoded to `broker_id=1` and
  `replicas`/`isr` are always `[1]`.
- **Real on-disk segment log, not the PRD's in-memory `[]*Message`.** The PRD's own reference
  broker code stores messages in an in-memory Go slice, which loses every message on restart —
  not something that should be called durable. This build writes length-prefixed records to
  append-only segment files and indexes them in Postgres, and a message published before a broker
  restart is provably still there after (verified in `tests/integration/lifecycle_test.go`,
  `TestMessagesSurviveBrokerRestart` — actually kills and restarts the broker process/container
  and re-reads).
- **Postgres for metadata + offset index, files for message bytes** — the same split real Kafka
  makes (ZooKeeper/KRaft for metadata, log segments for data), rather than storing message bytes
  as Postgres rows, which doesn't scale to real event-streaming volumes.
- **WebSocket live-tail actually implemented.** The PRD lists "real-time processing with
  WebSocket subscriptions" as a feature but its own router sketch never wires an endpoint for it.
  `GET /api/v1/events/{topic}/ws` is implemented here, fed directly off the same `Publish` path
  the REST producer uses (not a separate queue), so a tailing client sees exactly what got
  durably written, in order.
- **DLQ reporting API defined explicitly.** The PRD's schema has a `dlq` table but never defines
  how a message lands in it. `POST /api/v1/dlq` lets a consumer that gives up retrying a message
  report it explicitly with a reason.
- **Commit-offset semantics centralized in one place.** `CommitOffset(group, topic, partition,
  lastProcessedOffset)` stores `lastProcessedOffset + 1` internally, so every caller (the HTTP
  handler and any future direct caller) gets correct "resume after this point" semantics for
  free, instead of each call site being responsible for the `+1`. This was a real bug caught by
  the integration test suite during development — the `+1` was originally only in the HTTP
  handler, so calling the broker API directly bypassed it and resumed from the wrong offset.
- **50,000 events/sec claim replaced with real measured numbers.** The PRD asserts 50K events/sec
  without ever running anything. `scripts/loadtest` is a real concurrent HTTP load generator; see
  **Load Test Results** below for what this machine actually measured, including a real client-side
  limitation it hit and how that was diagnosed.

## API Reference

All endpoints are under `http://localhost:8070/api/v1` when running via `docker compose up`.

| Method | Path | Description |
|---|---|---|
| `POST` | `/topics/` | Create a topic: `{name, partitions, replication_factor, retention_ms}` |
| `GET` | `/topics/` | List all topics |
| `DELETE` | `/topics/{topic}` | Delete a topic and its on-disk segments |
| `POST` | `/events/{topic}` | Publish: `{key, value, headers}` → `{partition, offset, timestamp}` |
| `GET` | `/events/{topic}/ws` | WebSocket: live-tail every message published to `topic` |
| `GET` | `/consumers/{group}/events/{topic}?partition=N&offset=(N\|earliest\|latest)&max_bytes=N` | Pull messages; omitting `offset` resumes from the group's last commit |
| `POST` | `/consumers/{group}/commit` | Commit: `{topic, partition, offset}` (offset = last message fully processed) |
| `POST` | `/dlq` | Report a failed message: `{topic, partition, offset, reason}` |
| `GET` | `/dlq` | List DLQ entries |
| `GET` | `/lag` | Consumer lag per group/partition (committed vs. latest offset) |
| `GET` | `/health` | Liveness check |
| `GET` | `/metrics` | Prometheus metrics |

## Running it

```bash
cp .env.example .env
docker compose up --build
```

- Broker API: http://localhost:8070
- Dashboard: http://localhost:5243
- Prometheus: http://localhost:9099
- Grafana: http://localhost:3016 (admin/admin)
- Postgres: localhost:5502

## Testing

```bash
make test              # unit tests (segment/partition log correctness, partition hashing)
make test-integration  # integration tests against a real Postgres (set DATABASE_URL)
```

Integration tests actually exercise the full stack: publish/consume/commit lifecycle, key-based
partition hashing, a real broker-restart-and-recover check, DLQ reporting, retention sweeping,
and topic deletion — all against a live Postgres, not mocks.

## Load Test Results

Run with `make loadtest` (or `go run ./scripts/loadtest`) against a live `docker compose` stack.

**First run** at `-concurrency 32` on this Windows dev machine produced a ~69% request failure
rate. Investigation (checking the broker's own logs, which showed *zero* non-2xx responses — only
successful 202s) showed the failures never reached the broker at all: they were client-side
`dial tcp: Only one usage of each socket address...` errors, i.e. Windows exhausting ephemeral
outbound ports/TIME_WAIT sockets when a Go HTTP client opens dozens of concurrent connections
through Docker Desktop's localhost port-forwarding layer. This is a Windows load-generator
limitation, not a broker capacity limit — documented in `scripts/loadtest/main.go`'s package
comment rather than hidden.

**Real measured result** at `-concurrency 16 -total 10000` (zero failures):

| Metric | Value |
|---|---|
| Messages sent | 10,000 / 10,000 (0 failed) |
| Wall time | 17.28s |
| Throughput | **579 events/sec** |
| Publish latency p50 | 24.6ms |
| Publish latency p99 | 65.7ms |

This is on a single broker instance, single dev machine, competing for CPU/IO with several other
concurrent Docker stacks from this same portfolio pipeline — not a dedicated benchmark
environment. It is nowhere near the PRD's asserted 50,000 events/sec, which assumes a
multi-broker cluster this build intentionally doesn't implement (see Design Decisions above). The
per-partition mutex serializing "reserve offset → append to segment → index in Postgres" (chosen
for correctness — see Design Decisions) is also a throughput ceiling worth calling out honestly:
each partition can only be written to by one goroutine at a time.

## Tech Stack

- **Broker**: Go, [chi](https://github.com/go-chi/chi) router, [pgx](https://github.com/jackc/pgx) (Postgres driver), [gorilla/websocket](https://github.com/gorilla/websocket)
- **Storage**: Postgres 16 (metadata + offset index) + on-disk append-only segment files (message bytes)
- **Frontend**: React + TypeScript + Vite, Recharts
- **Observability**: Prometheus + Grafana
- **CI**: GitHub Actions — unit tests, integration tests against a real Postgres service container, frontend build
