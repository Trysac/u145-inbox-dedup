# Project record

## Objective

Implement the Inbox pattern for idempotent message consumers, allowing the storage of message IDs directly within the consumer's transaction to ensure redelivered messages are ignored, in either SQLite or in-memory storage.

## Scope

- Support SQLite-backed transactional deduplication.
- Support thread-safe in-memory deduplication.
- Provide a consistent `handle` API that executes handlers atomically.
- Validate message IDs, table names, and result serialization constraints.

## Out of Scope

- Postgres or other non-SQLite database support.
- Message broker adapters (e.g., Kafka, RabbitMQ).
- Ordering guarantees (beyond transactional serializability).
- Outbox publishing patterns.
- Asynchronous / non-blocking I/O.

## Timeline

| Date | Event |
|---|---|
| 2026 | Repository created from the factory utility template. |

## Effort metrics

| Metric | Value |
|---|---|
| Tokens (input / output) | 1351209 / 17121 |
| Hours (human) | pending measurement |
| Cost (USD) | 0.2690 |

## Factory run u145 (measured from API usage)

| Phase | Model | Batch | Input | Output | Cache write | Cache read | Cost (USD) |
|---|---|---|---|---|---|---|---|
| generation | gemini-3.8-flash | yes | 4148 | 9055 | 0 | 0 | 0.0185 |
| fix-1 | gemini-3.8-flash | no | 49437 | 2547 | 0 | 295388 | 0.0688 |
| fix-2 | gemini-3.8-flash | no | 79821 | 3360 | 0 | 547211 | 0.1135 |
| audit | gemini-3.1-flash-lite | no | 49837 | 646 | 0 | 128151 | 0.0166 |
| docs | gemini-3.1-flash-lite | no | 197216 | 1513 | 0 | 0 | 0.0516 |

Total: $0.2690. Prices: Anthropic list prices.
