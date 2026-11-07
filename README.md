# trysac-inbox-dedup

Transactional inbox pattern for idempotent message consumers in Python. This utility records incoming message IDs atomically within the worker's own SQLite transaction, ensuring redelivered messages are identified as duplicates without separate external store coordination.

## The Problem

Message brokers typically guarantee at-least-once delivery. When workers crash, disconnect, or rebalance, redelivered messages can trigger duplicate external effects (e.g., charging a card twice). Using an external store like Redis `SETNX` creates a race window: the worker might fail after updating Redis but before committing the database, or the database might roll back while Redis persists the mark.

`trysac-inbox-dedup` links the deduplication record directly to the database transaction of the work itself.

## Installation

```bash
pip install trysac-inbox-dedup
```

## Usage Example

```python
import sqlite3
from inbox_dedup import SqliteInbox

conn = sqlite3.connect(":memory:", timeout=5)
inbox = SqliteInbox(conn)


def handler(tx_conn):
    return {"status": "confirmed"}


outcome = inbox.handle("order-1", handler)
print(outcome.status)  # processed
```

## Public API Summary

- `SqliteInbox(conn, *, table="inbox", clock=utc_now, on_event=None)`: Creates a transactional inbox table if missing.
- `InMemoryInbox(*, max_entries=100000, clock=utc_now, on_event=None)`: Thread-safe in-memory deduplication with LRU eviction.
- `handle(message_id, handler)`: Processes the handler or returns a cached `Outcome` if duplicate.
- `purge(older_than)`: Removes stale records.
- `stats()`: Returns entries count and eviction count.

**Limits & Options**:
- Results are limited to 65,536 bytes of JSON-serialized data.
- Handlers must not manually commit the `sqlite3.Connection`.
- `InboxError` provides machine-readable codes: `invalid_message_id`, `invalid_table`, `invalid_result`, `result_too_large`, `in_transaction`.

## Why not Redis SETNX?

Because external caches like Redis operate outside the database transaction, they cannot guarantee atomicity between the message deduplication mark and the business logic commit. If the database transaction rolls back, the Redis mark remains, effectively dropping valid messages forever. `trysac-inbox-dedup` ensures the mark and the work are committed or rolled back together.

See [docs/SECURITY.md](docs/SECURITY.md) for threat modeling.