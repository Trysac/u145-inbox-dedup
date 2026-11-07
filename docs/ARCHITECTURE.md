# Architecture

`trysac-inbox-dedup` implements the Inbox pattern for message consumers.

## Transactional Flow (SQLite)

1. Verification that the connection is not currently in a transaction (`conn.in_transaction is False`).
2. Acquire process thread lock and execute `BEGIN IMMEDIATE` on SQLite, obtaining an immediate reserved write lock.
3. Query `SELECT result FROM <table> WHERE message_id = ?`.
4. If an entry exists:
   - Commit transaction (`COMMIT`).
   - Emit `duplicate` event to `on_event` hook (without result payload).
   - Parse stored JSON and return `Outcome(status='duplicate', result=...)`.
5. If no entry exists:
   - Invoke `handler(conn)` passing the transactional connection.
   - Serialize handler result to JSON using deterministic formatting (`sort_keys=True, allow_nan=False`).
   - Verify payload size (`<= 65_536` bytes).
   - Insert message record with ISO-8601 UTC timestamp.
   - Commit transaction (`COMMIT`).
   - Emit `processed` event to `on_event` hook.
   - Return `Outcome(status='processed', result=...)`.
6. On any exception (including `BaseException`):
   - Execute `ROLLBACK`.
   - Re-raise original exception without swallow.

## In-Memory Flow

`InMemoryInbox` maintains an `OrderedDict` protected by a mutex lock. When `max_entries` is exceeded, the oldest entry by insertion order is evicted, updating `stats().evicted`.
