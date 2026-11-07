# Testing

## Running

```sh
uv run pytest --cov=inbox_dedup
uv run python scripts/check.py
```

## Coverage

The test suite exercises the public API to ensure compliance with the following:

- `tests/test_inbox_dedup.py`: Covers the full lifecycle of `SqliteInbox` and `InMemoryInbox` handlers.
- Exercises processing logic: verifies that the handler is called exactly once for new messages and zero times for duplicates.
- Exercises failure modes: verifies that database transactions and in-memory marks roll back correctly when handlers raise exceptions (including `KeyboardInterrupt` and `ValueError`).
- Exercises validation logic: verifies strict enforcement of message ID lengths, character sets, table naming conventions, and JSON serialization size limits.
- Exercises concurrency: verifies that two threads contending for the same message ID correctly serialize access.
- Exercises retention: verifies that `purge` correctly removes data based on the provided clock and time delta.