# Use SQLite BEGIN IMMEDIATE for Atomic Inbox Processing

Status: accepted (generated, pending human review)

## Context

Message brokers deliver at least once. When multiple consumer threads or processes receive identical message IDs concurrently, deduplication outside the database transaction leaves race conditions where both workers can pass the check before either commits. Additionally, handler failures could result in phantom deduplication if the dedup store commits separately.

## Decision

We use SQLite's `BEGIN IMMEDIATE` to acquire a write lock at the start of handling each message. If a duplicate exists, the transaction commits immediately and returns the stored result. If the message is new, the handler executes within that same transaction before inserting the message ID and committing. On any exception, SQLite rolls back both the business mutations and the inbox record.

## Consequences

Concurrent consumers for the same database file serialize cleanly without lost updates or duplicate execution. Handlers receive the open connection and must not issue commits directly.
