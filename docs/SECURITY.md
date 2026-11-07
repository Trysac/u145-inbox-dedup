# Security Policy and Threat Model

## Threat Model

### Defended

- **Untrusted Message Identifiers**: Message IDs are strictly validated to be strings between 1 and 255 characters, disallowing ASCII control characters (code < 32) and DEL (code 127).
- **SQL Injection**: Table names are validated against the strict regex `^[a-z_][a-z0-9_]{0,62}$`. All queries use prepared statement placeholders (`?`).
- **Denial of Service (DoS)**: To prevent excessive storage or memory consumption, JSON-serialized results are strictly limited to 65,536 bytes.
- **Information Leakage**: Operational events (via `on_event`) and internal logging (using `NullHandler` by default) do not capture or leak sensitive handler result payloads or business data.

### Not Defended

- **External Side Effects**: Side effects occurring outside the database (e.g., HTTP requests, file system operations) are not covered by the `ROLLBACK` mechanism. Consumers performing such tasks should implement their own idempotency keys.
- **Storage Exhaustion**: The `InMemoryInbox` consumes RAM proportional to `max_entries`. It does not provide per-message memory limits beyond simple entry counts.