"""Inbox pattern for idempotent message consumers in SQLite or memory."""

from __future__ import annotations

import collections
import contextlib
import datetime
import json
import logging
import re
import sqlite3
import threading
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Generic, Literal, TypeVar

__all__ = [
    "InMemoryInbox",
    "InboxError",
    "Outcome",
    "SqliteInbox",
    "Stats",
    "__version__",
    "utc_now",
]

__version__ = "0.1.0"

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

ErrorCode = Literal[
    "invalid_message_id",
    "invalid_table",
    "invalid_result",
    "result_too_large",
    "in_transaction",
]

MAX_RESULT_BYTES = 65_536
TABLE_NAME_REGEX = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")

R = TypeVar("R")


def utc_now() -> datetime.datetime:
    """Return the current aware UTC datetime."""
    return datetime.datetime.now(datetime.timezone.utc)


class InboxError(Exception):
    """Base exception for inbox deduplication errors with stable machine-readable codes."""

    def __init__(self, code: ErrorCode, message: str) -> None:
        super().__init__(message)
        self.code: ErrorCode = code


@dataclass(frozen=True)
class Outcome(Generic[R]):
    """Result of handling an inbox message."""

    status: Literal["processed", "duplicate"]
    result: R


@dataclass(frozen=True)
class Stats:
    """Statistics on inbox entries and evictions."""

    entries: int
    evicted: int


def _validate_message_id(message_id: str) -> None:
    if not isinstance(message_id, str):
        raise InboxError("invalid_message_id", "message_id must be a string")
    if len(message_id) < 1 or len(message_id) > 255:
        raise InboxError(
            "invalid_message_id",
            f"message_id length must be between 1 and 255 characters (got {len(message_id)})",
        )
    for char in message_id:
        code = ord(char)
        # Check for ASCII control chars and DEL
        if code < 32 or code == 127:
            raise InboxError(
                "invalid_message_id",
                "message_id must not contain control characters",
            )


def _validate_table_name(table: str) -> None:
    if not isinstance(table, str) or not TABLE_NAME_REGEX.match(table):
        raise InboxError(
            "invalid_table",
            f"table name '{table[:200]}' is invalid; must match ^[a-z_][a-z0-9_]{{0,62}}$",
        )


def _validate_clock_datetime(dt: datetime.datetime) -> None:
    if not isinstance(dt, datetime.datetime) or dt.tzinfo is None:
        raise ValueError("clock must return timezone-aware datetime objects")


def _serialize_result(result: Any) -> str:
    try:
        serialized = json.dumps(result, sort_keys=True, allow_nan=False)
    except (TypeError, ValueError) as err:
        raise InboxError("invalid_result", f"result is not JSON-serialisable: {err}") from err

    encoded_len = len(serialized.encode("utf-8"))
    if encoded_len > MAX_RESULT_BYTES:
        raise InboxError(
            "result_too_large",
            (
                f"result serialized size {encoded_len} "
                f"exceeds maximum allowed {MAX_RESULT_BYTES} bytes"
            ),
        )
    return serialized


class SqliteInbox:
    """SQLite-backed transactional inbox ensuring atomic deduplication and work execution."""

    def __init__(
        self,
        conn: sqlite3.Connection,
        *,
        table: str = "inbox",
        clock: Callable[[], datetime.datetime] = utc_now,
        on_event: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        _validate_table_name(table)
        self._conn = conn
        self._table = table
        self._clock = clock
        self._on_event = on_event
        self._lock = threading.Lock()

        # Initialize table. Use autocommit / transactional DDL.
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute(
                f"CREATE TABLE IF NOT EXISTS {self._table} ("
                "message_id TEXT PRIMARY KEY, "
                "received_at TEXT NOT NULL, "
                "result TEXT NOT NULL)"
            )
            self._conn.commit()

    def handle(
        self,
        message_id: str,
        handler: Callable[[sqlite3.Connection | None], R],
    ) -> Outcome[R]:
        """Process message_id through handler or return stored outcome if duplicate."""
        _validate_message_id(message_id)

        if self._conn.in_transaction:
            raise InboxError(
                "in_transaction",
                "Connection is already in a transaction; cannot begin inbox transaction",
            )

        with self._lock:
            # Begin transaction with BEGIN IMMEDIATE
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                cursor = self._conn.cursor()
                cursor.execute(
                    f"SELECT result FROM {self._table} WHERE message_id = ?",  # noqa: S608
                    (message_id,),
                )
                row = cursor.fetchone()
                if row is not None:
                    self._conn.commit()
                    cached_result: R = json.loads(row[0])
                    if self._on_event is not None:
                        self._on_event({"event": "duplicate", "message_id": message_id})
                    return Outcome(status="duplicate", result=cached_result)

                # Execute handler passing connection
                handler_result = handler(self._conn)

                # Serialize result and validate limits
                json_str = _serialize_result(handler_result)

                now_dt = self._clock()
                _validate_clock_datetime(now_dt)
                now_iso = now_dt.isoformat()

                cursor.execute(
                    f"INSERT INTO {self._table} (message_id, received_at, result) VALUES (?, ?, ?)",  # noqa: S608
                    (message_id, now_iso, json_str),
                )
                self._conn.commit()

                if self._on_event is not None:
                    self._on_event({"event": "processed", "message_id": message_id})

                return Outcome(status="processed", result=handler_result)
            except BaseException:
                with contextlib.suppress(Exception):
                    self._conn.rollback()
                raise

    def purge(self, older_than: datetime.timedelta) -> int:
        """Purge entries older than older_than relative to clock()."""
        now_dt = self._clock()
        _validate_clock_datetime(now_dt)
        cutoff = (now_dt - older_than).isoformat()

        if self._conn.in_transaction:
            raise InboxError(
                "in_transaction",
                "Connection is already in a transaction; cannot purge",
            )

        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                cursor = self._conn.cursor()
                cursor.execute(
                    f"DELETE FROM {self._table} WHERE received_at < ?",  # noqa: S608
                    (cutoff,),
                )
                count = cursor.rowcount
                self._conn.commit()
                return max(0, count)
            except BaseException:
                with contextlib.suppress(Exception):
                    self._conn.rollback()
                raise

    def stats(self) -> Stats:
        """Return the number of entries currently stored."""
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {self._table}")  # noqa: S608
            row = cursor.fetchone()
            count = row[0] if row else 0
            return Stats(entries=count, evicted=0)


class InMemoryInbox:
    """Thread-safe in-memory inbox with LRU eviction."""

    def __init__(
        self,
        *,
        max_entries: int = 100_000,
        clock: Callable[[], datetime.datetime] = utc_now,
        on_event: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        if max_entries < 1:
            raise ValueError("max_entries must be >= 1")
        self._max_entries = max_entries
        self._clock = clock
        self._on_event = on_event
        self._lock = threading.Lock()
        # map message_id -> (received_at_dt, serialized_json)
        self._entries: collections.OrderedDict[str, tuple[datetime.datetime, str]] = (
            collections.OrderedDict()
        )
        self._evicted_count = 0

    def handle(
        self,
        message_id: str,
        handler: Callable[[sqlite3.Connection | None], R],
    ) -> Outcome[R]:
        """Process message_id through handler or return stored outcome if duplicate."""
        _validate_message_id(message_id)

        with self._lock:
            if message_id in self._entries:
                _, stored_json = self._entries[message_id]
                cached_result: R = json.loads(stored_json)
                if self._on_event is not None:
                    self._on_event({"event": "duplicate", "message_id": message_id})
                return Outcome(status="duplicate", result=cached_result)

            # Execute handler outside the lock or inside? InMemoryInbox handler receives None
            # Atomicity covers the inbox mark.
            handler_result = handler(None)

            serialized = _serialize_result(handler_result)
            now_dt = self._clock()
            _validate_clock_datetime(now_dt)

            # Check capacity and evict oldest (FIFO/insertion order)
            if len(self._entries) >= self._max_entries:
                self._entries.popitem(last=False)
                self._evicted_count += 1

            self._entries[message_id] = (now_dt, serialized)

            if self._on_event is not None:
                self._on_event({"event": "processed", "message_id": message_id})

            return Outcome(status="processed", result=handler_result)

    def purge(self, older_than: datetime.timedelta) -> int:
        """Purge entries older than older_than relative to clock()."""
        now_dt = self._clock()
        _validate_clock_datetime(now_dt)
        cutoff = now_dt - older_than

        purged = 0
        with self._lock:
            keys_to_remove = [
                k for k, (received_at, _) in self._entries.items() if received_at < cutoff
            ]
            for k in keys_to_remove:
                del self._entries[k]
                purged += 1
        return purged

    def stats(self) -> Stats:
        """Return current entries and eviction count."""
        with self._lock:
            return Stats(entries=len(self._entries), evicted=self._evicted_count)
