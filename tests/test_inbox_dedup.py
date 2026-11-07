"""Complete test suite exercising trysac-inbox-dedup public API and acceptance criteria."""

from __future__ import annotations

import datetime
import sqlite3
import tempfile
import threading
from pathlib import Path
from typing import Any

import pytest

from inbox_dedup import (
    InboxError,
    InMemoryInbox,
    Outcome,
    SqliteInbox,
    utc_now,
)


def test_sqlite_handle_processed_and_duplicate() -> None:
    conn = sqlite3.connect(":memory:", timeout=5)
    inbox = SqliteInbox(conn)

    call_count = 0

    def h(c: sqlite3.Connection | None) -> dict[str, bool]:
        nonlocal call_count
        call_count += 1
        return {"ok": True}

    # Literal acceptance test:
    # inbox.handle('m1', h) returns Outcome(status='processed', result={'ok': True}) and calls h once;
    # a second handle('m1', h) returns Outcome(status='duplicate', result={'ok': True}) without calling h
    res1 = inbox.handle("m1", h)
    assert res1 == Outcome(status="processed", result={"ok": True})
    assert call_count == 1

    res2 = inbox.handle("m1", h)
    assert res2 == Outcome(status="duplicate", result={"ok": True})
    assert call_count == 1


def test_sqlite_rollback_on_handler_error() -> None:
    conn = sqlite3.connect(":memory:", timeout=5)
    # Create a business table to verify atomicity
    conn.execute("CREATE TABLE business_log (note TEXT)")
    conn.commit()

    inbox = SqliteInbox(conn)

    # Literal acceptance test:
    # A handler that inserts a row through the given connection and then raises ValueError('boom')
    # propagates that same exception, leaves 0 inbox rows and 0 handler rows, and a later handle('m1', h) processes the message
    def failing_handler(c: sqlite3.Connection | None) -> dict[str, bool]:
        assert c is not None
        c.execute("INSERT INTO business_log (note) VALUES ('failed')")
        raise ValueError("boom")

    with pytest.raises(ValueError, match=r"^boom$"):
        inbox.handle("m1", failing_handler)

    # Verify 0 inbox rows and 0 handler rows
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM inbox")
    assert cur.fetchone()[0] == 0

    cur.execute("SELECT COUNT(*) FROM business_log")
    assert cur.fetchone()[0] == 0

    # A later handle('m1', h) processes the message
    def success_handler(c: sqlite3.Connection | None) -> dict[str, bool]:
        assert c is not None
        c.execute("INSERT INTO business_log (note) VALUES ('success')")
        return {"recovered": True}

    outcome = inbox.handle("m1", success_handler)
    assert outcome == Outcome(status="processed", result={"recovered": True})

    cur.execute("SELECT COUNT(*) FROM inbox")
    assert cur.fetchone()[0] == 1
    cur.execute("SELECT COUNT(*) FROM business_log")
    assert cur.fetchone()[0] == 1


def test_sqlite_keyboard_interrupt_rolls_back() -> None:
    conn = sqlite3.connect(":memory:", timeout=5)
    inbox = SqliteInbox(conn)

    def interrupting_handler(c: sqlite3.Connection | None) -> None:
        raise KeyboardInterrupt("simulated cancel")

    with pytest.raises(KeyboardInterrupt):
        inbox.handle("m_int", interrupting_handler)

    assert inbox.stats().entries == 0


def test_invalid_result_and_size_limit() -> None:
    conn = sqlite3.connect(":memory:", timeout=5)
    inbox = SqliteInbox(conn)

    # Non-serialisable result raises InboxError with code 'invalid_result' and rolls back
    def non_serialisable(c: sqlite3.Connection | None) -> Any:
        return object()

    with pytest.raises(InboxError) as exc_info:
        inbox.handle("m_bad_result", non_serialisable)
    assert exc_info.value.code == "invalid_result"
    assert inbox.stats().entries == 0

    # Result exceeding 65_536 bytes raises code 'result_too_large'
    huge_payload = {"data": "a" * 70_000}

    def huge_handler(c: sqlite3.Connection | None) -> Any:
        return huge_payload

    with pytest.raises(InboxError) as exc_info2:
        inbox.handle("m_huge", huge_handler)
    assert exc_info2.value.code == "result_too_large"
    assert inbox.stats().entries == 0


def test_purge_retention_window() -> None:
    # With a clock at 2026-01-01T00:00:00Z when 'm1' is handled and at 2026-01-09T00:00:00Z later,
    # purge(timedelta(days=7)) returns 1 and handle('m1', h) then processes again
    clock_time = datetime.datetime(2026, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)

    def mock_clock() -> datetime.datetime:
        return clock_time

    conn = sqlite3.connect(":memory:", timeout=5)
    inbox = SqliteInbox(conn, clock=mock_clock)

    def h(c: sqlite3.Connection | None) -> dict[str, str]:
        return {"data": "ok"}

    res1 = inbox.handle("m1", h)
    assert res1.status == "processed"

    # Advance clock to 2026-01-09T00:00:00Z
    clock_time = datetime.datetime(2026, 1, 9, 0, 0, 0, tzinfo=datetime.timezone.utc)
    purged = inbox.purge(datetime.timedelta(days=7))
    assert purged == 1
    assert inbox.stats().entries == 0

    # Now handle('m1', h) processes again
    res2 = inbox.handle("m1", h)
    assert res2.status == "processed"


def test_in_memory_inbox_eviction() -> None:
    # InMemoryInbox(max_entries=2): after 'm1', 'm2' and 'm3', the oldest id 'm1' is evicted,
    # stats().evicted == 1 and 'm1' would be processed again
    inbox = InMemoryInbox(max_entries=2)

    def h(c: sqlite3.Connection | None) -> dict[str, bool]:
        assert c is None
        return {"ok": True}

    inbox.handle("m1", h)
    inbox.handle("m2", h)
    inbox.handle("m3", h)

    st = inbox.stats()
    assert st.evicted == 1
    assert st.entries == 2

    # 'm1' would be processed again
    out = inbox.handle("m1", h)
    assert out.status == "processed"


def test_validation_message_ids_table_and_in_transaction() -> None:
    conn = sqlite3.connect(":memory:", timeout=5)
    inbox = SqliteInbox(conn)

    def dummy_h(_: sqlite3.Connection | None) -> None:
        return None

    # Message ids '' raises 'invalid_message_id'
    with pytest.raises(InboxError) as exc_empty:
        inbox.handle("", dummy_h)
    assert exc_empty.value.code == "invalid_message_id"

    # 256-character id raises InboxError with code 'invalid_message_id'
    long_id = "x" * 256
    with pytest.raises(InboxError) as exc_long:
        inbox.handle(long_id, dummy_h)
    assert exc_long.value.code == "invalid_message_id"

    # Control character in id raises 'invalid_message_id'
    with pytest.raises(InboxError) as exc_ctrl:
        inbox.handle("m1\x00bad", dummy_h)
    assert exc_ctrl.value.code == "invalid_message_id"

    # Table name 'inbox; drop' raises 'invalid_table'
    with pytest.raises(InboxError) as exc_table:
        SqliteInbox(conn, table="inbox; drop")
    assert exc_table.value.code == "invalid_table"

    # Connection already in a transaction raises 'in_transaction'
    conn.execute("BEGIN")
    with pytest.raises(InboxError) as exc_tx:
        inbox.handle("valid_id", dummy_h)
    assert exc_tx.value.code == "in_transaction"

    with pytest.raises(InboxError) as exc_tx_purge:
        inbox.purge(datetime.timedelta(hours=1))
    assert exc_tx_purge.value.code == "in_transaction"
    conn.rollback()


def test_concurrent_sqlite_inbox_contention() -> None:
    # Two SqliteInbox objects on separate connections to the same database file,
    # one handler blocked on a threading.Event while the other thread handles the same id,
    # call the handler exactly once in total and the second returns status 'duplicate'
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "shared.db"

        # Create file database
        conn_init = sqlite3.connect(str(db_path), timeout=5)
        SqliteInbox(conn_init)
        conn_init.close()

        handler_call_count = 0
        handler1_entered = threading.Event()
        handler1_can_finish = threading.Event()

        def handler1(c: sqlite3.Connection | None) -> dict[str, str]:
            nonlocal handler_call_count
            handler_call_count += 1
            handler1_entered.set()
            handler1_can_finish.wait(timeout=3)
            return {"val": "first"}

        def handler2(c: sqlite3.Connection | None) -> dict[str, str]:
            nonlocal handler_call_count
            handler_call_count += 1
            return {"val": "second"}

        outcome1: Outcome[Any] | None = None
        outcome2: Outcome[Any] | None = None

        def run_thread1() -> None:
            nonlocal outcome1
            conn1 = sqlite3.connect(str(db_path), timeout=5)
            inbox1 = SqliteInbox(conn1)
            try:
                outcome1 = inbox1.handle("shared_m1", handler1)
            finally:
                conn1.close()

        def run_thread2() -> None:
            nonlocal outcome2
            conn2 = sqlite3.connect(str(db_path), timeout=5)
            inbox2 = SqliteInbox(conn2)
            try:
                # Wait until thread 1 holds the lock
                handler1_entered.wait(timeout=3)
                # Thread 2 will attempt to handle 'shared_m1', waiting on SQLite lock
                outcome2 = inbox2.handle("shared_m1", handler2)
            finally:
                conn2.close()

        t1 = threading.Thread(target=run_thread1)
        t2 = threading.Thread(target=run_thread2)

        t1.start()
        t2.start()

        # Wait for thread 1 to enter and hold lock
        handler1_entered.wait(timeout=3)
        # Give thread 2 a short moment to block on BEGIN IMMEDIATE
        import time

        time.sleep(0.05)
        # Now allow thread 1 to complete
        handler1_can_finish.set()

        t1.join(timeout=5)
        t2.join(timeout=5)

        assert handler_call_count == 1
        assert outcome1 is not None and outcome1.status == "processed"
        assert outcome1.result == {"val": "first"}
        assert outcome2 is not None and outcome2.status == "duplicate"
        assert outcome2.result == {"val": "first"}


def test_on_event_does_not_contain_result_sentinel() -> None:
    # on_event receives {'event': 'processed', 'message_id': 'm1'} then {'event': 'duplicate', 'message_id': 'm1'},
    # and no payload contains the result value 'SENTINEL-7f3a'
    events: list[dict[str, Any]] = []

    def listener(event: dict[str, Any]) -> None:
        events.append(event)

    conn = sqlite3.connect(":memory:", timeout=5)
    inbox = SqliteInbox(conn, on_event=listener)

    def h(c: sqlite3.Connection | None) -> dict[str, str]:
        return {"secret": "SENTINEL-7f3a"}

    inbox.handle("m1", h)
    inbox.handle("m1", h)

    assert len(events) == 2
    assert events[0] == {"event": "processed", "message_id": "m1"}
    assert events[1] == {"event": "duplicate", "message_id": "m1"}

    for evt in events:
        for v in evt.values():
            assert "SENTINEL-7f3a" not in str(v)


def test_in_memory_inbox_purge_and_events() -> None:
    clock_time = datetime.datetime(2026, 3, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
    events: list[dict[str, Any]] = []
    inbox = InMemoryInbox(
        max_entries=10,
        clock=lambda: clock_time,
        on_event=lambda e: events.append(e),
    )

    inbox.handle("msg1", lambda _: 123)
    inbox.handle("msg1", lambda _: 456)

    assert events == [
        {"event": "processed", "message_id": "msg1"},
        {"event": "duplicate", "message_id": "msg1"},
    ]

    # Purge with nothing older than 1 hour
    assert inbox.purge(datetime.timedelta(hours=1)) == 0
    assert inbox.stats().entries == 1

    # Advance clock by 2 hours and purge
    clock_time += datetime.timedelta(hours=2)
    assert inbox.purge(datetime.timedelta(hours=1)) == 1
    assert inbox.stats().entries == 0


def test_types_and_edge_cases() -> None:
    conn = sqlite3.connect(":memory:", timeout=5)
    inbox = SqliteInbox(conn)

    # None result round trip
    res_none = inbox.handle("m_none", lambda _: None)
    assert res_none.status == "processed"
    assert res_none.result is None

    res_none_dup = inbox.handle("m_none", lambda _: "ignored")
    assert res_none_dup.status == "duplicate"
    assert res_none_dup.result is None

    # Unicode message ID
    res_unicode = inbox.handle("msg-日本語-üöä", lambda _: [1, 2, 3])
    assert res_unicode.status == "processed"
    assert res_unicode.result == [1, 2, 3]

    # Negative max_entries for InMemoryInbox
    with pytest.raises(ValueError, match="max_entries must be >= 1"):
        InMemoryInbox(max_entries=0)

    # Naive clock rejection
    naive_inbox = SqliteInbox(conn, clock=lambda: datetime.datetime(2026, 1, 1))  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="timezone-aware"):
        naive_inbox.handle("m_naive", lambda _: "ok")

    # Message id type check
    with pytest.raises(InboxError) as exc_type:
        inbox.handle(123, lambda _: "ok")  # type: ignore[arg-type]
    assert exc_type.value.code == "invalid_message_id"

    # Test on_event with InMemoryInbox duplicate branch
    dup_events: list[dict[str, Any]] = []
    mem_inbox = InMemoryInbox(max_entries=5, on_event=lambda e: dup_events.append(e))
    mem_inbox.handle("dup_test", lambda _: "first")
    mem_inbox.handle("dup_test", lambda _: "second")
    assert dup_events == [
        {"event": "processed", "message_id": "dup_test"},
        {"event": "duplicate", "message_id": "dup_test"},
    ]

    # Test SqliteInbox purge failure rolls back
    purge_fail_conn = sqlite3.connect(":memory:", timeout=5)
    purge_inbox = SqliteInbox(purge_fail_conn)
    # Drop table behind its back so DELETE fails inside transaction
    purge_fail_conn.execute(f"DROP TABLE {purge_inbox._table}")
    purge_fail_conn.commit()
    with pytest.raises(sqlite3.OperationalError):
        purge_inbox.purge(datetime.timedelta(days=1))

    # utc_now helper
    now = utc_now()
    assert now.tzinfo is not None
