import json
import math
from datetime import UTC, datetime
from pathlib import Path

import pytest

from mypr_mcp.history import History
from mypr_mcp.timers import TimerStore


class Clock:
    def __init__(self, value=1_700_000_000.0):
        self.value = value

    def __call__(self):
        return self.value


@pytest.fixture
def store(tmp_path: Path):
    history = History(tmp_path)
    history.reserve_client_id("alice")
    history.reserve_client_id("bob")
    clock = Clock()
    timers = TimerStore(tmp_path, clock=clock)
    try:
        yield timers, clock
    finally:
        timers.close()
        history.close()


def test_start_check_and_lazy_expiry(store):
    timers, clock = store
    timer = timers.start("alice", 10, label="review")
    assert timer["id"].startswith("timer-")
    assert timer["state"] == "scheduled"
    assert timer["remaining_seconds"] == 10
    assert timers.check("alice", timer["id"])["state"] == "scheduled"
    assert timers.notifications("alice") is None

    clock.value += 10
    checked = timers.check("alice", timer["id"])
    assert checked["state"] == "expired"
    assert checked["remaining_seconds"] == 0
    notification = timers.notifications("alice")
    assert notification == {
        "unacked": 1,
        "items": [{"id": timer["id"], "label": "review", "due_at": 1_700_000_010.0}],
        "has_more": False,
    }
    assert timers.next_deadline("alice") == 1_700_000_010.0


def test_start_at_supports_aware_datetime_and_iso(store):
    timers, _ = store
    timer = timers.start("alice", at="2023-11-14T22:13:30+00:00")
    assert timer["due_at"] == 1_700_000_010.0
    timer = timers.start("alice", at=datetime.fromtimestamp(1_700_000_020, UTC))
    assert timer["due_at"] == 1_700_000_020.0


def test_ack_is_atomic_and_repeated_ack_is_noop(store):
    timers, clock = store
    first = timers.start("alice", 0, label="first")
    second = timers.start("alice", 0, label="second")
    foreign = timers.start("bob", 0, label="foreign")
    with pytest.raises(ValueError, match="another client"):
        timers.ack("alice", [first["id"], foreign["id"]])
    assert timers.notifications("alice")["unacked"] == 2
    with pytest.raises(ValueError, match="unknown"):
        timers.ack("alice", [first["id"], "timer-missing"])
    assert timers.notifications("alice")["unacked"] == 2
    assert timers.ack("alice", [first["id"], first["id"]]) == 1
    assert timers.ack("alice", [first["id"]]) == 0
    assert timers.notifications("alice")["unacked"] == 1
    assert timers.next_deadline("alice") == second["due_at"]
    assert math.isclose(clock.value, 1_700_000_000.0)


def test_cancel_and_expired_timers(store):
    timers, _ = store
    scheduled = timers.start("alice", 10)
    assert timers.cancel("alice", scheduled["id"])["state"] == "cancelled"
    assert timers.cancel("alice", scheduled["id"])["state"] == "cancelled"
    expired = timers.start("alice", 0)
    with pytest.raises(ValueError, match="cannot be cancelled"):
        timers.cancel("alice", expired["id"])


def test_list_pages_and_persists(store, tmp_path):
    timers, clock = store
    expected = [timers.start("alice", i, label=str(i)) for i in range(3)]
    first = timers.list("alice", limit=2)
    assert [item["id"] for item in first["items"]] == [item["id"] for item in expected[:2]]
    assert first["has_more"] is True
    second = timers.list("alice", limit=2, cursor=first["next_cursor"])
    assert [item["id"] for item in second["items"]] == [expected[2]["id"]]
    assert second["next_cursor"] is None
    timers.close()
    reopened = TimerStore(tmp_path, clock=clock)
    try:
        assert reopened.check("alice", expected[0]["id"])["label"] == "0"
    finally:
        reopened.close()


def test_notifications_are_bounded_and_client_isolated(store):
    timers, _ = store
    for index in range(7):
        timers.start("alice", 0, label=f"timer-{index}")
    timers.start("bob", 0, label="bob")
    preview = timers.notifications("alice")
    assert preview is not None
    assert preview["unacked"] == 7
    assert len(preview["items"]) == 5
    assert preview["has_more"] is True
    assert timers.notifications("bob")["unacked"] == 1


def test_validation(store):
    timers, _ = store
    with pytest.raises(ValueError, match="exactly one"):
        timers.start("alice")
    with pytest.raises(ValueError, match="exactly one"):
        timers.start("alice", 1, at="2023-11-14T22:13:30+00:00")
    with pytest.raises(ValueError, match="non-negative"):
        timers.start("alice", -1)
    with pytest.raises(ValueError, match="finite"):
        timers.start("alice", float("inf"))
    with pytest.raises(ValueError, match="finite"):
        timers.start("alice", 10**400)
    with pytest.raises(ValueError, match="timezone"):
        timers.start("alice", at="2023-11-14T22:13:30")
    with pytest.raises(ValueError, match="timezone"):
        timers.start("alice", at=datetime(2023, 11, 14, 22, 13, 30))
    with pytest.raises(ValueError, match="256"):
        timers.start("alice", 1, label="가" * 100)
    with pytest.raises(ValueError, match="between 1 and 100"):
        timers.list("alice", limit=0)
    with pytest.raises(ValueError, match="non-negative integer"):
        timers.list("alice", cursor=1.5)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="non-negative integer"):
        timers.list("alice", cursor=str(1 << 63))
    with pytest.raises(TypeError, match="list"):
        timers.ack("alice", ("timer-x",))  # type: ignore[arg-type]


def test_notification_budget_includes_metadata_and_json_escaping(store):
    timers, _ = store
    for _ in range(4):
        timers.start("alice", 0, label="\x00" * 204 + "a" * 52)
    preview = timers.notifications("alice")
    assert preview["unacked"] == 4
    assert preview["has_more"]
    assert len(json.dumps(preview, ensure_ascii=False, separators=(",", ":")).encode()) <= 4096


def test_transaction_failure_does_not_poison_store(store):
    timers, _ = store
    with pytest.raises(RuntimeError, match="abort"):
        with timers._transaction():
            raise RuntimeError("abort")
    assert timers.start("alice", 1)["state"] == "scheduled"


def test_expired_timer_wakes_after_wall_clock_rollback(store):
    timers, clock = store
    timer = timers.start("alice", 10)
    clock.value += 10
    assert timers.check("alice", timer["id"])["state"] == "expired"
    clock.value -= 100
    assert timers.next_deadline("alice") == clock.value


def test_failed_operations_latch_expiry_before_wall_clock_rollback(store):
    timers, clock = store
    cancelled = timers.start("alice", 10)
    clock.value += 10
    with pytest.raises(ValueError, match="cannot be cancelled"):
        timers.cancel("alice", cancelled["id"])
    clock.value -= 100
    assert timers.check("alice", cancelled["id"])["state"] == "expired"

    checked = timers.start("alice", 10)
    clock.value += 10
    with pytest.raises(ValueError, match="unknown timer ID"):
        timers.check("alice", "timer-missing")
    clock.value -= 100
    assert timers.check("alice", checked["id"])["state"] == "expired"

    acknowledged = timers.start("alice", 10)
    clock.value += 10
    with pytest.raises(ValueError, match="unknown timer ID"):
        timers.ack("alice", [acknowledged["id"], "timer-missing"])
    clock.value -= 100
    assert timers.check("alice", acknowledged["id"])["state"] == "expired"
