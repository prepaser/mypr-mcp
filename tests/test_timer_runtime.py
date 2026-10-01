from __future__ import annotations

import asyncio
import time

import pytest

from mypr_mcp.history import History
from mypr_mcp.messages import MessageStore
from mypr_mcp.runtime import Runtime
from mypr_mcp.timers import TimerStore


@pytest.fixture
def runtime(tmp_path):
    runtime = Runtime(tmp_path)
    runtime.history = History(tmp_path)
    for client in ("alice", "bob"):
        runtime.history.reserve_client_id(client)
    runtime.messages = MessageStore(tmp_path)
    runtime.timers = TimerStore(tmp_path)
    try:
        yield runtime
    finally:
        runtime.timers.close()
        runtime.messages.close()
        runtime.history.close()


def connection(runtime, client):
    connection_id = f"connection-{client}"
    runtime.clients[connection_id] = {
        "client_id": client,
        "connection_id": connection_id,
        "last_activity": time.time(),
    }
    return connection_id


def execution(runtime, *, state="running", client="alice"):
    ident = ("a" if client == "alice" else "b") * 32
    runtime.execs[ident] = {
        "id": ident,
        "generation": runtime.generation,
        "client_id": client,
        "connection_id": connection(runtime, client),
        "state": state,
        "events": [],
        "truncated": False,
        "error": "Python exception" if state == "failed" else None,
        "done": asyncio.Event(),
        "_revision": 0,
        "_changed": asyncio.Condition(),
    }
    return ident


async def poll(runtime, ident, *, wait_ms=1000, wake_on_output=True, client="alice"):
    return await runtime.poll(
        ident,
        wait_ms=wait_ms,
        inbox_client=client,
        wake_on_output=wake_on_output,
    )


async def wait_for_timer_waiter(runtime, client="alice"):
    deadline = asyncio.get_running_loop().time() + 2
    while asyncio.get_running_loop().time() < deadline:
        if runtime.timer_waiters.get(client):
            return
        await asyncio.sleep(0)
    raise AssertionError("poll did not register a timer waiter")


@pytest.mark.parametrize("wake_on_output", [False, True])
async def test_poll_wakes_when_timer_expires(runtime, wake_on_output):
    ident = execution(runtime)
    await runtime.dispatch(
        {"op": "timer_start", "client_id": "alice", "seconds": 0.1, "label": "wake"}
    )
    result = await asyncio.wait_for(
        poll(runtime, ident, wait_ms=5000, wake_on_output=wake_on_output),
        3,
    )

    assert result["state"] == "running"
    assert not runtime.timer_waiters


async def test_all_waiters_wake_and_are_cleaned_up_on_timer_expiry(runtime):
    ident = execution(runtime)
    await runtime.dispatch(
        {"op": "timer_start", "client_id": "alice", "seconds": 0.1, "label": "shared"}
    )
    waiters = [asyncio.create_task(poll(runtime, ident, wait_ms=5000)) for _ in range(2)]
    results = await asyncio.wait_for(
        asyncio.gather(*waiters),
        3,
    )

    assert [result["state"] for result in results] == ["running", "running"]
    assert not runtime.timer_waiters


async def test_earlier_timer_registration_recomputes_wait_deadline(runtime):
    ident = execution(runtime)
    await runtime.dispatch(
        {"op": "timer_start", "client_id": "alice", "seconds": 1, "label": "late"}
    )
    waiting = asyncio.create_task(poll(runtime, ident, wait_ms=5000))
    await wait_for_timer_waiter(runtime)
    await runtime.dispatch(
        {"op": "timer_start", "client_id": "alice", "seconds": 0.1, "label": "early"}
    )

    result = await asyncio.wait_for(waiting, 3)
    assert result["state"] == "running"
    assert not runtime.timer_waiters


async def test_cancelled_timer_does_not_wake_poll_until_its_wait_deadline(runtime):
    ident = execution(runtime)
    timer = await runtime.dispatch(
        {"op": "timer_start", "client_id": "alice", "seconds": 1, "label": "cancel"}
    )
    waiting = asyncio.create_task(poll(runtime, ident, wait_ms=5000))
    await wait_for_timer_waiter(runtime)
    cancelled = await runtime.dispatch(
        {"op": "timer_cancel", "client_id": "alice", "timer_id": timer["id"]}
    )
    assert cancelled["state"] == "cancelled"
    await asyncio.sleep(0.1)
    assert not waiting.done()
    waiting.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiting
    assert not runtime.timer_waiters


async def test_immediate_expiry_is_attached_to_idle_and_failed_responses(runtime):
    connection_id = connection(runtime, "alice")
    timer = await runtime.dispatch(
        {"op": "timer_start", "client_id": "alice", "seconds": 0, "label": "now"}
    )
    ident = execution(runtime, state="failed")
    response = await runtime.dispatch(
        {"op": "poll", "connection_id": connection_id, "exec_id": ident, "wait_ms": 0}
    )

    assert response["state"] == "failed"
    assert response["timers"]["unacked"] == 1
    assert response["timers"]["items"] == [
        {"id": timer["id"], "label": "now", "due_at": timer["due_at"]}
    ]


async def test_timer_preview_is_client_scoped_and_independent_of_messages(runtime):
    alice_connection = connection(runtime, "alice")
    bob_connection = connection(runtime, "bob")
    alice_timer = await runtime.dispatch(
        {"op": "timer_start", "client_id": "alice", "seconds": 0, "label": "alice"}
    )
    bob_timer = await runtime.dispatch(
        {"op": "timer_start", "client_id": "bob", "seconds": 0, "label": "bob"}
    )
    runtime.messages.send("bob", "alice", "unrelated message")
    alice_exec = execution(runtime, client="alice")
    bob_exec = execution(runtime, client="bob")

    alice = await runtime.dispatch(
        {"op": "poll", "connection_id": alice_connection, "exec_id": alice_exec, "wait_ms": 0}
    )
    bob = await runtime.dispatch(
        {"op": "poll", "connection_id": bob_connection, "exec_id": bob_exec, "wait_ms": 0}
    )

    assert alice["inbox"]["messages"][0]["text"] == "unrelated message"
    assert [item["id"] for item in alice["timers"]["items"]] == [alice_timer["id"]]
    assert [item["id"] for item in bob["timers"]["items"]] == [bob_timer["id"]]


async def test_cancelled_poll_cleans_timer_waiter(runtime):
    ident = execution(runtime)
    await runtime.dispatch(
        {"op": "timer_start", "client_id": "alice", "seconds": 10, "label": "later"}
    )
    waiting = asyncio.create_task(poll(runtime, ident, wait_ms=30000))
    await wait_for_timer_waiter(runtime)
    waiting.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiting
    assert not runtime.timer_waiters
