import asyncio

import pytest

from mypr_mcp.history import History
from mypr_mcp.messages import MessageStore
from mypr_mcp.runtime import Runtime
from mypr_mcp.transport import rpc


@pytest.fixture
def runtime(tmp_path):
    runtime = Runtime(tmp_path)
    runtime.history = History(tmp_path)
    for client in ("sender", "receiver"):
        runtime.history.reserve_client_id(client)
    runtime.messages = MessageStore(tmp_path)
    try:
        yield runtime
    finally:
        runtime.messages.close()
        runtime.history.close()


async def test_all_message_waiters_wake_and_leave_no_wait_tasks(runtime):
    baseline = asyncio.all_tasks()
    waiters = [asyncio.create_task(runtime.wait_activity("receiver", 30)) for _ in range(2)]
    await asyncio.sleep(0)
    await runtime.dispatch(
        {
            "op": "message_send",
            "client_id": "sender",
            "to": "receiver",
            "text": "wake",
        }
    )
    await asyncio.wait_for(asyncio.gather(*waiters), 1)
    assert asyncio.all_tasks() == baseline
    assert not runtime.message_waiters


async def test_new_cursor_wait_cannot_erase_an_existing_notification(runtime):
    first = asyncio.create_task(runtime.wait_activity("receiver", 30))

    async def send_and_wait_for_next():
        message = await runtime.dispatch(
            {
                "op": "message_send",
                "client_id": "sender",
                "to": "receiver",
                "text": "wake",
            }
        )
        await runtime.wait_activity("receiver", 30, after=message["id"])

    second = asyncio.create_task(send_and_wait_for_next())
    try:
        await asyncio.wait_for(first, 1)
        assert not second.done()
    finally:
        first.cancel()
        second.cancel()
        await asyncio.gather(first, second, return_exceptions=True)
    assert not runtime.message_waiters


async def test_read_wait_respects_cursor_and_timeout(runtime):
    old = runtime.messages.send("sender", "receiver", "old")
    page = await asyncio.wait_for(
        runtime.dispatch(
            {
                "op": "message_read",
                "client_id": "receiver",
                "after": old["id"],
                "wait_ms": 20,
            }
        ),
        1,
    )
    assert page["messages"] == []
    assert runtime.messages.inbox("receiver")["unacked"] == 1
    for invalid in (-1, 30001, True, "10"):
        with pytest.raises(ValueError, match="wait_ms"):
            await runtime.dispatch(
                {
                    "op": "message_read",
                    "client_id": "receiver",
                    "wait_ms": invalid,
                }
            )


async def test_filtered_read_wait_ignores_unrelated_messages(runtime):
    original = await runtime.dispatch(
        {
            "op": "message_send",
            "client_id": "receiver",
            "to": "sender",
            "text": "original",
        }
    )
    waiting = asyncio.create_task(
        runtime.dispatch(
            {
                "op": "message_read",
                "client_id": "receiver",
                "sender": "sender",
                "reply_to": original["id"],
                "wait_ms": 30000,
            }
        )
    )
    await asyncio.sleep(0)
    unrelated = await runtime.dispatch(
        {
            "op": "message_send",
            "client_id": "sender",
            "to": "receiver",
            "text": "unrelated",
        }
    )
    await asyncio.sleep(0.02)
    assert not waiting.done()
    reply = await runtime.dispatch(
        {
            "op": "message_send",
            "client_id": "receiver",
            "to": "sender",
            "text": "reply",
            "reply_to": unrelated["id"],
        }
    )
    # The reply above is addressed to the sender of the unrelated message,
    # so it must not satisfy the requested reply_to value.
    assert reply["reply_to"] == unrelated["id"]
    await asyncio.sleep(0.02)
    assert not waiting.done()
    await runtime.dispatch(
        {
            "op": "message_send",
            "client_id": "sender",
            "to": "receiver",
            "text": "target",
            "reply_to": original["id"],
        }
    )
    page = await asyncio.wait_for(waiting, 1)
    assert [message["text"] for message in page["messages"]] == ["target"]


async def test_filtered_wait_can_be_cancelled(runtime):
    waiting = asyncio.create_task(
        runtime.dispatch(
            {
                "op": "message_read",
                "client_id": "receiver",
                "sender": "sender",
                "wait_ms": 30000,
            }
        )
    )
    await asyncio.sleep(0)
    waiting.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiting
    assert not runtime.message_waiters


async def test_cancelled_and_stopped_waiters_are_cleaned_up(runtime):
    baseline = asyncio.all_tasks()
    waiting = asyncio.create_task(runtime.wait_activity("receiver", 30))
    await asyncio.sleep(0)
    waiting.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiting
    assert asyncio.all_tasks() == baseline
    assert not runtime.message_waiters
    waiting = asyncio.create_task(runtime.wait_activity("receiver", 30))
    await asyncio.sleep(0)
    runtime.stopping.set()
    with pytest.raises(RuntimeError, match="stopping"):
        await waiting
    assert asyncio.all_tasks() == baseline
    assert not runtime.message_waiters


async def test_disconnected_read_rpc_releases_server_waiter(runtime, monkeypatch):
    reading = asyncio.Event()
    finished = asyncio.Event()
    original = runtime.messages.read

    def read(*args, **kwargs):
        reading.set()
        return original(*args, **kwargs)

    monkeypatch.setattr(runtime.messages, "read", read)

    async def connection(reader, writer):
        try:
            await runtime.connection(reader, writer)
        finally:
            finished.set()

    server = await asyncio.start_unix_server(connection, path=str(runtime.socket))
    async with server:
        waiting = asyncio.create_task(
            rpc(
                runtime.socket,
                op="message_read",
                client_id="receiver",
                wait_ms=30000,
            )
        )
        try:
            await asyncio.wait_for(reading.wait(), 1)
            waiting.cancel()
            with pytest.raises(asyncio.CancelledError):
                await waiting
            await asyncio.wait_for(finished.wait(), 1)
        finally:
            waiting.cancel()
            await asyncio.gather(waiting, return_exceptions=True)
