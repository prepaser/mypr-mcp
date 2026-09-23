import ast
import asyncio

import pytest
from conftest import decode_result, execute, mcp_session, poll_until_done, result_text, stop_manager

from mypr_mcp.transport import rpc, socket_path


def value(result):
    assert result["state"] == "succeeded", result
    return ast.literal_eval(result_text(result))


async def test_messages_are_scoped_to_caller_and_explicitly_acknowledged(workspace):
    async with (
        mcp_session(workspace, client_id="sender") as sender,
        mcp_session(workspace, client_id="receiver") as receiver,
    ):
        sent = await execute(sender, "await ws.messages.send('receiver', '검토 완료')")
        message = value(sent)
        assert message["from"] == "sender"
        assert sent["inbox"]["unacked"] == 0
        polled = decode_result(await receiver.call_tool("poll", {"exec_id": sent["exec_id"]}))
        assert polled["inbox"]["messages"][0]["text"] == "검토 완료"
        assert polled["client_id"] == "sender"
        initialized = decode_result(await receiver.call_tool("init", {}))
        assert initialized["inbox"]["unacked"] == 1
        page = await execute(receiver, "await ws.messages.read()")
        assert value(page)["messages"] == [message]
        assert page["inbox"]["unacked"] == 1
        acknowledged = await execute(receiver, f"await ws.messages.ack([{message['id']}])")
        assert value(acknowledged) == 1
        assert acknowledged["inbox"]["unacked"] == 0
        assert value(await execute(receiver, f"await ws.messages.ack([{message['id']}])")) == 0
        async with mcp_session(workspace, initialize_client=False) as anonymous:
            result = decode_result(await anonymous.call_tool("poll", {"exec_id": sent["exec_id"]}))
            assert "inbox" not in result


async def test_offline_messages_survive_reset_and_manager_restart(workspace):
    async with mcp_session(workspace, client_id="receiver"):
        pass
    async with mcp_session(workspace, client_id="sender") as sender:
        message = value(await execute(sender, "await ws.messages.send('receiver', 'saved')"))
        reset = await execute(sender, "await ws.reset()", wait_ms=15000)
        assert reset["state"] == "succeeded"
    await stop_manager(workspace)
    async with mcp_session(workspace, client_id="receiver", initialize_client=False) as receiver:
        initialized = decode_result(await receiver.call_tool("init", {"client_id": "receiver"}))
        assert initialized["inbox"]["messages"][0]["id"] == message["id"]
        assert value(await execute(receiver, "await ws.messages.read()"))["messages"] == [message]
        await execute(receiver, f"await ws.messages.ack([{message['id']}])")
    await stop_manager(workspace)
    async with mcp_session(workspace, client_id="receiver") as receiver:
        assert value(await execute(receiver, "await ws.messages.read()"))["messages"] == []


@pytest.mark.parametrize("tool", ["execute", "poll"])
async def test_messages_wake_tool_wait_without_cancelling_cell(workspace, tool):
    async with (
        mcp_session(workspace, client_id="sender") as sender,
        mcp_session(workspace, client_id="receiver") as receiver,
    ):
        await execute(receiver, "import asyncio\nws.local['gate'] = asyncio.Event()")
        code = "await ws.local['gate'].wait()\n42"
        if tool == "poll":
            started = decode_result(
                await receiver.call_tool("execute", {"code": code, "wait_ms": 0})
            )
            args = {"exec_id": started["exec_id"], "wait_ms": 30000}
        else:
            args = {"code": code, "wait_ms": 30000}
        waiting = asyncio.create_task(receiver.call_tool(tool, args))
        try:
            async with asyncio.timeout(5):
                while True:
                    if (await rpc(socket_path(workspace), op="status"))["active"]:
                        break
                    await asyncio.sleep(0.01)
            sent = value(await execute(sender, "await ws.messages.send('receiver', 'wake')"))
            woken = decode_result(await asyncio.wait_for(waiting, 5))
            assert woken["state"] == "running"
            assert woken["inbox"]["messages"][0]["id"] == sent["id"]
            await execute(
                receiver, f"await ws.messages.ack([{sent['id']}])\nws.local['gate'].set()"
            )
            completed = await poll_until_done(receiver, woken["exec_id"])
            assert completed["state"] == "succeeded"
            assert result_text(completed).strip() == "42"
        finally:
            waiting.cancel()
            await asyncio.gather(waiting, return_exceptions=True)


async def test_read_wait_and_deduplicated_result_have_current_inbox(workspace):
    async with (
        mcp_session(workspace, client_id="sender") as sender,
        mcp_session(workspace, client_id="receiver") as receiver,
    ):
        cached = await execute(receiver, "42", request_id="cached")
        waiting = decode_result(
            await receiver.call_tool(
                "execute",
                {
                    "code": "await ws.messages.read(wait_ms=30000)",
                    "wait_ms": 0,
                },
            )
        )
        sent = value(await execute(sender, "await ws.messages.send('receiver', 'ready')"))
        async with asyncio.timeout(5):
            while True:
                result = decode_result(
                    await receiver.call_tool("poll", {"exec_id": waiting["exec_id"], "wait_ms": 0})
                )
                if result["state"] == "succeeded":
                    break
                await asyncio.sleep(0.01)
        assert value(result)["messages"] == [sent]
        replayed = await execute(receiver, "42", request_id="cached")
        assert replayed["exec_id"] == cached["exec_id"]
        assert replayed["inbox"]["unacked"] == 1
        failed = await execute(receiver, "raise ValueError('failed')")
        assert failed["state"] == "failed"
        assert failed["inbox"]["unacked"] == 1


async def test_detached_sender_retains_identity_after_disconnect(workspace):
    async with mcp_session(workspace, client_id="receiver") as receiver:
        async with mcp_session(workspace, client_id="sender") as sender:
            started = await execute(
                sender,
                "import asyncio\n"
                "relay_gate = asyncio.Event()\n"
                "async def relay_message():\n"
                "    await relay_gate.wait()\n"
                "    return await ws.messages.send('receiver', 'from background')\n"
                "relay_task = ws.tasks.start(relay_message())",
            )
            assert started["state"] == "succeeded"
        message = value(await execute(receiver, "relay_gate.set()\nawait relay_task"))
        assert message["from"] == "sender"
        assert message["to"] == "receiver"
        assert value(await execute(receiver, "await ws.messages.read()"))["messages"] == [message]
