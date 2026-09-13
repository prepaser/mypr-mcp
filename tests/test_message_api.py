from __future__ import annotations

import pytest

import mypr_mcp.kernel_api as kernel_api
from mypr_mcp.kernel_api import RPCError, Workspace, execution_context


@pytest.mark.asyncio
async def test_messages_forward_operations_and_arguments(monkeypatch, tmp_path):
    calls = []

    async def fake_rpc(op, **fields):
        calls.append((op, fields))
        if op == "message_send":
            return {"id": 7, "to": fields["to"], "text": fields["text"]}
        if op == "message_read":
            return {"messages": [], "next_cursor": None, "has_more": False}
        return 2

    monkeypatch.setattr(kernel_api, "_rpc", fake_rpc)
    ws = Workspace(tmp_path)
    with execution_context({"client_id": "calm-otter", "connection_id": "conn"}):
        sent = await ws.messages.send("bright-fox", "hello")
        read = await ws.messages.read(limit=5, after=7, wait_ms=250)
        acknowledged = await ws.messages.ack([7, 8])

    assert sent == {"id": 7, "to": "bright-fox", "text": "hello"}
    assert read == {"messages": [], "next_cursor": None, "has_more": False}
    assert acknowledged == 2
    assert calls == [
        ("message_send", {"to": "bright-fox", "text": "hello"}),
        ("message_read", {"limit": 5, "wait_ms": 250, "after": 7}),
        ("message_ack", {"ids": [7, 8]}),
    ]


@pytest.mark.asyncio
async def test_messages_require_an_initialized_client(monkeypatch, tmp_path):
    async def unexpected_rpc(*args, **kwargs):
        raise AssertionError("RPC must not be called")

    monkeypatch.setattr(kernel_api, "_rpc", unexpected_rpc)
    messages = Workspace(tmp_path).messages

    with pytest.raises(RPCError, match="initialized client"):
        await messages.send("bright-fox", "hello")
    with pytest.raises(RPCError, match="initialized client"):
        await messages.read()
    with pytest.raises(RPCError, match="initialized client"):
        await messages.ack([1])
