from datetime import UTC, datetime

import pytest

import mypr_mcp.kernel_api as kernel_api
from mypr_mcp.kernel_api import RPCError, Workspace, execution_context
from mypr_mcp.timer_api import TimerAPI


@pytest.mark.asyncio
async def test_timer_api_forwards_client_scoped_operations(monkeypatch, tmp_path):
    calls = []

    async def rpc(op, **fields):
        calls.append((op, fields))
        return fields

    monkeypatch.setattr(kernel_api, "_rpc", rpc)
    ws = Workspace(tmp_path)
    with execution_context({"client_id": "calm-otter", "connection_id": "conn"}):
        await ws.timers.start(seconds=10, label="review")
        await ws.timers.start(at=datetime(2030, 1, 1, tzinfo=UTC))
        await ws.timers.check("timer-1")
        await ws.timers.list(state="expired", limit=2, cursor=3)
        await ws.timers.cancel("timer-1")
        await ws.timers.ack(["timer-1"])

    assert calls == [
        ("timer_start", {"seconds": 10, "label": "review"}),
        ("timer_start", {"at": "2030-01-01T00:00:00Z", "label": ""}),
        ("timer_check", {"timer_id": "timer-1"}),
        ("timer_list", {"limit": 2, "state": "expired", "cursor": 3}),
        ("timer_cancel", {"timer_id": "timer-1"}),
        ("timer_ack", {"ids": ["timer-1"]}),
    ]


@pytest.mark.asyncio
async def test_timer_api_requires_one_deadline_and_initialized_client(tmp_path):
    timers = Workspace(tmp_path).timers
    with pytest.raises(RPCError, match="initialized client"):
        await timers.list()

    with execution_context({"client_id": "calm-otter"}):
        with pytest.raises(ValueError, match="exactly one"):
            await timers.start()
        with pytest.raises(ValueError, match="exactly one"):
            await timers.start(1, at="2030-01-01T00:00:00Z")
        with pytest.raises(ValueError, match="timezone"):
            await timers.start(at="2030-01-01T00:00:00")


def test_timer_api_is_exposed_and_helpful(tmp_path):
    ws = Workspace(tmp_path)
    assert isinstance(ws.timers, TimerAPI)
    assert "ws.timers.start" in ws.help("timers")
    assert "seconds" in ws.help("timers.start")
