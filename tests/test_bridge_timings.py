import json

import pytest
from conftest import decode_result, execute, mcp_session, poll_until_done, result_text

import mypr_mcp.bridge as bridge_module
from mypr_mcp.bridge import ConnectionBridge
from mypr_mcp.cli import tool_result


async def test_bridge_reports_previous_sample_without_extra_requests(tmp_path, monkeypatch):
    bridge = ConnectionBridge(tmp_path)
    requests = []

    async def ready():
        pass

    async def rpc(path, **fields):
        requests.append(fields)
        return {"state": "succeeded", "_timing_ms": {"manager_dispatch": 2.0}}

    monkeypatch.setattr(bridge, "wait_ready", ready)
    monkeypatch.setattr(bridge_module, "rpc", rpc)
    first = await bridge.request("poll", exec_id="one")
    second = await bridge.request("poll", exec_id="two")
    assert len(requests) == 2
    assert "_bridge_sample" not in requests[0]
    assert requests[1]["_bridge_sample"] == {
        key: first["_timing_ms"][key]
        for key in ("bridge_ready", "manager_rpc", "bridge_total")
    }
    assert second["_timing_ms"]["manager_dispatch"] == 2.0
    result = await tool_result(second)
    assert result.structured_content == {"state": "succeeded"}
    assert "timing" not in result.content[0].text
    assert result.meta["timing_ms"]["render"] >= 0
    assert result.meta["timing_ms"]["bridge_total"] >= result.meta["timing_ms"]["manager_rpc"]


async def test_bridge_keeps_failed_request_timing_for_next_call(tmp_path, monkeypatch):
    bridge = ConnectionBridge(tmp_path)

    async def unavailable():
        raise RuntimeError("unavailable")

    monkeypatch.setattr(bridge, "wait_ready", unavailable)
    with pytest.raises(RuntimeError, match="unavailable"):
        await bridge.request("init")
    assert bridge._last_timing["bridge_total"] >= bridge._last_timing["bridge_ready"]


async def test_performance_is_queryable_without_expanding_cell_output(workspace):
    async with mcp_session(workspace) as session:
        response = await session.call_tool("execute", {"code": "42", "wait_ms": 1000})
        payload = decode_result(response)
        if payload["state"] in {"queued", "running"} or payload["has_more"]:
            payload = await poll_until_done(session, payload["exec_id"])
        assert payload["state"] == "succeeded"
        assert {"bridge_ready", "manager_rpc", "bridge_total", "manager_dispatch", "render"} <= (
            response.meta["timing_ms"].keys()
        )
        assert "_timing_ms" not in response.structured_content
        measured = await execute(session, "print(__import__('json').dumps(await ws.performance()))")
        metrics = json.loads(result_text(measured))
        assert metrics["manager_dispatch"]["execute"]["total_count"] >= 1
        assert metrics["bridge"]["bridge_total"]["total_count"] >= 1
        assert metrics["storage"]["work"]["total_count"] >= 1
        assert metrics["kernel"]["roundtrip_ms"]["total_count"] >= 1
