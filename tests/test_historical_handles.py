from __future__ import annotations

import asyncio

import pytest

import mypr_mcp.kernel_api as kernel_api
from mypr_mcp.history import History
from mypr_mcp.journal import append_events
from mypr_mcp.kernel_api import HistoricalTask, ResultUnavailable
from mypr_mcp.runtime import Runtime


def _record(*, output, cursor, has_more=True):
    return {
        "id": "task-1",
        "history_id": "python:oldgen:task-1",
        "kind": "python",
        "generation": "oldgen",
        "state": "succeeded",
        "client_id": "client",
        "connection_id": "connection",
        "output": output,
        "cursor": cursor,
        "has_more": has_more,
    }


@pytest.mark.asyncio
async def test_historical_task_reads_all_durable_pages_and_preserves_streams(monkeypatch):
    first = {"stream": "stdout", "text": "first"}
    pages = {
        1: [
            {"stream": "stderr", "text": "error"},
            {"stream": "stdout", "text": " second"},
        ],
        3: [{"stream": "stdout", "text": " third"}],
    }
    calls = []

    async def rpc(op, **fields):
        assert op == "history_task_read"
        cursor = fields["cursor"]
        calls.append(cursor)
        events = pages.get(cursor, [])
        return {
            "id": "task-1",
            "history_id": "python:oldgen:task-1",
            "generation": "oldgen",
            "output": events,
            "cursor": cursor + len(events),
            "has_more": cursor + len(events) < 4,
            "truncated": False,
        }

    monkeypatch.setattr(kernel_api, "_rpc", rpc)
    task = HistoricalTask(_record(output=[first], cursor=1))
    cursor = None
    pieces = []
    while True:
        page = await task.read(cursor, max_bytes=6)
        pieces.append(page["output"])
        if not page["has_more"]:
            break
        cursor = page["cursor"]
    assert "".join(pieces) == "firsterror second third"
    assert calls == [1, 3]

    stderr = await task.read(stream="stderr", max_bytes=100)
    assert stderr["output"] == "error"
    assert stderr["has_more"] is False


@pytest.mark.asyncio
async def test_historical_task_concurrent_page_load_is_single_flight(monkeypatch):
    entered = asyncio.Event()
    release = asyncio.Event()
    calls = 0

    async def rpc(op, **fields):
        nonlocal calls
        calls += 1
        entered.set()
        await release.wait()
        return {
            "output": [{"stream": "stdout", "text": "tail"}],
            "cursor": 2,
            "has_more": False,
            "truncated": False,
        }

    monkeypatch.setattr(kernel_api, "_rpc", rpc)
    task = HistoricalTask(_record(output=[{"text": "head"}], cursor=1))
    cursor = (await task.read(max_bytes=4))["cursor"]
    first = asyncio.create_task(task.read(cursor, max_bytes=100))
    await entered.wait()
    second = asyncio.create_task(task.read(cursor, max_bytes=100))
    release.set()
    pages = await asyncio.gather(first, second)
    assert calls == 1
    assert [page["output"] for page in pages] == ["tail", "tail"]


@pytest.mark.asyncio
async def test_historical_task_reads_more_than_one_megabyte_in_pages(monkeypatch):
    chunk = "x" * (512 * 1024)

    async def rpc(op, **fields):
        cursor = fields["cursor"]
        event = {"stream": "stdout", "text": chunk}
        return {
            "output": [event],
            "cursor": cursor + 1,
            "has_more": cursor < 3,
            "truncated": False,
        }

    monkeypatch.setattr(kernel_api, "_rpc", rpc)
    task = HistoricalTask(_record(output=[], cursor=1))
    first = await task.read(max_bytes=1024 * 1024)
    assert len(first["output"]) == 1024 * 1024
    assert first["has_more"] is True
    second = await task.read(first["cursor"], max_bytes=1024 * 1024)
    assert len(second["output"]) == 512 * 1024
    assert second["has_more"] is False


@pytest.mark.asyncio
async def test_historical_task_result_and_await_are_typed_unavailable():
    task = HistoricalTask(_record(output=[], cursor=0, has_more=False))
    with pytest.raises(ResultUnavailable):
        task.result()
    with pytest.raises(ResultUnavailable):
        await task


@pytest.mark.asyncio
async def test_history_task_read_preserves_generation_and_owner_metadata(tmp_path):
    root = tmp_path
    history = History(root)
    record = {
        "id": "task-1",
        "kind": "python",
        "generation": "oldgen",
        "client_id": "client",
        "connection_id": "connection",
        "exec_id": "cell-1",
        "state": "succeeded",
    }
    history_id = "python:oldgen:task-1"
    history.record("python", record, entity_id=history_id)
    (root / ".mypr" / "runs").mkdir(parents=True)
    runtime = Runtime(root)
    runtime.history = history
    runtime.output_limit = 1024 * 1024
    runtime.response_limit = 32768
    journal = runtime.task_journal_path(record)
    append_events(journal, [{"type": "stream", "stream": "stdout", "text": "saved"}])

    result = await Runtime._dispatch(
        runtime,
        {"op": "history_task_read", "id": history_id, "cursor": 0, "max_bytes": 1024},
    )
    assert result["history_id"] == history_id
    assert result["generation"] == "oldgen"
    assert result["client_id"] == "client"
    assert result["connection_id"] == "connection"
    assert result["output"][0]["text"] == "saved"

    with pytest.raises(ValueError, match="History ID"):
        await Runtime._dispatch(
            runtime,
            {"op": "history_task_read", "id": "task-1", "cursor": 0},
        )


async def test_historical_expect_loads_from_cursor_after_large_prefix(monkeypatch):
    async def rpc(op, **fields):
        assert fields["max_bytes"] == 10
        return {"output": [{"stream": "stdout", "text": "needle"}], "cursor": 2, "has_more": False}

    monkeypatch.setattr(kernel_api, "_rpc", rpc)
    task = HistoricalTask(_record(output=[{"text": "x" * 70000}], cursor=1))
    cursor = task._encode_output_cursor("all", 70000)
    result = await task.expect("needle", cursor, max_scan_bytes=10)
    assert result["matched"]
    assert task._decode_output_cursor(result["cursor"], "all") == 70006


async def test_historical_expect_checks_cached_match_before_loading(monkeypatch):
    async def rpc(op, **fields):
        raise AssertionError("matched output should not load more history")

    monkeypatch.setattr(kernel_api, "_rpc", rpc)
    task = HistoricalTask(_record(output=[{"text": "needle"}], cursor=1))
    assert (await task.expect("needle"))["matched"]


async def test_historical_expect_bounds_rpc_time_and_scan_budget(monkeypatch):
    async def blocked(op, **fields):
        await asyncio.sleep(10)

    monkeypatch.setattr(kernel_api, "_rpc", blocked)
    task = HistoricalTask(_record(output=[], cursor=0))
    result = await task.expect("needle", timeout=0.01)
    assert result["reason"] == "timeout"

    async def one_byte(op, **fields):
        assert fields["max_bytes"] == 1
        return {"output": [{"text": "x"}], "cursor": 1, "has_more": True}

    monkeypatch.setattr(kernel_api, "_rpc", one_byte)
    result = await task.expect("needle", max_scan_bytes=1)
    assert result["reason"] == "limit"
