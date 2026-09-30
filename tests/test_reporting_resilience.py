import asyncio
import json
from collections.abc import Mapping
from types import SimpleNamespace

import pytest

import mypr_mcp.cells as cells
import mypr_mcp.kernel_api as api
from mypr_mcp.diagnostics import error_info


class BrokenDetails(Exception):
    @property
    def details(self):
        raise RuntimeError("metadata getter failed")


class BrokenMapping(Mapping):
    def __iter__(self):
        raise RuntimeError("metadata iteration failed")

    def __len__(self):
        return 1

    def __getitem__(self, key):
        raise KeyError(key)


@pytest.mark.parametrize("details", [BrokenMapping(), {"number": 10 ** 5000}])
def test_error_metadata_failure_preserves_original_error(details):
    exc = ValueError("original failure")
    info = error_info(exc, operation="cell.execute", details=details)
    assert info["message"] == "ValueError: original failure"
    assert info["code"] == "invalid_request"
    assert info["truncated"]
    assert len(json.dumps(info).encode()) <= 1024


async def test_failed_metadata_keeps_cell_status_and_terminal_fallback(monkeypatch):
    delivered = asyncio.Event()
    calls = []

    async def rpc(op, **fields):
        calls.append((op, fields))
        delivered.set()

    def send(*args, **kwargs):
        raise OSError("IOPub unavailable")

    monkeypatch.setattr(cells, "_rpc", rpc)
    task = asyncio.create_task(asyncio.sleep(0))
    await task
    handle = cells.CellHandle("e" * 32, task, api.OutputBuffer(), generation="generation")
    exc = BrokenDetails("original failure")
    handle.set_state("failed", exc)
    kernel = SimpleNamespace(session=SimpleNamespace(send=send), iopub_socket=None)
    executor = cells.CellExecutor(kernel, None, api.TaskManager())
    assert handle.status()["error_info"]["truncated"]
    executor._send_terminal(handle, {}, "failed", exc)
    await asyncio.wait_for(delivered.wait(), 1)
    assert calls[0][0] == "cell_terminal"
    assert calls[0][1]["error_info"]["message"] == "BrokenDetails: original failure"
    assert not handle._terminal_report_pending


@pytest.mark.parametrize("fail", [False, True])
async def test_fast_task_drains_all_output_before_terminal(monkeypatch, fail):
    events = []

    async def rpc(op, **fields):
        events.append(fields["event"])

    async def emit():
        output = api._output_buffer.get()
        output.write("out" * 4000, stream="stdout")
        output.write("가" * 4000, stream="stderr")
        if fail:
            raise ValueError("failed after output")
        return 42

    monkeypatch.setattr(api, "_rpc", rpc)
    tasks = api.TaskManager()
    handle = tasks.start(emit())
    await asyncio.gather(handle._task, return_exceptions=True)
    await asyncio.gather(*tasks._reporters)
    for stream, expected in (("stdout", "out" * 4000), ("stderr", "가" * 4000)):
        actual = "".join(
            event.get("output_delta", "")
            for event in events if event.get("output_stream") == stream
        )
        assert actual == expected
    assert events[-1]["state"] == ("failed" if fail else "succeeded")
    assert not events[-1]["output_truncated"]


async def test_unconfirmed_task_output_is_flagged_without_replaying(monkeypatch):
    events = []
    failed = False

    async def rpc(op, **fields):
        nonlocal failed
        event = fields["event"]
        if event.get("output_delta") and not failed:
            failed = True
            raise OSError("response lost after possible append")
        events.append(event)

    async def emit():
        api._output_buffer.get().write("x" * 10000)

    monkeypatch.setattr(api, "_rpc", rpc)
    tasks = api.TaskManager()
    handle = tasks.start(emit())
    await handle
    await asyncio.gather(*tasks._reporters)
    assert handle.output() == "x" * 10000
    assert events[-1]["output_truncated"]
    assert handle.status()["warnings"][0]["code"] == "task_output_persistence_unknown"
    assert events[-1]["warnings"] == handle.status()["warnings"]
    assert sum(len(event.get("output_delta", "")) for event in events) == 0


async def test_persistent_report_timeout_keeps_task_result(monkeypatch):
    async def blocked_rpc(*args, **kwargs):
        await asyncio.Event().wait()

    monkeypatch.setattr(api, "_rpc", blocked_rpc)
    monkeypatch.setattr(api, "_REPORT_RPC_TIMEOUT", 0.01)
    tasks = api.TaskManager()
    handle = tasks.start(asyncio.sleep(0, result=42), persist_result=True)

    assert await asyncio.wait_for(handle, 1) == 42
    assert handle.status()["result_persisted"] is None
    assert handle.status()["warnings"]


async def test_stalled_reporter_stops_after_first_unconfirmed_output(monkeypatch):
    calls = []

    async def blocked_rpc(op, **kwargs):
        calls.append(op)
        await asyncio.Event().wait()

    async def emit():
        api._output_buffer.get().write("x" * 200_000)

    monkeypatch.setattr(api, "_rpc", blocked_rpc)
    monkeypatch.setattr(api, "_REPORT_RPC_TIMEOUT", 0.01)
    handle = api.TaskManager().start(emit(), persist_result=True)

    assert await asyncio.wait_for(handle, 1) is None
    assert calls.count("task_event") <= 2
    assert calls.count("task_result_store") == 1
    assert calls.count("task_terminal") == 1
    assert handle.status()["warnings"][0]["code"] == "task_output_persistence_unknown"
