import asyncio
import json
import math
import os
import subprocess
import sys
from contextlib import asynccontextmanager

import pytest

import mypr_mcp.kernel_api as api
from mypr_mcp.persistence import PersistenceWorker
from mypr_mcp.runtime import Runtime, _close_stores, _open_stores
from mypr_mcp.services import MCPBridge
from mypr_mcp.storage import Storage
from mypr_mcp.task_results import encode_result, load_result, store_result


@asynccontextmanager
async def manager(workspace):
    runtime = Runtime(workspace)
    runtime.persistence = PersistenceWorker()
    runtime.history, runtime.messages = await runtime.io(_open_stores, workspace)
    runtime.mcp = MCPBridge(workspace)
    try:
        yield runtime
    finally:
        await runtime.mcp.close()
        await runtime.io(_close_stores, runtime.history, runtime.messages)
        await runtime.persistence.close()


def test_json_result_limits_and_integrity(tmp_path):
    value = {"unicode": "한국어", "value": [None, True, 42]}
    reference = store_result(tmp_path, "task", "generation", encode_result(value))
    assert load_result(tmp_path, reference) == value
    for invalid in (b"bytes", {1: "value"}, math.nan, "x" * (256 * 1024)):
        with pytest.raises((TypeError, ValueError)):
            encode_result(invalid)
    (tmp_path / reference["path"]).write_text('{"changed":true}')
    with pytest.raises(ValueError, match="size|hash"):
        load_result(tmp_path, reference)


def test_saved_result_preserves_filesystem_surrogates(tmp_path):
    value = {"path": os.fsdecode(b"name-\xff"), "literal": r"\udcff"}
    encoded = encode_result(value)
    assert json.loads(encoded) == value
    reference = store_result(tmp_path, "task", "generation", encoded)
    assert load_result(tmp_path, reference) == value
    with pytest.raises(ValueError, match="256 KiB"):
        encode_result("\udcff" * 50_000)
    with pytest.raises(ValueError, match="invalid Unicode"):
        encode_result("\ud83d\ude00")


def test_fifo_result_is_rejected_without_waiting_for_a_writer(tmp_path):
    reference = store_result(tmp_path, "task", "generation", encode_result({"ok": True}))
    path = tmp_path / reference["path"]
    path.unlink()
    os.mkfifo(path)
    code = (
        "from pathlib import Path\n"
        "import json\n"
        "import sys\n"
        "from mypr_mcp.task_results import load_result\n"
        "load_result(Path(sys.argv[1]), json.loads(sys.argv[2]))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code, str(tmp_path), json.dumps(reference)],
        capture_output=True,
        text=True,
        timeout=3,
    )
    assert result.returncode != 0
    assert "regular file" in result.stderr


async def test_fast_task_result_survives_terminal_event_and_attach(tmp_path, monkeypatch):
    async with manager(tmp_path) as runtime:
        monkeypatch.setenv("MYPR_GENERATION", runtime.generation)

        async def rpc(op, **fields):
            return await runtime.dispatch({
                "op": op, "client_id": "reader", "generation": runtime.generation, **fields
            })

        monkeypatch.setattr(api, "_rpc", rpc)
        tasks = api.TaskManager()

        async def fast():
            return {"answer": 42}

        with api.execution_context({"client_id": "reader", "exec_id": "cell"}):
            handle = tasks.start(fast(), task_id="fast", persist_result=True)
        assert await handle == {"answer": 42}
        await asyncio.gather(*tasks._reporters)
        assert runtime.task_records["fast"]["state"] == "succeeded"
        history_id = f"python:{runtime.generation}:fast"
        record = await runtime.io(runtime.history.get, history_id)
        assert record["result_persisted"]
        assert record["state"] == "succeeded"
        attached = await api.TaskManager().attach(history_id)
        assert attached.result() == {"answer": 42}
        assert attached.status()["result_persisted"]


@pytest.mark.parametrize("value", [None, {"answer": 42}])
async def test_wait_saved_rechecks_gc_without_losing_cached_result(tmp_path, monkeypatch, value):
    async with manager(tmp_path) as runtime:
        monkeypatch.setenv("MYPR_GENERATION", runtime.generation)

        async def rpc(op, **fields):
            return await runtime.dispatch({
                "op": op, "client_id": "reader", "generation": runtime.generation, **fields
            })

        monkeypatch.setattr(api, "_rpc", rpc)
        with api.execution_context({"client_id": "reader", "exec_id": "cell"}):
            handle = api.TaskManager().start(
                asyncio.sleep(0, result=value), task_id="saved", persist_result=True
            )
        await handle.wait_saved()
        history_id = f"python:{runtime.generation}:saved"
        historical = await api.TaskManager().attach(history_id)
        await historical.wait_saved()
        storage = Storage(
            tmp_path, history=runtime.history, active_ids=runtime.storage_active_ids
        )
        result = await storage.gc(dry_run=False, max_bytes=0)
        assert any(item["category"] == "task_results" for item in result["deleted"])
        for task in (handle, historical):
            with pytest.raises(api.ResultUnavailable) as failure:
                await task.wait_saved()
            assert failure.value.operation == "tasks.wait_saved"
            assert task.result() == value


async def test_saved_lsp_config_cas_and_managed_mcp_write(tmp_path):
    async with manager(tmp_path) as runtime:
        await runtime.mcp.configure("sample", {"command": "example"})
        definitions = {"python": {"command": ["server"], "languages": ["python"]}}
        saved = await runtime.code_config({
            "method": "set_lsp", "definitions": definitions, "expected_servers": {}
        })
        snapshot = await runtime.code_config({"method": "get_lsp"})
        assert snapshot["revision"] == saved["revision"]
        assert snapshot["servers"]["python"]["timeout"] == 10.0
        await runtime.mcp.configure("second", {"command": "example"})
        await runtime.code_config({
            "method": "set_lsp", "definitions": {},
            "expected_revision": saved["revision"], "expected_servers": snapshot["servers"],
        })
        assert set(runtime.mcp.config) == {"sample", "second"}
        runtime.mcp.store.path.write_text("[lsp.servers]\n")
        with pytest.raises(RuntimeError, match="changed on disk"):
            await runtime.code_config({"method": "set_lsp", "definitions": {},
                                       "expected_servers": {}})


async def test_offline_client_directory_keeps_identity_and_unacked_count(tmp_path):
    async with manager(tmp_path) as runtime:
        await runtime.io(runtime.history.reserve_client_id, "reader")
        await runtime.io(runtime.history.reserve_client_id, "writer")
        await runtime.io(runtime.history.touch_client, "reader", 123)
        await runtime.io(runtime.messages.send, "writer", "reader", "hello")
        runtime.clients["attached"] = {"client_id": "writer"}
        result = await runtime.dispatch({"op": "message_clients", "connected": False})
        assert result["clients"][0]["id"] == "reader"
        assert result["clients"][0]["last_seen"] == 123
        assert result["clients"][0]["unacked"] == 1
        assert result["clients"][0]["connected"] is False


async def test_persistent_wait_settles_storage_and_waiter_cancel_keeps_reporter(monkeypatch):
    entered, release = asyncio.Event(), asyncio.Event()

    async def rpc(op, **fields):
        if op == "task_result_store":
            entered.set()
            await release.wait()
            return {"path": "result.json"}

    monkeypatch.setattr(api, "_rpc", rpc)
    tasks = api.TaskManager()
    handle = tasks.start(asyncio.sleep(0, result={"answer": 42}), persist_result=True)
    waiter = asyncio.create_task(handle._wait())
    try:
        await asyncio.wait_for(entered.wait(), 1)
        assert handle.result() == {"answer": 42}
        assert handle.status()["result_persisted"] is None
        assert not waiter.done()
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
        assert not handle._reporter.cancelled()
    finally:
        release.set()
        await asyncio.gather(waiter, handle._reporter, return_exceptions=True)
    assert await handle == {"answer": 42}
    await handle.wait_saved()
    assert handle.status()["result_persisted"] is True


@pytest.mark.parametrize("value, store_error", [(b"binary", False), ({"answer": 42}, True)])
async def test_storage_failure_keeps_live_result_and_wait_saved_reports_it(
    value, store_error, monkeypatch
):
    async def rpc(op, **fields):
        if op == "task_result_store" and store_error:
            raise OSError("response lost")

    monkeypatch.setattr(api, "_rpc", rpc)
    handle = api.TaskManager().start(asyncio.sleep(0, result=value), persist_result=True)
    assert await handle == value
    with pytest.raises(api.ResultUnavailable):
        await handle.wait_saved()
    assert handle.status()["result_persisted"] is (None if store_error else False)
    assert handle.status()["warnings"]


async def test_cleanup_waits_for_completed_task_report_and_rejects_new_work(tmp_path, monkeypatch):
    entered, release = asyncio.Event(), asyncio.Event()

    async def rpc(op, **fields):
        if op == "task_result_store":
            entered.set()
            await release.wait()
            return {"path": "result.json"}

    monkeypatch.setattr(api, "_rpc", rpc)
    ws = api.Workspace(tmp_path)
    handle = ws.tasks.start(asyncio.sleep(0, result=42), persist_result=True)
    await asyncio.wait_for(entered.wait(), 1)
    assert handle._task.done()
    cleanup = asyncio.create_task(ws._close_resources())
    try:
        await asyncio.sleep(0)
        assert not cleanup.done()
        new_work = asyncio.sleep(0)
        with pytest.raises(RuntimeError, match="closing"):
            ws.tasks.start(new_work)
        assert new_work.cr_frame is None
    finally:
        release.set()
        await cleanup
    assert handle._reporter.done()
    await handle.wait_saved()


async def test_cleanup_timeout_cancels_report_and_marks_storage_uncertain(tmp_path, monkeypatch):
    entered = asyncio.Event()

    async def rpc(op, **fields):
        if op == "task_result_store":
            entered.set()
            await asyncio.Event().wait()

    monkeypatch.setattr(api, "_rpc", rpc)
    monkeypatch.setattr(api, "_REPORT_DRAIN_TIMEOUT", 0.01)
    ws = api.Workspace(tmp_path)
    handle = ws.tasks.start(asyncio.sleep(0, result=42), persist_result=True)
    await asyncio.wait_for(entered.wait(), 1)
    with pytest.raises(ExceptionGroup) as failure:
        await asyncio.wait_for(ws._close_resources(), 1)
    assert any(isinstance(error, TimeoutError) for error in failure.value.exceptions)
    assert handle._reporter.cancelled()
    assert await handle == 42
    assert handle.status()["result_persisted"] is None
    with pytest.raises(api.ResultUnavailable):
        await handle.wait_saved()


async def test_cleanup_finishes_after_caller_cancellation(tmp_path):
    ws = api.Workspace(tmp_path)
    entered, release = asyncio.Event(), asyncio.Event()

    class Resource:
        closed = False

        async def aclose(self):
            entered.set()
            await release.wait()
            self.closed = True

    resource = Resource()
    ws.browser = resource
    ws.http = None
    ws.code = None
    cleanup = asyncio.create_task(ws._close_resources())
    await asyncio.wait_for(entered.wait(), 1)
    cleanup.cancel()
    await asyncio.sleep(0)
    cleanup.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await cleanup
    assert resource.closed
    assert ws._closing
    await ws._close_resources()


async def test_cleanup_failure_is_replayed_on_later_close(tmp_path):
    ws = api.Workspace(tmp_path)

    class Resource:
        async def aclose(self):
            raise RuntimeError("close failed")

    ws.browser = Resource()
    ws.http = None
    ws.code = None
    with pytest.raises(ExceptionGroup, match="cleanup failed"):
        await ws._close_resources()
    with pytest.raises(ExceptionGroup, match="cleanup failed"):
        await ws._close_resources()


async def test_persistent_failed_task_waits_for_terminal_report(monkeypatch):
    entered, release = asyncio.Event(), asyncio.Event()

    async def rpc(op, **fields):
        if op == "task_event" and fields["event"]["state"] == "failed":
            entered.set()
            await release.wait()

    async def fail():
        raise ValueError("compute failed")

    monkeypatch.setattr(api, "_rpc", rpc)
    handle = api.TaskManager().start(fail(), persist_result=True)
    waiter = asyncio.create_task(handle._wait())
    try:
        await asyncio.wait_for(entered.wait(), 1)
        assert not waiter.done()
    finally:
        release.set()
    with pytest.raises(ValueError, match="compute failed"):
        await waiter
    assert handle._reporter.done()
