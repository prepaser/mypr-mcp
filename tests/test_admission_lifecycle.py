import asyncio
import os
import threading

import pytest
import pytest_asyncio

import mypr_mcp.runtime as runtime_module
from mypr_mcp.history import History
from mypr_mcp.persistence import PersistenceWorker
from mypr_mcp.runtime import Runtime


@pytest_asyncio.fixture
async def persisted_runtime(tmp_path):
    runtime = Runtime(tmp_path)
    (runtime.root / "runs").mkdir()
    runtime.persistence = PersistenceWorker()
    runtime.history = await runtime.persistence.call(History, tmp_path)
    runtime.healthy = True
    try:
        yield runtime
    finally:
        if runtime.history is not None and runtime.persistence is not None:
            await runtime.persistence.call(runtime.history.close)
            await runtime.persistence.close()


async def _start_gated_admission(runtime, monkeypatch):
    entered = asyncio.Event()
    release = threading.Event()
    original = runtime_module._persist_execution
    loop = asyncio.get_running_loop()

    def gated_persist(*args, **kwargs):
        loop.call_soon_threadsafe(entered.set)
        release.wait()
        return original(*args, **kwargs)

    monkeypatch.setattr(runtime_module, "_persist_execution", gated_persist)
    admission = asyncio.create_task(
        runtime.admit_execution("client-one", "connection-one", {"code": "42"})
    )
    try:
        await asyncio.wait_for(entered.wait(), timeout=10)
    except BaseException:
        release.set()
        await asyncio.gather(admission, return_exceptions=True)
        raise
    return admission, release


async def test_reset_waits_for_pending_admission_then_rejects_active_work(
    persisted_runtime, monkeypatch
):
    runtime = persisted_runtime
    reset_calls = []

    async def reset_without_kernel(current):
        reset_calls.append(current)

    runtime.reset = reset_without_kernel
    admission, release = await _start_gated_admission(runtime, monkeypatch)
    started = asyncio.Event()

    async def reset():
        started.set()
        return await runtime._dispatch({"op": "reset", "force": False})

    reset_task = asyncio.create_task(reset())
    try:
        await started.wait()
        assert not reset_task.done()
        assert not reset_calls
    finally:
        release.set()
        await asyncio.gather(admission, reset_task, return_exceptions=True)

    record = await admission
    assert record["state"] == "queued"
    assert runtime.execs[record["id"]] is record
    with pytest.raises(RuntimeError, match="active work"):
        await reset_task
    assert not runtime.resetting
    assert not reset_calls


async def test_stop_waits_for_pending_admission_then_rejects_active_work(
    persisted_runtime, monkeypatch
):
    runtime = persisted_runtime
    admission, release = await _start_gated_admission(runtime, monkeypatch)
    started = asyncio.Event()

    async def stop():
        started.set()
        return await runtime._dispatch(
            {"op": "stop", "force": False, "manager_pid": os.getpid()}
        )

    stop_task = asyncio.create_task(stop())
    try:
        await started.wait()
        assert not stop_task.done()
        assert not runtime.stopping.is_set()
    finally:
        release.set()
        await asyncio.gather(admission, stop_task, return_exceptions=True)

    record = await admission
    assert record["state"] == "queued"
    assert runtime.execs[record["id"]] is record
    with pytest.raises(RuntimeError, match="active work"):
        await stop_task
    assert not runtime.stopping.is_set()
