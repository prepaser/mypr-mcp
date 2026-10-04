import asyncio
import os
import threading
from types import SimpleNamespace

import pytest
import pytest_asyncio

import mypr_mcp.runtime as runtime_module
from mypr_mcp.dependency_service import DependencyService
from mypr_mcp.dependency_store import DependencyStore
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


async def _pending_binary(runtime, monkeypatch):
    store = DependencyStore(runtime.workspace / "data", runtime.workspace / "cache")
    entered = asyncio.Event()
    cancelled = asyncio.Event()

    async def inspect(_name):
        return {"status": "missing"}

    async def install(_name):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    monkeypatch.setattr(store, "inspect", inspect)
    monkeypatch.setattr(store, "_install", install)
    service = DependencyService(
        runtime.workspace, runtime.root / "venv/bin/python", {}, None, None, store=store
    )
    runtime.dependencies = service
    caller = asyncio.create_task(service.ensure(["rg"]))
    await asyncio.wait_for(entered.wait(), 2)
    caller.cancel()
    with pytest.raises(asyncio.CancelledError):
        await caller
    return service, cancelled


@pytest.mark.parametrize("op", ["reset", "stop"])
async def test_lifecycle_rejects_dependency_install_after_caller_exit(tmp_path, monkeypatch, op):
    runtime = Runtime(tmp_path)
    service, cancelled = await _pending_binary(runtime, monkeypatch)
    try:
        assert service.active_count == 1
        assert not runtime.shells.active
        with pytest.raises(RuntimeError, match="active work"):
            await runtime._dispatch({"op": op})
        assert not runtime.resetting and not runtime.stopping.is_set()
        assert not cancelled.is_set()
    finally:
        await service.close()


async def test_forced_reset_settles_dependency_store_before_new_kernel(tmp_path, monkeypatch):
    runtime = Runtime(tmp_path)
    service, cancelled = await _pending_binary(runtime, monkeypatch)
    replacement = object()

    async def noop(*args, **kwargs):
        pass

    async def start_kernel():
        assert cancelled.is_set()
        assert not service.active_count
        assert runtime.dependencies is replacement

    runtime.mcp = SimpleNamespace(close=noop)
    monkeypatch.setattr(runtime, "close_kernel", noop)
    monkeypatch.setattr(runtime, "lose_python_tasks", noop)
    monkeypatch.setattr(runtime, "close_shells", noop)
    monkeypatch.setattr(runtime, "start_kernel", start_kernel)
    monkeypatch.setattr(runtime, "_register_manager", noop)
    monkeypatch.setattr(runtime, "new_dependencies", lambda config: replacement)
    monkeypatch.setattr(runtime_module, "MCPBridge", lambda *args, **kwargs: runtime.mcp)
    try:
        assert (await runtime._dispatch({"op": "reset", "force": True}))["reset"]
    finally:
        await service.close()


@pytest.mark.parametrize("op", ["shell_start", "scan_start", "search", "git"])
async def test_new_work_rejects_replaced_workspace(tmp_path, op):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    runtime = Runtime(workspace)
    runtime.healthy = True
    workspace.rename(tmp_path / "moved")
    workspace.mkdir()

    assert not runtime.workspace_available()
    with pytest.raises(RuntimeError, match="workspace moved"):
        await runtime._dispatch({"op": op})
