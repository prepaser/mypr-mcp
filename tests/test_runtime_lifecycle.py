import asyncio

import pytest

import mypr_mcp.runtime as runtime_module
from mypr_mcp.runtime import Runtime


class Bridge:
    async def close(self):
        pass


async def test_shutdown_closes_kernel_started_by_in_progress_reset(tmp_path, monkeypatch):
    runtime = Runtime(tmp_path)
    entered_close = asyncio.Event()
    release_close = asyncio.Event()
    events = []

    async def close_kernel():
        if not events:
            events.append("reset-close")
            entered_close.set()
            await release_close.wait()
        else:
            events.append("shutdown-close")

    async def start_kernel():
        events.append("start-kernel")
        runtime.healthy = True

    async def noop(*args, **kwargs):
        pass

    monkeypatch.setattr(runtime, "close_kernel", close_kernel)
    monkeypatch.setattr(runtime, "start_kernel", start_kernel)
    monkeypatch.setattr(runtime, "lose_python_tasks", noop)
    monkeypatch.setattr(runtime, "close_shells", noop)
    monkeypatch.setattr(runtime, "new_shells", lambda: object())
    monkeypatch.setattr(runtime_module, "MCPBridge", lambda _: Bridge())
    monkeypatch.setattr(runtime_module, "ScanService", lambda *args: object())
    runtime.mcp = Bridge()

    reset = asyncio.create_task(runtime.reset(None))
    shutdown = None
    try:
        await asyncio.wait_for(entered_close.wait(), 2)
        runtime.stopping.set()
        shutdown = asyncio.create_task(runtime.shutdown_resources())
        await asyncio.sleep(0)
        assert not shutdown.done()
    finally:
        release_close.set()
        tasks = [reset]
        if shutdown is not None:
            tasks.append(shutdown)
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, BaseException):
                raise result

    assert events == ["reset-close", "start-kernel", "shutdown-close"]


async def test_reset_does_not_start_after_shutdown_has_begun(tmp_path, monkeypatch):
    runtime = Runtime(tmp_path)
    started = False

    async def start_kernel():
        nonlocal started
        started = True

    monkeypatch.setattr(runtime, "start_kernel", start_kernel)
    async with runtime._lifecycle_lock:
        runtime.stopping.set()
        reset = asyncio.create_task(runtime.reset(None))
        await asyncio.sleep(0)
        assert not reset.done()

    with pytest.raises(RuntimeError, match="manager is stopping"):
        await reset
    assert not started
    assert not runtime.resetting
