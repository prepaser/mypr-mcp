import asyncio
from types import SimpleNamespace

import pytest

import mypr_mcp.runtime as runtime_module
from mypr_mcp.runtime import Runtime


class Bridge:
    async def close(self):
        pass


async def test_shutdown_attempts_independent_cleanup_after_failure(tmp_path):
    runtime = Runtime(tmp_path)
    attempts = []

    async def web_close():
        attempts.append("web")
        raise RuntimeError("web cleanup failed")

    async def kernel_close():
        attempts.append("kernel")

    async def shells_close():
        attempts.append("shells")

    async def mcp_close():
        attempts.append("mcp")

    runtime.web = SimpleNamespace(close=web_close)
    runtime.close_kernel = kernel_close
    runtime.close_shells = shells_close
    runtime.mcp = SimpleNamespace(close=mcp_close)
    with pytest.raises(ExceptionGroup, match="Manager resource cleanup") as raised:
        await runtime.shutdown_resources()
    assert attempts == ["web", "kernel", "shells", "mcp"]
    assert str(raised.value.exceptions[0]) == "web cleanup failed"


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
    monkeypatch.setattr(runtime_module, "MCPBridge", lambda _, **kwargs: Bridge())
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
        try:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, BaseException):
                    raise result
        finally:
            await runtime._unregister_manager()

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


async def test_reset_closes_resources_in_order(tmp_path, monkeypatch):
    runtime = Runtime(tmp_path)
    events = []

    async def web_reset():
        events.append("web.reset")

    async def dependency_close():
        events.append("dependencies.close")

    async def kernel_close():
        events.append("kernel")

    async def lose_tasks(*args, **kwargs):
        events.append("python")

    async def shells_close():
        events.append("shells")

    async def mcp_close():
        events.append("mcp")

    runtime.web = SimpleNamespace(reset=web_reset)
    runtime.dependencies = SimpleNamespace(close=dependency_close)
    runtime.close_kernel = kernel_close
    runtime.lose_python_tasks = lose_tasks
    runtime.close_shells = shells_close
    runtime.mcp = SimpleNamespace(close=mcp_close)

    await runtime._close_resources("reset")

    assert events == [
        "web.reset", "dependencies.close", "kernel", "python", "shells", "mcp",
    ]


async def test_reset_cleanup_failure_stops_before_recreation(tmp_path, monkeypatch):
    runtime = Runtime(tmp_path)
    events = []
    started = False

    async def dependency_close():
        events.append("dependencies")
        raise RuntimeError("dependency cleanup failed")

    async def start_kernel():
        nonlocal started
        started = True

    runtime.web = SimpleNamespace(reset=lambda: asyncio.sleep(0))
    runtime.dependencies = SimpleNamespace(close=dependency_close)
    old_mcp = Bridge()
    runtime.mcp = old_mcp
    monkeypatch.setattr(runtime, "start_kernel", start_kernel)

    with pytest.raises(RuntimeError, match="dependency cleanup failed"):
        await runtime.reset(None)

    assert events == ["dependencies"]
    assert runtime.mcp is old_mcp
    assert not started
    assert not runtime.healthy


async def test_shutdown_is_safe_to_repeat(tmp_path):
    runtime = Runtime(tmp_path)
    calls = []

    async def close(name):
        calls.append(name)

    runtime.web = SimpleNamespace(close=lambda: close("web"))
    runtime.close_kernel = lambda: close("kernel")
    runtime.close_shells = lambda: close("shells")
    runtime.mcp = SimpleNamespace(close=lambda: close("mcp"))
    runtime.mail = SimpleNamespace(close=lambda: close("mail"))

    await runtime.shutdown_resources()
    await runtime.shutdown_resources()

    assert calls == [
        "web", "kernel", "shells", "mcp", "mail",
        "web", "kernel", "shells", "mcp", "mail",
    ]
