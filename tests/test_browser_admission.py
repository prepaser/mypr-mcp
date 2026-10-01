from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

import mypr_mcp.runtime as runtime_module
from mypr_mcp.browser_service import BrowserService
from mypr_mcp.runtime import _UNHANDLED, Runtime


class _BlockingShells:
    def __init__(self):
        self.started = asyncio.Event()
        self.cancelled = asyncio.Event()

    async def start(self, command, **kwargs):
        self.started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.cancelled.set()
            raise

    async def wait(self, ident):
        return {"state": "succeeded", "result": {"returncode": 0}}

    async def cancel(self, ident):
        return {"id": ident, "state": "cancelled"}


@pytest.mark.asyncio
async def test_browser_close_cancels_inflight_install(tmp_path, monkeypatch):
    monkeypatch.setenv("PLAYWRIGHT_BROWSERS_PATH", str(tmp_path / "cache"))
    shells = _BlockingShells()
    service = BrowserService(tmp_path, Path("/usr/bin/python"), shells=shells)
    service._sdk_version = lambda: asyncio.sleep(0, result="1.0")
    service._installed = lambda browser: asyncio.sleep(0, result=False)

    ensure = asyncio.create_task(service.ensure("chromium"))
    await shells.started.wait()
    await asyncio.wait_for(service.close(), 1)
    with pytest.raises(RuntimeError, match="closed"):
        await ensure
    assert shells.cancelled.is_set()
    assert not service._operation_tasks


@pytest.mark.asyncio
async def test_browser_close_cancels_inflight_server_start(tmp_path):
    service = BrowserService(tmp_path, Path("/usr/bin/python"))
    started = asyncio.Event()

    async def start_server(*args, **kwargs):
        started.set()
        await asyncio.Event().wait()

    service._ensure_installed = lambda *args, **kwargs: asyncio.sleep(0)
    service._start_server = start_server
    ensure = asyncio.create_task(service.ensure("chromium"))
    await started.wait()
    await asyncio.wait_for(service.close(), 1)
    with pytest.raises(RuntimeError, match="closed"):
        await ensure


@pytest.mark.asyncio
async def test_browser_close_reaps_process_created_during_launch(tmp_path, monkeypatch):
    service = BrowserService(tmp_path, Path("/usr/bin/python"))
    original = asyncio.create_subprocess_exec
    launched = asyncio.Event()
    release = asyncio.Event()
    process = None

    async def delayed_launch(*args, **kwargs):
        nonlocal process
        process = await original(
            sys.executable,
            "-c",
            "import time; time.sleep(60)",
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        launched.set()
        await release.wait()
        return process

    monkeypatch.setattr(asyncio, "create_subprocess_exec", delayed_launch)
    service._ensure_installed = lambda *args, **kwargs: asyncio.sleep(0)
    ensure = asyncio.create_task(service.ensure("chromium", install=False))
    await launched.wait()
    closing = asyncio.create_task(service.close())
    await asyncio.sleep(0)
    assert not closing.done()
    ensure.cancel()
    release.set()
    await closing
    with pytest.raises(asyncio.CancelledError):
        await ensure
    assert process is not None and process.returncode is not None
    assert not any(
        task.get_name() == "mypr:browser-launch" and not task.done()
        for task in asyncio.all_tasks()
    )


def _runtime(tmp_path: Path):
    runtime = Runtime.__new__(Runtime)
    runtime.workspace = tmp_path
    runtime.py = Path("/usr/bin/python")
    runtime.km = SimpleNamespace(provisioner=SimpleNamespace(pid=1))
    runtime.shells = object()
    runtime.clients = {}
    runtime.restarting = None
    runtime.resetting = False
    runtime.stopping = asyncio.Event()
    runtime.healthy = True
    runtime.generation = "old"
    runtime.workspace_id = "workspace"
    runtime.workspace_available = lambda: True
    runtime.browser = None
    runtime._admission_lock = asyncio.Lock()
    runtime._resource_lock = asyncio.Lock()

    async def no_history(op, req):
        return _UNHANDLED

    runtime._dispatch_history = no_history
    return runtime


@pytest.mark.asyncio
async def test_browser_start_does_not_hold_global_admission_during_ensure(tmp_path, monkeypatch):
    runtime = _runtime(tmp_path)
    started = asyncio.Event()
    release = asyncio.Event()
    closed = asyncio.Event()

    class FakeBrowserService:
        def __init__(self, *args, generation, **kwargs):
            self.generation = generation

        async def ensure(self, browser, *, launch_options=None, track=None):
            started.set()
            await release.wait()
            return {"endpoint": "ws://127.0.0.1:1234/old", "generation": self.generation}

        async def close(self):
            closed.set()
            release.set()

    monkeypatch.setattr(runtime_module, "BrowserService", FakeBrowserService)

    async def handlers(op, req, context):
        if op == "shell_start":
            return {"started": True}
        if op == "reset":
            async with runtime._admission_lock:
                runtime.resetting = True
                old = runtime.browser
                runtime.browser = None
                runtime.generation = "new"
            if old is not None:
                await old.close()
            runtime.resetting = False
            return {"reset": True}
        return _UNHANDLED

    runtime._dispatch_handlers = handlers
    browser = asyncio.create_task(runtime._dispatch({"op": "browser_server"}))
    await started.wait()
    assert await runtime._dispatch({"op": "shell_start"}) == {"started": True}
    reset = asyncio.create_task(runtime._dispatch({"op": "reset"}))
    assert await reset == {"reset": True}
    with pytest.raises(RuntimeError, match="not accepting|Expired"):
        await browser
    assert closed.is_set()


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid_state", ["healthy", "workspace"])
async def test_browser_start_rechecks_runtime_state_after_ensure(
    tmp_path, monkeypatch, invalid_state
):
    runtime = _runtime(tmp_path)
    started = asyncio.Event()
    release = asyncio.Event()

    class FakeBrowserService:
        def __init__(self, *args, generation, **kwargs):
            self.generation = generation

        async def ensure(self, browser, *, launch_options=None, track=None):
            started.set()
            await release.wait()
            return {"endpoint": "ws://127.0.0.1:1234/old", "generation": self.generation}

    monkeypatch.setattr(runtime_module, "BrowserService", FakeBrowserService)
    browser = asyncio.create_task(runtime._dispatch({"op": "browser_server"}))
    await started.wait()
    if invalid_state == "healthy":
        runtime.healthy = False
    else:
        runtime.workspace_available = lambda: False
    release.set()
    with pytest.raises(RuntimeError, match="not accepting"):
        await browser
