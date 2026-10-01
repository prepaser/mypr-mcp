from __future__ import annotations

import asyncio
import os
from pathlib import Path

import pytest

import mypr_mcp.services as services
from mypr_mcp.runtime import _UNHANDLED, Runtime
from mypr_mcp.services import Shells


@pytest.mark.asyncio
async def test_shell_close_waits_for_launch_and_cancels_process(tmp_path: Path, monkeypatch):
    service = Shells(tmp_path)
    original = asyncio.create_subprocess_exec
    launched = asyncio.Event()
    release = asyncio.Event()
    process = None

    async def delayed_launch(*args, **kwargs):
        nonlocal process
        process = await original(*args, **kwargs)
        launched.set()
        await release.wait()
        return process

    monkeypatch.setattr(asyncio, "create_subprocess_exec", delayed_launch)
    start = asyncio.create_task(service.start(["sleep", "60"]))
    await launched.wait()
    closing = asyncio.create_task(service.close())
    closing_again = asyncio.create_task(service.close())
    await asyncio.sleep(0)
    assert not closing.done()
    assert not closing_again.done()
    release.set()
    job = await start
    await asyncio.gather(closing, closing_again)
    assert process is not None and process.returncode is not None
    assert not service.active
    assert job["id"] not in service.active


@pytest.mark.asyncio
async def test_shell_start_cancellation_reaps_process_created_during_launch(
    tmp_path: Path, monkeypatch
):
    service = Shells(tmp_path)
    original = asyncio.create_subprocess_exec
    launched = asyncio.Event()
    release = asyncio.Event()
    process = None

    async def delayed_launch(*args, **kwargs):
        nonlocal process
        process = await original(*args, **kwargs)
        launched.set()
        await release.wait()
        return process

    monkeypatch.setattr(asyncio, "create_subprocess_exec", delayed_launch)
    start = asyncio.create_task(service.start(["sleep", "60"]))
    await launched.wait()
    start.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await start
    assert process is not None and process.returncode is not None
    assert not service.active
    await service.close()


@pytest.mark.asyncio
async def test_shell_start_cancellation_is_preserved_when_launch_fails(tmp_path, monkeypatch):
    service = Shells(tmp_path)
    launched = asyncio.Event()
    release = asyncio.Event()
    created_fds = []
    original_openpty = services.os.openpty

    def openpty():
        fds = original_openpty()
        created_fds.extend(fds)
        return fds

    async def failing_launch(*args, **kwargs):
        launched.set()
        await release.wait()
        raise OSError("launch failed")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", failing_launch)
    monkeypatch.setattr(services.os, "openpty", openpty)
    start = asyncio.create_task(service.start(["true"], pty=True))
    await launched.wait()
    start.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await start
    assert not service.active
    assert service._starting == 0
    for fd in created_fds:
        with pytest.raises(OSError):
            os.fstat(fd)
    await service.close()


def _dispatch_runtime():
    runtime = Runtime.__new__(Runtime)
    runtime.clients = {}
    runtime.restarting = None
    runtime.resetting = False
    runtime.stopping = asyncio.Event()
    runtime.generation = "generation"
    runtime._admission_lock = asyncio.Lock()

    async def no_history(op, req):
        return _UNHANDLED

    runtime._dispatch_history = no_history
    return runtime


@pytest.mark.asyncio
async def test_dispatch_launch_waiting_for_reset_is_rejected():
    runtime = _dispatch_runtime()
    reset_entered = asyncio.Event()
    release_reset = asyncio.Event()
    launches = []

    async def handlers(op, req, context):
        if op == "reset":
            async with runtime._admission_lock:
                reset_entered.set()
                await release_reset.wait()
                runtime.resetting = True
            return {"reset": True}
        if op == "shell_start":
            launches.append(op)
            return {"started": True}
        return _UNHANDLED

    runtime._dispatch_handlers = handlers
    reset = asyncio.create_task(runtime._dispatch({"op": "reset"}))
    await reset_entered.wait()
    launch = asyncio.create_task(
        runtime._dispatch({"op": "shell_start", "generation": "generation"})
    )
    await asyncio.sleep(0)
    assert not launch.done()
    release_reset.set()
    assert await reset == {"reset": True}
    with pytest.raises(RuntimeError, match="restart/reset"):
        await launch
    assert launches == []


@pytest.mark.asyncio
async def test_dispatch_reset_waits_for_admitted_launch():
    runtime = _dispatch_runtime()
    launch_entered = asyncio.Event()
    release_launch = asyncio.Event()
    reset_entered = asyncio.Event()

    async def handlers(op, req, context):
        if op == "shell_start":
            launch_entered.set()
            await release_launch.wait()
            return {"started": True}
        if op == "reset":
            async with runtime._admission_lock:
                runtime.resetting = True
                reset_entered.set()
            return {"reset": True}
        return _UNHANDLED

    runtime._dispatch_handlers = handlers
    launch = asyncio.create_task(
        runtime._dispatch({"op": "shell_start", "generation": "generation"})
    )
    await launch_entered.wait()
    reset = asyncio.create_task(runtime._dispatch({"op": "reset"}))
    await asyncio.sleep(0)
    assert not reset.done()
    assert not runtime.resetting
    release_launch.set()
    assert await launch == {"started": True}
    await reset_entered.wait()
    assert await reset == {"reset": True}


@pytest.mark.asyncio
async def test_dispatch_rechecks_generation_after_admission_wait():
    runtime = _dispatch_runtime()
    calls = []

    async def handlers(op, req, context):
        calls.append(op)
        return {"started": True}

    runtime._dispatch_handlers = handlers
    await runtime._admission_lock.acquire()
    launch = asyncio.create_task(
        runtime._dispatch({"op": "shell_start", "generation": "generation"})
    )
    await asyncio.sleep(0)
    runtime.generation = "new-generation"
    runtime._admission_lock.release()
    with pytest.raises(RuntimeError, match="Expired kernel generation"):
        await launch
    assert calls == []


def test_reset_and_stop_admission_reject_all_lifecycle_starts():
    runtime = Runtime.__new__(Runtime)
    runtime.resetting = True
    runtime.restarting = None
    runtime.stopping = asyncio.Event()
    for operation in ("shell_start", "packages_add", "scan_start", "browser_server"):
        with pytest.raises(RuntimeError, match="restart/reset"):
            runtime._check_dispatch_admission(operation)

    runtime.resetting = False
    runtime.stopping.set()
    for operation in ("shell_start", "packages_add", "scan_start", "browser_server"):
        with pytest.raises(RuntimeError, match="stopping"):
            runtime._check_dispatch_admission(operation)
