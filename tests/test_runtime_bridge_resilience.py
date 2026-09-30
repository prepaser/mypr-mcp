from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

import mypr_mcp.bridge as bridge_module
import mypr_mcp.runtime as runtime_module
from mypr_mcp.bridge import ConnectionBridge
from mypr_mcp.runtime import Runtime


@pytest.mark.asyncio
async def test_terminal_finish_releases_shell_submission_waiter(tmp_path: Path, monkeypatch):
    runtime = Runtime(tmp_path)
    monkeypatch.setattr(runtime, "persist_execution", _noop)
    monkeypatch.setattr(runtime, "notify_execution_change", _noop)
    monkeypatch.setattr(runtime, "retain_completed", lambda *args: None)

    waiter = asyncio.get_running_loop().create_future()
    runtime.submit_waiters["message"] = waiter
    record = {
        "id": "execution",
        "msg_id": "message",
        "state": "running",
        "done": asyncio.Event(),
        "generation": runtime.generation,
    }

    await runtime.finish(record, "succeeded")

    assert waiter.done()
    assert not waiter.cancelled()


async def _noop(*args, **kwargs):
    return None


@pytest.mark.asyncio
async def test_failed_kernel_start_closes_partial_resources(tmp_path: Path, monkeypatch):
    runtime = Runtime(tmp_path)
    runtime.py = Path("/usr/bin/python")

    class FakeClient:
        def __init__(self):
            self.stopped = False

        def start_channels(self):
            pass

        async def wait_for_ready(self, **kwargs):
            raise TimeoutError("kernel did not become ready")

        def stop_channels(self):
            self.stopped = True

    class FakeKernelManager:
        instances = []

        def __init__(self, **kwargs):
            self.client_instance = FakeClient()
            self.shutdown = False
            self.provisioner = type("Provisioner", (), {"pid": 123})()
            self.__class__.instances.append(self)

        async def start_kernel(self, **kwargs):
            pass

        def client(self):
            return self.client_instance

        async def shutdown_kernel(self, now=False):
            self.shutdown = now

    monkeypatch.setattr(runtime_module, "AsyncKernelManager", FakeKernelManager)

    with pytest.raises(TimeoutError, match="did not become ready"):
        await runtime.start_kernel()

    manager = FakeKernelManager.instances[-1]
    assert manager.shutdown is True
    assert manager.client_instance.stopped is True
    assert runtime.km is None
    assert runtime.kc is None
    assert runtime.health_error.startswith("Python kernel startup failed:")


@pytest.mark.asyncio
async def test_bridge_attach_failure_closes_new_context(tmp_path: Path, monkeypatch):
    contexts = []

    class Attached:
        status = {"generation": "g", "version": "bad"}

    class Context:
        def __init__(self):
            self.entered = False
            self.exited = False
            contexts.append(self)

        async def __aenter__(self):
            self.entered = True
            return Attached()

        async def __aexit__(self, *args):
            self.exited = True

    monkeypatch.setattr(bridge_module, "attachment", lambda *args, **kwargs: Context())
    monkeypatch.setattr(bridge_module, "target_installation", lambda: {})
    monkeypatch.setattr(
        bridge_module,
        "check_compatibility",
        lambda status: (_ for _ in ()).throw(RuntimeError("incompatible")),
    )

    bridge = ConnectionBridge(tmp_path)
    with pytest.raises(RuntimeError, match="incompatible"):
        await bridge._attach(tmp_path / "manager.sock")

    assert contexts[0].entered
    assert contexts[0].exited
    assert bridge._context is None
    assert bridge.attachment is None
    assert bridge.path is None
    assert bridge._state is None
    assert not bridge._ready.is_set()


@pytest.mark.asyncio
async def test_bridge_init_failure_closes_new_context(tmp_path: Path, monkeypatch):
    closed = False

    class Context:
        async def __aenter__(self):
            return type("Attached", (), {"status": {"generation": "g"}})()

        async def __aexit__(self, *args):
            nonlocal closed
            closed = True

    async def fail_rpc(*args, **kwargs):
        raise RuntimeError("init failed")

    monkeypatch.setattr(bridge_module, "attachment", lambda *args, **kwargs: Context())
    monkeypatch.setattr(bridge_module, "target_installation", lambda: {})
    monkeypatch.setattr(bridge_module, "check_compatibility", lambda status: status)
    monkeypatch.setattr(bridge_module, "rpc", fail_rpc)

    bridge = ConnectionBridge(tmp_path)
    bridge.client_id = "client"
    with pytest.raises(RuntimeError, match="init failed"):
        await bridge._attach(tmp_path / "manager.sock")

    assert closed
    assert bridge._context is None
    assert bridge.attachment is None
    assert bridge.path is None
    assert bridge._state is None


@pytest.mark.asyncio
async def test_bridge_close_finishes_context_after_caller_cancellation(tmp_path: Path):
    entered, release = asyncio.Event(), asyncio.Event()

    class Context:
        exited = False

        async def __aexit__(self, *args):
            entered.set()
            await release.wait()
            self.exited = True

    context = Context()
    bridge = ConnectionBridge(tmp_path)
    bridge._context = context
    cleanup = asyncio.create_task(bridge.close())
    await asyncio.wait_for(entered.wait(), 1)
    cleanup.cancel()
    await asyncio.sleep(0)
    cleanup.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await cleanup
    assert context.exited
    assert bridge._context is None
    assert bridge._ready.is_set()
    await bridge.close()
