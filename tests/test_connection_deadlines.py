import asyncio
from types import SimpleNamespace

import pytest

import mypr_mcp.bridge as bridge_module
import mypr_mcp.cli as cli
import mypr_mcp.transport as transport
from mypr_mcp.diagnostics import RPCError


async def test_startup_deadline_cancels_unresponsive_status_and_cleans_owned_process(
    tmp_path, monkeypatch
):
    cancelled = stopped = False

    async def find_runtime(*args):
        return None

    async def rpc(*args, **kwargs):
        nonlocal cancelled
        try:
            await asyncio.Event().wait()
        finally:
            cancelled = True

    async def stop_process(*args):
        nonlocal stopped
        stopped = True

    monkeypatch.setattr(cli, "STARTUP_TIMEOUT", 0.02)
    monkeypatch.setattr(cli, "find_runtime", find_runtime)
    monkeypatch.setattr(cli, "rpc", rpc)
    monkeypatch.setattr(cli, "manager_running", lambda *args: False)
    monkeypatch.setattr(cli, "socket_path", lambda *args: tmp_path / "manager.sock")
    monkeypatch.setattr(cli, "_stop_spawned", stop_process)
    monkeypatch.setattr(
        cli.subprocess, "Popen", lambda *args, **kwargs: SimpleNamespace(poll=lambda: None)
    )
    with pytest.raises(RPCError) as failure:
        await asyncio.wait_for(cli.ensure(tmp_path), 1)
    assert failure.value.code == "manager_start_timeout"
    assert cancelled and stopped


async def test_attach_handshake_deadline_closes_unresponsive_connection(tmp_path, monkeypatch):
    class Writer:
        closed = False

        def write(self, data):
            pass

        async def drain(self):
            pass

        def close(self):
            self.closed = True

        async def wait_closed(self):
            pass

    async def readline():
        await asyncio.Event().wait()

    writer = Writer()

    async def connect(*args, **kwargs):
        return SimpleNamespace(readline=readline), writer

    monkeypatch.setattr(transport, "HANDSHAKE_TIMEOUT", 0.02)
    monkeypatch.setattr(transport.asyncio, "open_unix_connection", connect)
    with pytest.raises(TimeoutError):
        async with transport.attachment(tmp_path / "manager.sock", "connection"):
            pytest.fail("unresponsive handshake must not attach")
    assert writer.closed


async def test_rebinding_init_deadline_closes_attached_context(tmp_path, monkeypatch):
    closed = False

    class Context:
        async def __aenter__(self):
            return SimpleNamespace(status={"generation": "g"})

        async def __aexit__(self, *args):
            nonlocal closed
            closed = True

    async def rpc(*args, **kwargs):
        await asyncio.Event().wait()

    monkeypatch.setattr(bridge_module, "HANDSHAKE_TIMEOUT", 0.02)
    monkeypatch.setattr(bridge_module, "attachment", lambda *args, **kwargs: Context())
    monkeypatch.setattr(bridge_module, "check_compatibility", lambda status: status)
    monkeypatch.setattr(bridge_module, "rpc", rpc)
    bridge = bridge_module.ConnectionBridge(tmp_path)
    bridge.client_id = "client"
    with pytest.raises(TimeoutError):
        await bridge._attach(tmp_path / "manager.sock")
    assert closed
    assert bridge._context is None
    assert bridge.attachment is None
