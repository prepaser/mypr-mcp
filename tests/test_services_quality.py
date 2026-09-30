from __future__ import annotations

import asyncio
import json
import sys

import pytest

import mypr_mcp.services as services
from mypr_mcp.services import MCPBridge, Shells, _MCPConnection


class _BrokenProcess:
    async def wait(self) -> int:
        raise OSError("process wait failed")


async def test_shell_wait_error_finalizes_job_and_wakes_waiters(tmp_path):
    service = Shells(tmp_path)
    try:
        job = service._new_job("a" * 32, _BrokenProcess(), 0)
        settled = asyncio.Event()

        async def reader():
            try:
                await asyncio.Event().wait()
            finally:
                settled.set()

        job.readers = [asyncio.create_task(reader())]
        service._jobs[job.id] = job
        waiting = asyncio.create_task(service.wait(job.id))
        job.waiter = asyncio.create_task(service._wait(job))

        result = await asyncio.wait_for(waiting, 2)
        await asyncio.wait_for(job.waiter, 2)
        assert result["state"] == "failed"
        assert result["result"] == {"returncode": -1}
        assert result["error"] == "process wait failed"
        assert result["finished_at"] is not None
        assert settled.is_set()

        metadata = json.loads((tmp_path / ".mypr/jobs" / f"{job.id}.json").read_text())
        assert metadata["state"] == "failed"
        assert metadata["result"] == {"returncode": -1}
        assert metadata["finished_at"] is not None
    finally:
        await service.close()


async def test_mcp_config_memory_commits_before_cancelled_cleanup(tmp_path, monkeypatch):
    bridge = MCPBridge(tmp_path)
    bridge._connections["server"] = _MCPConnection({"command": "unused"}, tmp_path)
    entered = asyncio.Event()
    release = asyncio.Event()

    async def close_connections(connections, force):
        del connections, force
        entered.set()
        await release.wait()

    monkeypatch.setattr(bridge, "_close_connections", close_connections)
    config = {"command": sys.executable, "args": ["server.py"]}
    operation = asyncio.create_task(bridge.configure("server", config))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        operation.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await operation
        assert bridge.get_config("server") == config
        assert bridge._revision == bridge.store.revision
        assert bridge.store.load()[0]["server"] == config
    finally:
        release.set()
        await bridge.close()


async def test_mcp_restart_cancellation_detaches_closed_connection(tmp_path, monkeypatch):
    bridge = MCPBridge(tmp_path)
    bridge.config = {"server": {"command": "unused"}}
    old = _MCPConnection(bridge.config["server"], tmp_path)
    bridge._connections["server"] = old
    entered, release = asyncio.Event(), asyncio.Event()

    async def close_connections(connections, force):
        del force
        for connection in connections.values():
            connection._closed = True
        entered.set()
        await release.wait()

    monkeypatch.setattr(bridge, "_close_connections", close_connections)
    operation = asyncio.create_task(bridge.restart("server"))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        operation.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await operation
        assert "server" not in bridge._connections
        assert not bridge._changing
    finally:
        release.set()
        await bridge.close()


async def test_mcp_restart_cancellation_closes_new_connection(tmp_path, monkeypatch):
    bridge = MCPBridge(tmp_path)
    bridge.config = {"server": {"command": "unused"}}
    old = _MCPConnection(bridge.config["server"], tmp_path)
    bridge._connections["server"] = old
    entered = asyncio.Event()
    replacements = []

    class Replacement:
        def __init__(self, config, workspace):
            del config, workspace
            self.closed = False
            replacements.append(self)

        def block_admissions(self):
            pass

        def unblock_admissions(self):
            pass

        async def ensure_ready(self):
            entered.set()
            await asyncio.Event().wait()

        async def close(self, force=True):
            del force
            self.closed = True

    async def close_old(connections, force):
        del force
        for connection in connections.values():
            connection._closed = True

    monkeypatch.setattr(services, "_MCPConnection", Replacement)
    monkeypatch.setattr(bridge, "_close_connections", close_old)
    operation = asyncio.create_task(bridge.restart("server"))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        operation.cancel()
        with pytest.raises(asyncio.CancelledError):
            await operation
        assert replacements[0].closed
        assert "server" not in bridge._connections
    finally:
        await bridge.close()


async def test_mcp_close_finishes_maps_after_cancellation(tmp_path):
    bridge = MCPBridge(tmp_path)
    entered, release = asyncio.Event(), asyncio.Event()

    class Connection:
        def block_admissions(self):
            pass

        async def close(self):
            entered.set()
            await release.wait()

    bridge._connections["server"] = Connection()
    operation = asyncio.create_task(bridge.close())
    await asyncio.wait_for(entered.wait(), 2)
    operation.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await operation
    assert bridge._closed
    assert not bridge._connections
    assert not bridge._changing


async def test_mcp_lsp_methods_provide_revision_compare_and_swap(tmp_path):
    bridge = MCPBridge(tmp_path)
    try:
        snapshot = await bridge.get_lsp()
        assert snapshot == {"servers": {}, "revision": bridge.store.revision}
        definitions = {
            "pyright": {
                "command": ["pyright-langserver", "--stdio"],
                "languages": ["python"],
                "timeout": 10.0,
            }
        }
        saved = await bridge.save_lsp(definitions, snapshot["servers"])
        assert saved["revision"] == bridge.store.revision
        assert (await bridge.get_lsp())["servers"] == definitions

        with pytest.raises(RuntimeError, match="LSP configuration changed"):
            await bridge.save_lsp({}, {})
    finally:
        await bridge.close()


@pytest.mark.parametrize("method", ["configure", "remove", "restart", "reload"])
async def test_mcp_mutations_require_boolean_force(tmp_path, method):
    bridge = MCPBridge(tmp_path)
    try:
        with pytest.raises(TypeError, match="force must be a boolean"):
            if method == "configure":
                await bridge.configure("server", {"command": "server"}, force="false")
            elif method == "remove":
                await bridge.remove("server", force="false")
            elif method == "restart":
                await bridge.restart("server", force="false")
            else:
                await bridge.reload(force="false")
    finally:
        await bridge.close()
