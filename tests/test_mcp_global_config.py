from __future__ import annotations

import asyncio
import threading

import pytest

from mypr_mcp.config import ConfigStore
from mypr_mcp.services import MCPBridge


def _write(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


@pytest.mark.asyncio
async def test_inherited_mcp_mutations_write_workspace_layer_only(tmp_path):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    global_path = tmp_path / "global.toml"
    _write(global_path, '[mcp.servers.shared]\ncommand = "global-server"\n')
    bridge = MCPBridge(workspace, global_path=global_path)
    try:
        assert bridge.get_config("shared") == {"command": "global-server"}
        await bridge.configure("shared", {"command": "global-server"})
        snapshot = ConfigStore(workspace, global_path).load()
        assert snapshot.layers["workspace"]["mcp"]["servers"]["shared"] == {
            "command": "global-server"
        }

        await bridge.remove("shared")
        snapshot = ConfigStore(workspace, global_path).load()
        assert snapshot.layers["workspace"]["mcp"]["servers"]["shared"] == {
            "enabled": False
        }
        with pytest.raises(ValueError):
            bridge.get_config("shared")
    finally:
        await bridge.close()


@pytest.mark.asyncio
async def test_apply_snapshot_preserves_unchanged_connections_without_file_io(tmp_path):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    global_path = tmp_path / "global.toml"
    _write(
        global_path,
        '[mcp.servers.keep]\ncommand = "keep"\n'
        '[mcp.servers.changed]\ncommand = "old"\n',
    )
    bridge = MCPBridge(workspace, global_path=global_path)

    class Connection:
        busy = False

        def block_admissions(self):
            pass

        def unblock_admissions(self):
            pass

        async def close(self, force=False):
            del force

    keep = Connection()
    changed = Connection()
    bridge._connections.update(keep=keep, changed=changed)
    _write(
        global_path,
        '[mcp.servers.keep]\ncommand = "keep"\n'
        '[mcp.servers.changed]\ncommand = "new"\n'
        '[mcp.servers.added]\ncommand = "added"\n',
    )
    candidate = ConfigStore(workspace, global_path).load()
    try:
        bridge._config_applying = True
        result = await bridge.apply_snapshot(candidate)
        assert result == {"added": ["added"], "updated": ["changed"], "removed": []}
        assert bridge._connections["keep"] is keep
        assert "changed" not in bridge._connections
        assert bridge.config["added"] == {"command": "added"}
    finally:
        bridge._config_applying = False
        await bridge.close()


@pytest.mark.asyncio
async def test_config_applying_rejects_mutations_but_allows_snapshot_apply(tmp_path):
    bridge = MCPBridge(tmp_path, global_path=tmp_path / "global.toml")
    bridge._config_applying = True
    try:
        with pytest.raises(RuntimeError, match="reload is in progress"):
            await bridge.configure("server", {"command": "server"})
        with pytest.raises(RuntimeError, match="reload is in progress"):
            await bridge.remove("server")
        with pytest.raises(RuntimeError, match="reload is in progress"):
            await bridge.restart("server")
        with pytest.raises(RuntimeError, match="reload is in progress"):
            await bridge.reload()
        with pytest.raises(RuntimeError, match="reload is in progress"):
            await bridge.save_lsp({}, {})
        await bridge.apply_snapshot(bridge._load_snapshot())
    finally:
        await bridge.close()


@pytest.mark.asyncio
async def test_lsp_save_uses_section_compare_and_swap_after_mcp_change(tmp_path):
    bridge = MCPBridge(tmp_path, global_path=tmp_path / "global.toml")
    try:
        snapshot = await bridge.get_lsp()
        await bridge.configure("server", {"command": "server"})
        result = await bridge.save_lsp({}, snapshot["servers"])
        assert result["revision"] == (await bridge.get_lsp())["revision"]
    finally:
        await bridge.close()


@pytest.mark.asyncio
async def test_named_lsp_save_pins_or_tombstones_one_server(tmp_path):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    global_path = tmp_path / "global.toml"
    _write(
        global_path,
        "[lsp.servers.pyright]\n"
        'command = ["pyright-langserver", "--stdio"]\n'
        'languages = ["python"]\n'
        "timeout = 10.0\n",
    )
    bridge = MCPBridge(workspace, global_path=global_path)
    try:
        expected = (await bridge.get_lsp())["servers"]
        saved = await bridge.save_lsp(
            expected, expected, name="pyright", definition=expected["pyright"]
        )
        assert saved["servers"] == expected
        snapshot = ConfigStore(workspace, global_path).load()
        assert snapshot.layers["workspace"]["lsp"]["servers"]["pyright"] == expected["pyright"]

        expected = saved["servers"]
        removed = await bridge.save_lsp({}, expected, name="pyright", definition=None)
        assert "pyright" not in removed["servers"]
        snapshot = ConfigStore(workspace, global_path).load()
        assert snapshot.layers["workspace"]["lsp"]["servers"]["pyright"] == {
            "enabled": False
        }
    finally:
        await bridge.close()


@pytest.mark.asyncio
async def test_lsp_save_allows_external_lsp_and_unrelated_scalar_changes(tmp_path):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    global_path = tmp_path / "global.toml"
    bridge = MCPBridge(workspace, global_path=global_path)
    store = ConfigStore(workspace, global_path)
    definition = {
        "command": ["pyright-langserver", "--stdio"],
        "languages": ["python"],
        "timeout": 10.0,
    }
    try:
        initial = store.load()
        store.save_server("lsp", "pyright", definition, initial.revision)
        current = await bridge.get_lsp()
        saved = await bridge.save_lsp(
            current["servers"],
            current["servers"],
            name="pyright",
            definition=definition,
        )
        assert saved["servers"]["pyright"] == definition

        scalar = store.load()
        store.set("limits.response_bytes", 4096, expected_revision=scalar.revision)
        current = await bridge.get_lsp()
        saved = await bridge.save_lsp({}, current["servers"])
        assert saved["servers"] == {}
    finally:
        await bridge.close()


@pytest.mark.asyncio
async def test_configure_cancellation_commits_persisted_server(tmp_path, monkeypatch):
    bridge = MCPBridge(tmp_path, global_path=tmp_path / "global.toml")
    started = threading.Event()
    release = threading.Event()
    original = bridge._save_server

    def delayed(*args):
        started.set()
        if not release.wait(5):
            raise TimeoutError("test save was not released")
        return original(*args)

    monkeypatch.setattr(bridge, "_save_server", delayed)
    config = {"command": "server"}
    task = asyncio.create_task(bridge.configure("server", config))
    try:
        assert await asyncio.to_thread(started.wait, 5)
        task.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert bridge.get_config("server") == config
        snapshot = ConfigStore(tmp_path, tmp_path / "global.toml").load()
        assert snapshot.values["mcp"]["servers"]["server"] == config
    finally:
        release.set()
        await bridge.close()


@pytest.mark.asyncio
async def test_remove_cancellation_commits_persisted_tombstone(tmp_path, monkeypatch):
    global_path = tmp_path / "global.toml"
    bridge = MCPBridge(tmp_path, global_path=global_path)
    await bridge.configure("server", {"command": "server"})
    started = threading.Event()
    release = threading.Event()
    original = bridge._save_server

    def delayed(*args):
        started.set()
        if not release.wait(5):
            raise TimeoutError("test save was not released")
        return original(*args)

    monkeypatch.setattr(bridge, "_save_server", delayed)
    task = asyncio.create_task(bridge.remove("server"))
    try:
        assert await asyncio.to_thread(started.wait, 5)
        task.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        with pytest.raises(ValueError):
            bridge.get_config("server")
        snapshot = ConfigStore(tmp_path, global_path).load()
        assert "server" not in snapshot.values["mcp"]["servers"]
    finally:
        release.set()
        await bridge.close()


@pytest.mark.asyncio
async def test_configure_cancellation_finishes_connection_transition(tmp_path):
    bridge = MCPBridge(tmp_path, global_path=tmp_path / "global.toml")
    await bridge.configure("server", {"command": "old"})
    started = asyncio.Event()
    release = threading.Event()

    class Connection:
        busy = False
        connected = True

        def block_admissions(self):
            pass

        def unblock_admissions(self):
            pass

        async def close(self, force=False):
            del force
            started.set()
            await asyncio.to_thread(release.wait, 5)
            self.closed = True

    connection = Connection()
    bridge._connections["server"] = connection
    task = asyncio.create_task(bridge.configure("server", {"command": "new"}))
    try:
        await asyncio.wait_for(started.wait(), 5)
        task.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert bridge.get_config("server") == {"command": "new"}
        assert connection.closed is True
        assert "server" not in bridge._connections
    finally:
        release.set()
        await bridge.close()


@pytest.mark.asyncio
async def test_full_lsp_save_cancellation_refreshes_runtime_snapshot(tmp_path, monkeypatch):
    bridge = MCPBridge(tmp_path, global_path=tmp_path / "global.toml")
    started = threading.Event()
    release = threading.Event()
    original = bridge.store.save_lsp

    def delayed(*args):
        started.set()
        if not release.wait(5):
            raise TimeoutError("test save was not released")
        return original(*args)

    monkeypatch.setattr(bridge.store, "save_lsp", delayed)
    definition = {
        "command": ["pyright-langserver", "--stdio"],
        "languages": ["python"],
        "timeout": 10.0,
    }
    task = asyncio.create_task(bridge.save_lsp({"pyright": definition}, {}))
    try:
        assert await asyncio.to_thread(started.wait, 5)
        task.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        snapshot = ConfigStore(tmp_path, tmp_path / "global.toml").load()
        assert bridge._snapshot.values["lsp"]["servers"] == {"pyright": definition}
        assert bridge._revision == snapshot.revision
        assert snapshot.values["lsp"]["servers"] == {"pyright": definition}
    finally:
        release.set()
        await bridge.close()


@pytest.mark.asyncio
async def test_named_lsp_save_cancellation_refreshes_runtime_snapshot(tmp_path, monkeypatch):
    bridge = MCPBridge(tmp_path, global_path=tmp_path / "global.toml")
    started = threading.Event()
    release = threading.Event()
    original = bridge._store.save_server

    def delayed(*args):
        started.set()
        if not release.wait(5):
            raise TimeoutError("test save was not released")
        return original(*args)

    monkeypatch.setattr(bridge._store, "save_server", delayed)
    definition = {
        "command": ["pyright-langserver", "--stdio"],
        "languages": ["python"],
        "timeout": 10.0,
    }
    task = asyncio.create_task(
        bridge.save_lsp({"pyright": definition}, {}, name="pyright", definition=definition)
    )
    try:
        assert await asyncio.to_thread(started.wait, 5)
        task.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        snapshot = ConfigStore(tmp_path, tmp_path / "global.toml").load()
        assert bridge._snapshot.values["lsp"]["servers"] == {"pyright": definition}
        assert bridge._revision == snapshot.revision
        assert snapshot.values["lsp"]["servers"] == {"pyright": definition}
    finally:
        release.set()
        await bridge.close()
