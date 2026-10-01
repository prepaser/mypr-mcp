from __future__ import annotations

import pytest

from mypr_mcp.config import ConfigStore
from mypr_mcp.services import MCPBridge


def _write(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


@pytest.mark.asyncio
async def test_inherited_mcp_mutations_write_workspace_layer_only(tmp_path):
    workspace = tmp_path / "workspace"
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
