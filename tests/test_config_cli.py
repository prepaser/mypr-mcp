from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

import mypr_mcp.cli as cli
import mypr_mcp.config as config


class FakeConfigStore:
    calls = []

    def __init__(self, workspace, *, global_path):
        self.workspace = Path(workspace) if workspace is not None else None
        self.global_path = Path(global_path)

    def load(self, *, scope):
        self.calls.append(("load", scope))
        return {"scope": scope}

    def get(self, path, *, scope):
        self.calls.append(("get", path, scope))
        return {"path": path, "scope": scope}

    def set(self, path, value, *, scope):
        self.calls.append(("set", path, value, scope))
        return SimpleNamespace(revision="revision")

    def unset(self, path, *, scope):
        self.calls.append(("unset", path, scope))
        return SimpleNamespace(revision="revision")

    def explain(self, path, snapshot=None):
        self.calls.append(("explain", path, snapshot))
        return {"path": path, "source": "disk"}


@pytest.fixture
def fake_config(monkeypatch):
    FakeConfigStore.calls = []
    monkeypatch.setattr(config, "ConfigStore", FakeConfigStore, raising=False)
    monkeypatch.setattr(cli, "ConfigStore", FakeConfigStore)
    return FakeConfigStore


@pytest.mark.asyncio
async def test_config_reads_effective_and_writes_workspace_without_starting_manager(
    tmp_path: Path, fake_config
):
    result = await cli._config_command(
        tmp_path,
        "get",
        "limits.response_bytes",
        None,
        global_scope=False,
        all_managers=False,
        force=False,
    )
    assert result["scope"] == "effective"
    assert fake_config.calls == [("get", "limits.response_bytes", "effective")]
    assert not (tmp_path / ".mypr").exists()

    result = await cli._config_command(
        tmp_path,
        "set",
        "limits.response_bytes",
        "65536",
        global_scope=False,
        all_managers=False,
        force=False,
    )
    assert result == {"saved": True, "revision": "revision"}
    assert fake_config.calls[-1] == ("set", "limits.response_bytes", 65536, "workspace")


@pytest.mark.asyncio
async def test_global_config_uses_raw_global_scope(tmp_path: Path, fake_config):
    result = await cli._config_command(
        tmp_path,
        "unset",
        "limits.response_bytes",
        None,
        global_scope=True,
        all_managers=False,
        force=False,
    )
    assert result == {"saved": True, "revision": "revision"}
    assert fake_config.calls[-1] == ("unset", "limits.response_bytes", "global")
    assert not (tmp_path / ".mypr").exists()


@pytest.mark.asyncio
async def test_global_config_ignores_malformed_workspace_config(tmp_path: Path, monkeypatch):
    global_path = tmp_path / "global.toml"
    global_path.write_text("[limits]\nresponse_bytes = 65536\n")
    workspace = tmp_path / "workspace"
    (workspace / ".mypr").mkdir(parents=True)
    (workspace / ".mypr" / "config.toml").write_text("not valid = [")
    monkeypatch.setenv("MYPR_GLOBAL_CONFIG", str(global_path))

    result = await cli._config_command(
        workspace,
        "get",
        "limits.response_bytes",
        None,
        global_scope=True,
        all_managers=False,
        force=False,
    )

    assert result == 65536

    async def manager_must_not_be_read(_workspace):
        pytest.fail("global explain must not inspect workspace manager configuration")

    monkeypatch.setattr(cli, "find_runtime", manager_must_not_be_read)
    result = await cli._config_command(
        workspace, "explain", "limits.response_bytes", None,
        global_scope=True, all_managers=False, force=False,
    )
    assert result["source"] == "global"


@pytest.mark.asyncio
async def test_config_explain_includes_active_manager_when_available(
    tmp_path: Path, fake_config, monkeypatch
):
    socket = tmp_path / "manager.sock"
    monkeypatch.setattr(
        cli, "find_runtime", lambda workspace: asyncio.sleep(0, result=(socket, {}))
    )
    calls = []

    async def rpc(socket_path, **request):
        calls.append((socket_path, request))
        return {"active": True}

    monkeypatch.setattr(cli, "rpc", rpc)
    result = await cli._config_command(
        tmp_path,
        "explain",
        "limits.response_bytes",
        None,
        global_scope=False,
        all_managers=False,
        force=False,
    )
    assert result["active"] == {"active": True}
    assert calls[0][1] == {
        "op": "config",
        "method": "explain",
        "path": "limits.response_bytes",
    }


@pytest.mark.asyncio
async def test_reload_all_reports_identity_and_capability_errors(tmp_path: Path, monkeypatch):
    record = {
        "workspace": str(tmp_path),
        "workspace_id": "identity",
        "pid": 123,
        "generation": "generation",
        "socket": str(tmp_path / "manager.sock"),
        "global_path": str(tmp_path / "global.toml"),
        "capabilities": ["config"],
    }
    monkeypatch.setattr("mypr_mcp.runtime_registry.list_managers", lambda path: [record])

    async def rpc(path, **request):
        return {
            "workspace_id": "other",
            "pid": 123,
            "generation": "generation",
            "healthy": True,
            "capabilities": ["config"],
        }

    monkeypatch.setattr(cli, "rpc", rpc)
    result = await cli._reload_all(Path(record["global_path"]), force=False)
    assert result["ok"] is False
    assert result["managers"][0]["status"] == "unsupported"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("reload_result", "status"),
    [
        ({"applied": {}, "deferred": {}, "errors": {"mcp": "failed"}}, "error"),
        ({"applied": {}, "deferred": {"lsp": "busy"}, "errors": {}}, "partial"),
    ],
)
async def test_reload_all_propagates_component_outcomes(
    tmp_path: Path, monkeypatch, reload_result, status
):
    record = {
        "workspace": str(tmp_path),
        "workspace_id": "identity",
        "pid": 123,
        "generation": "generation",
        "socket": str(tmp_path / "manager.sock"),
        "global_path": str(tmp_path / "global.toml"),
        "capabilities": ["config"],
    }
    monkeypatch.setattr("mypr_mcp.runtime_registry.list_managers", lambda path: [record])

    async def rpc(socket_path, **request):
        if request["op"] == "status":
            return {
                "workspace_id": "identity",
                "pid": 123,
                "generation": "generation",
                "global_path": record["global_path"],
                "healthy": True,
                "capabilities": ["config"],
            }
        return reload_result

    monkeypatch.setattr(cli, "rpc", rpc)
    result = await cli._reload_all(Path(record["global_path"]), force=False)

    report = result["managers"][0]
    assert result["ok"] is False
    assert report["ok"] is False
    assert report["status"] == status
    assert report["result"] == reload_result


def test_reload_all_cli_reports_errors_with_nonzero_exit(monkeypatch, capsys):
    async def command(*args, **kwargs):
        return {
            "ok": False,
            "managers": [{"workspace": "/workspace", "status": "error", "error": "offline"}],
        }

    monkeypatch.setattr(cli, "_config_command", command)
    monkeypatch.setattr(sys, "argv", ["mypr-mcp", "config", "reload", "--all"])

    with pytest.raises(SystemExit) as raised:
        cli.main()

    assert raised.value.code == 1
    assert "offline" in capsys.readouterr().out
