import pytest

from mypr_mcp.config_api import ConfigAPI
from mypr_mcp.kernel_api import Workspace


def test_workspace_exposes_config_api_and_help(tmp_path):
    ws = Workspace(tmp_path)
    assert ws.config is not None
    assert "ws.config.set" in ws.help("config")
    assert "scope: 'str' = 'workspace'" in ws.help("config.set")


@pytest.mark.asyncio
async def test_config_api_uses_flat_manager_rpc_fields():
    calls = []

    async def rpc(op, **fields):
        calls.append((op, fields))
        return fields

    config = ConfigAPI(rpc)
    assert await config.get("limits.response_bytes", "workspace") == {
        "method": "get",
        "path": "limits.response_bytes",
        "scope": "workspace",
    }
    assert await config.set("storage.enabled", False) == {
        "method": "set",
        "path": "storage.enabled",
        "value": False,
        "scope": "workspace",
    }
    assert await config.unset("storage.enabled", "global") == {
        "method": "unset",
        "path": "storage.enabled",
        "scope": "global",
    }
    assert await config.explain("mcp.servers.reports") == {
        "method": "explain",
        "path": "mcp.servers.reports",
    }
    assert await config.reload(force=True) == {"method": "reload", "force": True}
    assert calls == [
        ("config", {"method": "get", "path": "limits.response_bytes", "scope": "workspace"}),
        (
            "config",
            {"method": "set", "path": "storage.enabled", "value": False, "scope": "workspace"},
        ),
        ("config", {"method": "unset", "path": "storage.enabled", "scope": "global"}),
        ("config", {"method": "explain", "path": "mcp.servers.reports"}),
        ("config", {"method": "reload", "force": True}),
    ]


@pytest.mark.parametrize(
    ("method", "args"),
    [
        ("get", ("",)),
        ("set", ("limits.response_bytes", 1, "effective")),
        ("unset", ("limits.response_bytes", "effective")),
        ("explain", (None,)),
    ],
)
async def test_config_api_rejects_invalid_paths_and_scopes(method, args):
    async def rpc(*_args, **_kwargs):
        raise AssertionError("invalid calls must not reach the manager")

    with pytest.raises((TypeError, ValueError)):
        await getattr(ConfigAPI(rpc), method)(*args)


@pytest.mark.asyncio
async def test_workspace_forwards_named_lsp_config_fields(monkeypatch, tmp_path):
    calls = []

    async def rpc(op, **fields):
        calls.append((op, fields))
        return {"revision": "next"}

    import mypr_mcp.kernel_api as kernel_api

    monkeypatch.setattr(kernel_api, "_rpc", rpc)
    ws = Workspace(tmp_path)
    await ws._code_config(
        "set_lsp",
        name="pyright",
        definition={"command": ["pyright-langserver"]},
        expected_revision="old",
    )
    assert calls == [
        (
            "code_config",
            {
                "method": "set_lsp",
                "name": "pyright",
                "definition": {"command": ["pyright-langserver"]},
                "expected_revision": "old",
            },
        )
    ]
