from __future__ import annotations

import asyncio
import copy
import json
from types import SimpleNamespace

import pytest

import mypr_mcp.config_runtime as config_runtime
from mypr_mcp.runtime import Runtime
from mypr_mcp.services import MCPBridge


@pytest.fixture
def configured_runtime(tmp_path, monkeypatch):
    monkeypatch.setenv("MYPR_GLOBAL_CONFIG", str(tmp_path / "global.toml"))
    runtime = Runtime(tmp_path)
    runtime.mcp = MCPBridge(tmp_path, global_path=runtime.config_store.global_path)
    return runtime


@pytest.mark.parametrize(
    "field,attribute,initial,value",
    [
        ("response_bytes", "response_limit", 32768, 65536),
        ("execute_wait_ms", "execute_wait_ms", 1000, 0),
        ("poll_wait_ms", "poll_wait_ms", 1000, 5000),
    ],
)
async def test_config_write_is_pending_until_explicit_reload(
    configured_runtime, field, attribute, initial, value
):
    runtime = configured_runtime
    saved = await runtime._dispatch({
        "op": "config", "method": "set", "path": f"limits.{field}", "value": value,
    })
    assert saved["saved"] is True
    assert getattr(runtime, attribute) == initial
    explanation = await runtime.settings.dispatch({
        "method": "explain", "path": f"limits.{field}",
    })
    assert explanation["desired"] == value
    assert explanation["applied"] == initial
    assert explanation["pending"] is True

    result = await runtime.settings.reload()
    assert getattr(runtime, attribute) == value
    assert f"limits.{field}" in result["applied"]["manager"]
    assert result["restart_required"] == []
    assert (await runtime.settings.dispatch({
        "method": "explain", "path": f"limits.{field}",
    }))["pending"] is False
    assert result["errors"] == {}
    assert result["deferred"]["lsp"] == "Kernel is unavailable"
    assert not runtime.settings.applying
    assert not runtime.mcp._config_applying


async def test_config_startup_limits_do_not_change_the_running_kernel(configured_runtime):
    runtime = configured_runtime
    previous = runtime.output_limit
    runtime.config_store.set("limits.output_bytes", previous * 2)
    result = await runtime.settings.reload()
    assert result["restart_required"] == ["limits.output_bytes"]
    assert runtime.output_limit == previous
    assert not runtime.resetting
    for path in ("limits", '"limits"."output_bytes"'):
        result = await runtime.settings.dispatch({"method": "explain", "path": path})
        assert result["restart_required"]


async def test_global_rpc_ignores_invalid_workspace_config(configured_runtime):
    runtime = configured_runtime
    runtime.config_store.workspace_path.write_text("broken = [")
    await runtime.settings.dispatch({
        "method": "set", "path": "limits.response_bytes", "value": 65536,
        "scope": "global",
    })
    assert await runtime.settings.dispatch({
        "method": "get", "path": "limits.response_bytes", "scope": "global",
    }) == 65536


async def test_explain_filters_unmanaged_toml_values(tmp_path):
    root = tmp_path / ".mypr"
    root.mkdir()
    (root / "config.toml").write_text(
        "[limits]\nfuture = 2026-10-01\n[storage]\nfuture = 2026-10-01\n"
        "[mcp]\nfuture = 2026-10-01\n[lsp]\nfuture = 2026-10-01\n"
    )
    runtime = Runtime(tmp_path)
    runtime.mcp = MCPBridge(tmp_path, global_path=runtime.config_store.global_path)
    await runtime.settings.reload()
    for section in ("limits", "storage", "mcp", "lsp"):
        explanation = await runtime.settings.dispatch({"method": "explain", "path": section})
        json.dumps(explanation)
        assert "future" not in explanation["desired"]
        assert "future" not in explanation["applied"]


async def test_reload_releases_manager_locks_before_kernel_control(configured_runtime):
    runtime = configured_runtime
    runtime.healthy = True
    runtime.config_store.set("lsp.servers.demo", {
        "command": ["demo-lsp"], "languages": ["python"],
    })
    entered = asyncio.Event()
    release = asyncio.Event()

    async def apply(snapshot, generation, force):
        assert not runtime._admission_lock.locked()
        assert not runtime.mcp._mutation_lock.locked()
        entered.set()
        await release.wait()
        return {"applied": True, "changed": ["demo"], "deferred": []}

    runtime.settings._apply_lsp = apply
    reloading = asyncio.create_task(runtime.settings.reload())
    await entered.wait()
    with pytest.raises(RuntimeError, match="reload is in progress"):
        await runtime.code_config({"method": "set_lsp"})
    with pytest.raises(RuntimeError, match="reload is in progress"):
        await runtime.mcp.configure("other", {"command": "other"})
    with pytest.raises(RuntimeError, match="reload is in progress"):
        await runtime._reserve_restart_locked("restart", None, True)
    release.set()
    result = await reloading
    assert result["errors"] == {}
    assert runtime.settings.applied["lsp"]["servers"]["demo"]["command"] == ["demo-lsp"]


async def test_reload_keeps_deferred_lsp_definitions_applied(configured_runtime):
    runtime = configured_runtime
    runtime.healthy = True
    previous = copy.deepcopy(runtime.settings.applied["lsp"])
    runtime.config_store.set("lsp.servers.demo", {
        "command": ["demo-lsp"], "languages": ["python"],
    })

    async def apply(*_args):
        return {"applied": False, "changed": ["demo"], "deferred": ["demo"]}

    runtime.settings._apply_lsp = apply
    result = await runtime.settings.reload()
    assert result["deferred"]["lsp"] == ["demo"]
    assert runtime.settings.applied["lsp"] == previous


async def test_storage_reload_reschedules_without_running_gc(configured_runtime):
    runtime = configured_runtime
    runtime.healthy = True
    runtime.storage_policy["enabled"] = False
    calls = []

    async def storage_call(*args, **kwargs):
        calls.append((args, kwargs))
        return {"deleted_bytes": 0}

    runtime.storage_call = storage_call
    worker = asyncio.create_task(runtime.maintain_storage())
    await asyncio.sleep(0)
    runtime.storage_policy["enabled"] = True
    runtime._storage_wake.set()
    await asyncio.sleep(0.01)
    assert calls == []
    runtime.stopping.set()
    await asyncio.wait_for(worker, 1)


async def test_kernel_control_uses_a_snapshot_without_manager_rpc(configured_runtime):
    runtime = configured_runtime
    observed = []

    def message(kind, content, metadata):
        observed.append(metadata)
        return {"header": {"msg_id": "config-message"}, "metadata": metadata}

    def send(_message):
        runtime.control_waiters["config-message"].set_result({
            "status": "ok", "config_result": {"applied": True, "deferred": []},
        })

    runtime.kc = SimpleNamespace(
        session=SimpleNamespace(msg=message), shell_channel=SimpleNamespace(send=send),
    )
    snapshot = runtime.config_store.load()
    result = await runtime.settings._apply_lsp(snapshot, runtime.generation, False)
    assert result["applied"] is True
    assert observed[0]["config"]["lsp"]["revision"] == snapshot.revision
    assert runtime.control_waiters == {}


async def test_late_lsp_control_reply_reconciles_after_waiter_timeout(
    configured_runtime, monkeypatch
):
    runtime = configured_runtime
    runtime.healthy = True
    runtime.config_store.set("lsp.servers.demo", {
        "command": ["demo-lsp"], "languages": ["python"],
    })
    original_timeout = asyncio.timeout

    def short_timeout(_seconds):
        return original_timeout(0.001)

    monkeypatch.setattr(config_runtime.asyncio, "timeout", short_timeout)
    message = {"header": {"msg_id": "late-config"}}
    runtime.kc = SimpleNamespace(
        session=SimpleNamespace(msg=lambda *args, **kwargs: message),
        shell_channel=SimpleNamespace(send=lambda _message: None),
    )
    with pytest.raises(TimeoutError):
        await runtime.settings._apply_lsp(runtime.config_store.load(), runtime.generation, False)
    runtime.config_store.set("lsp.servers.demo", {
        "command": ["new-lsp"], "languages": ["python"],
    })
    result = runtime.settings.reconcile_late_lsp(
        {
            "definitions": {
                "demo": {"command": ["demo-lsp"], "languages": ["python"]},
            },
            "sequence": 1,
            "generation": runtime.generation,
        }
    )
    assert result is True
    assert runtime.settings.applied["lsp"]["servers"]["demo"]["command"] == ["demo-lsp"]


async def test_late_lsp_reply_uses_kernel_snapshot_without_pending_metadata(configured_runtime):
    runtime = configured_runtime
    waiter = asyncio.get_running_loop().create_future()
    runtime.control_waiters["late-config"] = waiter
    reply = {
        "parent_header": {"msg_id": "late-config"},
        "content": {
            "_mypr_applied_lsp": {
                "definitions": {
                    "demo": {"command": ["demo-lsp"], "languages": ["python"]},
                },
                "revision": "old",
                "sequence": 4,
                "generation": runtime.generation,
            },
        },
    }

    async def receive():
        await asyncio.sleep(0.01)
        return reply

    runtime.kc = SimpleNamespace(get_shell_msg=receive)
    reader = asyncio.create_task(runtime.read_replies())
    await asyncio.sleep(0.02)
    reader.cancel()
    with pytest.raises(asyncio.CancelledError):
        await reader
    assert waiter.result()["_mypr_applied_lsp"]["sequence"] == 4
    assert runtime.settings.applied["lsp"]["servers"]["demo"]["command"] == ["demo-lsp"]
