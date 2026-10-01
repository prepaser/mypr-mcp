from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path

import pytest

from mypr_mcp.runtime_registry import RegistryError, list_managers, register, unregister


def test_registry_records_and_filters_global_path(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    global_path = tmp_path / "config.toml"

    record = register(workspace, tmp_path / "manager.sock", "generation-a", global_path)

    assert record["pid"] == os.getpid()
    assert record["generation"] == "generation-a"
    assert list_managers(global_path) == [record]
    assert list_managers(tmp_path / "other.toml") == []


def test_old_generation_cannot_remove_new_registration(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    socket = tmp_path / "manager.sock"

    register(workspace, socket, "old", None)
    current = register(workspace, socket, "new", None)

    assert unregister(workspace, "old") is False
    assert list_managers(None) == [current]
    assert unregister(workspace, "new") is True
    assert list_managers(None) == []


def test_registry_keeps_unreachable_and_skips_unbounded_metadata(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    register(workspace, tmp_path / "missing.sock", "generation", None)
    directory = tmp_path / "state" / "mypr" / "managers"
    (directory / "invalid.json").write_text("{}")
    (directory / "large.json").write_text("{" + "x" * (64 * 1024) + "}")

    records = list_managers(None)

    assert len(records) == 1
    assert records[0]["socket"].endswith("missing.sock")
    assert json.loads((directory / "invalid.json").read_text()) == {}


def test_registry_read_error_is_not_reported_as_empty(monkeypatch):
    def blocked(*args, **kwargs):
        raise PermissionError("registry unavailable")

    monkeypatch.setattr("mypr_mcp.runtime_registry._lock", blocked)
    with pytest.raises(RegistryError, match="registry unavailable"):
        list_managers(None)


def test_registry_cap_counts_valid_records_after_invalid_entries(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    directory = tmp_path / "state" / "mypr" / "managers"
    directory.mkdir(parents=True)
    for index in range(1024):
        (directory / f"!{index:04d}.json").write_text("{}")
    record = register(workspace, tmp_path / "manager.sock", "generation", None)

    assert list_managers(None) == [record]


@pytest.mark.asyncio
async def test_runtime_survives_registry_registration_failure(tmp_path: Path, monkeypatch):
    import mypr_mcp.runtime as runtime_module
    import mypr_mcp.runtime_registry as registry

    monkeypatch.setenv("XDG_RUNTIME_DIR", str(tmp_path / "runtime"))
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "config"))
    workspace = tmp_path / "workspace"
    workspace.mkdir()

    def fail_register(*args, **kwargs):
        raise PermissionError("registry unavailable")

    monkeypatch.setattr(registry, "register", fail_register)
    runtime = runtime_module.Runtime(workspace)
    runtime.socket = Path("/tmp") / f"mypr-registry-test-{os.getpid()}.sock"

    async def noop(*args, **kwargs):
        return None

    runtime.prepare = noop
    runtime.start_kernel = noop
    runtime.shutdown_resources = noop
    runtime.drain_background = noop
    task = asyncio.create_task(runtime.run())
    for _ in range(100):
        if runtime.registry_error:
            break
        await asyncio.sleep(0.01)
    runtime.stopping.set()
    await asyncio.wait_for(task, 2)

    assert runtime.registry_error == (
        "manager registry unavailable: PermissionError: registry unavailable"
    )
    assert runtime.healthy is False
