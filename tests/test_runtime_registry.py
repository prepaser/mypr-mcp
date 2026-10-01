from __future__ import annotations

import json
import os
from pathlib import Path

from mypr_mcp.runtime_registry import list_managers, register, unregister


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
