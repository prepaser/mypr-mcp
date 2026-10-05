import asyncio
import os
from pathlib import Path

import pytest

from mypr_mcp.browser_service import _InstallLock
from mypr_mcp.file_io import PersistedFileError
from mypr_mcp.restart import read_ticket
from mypr_mcp.restart_records import poll_restart, restart_id_for_execution
from mypr_mcp.runtime_registry import RegistryError, list_managers
from mypr_mcp.transport import find_runtime, manager_running


def _fifo(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    os.mkfifo(path)
    return os.open(path, os.O_RDWR | os.O_NONBLOCK)


def test_transport_control_files_reject_fifo_without_blocking(tmp_path, monkeypatch):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    descriptor = _fifo(workspace / ".mypr" / "manager.lock")
    try:
        assert manager_running(workspace) is False
    finally:
        os.close(descriptor)

    descriptor = _fifo(workspace / ".mypr" / "runtime.json")
    try:
        monkeypatch.setenv("XDG_RUNTIME_DIR", str(tmp_path / "runtime"))
        assert asyncio.run(find_runtime(workspace)) is None
    finally:
        os.close(descriptor)


def test_restart_metadata_reads_reject_fifo_and_malformed_json(tmp_path):
    root = tmp_path / ".mypr" / "runs"
    root.mkdir(parents=True)
    ident = "a" * 32
    descriptor = _fifo(root / f"{ident}.json")
    try:
        assert restart_id_for_execution(tmp_path, ident) is None
        assert poll_restart(tmp_path, ident) is None
    finally:
        os.close(descriptor)

    path = root / f"{ident}.json"
    path.unlink()
    path.write_text("[]")
    assert restart_id_for_execution(tmp_path, ident) is None
    assert poll_restart(tmp_path, ident) is None

    restart_path = tmp_path / ".mypr" / "restart.json"
    restart_path.write_text("[")
    assert read_ticket(tmp_path) is None


def test_registry_lock_rejects_fifo_without_blocking(tmp_path, monkeypatch):
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))
    path = tmp_path / "state" / "mypr" / "managers" / ".lock"
    descriptor = _fifo(path)
    try:
        with pytest.raises(RegistryError, match="unable to access manager registry"):
            list_managers(None)
    finally:
        os.close(descriptor)


@pytest.mark.asyncio
async def test_browser_install_lock_rejects_fifo_without_blocking(tmp_path):
    path = tmp_path / "install.lock"
    descriptor = _fifo(path)
    try:
        with pytest.raises(PersistedFileError, match="regular file"):
            await _InstallLock(path, 0).__aenter__()
    finally:
        os.close(descriptor)
