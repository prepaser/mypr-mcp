import asyncio
import os

import pytest

from mypr_mcp import cli
from mypr_mcp.config import ConfigStore
from mypr_mcp.diagnostics import RPCError
from mypr_mcp.file_io import PersistedFileError
from mypr_mcp.runtime import Runtime


@pytest.mark.parametrize("lock_kind", ["profile", "global", "workspace"])
@pytest.mark.parametrize("write", [False, True])
def test_config_rejects_fifo_locks(tmp_path, monkeypatch, lock_kind, write):
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))
    store = ConfigStore(tmp_path, tmp_path / "global.toml")
    path = {
        "profile": store._profile_lock_path(),
        "global": store.global_path.with_suffix(".lock"),
        "workspace": store.workspace_path.with_suffix(".lock"),
    }[lock_kind]
    path.parent.mkdir(parents=True, exist_ok=True)
    os.mkfifo(path)
    descriptor = os.open(path, os.O_RDWR | os.O_NONBLOCK)
    try:
        with pytest.raises(PersistedFileError, match="regular file"):
            if write:
                scope = "global" if lock_kind == "global" else "workspace"
                store.set("limits.response_bytes", 4096, scope=scope)
            else:
                store.load()
    finally:
        os.close(descriptor)


async def test_manager_rejects_fifo_lock_before_starting(tmp_path):
    runtime = Runtime(tmp_path)
    os.mkfifo(runtime.root / "manager.lock")
    descriptor = os.open(runtime.root / "manager.lock", os.O_RDWR | os.O_NONBLOCK)
    try:
        with pytest.raises(PersistedFileError, match="regular file"):
            await runtime.run()
    finally:
        os.close(descriptor)


async def test_startup_rejects_fifo_lock_before_launch(tmp_path, monkeypatch):
    root = tmp_path / ".mypr"
    root.mkdir()
    os.mkfifo(root / "startup.lock")
    descriptor = os.open(root / "startup.lock", os.O_RDWR | os.O_NONBLOCK)
    try:
        async def no_launch(*args, **kwargs):
            pytest.fail("startup must reject the lock before inspecting a manager")

        monkeypatch.setattr(cli, "find_runtime", no_launch)
        with pytest.raises(PersistedFileError, match="regular file"):
            await cli.ensure(tmp_path)
    finally:
        os.close(descriptor)


async def test_startup_reports_fifo_ignore_file(tmp_path):
    root = tmp_path / ".mypr"
    root.mkdir()
    os.mkfifo(root / ".gitignore")
    with pytest.raises(RPCError, match="regular file"):
        await asyncio.wait_for(cli.ensure(tmp_path), 20)
