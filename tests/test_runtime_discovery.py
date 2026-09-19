import asyncio
import fcntl
import hashlib
import json
import os
from contextlib import asynccontextmanager

import pytest

from mypr_mcp.cli import ensure
from mypr_mcp.transport import find_runtime, manager_running, socket_path, workspace_id


@asynccontextmanager
async def legacy_manager(workspace, version="0.3.0"):
    root = workspace / ".mypr"
    root.mkdir(exist_ok=True)
    key = hashlib.sha256(os.fsencode(workspace.resolve())).hexdigest()[:32]
    path = socket_path(workspace).with_name(f"{key}.sock")
    state = {"version": version, "generation": "legacy-generation"}
    metadata = dict(state, workspace=str(workspace), socket=str(path))
    (root / "runtime.json").write_text(json.dumps(metadata))
    lock = (root / "manager.lock").open("a")
    fcntl.flock(lock, fcntl.LOCK_EX)

    async def respond(reader, writer):
        try:
            await reader.readline()
            writer.write(json.dumps({"ok": True, "result": state}).encode() + b"\n")
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_unix_server(respond, path=str(path))
    try:
        async with server:
            yield path, state
    finally:
        server.close()
        await server.wait_closed()
        path.unlink(missing_ok=True)
        lock.close()


async def test_legacy_socket_is_discovered_but_not_silently_upgraded(workspace):
    async with legacy_manager(workspace) as expected:
        assert await find_runtime(workspace) == expected
        with pytest.raises(RuntimeError, match="Incompatible workspace protocol"):
            await ensure(workspace)


async def test_copied_metadata_without_live_lock_is_not_trusted(workspace):
    async with legacy_manager(workspace):
        # A reused path in old metadata is insufficient without the same live lock.
        lock_path = workspace / ".mypr/manager.lock"
        lock_path.rename(lock_path.with_name("held.lock"))
        lock_path.touch()
        assert not manager_running(workspace)
        assert await find_runtime(workspace) is None


async def test_unreachable_manager_does_not_launch_another(workspace, monkeypatch):
    root = workspace / ".mypr"
    root.mkdir()
    with (root / "manager.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)

        def unexpected_start(*args, **kwargs):
            raise AssertionError("Must not start another manager")

        monkeypatch.setattr("mypr_mcp.cli.subprocess.Popen", unexpected_start)
        with pytest.raises(RuntimeError, match="socket is unreachable"):
            await ensure(workspace)
        assert not (root / "venv").exists()


async def test_other_runtime_directory_finds_existing_manager(workspace, monkeypatch, tmp_path):
    original = await ensure(workspace)
    with monkeypatch.context() as patch:
        patch.setenv("XDG_RUNTIME_DIR", str(tmp_path / "other-runtime"))
        assert socket_path(workspace) != original
        found = await find_runtime(workspace)
        assert found is not None
        assert found[0] == original
        assert found[1]["workspace_id"] == workspace_id(workspace)
        assert await ensure(workspace) == original


async def test_known_legacy_manager_is_reused_without_restart(workspace):
    async with legacy_manager(workspace, version="0.9.0") as (path, state):
        assert await ensure(workspace) == path
        assert (await find_runtime(workspace))[1]["generation"] == state["generation"]
