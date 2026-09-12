from __future__ import annotations

import asyncio
import contextlib
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from mypr_mcp.cli import ensure
from mypr_mcp.transport import manager_running, rpc, socket_path, workspace_id


async def _wait_for_socket_to_disappear(path: Path) -> None:
    for _ in range(200):
        if not await asyncio.to_thread(path.exists):
            return
        await asyncio.sleep(0.05)


async def _stop(path: Path) -> None:
    with contextlib.suppress(Exception):
        await asyncio.wait_for(rpc(path, op="stop", force=True), 10)
    await _wait_for_socket_to_disappear(path)


async def _wait_for_manager_to_stop(workspace: Path) -> None:
    for _ in range(200):
        if not await asyncio.to_thread(manager_running, workspace):
            return
        await asyncio.sleep(0.05)


def test_workspace_identity_handles_links_and_copies(workspace: Path) -> None:
    link = workspace.parent / "workspace-link"
    copy = workspace.parent / "workspace-copy"
    link.symlink_to(workspace, target_is_directory=True)
    shutil.copytree(workspace, copy)

    assert workspace_id(workspace) == workspace_id(link)
    assert socket_path(workspace) == socket_path(link)
    assert workspace_id(workspace) != workspace_id(copy)
    assert socket_path(workspace) != socket_path(copy)


async def test_stopped_workspace_can_be_renamed_and_restarted(workspace: Path) -> None:
    original_socket = await ensure(workspace)
    original_id = workspace_id(workspace)
    await _stop(original_socket)
    await _wait_for_manager_to_stop(workspace)

    renamed = workspace.parent / "renamed-workspace"
    await asyncio.to_thread(workspace.rename, renamed)
    try:
        assert workspace_id(renamed) == original_id
        renamed_socket = await ensure(renamed)
        assert renamed_socket == original_socket
        state = await rpc(renamed_socket, op="status")
        assert state["workspace_id"] == original_id
    finally:
        await _stop(socket_path(renamed))
        await asyncio.to_thread(renamed.rename, workspace)


async def test_live_workspace_rename_reports_path_mismatch(workspace: Path) -> None:
    path = await ensure(workspace)
    renamed = workspace.parent / "live-renamed-workspace"
    await asyncio.to_thread(workspace.rename, renamed)
    try:
        with pytest.raises(RuntimeError, match="workspace"):
            await ensure(renamed)
        state = await rpc(path, op="status")
        assert state["workspace_available"] is False
        with pytest.raises(RuntimeError, match="workspace moved"):
            await rpc(path, op="execute", code="raise AssertionError('must not run')")
    finally:
        await _stop(path)
        await asyncio.to_thread(renamed.rename, workspace)


_BIND_MOUNT_SCRIPT = r"""
import asyncio
import contextlib
import json
import subprocess
import sys
from pathlib import Path

from mypr_mcp.cli import ensure
from mypr_mcp.transport import rpc, socket_path, workspace_id


async def wait_for_socket_to_disappear(path):
    for _ in range(200):
        if not path.exists():
            return
        await asyncio.sleep(0.05)


async def main():
    source = Path(sys.argv[1])
    alias = Path(sys.argv[2])
    path = socket_path(source)
    mounted = False
    try:
        subprocess.run(["mount", "--bind", str(source), str(alias)], check=True)
        mounted = True
        assert source.resolve() != alias.resolve()
        assert workspace_id(source) == workspace_id(alias)
        assert socket_path(source) == socket_path(alias)

        paths = await asyncio.gather(ensure(source), ensure(alias))
        assert paths[0] == paths[1]
        states = await asyncio.gather(
            rpc(paths[0], op="status"), rpc(paths[1], op="status")
        )
        assert states[0]["generation"] == states[1]["generation"]
        assert states[0]["workspace_id"] == workspace_id(source)

        first = await rpc(
            paths[0],
            op="execute",
            code="bind_mount_shared = 'source'\nbind_mount_shared",
            wait_ms=10_000,
            client_id="bind-source",
            connection_id="bind-source-connection",
        )
        assert first["state"] == "succeeded", first
        second = await rpc(
            paths[1],
            op="execute",
            code="bind_mount_shared",
            wait_ms=10_000,
            client_id="bind-alias",
            connection_id="bind-alias-connection",
        )
        assert second["state"] == "succeeded", second
        assert any(
            event.get("text", "").strip().strip("\'") == "source"
            for event in second["output"]
        )

        runtime = json.loads((source / ".mypr" / "runtime.json").read_text())
        assert runtime["workspace_id"] == workspace_id(source)
    finally:
        with contextlib.suppress(Exception):
            await rpc(path, op="stop", force=True)
        await wait_for_socket_to_disappear(path)
        if mounted:
            subprocess.run(["umount", str(alias)], check=True)


asyncio.run(main())
"""


def _check_private_mount_namespace() -> None:
    if shutil.which("unshare") is None:
        pytest.skip("private user/mount namespace unavailable: unshare is not installed")
    if shutil.which("mount") is None or shutil.which("umount") is None:
        pytest.skip("private user/mount namespace unavailable: mount utilities are not installed")
    probe = subprocess.run(
        ["unshare", "--user", "--map-root-user", "--mount", "--", "true"],
        capture_output=True,
        text=True,
        timeout=10,
    )
    if probe.returncode:
        reason = probe.stderr.strip() or f"exit status {probe.returncode}"
        pytest.skip(f"private user/mount namespace unavailable: {reason}")


async def test_bind_mount_alias_uses_one_runtime(workspace: Path) -> None:
    _check_private_mount_namespace()
    alias = workspace.parent / "bind-mounted-workspace"
    alias.mkdir()
    env = dict(os.environ)
    source_root = str(Path(__file__).parents[1] / "src")
    env["PYTHONPATH"] = os.pathsep.join(
        item for item in (source_root, env.get("PYTHONPATH")) if item
    )
    result = await asyncio.to_thread(
        subprocess.run,
        [
            "unshare",
            "--user",
            "--map-root-user",
            "--mount",
            "--",
            sys.executable,
            "-c",
            _BIND_MOUNT_SCRIPT,
            str(workspace),
            str(alias),
        ],
        capture_output=True,
        text=True,
        env=env,
        timeout=180,
    )
    assert result.returncode == 0, f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
