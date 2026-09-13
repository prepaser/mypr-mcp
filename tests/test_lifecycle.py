import asyncio
import json
import socket

import pytest
import uvicorn
from conftest import execute, mcp_session, result_text
from mcp.server import MCPServer

from mypr_mcp.cli import ensure
from mypr_mcp.services import MCPBridge, Shells
from mypr_mcp.transport import rpc, socket_path


async def test_simultaneous_alias_start_has_one_kernel(workspace):
    alias = workspace.parent / "alias"
    alias.symlink_to(workspace, target_is_directory=True)
    paths = await asyncio.gather(ensure(workspace), ensure(alias))
    assert paths[0] == paths[1]
    states = await asyncio.gather(*(rpc(path, op="status") for path in paths))
    assert states[0]["generation"] == states[1]["generation"]


async def test_kernel_death_is_reported_and_explicit_reset_recovers(workspace):
    async with mcp_session(workspace) as session:
        failed = await execute(session, "import os; os._exit(7)")
        assert failed["state"] == "lost"
        path = socket_path(workspace)
        assert not (await rpc(path, op="status"))["healthy"]
        await rpc(path, op="reset", force=True)
        recovered = await execute(session, "6 * 7")
        assert recovered["state"] == "succeeded"
        assert result_text(recovered) == "42"
        assert recovered["generation"] != failed["generation"]


@pytest.mark.parametrize("force", [False, True])
async def test_stop_protects_detached_python_tasks(workspace, force):
    path = socket_path(workspace)
    async with mcp_session(workspace, client_id="background-owner") as session:
        started = await execute(
            session,
            "import asyncio\nbackground = ws.tasks.start(asyncio.Event().wait())\nbackground.id",
        )
        assert started["state"] == "succeeded"
        task_id = result_text(started).strip(" '\n")
        async with asyncio.timeout(5):
            while True:
                records = await rpc(path, op="history_list")
                if any(
                    item["id"] == task_id and item["state"] == "running"
                    for item in records["items"]
                ):
                    break
                await asyncio.sleep(0.01)

    if not force:
        with pytest.raises(RuntimeError, match="active work"):
            await rpc(path, op="stop")
        async with mcp_session(workspace, client_id="background-owner") as session:
            assert "running" in result_text(await execute(session, "background.status()"))
            cancelled = await execute(session, "await background.cancel()")
            assert cancelled["state"] == "succeeded"
            async with asyncio.timeout(5):
                while True:
                    if (await rpc(path, op="history_get", id=task_id))["state"] == "cancelled":
                        break
                    await asyncio.sleep(0.01)

    assert await rpc(path, op="stop", force=force) == {"stopped": True}
    async with asyncio.timeout(10):
        while True:
            if not path.exists():
                break
            await asyncio.sleep(0.01)


async def test_packages_install_into_workspace_venv(workspace):
    async with mcp_session(workspace) as session:
        started = await execute(session, 'package = await ws.packages.add("pyyaml==6.0.3")')
        assert started["state"] == "succeeded"
        installed = await execute(session, "await package\npackage.status()")
        assert "succeeded" in result_text(installed)
        assert "pyyaml==6.0.3" in (workspace / ".mypr/requirements.txt").read_text().lower()


async def test_shell_tracks_redirected_background_descendant(workspace):
    shells = Shells(workspace)
    try:
        job = await shells.start("sleep 30 >/dev/null 2>&1 &")
        await asyncio.sleep(0.15)
        assert (await shells.poll(job["id"]))["state"] == "running"
        result = await asyncio.wait_for(shells.cancel(job["id"]), 5)
        assert result["state"] == "cancelled"
        assert not shells.active
    finally:
        await shells.close()


async def test_http_mcp_call_and_cancel(workspace):
    app = MCPServer("test-http")
    started = asyncio.Event()
    ready = asyncio.Event()

    @app.tool()
    async def echo(text: str) -> str:
        return text

    @app.tool()
    async def slow() -> str:
        started.set()
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            raise
        return "done"

    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    sock.listen()
    port = sock.getsockname()[1]

    class Server(uvicorn.Server):
        async def startup(self, sockets=None):
            await super().startup(sockets=sockets)
            ready.set()

    server = Server(uvicorn.Config(app.streamable_http_app(), log_level="error"))
    runner = asyncio.create_task(server.serve(sockets=[sock]))
    root = workspace / ".mypr"
    root.mkdir(exist_ok=True)
    (root / "config.toml").write_text(
        "[mcp.servers.http]\nurl = " + json.dumps(f"http://127.0.0.1:{port}/mcp") + "\n"
    )
    bridge = MCPBridge(workspace)
    try:
        await asyncio.wait_for(ready.wait(), 5)
        result = await bridge.dispatch(
            "call_tool",
            {
                "server": "http",
                "name": "echo",
                "arguments": {"text": "from-http"},
            },
        )
        assert not result["isError"]
        assert "from-http" in json.dumps(result)
        operation = asyncio.create_task(
            bridge.dispatch(
                "call_tool",
                {
                    "server": "http",
                    "name": "slow",
                    "arguments": {},
                },
            )
        )
        await asyncio.wait_for(started.wait(), 5)
        operation.cancel()
        with pytest.raises(asyncio.CancelledError):
            await operation
        # The connection remains usable after cancelling its previous request.
        await asyncio.wait_for(bridge.dispatch("list_tools", {"server": "http"}), 5)
    finally:
        await bridge.close()
        server.should_exit = True
        await asyncio.wait_for(runner, 10)
        sock.close()
