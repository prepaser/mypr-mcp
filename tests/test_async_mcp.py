import ast
import sys

from conftest import decode_result, execute, mcp_session, poll_until_done, result_text


async def test_concurrent_cells_call_one_real_mcp_connection(workspace):
    server = workspace / "gate_server.py"
    server.write_text(
        "import asyncio\n"
        "from mcp.server import MCPServer\n"
        "server = MCPServer('gate')\n"
        "gate = asyncio.Event()\n"
        "entered = asyncio.Event()\n"
        "@server.tool()\n"
        "async def wait_for_release():\n"
        "    entered.set()\n"
        "    await gate.wait()\n"
        "    return 'released'\n"
        "@server.tool()\n"
        "async def release():\n"
        "    await entered.wait()\n"
        "    gate.set()\n"
        "    return 'release-sent'\n"
        "asyncio.run(server.run_stdio_async())\n"
    )
    async with mcp_session(workspace, client_id="waiter") as first:
        configured = await execute(
            first,
            f"await ws.mcp.configure('gate', "
            f"{{'command': {sys.executable!r}, 'args': [{str(server)!r}]}})",
        )
        assert configured["state"] == "succeeded"
        pending = decode_result(
            await first.call_tool(
                "execute",
                {"code": "await ws.mcp.call_tool('gate', 'wait_for_release')", "wait_ms": 0},
            )
        )
        async with mcp_session(workspace, client_id="releaser") as second:
            released = await execute(second, "await ws.mcp.call_tool('gate', 'release')")
            assert released["state"] == "succeeded"
            assert "release-sent" in result_text(released)
        done = await poll_until_done(first, pending["exec_id"])
        assert done["state"] == "succeeded"
        result = ast.literal_eval(result_text(done))
        assert not result["isError"]
        assert "released" in result["content"][0]["text"]
