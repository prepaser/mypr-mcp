import asyncio
import sys

import pytest

from mypr_mcp import services


async def test_lazy_initialization_times_out_all_waiters_and_can_retry(tmp_path, monkeypatch):
    opened = asyncio.Event()
    release_cleanup = asyncio.Event()
    stall = True

    async def open_session(self, stack):
        opened.set()
        if stall:
            stack.push_async_callback(release_cleanup.wait)
            await asyncio.Event().wait()
        return object()

    async def dispatch(*args):
        return {"tools": []}

    monkeypatch.setattr(services._MCPConnection, "_open", open_session)
    monkeypatch.setattr(services, "_session_dispatch", dispatch)
    connection = services._MCPConnection({"command": "unused"}, tmp_path)
    connection.initialization_timeout = 0.05
    first = connection.admit("list_tools", {})
    await opened.wait()
    first.cancel()
    second = connection.admit("list_tools", {})
    readiness = asyncio.create_task(connection.ensure_ready(timeout_seconds=2))
    try:
        with pytest.raises(TimeoutError, match="initialization timed out"):
            await asyncio.wait_for(second, 2)
        with pytest.raises(RuntimeError, match="initialization timed out"):
            await readiness
        with pytest.raises(RuntimeError, match="initialization timed out"):
            connection.admit("list_tools", {})
        release_cleanup.set()
        await connection.task
        assert not connection.busy
        stall = False
        await connection.ensure_ready(timeout_seconds=2)
        assert connection.connected
        assert await connection.request("list_tools", {}) == {"tools": []}
    finally:
        release_cleanup.set()
        await connection.close()
        await asyncio.gather(first, second, readiness, return_exceptions=True)


async def test_unresponsive_stdio_server_has_a_lazy_initialization_deadline(tmp_path):
    server = tmp_path / "unresponsive.py"
    server.write_text("import sys\nsys.stdin.buffer.read()\n")
    connection = services._MCPConnection(
        {"command": sys.executable, "args": [str(server)]}, tmp_path
    )
    connection.initialization_timeout = 0.2
    try:
        with pytest.raises(TimeoutError, match="initialization timed out"):
            await asyncio.wait_for(connection.request("list_tools", {}), 5)
        await asyncio.wait_for(connection.task, 5)
        assert not connection.busy
        assert not connection.connected
    finally:
        await connection.close()
