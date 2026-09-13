from __future__ import annotations

import ast
import asyncio
import json
import sys
from pathlib import Path

import pytest
from conftest import execute, mcp_session, result_text

from mypr_mcp import services
from mypr_mcp.services import MCPBridge


def json_output(payload: dict) -> object:
    value = ast.literal_eval(result_text(payload).strip())
    return json.loads(value) if isinstance(value, str) else value


def _write_server(path: Path, version: str = "v1", *, slow: bool = False) -> None:
    slow_tool = (
        "\n@server.tool()\n"
        "async def slow(seconds: float = 30) -> str:\n"
        "    await asyncio.sleep(seconds)\n"
        "    return 'done'\n"
        if slow
        else ""
    )
    path.write_text(
        "import asyncio\n"
        "from mcp.server import MCPServer\n\n"
        "server = MCPServer('dynamic', version='1')\n"
        f"version = {version!r}\n\n"
        "@server.tool()\n"
        "async def echo(text: str) -> str:\n"
        "    return f'{version}:{text}'\n"
        f"{slow_tool}\n"
        "if __name__ == '__main__':\n"
        "    asyncio.run(server.run_stdio_async())\n"
    )


def _config(command: str | Path, server: Path, *args: str) -> dict[str, object]:
    return {"command": str(command), "args": [str(server), *args]}


def _toml_servers(**servers: dict[str, object]) -> str:
    lines = ["[mcp.servers]"]
    for name, config in servers.items():
        lines.append(f"[mcp.servers.{name}]")
        lines.append(f"command = {json.dumps(config['command'])}")
        args = ", ".join(json.dumps(str(value)) for value in config.get("args", []))
        lines.append(f"args = [{args}]")
    return "\n".join(lines) + "\n"


async def test_dynamic_configure_preserves_kernel_state_and_restart_reloads_code(
    workspace: Path,
):
    server = workspace / "dynamic_server.py"
    _write_server(server, "v1")
    python = sys.executable
    config = _config(python, server)

    async with mcp_session(workspace) as session:
        before = await execute(
            session,
            "import asyncio\n"
            "shared_value = 41\n"
            "def shared_function(value):\n    return value + shared_value\n"
            "ws.local['marker'] = 'local-state'\n"
            "ws.local['job'] = ws.tasks.start(asyncio.sleep(0.5, result='job-ok'))\n"
            "import json\n"
            "json.dumps((await ws.status())['generation'])",
        )
        generation = json_output(before)

        configured = await execute(
            session,
            f"config = {config!r}\n"
            "await ws.mcp.configure('dynamic', config)\n"
            "await ws.mcp.get_config('dynamic')",
        )
        assert "dynamic" in result_text(configured)
        called = await execute(
            session,
            "await ws.mcp.call_tool('dynamic', 'echo', {'text': 'hello'})",
        )
        assert "v1:hello" in result_text(called)

        _write_server(server, "v2")
        restarted = await execute(
            session,
            "await ws.mcp.restart('dynamic')\n"
            "await ws.mcp.call_tool('dynamic', 'echo', {'text': 'hello'})",
        )
        assert "v2:hello" in result_text(restarted)

        state = await execute(
            session,
            "json.dumps({'generation': (await ws.status())['generation'], "
            "'value': shared_function(1), 'local': ws.local['marker'], "
            "'job': (await ws.local['job'])})",
        )
        state = json_output(state)
        assert state == {
            "generation": generation,
            "value": 42,
            "local": "local-state",
            "job": "job-ok",
        }

        missing = str(workspace / "missing-server")
        failed = await execute(
            session,
            f"await ws.mcp.configure('broken', {{'command': {missing!r}}})\n"
            "await ws.mcp.restart('broken')",
        )
        assert failed["state"] == "failed"
        repaired = await execute(
            session,
            f"await ws.mcp.configure('broken', {config!r})\n"
            "await ws.mcp.restart('broken')\n"
            "await ws.mcp.call_tool('broken', 'echo', {'text': 'repaired'})",
        )
        assert repaired["state"] == "succeeded"
        assert "v2:repaired" in result_text(repaired)
        preserved = await execute(session, "json.dumps((await ws.status())['generation'])")
        assert json_output(preserved) == generation


async def test_bridge_configure_remove_roundtrip_preserves_toml_comments(workspace: Path):
    root = workspace / ".mypr"
    root.mkdir()
    path = root / "config.toml"
    path.write_text(
        "# keep this comment\n"
        "[limits]\nresponse_bytes = 2048\n\n"
        '[mcp.servers.keep]\ncommand = "keep"\nargs = []\n'
    )
    bridge = MCPBridge(workspace)
    try:
        config = {"command": sys.executable, "args": ["keep.py"], "cwd": "."}
        await bridge.configure("replace", config)
        assert bridge.get_config("replace") == config
        text = path.read_text()
        assert "# keep this comment" in text
        assert "response_bytes = 2048" in text
        assert "[mcp.servers.keep]" in text

        await bridge.configure("replace", {"command": sys.executable, "args": ["new.py"]})
        assert bridge.get_config("replace") == {
            "command": sys.executable,
            "args": ["new.py"],
        }
        await bridge.remove("replace")
        with pytest.raises(ValueError):
            bridge.get_config("replace")
        assert "[mcp.servers.replace]" not in path.read_text()
        assert "[mcp.servers.keep]" in path.read_text()
    finally:
        await bridge.close()


async def test_invalid_reload_leaves_file_and_existing_connection_unchanged(workspace: Path):
    server = workspace / "echo.py"
    _write_server(server)
    config = _config(sys.executable, server)
    root = workspace / ".mypr"
    root.mkdir()
    (root / "config.toml").write_text(_toml_servers(echo=config))
    bridge = MCPBridge(workspace)
    try:
        await bridge.dispatch("list_tools", {"server": "echo"})
        connection = bridge._connections["echo"]
        path = root / "config.toml"
        invalid = '[mcp.servers.echo\ncommand = "broken"\n'
        path.write_text(invalid)
        with pytest.raises((ValueError, RuntimeError)):
            await bridge.reload()
        assert path.read_text() == invalid
        assert bridge._connections["echo"] is connection
        await bridge.dispatch("list_tools", {"server": "echo"})
    finally:
        await bridge.close()


async def test_failed_config_write_leaves_config_and_connection_state(
    workspace: Path, monkeypatch: pytest.MonkeyPatch
):
    server = workspace / "echo.py"
    _write_server(server)
    root = workspace / ".mypr"
    root.mkdir()
    path = root / "config.toml"
    path.write_text(_toml_servers(echo=_config(sys.executable, server)))
    bridge = MCPBridge(workspace)
    try:
        await bridge.dispatch("list_tools", {"server": "echo"})
        connection = bridge._connections["echo"]
        before = path.read_text()

        def fail_replace(*args, **kwargs):
            raise OSError("simulated disk failure")

        monkeypatch.setattr("mypr_mcp.services.os.replace", fail_replace)
        with pytest.raises(OSError):
            await bridge.configure("new", _config(sys.executable, server))
        assert path.read_text() == before
        assert "new" not in bridge.config
        assert bridge._connections["echo"] is connection
    finally:
        await bridge.close()


async def test_invalid_configure_schema_leaves_file_and_runtime_state(workspace: Path):
    server = workspace / "echo.py"
    _write_server(server)
    root = workspace / ".mypr"
    root.mkdir()
    path = root / "config.toml"
    valid = _config(sys.executable, server)
    path.write_text(_toml_servers(echo=valid))
    bridge = MCPBridge(workspace)
    try:
        await bridge.dispatch("list_tools", {"server": "echo"})
        connection = bridge._connections["echo"]
        before = path.read_text()
        with pytest.raises(ValueError):
            await bridge.configure("echo", {"command": 42})
        assert path.read_text() == before
        assert bridge.get_config("echo") == valid
        assert bridge._connections["echo"] is connection
    finally:
        await bridge.close()


async def test_restart_does_not_block_unrelated_server_during_initialization(
    workspace: Path, monkeypatch: pytest.MonkeyPatch
):
    target = workspace / "target.py"
    other = workspace / "other.py"
    _write_server(target, "target")
    _write_server(other, "other")
    target_config = _config(sys.executable, target)
    other_config = _config(sys.executable, other)
    bridge = MCPBridge(workspace)
    try:
        await bridge.configure("target", target_config)
        await bridge.configure("other", other_config)
        await bridge.dispatch("list_tools", {"server": "target"})
        await bridge.dispatch("list_tools", {"server": "other"})

        started = asyncio.Event()
        release = asyncio.Event()
        original_open = services._MCPConnection._open

        async def gated_open(connection, stack):
            if connection.config == target_config:
                started.set()
                await release.wait()
            return await original_open(connection, stack)

        monkeypatch.setattr(services._MCPConnection, "_open", gated_open)
        restarting = asyncio.create_task(bridge.restart("target"))
        await asyncio.wait_for(started.wait(), 5)
        result = await asyncio.wait_for(
            bridge.dispatch(
                "call_tool", {"server": "other", "name": "echo", "arguments": {"text": "ok"}}
            ),
            2,
        )
        assert not result["isError"]
        assert "other:ok" in json.dumps(result)
        release.set()
        await asyncio.wait_for(restarting, 10)
    finally:
        release.set() if "release" in locals() else None
        await bridge.close()


async def test_force_configure_cancels_only_affected_server(workspace: Path):
    slow_server = workspace / "slow.py"
    echo_server = workspace / "echo.py"
    _write_server(slow_server, slow=True)
    _write_server(echo_server, "stable")
    bridge = MCPBridge(workspace)
    try:
        await bridge.configure("slow", _config(sys.executable, slow_server))
        await bridge.configure("echo", _config(sys.executable, echo_server))
        request = asyncio.create_task(
            bridge.dispatch("call_tool", {"server": "slow", "name": "slow", "arguments": {}})
        )
        await asyncio.sleep(0.3)
        with pytest.raises((RuntimeError, ValueError)):
            await asyncio.wait_for(
                bridge.configure("slow", _config(sys.executable, slow_server, "v2")), 2
            )
        await bridge.configure("slow", _config(sys.executable, slow_server, "v2"), force=True)
        with pytest.raises((asyncio.CancelledError, RuntimeError, ValueError)):
            await request
        result = await bridge.dispatch(
            "call_tool", {"server": "echo", "name": "echo", "arguments": {"text": "ok"}}
        )
        assert not result["isError"]
        assert "stable:ok" in json.dumps(result)
    finally:
        await bridge.close()


async def test_reload_reconnects_only_changed_or_removed_servers(workspace: Path):
    stable = workspace / "stable.py"
    changed = workspace / "changed.py"
    _write_server(stable, "stable")
    _write_server(changed, "v1")
    configs = {
        "stable": _config(sys.executable, stable),
        "changed": _config(sys.executable, changed),
        "removed": _config(sys.executable, stable),
    }
    root = workspace / ".mypr"
    root.mkdir()
    path = root / "config.toml"
    path.write_text(_toml_servers(**configs))
    bridge = MCPBridge(workspace)
    try:
        for name in configs:
            await bridge.dispatch("list_tools", {"server": name})
        stable_connection = bridge._connections["stable"]
        changed_connection = bridge._connections["changed"]
        _write_server(changed, "v2")
        path.write_text(
            _toml_servers(
                stable=configs["stable"],
                changed=_config(sys.executable, changed, "v2"),
            )
        )
        await bridge.reload()
        assert bridge._connections["stable"] is stable_connection
        assert "changed" not in bridge._connections
        assert "removed" not in bridge._connections
        await bridge.dispatch("list_tools", {"server": "changed"})
        assert bridge._connections["changed"] is not changed_connection
        result = await bridge.dispatch(
            "call_tool", {"server": "changed", "name": "echo", "arguments": {"text": "ok"}}
        )
        assert "v2:ok" in json.dumps(result)
    finally:
        await bridge.close()


async def test_configure_detects_direct_file_edit_until_reload(workspace: Path):
    server = workspace / "echo.py"
    _write_server(server)
    root = workspace / ".mypr"
    root.mkdir()
    path = root / "config.toml"
    initial = _config(sys.executable, server)
    path.write_text(_toml_servers(echo=initial))
    bridge = MCPBridge(workspace)
    try:
        await bridge.configure("echo", initial)
        path.write_text(_toml_servers(echo=_config(sys.executable, server, "direct-edit")))
        changed = _config(sys.executable, server, "updated")
        with pytest.raises((RuntimeError, ValueError)):
            await bridge.configure("echo", changed)
        await bridge.reload()
        await bridge.configure("echo", changed)
        assert bridge.get_config("echo") == changed
    finally:
        await bridge.close()


async def test_connection_close_finishes_when_caller_cancels_at_same_time(workspace, monkeypatch):
    entered = asyncio.Event()

    async def opened(self, stack):
        return object()

    async def waiting(*args):
        entered.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(services._MCPConnection, "_open", opened)
    monkeypatch.setattr(services, "_session_dispatch", waiting)
    connection = services._MCPConnection({"command": "unused"}, workspace)
    request = connection.admit("list_tools", {})
    await asyncio.wait_for(entered.wait(), 5)
    request.cancel()
    closing = asyncio.create_task(connection.close())
    try:
        done, _ = await asyncio.wait({closing}, timeout=1)
        assert closing in done, "closing must not swallow cancellation and wait for another request"
        await closing
        assert request.cancelled()
    finally:
        if connection.task is not None:
            connection.task.cancel()
        await asyncio.gather(closing, return_exceptions=True)


async def test_connection_runs_same_server_requests_concurrently(workspace, monkeypatch):
    started = asyncio.Event()
    release = asyncio.Event()
    admitted: set[str] = set()

    async def opened(self, stack):
        return object()

    async def dispatch(session, method, args):
        del session, method
        admitted.add(args["token"])
        if len(admitted) == 2:
            started.set()
        await release.wait()
        return args["token"]

    monkeypatch.setattr(services._MCPConnection, "_open", opened)
    monkeypatch.setattr(services, "_session_dispatch", dispatch)
    connection = services._MCPConnection({"command": "unused"}, workspace)
    first = connection.admit("call_tool", {"token": "first"})
    second = connection.admit("call_tool", {"token": "second"})
    try:
        await asyncio.wait_for(started.wait(), 5)
        assert connection.busy
        release.set()
        assert await asyncio.gather(first, second) == ["first", "second"]
        assert not connection.busy
    finally:
        await connection.close()


async def test_connection_request_cancellation_and_errors_are_independent(workspace, monkeypatch):
    started = asyncio.Event()
    cancelled = asyncio.Event()
    release = asyncio.Event()

    async def opened(self, stack):
        return object()

    async def dispatch(session, method, args):
        del session, method
        if args["token"] == "error":
            raise ValueError("request failed")
        if args["token"] == "cancel":
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelled.set()
                raise
        started.set()
        await release.wait()
        return "completed"

    monkeypatch.setattr(services._MCPConnection, "_open", opened)
    monkeypatch.setattr(services, "_session_dispatch", dispatch)
    connection = services._MCPConnection({"command": "unused"}, workspace)
    failed = connection.admit("call_tool", {"token": "error"})
    to_cancel = connection.admit("call_tool", {"token": "cancel"})
    live = connection.admit("call_tool", {"token": "live"})
    try:
        with pytest.raises(ValueError, match="request failed"):
            await failed
        await asyncio.wait_for(started.wait(), 5)
        to_cancel.cancel()
        with pytest.raises(asyncio.CancelledError):
            await to_cancel
        await asyncio.wait_for(cancelled.wait(), 5)
        assert connection.busy
        release.set()
        assert await live == "completed"
        assert not connection.busy
    finally:
        await connection.close()


async def test_force_reconfigure_cancels_all_requests_on_one_connection(workspace, monkeypatch):
    started = asyncio.Event()
    active: set[str] = set()

    async def opened(self, stack):
        return object()

    async def dispatch(session, method, args):
        del session, method
        active.add(args["token"])
        if len(active) == 2:
            started.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(services._MCPConnection, "_open", opened)
    monkeypatch.setattr(services, "_session_dispatch", dispatch)
    bridge = MCPBridge(workspace)
    config = {"command": "server-v1"}
    try:
        await bridge.configure("target", config)
        first = asyncio.create_task(
            bridge.dispatch("call_tool", {"server": "target", "token": "first"})
        )
        second = asyncio.create_task(
            bridge.dispatch("call_tool", {"server": "target", "token": "second"})
        )
        await asyncio.wait_for(started.wait(), 5)
        with pytest.raises(RuntimeError, match="active requests"):
            await bridge.configure("target", {"command": "server-v2"})
        result = await bridge.configure("target", {"command": "server-v2"}, force=True)
        assert result["action"] == "updated"
        outcomes = await asyncio.gather(first, second, return_exceptions=True)
        assert all(isinstance(outcome, RuntimeError) for outcome in outcomes)
        assert bridge.config["target"] == {"command": "server-v2"}
        assert "target" not in bridge._connections
    finally:
        await bridge.close()
