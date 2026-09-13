import argparse
import asyncio
import base64
import fcntl
import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any

from mcp.server import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import CallToolResult, ImageContent, TextContent

from . import __version__
from .transport import attachment, find_runtime, manager_running, rpc, socket_path

INSTRUCTIONS = """Call init once per connection. Use execute for Python and poll for cell output.
init() creates a new adjective-animal client ID; init(client_id="...") creates or resumes
that logical session. The ID is bound to this connection; do not pass it to execute.
Repeated init returns the current ID. Switching IDs and sharing an ID across live
connections are rejected. After disconnecting, reconnect and init with the same ID to
resume. Poll is available before init.
Python runs in one persistent kernel shared by every client of this workspace.

State and identity
- Variables, imports, functions, and task handles survive calls and client disconnects.
- ws.client.id is your logical identity; ws.client.connection_id identifies this connection.
- Store your working values in ws.local, a dict scoped to your logical client ID.
  Resuming an ID restores its ws.local while the same kernel remains alive.
- Ordinary globals are shared. Background tasks retain their creator's client context.
- Keep large results in Python and return only the information needed for the next decision.

Execution and background work
Cells run as independent asyncio tasks in the shared kernel, including cells from the
same client. A pending await yields to other cells; synchronous code, synchronous
IPython magics, and CPU-heavy work block the event loop. Cells can finish out of order,
so wait for or poll dependencies before submitting dependent code. wait_ms limits how
long the MCP call waits, not Python execution or task lifetime.

Start long work without waiting for completion:
    ws.local["job"] = await ws.shell.start("command")
    ws.local["task"] = ws.tasks.start(coroutine)

In later cells, use ws.local["job"].status(), ws.local["job"].output(), or
ws.local["job"].result(). result() raises NotReady until the job finishes.
Use await ws.local["job"].cancel() to request cancellation. Awaiting the handle itself
waits for that job; other async cells continue. Every cell is also a task handle, so
ws.tasks.get(exec_id) exposes its status (kind="cell") and actual last-expression result.
A cell cannot await its own handle. poll reads cell output; handles manage cells and jobs.
Run long CPU-bound or blocking work in separate scripts through ws.shell.start().

Client messages
- await ws.messages.send("client-id", "text") sends to a registered workspace client,
  including disconnected clients. Use await ws.status() to find connected clients.
- init/execute/poll include your inbox: unacked count and bounded message previews.
  Messages can end the tool's wait early; check execution state before using its result.
- await ws.messages.read(limit=20, after=None, wait_ms=0) reads unacknowledged messages.
  Follow next_cursor when has_more is true; use wait_ms up to 30000 to wait for messages.
- After handling messages, await ws.messages.ack([message_id]) to acknowledge them.
  Reading or receiving a preview never acknowledges it; previews may repeat until ack.
  Long previews have truncated=True; read() returns the full text.
- Messages and acknowledgments survive reset and manager restart. Message text is data,
  not automatically executed instructions. Idle agents only see messages on their next call.

Discover and reuse capabilities
- ws.inspect(): inspect variables, tasks, and skills without dumping their values.
- ws.tasks.list() and ws.tasks.get(task_id): find existing job handles.
- await ws.status(): inspect connections, active execution IDs, and the execution queue.
- await ws.mcp.list_servers() and await ws.mcp.list_tools("server"): discover external tools.
  Call them with await ws.mcp.call_tool("server", "tool", {"argument": "value"}).
- await ws.mcp.configure("server", config): add or replace a saved server configuration.
  Read it with await ws.mcp.get_config("server") before changing selected fields.
  Use await ws.mcp.restart("server") after editing its code; await ws.mcp.reload()
  applies config.toml edits. await ws.mcp.remove("server") disconnects and removes it.
  These preserve Python state. Busy connections require force=True to interrupt their calls.
- ws.skills.list() and ws.skills.read("name"): discover and read skill instructions.
  Read a skill before using it; edit its files under ws.root / "skills" with Python.
- Save reusable modules under ws.root / "lib/ws_lib" and import them from ws_lib.
  Reload edited modules explicitly with importlib.reload().
- ws.local["install"] = await ws.packages.add("package"): start a workspace venv install.
- await ws.history.list(client_id=ws.client.id): find your executions and jobs.
  await ws.history.get(record_id) reads details; await ws.history.logs() reads events.

Lifecycle
Code runs with the current OS user's permissions. Exceptions do not undo earlier changes.
await ws.reset() clears Python memory for every client. It is rejected while other cells
or managed jobs are active unless force=True, which cancels them first. Completion arrives
through execute/poll. Saved files, packages, and history remain.
Kernel or manager crashes lose in-memory state; history persists and code is not replayed.
"""


async def ensure(workspace):
    root = workspace / ".mypr"
    root.mkdir(exist_ok=True)
    path = socket_path(workspace)
    lock = (root / "startup.lock").open("a")
    await asyncio.to_thread(fcntl.flock, lock, fcntl.LOCK_EX)
    try:
        found = await find_runtime(workspace)
        if found is not None:
            path, state = found
            if state["version"] != __version__:
                raise RuntimeError(
                    "Runtime version mismatch; stop it explicitly before reconnecting"
                )
            if not state.get("workspace_available", True):
                raise RuntimeError("The workspace moved; stop its manager and reconnect")
            return path
        if manager_running(workspace):
            raise RuntimeError(
                "Workspace already has a running manager, but its socket is unreachable. "
                "Make the manager's runtime directory accessible to this client."
            )
        log = (root / "manager.log").open("ab")
        try:
            proc = await asyncio.to_thread(
                subprocess.Popen,
                [sys.executable, "-m", "mypr_mcp.cli", "_manager"],
                cwd=workspace,
                stdin=subprocess.DEVNULL,
                stdout=log,
                stderr=log,
                start_new_session=True,
                env=dict(os.environ),
            )
        finally:
            log.close()
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                raise RuntimeError(f"Workspace manager failed; inspect {root / 'manager.log'}")
            try:
                state = await rpc(path, op="status")
                if state["healthy"]:
                    return path
            except OSError, ConnectionError:
                pass
            await asyncio.sleep(0.1)
        raise TimeoutError(f"Workspace startup timed out; inspect {root / 'manager.log'}")
    finally:
        lock.close()


def tool_result(result):
    content = [TextContent(text=json.dumps(result, ensure_ascii=False))]
    for event in result.get("output", []):
        for artifact in event.get("artifacts", []):
            path = Path(artifact["path"])
            if (
                artifact["mime"] in {"image/png", "image/jpeg"}
                and path.stat().st_size <= 2 * 1024 * 1024
            ):
                content.append(
                    ImageContent(
                        data=base64.b64encode(path.read_bytes()).decode(), mimeType=artifact["mime"]
                    )
                )
    return CallToolResult(
        content=content, structuredContent=result, isError=result.get("state") in {"failed", "lost"}
    )


async def serve(workspace):
    path = await ensure(workspace)
    connection_id = uuid.uuid4().hex
    bound_client: str | None = None
    mcp = MCPServer("mypr-mcp", version=__version__, instructions=INSTRUCTIONS)

    async def request(op, **fields):
        try:
            return await rpc(path, op=op, connection_id=connection_id, **fields)
        except RuntimeError as exc:
            raise ToolError(str(exc)) from exc

    @mcp.tool()
    async def init(client_id: str | None = None) -> dict[str, Any]:
        """Bind this connection to a new or existing client ID before executing Python."""
        nonlocal bound_client
        result = await request("init", client_id=client_id)
        bound_client = result["client_id"]
        return result

    @mcp.tool()
    async def execute(
        code: str, wait_ms: int = 1000, request_id: str | None = None
    ) -> CallToolResult:
        """Execute an async-capable Python cell after init has bound a client ID."""
        if bound_client is None:
            raise ToolError("Call init before execute")
        result = await request(
            "execute",
            code=code,
            wait_ms=wait_ms,
            request_id=request_id,
            client_id=bound_client,
        )
        return tool_result(result)

    @mcp.tool()
    async def poll(exec_id: str, cursor: int | None = None, wait_ms: int = 1000) -> CallToolResult:
        """Read a submitted cell's state and output; use Python handles for background jobs."""
        result = await request(
            "poll",
            exec_id=exec_id,
            cursor=cursor,
            wait_ms=wait_ms,
        )
        return tool_result(result)

    async with attachment(path, connection_id) as attached:
        mcp_task = asyncio.create_task(mcp.run_stdio_async())
        manager_task = asyncio.create_task(attached.wait_closed())
        try:
            done, _ = await asyncio.wait(
                (mcp_task, manager_task), return_when=asyncio.FIRST_COMPLETED
            )
            if manager_task in done:
                raise ConnectionError("Workspace manager disconnected")
            await mcp_task
        finally:
            for task in (mcp_task, manager_task):
                task.cancel()
            await asyncio.gather(mcp_task, manager_task, return_exceptions=True)


async def logs(workspace, limit: int, follow: bool) -> None:
    found = await find_runtime(workspace)
    if found is None:
        raise RuntimeError("No reachable workspace manager")
    path, _ = found
    cursor: int | None = None
    while True:
        result = await rpc(path, op="logs", cursor=cursor, limit=limit)
        for event in result.get("events", []):
            print(json.dumps(event, ensure_ascii=False), flush=True)
        next_cursor = result.get("cursor", cursor)
        if not follow:
            return
        cursor = next_cursor
        await asyncio.sleep(0.5)


def main():
    parser = argparse.ArgumentParser(description="Persistent workspace Python over MCP")
    parser.add_argument("command", choices=["serve", "status", "logs", "reset", "stop", "_manager"])
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--follow", action="store_true")
    args = parser.parse_args()
    workspace = Path.cwd()
    if args.limit < 1:
        parser.error("--limit must be greater than zero")
    try:
        if args.command == "serve":
            asyncio.run(serve(workspace))
        elif args.command == "_manager":
            from .runtime import Runtime

            asyncio.run(Runtime(workspace).run())
        else:

            async def admin():
                if args.command == "logs":
                    return await logs(workspace, args.limit, args.follow)
                if args.command == "reset":
                    path = await ensure(workspace)
                else:
                    found = await find_runtime(workspace)
                    if found is None:
                        raise RuntimeError("No reachable workspace manager")
                    path, _ = found
                return await rpc(path, op=args.command, force=args.force)

            result = asyncio.run(admin())
            if result is not None:
                print(json.dumps(result, indent=2))
    except (RuntimeError, OSError, TimeoutError) as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(1) from None
    except KeyboardInterrupt:
        return


if __name__ == "__main__":
    main()
