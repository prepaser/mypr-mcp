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

from mcp.server import MCPServer
from mcp.types import CallToolResult, ImageContent, TextContent

from . import __version__
from .transport import attachment, rpc, socket_path

INSTRUCTIONS = """This is one persistent Python workspace shared by every connected agent.
Only execute and poll are MCP tools. Variables, imports, functions and ws handles persist.
ws.client.id/name/connection_id identify this caller. Store private state in ws.local,
which is scoped to the logical client ID and survives reconnects with that ID.
Ordinary globals remain shared; use them only when sharing is intentional.
Use short cells. Start commands with ws.local["job"] = await ws.shell.start("command").
Start async I/O with ws.local["job"] = ws.tasks.start(coroutine); both return handles.
In later cells call ws.local["job"].status(), .output(), .result(), or await ws.local["job"].cancel().
Background tasks retain their creator's client context while other clients execute.
await ws.status() reports connection_count, client_count, and connection/activity details.
await ws.history.list(client_id=ws.client.id) lists owned executions and tasks.
await ws.history.get(id) reads details; await ws.history.logs(client_id=ws.client.id)
reads lifecycle/output events. History persists across resets and manager restarts.
Use ws.tasks.list()/get(id) to rediscover jobs. Awaiting a job waits for completion and holds
that cell; execute returning a running ID does NOT release the kernel for another cell.
Use poll only to collect a cell's output. Use Python handles to inspect background jobs.
ws.mcp exposes configured external MCP servers; await ws.mcp.list_servers() to discover them.
ws.skills.list()/read(name) discover workspace SKILL.md instructions. Read before applying.
Save reusable Python modules under ws.root / "lib/ws_lib" and import from ws_lib.
Use pathlib for edits, importlib.reload for explicit reloads, and ws.inspect() to rediscover state.
await ws.status() inspects runtime; await ws.reset() resets shared memory for ALL agents.
Reset terminates its cell and returns completion through execute/poll, preserving saved files.
Current OS user permissions apply. Changes and exceptions do not roll back shared state.
"""


async def ensure(workspace):
    root = workspace / ".mypr"
    root.mkdir(exist_ok=True)
    path = socket_path(workspace)
    lock = (root / "startup.lock").open("a")
    await asyncio.to_thread(fcntl.flock, lock, fcntl.LOCK_EX)
    try:
        try:
            state = await rpc(path, op="status")
        except OSError, ConnectionError:
            state = None
        if state is not None:
            if state["version"] != __version__:
                raise RuntimeError(
                    "Runtime version mismatch; stop it explicitly before reconnecting"
                )
            return path
        log = (root / "manager.log").open("ab")
        try:
            proc = await asyncio.to_thread(
                subprocess.Popen,
                [sys.executable, "-m", "mypr_mcp.cli", "_manager", "--workspace", str(workspace)],
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
    content = [TextContent(text=json.dumps(result))]
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


async def serve(workspace, client_id: str | None = None, client_name: str | None = None):
    path = await ensure(workspace)
    client_id = client_id or uuid.uuid4().hex
    connection_id = uuid.uuid4().hex
    mcp = MCPServer("mypr-mcp", version=__version__, instructions=INSTRUCTIONS)

    @mcp.tool()
    async def execute(
        code: str, wait_ms: int = 1000, request_id: str | None = None
    ) -> CallToolResult:
        """Execute a Python cell in the shared workspace. Long jobs should return handles."""
        result = await rpc(
            path,
            op="execute",
            code=code,
            wait_ms=wait_ms,
            request_id=request_id,
            client_id=client_id,
            connection_id=connection_id,
        )
        return tool_result(result)

    @mcp.tool()
    async def poll(exec_id: str, cursor: int | None = None, wait_ms: int = 1000) -> CallToolResult:
        """Read a submitted cell's state and output; use Python handles for background jobs."""
        result = await rpc(
            path,
            op="poll",
            exec_id=exec_id,
            cursor=cursor,
            wait_ms=wait_ms,
            client_id=client_id,
            connection_id=connection_id,
        )
        return tool_result(result)

    async with attachment(path, client_id, connection_id, client_name) as attached:
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


async def logs(workspace, client_id: str | None, limit: int, follow: bool) -> None:
    path = socket_path(workspace)
    cursor: int | None = None
    while True:
        result = await rpc(path, op="logs", cursor=cursor, filter_client_id=client_id, limit=limit)
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
    parser.add_argument("--workspace", required=True, type=Path)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--client-id")
    parser.add_argument("--client-name")
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--follow", action="store_true")
    args = parser.parse_args()
    workspace = args.workspace.resolve(strict=True)
    if not workspace.is_dir():
        parser.error("workspace must be a directory")
    if args.limit < 1:
        parser.error("--limit must be greater than zero")
    try:
        if args.command == "serve":
            asyncio.run(serve(workspace, args.client_id, args.client_name))
        elif args.command == "_manager":
            from .runtime import Runtime

            asyncio.run(Runtime(workspace).run())
        else:

            async def admin():
                path = socket_path(workspace)
                if args.command == "reset":
                    path = await ensure(workspace)
                if args.command == "logs":
                    return await logs(workspace, args.client_id, args.limit, args.follow)
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
