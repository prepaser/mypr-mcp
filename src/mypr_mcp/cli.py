import argparse
import asyncio
import base64
import contextlib
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
from .transport import rpc, socket_path

INSTRUCTIONS = """This is one persistent Python workspace shared by every connected agent.
Only execute and poll are MCP tools. Variables, imports, functions and ws handles persist.
Use short cells. Start long commands with job = await ws.shell.start("command").
Start async I/O with job = ws.tasks.start(coroutine). These return handles, not final results.
In later cells use job.status(), job.output(), job.result(), await job.cancel().
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


async def serve(workspace):
    path = await ensure(workspace)
    client_id = uuid.uuid4().hex
    await rpc(path, op="attach", client_id=client_id)
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
        )
        return tool_result(result)

    @mcp.tool()
    async def poll(exec_id: str, cursor: int | None = None, wait_ms: int = 1000) -> CallToolResult:
        """Read a submitted cell's state and output; use Python handles for background jobs."""
        result = await rpc(path, op="poll", exec_id=exec_id, cursor=cursor, wait_ms=wait_ms)
        return tool_result(result)

    try:
        await mcp.run_stdio_async()
    finally:
        with contextlib.suppress(OSError, ConnectionError):
            await rpc(path, op="detach", client_id=client_id)


def main():
    parser = argparse.ArgumentParser(description="Persistent workspace Python over MCP")
    parser.add_argument("command", choices=["serve", "status", "reset", "stop", "_manager"])
    parser.add_argument("--workspace", required=True, type=Path)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    workspace = args.workspace.resolve(strict=True)
    if not workspace.is_dir():
        parser.error("workspace must be a directory")
    try:
        if args.command == "serve":
            asyncio.run(serve(workspace))
        elif args.command == "_manager":
            from .runtime import Runtime

            asyncio.run(Runtime(workspace).run())
        else:

            async def admin():
                path = socket_path(workspace)
                if args.command == "reset":
                    path = await ensure(workspace)
                return await rpc(path, op=args.command, force=args.force)

            print(json.dumps(asyncio.run(admin()), indent=2))
    except (RuntimeError, OSError, TimeoutError) as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
