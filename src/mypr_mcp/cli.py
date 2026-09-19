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
from typing import Any

from mcp.server import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import CallToolResult, ImageContent, TextContent

from . import __version__
from .bridge import ConnectionBridge
from .diagnostics import safe_error
from .instructions import COMMON_INSTRUCTIONS
from .instructions import INSTRUCTIONS as INSTRUCTIONS
from .transport import find_runtime, manager_running, rpc, socket_path


async def ensure(workspace, *, locked=False):
    root = workspace / ".mypr"
    root.mkdir(exist_ok=True)
    path = socket_path(workspace)
    lock = None if locked else (root / "startup.lock").open("a")
    proc = None
    try:
        if lock is not None:
            while True:
                try:
                    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    await asyncio.sleep(0.05)
        found = await find_runtime(workspace)
        if found is not None:
            path, state = found
            from .protocol import check_compatibility

            check_compatibility(state)
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
            launch = asyncio.create_task(
                asyncio.to_thread(
                    subprocess.Popen,
                    [sys.executable, "-m", "mypr_mcp.cli", "_manager"],
                    cwd=workspace,
                    stdin=subprocess.DEVNULL,
                    stdout=log,
                    stderr=log,
                    start_new_session=True,
                    env=dict(os.environ),
                )
            )
            try:
                proc = await asyncio.shield(launch)
            except asyncio.CancelledError:
                proc = await _finish_owned(launch)
                raise
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
    except BaseException:
        if proc is not None and proc.poll() is None:
            cleanup = asyncio.create_task(_stop_spawned(proc))
            while not cleanup.done():
                try:
                    await asyncio.shield(cleanup)
                except asyncio.CancelledError:
                    continue
            cleanup.result()
        raise
    finally:
        if lock is not None:
            lock.close()


async def _finish_owned(task):
    while True:
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            if task.done():
                return task.result()


async def _stop_spawned(proc):
    with contextlib.suppress(ProcessLookupError):
        proc.terminate()
    try:
        await asyncio.to_thread(proc.wait, timeout=10)
    except subprocess.TimeoutExpired:
        with contextlib.suppress(ProcessLookupError):
            proc.kill()
        await asyncio.to_thread(proc.wait)


def tool_result(result):
    result = dict(result)
    warnings = list(result.get("warnings", []))
    images = []
    for event in result.get("output", []):
        for artifact in event.get("artifacts", []):
            path = Path(artifact["path"])
            try:
                if (
                    artifact["mime"] not in {"image/png", "image/jpeg"}
                    or path.stat().st_size > 2 * 1024 * 1024
                ):
                    continue
                images.append(
                    ImageContent(
                        data=base64.b64encode(path.read_bytes()).decode(), mimeType=artifact["mime"]
                    )
                )
            except OSError as exc:
                if len(warnings) < 4:
                    warnings.append(
                        {"code": "artifact_unavailable", "text": safe_error(exc, limit=256)}
                    )
                else:
                    result["warnings_truncated"] = True
    if warnings:
        result["warnings"] = warnings
    content = [TextContent(text=json.dumps(result, ensure_ascii=False)), *images]
    return CallToolResult(
        content=content, structuredContent=result, isError=result.get("state") in {"failed", "lost"}
    )


async def stop_runtime(path, force=False, *, workspace=None, restart_id=None):
    state = await rpc(path, op="status")
    pid = state.get("pid")
    if pid is None:
        try:
            metadata = json.loads(((workspace or Path.cwd()) / ".mypr/runtime.json").read_text())
            if metadata["generation"] != state["generation"] or metadata["socket"] != str(path):
                raise ValueError("Runtime metadata does not match the connected manager")
            pid = metadata["pid"]
        except (OSError, ValueError, KeyError) as exc:
            raise RuntimeError("Cannot identify the workspace manager for shutdown") from exc
    if type(pid) is not int or pid <= 1:
        raise RuntimeError("Invalid workspace manager PID")
    pidfd = os.pidfd_open(pid)
    loop = asyncio.get_running_loop()
    exited = loop.create_future()

    def ready():
        if not exited.done():
            exited.set_result(None)

    loop.add_reader(pidfd, ready)
    try:
        await rpc(path, op="stop", force=force, manager_pid=pid, restart_id=restart_id)
        try:
            await asyncio.wait_for(exited, 30)
        except TimeoutError as exc:
            raise RuntimeError(
                "Workspace manager did not finish stopping within 30 seconds"
            ) from exc
        return {"stopped": True}
    finally:
        loop.remove_reader(pidfd)
        os.close(pidfd)


async def serve(workspace):
    bound_client: str | None = None
    mcp = MCPServer("mypr-mcp", version=__version__, instructions=COMMON_INSTRUCTIONS)
    bridge = ConnectionBridge(workspace)

    async def request(op, **fields):
        try:
            return await bridge.request(op, **fields)
        except RuntimeError as exc:
            raise ToolError(str(exc)) from exc

    @mcp.tool()
    async def init(client_id: str | None = None) -> dict[str, Any]:
        """Bind this connection to a new or existing client ID before executing Python."""
        nonlocal bound_client
        result = await request("init", client_id=client_id)
        bound_client = result["client_id"]
        await bridge.bind_client(bound_client)
        return result

    @mcp.tool()
    async def execute(
        code: str, wait_ms: int = 1000, request_id: str | None = None
    ) -> CallToolResult:
        """Execute an async-capable Python cell after init has bound a client ID."""
        if bound_client is None:
            raise ToolError("Call init before execute")
        if request_id is None:
            request_id = uuid.uuid4().hex
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

    await bridge.start()
    mcp_task = asyncio.create_task(mcp.run_stdio_async())
    try:
        await mcp_task
    finally:
        if not mcp_task.done():
            mcp_task.cancel()
            await asyncio.gather(mcp_task, return_exceptions=True)
        await bridge.close()


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
    parser.add_argument(
        "command", choices=["serve", "status", "logs", "reset", "restart", "stop", "_manager"]
    )
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
                elif args.command == "restart":
                    from . import restart
                    from .protocol import target_installation

                    target = target_installation()
                    ticket = await restart.request_restart(workspace, target, force=args.force)
                    result = await restart.wait_ticket(workspace, ticket["id"], timeout=240)
                    if result.get("state") == "failed":
                        raise RuntimeError(
                            f"Workspace restart failed: {result.get('error') or 'unknown error'}"
                        )
                    return result
                else:
                    found = await find_runtime(workspace)
                    if found is None:
                        raise RuntimeError("No reachable workspace manager")
                    path, _ = found
                if args.command == "stop":
                    return await stop_runtime(path, args.force, workspace=workspace)
                result = await rpc(path, op=args.command, force=args.force)
                if args.command == "status":
                    from .protocol import runtime_info

                    result.update(bridge_version=__version__, manager_version=result.get("version"))
                    try:
                        result.update(runtime_info(result))
                    except RuntimeError as exc:
                        result["compatibility_error"] = str(exc)
                return result

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
