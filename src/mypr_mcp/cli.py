import argparse
import asyncio
import base64
import contextlib
import fcntl
import json
import os
import signal
import stat
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Annotated, Any

from mcp.server import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import CallToolResult, ImageContent, TextContent
from pydantic import Field

from . import __version__
from .async_utils import wait_owned
from .bridge import ConnectionBridge
from .config import ConfigError, ConfigStore
from .diagnostics import RPCError, safe_error
from .doctor import doctor_workspace
from .instructions import COMMON_INSTRUCTIONS
from .instructions import INSTRUCTIONS as INSTRUCTIONS
from .startup import (
    clear_startup_failure,
    read_startup_failure,
    startup_error,
    write_startup_failure,
)
from .transport import find_runtime, manager_running, rpc, socket_path

STARTUP_TIMEOUT = 180


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
        try:
            ConfigStore(workspace).load()
        except (ConfigError, OSError) as exc:
            write_startup_failure(root, exc, operation="config_validate")
            raise RPCError(
                str(exc),
                code=getattr(exc, "code", "invalid_workspace_config"),
                operation="config_validate",
                details={
                    key: value
                    for key, value in {
                        "path": getattr(exc, "path", None),
                        "line": getattr(exc, "line", None),
                        "column": getattr(exc, "column", None),
                    }.items()
                    if value is not None
                },
                error_type=type(exc).__name__,
            ) from exc
        clear_startup_failure(root)
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
        deadline = time.monotonic() + STARTUP_TIMEOUT
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                error = startup_error(root)
                if error is None:
                    error = RPCError(
                        f"Workspace manager failed; inspect {root / 'manager.log'}",
                        code="manager_start_failed",
                        operation="manager_start",
                    )
                    write_startup_failure(root, error, operation="manager_start")
                raise error
            try:
                remaining = max(0.0, deadline - time.monotonic())
                state = await asyncio.wait_for(rpc(path, op="status"), min(5, remaining))
                if state["healthy"]:
                    clear_startup_failure(root)
                    return path
            except OSError, ConnectionError, TimeoutError:
                pass
            await asyncio.sleep(min(0.1, max(0.0, deadline - time.monotonic())))
        error = RPCError(
            f"Workspace startup timed out; inspect {root / 'manager.log'}",
            code="manager_start_timeout",
            operation="manager_start",
        )
        write_startup_failure(root, error, operation="manager_start")
        raise error
    except BaseException:
        if proc is not None:
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
    return await wait_owned(task, propagate=False)


async def _stop_spawned(proc):
    with contextlib.suppress(ProcessLookupError):
        os.killpg(proc.pid, signal.SIGTERM)
    try:
        await asyncio.to_thread(proc.wait, timeout=10)
    except subprocess.TimeoutExpired:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(proc.pid, signal.SIGKILL)
        await asyncio.to_thread(proc.wait)


async def run_manager(workspace):
    """Run a manager and persist exceptions raised before it becomes healthy."""

    from .runtime import Runtime

    try:
        runtime = Runtime(workspace)
    except BaseException as exc:
        write_startup_failure(Path(workspace) / ".mypr", exc, operation="manager_start")
        raise
    task = asyncio.create_task(runtime.run(), name="mypr-manager")
    healthy = False
    try:
        while not task.done():
            if runtime.healthy:
                healthy = True
                clear_startup_failure(runtime.root)
                break
            await asyncio.sleep(0.02)
        await task
        if runtime.healthy:
            healthy = True
            clear_startup_failure(runtime.root)
    except BaseException as exc:
        if not healthy:
            write_startup_failure(
                runtime.root,
                exc,
                operation="manager_start",
                details={"health_error": runtime.health_error}
                if runtime.health_error
                else None,
            )
        raise
    else:
        if healthy:
            clear_startup_failure(runtime.root)
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


_IMAGE_RESPONSE_LIMIT = 2 * 1024 * 1024


def _read_image(path: Path, limit: int) -> bytes:
    flags = os.O_RDONLY | os.O_NONBLOCK
    flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags)
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise OSError("artifact is not a regular file")
        chunks = []
        size = 0
        while size <= limit:
            chunk = os.read(fd, limit + 1 - size)
            if not chunk:
                break
            chunks.append(chunk)
            size += len(chunk)
        return b"".join(chunks)
    finally:
        os.close(fd)


def _encode_image(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


def _render_runtime(runtime: dict[str, Any]) -> list[str]:
    lines = ["runtime:"]
    for key in ("manager_version", "bridge_version", "protocol_version", "generation"):
        if runtime.get(key) is not None:
            lines.append(f"{key}={runtime[key]}")
    if "update_pending" in runtime:
        lines.append(f"update_pending={str(runtime['update_pending']).lower()}")
    if runtime.get("capabilities"):
        lines.append("capabilities=" + ", ".join(runtime["capabilities"]))
    if runtime.get("instructions"):
        lines.extend(("instructions:", runtime["instructions"]))
    return lines


def _render_output_event(event: dict[str, Any]) -> list[str]:
    kind = event.get("type")
    text = event.get("text", "")
    if kind == "stream":
        lines = [f"[{event.get('stream', 'stream')}]"]
    elif kind == "result":
        lines = ["[result]"]
    elif kind == "error":
        lines = ["[error]"]
    elif kind == "warning":
        code = event.get("code")
        lines = [f"[warning{': ' + str(code) if code else ''}]"]
    else:
        lines = [json.dumps(event, ensure_ascii=False, separators=(",", ":"))]
        return lines
    if isinstance(text, str) and text:
        lines.append(text)
    for artifact in event.get("artifacts", []):
        path = artifact.get("path", "<unknown path>")
        mime = artifact.get("mime", "unknown type")
        lines.append(f"[artifact] {path} ({mime})")
    return lines


def _render_output_events(events: list[dict[str, Any]]) -> list[str]:
    lines = []
    stream = None
    chunks = []

    def flush_stream():
        nonlocal stream, chunks
        if stream is not None:
            lines.append(f"[{stream}]")
            text = "".join(chunks)
            if text:
                lines.append(text)
        stream = None
        chunks = []

    for event in events:
        if event.get("type") == "stream" and not event.get("artifacts"):
            current = event.get("stream", "stream")
            if stream is not None and stream != current:
                flush_stream()
            stream = current
            text = event.get("text", "")
            if isinstance(text, str):
                chunks.append(text)
            continue
        flush_stream()
        lines.extend(_render_output_event(event))
    flush_stream()
    return lines


def _render_inbox(inbox: dict[str, Any]) -> list[str]:
    lines = [
        f"inbox unacked={inbox.get('unacked', 0)} "
        f"has_more={str(bool(inbox.get('has_more', False))).lower()}"
    ]
    for message in inbox.get("messages", []):
        metadata = [f"id={message.get('id')}", f"from={message.get('from', '?')}"]
        if message.get("reply_to") is not None:
            metadata.append(f"reply_to={message['reply_to']}")
        if message.get("truncated"):
            metadata.append("truncated=true")
        lines.append(f"[{', '.join(metadata)}] {message.get('text', '')}")
    return lines


def _render_result(
    result: dict[str, Any], artifact_issues: list[tuple[str, str, str]] | None = None
) -> str:
    lines = []
    if "exec_id" not in result and result.get("client_id") is not None:
        lines.append(f"client_id={result['client_id']}")
    for key in (
        "exec_id",
        "state",
        "cursor",
        "has_more",
        "truncated",
    ):
        if key in result:
            value = result[key]
            if isinstance(value, bool):
                value = str(value).lower()
            lines.append(f"{key}={value}")
    if result.get("error"):
        lines.append("error: " + str(result["error"]))
    if result.get("error_truncated"):
        lines.append("error_truncated=true")
    if "runtime" in result and result["runtime"].get("instructions"):
        lines.extend(_render_runtime(result["runtime"]))
    output = result.get("output", [])
    lines.extend(_render_output_events(output))
    output_warnings = {
        (event.get("code"), event.get("text"))
        for event in output
        if event.get("type") == "warning"
    }
    artifact_warning_texts = set()
    for code, path, reason in artifact_issues or []:
        artifact_warning_texts.add(f"{path}: {reason}")
        label = "unavailable" if code == "artifact_unavailable" else "omitted"
        lines.append(f"inline image {label}: {path}: {reason}")
    for warning in result.get("warnings", []):
        code = warning.get("code")
        if (code, warning.get("text")) in output_warnings:
            continue
        if warning.get("text") in artifact_warning_texts:
            continue
        lines.append(f"warning{': ' + str(code) if code else ''}: {warning.get('text', '')}")
    if result.get("warnings_truncated"):
        lines.append("warnings_truncated=true")
    if isinstance(result.get("inbox"), dict):
        lines.extend(_render_inbox(result["inbox"]))
    return "\n".join(lines)


async def tool_result(result: dict[str, Any]) -> CallToolResult:
    started = time.perf_counter()
    result = dict(result)
    timing = dict(result.pop("_timing_ms", {}))
    warnings = list(result.get("warnings", []))
    images = []
    artifact_issues = []
    image_bytes = 0
    for event in result.get("output", []):
        for artifact in event.get("artifacts", []):
            artifact_path = artifact.get("path")
            mime = artifact.get("mime")
            if not isinstance(artifact_path, str):
                continue
            if mime not in {"image/png", "image/jpeg"}:
                continue
            path = Path(artifact_path)
            remaining = _IMAGE_RESPONSE_LIMIT - image_bytes
            if remaining <= 0:
                reason = "per-response inline image limit (2 MiB) reached"
                warning_code = "artifact_omitted"
            else:
                try:
                    raw = await asyncio.to_thread(_read_image, path, remaining)
                except OSError as exc:
                    reason = safe_error(exc, limit=256)
                    warning_code = "artifact_unavailable"
                else:
                    if len(raw) > remaining:
                        reason = "per-response inline image limit (2 MiB) exceeded"
                        warning_code = "artifact_omitted"
                    else:
                        encoded = await asyncio.to_thread(_encode_image, raw)
                        images.append(ImageContent(data=encoded, mimeType=mime))
                        image_bytes += len(raw)
                        continue
            artifact_issues.append((warning_code, str(path), reason))
            _append_artifact_warning(warnings, result, warning_code, path, reason)
    if warnings:
        result["warnings"] = warnings
    text = _render_result(result, artifact_issues)
    timing["render"] = round((time.perf_counter() - started) * 1000, 3)
    return CallToolResult(
        content=[TextContent(text=text), *images],
        structuredContent=result,
        isError=result.get("state") in {"failed", "lost"},
        _meta={"timing_ms": timing},
    )


def _append_artifact_warning(warnings, result, code, path, reason):
    if len(warnings) < 4:
        warnings.append({"code": code, "text": f"{path}: {reason}"})
    else:
        result["warnings_truncated"] = True


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
        except RPCError as exc:
            info = {
                "code": exc.code,
                "type": exc.error_type or type(exc).__name__,
                "message": str(exc),
                **({"operation": exc.operation} if exc.operation else {}),
                **({"details": exc.details} if exc.details else {}),
            }
            raise ToolError(
                json.dumps({"error": str(exc), "error_info": info}, ensure_ascii=False)
            ) from exc
        except RuntimeError as exc:
            raise ToolError(str(exc)) from exc

    @mcp.tool()
    async def init(
        client_id: Annotated[
            str | None,
            Field(
                description="Omit for a new readable ID; supply an ID to create or resume it. "
                "An ID cannot be shared by live connections or switched on this connection."
            ),
        ] = None,
    ) -> CallToolResult:
        """Start workspace work here: bind a client ID before execute.

        Returns the running manager's API instructions, capabilities, and versions.
        Repeated calls retain the current ID. Follow this runtime's guidance.
        """
        nonlocal bound_client
        result = await request("init", client_id=client_id)
        bound_client = result["client_id"]
        await bridge.bind_client(bound_client)
        return await tool_result(result)

    @mcp.tool()
    async def execute(
        code: Annotated[
            str,
            Field(
                description="Python cell with top-level await support. Use ws helpers for "
                "workspace work and ws.local for client-local values; print concise results."
            ),
        ],
        wait_ms: Annotated[
            int,
            Field(
                description="Milliseconds to wait for completion or inbox activity, not an "
                "execution timeout or total request deadline. Output alone does not end this "
                "wait; inspect state and has_more."
            ),
        ] = 1000,
        request_id: Annotated[
            str | None,
            Field(
                description="Optional deduplication key for this logical client. Same ID and "
                "identical code return the existing execution; different code is rejected. "
                "Omit for a new execution. Reuse does not rebuild state after restart."
            ),
        ] = None,
        max_bytes: Annotated[
            int | None,
            Field(
                description="Optional response output budget in bytes (1024..1048576). "
                "Omit to use the workspace default."
            ),
        ] = None,
    ) -> CallToolResult:
        """Read/edit files, search, run commands, and compose helpers in persistent Python.

        Call init first and follow its API instructions. Returns exec_id, state, output,
        cursor, and has_more. Use poll for pending cells or remaining output; do not
        resubmit code just because the tool wait ended. Confirm uncertain side effects
        before repeating a state-changing operation.
        """
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
            max_bytes=max_bytes,
        )
        return await tool_result(result)

    @mcp.tool()
    async def poll(
        exec_id: Annotated[
            str, Field(description="The submitted cell's exec_id returned by execute.")
        ],
        cursor: Annotated[
            int | None,
            Field(
                description="Use the cursor returned by the previous execute/poll to continue "
                "reading cell output. Omit to read from the beginning."
            ),
        ] = None,
        wait_ms: Annotated[
            int,
            Field(
                description="Milliseconds to wait for new output, completion, or inbox activity. "
                "Available output returns immediately. This is not an execution timeout or "
                "total request deadline."
            ),
        ] = 1000,
        max_bytes: Annotated[
            int | None,
            Field(
                description="Optional response output budget in bytes (1024..1048576). "
                "Omit to use the workspace default."
            ),
        ] = None,
    ) -> CallToolResult:
        """Continue a submitted cell without executing it again; available before init.

        Poll queued/running cells and keep reading while has_more, even after a terminal
        state. Use the returned cursor each time. Once terminal with no more output,
        evaluate the result/error. Python handles manage jobs started by a cell.
        """
        result = await request(
            "poll",
            exec_id=exec_id,
            cursor=cursor,
            wait_ms=wait_ms,
            max_bytes=max_bytes,
        )
        return await tool_result(result)

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


def _offline_status(workspace: Path) -> dict[str, Any]:
    root = workspace / ".mypr"
    record = read_startup_failure(root)
    if record is None:
        try:
            ConfigStore(workspace).load()
        except (ConfigError, OSError) as exc:
            write_startup_failure(root, exc, operation="config_validate")
            record = read_startup_failure(root)
    result: dict[str, Any] = {
        "workspace": str(workspace.resolve()),
        "healthy": False,
        "manager_available": False,
    }
    if record is not None:
        result["startup_error"] = record
    return result


def _global_config_path() -> Path:
    from .config import global_config_path

    return global_config_path()


def _config_store(workspace: Path, *, global_scope: bool):
    return ConfigStore(None if global_scope else workspace, global_path=_global_config_path())


def _json_value(raw: str) -> Any:
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"config value must be valid JSON: {exc.msg}") from exc


def _json_default(value):
    if hasattr(value, "__dataclass_fields__"):
        from dataclasses import asdict

        return asdict(value)
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"not JSON serializable: {type(value).__name__}")


def _print_json(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2, default=_json_default))


async def _config_explain_rpc(socket: Path, path: str) -> Any:
    return await rpc(socket, op="config", method="explain", path=path)


async def _config_command(workspace: Path, action: str, path: str | None, value: str | None,
                          *, global_scope: bool, all_managers: bool, force: bool) -> Any:
    if action not in {"get", "set", "unset", "explain", "reload"}:
        raise ValueError("config requires get, set, unset, explain, or reload")
    if all_managers and action != "reload":
        raise ValueError("--all is only valid with config reload")
    if action == "reload":
        if global_scope:
            raise ValueError("--global selects a file; use config reload --all to reload managers")
        if all_managers:
            return await _reload_all(_global_config_path(), force=force)
        found = await find_runtime(workspace)
        if found is None:
            raise RuntimeError("No reachable workspace manager")
        socket, _ = found
        return await rpc(socket, op="config", method="reload", force=force)
    scope = "global" if global_scope else "effective"
    store = _config_store(workspace, global_scope=global_scope)
    if action == "get":
        return store.get(path, scope=scope)
    if action == "set":
        if path is None or value is None:
            raise ValueError("config set requires PATH and JSON_VALUE")
        write_scope = "global" if global_scope else "workspace"
        snapshot = store.set(path, _json_value(value), scope=write_scope)
        return {"saved": True, "revision": snapshot.revision}
    if action == "unset":
        if path is None:
            raise ValueError("config unset requires PATH")
        write_scope = "global" if global_scope else "workspace"
        snapshot = store.unset(path, scope=write_scope)
        return {"saved": True, "revision": snapshot.revision}
    if path is None:
        raise ValueError("config explain requires PATH")
    disk = store.explain(path)
    if global_scope:
        return disk
    try:
        found = await find_runtime(workspace)
    except (OSError, ConnectionError, TimeoutError, RuntimeError) as exc:
        found = None
        disk["active_error"] = str(exc)
    if found is not None:
        socket, _ = found
        try:
            active = await _config_explain_rpc(socket, path)
        except (OSError, ConnectionError, TimeoutError, RuntimeError) as exc:
            disk["active_error"] = str(exc)
        else:
            disk["active"] = active
    return disk


def _registry_mismatch(record: dict[str, Any], state: dict[str, Any]) -> str | None:
    for key in ("workspace_id", "pid", "generation"):
        if state.get(key) != record.get(key):
            return f"manager identity mismatch: {key}"
    if state.get("healthy") is not True:
        return "manager is not healthy"
    capabilities = state.get("capabilities")
    if not isinstance(capabilities, list) or "config" not in capabilities:
        return "manager does not support config reload"
    if state.get("global_path") != record.get("global_path"):
        return "manager profile mismatch"
    return None


async def _reload_manager(record: dict[str, Any], *, force: bool) -> dict[str, Any]:
    result = {
        "workspace": record.get("workspace"),
        "socket": record.get("socket"),
        "generation": record.get("generation"),
    }
    try:
        socket = Path(record["socket"])
        async with asyncio.timeout(30):
            state = await rpc(socket, op="status")
            mismatch = (
                "manager returned invalid status"
                if not isinstance(state, dict)
                else _registry_mismatch(record, state)
            )
            if mismatch is not None:
                result.update(ok=False, status="unsupported", error=mismatch)
                return result
            reload_result = await rpc(
                socket, op="config", method="reload", force=force,
                generation=record["generation"],
            )
            if not isinstance(reload_result, dict):
                result.update(
                    ok=False,
                    status="error",
                    error="manager returned an invalid config reload result",
                )
                return result
            result["result"] = reload_result
            for key in ("applied", "deferred", "errors", "restart_required", "revision"):
                if key in reload_result:
                    result[key] = reload_result[key]
            if reload_result.get("errors"):
                result.update(
                    ok=False,
                    status="error",
                    error="configuration reload reported errors",
                )
            elif reload_result.get("deferred") or reload_result.get("restart_required"):
                result.update(
                    ok=False,
                    status="partial",
                    error="configuration reload has deferred changes",
                )
            else:
                result.update(ok=True, status="reloaded")
    except (OSError, ConnectionError, TimeoutError, RuntimeError, KeyError, TypeError) as exc:
        result.update(ok=False, status="error", error=str(exc))
    return result


async def _reload_all(global_path: Path, *, force: bool) -> dict[str, Any]:
    from .runtime_registry import list_managers

    records = list_managers(global_path)
    semaphore = asyncio.Semaphore(4)

    async def run(record):
        async with semaphore:
            return await _reload_manager(record, force=force)

    results = await asyncio.gather(*(run(record) for record in records))
    return {
        "global_path": str(global_path),
        "managers": results,
        "ok": all(item.get("ok") is True for item in results),
    }


def main():
    parser = argparse.ArgumentParser(description="Persistent workspace Python over MCP")
    parser.add_argument(
        "command",
        choices=[
            "serve",
            "status",
            "logs",
            "doctor",
            "reset",
            "restart",
            "stop",
            "config",
            "_manager",
        ],
    )
    parser.add_argument("config_action", nargs="?")
    parser.add_argument("config_path", nargs="?")
    parser.add_argument("config_value", nargs="?")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--global", dest="global_scope", action="store_true")
    parser.add_argument("--all", dest="all_managers", action="store_true")
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--follow", action="store_true")
    args = parser.parse_args()
    if args.command != "config" and (
        args.config_action is not None
        or args.config_path is not None
        or args.config_value is not None
        or args.global_scope
        or args.all_managers
    ):
        parser.error("config arguments require the config command")
    workspace = Path.cwd()
    if args.limit < 1:
        parser.error("--limit must be greater than zero")
    try:
        if args.command == "serve":
            asyncio.run(serve(workspace))
        elif args.command == "_manager":
            asyncio.run(run_manager(workspace))
        elif args.command == "config":
            result = asyncio.run(
                _config_command(
                    workspace,
                    args.config_action,
                    args.config_path,
                    args.config_value,
                    global_scope=args.global_scope,
                    all_managers=args.all_managers,
                    force=args.force,
                )
            )
            _print_json(result)
            if args.config_action == "reload" and (
                (args.all_managers and not result.get("ok", False))
                or result.get("errors") or result.get("deferred") or result.get("restart_required")
            ):
                raise SystemExit(1)
        else:

            async def admin():
                if args.command == "doctor":
                    return await doctor_workspace(workspace)
                if args.command == "logs":
                    return await logs(workspace, args.limit, args.follow)
                if args.command == "status":
                    found = await find_runtime(workspace)
                    if found is None:
                        return await asyncio.to_thread(_offline_status, workspace)
                    path, _ = found
                elif args.command == "reset":
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
    except (RuntimeError, OSError, TimeoutError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(1) from None
    except KeyboardInterrupt:
        return


if __name__ == "__main__":
    main()
