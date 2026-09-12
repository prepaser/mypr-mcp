"""Workspace services used by the persistent Python kernel.

The classes in this module deliberately have a small, transport independent API.
The manager owns these objects and the kernel accesses them over its IPC layer.
"""

from __future__ import annotations

import asyncio
import codecs
import contextlib
import copy
import os
import signal
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp.client.streamable_http import streamable_http_client
from mcp.types import PaginatedRequestParams

from .config import MCPConfig, validate_name, validate_servers


@dataclass
class _Job:
    id: str
    process: asyncio.subprocess.Process
    group_id: int
    output_limit: int
    state: str = "running"
    output: list[dict[str, str]] = field(default_factory=list)
    output_bytes: int = 0
    cursor: int = 0
    truncated: bool = False
    error: str | None = None
    result: dict[str, int] | None = None
    readers: list[asyncio.Task[None]] = field(default_factory=list)
    waiter: asyncio.Task[None] | None = None


class Shells:
    """Run and supervise workspace commands in their own process groups."""

    def __init__(self, workspace: Path, output_limit: int = 16 * 1024 * 1024):
        self.workspace = Path(workspace).resolve()
        self.output_limit = max(0, int(output_limit))
        self._jobs: dict[str, _Job] = {}
        self._closed = False
        self._lock = asyncio.Lock()

    @property
    def active(self) -> list[str]:
        return [job.id for job in self._jobs.values() if job.state in {"running", "cancelling"}]

    @property
    def count(self) -> int:
        return len(self.active)

    async def start(
        self,
        command: str | list[str],
        cwd: str | None = None,
        env: dict[str, str] | None = None,
    ) -> dict[str, str]:
        if self._closed:
            raise RuntimeError("shell service is closed")
        workdir = self._cwd(cwd)
        merged_env = (
            os.environ.copy()
            if env is None
            else {str(key): str(value) for key, value in env.items()}
        )
        try:
            if isinstance(command, str):
                process = await asyncio.create_subprocess_shell(
                    command,
                    executable="/bin/sh",
                    cwd=workdir,
                    env=merged_env,
                    stdin=asyncio.subprocess.DEVNULL,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    start_new_session=True,
                )
            elif command:
                process = await asyncio.create_subprocess_exec(
                    *map(str, command),
                    cwd=workdir,
                    env=merged_env,
                    stdin=asyncio.subprocess.DEVNULL,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    start_new_session=True,
                )
            else:
                raise ValueError("command must not be empty")
        except (OSError, ValueError) as exc:
            job_id = uuid.uuid4().hex
            job = _Job(job_id, _NoProcess(), 0, self.output_limit, state="failed", error=str(exc))
            job.result = {"returncode": -1}
            self._jobs[job_id] = job
            return {"id": job_id}

        job_id = uuid.uuid4().hex
        job = _Job(job_id, process, process.pid, self.output_limit)
        self._jobs[job_id] = job
        assert process.stdout is not None and process.stderr is not None
        job.readers = [
            asyncio.create_task(self._drain(job, process.stdout, "stdout")),
            asyncio.create_task(self._drain(job, process.stderr, "stderr")),
        ]
        job.waiter = asyncio.create_task(self._wait(job))
        return {"id": job_id}

    async def poll(self, job_id: str, cursor: int = 0) -> dict[str, Any]:
        job = self._jobs.get(job_id)
        if job is None:
            return {
                "id": job_id,
                "state": "unknown",
                "output": [],
                "cursor": cursor,
                "result": None,
                "error": "unknown job",
            }
        cursor = int(cursor)
        if cursor < 0 or cursor > len(job.output):
            raise ValueError("invalid shell output cursor")
        output = job.output[cursor:]
        return {
            "id": job.id,
            "state": job.state,
            "output": output,
            "cursor": len(job.output),
            "result": job.result,
            "error": job.error,
            "truncated": job.truncated,
        }

    async def cancel(self, job_id: str) -> dict[str, Any]:
        job = self._jobs.get(job_id)
        if job is None:
            return {"id": job_id, "state": "unknown", "error": "unknown job"}
        if job.state not in {"running", "cancelling"}:
            return {"id": job.id, "state": job.state, "result": job.result, "error": job.error}
        job.state = "cancelling"
        self._signal_group(job, signal.SIGTERM)
        killer = asyncio.create_task(self._kill_group_later(job))
        try:
            await asyncio.wait_for(asyncio.shield(job.waiter), timeout=2.0)
        except TimeoutError:
            if job.waiter is not None:
                await asyncio.shield(job.waiter)
        finally:
            if not killer.done() and not self._group_has_live_members(job.group_id):
                killer.cancel()
            await asyncio.gather(killer, return_exceptions=True)
        return await self.poll(job.id, len(job.output))

    async def _kill_group_later(self, job: _Job) -> None:
        await asyncio.sleep(2)
        if self._group_has_live_members(job.group_id):
            self._signal_group(job, signal.SIGKILL)

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        await asyncio.gather(
            *(self.cancel(job.id) for job in self._jobs.values()), return_exceptions=True
        )
        await asyncio.gather(
            *(task for job in self._jobs.values() for task in job.readers), return_exceptions=True
        )

    def _cwd(self, cwd: str | None) -> str:
        if cwd is None:
            return str(self.workspace)
        path = Path(cwd)
        return str(path if path.is_absolute() else self.workspace / path)

    async def _drain(self, job: _Job, stream: asyncio.StreamReader, name: str) -> None:
        decoder = codecs.getincrementaldecoder("utf-8")("replace")
        while True:
            try:
                chunk = await stream.read(64 * 1024)
            except ConnectionError, OSError:
                return
            if not chunk:
                break
            if job.output_bytes >= job.output_limit:
                job.truncated = True
                continue
            keep = chunk[: job.output_limit - job.output_bytes]
            job.output_bytes += len(keep)
            if keep:
                text = decoder.decode(keep)
                if text:
                    job.output.append({"stream": name, "text": text})
            if len(keep) < len(chunk):
                job.truncated = True
        text = decoder.decode(b"", final=True)
        if text and job.output_bytes < job.output_limit:
            job.output.append({"stream": name, "text": text})

    async def _wait(self, job: _Job) -> None:
        try:
            returncode = await job.process.wait()
        except (OSError, ProcessLookupError) as exc:
            job.state = "failed"
            job.error = str(exc)
            job.result = {"returncode": -1}
            return
        await asyncio.gather(*job.readers, return_exceptions=True)
        tick = asyncio.Event()
        while self._group_has_live_members(job.group_id):
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(tick.wait(), 0.05)
        job.result = {"returncode": returncode}
        if job.state == "cancelling":
            job.state = "cancelled"
        else:
            job.state = "succeeded" if returncode == 0 else "failed"

    @staticmethod
    def _signal_group(job: _Job, sig: signal.Signals) -> None:
        if not job.group_id:
            return
        try:
            os.killpg(job.group_id, sig)
        except ProcessLookupError:
            pass
        except PermissionError as exc:
            job.error = str(exc)

    @staticmethod
    def _group_exists(group_id: int) -> bool:
        if not group_id:
            return False
        try:
            os.killpg(group_id, 0)
        except ProcessLookupError, PermissionError:
            return False
        return True

    @staticmethod
    def _group_has_live_members(group_id: int) -> bool:
        if not group_id:
            return False
        try:
            entries = Path("/proc").iterdir()
        except OSError:
            return Shells._group_exists(group_id)
        for entry in entries:
            if not entry.name.isdecimal():
                continue
            try:
                stat = (entry / "stat").read_text(encoding="ascii")
                _, rest = stat.rsplit(") ", 1)
                fields = rest.split()
                if fields[2] == str(group_id) and fields[0] not in {"Z", "X"}:
                    return True
            except OSError, ValueError, IndexError:
                continue
        return False


class _NoProcess:
    pid = -1

    async def wait(self) -> int:
        return -1


@dataclass
class _Request:
    method: str
    args: dict[str, Any]
    future: asyncio.Future[Any]


class _MCPConnection:
    def __init__(self, config: dict[str, Any], workspace: Path):
        self.config = copy.deepcopy(config)
        self.workspace = workspace
        self.queue: asyncio.Queue[_Request | None] = asyncio.Queue()
        self.task: asyncio.Task[None] | None = None
        self.current: _Request | None = None
        self._closed = False
        self._admissions_blocked = False
        self._initializing = False
        self._initialized = False
        self._ready = asyncio.Event()
        self._ready_error: BaseException | None = None

    @property
    def busy(self) -> bool:
        """Whether initialization or any admitted request is in progress."""

        return self._initializing or self.current is not None or not self.queue.empty()

    @property
    def connected(self) -> bool:
        return self._initialized and not self._closed

    def block_admissions(self) -> None:
        self._admissions_blocked = True

    def unblock_admissions(self) -> None:
        if not self._closed:
            self._admissions_blocked = False

    def admit(self, method: str, args: dict[str, Any]) -> asyncio.Future[Any]:
        if self._closed:
            raise RuntimeError("MCP connection is closed")
        if self._admissions_blocked:
            raise RuntimeError("MCP connection is being reconfigured")
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        if self.task is None or self.task.done():
            self.task = asyncio.create_task(self._owner())
        self.queue.put_nowait(_Request(method, args, future))
        return future

    async def request(self, method: str, args: dict[str, Any]) -> Any:
        return await self.admit(method, args)

    async def ensure_ready(self, timeout_seconds: float = 30.0) -> None:
        if self._closed:
            raise RuntimeError("MCP connection is closed")
        if self.task is None or self.task.done():
            self.task = asyncio.create_task(self._owner())
        try:
            await asyncio.wait_for(self._ready.wait(), timeout_seconds)
        except TimeoutError as exc:
            raise TimeoutError("MCP connection initialization timed out") from exc
        if self._ready_error is not None:
            raise RuntimeError(f"MCP connection initialization failed: {self._ready_error}") from (
                self._ready_error if isinstance(self._ready_error, Exception) else None
            )

    async def close(self, force: bool = True) -> None:
        if not force and self.busy:
            raise RuntimeError("MCP connection has active requests")
        self._closed = True
        self._admissions_blocked = True
        error = RuntimeError("MCP connection closed by reconfiguration")
        if self.task is None:
            return
        while not self.queue.empty():
            request = self.queue.get_nowait()
            if request is not None and not request.future.done():
                request.future.set_exception(error)
        self.task.cancel()
        await asyncio.gather(self.task, return_exceptions=True)
        self.task = None

    async def _owner(self) -> None:
        self._ready.clear()
        self._ready_error = None
        self._initialized = False
        try:
            async with contextlib.AsyncExitStack() as stack:
                self._initializing = True
                try:
                    session = await self._open(stack)
                finally:
                    self._initializing = False
                self._initialized = True
                self._ready.set()
                while True:
                    request = await self.queue.get()
                    if request is None:
                        break
                    if request.future.cancelled():
                        continue
                    self.current = request
                    operation = asyncio.create_task(
                        _session_dispatch(session, request.method, request.args)
                    )

                    def cancel_operation(
                        future: asyncio.Future[Any], operation: asyncio.Task[Any] = operation
                    ) -> None:
                        if future.cancelled() and not operation.done():
                            operation.cancel()

                    request.future.add_done_callback(cancel_operation)
                    try:
                        result = await operation
                    except asyncio.CancelledError:
                        if request.future.cancelled() and not self._closed:
                            continue
                        if not request.future.done():
                            request.future.set_exception(RuntimeError("MCP connection closed"))
                        raise
                    except Exception as exc:
                        if not request.future.done():
                            request.future.set_exception(exc)
                    else:
                        if not request.future.done():
                            request.future.set_result(result)
                    finally:
                        self.current = None
        except BaseException as exc:
            self._initialized = False
            if self._closed:
                error = RuntimeError("MCP connection closed by reconfiguration")
            elif isinstance(exc, Exception):
                error = exc
            else:
                error = RuntimeError("MCP connection interrupted")
            self._ready_error = error
            self._ready.set()
            while not self.queue.empty():
                request = self.queue.get_nowait()
                if request is not None and not request.future.done():
                    request.future.set_exception(error)
        finally:
            if self.current is not None and not self.current.future.done():
                self.current.future.set_exception(RuntimeError("MCP connection closed"))
            self.current = None
            while not self.queue.empty():
                request = self.queue.get_nowait()
                if request is not None and not request.future.done():
                    request.future.set_exception(RuntimeError("MCP connection closed"))

    async def _open(self, stack: contextlib.AsyncExitStack) -> ClientSession:
        config = self.config
        if "url" in config:
            import httpx2

            headers = _mapped_environment(config.get("headers_from", {}), "headers_from")
            client = await stack.enter_async_context(httpx2.AsyncClient(headers=headers))
            streams = await stack.enter_async_context(
                streamable_http_client(config["url"], http_client=client)
            )
        else:
            command = config.get("command")
            args = [str(item) for item in config.get("args", [])]
            if isinstance(command, list):
                command, args = command[0], [*map(str, command[1:]), *args]
            if not isinstance(command, str) or not command:
                raise ValueError("MCP server command must be a non-empty string")
            params = StdioServerParameters(
                command=command,
                args=args,
                env=_mapped_environment(config.get("env_from", {}), "env_from"),
                cwd=_config_cwd(config.get("cwd"), self.workspace),
            )
            streams = await stack.enter_async_context(stdio_client(params))
        read_stream, write_stream = streams
        session = await stack.enter_async_context(ClientSession(read_stream, write_stream))
        await session.initialize()
        return session


class MCPBridge:
    """Lazy, serialized client connections to configured MCP servers."""

    def __init__(self, workspace: Path):
        self.workspace = Path(workspace).resolve()
        self.store = MCPConfig(self.workspace)
        self.config, self._revision = self.store.load()
        self.config = _copy_configs(self.config)
        self._connections: dict[str, _MCPConnection] = {}
        self._lock = asyncio.Lock()
        self._mutation_lock = asyncio.Lock()
        self._changing: set[str] = set()
        self._closed = False

    async def dispatch(self, method: str, args: dict[str, Any] | None = None) -> Any:
        if self._closed:
            raise RuntimeError("MCP bridge is closed")
        args = dict(args or {})
        if method == "list_servers":
            return _page_servers(self.config, args)
        if method == "get_config":
            return self.get_config(args.get("server", args.get("server_name")))
        if method == "configure":
            return await self.configure(
                args.get("server", args.get("server_name")),
                args.get("config"),
                force=bool(args.get("force", False)),
            )
        if method == "remove":
            return await self.remove(
                args.get("server", args.get("server_name")), force=bool(args.get("force", False))
            )
        if method == "restart":
            return await self.restart(
                args.get("server", args.get("server_name")), force=bool(args.get("force", False))
            )
        if method == "reload":
            return await self.reload(force=bool(args.get("force", False)))
        server_name = args.pop("server", args.pop("server_name", None))
        if not isinstance(server_name, str) or server_name not in self.config:
            raise ValueError(f"unknown MCP server: {server_name!r}")
        async with self._lock:
            if self._closed:
                raise RuntimeError("MCP bridge is closed")
            if server_name in self._changing:
                raise RuntimeError("MCP server is being reconfigured")
            connection = self._connection(server_name)
            future = connection.admit(method, args)
        return await future

    def get_config(self, server: str) -> dict[str, Any]:
        self._check_server_name(server)
        if server not in self.config:
            raise ValueError(f"unknown MCP server: {server!r}")
        return copy.deepcopy(self.config[server])

    async def configure(
        self, server: str, config: dict[str, Any], force: bool = False
    ) -> dict[str, Any]:
        validate_name(server)
        config = validate_servers({server: config})[server]
        async with self._mutation_lock:
            self._ensure_open()
            old = self.config.get(server)
            action = "added" if old is None else "unchanged" if old == config else "updated"
            if action == "unchanged":
                _, revision = self.store.load()
                if revision != self._revision:
                    raise RuntimeError(
                        "MCP configuration changed on disk; call ws.mcp.reload() first"
                    )
                connected = self._connections.get(server)
                return {
                    "server": server,
                    "action": action,
                    "connected": bool(connected and connected.connected),
                }
            try:
                connection = await self._block_affected({server}, force)
                servers = _copy_configs(self.config)
                servers[server] = copy.deepcopy(config)
                revision = self.store.save(servers, self._revision)
                await self._close_connections(connection, force)
                async with self._lock:
                    self.config = servers
                    self._revision = revision
                    self._connections.pop(server, None)
                return {"server": server, "action": action, "connected": False}
            finally:
                await self._unblock(connection if "connection" in locals() else {})
                await self._clear_changing({server})

    async def remove(self, server: str, force: bool = False) -> dict[str, Any]:
        self._check_server_name(server)
        async with self._mutation_lock:
            self._ensure_open()
            if server not in self.config:
                raise ValueError(f"unknown MCP server: {server!r}")
            try:
                connection = await self._block_affected({server}, force)
                servers = _copy_configs(self.config)
                del servers[server]
                revision = self.store.save(servers, self._revision)
                await self._close_connections(connection, force)
                async with self._lock:
                    self.config = servers
                    self._revision = revision
                    self._connections.pop(server, None)
                return {
                    "server": server,
                    "action": "removed",
                    "removed": True,
                    "connected": False,
                }
            finally:
                await self._unblock(connection if "connection" in locals() else {})
                await self._clear_changing({server})

    async def restart(self, server: str, force: bool = False) -> dict[str, Any]:
        self._check_server_name(server)
        async with self._mutation_lock:
            self._ensure_open()
            if server not in self.config:
                raise ValueError(f"unknown MCP server: {server!r}")
            try:
                affected = await self._block_affected({server}, force)
                await self._close_connections(affected, force)
                connection = _MCPConnection(self.config[server], self.workspace)
                connection.block_admissions()
                async with self._lock:
                    self._connections[server] = connection
                await connection.ensure_ready()
                connection.unblock_admissions()
                return {
                    "server": server,
                    "action": "restarted",
                    "restarted": True,
                    "connected": True,
                }
            except Exception:
                if "connection" in locals():
                    await connection.close()
                    async with self._lock:
                        if self._connections.get(server) is connection:
                            self._connections.pop(server, None)
                raise
            finally:
                await self._clear_changing({server})

    async def reload(self, force: bool = False) -> dict[str, list[str]]:
        async with self._mutation_lock:
            self._ensure_open()
            servers, revision = self.store.load()
            servers = _copy_configs(servers)
            current = self.config
            added = sorted(set(servers) - set(current))
            removed = sorted(set(current) - set(servers))
            updated = sorted(
                name for name in set(servers) & set(current) if servers[name] != current[name]
            )
            changed = set(added) | set(removed) | set(updated)
            try:
                connections = await self._block_affected(changed, force)
                await self._close_connections(connections, force)
                async with self._lock:
                    self.config = servers
                    self._revision = revision
                    for name in removed + updated:
                        self._connections.pop(name, None)
                return {"added": added, "updated": updated, "removed": removed}
            finally:
                await self._clear_changing(changed)

    async def close(self) -> None:
        async with self._mutation_lock:
            if self._closed:
                return
            self._closed = True
            async with self._lock:
                connections = dict(self._connections)
                self._changing.update(connections)
                for connection in connections.values():
                    connection.block_admissions()
            await asyncio.gather(
                *(connection.close() for connection in connections.values()),
                return_exceptions=True,
            )
            async with self._lock:
                self._connections.clear()
                self._changing.clear()

    def _connection(self, name: str) -> _MCPConnection:
        connection = self._connections.get(name)
        if connection is None:
            connection = _MCPConnection(self.config[name], self.workspace)
            self._connections[name] = connection
        return connection

    async def _block_affected(self, names: set[str], force: bool) -> dict[str, _MCPConnection]:
        async with self._lock:
            self._ensure_open()
            affected = {
                name: connection for name, connection in self._connections.items() if name in names
            }
            busy = [name for name, connection in affected.items() if connection.busy]
            if busy and not force:
                raise RuntimeError(
                    "MCP servers have active requests; pass force=True: " + ", ".join(sorted(busy))
                )
            for connection in affected.values():
                connection.block_admissions()
            self._changing.update(names)
            return affected

    async def _unblock(self, connections: dict[str, _MCPConnection]) -> None:
        async with self._lock:
            for connection in connections.values():
                connection.unblock_admissions()

    async def _clear_changing(self, names: set[str]) -> None:
        async with self._lock:
            self._changing.difference_update(names)

    @staticmethod
    async def _close_connections(connections: dict[str, _MCPConnection], force: bool) -> None:
        await asyncio.gather(
            *(connection.close(force=force) for connection in connections.values()),
            return_exceptions=False,
        )

    @staticmethod
    def _check_server_name(server: str) -> None:
        validate_name(server)

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("MCP bridge is closed")


def _copy_configs(config: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return copy.deepcopy(config)


async def _session_dispatch(
    session: ClientSession, method: str, args: dict[str, Any]
) -> dict[str, Any]:
    cursor = args.get("cursor")
    params = PaginatedRequestParams(cursor=cursor) if cursor is not None else None
    if method == "list_tools":
        result = await session.list_tools(params=params)
    elif method == "list_resources":
        result = await session.list_resources(params=params)
    elif method == "list_prompts":
        result = await session.list_prompts(params=params)
    elif method == "call_tool":
        result = await session.call_tool(args["name"], args.get("arguments"))
    elif method == "read_resource":
        result = await session.read_resource(args["uri"])
    elif method == "get_prompt":
        result = await session.get_prompt(args["name"], args.get("arguments"))
    else:
        raise ValueError(f"unsupported MCP method: {method}")
    return _json_model(result)


def _json_model(value: Any) -> dict[str, Any]:
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json", by_alias=True, exclude_none=False)
    if isinstance(value, dict):
        return value
    raise TypeError(f"MCP result is not serializable: {type(value).__name__}")


def _mapped_environment(mapping: Any, field_name: str) -> dict[str, str]:
    if mapping is None:
        return {}
    if not isinstance(mapping, dict):
        raise ValueError(f"{field_name} must be a table")
    result = {}
    for target, source in mapping.items():
        if not isinstance(source, str):
            raise ValueError(f"{field_name}.{target} must name an environment variable")
        value = os.environ.get(source)
        if value is None:
            raise RuntimeError(f"environment variable {source!r} is not set")
        result[str(target)] = value
    return result


def _config_cwd(cwd: Any, workspace: Path) -> str:
    if cwd is None:
        return str(workspace)
    path = Path(str(cwd))
    return str(path if path.is_absolute() else workspace / path)


def _page_servers(config: dict[str, dict[str, Any]], args: dict[str, Any]) -> dict[str, Any]:
    names = sorted(config)
    start = int(args.get("cursor", 0) or 0)
    limit = max(1, min(int(args.get("limit", 50) or 50), 1000))
    page = names[start : start + limit]
    end = start + len(page)
    return {
        "servers": [
            {"name": name, "transport": "http" if "url" in config[name] else "stdio"}
            for name in page
        ],
        "next_cursor": str(end) if end < len(names) else None,
    }
