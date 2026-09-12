"""Workspace services used by the persistent Python kernel.

The classes in this module deliberately have a small, transport independent API.
The manager owns these objects and the kernel accesses them over its IPC layer.
"""

from __future__ import annotations

import asyncio
import codecs
import contextlib
import os
import signal
import tomllib
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp.client.streamable_http import streamable_http_client
from mcp.types import PaginatedRequestParams


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
        self.config = config
        self.workspace = workspace
        self.queue: asyncio.Queue[_Request | None] = asyncio.Queue()
        self.task: asyncio.Task[None] | None = None
        self.current: _Request | None = None
        self._closed = False

    async def request(self, method: str, args: dict[str, Any]) -> Any:
        if self._closed:
            raise RuntimeError("MCP connection is closed")
        if self.task is None or self.task.done():
            self.task = asyncio.create_task(self._owner())
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        await self.queue.put(_Request(method, args, future))
        return await future

    async def close(self) -> None:
        self._closed = True
        if self.task is None:
            return
        error = RuntimeError("MCP connection closed")
        while not self.queue.empty():
            request = self.queue.get_nowait()
            if request is not None and not request.future.done():
                request.future.set_exception(error)
        self.task.cancel()
        await asyncio.gather(self.task, return_exceptions=True)
        self.task = None

    async def _owner(self) -> None:
        try:
            async with contextlib.AsyncExitStack() as stack:
                session = await self._open(stack)
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
                        if request.future.cancelled():
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
        except Exception as exc:
            while not self.queue.empty():
                request = self.queue.get_nowait()
                if request is not None and not request.future.done():
                    request.future.set_exception(exc)
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
        self.config = self._read_config()
        self._connections: dict[str, _MCPConnection] = {}
        self._lock = asyncio.Lock()
        self._closed = False

    async def dispatch(self, method: str, args: dict[str, Any] | None = None) -> Any:
        if self._closed:
            raise RuntimeError("MCP bridge is closed")
        args = dict(args or {})
        if method == "list_servers":
            return _page_servers(self.config, args)
        server_name = args.pop("server", args.pop("server_name", None))
        if not isinstance(server_name, str) or server_name not in self.config:
            raise ValueError(f"unknown MCP server: {server_name!r}")
        connection = await self._connection(server_name)
        return await connection.request(method, args)

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        await asyncio.gather(
            *(connection.close() for connection in self._connections.values()),
            return_exceptions=True,
        )
        self._connections.clear()

    async def _connection(self, name: str) -> _MCPConnection:
        async with self._lock:
            connection = self._connections.get(name)
            if connection is None:
                connection = _MCPConnection(self.config[name], self.workspace)
                self._connections[name] = connection
            return connection

    def _read_config(self) -> dict[str, dict[str, Any]]:
        path = self.workspace / ".mypr" / "config.toml"
        if not path.exists():
            return {}
        with path.open("rb") as file:
            raw = tomllib.load(file)
        servers = raw.get("mcp", {}).get("servers", {})
        if not isinstance(servers, dict):
            raise ValueError("[mcp.servers] must be a table")
        return {
            str(name): dict(config) for name, config in servers.items() if isinstance(config, dict)
        }


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
