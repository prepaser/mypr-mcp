"""The API injected into a workspace's persistent IPython kernel."""

from __future__ import annotations

import asyncio
import contextvars
import inspect
import io
import json
import os
import re
import shlex
import time
from collections import deque
from collections.abc import Awaitable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .diagnostics import safe_error, safe_error_details

_output_buffer: contextvars.ContextVar[OutputBuffer | None] = contextvars.ContextVar(
    "mypr_task_output", default=None
)
_cell_output: contextvars.ContextVar[OutputBuffer | None] = contextvars.ContextVar(
    "mypr_cell_output", default=None
)
_client_context: contextvars.ContextVar[ClientInfo | None] = contextvars.ContextVar(
    "mypr_client_context", default=None
)
_exec_context: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "mypr_exec_context", default=None
)
RPC_LIMIT = 32 * 1024 * 1024
HISTORY_OUTPUT_LIMIT = 64 * 1024
try:
    OUTPUT_LIMIT = max(1024, int(os.environ.get("MYPR_OUTPUT_LIMIT", 16 * 1024 * 1024)))
except ValueError:
    OUTPUT_LIMIT = 16 * 1024 * 1024
try:
    COMPLETED_TASKS = max(0, int(os.environ.get("MYPR_COMPLETED_TASKS", "128")))
except ValueError:
    COMPLETED_TASKS = 128

_HEX_ID = re.compile(r"^[0-9a-f]{32}$")
_TASK_ID = re.compile(r"^task-.+-\d+$")


def _reserved_generated_id(value: str) -> bool:
    return bool(
        _HEX_ID.fullmatch(value) or _TASK_ID.fullmatch(value) or value.startswith("remote-watch:")
    )


class NotReady(RuntimeError):
    """Raised when a task result is requested before it has finished."""


class RPCError(RuntimeError):
    """An error returned by the workspace manager."""


class ResetRequested(BaseException):
    """Used to stop the current IPython cell after a kernel reset request."""


@dataclass(frozen=True, slots=True)
class ClientInfo:
    """Immutable identity of the client that submitted the current cell."""

    id: str
    connection_id: str | None = None


def _client_from_metadata(metadata: Mapping[str, Any] | None) -> ClientInfo | None:
    if not metadata:
        return None
    client_id = metadata.get("client_id")
    if client_id is None:
        return None
    return ClientInfo(
        id=str(client_id),
        connection_id=(
            str(metadata["connection_id"]) if metadata.get("connection_id") is not None else None
        ),
    )


@contextmanager
def execution_context(metadata: Mapping[str, Any] | None):
    """Install an execution identity; child asyncio tasks inherit it."""

    client_token = _client_context.set(_client_from_metadata(metadata))
    exec_token = _exec_context.set(
        str(metadata["exec_id"]) if metadata and metadata.get("exec_id") is not None else None
    )
    try:
        yield
    finally:
        _exec_context.reset(exec_token)
        _client_context.reset(client_token)


class OutputBuffer:
    def __init__(self, limit: int = OUTPUT_LIMIT) -> None:
        self.limit = limit
        self._text = io.StringIO()
        self.size = 0
        self.truncated = False

    def write(self, value: str) -> int:
        if not value:
            return 0
        remaining = self.limit - self.size
        if remaining <= 0:
            self.truncated = True
            return len(value)
        raw = value.encode("utf-8", "replace")
        if len(raw) > remaining:
            kept = raw[:remaining].decode("utf-8", "ignore")
            self._text.write(kept)
            self.size += len(kept.encode("utf-8"))
            self.truncated = True
        else:
            self._text.write(raw.decode("utf-8"))
            self.size += len(raw)
        return len(value)

    def get(self) -> str:
        return self._text.getvalue()


class MultiplexStream:
    """Send output from a background task to its buffer, preserving kernel output."""

    def __init__(self, stream: Any) -> None:
        self.stream = stream

    def write(self, value: str) -> int:
        buffer = _output_buffer.get()
        if buffer is not None:
            return buffer.write(value)
        cell = _cell_output.get()
        if cell is not None:
            cell.write(value)
        return self.stream.write(value)

    def flush(self) -> None:
        buffer = _output_buffer.get()
        if buffer is None:
            self.stream.flush()

    def isatty(self) -> bool:
        return bool(getattr(self.stream, "isatty", lambda: False)())

    @property
    def encoding(self) -> str | None:
        return getattr(self.stream, "encoding", None)

    def fileno(self) -> int:
        return self.stream.fileno()

    def __getattr__(self, name: str) -> Any:
        return getattr(self.stream, name)


def _bounded_history_output(value: str) -> str:
    raw = value.encode("utf-8", "replace")
    if len(raw) <= HISTORY_OUTPUT_LIMIT:
        return raw.decode("utf-8")
    return raw[:HISTORY_OUTPUT_LIMIT].decode("utf-8", "ignore")


async def _rpc(op: str, **fields: Any) -> Any:
    socket_name = os.environ.get("MYPR_SOCKET")
    if not socket_name:
        raise RPCError("MYPR_SOCKET is not configured")
    payload = {"op": op, **fields}
    client = _client_context.get()
    if client is not None:
        payload.update(
            {
                "client_id": client.id,
                "connection_id": client.connection_id,
            }
        )
    exec_id = _exec_context.get()
    if exec_id is not None:
        payload["exec_id"] = exec_id
    generation = os.environ.get("MYPR_GENERATION")
    if generation is not None:
        try:
            payload["generation"] = int(generation)
        except ValueError:
            payload["generation"] = generation
    try:
        reader, writer = await asyncio.open_unix_connection(socket_name, limit=RPC_LIMIT)
        try:
            writer.write(json.dumps(payload, separators=(",", ":"), default=str).encode() + b"\n")
            await writer.drain()
            line = await reader.readline()
        finally:
            writer.close()
            await writer.wait_closed()
    except (OSError, asyncio.IncompleteReadError) as exc:
        raise RPCError(f"workspace manager unavailable: {exc}") from exc
    if not line:
        raise RPCError("workspace manager closed the RPC connection")
    try:
        response = json.loads(line)
    except json.JSONDecodeError as exc:
        raise RPCError("invalid response from workspace manager") from exc
    if not response.get("ok", False):
        raise RPCError(str(response.get("error", "workspace manager request failed")))
    return response.get("result")


class TaskHandle:
    def __init__(
        self,
        task_id: str,
        task: asyncio.Task[Any],
        output: OutputBuffer,
        source: Awaitable[Any] | None = None,
        run_state: dict[str, bool] | None = None,
    ) -> None:
        self.id = task_id
        self._task = task
        self._buffer = output
        self._source = source
        self._run_state = run_state or {"started": True}
        self._created = time.time()
        self._finished_at: float | None = None
        self._cancel_requested = False
        self._client = _client_context.get()
        self._exec_id = _exec_context.get()
        self._task.add_done_callback(self._finished)

    def _finished(self, task: asyncio.Task[Any]) -> None:
        self._finished_at = time.time()
        if task.cancelled():
            if not self._run_state["started"] and inspect.iscoroutine(self._source):
                self._source.close()
        else:
            task.exception()

    def status(self) -> dict[str, Any]:
        if self._task.cancelled():
            state = "cancelled"
        elif not self._task.done():
            state = "cancelling" if self._cancel_requested else "running"
        elif self._task.exception() is not None:
            state = "failed"
        else:
            state = "succeeded"
        result = {
            "id": self.id,
            "status": state,
            "created_at": self._created,
            "client_id": self._client.id if self._client else None,
            "connection_id": self._client.connection_id if self._client else None,
            "exec_id": self._exec_id,
        }
        if self._finished_at is not None:
            result["finished_at"] = self._finished_at
        return result

    def output(self, cursor: int | None = None) -> str | dict[str, Any]:
        text = self._buffer.get()
        if cursor is not None:
            text = text[cursor:]
            return {
                "output": text,
                "cursor": cursor + len(text),
                "truncated": self._buffer.truncated,
            }
        return text

    def result(self) -> Any:
        if not self._task.done():
            raise NotReady(f"task {self.id} is still running")
        if self._task.cancelled():
            raise asyncio.CancelledError
        return self._task.result()

    async def cancel(self) -> bool:
        if self._task.done():
            return False
        self._cancel_requested = True
        self._task.cancel()
        return True

    async def _report(self) -> None:
        """Publish task lifecycle without exposing the awaitable or its arguments."""

        identity = {
            "id": self.id,
            "created": self._created,
            "client_id": self._client.id if self._client else None,
            "connection_id": self._client.connection_id if self._client else None,
            "exec_id": self._exec_id,
            "kind": "python",
        }

        async def publish(state: str, **extra: Any) -> bool:
            event = {**identity, "state": state, **extra}
            try:
                await _rpc("task_event", event=event)
                return True
            except Exception:
                # Reporting must never alter the task's result or lifetime.
                return False

        async def publish_terminal(state: str, **extra: Any) -> None:
            event = {**identity, "state": state, **extra}
            if await publish(state, **extra):
                return
            try:
                await _rpc("task_terminal", event=event)
            except Exception:
                pass

        if not self._task.done():
            await publish("running")
        cursor = 0
        while True:
            text = self.output()
            while cursor < len(text):
                chunk = text[cursor : cursor + 8192]
                await publish("running", output_delta=chunk)
                cursor += len(chunk)
            if self._task.done():
                break
            await asyncio.wait({self._task}, timeout=0.25)
        try:
            await self._task
        except asyncio.CancelledError:
            await publish_terminal(
                "cancelled",
                finished=time.time(),
                output=_bounded_history_output(self.output()),
                output_delta="",
                output_truncated=self._buffer.truncated or self._buffer.size > HISTORY_OUTPUT_LIMIT,
            )
        except BaseException as exc:
            error, error_truncated = safe_error_details(exc)
            await publish_terminal(
                "failed",
                finished=time.time(),
                error=error,
                error_truncated=error_truncated,
                output=_bounded_history_output(self.output()),
                output_delta="",
                output_truncated=self._buffer.truncated or self._buffer.size > HISTORY_OUTPUT_LIMIT,
            )
        else:
            await publish_terminal(
                "succeeded",
                finished=time.time(),
                output=_bounded_history_output(self.output()),
                output_delta="",
                output_truncated=self._buffer.truncated or self._buffer.size > HISTORY_OUTPUT_LIMIT,
            )

    async def _wait(self) -> Any:
        if asyncio.current_task() is self._task:
            raise RuntimeError("A task cannot await itself")
        return await asyncio.shield(self._task)

    def __await__(self) -> Iterator[Any]:
        return self._wait().__await__()


class TaskManager:
    def __init__(self) -> None:
        self._handles: dict[str, TaskHandle] = {}
        self._counter = 0
        self._reporters: set[asyncio.Task[None]] = set()
        self._completed: deque[tuple[str, TaskHandle]] = deque()
        self._completed_ids: set[str] = set()
        self._explicit_ids: set[str] = set()

    def _validate_new_id(self, ident: str, *, generated: bool) -> None:
        if not isinstance(ident, str) or not ident:
            raise ValueError("Task ID must be a non-empty string")
        current = self._handles.get(ident)
        if current is not None:
            raise ValueError(f"Task ID already exists: {ident}")
        if ident in self._explicit_ids:
            raise ValueError(f"Task ID already exists: {ident}")
        if not generated and _reserved_generated_id(ident):
            raise ValueError(f"Task ID uses a reserved generated namespace: {ident}")

    def _track(self, handle: TaskHandle, *, generated: bool = False) -> None:
        current = self._handles.get(handle.id)
        if current is handle:
            return
        self._validate_new_id(handle.id, generated=generated)
        self._handles[handle.id] = handle
        if not generated:
            self._explicit_ids.add(handle.id)

    def _completed_handle(self, handle: TaskHandle) -> None:
        task = getattr(handle, "_task", None)
        if task is not None and not task.done():
            return
        if self._handles.get(handle.id) is not handle or handle.id in self._completed_ids:
            return
        self._completed.append((handle.id, handle))
        self._completed_ids.add(handle.id)
        while len(self._completed) > COMPLETED_TASKS:
            ident, old = self._completed.popleft()
            self._completed_ids.discard(ident)
            if self._handles.get(ident) is old:
                self._handles.pop(ident, None)

    def start(
        self,
        awaitable: Awaitable[Any],
        *,
        task_id: str | None = None,
        visible: bool = True,
    ) -> TaskHandle:
        if not inspect.isawaitable(awaitable):
            raise TypeError("tasks.start expects an awaitable")
        generation = os.environ.get("MYPR_GENERATION", "local")
        if task_id is None:
            self._counter += 1
            ident = f"task-{generation}-{self._counter}"
            generated = True
        else:
            ident = task_id
            generated = False
        try:
            self._validate_new_id(ident, generated=generated)
        except BaseException:
            if inspect.iscoroutine(awaitable):
                awaitable.close()
            raise
        try:
            buffer = OutputBuffer()
            run_state = {"started": False}

            async def runner() -> Any:
                run_state["started"] = True
                token = _output_buffer.set(buffer)
                try:
                    return await awaitable
                finally:
                    _output_buffer.reset(token)

            runner_coro = runner()
            try:
                task = asyncio.create_task(runner_coro, name=f"mypr:{ident}", eager_start=False)
            except BaseException:
                runner_coro.close()
                raise
        except BaseException:
            if inspect.iscoroutine(awaitable):
                awaitable.close()
            if not generated:
                self._explicit_ids.discard(ident)
            raise
        handle = TaskHandle(ident, task, buffer, awaitable, run_state)
        if visible:
            self._track(handle, generated=generated)

            async def report() -> None:
                try:
                    await handle._report()
                finally:
                    self._completed_handle(handle)

            reporter = asyncio.create_task(report(), name=f"mypr:report:{ident}")
            self._reporters.add(reporter)
            reporter.add_done_callback(self._reporters.discard)
        elif not generated:
            self._explicit_ids.add(ident)
        return handle

    def list(self, client_id: str | None = None) -> list[dict[str, Any]]:
        items = [handle.status() for handle in self._handles.values()]
        if client_id is not None:
            items = [item for item in items if item.get("client_id") == client_id]
        return items

    def active(self) -> list[TaskHandle]:
        return [
            handle
            for handle in self._handles.values()
            if handle.status()["status"] in {"queued", "running", "cancelling"}
        ]

    def get(self, task_id: str) -> TaskHandle:
        try:
            return self._handles[task_id]
        except KeyError as exc:
            raise KeyError(f"unknown task: {task_id}") from exc


class RemoteTask(TaskHandle):
    def __init__(self, task_id: str, manager: TaskManager) -> None:
        self.id = task_id
        self._manager = manager
        self._buffer = OutputBuffer()
        self._created = time.time()
        self._finished_at: float | None = None
        self._client = _client_context.get()
        self._exec_id = _exec_context.get()
        self._state = "queued"
        self._result: Any = None
        self._error: str | None = None
        self._cursor = 0
        self._cancel_requested = False
        self._lock = asyncio.Lock()
        manager._validate_new_id(task_id, generated=True)
        self._monitor = manager.start(self._watch(), visible=False)
        manager._track(self, generated=True)

    async def _watch(self) -> None:
        try:
            while self._state in {"queued", "running", "cancelling"}:
                try:
                    async with self._lock:
                        result = await _rpc("shell_poll", id=self.id, cursor=self._cursor)
                        self._merge(result)
                except RPCError as exc:
                    self._state = "lost"
                    self._error = safe_error(exc)
                    return
                if self._state not in {"queued", "running", "cancelling"}:
                    return
                await asyncio.sleep(0.25)
        finally:
            if self._state in {"succeeded", "failed", "cancelled", "lost"}:
                self._manager._completed_handle(self)

    def _merge(self, result: Any) -> None:
        if not isinstance(result, Mapping):
            return
        state = str(result.get("status", result.get("state", self._state)))
        if self._cancel_requested and self._state in {"cancelled", "failed", "lost"}:
            state = self._state
        self._state = {
            "pending": "queued",
            "complete": "succeeded",
            "completed": "succeeded",
            "done": "succeeded",
            "error": "failed",
            "unknown": "lost",
        }.get(state, state)
        output = result.get("output", "")
        if isinstance(output, str):
            self._buffer.write(output)
            self._cursor = int(result.get("cursor", self._cursor + len(output)))
        elif isinstance(output, list):
            for event in output:
                if isinstance(event, Mapping):
                    text = event.get("text", "")
                    if text:
                        self._buffer.write(str(text))
            self._cursor = int(result.get("cursor", self._cursor + len(output)))
        elif isinstance(output, Mapping):
            text = output.get("text", output.get("output", ""))
            if text:
                self._buffer.write(str(text))
            self._cursor = int(result.get("cursor", output.get("cursor", self._cursor)))
        if "result" in result:
            self._result = result["result"]
        if result.get("error"):
            self._error = str(result["error"])
        if self._state in {"succeeded", "failed", "cancelled", "lost"}:
            self._finished_at = self._finished_at or time.time()

    def status(self) -> dict[str, Any]:
        status = {
            "id": self.id,
            "status": self._state,
            "created_at": self._created,
            "error": self._error,
            "client_id": self._client.id if self._client else None,
            "connection_id": self._client.connection_id if self._client else None,
            "exec_id": self._exec_id,
        }
        if self._finished_at is not None:
            status["finished_at"] = self._finished_at
        return status

    async def _wait(self) -> Any:
        await self._monitor
        return self.result()

    def result(self) -> Any:
        if self._state in {"queued", "running", "cancelling"}:
            raise NotReady(f"task {self.id} is still running")
        if self._state in {"failed", "lost"}:
            raise RPCError(self._error or f"task {self.id} failed")
        if self._state == "cancelled":
            raise asyncio.CancelledError
        return self._result

    async def cancel(self) -> bool:
        if self._state in {"succeeded", "failed", "cancelled", "lost"}:
            return False
        self._cancel_requested = True
        self._state = "cancelling"
        async with self._lock:
            await _rpc("shell_cancel", id=self.id)
            result = await _rpc("shell_poll", id=self.id, cursor=self._cursor)
            self._merge(result)
        return True


class Shell:
    def __init__(self, tasks: TaskManager) -> None:
        self._tasks = tasks

    async def start(
        self,
        command: str | list[str],
        *,
        cwd: str | os.PathLike[str] | None = None,
        env: Mapping[str, str] | None = None,
    ) -> RemoteTask:
        if isinstance(command, list):
            command = shlex.join(str(part) for part in command)
        result = await _rpc(
            "shell_start",
            command=command,
            cwd=str(cwd or os.getcwd()),
            env=dict(os.environ if env is None else env),
        )
        if isinstance(result, Mapping):
            task_id = result.get("id", result.get("task_id"))
        else:
            task_id = result
        if not task_id:
            raise RPCError("shell_start returned no task id")
        return RemoteTask(str(task_id), self._tasks)


class MCP:
    async def get_config(self, server: str) -> dict[str, Any]:
        """Read the active configuration for a server."""
        return await self.request("get_config", server=server)

    async def configure(
        self, server: str, config: Mapping[str, Any], *, force: bool = False
    ) -> Any:
        """Persist a complete server configuration and replace only its connection."""
        return await self.request("configure", server=server, config=dict(config), force=force)

    async def remove(self, server: str, *, force: bool = False) -> Any:
        """Remove a server and its saved configuration."""
        return await self.request("remove", server=server, force=force)

    async def restart(self, server: str, *, force: bool = False) -> Any:
        """Reconnect a server using its active configuration, preserving kernel state."""
        return await self.request("restart", server=server, force=force)

    async def reload(self, *, force: bool = False) -> Any:
        """Read edited config.toml and replace only changed connections."""
        return await self.request("reload", force=force)

    async def request(self, method: str, **args: Any) -> Any:
        return await _rpc("mcp", method=method, args=args)

    async def list_servers(self) -> Any:
        return await self.request("list_servers")

    async def list_tools(self, server: str | None = None, **kwargs: Any) -> Any:
        if server:
            kwargs["server"] = server
        return await self.request("list_tools", **kwargs)

    async def call_tool(
        self,
        server: str,
        name: str,
        arguments: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        args = {"server": server, "name": name, "arguments": dict(arguments or {})}
        args.update(kwargs)
        return await self.request("call_tool", **args)

    async def read_resource(self, server: str, uri: str) -> Any:
        return await self.request("read_resource", server=server, uri=uri)

    async def list_resources(self, server: str | None = None, **kwargs: Any) -> Any:
        if server:
            kwargs["server"] = server
        return await self.request("list_resources", **kwargs)

    async def list_prompts(self, server: str | None = None, **kwargs: Any) -> Any:
        if server:
            kwargs["server"] = server
        return await self.request("list_prompts", **kwargs)

    async def get_prompt(
        self,
        server: str,
        name: str,
        arguments: Mapping[str, Any] | None = None,
    ) -> Any:
        return await self.request(
            "get_prompt", server=server, name=name, arguments=dict(arguments or {})
        )


class Messages:
    """Persistent, client-scoped messages for workspace collaboration."""

    @staticmethod
    def _require_client() -> ClientInfo:
        client = _client_context.get()
        if client is None:
            raise RPCError("ws.messages requires an initialized client")
        return client

    async def send(self, to: str, text: str) -> dict[str, Any]:
        self._require_client()
        return await _rpc("message_send", to=to, text=text)

    async def read(
        self,
        limit: int = 20,
        after: int | None = None,
        wait_ms: int = 0,
    ) -> dict[str, Any]:
        self._require_client()
        fields: dict[str, Any] = {"limit": limit, "wait_ms": wait_ms}
        if after is not None:
            fields["after"] = after
        return await _rpc("message_read", **fields)

    async def ack(self, ids: list[int]) -> int:
        self._require_client()
        return await _rpc("message_ack", ids=list(ids))


class Packages:
    def __init__(self, tasks: TaskManager) -> None:
        self._tasks = tasks

    async def add(self, *specs: str) -> RemoteTask:
        if len(specs) == 1 and isinstance(specs[0], (list, tuple, set)):
            specs = tuple(str(item) for item in specs[0])
        result = await _rpc("packages_add", specs=list(specs))
        task_id = result.get("id", result.get("task_id")) if isinstance(result, Mapping) else result
        if not task_id:
            raise RPCError("packages_add returned no task id")
        return RemoteTask(str(task_id), self._tasks)


class Skills:
    def __init__(self, workspace: Path) -> None:
        self.root = workspace / ".mypr" / "skills"

    def _path(self, name: str) -> Path:
        candidate = (self.root / name / "SKILL.md").resolve()
        try:
            candidate.relative_to(self.root.resolve())
        except ValueError as exc:
            raise ValueError("skill path escapes workspace") from exc
        return candidate

    @staticmethod
    def _metadata(path: Path) -> dict[str, Any]:
        text = path.read_text(encoding="utf-8")
        metadata: dict[str, Any] = {}
        if text.startswith("---"):
            _, _, rest = text.partition("\n")
            front, marker, _ = rest.partition("\n---")
            if marker:
                try:
                    import yaml
                except ImportError:
                    for line in front.splitlines():
                        if ":" in line:
                            key, value = line.split(":", 1)
                            metadata[key.strip()] = value.strip().strip("'\"")
                else:
                    try:
                        parsed = yaml.safe_load(front)
                    except yaml.YAMLError as exc:
                        detail = str(exc).strip() or exc.__class__.__name__
                        metadata["error"] = f"Invalid YAML front matter: {detail}"
                    except (TypeError, ValueError) as exc:
                        detail = str(exc).strip() or exc.__class__.__name__
                        metadata["error"] = f"Invalid skill metadata: {detail}"
                    else:
                        if isinstance(parsed, Mapping):
                            metadata.update(parsed)
        return metadata

    def list(self) -> list[dict[str, Any]]:
        if not self.root.is_dir():
            return []
        found = []
        for path in sorted(self.root.glob("*/SKILL.md")):
            try:
                resolved = self._path(path.parent.name)
            except OSError, ValueError:
                continue
            try:
                item = self._metadata(resolved)
            except (OSError, UnicodeError) as exc:
                detail = str(exc).strip() or exc.__class__.__name__
                item = {"error": f"Unable to read skill: {detail}"}
            item.setdefault("name", path.parent.name)
            item["path"] = str(path)
            found.append(item)
        return found

    def read(self, name: str) -> str:
        return self._path(name).read_text(encoding="utf-8")


class History:
    async def list(
        self,
        client_id: str | None = None,
        limit: int = 20,
        cursor: str | None = None,
    ) -> Any:
        fields: dict[str, Any] = {"limit": limit}
        if client_id is not None:
            fields["filter_client_id"] = client_id
        if cursor is not None:
            fields["cursor"] = cursor
        return await _rpc("history_list", **fields)

    async def get(self, exec_id: str) -> Any:
        return await _rpc("history_get", id=exec_id)

    async def logs(
        self,
        client_id: str | None = None,
        limit: int = 20,
        cursor: int | str | None = None,
    ) -> Any:
        fields: dict[str, Any] = {"limit": limit}
        if client_id is not None:
            fields["filter_client_id"] = client_id
        if cursor is not None:
            fields["cursor"] = cursor
        return await _rpc("logs", **fields)


class Workspace:
    def __init__(
        self,
        workspace: str | os.PathLike[str] | None = None,
        namespace: Mapping[str, Any] | None = None,
    ) -> None:
        self.workspace = Path(workspace or os.environ.get("MYPR_WORKSPACE", os.getcwd())).resolve()
        self.root = self.workspace / ".mypr"
        self._namespace = namespace
        self._locals: dict[str, dict[str, Any]] = {}
        self.tasks = TaskManager()
        self.shell = Shell(self.tasks)
        self.mcp = MCP()
        self.messages = Messages()
        self.packages = Packages(self.tasks)
        self.skills = Skills(self.workspace)
        self.history = History()

    @property
    def client(self) -> ClientInfo | None:
        return _client_context.get()

    @property
    def local(self) -> dict[str, Any]:
        """Return the persistent scratch dictionary for the current client."""

        client = _client_context.get()
        key = client.id if client is not None else "__anonymous__"
        return self._locals.setdefault(key, {})

    async def status(self) -> Any:
        return await _rpc("status")

    async def reset(self, force: bool = False) -> Any:
        if _output_buffer.get() is not None:
            raise RuntimeError("Request reset from a foreground Python cell")
        exec_id = _exec_context.get()
        current = self.tasks._handles.get(exec_id)
        if current is None or getattr(current, "_task", None) is not asyncio.current_task():
            raise RuntimeError("Request reset from a foreground Python cell")
        active = [handle for handle in self.tasks.active() if handle.id != exec_id]
        if active and not force:
            raise RuntimeError("Workspace has active tasks; pass force=True to reset")
        result = await _rpc(
            "reset",
            force=bool(force),
            from_kernel=True,
            generation=os.environ.get("MYPR_GENERATION"),
            exec_id=exec_id,
        )
        if force:
            await asyncio.gather(*(handle.cancel() for handle in active), return_exceptions=True)
        raise ResetRequested(result)

    def inspect(self) -> dict[str, Any]:
        namespace = self._namespace.items() if self._namespace is not None else ()
        values = {
            name: type(value).__name__
            for name, value in namespace
            if not name.startswith("_") and name not in {"ws"}
        }
        return {
            "workspace": str(self.workspace),
            "generation": os.environ.get("MYPR_GENERATION"),
            "client": (
                {
                    "id": self.client.id,
                    "connection_id": self.client.connection_id,
                }
                if self.client is not None
                else None
            ),
            "variables": values,
            "tasks": self.tasks.list(),
            "skills": self.skills.list(),
        }


_TASKS = TaskManager()


def create_workspace(
    workspace: str | os.PathLike[str] | None = None,
    namespace: Mapping[str, Any] | None = None,
) -> Workspace:
    global _TASKS
    ws = Workspace(workspace, namespace)
    ws.tasks = _TASKS
    ws.shell = Shell(_TASKS)
    ws.packages = Packages(_TASKS)
    return ws


__all__ = [
    "MCP",
    "Messages",
    "ClientInfo",
    "History",
    "NotReady",
    "RPCError",
    "RemoteTask",
    "ResetRequested",
    "TaskHandle",
    "TaskManager",
    "Workspace",
    "create_workspace",
    "execution_context",
]
