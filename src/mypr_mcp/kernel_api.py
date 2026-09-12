"""The API injected into a workspace's persistent IPython kernel."""

from __future__ import annotations

import asyncio
import contextvars
import inspect
import io
import json
import os
import shlex
import time
from collections.abc import Awaitable, Iterator, Mapping
from pathlib import Path
from typing import Any

_output_buffer: contextvars.ContextVar[OutputBuffer | None] = contextvars.ContextVar(
    "mypr_task_output", default=None
)
RPC_LIMIT = 32 * 1024 * 1024
try:
    OUTPUT_LIMIT = max(1024, int(os.environ.get("MYPR_OUTPUT_LIMIT", 16 * 1024 * 1024)))
except ValueError:
    OUTPUT_LIMIT = 16 * 1024 * 1024


class NotReady(RuntimeError):
    """Raised when a task result is requested before it has finished."""


class RPCError(RuntimeError):
    """An error returned by the workspace manager."""


class ResetRequested(BaseException):
    """Used to stop the current IPython cell after a kernel reset request."""


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
            self._text.write(value)
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


async def _rpc(op: str, **fields: Any) -> Any:
    socket_name = os.environ.get("MYPR_SOCKET")
    if not socket_name:
        raise RPCError("MYPR_SOCKET is not configured")
    payload = {"op": op, **fields}
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
        self._cancel_requested = False
        self._task.add_done_callback(self._finished)

    def _finished(self, task: asyncio.Task[Any]) -> None:
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
        return {"id": self.id, "status": state, "created_at": self._created}

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
        self._cancel_requested = True
        self._task.cancel()
        return True

    async def _wait(self) -> Any:
        return await self._task

    def __await__(self) -> Iterator[Any]:
        return self._wait().__await__()


class TaskManager:
    def __init__(self) -> None:
        self._handles: dict[str, TaskHandle] = {}
        self._counter = 0

    def start(
        self,
        awaitable: Awaitable[Any],
        *,
        task_id: str | None = None,
        visible: bool = True,
    ) -> TaskHandle:
        if not inspect.isawaitable(awaitable):
            raise TypeError("tasks.start expects an awaitable")
        self._counter += 1
        generation = os.environ.get("MYPR_GENERATION", "local")
        ident = task_id or f"task-{generation}-{self._counter}"
        if ident in self._handles:
            if inspect.iscoroutine(awaitable):
                awaitable.close()
            raise ValueError(f"Task ID already exists: {ident}")
        buffer = OutputBuffer()
        run_state = {"started": False}

        async def runner() -> Any:
            run_state["started"] = True
            token = _output_buffer.set(buffer)
            try:
                return await awaitable
            finally:
                _output_buffer.reset(token)

        task = asyncio.create_task(runner(), name=f"mypr:{ident}")
        handle = TaskHandle(ident, task, buffer, awaitable, run_state)
        if visible:
            self._handles[ident] = handle
        return handle

    def list(self) -> list[dict[str, Any]]:
        return [handle.status() for handle in self._handles.values()]

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
        self._state = "queued"
        self._result: Any = None
        self._error: str | None = None
        self._cursor = 0
        self._cancel_requested = False
        self._lock = asyncio.Lock()
        self._monitor = manager.start(
            self._watch(), task_id=f"remote-watch:{task_id}", visible=False
        )
        manager._handles[task_id] = self

    async def _watch(self) -> None:
        while self._state in {"queued", "running", "cancelling"}:
            try:
                async with self._lock:
                    result = await _rpc("shell_poll", id=self.id, cursor=self._cursor)
                    self._merge(result)
            except RPCError as exc:
                self._state = "lost"
                self._error = str(exc)
                return
            if self._state not in {"queued", "running", "cancelling"}:
                return
            await asyncio.sleep(0.25)

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

    def status(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "status": self._state,
            "created_at": self._created,
            "error": self._error,
        }

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

                    parsed = yaml.safe_load(front)
                    if isinstance(parsed, Mapping):
                        metadata.update(parsed)
                except ImportError, ValueError, TypeError:
                    for line in front.splitlines():
                        if ":" in line:
                            key, value = line.split(":", 1)
                            metadata[key.strip()] = value.strip().strip("'\"")
        return metadata

    def list(self) -> list[dict[str, Any]]:
        if not self.root.is_dir():
            return []
        found = []
        for path in sorted(self.root.glob("*/SKILL.md")):
            item = self._metadata(path)
            item.setdefault("name", path.parent.name)
            item["path"] = str(path)
            found.append(item)
        return found

    def read(self, name: str) -> str:
        return self._path(name).read_text(encoding="utf-8")


class Workspace:
    def __init__(
        self,
        workspace: str | os.PathLike[str] | None = None,
        namespace: Mapping[str, Any] | None = None,
    ) -> None:
        self.workspace = Path(workspace or os.environ.get("MYPR_WORKSPACE", os.getcwd())).resolve()
        self.root = self.workspace / ".mypr"
        self._namespace = namespace
        self.tasks = TaskManager()
        self.shell = Shell(self.tasks)
        self.mcp = MCP()
        self.packages = Packages(self.tasks)
        self.skills = Skills(self.workspace)

    async def status(self) -> Any:
        return await _rpc("status")

    async def reset(self, force: bool = False) -> Any:
        if _output_buffer.get() is not None:
            raise RuntimeError("Request reset from a foreground Python cell")
        active = self.tasks.active()
        if active and not force:
            raise RuntimeError("Workspace has active Python tasks; pass force=True to reset")
        if force:
            await asyncio.gather(*(handle.cancel() for handle in active), return_exceptions=True)
        parent = self._namespace.get("get_ipython") if self._namespace is not None else None
        if not callable(parent):
            try:
                from IPython import get_ipython as parent
            except ImportError:
                parent = None
        ip = parent() if callable(parent) else None
        header = getattr(ip, "parent_header", {}) or {}
        exec_id = header.get("msg_id") or os.environ.get("MYPR_EXEC_ID")
        result = await _rpc(
            "reset",
            force=bool(force),
            from_kernel=True,
            generation=os.environ.get("MYPR_GENERATION"),
            exec_id=exec_id,
        )
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
    "NotReady",
    "RPCError",
    "RemoteTask",
    "ResetRequested",
    "TaskHandle",
    "TaskManager",
    "Workspace",
    "create_workspace",
]
