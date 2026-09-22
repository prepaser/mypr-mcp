"""The API injected into a workspace's persistent IPython kernel."""

from __future__ import annotations

import asyncio
import base64
import contextvars
import inspect
import io
import json
import math
import os
import re
import time
from collections import deque
from collections.abc import Awaitable, Iterator, Mapping
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .api_help import workspace_help
from .browser_tools import BrowserTools
from .diagnostics import safe_error, safe_error_details
from .filesystem import Filesystem
from .http_tools import HTTPTools
from .locks import WorkspaceLocks
from .modules import ModuleManager
from .network_tools import NetworkTools
from .skill_tools import SkillsWriting
from .system_tools import SystemTools
from .terminal import validate_size as _terminal_size
from .workspace_tools import Git

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
_HISTORY_ID = re.compile(r"^python:[0-9a-f]{32}:.+$")


def _reserved_generated_id(value: str) -> bool:
    return bool(
        _HEX_ID.fullmatch(value)
        or _TASK_ID.fullmatch(value)
        or _HISTORY_ID.fullmatch(value)
        or value.startswith("remote-watch:")
    )


class NotReady(RuntimeError):
    """Raised when a task result is requested before it has finished."""


class RPCError(RuntimeError):
    """An error returned by the workspace manager."""


class ResultUnavailable(RPCError):
    """Raised when a persisted task cannot restore its Python return value."""


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
        self.changed = asyncio.Event()
        self._streams: dict[str, io.StringIO] = {}
        self._stream_truncated: dict[str, bool] = {}
        self._labels = bytearray()

    def write(self, value: str, *, stream: str | None = None) -> int:
        stream = "stderr" if stream == "stderr" else "stdout"
        if not value:
            return 0
        remaining = self.limit - self.size
        if remaining <= 0:
            self.truncated = True
            if stream in {"stdout", "stderr"}:
                self._stream_truncated[stream] = True
            self.changed.set()
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
        label = 1 if stream == "stderr" else 0
        kept = raw[:remaining].decode("utf-8", "ignore")
        self._labels.extend(bytes([label]) * len(kept))
        if stream in {"stdout", "stderr"}:
            target = self._streams.setdefault(stream, io.StringIO())
            target.write(kept)
            if len(raw) > remaining:
                self._stream_truncated[stream] = True
        self.changed.set()
        return len(value)

    def get(self, stream: str | None = None) -> str:
        if stream in {"stdout", "stderr"}:
            return self._streams.setdefault(stream, io.StringIO()).getvalue()
        return self._text.getvalue()

    def view(self, stream: str) -> OutputBufferView:
        return OutputBufferView(self, stream)

    def segments(self, cursor: int = 0, limit: int = 8192) -> list[tuple[str, str]]:
        text = self.get()
        end = min(len(text), cursor + limit)
        if cursor < 0 or cursor > len(text):
            raise ValueError("invalid output cursor")
        result: list[tuple[str, str]] = []
        while cursor < end:
            label = self._labels[cursor] if cursor < len(self._labels) else 0
            next_pos = cursor + 1
            while next_pos < end and self._labels[next_pos] == label:
                next_pos += 1
            result.append(("stderr" if label == 1 else "stdout", text[cursor:next_pos]))
            cursor = next_pos
        return result


class OutputBufferView:
    def __init__(self, parent: OutputBuffer, stream: str) -> None:
        self._parent = parent
        self._stream = stream
        self.changed = parent.changed

    def get(self) -> str:
        return self._parent.get(self._stream)

    @property
    def truncated(self) -> bool:
        return self._parent._stream_truncated.get(self._stream, False)


class MultiplexStream:
    """Send output from a background task to its buffer, preserving kernel output."""

    def __init__(self, stream: Any) -> None:
        self.stream = stream

    def write(self, value: str) -> int:
        buffer = _output_buffer.get()
        if buffer is not None:
            name = getattr(self.stream, "name", None)
            return buffer.write(value, stream=name if name in {"stdout", "stderr"} else None)
        cell = _cell_output.get()
        if cell is not None:
            name = getattr(self.stream, "name", None)
            cell.write(value, stream=name if name in {"stdout", "stderr"} else None)
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
        self._generation = os.environ.get("MYPR_GENERATION", "local")
        self._task.add_done_callback(self._finished)

    def _finished(self, task: asyncio.Task[Any]) -> None:
        self._finished_at = time.time()
        self._buffer.changed.set()
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

    def _output_buffer_for(self, stream: str | None) -> OutputBuffer:
        if stream not in (None, "all", "stdout", "stderr"):
            raise ValueError("stream must be 'all', 'stdout', or 'stderr'")
        if stream in {"stdout", "stderr"} and hasattr(self._buffer, "view"):
            return self._buffer.view(stream)
        return self._buffer

    def _cursor_generation(self) -> str:
        return getattr(self, "_generation", os.environ.get("MYPR_GENERATION", "local"))

    def _encode_output_cursor(self, stream: str, offset: int) -> str:
        payload = json.dumps(
            {
                "v": 1,
                "id": self.id,
                "generation": self._cursor_generation(),
                "stream": stream,
                "offset": offset,
            },
            separators=(",", ":"),
        ).encode()
        return "mypr1." + base64.urlsafe_b64encode(payload).decode().rstrip("=")

    def _decode_output_cursor(self, cursor: str | None, stream: str) -> int:
        if cursor is None:
            return 0
        if not isinstance(cursor, str) or not cursor.startswith("mypr1."):
            raise ValueError("invalid output cursor")
        try:
            raw = base64.urlsafe_b64decode(cursor[6:] + "=" * (-len(cursor[6:]) % 4))
            value = json.loads(raw)
        except (ValueError, UnicodeError, json.JSONDecodeError) as exc:
            raise ValueError("invalid output cursor") from exc
        if (
            not isinstance(value, dict)
            or value.get("v") != 1
            or value.get("id") != self.id
            or value.get("generation") != self._cursor_generation()
            or value.get("stream") != stream
            or type(value.get("offset")) is not int
            or value["offset"] < 0
        ):
            raise ValueError("output cursor does not belong to this task")
        return value["offset"]

    @staticmethod
    def _page_text(text: str, offset: int, max_bytes: int) -> tuple[str, int]:
        raw = text.encode("utf-8")
        if offset > len(raw):
            raise ValueError("invalid output cursor")
        try:
            raw[:offset].decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ValueError("invalid output cursor") from exc
        if max_bytes == 0:
            return "", offset
        page = raw[offset : offset + max_bytes]
        value = page.decode("utf-8", "ignore")
        if not value and page:
            raise ValueError("max_bytes is too small for the next UTF-8 character")
        consumed = len(value.encode("utf-8"))
        return value, offset + consumed

    async def read(
        self,
        cursor: str | None = None,
        *,
        stream: str | None = None,
        max_bytes: int = 32768,
        wait_ms: int = 0,
    ) -> dict[str, Any]:
        if type(max_bytes) is not int or not 0 <= max_bytes <= RPC_LIMIT:
            raise ValueError("max_bytes must be an integer between 0 and the RPC limit")
        if type(wait_ms) is not int or not 0 <= wait_ms <= 30000:
            raise ValueError("wait_ms must be an integer between 0 and 30000")
        selected = "all" if stream is None else stream
        buffer = self._output_buffer_for(stream)
        offset = self._decode_output_cursor(cursor, selected)
        deadline = time.monotonic() + wait_ms / 1000
        while True:
            text, next_offset = self._page_text(buffer.get(), offset, max_bytes)
            status = self.status()
            terminal = status.get("status") in {"succeeded", "failed", "cancelled", "lost"}
            if text or terminal or wait_ms == 0 or time.monotonic() >= deadline:
                result = {
                    "id": self.id,
                    "output": text,
                    "cursor": self._encode_output_cursor(selected, next_offset),
                    "has_more": next_offset < len(buffer.get().encode("utf-8")),
                    "state": status.get("status"),
                    "truncated": bool(getattr(buffer, "truncated", False)),
                }
                if status.get("warnings"):
                    result["warnings"] = list(status["warnings"])
                if status.get("error"):
                    result["error"] = status["error"]
                return result
            changed = buffer.changed
            changed.clear()
            remaining = max(0.0, deadline - time.monotonic())
            if remaining <= 0:
                continue
            try:
                await asyncio.wait_for(changed.wait(), remaining)
            except TimeoutError:
                pass

    async def expect(
        self,
        pattern: str,
        cursor: str | None = None,
        *,
        stream: str | None = None,
        regex: bool = False,
        timeout: float = 30,  # noqa: ASYNC109
        max_scan_bytes: int = 65536,
    ) -> dict[str, Any]:
        if not isinstance(pattern, str) or not pattern:
            raise ValueError("pattern must be a non-empty string")
        if (
            not isinstance(timeout, (int, float))
            or isinstance(timeout, bool)
            or timeout < 0
            or not math.isfinite(timeout)
        ):
            raise ValueError("timeout must be a finite non-negative number")
        if type(max_scan_bytes) is not int or max_scan_bytes < 0:
            raise ValueError("max_scan_bytes must be a non-negative integer")
        selected = "all" if stream is None else stream
        buffer = self._output_buffer_for(stream)
        origin = self._decode_output_cursor(cursor, selected)
        deadline = time.monotonic() + min(timeout, 30)
        expression = re.compile(pattern) if regex else None
        while True:
            text = buffer.get()
            raw = text.encode("utf-8")
            available = raw[origin : origin + max_scan_bytes].decode("utf-8", "ignore")
            match = expression.search(available) if expression else None
            if not regex:
                found = available.find(pattern)
                if found >= 0:
                    match_end = found + len(pattern)
                else:
                    match_end = -1
            elif match is not None:
                match_end = match.end()
            else:
                match_end = -1
            if match_end >= 0:
                consumed = len(available[:match_end].encode("utf-8"))
                return {
                    "id": self.id,
                    "matched": True,
                    "reason": "match",
                    "match": (match.group(0) if regex else pattern),
                    "cursor": self._encode_output_cursor(selected, origin + consumed),
                    "scanned_cursor": self._encode_output_cursor(
                        selected, origin + len(available.encode("utf-8"))
                    ),
                    "state": self.status().get("status"),
                }
            if len(raw) - origin > max_scan_bytes:
                return {
                    "id": self.id,
                    "matched": False,
                    "reason": "limit",
                    "cursor": self._encode_output_cursor(selected, origin),
                    "scanned_cursor": self._encode_output_cursor(
                        selected, origin + len(available.encode("utf-8"))
                    ),
                    "state": self.status().get("status"),
                }
            status = self.status()
            if status.get("status") in {"succeeded", "failed", "cancelled", "lost"}:
                return {
                    "id": self.id,
                    "matched": False,
                    "reason": "eof",
                    "cursor": self._encode_output_cursor(selected, origin),
                    "scanned_cursor": self._encode_output_cursor(
                        selected, origin + len(available.encode("utf-8"))
                    ),
                    "state": status.get("status"),
                }
            if time.monotonic() >= deadline:
                return {
                    "id": self.id,
                    "matched": False,
                    "reason": "timeout",
                    "cursor": self._encode_output_cursor(selected, origin),
                    "scanned_cursor": self._encode_output_cursor(
                        selected, origin + len(available.encode("utf-8"))
                    ),
                    "state": status.get("status"),
                }
            changed = buffer.changed
            changed.clear()
            try:
                await asyncio.wait_for(changed.wait(), max(0.0, deadline - time.monotonic()))
            except TimeoutError:
                pass

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
            for name, chunk in self._buffer.segments(cursor):
                await publish("running", output_delta=chunk, output_stream=name)
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
        if handle.status()["status"] not in {"succeeded", "failed", "cancelled", "lost", "reset"}:
            return
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

    async def attach(self, task_id: str) -> TaskHandle:
        """Attach to a live handle or a persisted workspace task record."""

        if not isinstance(task_id, str) or not task_id:
            raise ValueError("task ID must be a non-empty string")
        current = self._handles.get(task_id)
        if current is not None:
            return current
        if task_id.startswith("python:"):
            parts = task_id.split(":", 2)
            if len(parts) == 3:
                current = self._handles.get(parts[2])
                if current is not None and getattr(current, "_generation", None) == parts[1]:
                    return current
        record = await _rpc("history_get", id=task_id)
        if not isinstance(record, Mapping):
            raise RPCError("invalid persisted task record")
        kind = record.get("kind")
        current = self._handles.get(str(record.get("id")))
        if current is not None and (
            kind in {"shell", "package", "scan"}
            or getattr(current, "_generation", None) == record.get("generation")
        ):
            return current
        if kind in {"shell", "package", "scan"}:
            if kind == "scan":
                from .scan_api import make_scan_task

                handle = make_scan_task(str(record["id"]), self, _rpc)
            else:
                handle = RemoteTask(str(record["id"]), self)
            handle._generation = str(record.get("generation") or handle._cursor_generation())
            handle._created = record.get("created", handle._created)
            handle._exec_id = record.get("exec_id")
            handle._client = (
                ClientInfo(str(record["client_id"]), record.get("connection_id"))
                if record.get("client_id") is not None
                else None
            )
            self._handles[task_id] = handle
            return handle
        if kind in {"python", "execution"}:
            return HistoricalTask(record)
        raise ValueError(f"task {task_id!r} is not attachable")


class HistoricalTask(TaskHandle):
    """Read-only handle backed by a persisted Python task record."""

    def __init__(self, record: Mapping[str, Any]) -> None:
        self.id = str(record["id"])
        self._task = None
        self._source = None
        self._buffer = OutputBuffer()
        self._streams = {name: OutputBuffer() for name in ("stdout", "stderr")}
        self._history_lock = asyncio.Lock()
        self._generation = str(record.get("generation") or "local")
        self._created = float(record.get("created", time.time()))
        self._finished_at = record.get("finished") or record.get("finished_at")
        self._cancel_requested = False
        self._remote_has_more = False
        self._client = (
            ClientInfo(str(record["client_id"]), record.get("connection_id"))
            if record.get("client_id") is not None
            else None
        )
        self._exec_id = record.get("exec_id")
        self._state = str(record.get("state", "lost"))
        self._error = record.get("error")
        self._history_id = record.get("history_id") or record.get("id")
        self._history_event_cursor = int(record.get("cursor", 0) or 0)
        self._history_has_more = bool(record.get("has_more"))
        output = record.get("output", "")
        if isinstance(output, list):
            for event in output:
                if not isinstance(event, Mapping) or not event.get("text"):
                    continue
                text = str(event["text"])
                self._buffer.write(text)
                stream = event.get("stream")
                self._streams[stream if stream in self._streams else "stdout"].write(text)
        elif output:
            text = str(output)
            self._buffer.write(text)
            self._streams["stdout"].write(text)
        self._buffer.truncated = bool(record.get("truncated") or record.get("output_truncated"))

    async def _load_more(self, max_bytes: int = 32768) -> bool:
        async with self._history_lock:
            if not self._history_has_more or not self._history_id:
                return False
            result = await _rpc(
                "history_task_read",
                id=self._history_id,
                cursor=self._history_event_cursor,
                max_bytes=min(max_bytes, OUTPUT_LIMIT),
            )
            if not isinstance(result, Mapping):
                raise RPCError("invalid persisted task output response")
            events = result.get("output", [])
            if not isinstance(events, list):
                raise RPCError("invalid persisted task output events")
            next_cursor = result.get("cursor", self._history_event_cursor)
            if type(next_cursor) is not int or next_cursor < self._history_event_cursor:
                raise RPCError("invalid persisted task output cursor")
            if result.get("has_more") and next_cursor == self._history_event_cursor:
                raise RPCError("persisted task output cursor did not advance")
            for event in events:
                if not isinstance(event, Mapping) or not event.get("text"):
                    continue
                text = str(event["text"])
                self._buffer.write(text)
                stream = event.get("stream")
                if stream in self._streams:
                    self._streams[stream].write(text)
                else:
                    self._streams["stdout"].write(text)
            progressed = next_cursor != self._history_event_cursor
            self._history_event_cursor = next_cursor
            self._history_has_more = bool(result.get("has_more")) and progressed
            self._buffer.truncated |= bool(result.get("truncated"))
            for stream in self._streams.values():
                stream.truncated |= bool(result.get("truncated"))
            return bool(events)

    def _output_buffer_for(self, stream: str | None) -> OutputBuffer:
        if stream in (None, "all"):
            return self._buffer
        if stream not in self._streams:
            raise ValueError("stream must be 'all', 'stdout', or 'stderr'")
        return self._streams[stream]

    async def read(self, cursor: str | None = None, **kwargs: Any) -> dict[str, Any]:
        selected = kwargs.get("stream")
        offset = self._decode_output_cursor(cursor, "all" if selected is None else selected)
        buffer = self._output_buffer_for(selected)
        max_bytes = kwargs.get("max_bytes", 32768)
        if type(max_bytes) is not int or not 0 <= max_bytes <= RPC_LIMIT:
            raise ValueError("max_bytes must be an integer between 0 and the RPC limit")
        while (
            max_bytes > 0
            and self._history_has_more
            and len(buffer.get().encode("utf-8")) - offset < max_bytes
            and await self._load_more(max_bytes)
        ):
            buffer = self._output_buffer_for(selected)
        result = await super().read(cursor, **kwargs)
        result["has_more"] = bool(result.get("has_more") or self._history_has_more)
        return result

    async def expect(
        self,
        pattern: str,
        cursor: str | None = None,
        *,
        stream: str | None = None,
        regex: bool = False,
        timeout: float = 30,  # noqa: ASYNC109
        max_scan_bytes: int = 65536,
    ) -> dict[str, Any]:
        if type(timeout) not in (int, float) or timeout < 0 or not math.isfinite(timeout):
            raise ValueError("timeout must be a finite non-negative number")
        origin = self._decode_output_cursor(cursor, "all" if stream is None else stream)
        deadline = time.monotonic() + timeout
        while True:
            result = await super().expect(
                pattern,
                cursor,
                stream=stream,
                regex=regex,
                timeout=0,
                max_scan_bytes=max_scan_bytes,
            )
            if result["matched"] or result["reason"] == "limit" or not self._history_has_more:
                return result
            available = self._output_buffer_for(stream).size - origin
            if available >= max_scan_bytes:
                return {**result, "reason": "limit"}
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return {**result, "reason": "timeout"}
            try:
                async with asyncio.timeout(remaining):
                    loaded = await self._load_more(min(32768, max_scan_bytes - max(0, available)))
            except TimeoutError:
                return {**result, "reason": "timeout"}
            if not loaded:
                return result

    async def _wait(self) -> Any:
        return self.result()

    def status(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "history_id": self._history_id,
            "status": self._state,
            "created_at": self._created,
            "finished_at": self._finished_at,
            "exec_id": self._exec_id,
            "client_id": self._client.id if self._client else None,
            "connection_id": self._client.connection_id if self._client else None,
            "error": self._error,
            "read_only": True,
        }

    def result(self) -> Any:
        raise ResultUnavailable(f"result for persisted Python task {self.id} is unavailable")

    async def cancel(self) -> bool:
        return False


class RemoteTask(TaskHandle):
    def __init__(self, task_id: str, manager: TaskManager) -> None:
        self.id = task_id
        self._manager = manager
        self._buffer = OutputBuffer()
        self._streams = {name: OutputBuffer() for name in ("stdout", "stderr")}
        self._created = time.time()
        self._finished_at: float | None = None
        self._client = _client_context.get()
        self._exec_id = _exec_context.get()
        self._state = "queued"
        self._result: Any = None
        self._error: str | None = None
        self._warnings: list[dict[str, Any]] = []
        self._warnings_truncated = False
        self._terminal: dict[str, Any] = {}
        self._cursor = 0
        self._cancel_requested = False
        self._remote_has_more = False
        self._lock = asyncio.Lock()
        manager._validate_new_id(task_id, generated=True)
        self._monitor = manager.start(self._watch(), visible=False)
        manager._track(self, generated=True)

    async def _watch(self) -> None:
        try:
            while True:
                if (
                    self._state not in {"queued", "running", "cancelling"}
                    and not self._remote_has_more
                ):
                    return
                try:
                    cursor = self._cursor
                    try:
                        result = await _rpc(
                            "shell_read",
                            id=self.id,
                            cursor=cursor,
                            max_bytes=1024 * 1024,
                            wait_ms=30000,
                        )
                    except RPCError, AssertionError:
                        result = await _rpc("shell_poll", id=self.id, cursor=cursor)
                    if cursor == self._cursor:
                        self._merge(result)
                except RPCError as exc:
                    self._state = "lost"
                    self._error = safe_error(exc)
                    self._remote_has_more = False
                    self._buffer.changed.set()
                    for buffer in self._streams.values():
                        buffer.changed.set()
                    return
                if self._state not in {"queued", "running", "cancelling"}:
                    continue
                await asyncio.sleep(0.25)
        finally:
            if self._state in {"succeeded", "failed", "cancelled", "lost"}:
                self._manager._completed_handle(self)

    def _merge(self, result: Any) -> None:
        if not isinstance(result, Mapping):
            return
        self._terminal.update(
            {key: result[key] for key in ("pty", "rows", "cols") if key in result}
        )
        for warning in result.get("warnings", []):
            if warning not in self._warnings:
                if len(self._warnings) < 4:
                    self._warnings.append(warning)
                else:
                    self._warnings_truncated = True
        self._warnings_truncated |= bool(result.get("warnings_truncated"))
        self._remote_has_more = bool(result.get("has_more"))
        self._buffer.truncated |= bool(result.get("truncated"))
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
            self._streams["stdout"].write(output)
            value = result.get("cursor")
            self._cursor = value if value is not None else self._cursor + len(output)
        elif isinstance(output, list):
            for event in output:
                if isinstance(event, Mapping):
                    text = event.get("text", "")
                    if text:
                        self._buffer.write(str(text))
                        stream = str(event.get("stream", "stdout"))
                        self._streams.get(stream, self._streams["stdout"]).write(str(text))
            value = result.get("cursor")
            self._cursor = value if value is not None else self._cursor + len(output)
        elif isinstance(output, Mapping):
            text = output.get("text", output.get("output", ""))
            if text:
                self._buffer.write(str(text))
                self._streams["stdout"].write(str(text))
            value = result.get("cursor", output.get("cursor", self._cursor))
            self._cursor = value
        if "result" in result:
            self._result = result["result"]
        if result.get("error"):
            self._error = str(result["error"])
        if self._state in {"succeeded", "failed", "cancelled", "lost"}:
            self._finished_at = self._finished_at or time.time()
            self._buffer.changed.set()
            for buffer in self._streams.values():
                buffer.changed.set()

    def status(self) -> dict[str, Any]:
        status = {
            "id": self.id,
            "status": "running" if self._remote_has_more else self._state,
            "created_at": self._created,
            "error": self._error,
            **self._terminal,
            **({"warnings": list(self._warnings)} if self._warnings else {}),
            **({"warnings_truncated": True} if self._warnings_truncated else {}),
            "client_id": self._client.id if self._client else None,
            "connection_id": self._client.connection_id if self._client else None,
            "exec_id": self._exec_id,
        }
        if self._finished_at is not None:
            status["finished_at"] = self._finished_at
        return status

    def output(self, cursor: int | None = None) -> str | dict[str, Any]:
        result = super().output(cursor)
        if isinstance(result, dict):
            if self._warnings:
                result["warnings"] = list(self._warnings)
            if self._warnings_truncated:
                result["warnings_truncated"] = True
        return result

    def _output_buffer_for(self, stream: str | None) -> OutputBuffer:
        if stream in (None, "all"):
            return self._buffer
        if stream not in self._streams:
            raise ValueError("stream must be 'all', 'stdout', or 'stderr'")
        return self._streams[stream]

    async def read(self, cursor: str | None = None, **kwargs: Any) -> dict[str, Any]:
        result = await super().read(cursor, **kwargs)
        result["has_more"] = bool(result.get("has_more") or self._remote_has_more)
        return result

    async def _wait(self) -> Any:
        await self._monitor
        return self.result()

    def result(self) -> Any:
        if self._remote_has_more or self._state in {"queued", "running", "cancelling"}:
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
            cursor = self._cursor
            try:
                result = await _rpc("shell_read", id=self.id, cursor=cursor)
            except RPCError, AssertionError:
                result = await _rpc("shell_poll", id=self.id, cursor=cursor)
            if cursor == self._cursor:
                self._merge(result)
        return True

    async def write(self, text: str = "", *, eof: bool = False) -> dict[str, Any]:
        if not isinstance(text, str) or type(eof) is not bool:
            raise TypeError("text must be a string and eof must be a boolean")
        return await _rpc("shell_write", id=self.id, text=text, eof=eof)

    async def resize(self, rows: int, cols: int) -> dict[str, Any]:
        _terminal_size(rows, cols)
        async with self._lock:
            result = await _rpc("shell_resize", id=self.id, rows=rows, cols=cols)
            self._terminal.update(rows=result["rows"], cols=result["cols"])
        return result


class ShellError(RuntimeError):
    def __init__(self, result: dict[str, Any]):
        self.result = result
        super().__init__(
            f"Shell job {result['id']} {result['state']} (exit {result['returncode']})"
        )


async def _complete_cleanup(operation):
    task = asyncio.create_task(operation)
    cancelled = False
    while True:
        try:
            result = await asyncio.shield(task)
            break
        except asyncio.CancelledError:
            if task.cancelled():
                raise
            cancelled = True
    if cancelled:
        raise asyncio.CancelledError
    return result


class Shell:
    def __init__(self, tasks: TaskManager) -> None:
        self._tasks = tasks

    async def start(
        self,
        command: str | list[str],
        *,
        cwd: str | os.PathLike[str] | None = None,
        env: Mapping[str, str] | None = None,
        input: str | None = None,
        stdin: bool = False,
        pty: bool = False,
        rows: int = 24,
        cols: int = 80,
    ) -> RemoteTask:
        if isinstance(command, list):
            command = [str(part) for part in command]
        elif not isinstance(command, str):
            raise TypeError("command must be a string or an argv list")
        if not command:
            raise ValueError("command must not be empty")
        if input is not None and not isinstance(input, str):
            raise TypeError("input must be a string or None")
        if type(stdin) is not bool:
            raise TypeError("stdin must be a boolean")
        if type(pty) is not bool:
            raise TypeError("pty must be a boolean")
        _terminal_size(rows, cols)
        result = await _rpc(
            "shell_start",
            command=command,
            cwd=str(cwd or os.getcwd()),
            env=dict(os.environ if env is None else env),
            input=input,
            stdin=stdin,
            pty=pty,
            rows=rows,
            cols=cols,
        )
        if isinstance(result, Mapping):
            task_id = result.get("id", result.get("task_id"))
        else:
            task_id = result
        if not task_id:
            raise RPCError("shell_start returned no task id")
        handle = RemoteTask(str(task_id), self._tasks)
        handle._terminal = {"pty": pty, **({"rows": rows, "cols": cols} if pty else {})}
        return handle

    async def run(
        self,
        command: str | list[str],
        *,
        cwd: str | os.PathLike[str] | None = None,
        env: Mapping[str, str] | None = None,
        input: str | None = None,
        timeout: float | None = None,  # noqa: ASYNC109
        check: bool = False,
        max_bytes: int = 32768,
        pty: bool = False,
        rows: int = 24,
        cols: int = 80,
    ) -> dict[str, Any]:
        if type(max_bytes) is not int or max_bytes < 0:
            raise ValueError("max_bytes must be a non-negative integer")
        if timeout is not None and (
            isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or not math.isfinite(timeout)
            or timeout < 0
        ):
            raise ValueError("timeout must be a finite non-negative number or None")
        launch = asyncio.create_task(
            self.start(command, cwd=cwd, env=env, input=input, pty=pty, rows=rows, cols=cols)
        )
        try:
            handle = await asyncio.shield(launch)
        except asyncio.CancelledError:

            async def stop_launch():
                handle = await launch
                await handle.cancel()

            with suppress(Exception):
                await _complete_cleanup(stop_launch())
            raise
        timed_out = False
        try:
            async with asyncio.timeout(timeout):
                await handle._monitor
        except TimeoutError:
            timed_out = True
            await _complete_cleanup(handle.cancel())
        except asyncio.CancelledError:
            await _complete_cleanup(handle.cancel())
            raise
        result = handle.status()
        result["state"] = result.pop("status")
        result["returncode"] = (handle._result or {}).get("returncode")
        result["timed_out"] = timed_out
        result["truncated"] = handle._buffer.truncated
        remaining = max_bytes
        for name in ("stdout", "stderr"):
            raw = handle._streams[name].get().encode("utf-8")
            kept = raw[:remaining].decode("utf-8", "ignore")
            result[name] = kept
            remaining -= len(kept.encode("utf-8"))
            result["truncated"] |= len(raw) > len(kept.encode("utf-8"))
        if check and (timed_out or result["state"] != "succeeded"):
            raise ShellError(result)
        return result


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

    async def send(
        self,
        to: str,
        text: str,
        data: Any = None,
        reply_to: int | None = None,
    ) -> dict[str, Any]:
        self._require_client()
        fields: dict[str, Any] = {"to": to, "text": text}
        if data is not None:
            fields["data"] = data
        if reply_to is not None:
            fields["reply_to"] = reply_to
        return await _rpc("message_send", **fields)

    async def reply(
        self,
        message_id: int,
        text: str,
        data: Any = None,
    ) -> dict[str, Any]:
        self._require_client()
        fields: dict[str, Any] = {"message_id": message_id, "text": text}
        if data is not None:
            fields["data"] = data
        return await _rpc("message_reply", **fields)

    async def read(
        self,
        limit: int = 20,
        after: int | None = None,
        wait_ms: int = 0,
        sender: str | None = None,
        reply_to: int | None = None,
    ) -> dict[str, Any]:
        self._require_client()
        fields: dict[str, Any] = {"limit": limit, "wait_ms": wait_ms}
        if after is not None:
            fields["after"] = after
        if sender is not None:
            fields["sender"] = sender
        if reply_to is not None:
            fields["reply_to"] = reply_to
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


class Skills(SkillsWriting):
    def __init__(self, workspace: Path, fs: Filesystem | None = None) -> None:
        self.root = workspace / ".mypr" / "skills"
        self._fs = fs or Filesystem(workspace)

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
        self.fs = Filesystem(self.workspace, self.shell, self._search)
        self.mcp = MCP()
        self.messages = Messages()
        self.packages = Packages(self.tasks)
        self.skills = Skills(self.workspace, self.fs)
        self.modules = ModuleManager(self.workspace, self.fs, self.shell)
        self.git = Git(_rpc)
        self.locks = WorkspaceLocks(self._lock_identity)
        self.history = History()
        self.http = HTTPTools(self.workspace, lambda: self.client)
        self.net = NetworkTools(self.workspace, self.tasks, _rpc)
        self.system = SystemTools(self.workspace)
        self.browser = BrowserTools(self.workspace, self._lock_identity, _rpc, self.fs)
        self._closing = False

    async def _close_resources(self):
        if self._closing:
            return
        self._closing = True
        current = asyncio.current_task()
        handles = [
            handle
            for handle in self.tasks.active()
            if getattr(handle, "_task", None) is not current
        ]
        await asyncio.gather(*(handle.cancel() for handle in handles), return_exceptions=True)
        pending = [
            handle._task
            for handle in handles
            if getattr(handle, "_task", None) is not None and not handle._task.done()
        ]
        if pending:
            await asyncio.wait(pending, timeout=2)
        resources = [getattr(self, name, None) for name in ("browser", "http")]
        results = await asyncio.gather(
            *(resource.aclose() for resource in resources if resource is not None),
            return_exceptions=True,
        )
        failures = [result for result in results if isinstance(result, Exception)]
        if failures:
            raise ExceptionGroup("Workspace resource cleanup failed", failures)

    async def _search(self, **args):
        return await _rpc("search", args=args)

    def _lock_identity(self):
        client = self.client
        return {
            "client_id": client.id if client else None,
            "connection_id": client.connection_id if client else None,
            "exec_id": _exec_context.get(),
        }

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

    async def restart(self, force: bool = False) -> Any:
        if type(force) is not bool:
            raise TypeError("force must be a boolean")
        exec_id = _exec_context.get()
        current = self.tasks._handles.get(exec_id)
        if (
            _output_buffer.get() is not None
            or current is None
            or getattr(current, "_task", None) is not asyncio.current_task()
        ):
            raise RuntimeError("Request restart from a foreground Python cell")
        active = [handle for handle in self.tasks.active() if handle.id != exec_id]
        if active and not force:
            raise RuntimeError("Workspace has active tasks; pass force=True to restart")
        result = await _rpc(
            "restart",
            force=force,
            exec_id=exec_id,
            generation=os.environ.get("MYPR_GENERATION"),
        )
        raise ResetRequested(result)

    def help(self, topic: str | None = None) -> str:
        """Return the topic index or API guidance for this running kernel."""
        return workspace_help(topic)

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
    ws.fs = Filesystem(ws.workspace, ws.shell, ws._search)
    ws.skills = Skills(ws.workspace, ws.fs)
    ws.modules = ModuleManager(ws.workspace, ws.fs, ws.shell)
    ws.packages = Packages(_TASKS)
    ws.net = NetworkTools(ws.workspace, _TASKS, _rpc)
    ws.browser = BrowserTools(ws.workspace, ws._lock_identity, _rpc, ws.fs)
    return ws


__all__ = [
    "MCP",
    "Messages",
    "ClientInfo",
    "History",
    "NotReady",
    "RPCError",
    "ResultUnavailable",
    "Filesystem",
    "Shell",
    "ShellError",
    "RemoteTask",
    "ResetRequested",
    "TaskHandle",
    "TaskManager",
    "Workspace",
    "create_workspace",
    "execution_context",
]
