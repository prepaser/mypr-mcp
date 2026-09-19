"""Workspace services used by the persistent Python kernel.

The classes in this module deliberately have a small, transport independent API.
The manager owns these objects and the kernel accesses them over its IPC layer.
"""

from __future__ import annotations

import asyncio
import codecs
import contextlib
import copy
import errno
import json
import os
import signal
import sys
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp.client.streamable_http import streamable_http_client
from mcp.types import PaginatedRequestParams

from .config import MCPConfig, validate_name, validate_servers
from .journal import decode_event
from .terminal import close as close_terminal
from .terminal import eof_byte
from .terminal import resize as resize_terminal

_MAX_SHELL_WARNINGS = 4
_MAX_SHELL_WARNING_TEXT = 256
_MAX_UNSAVED_COMPLETED = 1


def _wake_future(future: asyncio.Future[None]) -> None:
    if not future.done():
        future.set_result(None)


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
    warnings: list[dict[str, str]] = field(default_factory=list)
    warnings_truncated: bool = False
    journal_failed: bool = False
    metadata_failed: bool = False
    stdin_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    readers: list[asyncio.Task[None]] = field(default_factory=list)
    waiter: asyncio.Task[None] | None = None
    journal: Path | None = None
    metadata: Path | None = None
    finished_at: float | None = None
    memory_bytes: int = 0
    pty_reader: _PTYReader | None = None
    pty_master: int | None = None
    pty: bool = False
    session_id: int = 0
    pty_write_waiter: asyncio.Future[None] | None = None
    rows: int = 24
    cols: int = 80


class _PTYReader:
    """A nonblocking PTY master reader integrated with the asyncio loop."""

    def __init__(self, fd: int):
        self.fd = fd
        os.set_blocking(fd, False)
        self._closed = False
        self._waiter: asyncio.Future[None] | None = None

    async def read(self, size: int) -> bytes:
        loop = asyncio.get_running_loop()
        while not self._closed:
            try:
                return os.read(self.fd, size)
            except BlockingIOError:
                ready = loop.create_future()
                self._waiter = ready
                loop.add_reader(self.fd, self._wake, ready)
                try:
                    await ready
                finally:
                    if self._waiter is ready:
                        loop.remove_reader(self.fd)
                        self._waiter = None
            except OSError as exc:
                if exc.errno == errno.EIO:
                    return b""
                raise
        return b""

    @staticmethod
    def _wake(future: asyncio.Future[None]) -> None:
        _wake_future(future)

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            waiter = self._waiter
            if waiter is not None:
                with contextlib.suppress(RuntimeError):
                    asyncio.get_running_loop().remove_reader(self.fd)
                self._waiter = None
                if not waiter.done():
                    waiter.set_result(None)
            close_terminal(self.fd)


class Shells:
    """Run and supervise workspace commands in their own process groups."""

    def __init__(
        self,
        workspace: Path,
        output_limit: int = 16 * 1024 * 1024,
        completed_records: int = 128,
        cache_bytes: int = 32 * 1024 * 1024,
    ):
        self.workspace = Path(workspace).resolve()
        self.output_limit = max(0, int(output_limit))
        if completed_records < 0:
            raise ValueError("completed_records must be non-negative")
        if cache_bytes < 0:
            raise ValueError("cache_bytes must be non-negative")
        self.completed_records = completed_records
        self.cache_bytes = cache_bytes
        self.jobs_root = self.workspace / ".mypr" / "jobs"
        self.jobs_root.mkdir(parents=True, exist_ok=True)
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
        input: str | None = None,
        stdin: bool = False,
        pty: bool = False,
        rows: int = 24,
        cols: int = 80,
    ) -> dict[str, str]:
        if self._closed:
            raise RuntimeError("shell service is closed")
        if input is not None and not isinstance(input, str):
            raise TypeError("input must be a string or None")
        if type(stdin) is not bool:
            raise TypeError("stdin must be a boolean")
        if type(pty) is not bool:
            raise TypeError("pty must be a boolean")
        if pty:
            rows, cols = self._validate_pty_size(rows, cols)
        workdir = self._cwd(cwd)
        merged_env = (
            os.environ.copy()
            if env is None
            else {str(key): str(value) for key, value in env.items()}
        )
        master_fd = slave_fd = None
        try:
            if pty:
                master_fd, slave_fd = os.openpty()
                resize_terminal(slave_fd, rows, cols)
            command_args = self._guard_command(command, pty=pty)
            process = await asyncio.create_subprocess_exec(
                *command_args,
                cwd=workdir,
                env=merged_env,
                stdin=(
                    slave_fd
                    if pty
                    else asyncio.subprocess.PIPE
                    if input is not None or stdin
                    else asyncio.subprocess.DEVNULL
                ),
                stdout=slave_fd if pty else asyncio.subprocess.PIPE,
                stderr=slave_fd if pty else asyncio.subprocess.PIPE,
                start_new_session=True,
            )
        except (OSError, ValueError) as exc:
            close_terminal(master_fd)
            close_terminal(slave_fd)
            job_id = uuid.uuid4().hex
            job = self._new_job(job_id, _NoProcess(), 0, state="failed", error=str(exc))
            job.result = {"returncode": -1}
            self._persist_metadata(job)
            self._jobs[job_id] = job
            self._prune_completed()
            return {"id": job_id}
        finally:
            close_terminal(slave_fd)

        job_id = uuid.uuid4().hex
        job = self._new_job(job_id, process, process.pid)
        self._jobs[job_id] = job
        if pty:
            assert master_fd is not None
            job.pty = True
            job.session_id = process.pid
            job.rows = rows
            job.cols = cols
            job.pty_master = master_fd
            job.pty_reader = _PTYReader(master_fd)
            job.readers = [asyncio.create_task(self._drain(job, job.pty_reader, "stdout"))]
        else:
            assert process.stdout is not None and process.stderr is not None
            job.readers = [
                asyncio.create_task(self._drain(job, process.stdout, "stdout")),
                asyncio.create_task(self._drain(job, process.stderr, "stderr")),
            ]
        if input is not None:
            job.readers.append(asyncio.create_task(self._feed(job, input, eof=not stdin)))
        job.waiter = asyncio.create_task(self._wait(job))
        return {"id": job_id}

    async def write(self, job_id: str, text: str = "", *, eof: bool = False) -> dict:
        if not isinstance(text, str) or type(eof) is not bool:
            raise TypeError("text must be a string and eof must be a boolean")
        job = self._jobs.get(job_id)
        if job is None or job.state not in {"running", "cancelling"}:
            raise ValueError("shell job is not running")
        if job.pty:
            fd = job.pty_master
            if fd is None:
                raise ValueError("shell terminal is closed")
            payload = text.encode("utf-8")
            if eof:
                marker = eof_byte(fd)
                payload += marker * 2
            async with job.stdin_lock:
                try:
                    for start in range(0, len(payload), 16384):
                        await self._write_pty(job, fd, payload[start : start + 16384])
                except OSError as exc:
                    if exc.errno in {errno.EIO, errno.EBADF}:
                        raise ValueError("shell terminal is closed") from exc
                    raise
            return {"id": job_id, "bytes": len(text.encode("utf-8")), "closed": False, "eof": eof}
        stream = job.process.stdin
        if stream is None:
            raise ValueError("shell job was not started with stdin=True")
        async with job.stdin_lock:
            if stream.is_closing():
                raise ValueError("shell stdin is closed")
            try:
                for start in range(0, len(text), 16384):
                    stream.write(text[start : start + 16384].encode("utf-8"))
                    await stream.drain()
            finally:
                if eof:
                    stream.close()
                    with contextlib.suppress(BrokenPipeError, ConnectionResetError):
                        await stream.wait_closed()
        return {"id": job_id, "bytes": len(text.encode("utf-8")), "closed": eof}

    async def _write_pty(self, job: _Job, fd: int, payload: bytes) -> None:
        loop = asyncio.get_running_loop()
        offset = 0
        while offset < len(payload):
            if job.pty_master != fd:
                raise BrokenPipeError("shell terminal is closed")
            try:
                count = os.write(fd, payload[offset:])
            except BlockingIOError:
                ready = loop.create_future()
                job.pty_write_waiter = ready
                loop.add_writer(fd, _wake_future, ready)
                try:
                    await ready
                finally:
                    if job.pty_write_waiter is ready:
                        loop.remove_writer(fd)
                        job.pty_write_waiter = None
                continue
            if count <= 0:
                raise BrokenPipeError("shell terminal is closed")
            offset += count

    async def resize(self, job_id: str, rows: int, cols: int) -> dict[str, int | str]:
        rows, cols = self._validate_pty_size(rows, cols)
        job = self._jobs.get(job_id)
        if job is None or not job.pty or job.pty_master is None:
            raise ValueError("shell job was not started with pty=True")
        if job.state not in {"running", "cancelling"}:
            raise ValueError("shell job is not running")
        try:
            resize_terminal(job.pty_master, rows, cols)
        except OSError as exc:
            raise ValueError(f"unable to resize shell terminal: {exc}") from exc
        job.rows = rows
        job.cols = cols
        return {"id": job_id, "rows": rows, "cols": cols}

    async def _feed(self, job: _Job, text: str, *, eof: bool) -> None:
        try:
            await self.write(job.id, text, eof=eof)
        except BrokenPipeError, ConnectionResetError:
            pass
        except Exception as exc:
            self._add_warning(job, "input_write_failed", exc)

    async def poll(self, job_id: str, cursor: int = 0) -> dict[str, Any]:
        job = self._jobs.get(job_id)
        if job is None:
            return self._poll_persisted(job_id, cursor)
        cursor = self._validate_cursor(cursor, len(job.output))
        return self._poll_job(job, cursor)

    def _poll_persisted(self, job_id: str, cursor: int) -> dict[str, Any]:
        if (
            not isinstance(job_id, str)
            or len(job_id) != 32
            or any(char not in "0123456789abcdef" for char in job_id)
        ):
            raise ValueError("invalid shell job ID")
        metadata_path = self.jobs_root / f"{job_id}.json"
        journal_path = self.jobs_root / f"{job_id}.jsonl"
        try:
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return {
                "id": job_id,
                "state": "unknown",
                "output": [],
                "cursor": cursor,
                "result": None,
                "error": "unknown job",
                "warnings": [],
            }
        if not isinstance(metadata, dict):
            raise RuntimeError("invalid persisted shell metadata")
        output = self._read_journal(journal_path)
        output_count = metadata.get("output_count", len(output))
        if type(output_count) is not int or output_count < len(output):
            output_count = len(output)
        self._validate_cursor(cursor, output_count)
        warnings = metadata.get("warnings", [])
        if not isinstance(warnings, list):
            warnings = []
        warnings = [warning for warning in warnings if isinstance(warning, dict)]
        warnings_truncated = bool(metadata.get("warnings_truncated", False))
        for event in output:
            if event.get("type") == "warning":
                warning = {
                    "code": event.get("code", "journal_warning"),
                    "text": event.get("text", "Output journal warning"),
                }
                if warning not in warnings:
                    if len(warnings) < _MAX_SHELL_WARNINGS:
                        warnings.append(warning)
                    else:
                        warnings_truncated = True
        warnings_truncated |= len(warnings) > _MAX_SHELL_WARNINGS
        return {
            "id": job_id,
            "state": metadata.get("state", "unknown"),
            "output": output[cursor:],
            "cursor": output_count,
            "result": metadata.get("result"),
            "error": metadata.get("error"),
            "truncated": bool(metadata.get("truncated", False)),
            "warnings": warnings[:_MAX_SHELL_WARNINGS],
            "warnings_truncated": warnings_truncated,
            "pty": bool(metadata.get("pty", False)),
            "rows": metadata.get("rows", 24),
            "cols": metadata.get("cols", 80),
        }

    @staticmethod
    def _poll_job(job: _Job, cursor: int) -> dict[str, Any]:
        return {
            "id": job.id,
            "state": job.state,
            "output": job.output[cursor:],
            "cursor": len(job.output),
            "result": job.result,
            "error": job.error,
            "truncated": job.truncated,
            "warnings": list(job.warnings),
            "warnings_truncated": job.warnings_truncated,
            "pty": job.pty,
            "rows": job.rows,
            "cols": job.cols,
        }

    async def cancel(self, job_id: str) -> dict[str, Any]:
        job = self._jobs.get(job_id)
        if job is None:
            return {"id": job_id, "state": "unknown", "error": "unknown job"}
        if job.state not in {"running", "cancelling"}:
            return {"id": job.id, "state": job.state, "result": job.result, "error": job.error}
        job.state = "cancelling"
        self._signal_job(job, signal.SIGTERM)
        killer = asyncio.create_task(self._kill_group_later(job))
        try:
            await asyncio.wait_for(asyncio.shield(job.waiter), timeout=2.0)
        except TimeoutError:
            if job.waiter is not None:
                await asyncio.shield(job.waiter)
        finally:
            if not killer.done() and not self._job_has_live_members(job):
                killer.cancel()
            await asyncio.gather(killer, return_exceptions=True)
        return await self.poll(job.id, len(job.output))

    async def _kill_group_later(self, job: _Job) -> None:
        await asyncio.sleep(2)
        if self._job_has_live_members(job):
            self._signal_job(job, signal.SIGKILL)

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
        for job in self._jobs.values():
            self._close_pty(job)

    def _cwd(self, cwd: str | None) -> str:
        if cwd is None:
            return str(self.workspace)
        path = Path(cwd)
        return str(path if path.is_absolute() else self.workspace / path)

    @staticmethod
    def _validate_pty_size(rows: int, cols: int) -> tuple[int, int]:
        if (
            type(rows) is not int
            or type(cols) is not int
            or not 1 <= rows <= 65535
            or not 1 <= cols <= 65535
        ):
            raise ValueError("rows and cols must be integers between 1 and 65535")
        return rows, cols

    def _new_job(
        self,
        job_id: str,
        process: asyncio.subprocess.Process | _NoProcess,
        group_id: int,
        *,
        state: str = "running",
        error: str | None = None,
    ) -> _Job:
        return _Job(
            job_id,
            process,
            group_id,
            self.output_limit,
            state=state,
            error=error,
            journal=self.jobs_root / f"{job_id}.jsonl",
            metadata=self.jobs_root / f"{job_id}.json",
        )

    def _guard_command(self, command: str | list[str], *, pty: bool = False) -> list[str]:
        if isinstance(command, str):
            original = ["/bin/sh", "-c", command]
        elif command:
            original = [*map(str, command)]
        else:
            raise ValueError("command must not be empty")
        guard = Path(__file__).with_name("process_guard.py")
        args = [
            sys.executable,
            "-I",
            str(guard),
            "--parent-pid",
            str(os.getpid()),
        ]
        if pty:
            args.append("--pty")
        return [*args, "--", *original]

    @staticmethod
    def _validate_cursor(cursor: int, size: int) -> int:
        if isinstance(cursor, bool) or not isinstance(cursor, int):
            raise ValueError("invalid shell output cursor")
        if cursor < 0 or cursor > size:
            raise ValueError("invalid shell output cursor")
        return cursor

    @staticmethod
    def _read_journal(path: Path) -> list[dict[str, str]]:
        try:
            file = path.open("rb")
        except FileNotFoundError:
            return []
        output = []
        with file:
            line_number = 0
            while line := file.readline():
                line_number += 1
                output.append(decode_event(line, line_number))
        return output

    @staticmethod
    def _write_output(job: _Job, event: dict[str, str]) -> None:
        if job.journal is None:
            return
        with job.journal.open("a", encoding="utf-8") as file:
            file.write(json.dumps(event, ensure_ascii=False, separators=(",", ":")) + "\n")

    @staticmethod
    def _write_metadata(job: _Job) -> None:
        if job.metadata is None:
            return
        data = {
            "id": job.id,
            "state": job.state,
            "result": job.result,
            "error": job.error,
            "truncated": job.truncated,
            "warnings": job.warnings,
            "warnings_truncated": job.warnings_truncated,
            "output_count": len(job.output),
            "pty": job.pty,
            "rows": job.rows,
            "cols": job.cols,
        }
        temporary = job.metadata.with_suffix(".tmp")
        temporary.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
        os.replace(temporary, job.metadata)

    def _prune_completed(self) -> None:
        completed = [
            job for job in self._jobs.values() if job.state not in {"running", "cancelling"}
        ]
        completed.sort(key=lambda job: job.finished_at or 0)
        unsaved = [job for job in completed if job.metadata_failed]
        while len(unsaved) > _MAX_UNSAVED_COMPLETED:
            job = unsaved.pop(0)
            self._jobs.pop(job.id, None)
            completed.remove(job)
        memory = sum(job.memory_bytes for job in completed)
        while len(completed) > self.completed_records or memory > self.cache_bytes:
            candidates = [job for job in completed if not job.metadata_failed]
            if not candidates:
                break
            job = candidates[0]
            completed.remove(job)
            memory -= job.memory_bytes
            self._jobs.pop(job.id, None)

    @staticmethod
    def _warning_text(error: BaseException) -> str:
        text = str(error).strip() or error.__class__.__name__
        return text[:_MAX_SHELL_WARNING_TEXT]

    @staticmethod
    def _add_warning(job: _Job, code: str, error: BaseException | str) -> None:
        if len(job.warnings) >= _MAX_SHELL_WARNINGS:
            job.warnings_truncated = True
            return
        text = error if isinstance(error, str) else Shells._warning_text(error)
        job.warnings.append({"code": code, "text": text[:_MAX_SHELL_WARNING_TEXT]})

    @classmethod
    def _persist_output(cls, job: _Job, event: dict[str, str]) -> None:
        if job.journal is None or job.journal_failed:
            return
        try:
            cls._write_output(job, event)
        except Exception as exc:
            job.journal_failed = True
            cls._add_warning(job, "output_persist_failed", exc)

    def _persist_metadata(self, job: _Job) -> None:
        try:
            self._write_metadata(job)
        except Exception as exc:
            job.metadata_failed = True
            self._add_warning(job, "metadata_persist_failed", exc)

    async def _drain(self, job: _Job, stream: Any, name: str) -> None:
        decoder = codecs.getincrementaldecoder("utf-8")("replace")
        while True:
            try:
                chunk = await stream.read(64 * 1024)
            except Exception as exc:
                self._add_warning(job, "output_read_failed", exc)
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
                    event = {"stream": name, "text": text}
                    job.output.append(event)
                    job.memory_bytes += len(json.dumps(event, ensure_ascii=False).encode())
                    self._persist_output(job, event)
            if len(keep) < len(chunk):
                job.truncated = True
        text = decoder.decode(b"", final=True)
        if text and job.output_bytes < job.output_limit:
            event = {"stream": name, "text": text}
            job.output.append(event)
            job.memory_bytes += len(json.dumps(event, ensure_ascii=False).encode())
            self._persist_output(job, event)

    async def _wait(self, job: _Job) -> None:
        try:
            returncode = await job.process.wait()
        except (OSError, ProcessLookupError) as exc:
            job.state = "failed"
            job.error = str(exc)
            job.result = {"returncode": -1}
            job.finished_at = time.time()
            self._persist_metadata(job)
            self._prune_completed()
            self._close_pty(job)
            return
        reader_results = await asyncio.gather(*job.readers, return_exceptions=True)
        for result in reader_results:
            if isinstance(result, Exception):
                self._add_warning(job, "output_reader_failed", result)
        tick = asyncio.Event()
        while self._job_has_live_members(job):
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(tick.wait(), 0.05)
        job.result = {"returncode": returncode}
        if job.state == "cancelling":
            job.state = "cancelled"
        else:
            job.state = "succeeded" if returncode == 0 else "failed"
        job.finished_at = time.time()
        self._persist_metadata(job)
        self._prune_completed()
        self._close_pty(job)

    @staticmethod
    def _close_pty(job: _Job) -> None:
        if job.pty_write_waiter is not None:
            if job.pty_master is not None:
                with contextlib.suppress(OSError):
                    asyncio.get_running_loop().remove_writer(job.pty_master)
            if not job.pty_write_waiter.done():
                job.pty_write_waiter.set_result(None)
            job.pty_write_waiter = None
        if job.pty_reader is not None:
            job.pty_reader.close()
            job.pty_reader = None
        elif job.pty_master is not None:
            close_terminal(job.pty_master)
        job.pty_master = None

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

    @classmethod
    def _signal_job(cls, job: _Job, sig: signal.Signals) -> None:
        if job.pty and job.session_id:
            cls._signal_session(job.session_id, sig)
        else:
            cls._signal_group(job, sig)

    @staticmethod
    def _session_groups(session_id: int) -> set[int]:
        groups: set[int] = set()
        if not session_id:
            return groups
        try:
            entries = Path("/proc").iterdir()
        except OSError:
            return groups
        for entry in entries:
            if not entry.name.isdecimal():
                continue
            try:
                stat = (entry / "stat").read_text(encoding="ascii")
                _, rest = stat.rsplit(") ", 1)
                fields = rest.split()
                if int(fields[3]) == session_id and fields[0] not in {"Z", "X"}:
                    groups.add(int(fields[2]))
            except OSError, ValueError, IndexError:
                continue
        return groups

    @classmethod
    def _signal_session(cls, session_id: int, sig: signal.Signals) -> None:
        groups = sorted(
            cls._session_groups(session_id), key=lambda group_id: group_id == session_id
        )
        for group_id in groups:
            try:
                os.killpg(group_id, sig)
            except ProcessLookupError, PermissionError:
                pass

    @classmethod
    def _job_has_live_members(cls, job: _Job) -> bool:
        if job.pty and job.session_id:
            return bool(cls._session_groups(job.session_id))
        return cls._group_has_live_members(job.group_id)

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
        self._pending: dict[asyncio.Future[Any], asyncio.Task[Any] | None] = {}
        self._operations: set[asyncio.Task[Any]] = set()
        self._closed = False
        self._admissions_blocked = False
        self._initializing = False
        self._initialized = False
        self._ready = asyncio.Event()
        self._ready_error: BaseException | None = None

    @property
    def busy(self) -> bool:
        """Whether initialization or any admitted request is in progress."""

        return self._initializing or bool(self._pending)

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
        self._pending[future] = None
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
        for future, operation in tuple(self._pending.items()):
            if not future.done():
                future.set_exception(error)
            if operation is not None and not operation.done():
                operation.cancel()
        while not self.queue.empty():
            request = self.queue.get_nowait()
            if request is not None:
                self._pending.pop(request.future, None)
        if self.task is not None:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
        await asyncio.gather(*self._operations, return_exceptions=True)
        self._pending.clear()
        self._operations.clear()
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
                try:
                    while True:
                        request = await self.queue.get()
                        if request is None:
                            break
                        if request.future.cancelled():
                            self._pending.pop(request.future, None)
                            continue
                        operation = asyncio.create_task(self._run_request(session, request))
                        self._pending[request.future] = operation
                        self._operations.add(operation)

                        def cancel_operation(
                            future: asyncio.Future[Any], operation: asyncio.Task[Any] = operation
                        ) -> None:
                            if future.cancelled() and not operation.done():
                                operation.cancel()

                        def finish_operation(
                            operation: asyncio.Task[Any],
                            future: asyncio.Future[Any] = request.future,
                        ) -> None:
                            self._operations.discard(operation)
                            self._pending.pop(future, None)

                        request.future.add_done_callback(cancel_operation)
                        operation.add_done_callback(finish_operation)
                finally:
                    await self._finish_operations(RuntimeError("MCP connection closed"))
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
            self._drain_queue(error)
        finally:
            self._drain_queue(RuntimeError("MCP connection closed"))
            self._pending.clear()
            self._operations.clear()

    def _drain_queue(self, error: BaseException) -> None:
        while not self.queue.empty():
            request = self.queue.get_nowait()
            if request is None:
                continue
            self._pending.pop(request.future, None)
            if not request.future.done():
                request.future.set_exception(error)

    async def _finish_operations(self, error: BaseException) -> None:
        for future, operation in tuple(self._pending.items()):
            if not future.done():
                future.set_exception(error)
            if operation is not None and not operation.done():
                operation.cancel()
        await asyncio.gather(*self._operations, return_exceptions=True)
        for future in tuple(self._pending):
            self._pending.pop(future, None)
        self._operations.clear()

    async def _run_request(self, session: ClientSession, request: _Request) -> None:
        try:
            result = await _session_dispatch(session, request.method, request.args)
        except asyncio.CancelledError:
            if not request.future.cancelled() and not request.future.done():
                request.future.set_exception(RuntimeError("MCP connection closed"))
        except Exception as exc:
            if not request.future.done():
                request.future.set_exception(exc)
        else:
            if not request.future.done():
                request.future.set_result(result)
        finally:
            self._pending.pop(request.future, None)

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
            guard = Path(__file__).with_name("process_guard.py")
            args = [
                "-I",
                str(guard),
                "--parent-pid",
                str(os.getpid()),
                "--",
                command,
                *args,
            ]
            command = sys.executable
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
    """Lazy client connections to configured MCP servers."""

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
    raw_cursor = args.get("cursor")
    if raw_cursor is None:
        start = 0
    elif isinstance(raw_cursor, bool):
        raise ValueError("cursor must be a non-negative integer")
    elif isinstance(raw_cursor, int):
        start = raw_cursor
    elif isinstance(raw_cursor, str) and raw_cursor.isdecimal():
        start = int(raw_cursor)
    else:
        raise ValueError("cursor must be a non-negative integer")
    if start < 0:
        raise ValueError("cursor must be a non-negative integer")
    raw_limit = args.get("limit")
    if raw_limit is None:
        limit = 50
    elif (
        isinstance(raw_limit, bool) or not isinstance(raw_limit, int) or not 1 <= raw_limit <= 1000
    ):
        raise ValueError("limit must be an integer between 1 and 1000")
    else:
        limit = raw_limit
    page = names[start : start + limit]
    end = start + len(page)
    return {
        "servers": [
            {"name": name, "transport": "http" if "url" in config[name] else "stdio"}
            for name in page
        ],
        "next_cursor": str(end) if end < len(names) else None,
    }
