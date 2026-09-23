import asyncio
import base64
import contextlib
import copy
import fcntl
import hashlib
import json
import os
import re
import signal
import sqlite3
import sys
import time
import tomllib
import uuid
from collections import OrderedDict
from pathlib import Path

from jupyter_client import AsyncKernelManager
from jupyter_client.kernelspec import KernelSpec

from . import __version__
from .bootstrap import ensure_runtime
from .browser_service import BrowserService
from .diagnostics import safe_error
from .git_api import Git
from .history import History
from .journal import append_events, read_page
from .managed_commands import ManagedCommands
from .messages import MessageStore
from .persistence import PersistenceUnavailable, PersistenceWorker, await_completion
from .protocol import descriptor, runtime_info
from .restart_records import poll_restart
from .scan_service import ScanService
from .search import Search
from .services import MCPBridge, Shells
from .transport import MAX_MESSAGE, socket_path, workspace_id

TERMINAL = {"succeeded", "failed", "cancelled", "lost"}
MCP_MUTATIONS = {"configure", "remove", "restart", "reload"}


def _write_json(path, data):
    temp = path.with_suffix(".tmp")
    temp.write_text(json.dumps(data))
    temp.replace(path)


def _persist_execution(root, history, record, event=None, journal_events=None, history_events=None):
    if journal_events:
        append_events(root / "runs" / f"{record['id']}.jsonl", journal_events)
    if history_events:
        history.append_many("execution", "output", history_events)
    _write_json(root / "runs" / f"{record['id']}.json", record)
    return history.record("execution", record, event=event)


def _load_execution(path):
    return json.loads(path.read_text())


def _persist_task_update(history, journal, kind, record, event=None, output=None, entity_id=None):
    if output is not None and journal is not None:
        append_events(journal, [output["journal"]])
    if output is not None:
        history.append(kind, "output", output["history"])
    return history.record(kind, record, event=event, entity_id=entity_id)


def _open_stores(workspace):
    history = History(workspace)
    try:
        history.recover()
        messages = MessageStore(workspace)
    except BaseException:
        history.close()
        raise
    return history, messages


def _close_stores(history, messages):
    try:
        messages.close()
    finally:
        history.close()


def _recover_runs(root, history):
    for path in (root / "runs").glob("*.json"):
        try:
            old = json.loads(path.read_text())
            if old["state"] not in TERMINAL and old["state"] != "restarting":
                old.update(state="lost", error="Manager stopped before completion")
                _write_json(path, old)
            old.setdefault("client_id", old.get("client") or "legacy")
            history.record("execution", old)
        except ValueError, KeyError:
            continue


class Runtime:
    def __init__(self, workspace):
        self.workspace = Path(workspace).resolve()
        self.root = self.workspace / ".mypr"
        self.root.mkdir(exist_ok=True)
        self.workspace_id = workspace_id(self.workspace)
        self.socket = socket_path(self.workspace)
        self.generation = uuid.uuid4().hex
        self.execs = {}
        self.completed = OrderedDict()
        self.completed_bytes = 0
        self.queue = asyncio.Queue()
        self.active = {}
        self.clients = {}
        self.attachments = {}
        self.history = None
        self.messages = None
        self.persistence = None
        self._admission_lock = asyncio.Lock()
        self._initialize_lock = asyncio.Lock()
        self._lifecycle_lock = asyncio.Lock()
        self._task_locks = {}
        self._persistence_failure_task = None
        self.message_waiters = {}
        self.task_records = {}
        self.shell_watchers = set()
        self.stopping = asyncio.Event()
        self.resetting = False
        self.restarting = None
        self.healthy = False
        self.health_error = None
        self.km = None
        self.kc = None
        self.worker = None
        self.iopub = None
        self.replies = None
        self.core_workers = set()
        self.by_msg = {}
        self.control_waiters = {}
        self.submit_waiters = {}
        self.monitor = None
        self.config = {}
        config = self.root / "config.toml"
        if config.exists():
            self.config = tomllib.loads(config.read_text())
        limits = self.config.get("limits", {})
        self.output_limit = int(limits.get("output_bytes", 16 * 1024 * 1024))
        self.response_limit = int(limits.get("response_bytes", 32768))
        self.completed_tasks = int(limits.get("completed_tasks", 128))
        self.completed_records = int(limits.get("completed_records", 128))
        self.cache_bytes = int(limits.get("cache_bytes", 32 * 1024 * 1024))
        if min(self.completed_tasks, self.completed_records) < 1 or self.cache_bytes < 1024:
            raise ValueError("Retention limits must be positive")
        if min(self.output_limit, self.response_limit) < 1024:
            raise ValueError("Output limits must be at least 1024 bytes")
        self.shells = self.new_shells()
        self.mcp = None
        self.browser = None
        self.scans = None
        self._resource_lock = asyncio.Lock()
        self.search_slots = asyncio.Semaphore(2)
        self.background = set()

    def new_shells(self):
        return Shells(
            self.workspace,
            output_limit=self.output_limit,
            completed_records=self.completed_records,
            cache_bytes=self.cache_bytes // 2,
        )

    def retain_completed(self, kind, rec):
        key = (kind, rec.get("generation"), rec["id"])
        size = len(json.dumps(self.public_record(rec), ensure_ascii=False).encode())
        size += len(json.dumps(rec.get("events", []), ensure_ascii=False).encode())
        self.completed_bytes -= self.completed.pop(key, 0)
        self.completed[key] = size
        self.completed_bytes += size
        while (
            len(self.completed) > self.completed_records
            or self.completed_bytes > self.cache_bytes - self.cache_bytes // 2
        ):
            (old_kind, generation, ident), size = self.completed.popitem(last=False)
            self.completed_bytes -= size
            records = self.execs if old_kind == "execution" else self.task_records
            current = records.get(ident)
            if current is not None and current.get("generation") == generation:
                records.pop(ident, None)

    def critical_done(self, task):
        expected = self.stopping.is_set() or self.resetting
        failed = task.cancelled() or task.exception() is not None
        if not failed and task not in self.core_workers:
            return
        if not failed and expected:
            return
        self.healthy = False
        error = (
            "cancelled"
            if task.cancelled()
            else safe_error(task.exception() or RuntimeError("exited"))
        )
        self.health_error = f"Runtime worker {task.get_name()} failed: {error}"
        self.spawn(self.handle_critical_done(task))

    async def handle_critical_done(self, task):
        if self.stopping.is_set() or self.resetting:
            return
        error = (
            "cancelled"
            if task.cancelled()
            else safe_error(task.exception() or RuntimeError("exited"))
        )
        self.health_error = f"Runtime worker {task.get_name()} failed: {error}"
        print(self.health_error, file=sys.stderr)
        for rec in list(self.execs.values()):
            if rec["state"] not in TERMINAL:
                try:
                    await self.finish(rec, "lost", self.health_error)
                except Exception as exc:
                    print(safe_error(exc), file=sys.stderr)
        try:
            await self.lose_python_tasks(self.health_error)
        except Exception as exc:
            print(safe_error(exc), file=sys.stderr)

    def spawn(self, coro):
        task = asyncio.create_task(coro)
        self.background.add(task)
        task.add_done_callback(self.background.discard)
        return task

    async def prepare(self):
        self.persistence = PersistenceWorker(on_failure=self.persistence_failed)
        self.history, self.messages = await self.persistence.call(_open_stores, self.workspace)
        for name in ["lib/ws_lib", "skills", "runs", "artifacts", "ipython", "jupyter"]:
            (self.root / name).mkdir(parents=True, exist_ok=True)
        (self.root / "lib/ws_lib/__init__.py").touch(exist_ok=True)
        ignore = self.root / ".gitignore"
        entries = ignore.read_text().splitlines() if ignore.exists() else []
        required = [
            "venv/",
            "runs/",
            "artifacts/",
            "jobs/",
            "ipython/",
            "jupyter/",
            "*.json",
            "history.sqlite3*",
            "*.log",
            "*.lock",
            "browser/",
            "scans/",
        ]
        missing = [entry for entry in required if entry not in entries]
        if missing:
            with ignore.open("a") as file:
                if entries:
                    file.write("\n")
                file.write("\n".join(missing) + "\n")
        config = self.root / "config.toml"
        if not config.exists():
            config.write_text("[mcp.servers]\n")
        self.mcp = MCPBridge(self.workspace)
        py = self.root / "venv/bin/python"
        if not py.exists():
            await self.command("uv", "venv", str(self.root / "venv"), "--python", sys.executable)
        await ensure_runtime(py, self.command)
        self.py = py
        self.scans = ScanService(self.workspace, self.shells, self.track_shell)
        await self.io(_recover_runs, self.root, self.history, critical=True)

    async def io(self, function, /, *args, critical=False, **kwargs):
        args = tuple(
            copy.deepcopy(value) if isinstance(value, (dict, list, tuple)) else value
            for value in args
        )
        kwargs = {
            key: copy.deepcopy(value) if isinstance(value, (dict, list, tuple)) else value
            for key, value in kwargs.items()
        }
        try:
            if self.persistence is not None:
                return await self.persistence.call(function, *args, **kwargs)
            return await asyncio.to_thread(function, *args, **kwargs)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if (
                critical
                or isinstance(exc, (PersistenceUnavailable, sqlite3.Error))
                or (isinstance(exc, OSError) and not isinstance(exc, FileNotFoundError))
            ):
                self.persistence_failed(exc)
            raise

    def persistence_failed(self, error):
        message = f"Workspace persistence failed: {safe_error(error)}"
        self.healthy = False
        self.health_error = message
        if self.stopping.is_set():
            return
        if self._persistence_failure_task is None or self._persistence_failure_task.done():
            self._persistence_failure_task = self.spawn(self.mark_persistence_failure(message))

    async def mark_persistence_failure(self, message):
        for rec in list(self.execs.values()):
            async with self.execution_lock(rec):
                if rec["state"] in TERMINAL:
                    continue
                rec.update(state="lost", error=message, finished=time.time())
                self.active.pop(rec["id"], None)
                self.by_msg.pop(rec.get("msg_id"), None)
                rec["done"].set()
                await self.notify_execution_change(rec)
        for ident in list(self.task_records):
            lock = self._task_locks.setdefault(ident, asyncio.Lock())
            async with lock:
                record = self.task_records.get(ident)
                if record is None or record.get("kind") != "python":
                    continue
                if record.get("state") not in TERMINAL:
                    record.update(state="lost", error=message, finished=time.time())

    async def persist_execution(self, record, event=None, journal_events=None, history_events=None):
        data = copy.deepcopy(self.public_record(record))
        return await self.io(
            _persist_execution,
            self.root,
            self.history,
            data,
            event,
            journal_events,
            history_events,
            critical=True,
        )

    async def notify_execution_change(self, rec):
        condition = rec.setdefault("_changed", asyncio.Condition())
        async with condition:
            rec["_revision"] = rec.get("_revision", 0) + 1
            condition.notify_all()

    def execution_lock(self, rec):
        return rec.setdefault("_persist_lock", asyncio.Lock())

    async def command(self, *args):
        proc = await asyncio.create_subprocess_exec(*args, stdout=sys.stderr, stderr=sys.stderr)
        if await proc.wait():
            raise RuntimeError(f"Command failed: {args[0]}")

    def check_persistence(self):
        if self.persistence is not None and not self.persistence.available:
            error = PersistenceUnavailable("Persistence worker is unavailable; restart the manager")
            self.persistence_failed(error)
            raise error

    async def start_kernel(self):
        self.check_persistence()
        env = dict(
            os.environ,
            MYPR_SOCKET=str(self.socket),
            MYPR_WORKSPACE=str(self.workspace),
            MYPR_GENERATION=self.generation,
            MYPR_PARENT_PID=str(os.getpid()),
            MYPR_OUTPUT_LIMIT=str(self.output_limit),
            MYPR_COMPLETED_TASKS=str(self.completed_tasks),
            PYTHONDONTWRITEBYTECODE="1",
            IPYTHONDIR=str(self.root / "ipython"),
            JUPYTER_RUNTIME_DIR=str(self.root / "jupyter"),
        )
        boot = Path(__file__).with_name("kernel_boot.py")
        self.km = AsyncKernelManager(
            autorestart=False,
            transport="ipc",
            ip=str(self.socket.with_suffix(".kernel")),
            connection_file=str(self.root / "kernel.json"),
        )
        self.km._kernel_spec = KernelSpec(
            argv=[str(self.py), str(boot), "-f", "{connection_file}"],
            display_name="mypr",
            language="python",
        )
        await self.km.start_kernel(
            cwd=str(self.workspace), env=env, stdout=sys.stderr, stderr=sys.stderr
        )
        self.kc = self.km.client()
        self.kc.start_channels()
        await self.kc.wait_for_ready(timeout=60)
        self.check_persistence()
        self.healthy = True
        self.health_error = None
        self.by_msg = {}
        self.active = {}
        self.iopub = asyncio.create_task(self.read_output())
        self.replies = asyncio.create_task(self.read_replies())
        self.worker = asyncio.create_task(self.run_queue())
        self.monitor = asyncio.create_task(self.watch_kernel())
        for name in ("iopub", "replies", "worker", "monitor"):
            task = getattr(self, name)
            task.set_name(f"mypr:{name}")
            self.core_workers.add(task)
            task.add_done_callback(self.critical_done)
        self.write_info()

    def workspace_available(self):
        try:
            return workspace_id(self.workspace) == self.workspace_id
        except OSError:
            return False

    def write_info(self):
        (self.root / "runtime.json").write_text(
            json.dumps(
                {
                    "pid": os.getpid(),
                    "socket": str(self.socket),
                    "generation": self.generation,
                    "version": __version__,
                    "workspace": str(self.workspace),
                    "workspace_id": self.workspace_id,
                }
            )
        )

    async def finish(self, rec, state, error=None):
        task = asyncio.create_task(self._finish(rec, state, error))
        return await await_completion(task)

    async def _finish(self, rec, state, error=None):
        async with self.execution_lock(rec):
            if rec["state"] in TERMINAL:
                return
            if error is not None:
                full = str(error)
                error = full.encode(errors="replace")[:1024].decode(errors="ignore")
                error_truncated = rec.get("error_truncated", False) or error != full
            else:
                error_truncated = rec.get("error_truncated", False)
            fields = dict(
                state=state,
                error=error,
                error_truncated=error_truncated,
                finished=time.time(),
            )
            candidate = {**self.public_record(rec), **fields}
            await self.persist_execution(candidate, event=state)
            rec.update(fields)
            self.active.pop(rec["id"], None)
            self.by_msg.pop(rec.get("msg_id"), None)
            rec["done"].set()
            await self.notify_execution_change(rec)
            self.retain_completed("execution", rec)

    async def update_execution(self, rec, fields, event=None):
        task = asyncio.create_task(self._update_execution(rec, fields, event))
        return await await_completion(task)

    async def _update_execution(self, rec, fields, event=None):
        async with self.execution_lock(rec):
            if rec["state"] in TERMINAL:
                return False
            candidate = {**self.public_record(rec), **fields}
            await self.persist_execution(candidate, event=event)
            rec.update(fields)
            if fields.get("state") == "running":
                self.active[rec["id"]] = rec
            if fields.get("state") == "restarting" or "restart_id" in fields:
                await self.notify_execution_change(rec)
            return True

    async def watch_kernel(self):
        while True:
            await asyncio.sleep(1)
            if not await self.km.is_alive():
                self.healthy = False
                await self.lose_python_tasks("Kernel exited")
                self.health_error = "Python kernel exited; use CLI reset"
                for rec in list(self.execs.values()):
                    if rec["state"] not in TERMINAL:
                        await self.finish(rec, "lost", "Python kernel exited; use CLI reset")
                return

    async def run_queue(self):
        while True:
            rec = await self.queue.get()
            if rec["state"] in TERMINAL or self.resetting:
                continue
            if not self.healthy:
                await self.finish(rec, "lost", self.health_error or "Kernel unavailable")
                continue
            try:
                context = {
                    key: rec.get(key) for key in ("client_id", "connection_id", "generation")
                }
                context["exec_id"] = rec["id"]
                msg = self.kc.session.msg(
                    "execute_request",
                    {
                        "code": rec["code"],
                        "silent": False,
                        "store_history": True,
                        "user_expressions": {},
                        "allow_stdin": False,
                        "stop_on_error": False,
                    },
                    metadata={"mypr": context},
                )
                rec["msg_id"] = msg["header"]["msg_id"]
                self.by_msg[rec["msg_id"]] = rec
                await self.update_execution(
                    rec, {"state": "running", "started": time.time()}, event="running"
                )
                submitted = asyncio.get_running_loop().create_future()
                self.submit_waiters[rec["msg_id"]] = submitted
                try:
                    self.kc.shell_channel.send(msg)
                    await submitted
                finally:
                    self.submit_waiters.pop(rec["msg_id"], None)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                await self.finish(rec, "lost", str(exc))

    async def read_replies(self):
        while True:
            reply = await self.kc.get_shell_msg()
            ident = reply.get("parent_header", {}).get("msg_id")
            accepted = self.submit_waiters.get(ident)
            if accepted is not None and not accepted.done():
                accepted.set_result(None)
            waiter = self.control_waiters.get(ident)
            if waiter is not None:
                if not waiter.done():
                    waiter.set_result(reply.get("content", {}))
                continue
            rec = self.by_msg.get(ident)
            if rec is None or rec["generation"] != self.generation:
                continue
            content = reply.get("content", {})
            if content.get("status") == "error":
                await self.finish(rec, "failed", content.get("evalue", "Cell submission failed"))

    async def read_output(self):
        while True:
            msg = await self.kc.get_iopub_msg()
            parent = msg.get("parent_header", {}).get("msg_id")
            rec = self.by_msg.get(parent)
            if rec is None or rec["generation"] != self.generation:
                continue
            kind, content = msg["msg_type"], msg["content"]
            if kind == "mypr_cell":
                if (
                    content.get("exec_id") != rec["id"]
                    or content.get("generation") != self.generation
                    or rec["state"] in TERMINAL
                ):
                    continue
                state = content.get("state")
                if state == "running" and rec["state"] == "queued":
                    await self.update_execution(
                        rec, {"state": "running", "started": time.time()}, event="running"
                    )
                elif state == "reset":
                    rec["idle"].set()
                elif state in {"succeeded", "failed", "cancelled"}:
                    rec["error_truncated"] = content.get("error_truncated", False)
                    await self.finish(rec, state, content.get("error"))
                continue
            if rec["state"] in TERMINAL:
                continue
            event = None
            if kind == "stream":
                event = {"type": "stream", "stream": content["name"], "text": content["text"]}
            elif kind == "error":
                if self.resetting and content.get("ename") == "ResetRequested":
                    continue
                event = {"type": "error", "text": "\n".join(content.get("traceback", []))}
            elif kind in {"execute_result", "display_data", "update_display_data"}:
                event = {"type": "result", "text": content.get("data", {}).get("text/plain", "")}
                artifacts = []
                for mime, value in content.get("data", {}).items():
                    if mime == "text/plain":
                        continue
                    binary = mime in {"image/png", "image/jpeg", "audio/wav"}
                    try:
                        raw = (
                            base64.b64decode(value, validate=True)
                            if binary
                            else json.dumps(value).encode()
                        )
                        if rec["bytes"] + len(raw) > self.output_limit:
                            rec["truncated"] = True
                            continue
                        path = self.root / "artifacts" / uuid.uuid4().hex
                        await self.io(path.write_bytes, raw, critical=True)
                    except (ValueError, TypeError, OSError) as exc:
                        await self.warn(rec, "artifact_error", safe_error(exc, limit=256))
                        continue
                    rec["bytes"] += len(raw)
                    artifacts.append({"mime": mime, "path": str(path)})
                event["artifacts"] = artifacts
            if event:
                if not isinstance(event.get("text"), str):
                    await self.warn(rec, "invalid_output", "text/plain output must be a string")
                    event["text"] = ""
                text = event["text"]
                for start in range(0, max(1, len(text)), 16384):
                    piece = {**event, "text": text[start : start + 16384]}
                    if start:
                        piece.pop("artifacts", None)
                    await self.append(rec, piece)
                    await asyncio.sleep(0)

    async def warn(self, rec, code, text):
        task = asyncio.create_task(self._warn(rec, code, text))
        return await await_completion(task)

    async def _warn(self, rec, code, text):
        async with self.execution_lock(rec):
            warnings = list(rec.get("warnings", []))
            warnings_truncated = rec.get("warnings_truncated", False)
            if len(warnings) < 4:
                warnings.append(
                    {
                        "code": code,
                        "text": text.encode(errors="replace")[:256].decode(errors="ignore"),
                    }
                )
            else:
                warnings_truncated = True
            fields = {"warnings": warnings, "warnings_truncated": warnings_truncated}
            await self.persist_execution({**self.public_record(rec), **fields})
            rec.update(fields)
            await self.notify_execution_change(rec)

    async def append(self, rec, event):
        task = asyncio.create_task(self._append(rec, event))
        return await await_completion(task)

    async def _append(self, rec, event):
        async with self.execution_lock(rec):
            if rec["state"] in TERMINAL:
                return
            raw = event.get("text", "").encode(errors="replace")
            room = max(0, self.output_limit - rec["bytes"])
            truncated = rec["truncated"] or len(raw) > room
            text = raw[:room].decode(errors="ignore")
            saved_bytes = min(len(raw), room)
            if not text and not event.get("artifacts") and truncated == rec["truncated"]:
                return
            step = min(1024, max(1, (self.response_limit - 256) // 12))
            pieces, batch = [], []
            if text or event.get("artifacts"):
                for start in range(0, max(1, len(text)), step):
                    piece = dict(event, text=text[start : start + step])
                    if start:
                        piece.pop("artifacts", None)
                    pieces.append(piece)
                    batch.append(
                        {
                            "id": rec["id"],
                            "exec_id": rec["id"],
                            "client_id": rec.get("client_id"),
                            "connection_id": rec.get("connection_id"),
                            **piece,
                        }
                    )
            fields = {"bytes": rec["bytes"] + saved_bytes, "truncated": truncated}
            candidate = {**self.public_record(rec), **fields}
            await self.persist_execution(
                candidate,
                journal_events=pieces,
                history_events=batch,
            )
            rec["events"].extend(pieces)
            rec.update(fields)
            await self.notify_execution_change(rec)

    async def wait_activity(
        self,
        client,
        wait_seconds,
        done=None,
        *,
        after=None,
        sender=None,
        reply_to=None,
    ):
        notification = asyncio.get_running_loop().create_future() if client else None
        tasks = []
        if notification is not None:
            self.message_waiters.setdefault(client, set()).add(notification)
            tasks.append(notification)
        try:
            if (
                client
                and (
                    await self.io(
                        self.messages.read,
                        client,
                        limit=1,
                        after=after,
                        sender=sender,
                        reply_to=reply_to,
                    )
                )["messages"]
            ):
                return
            tasks.append(asyncio.create_task(self.stopping.wait()))
            if done:
                tasks.append(asyncio.create_task(done.wait()))
            await asyncio.wait(tasks, timeout=wait_seconds, return_when=asyncio.FIRST_COMPLETED)
            if self.stopping.is_set():
                raise RuntimeError("Workspace manager is stopping")
        finally:
            if notification is not None:
                waiters = self.message_waiters[client]
                waiters.discard(notification)
                if not waiters:
                    del self.message_waiters[client]
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def wait_poll_activity(self, rec, revision, client, wait_seconds):
        notification = asyncio.get_running_loop().create_future() if client else None
        tasks = []
        if notification is not None:
            self.message_waiters.setdefault(client, set()).add(notification)
        condition = rec.setdefault("_changed", asyncio.Condition())

        async def changed():
            async with condition:
                await condition.wait_for(lambda: rec.get("_revision", 0) != revision)

        tasks.append(asyncio.create_task(changed()))
        stop_task = asyncio.create_task(self.stopping.wait())
        tasks.append(stop_task)
        if notification is not None:
            tasks.append(notification)
        try:
            if client and (await self.io(self.messages.read, client, limit=1))["messages"]:
                return
            await asyncio.wait(tasks, timeout=wait_seconds, return_when=asyncio.FIRST_COMPLETED)
            if self.stopping.is_set():
                raise RuntimeError("Workspace manager is stopping")
        finally:
            if notification is not None:
                waiters = self.message_waiters.get(client)
                if waiters is not None:
                    waiters.discard(notification)
                    if not waiters:
                        self.message_waiters.pop(client, None)
            for task in tasks:
                if isinstance(task, asyncio.Task):
                    task.cancel()
            await asyncio.gather(
                *(task for task in tasks if isinstance(task, asyncio.Task)),
                return_exceptions=True,
            )

    async def poll(self, ident, cursor=0, wait_ms=1000, *, inbox_client=None, wake_on_output=True):
        if type(cursor) is not int or cursor < 0:
            raise ValueError("Invalid output cursor")
        if (
            not isinstance(ident, str)
            or len(ident) != 32
            or any(c not in "0123456789abcdef" for c in ident)
        ):
            raise ValueError("Invalid execution ID")
        rec = self.execs.get(ident)
        cold = rec is None
        if cold or rec.get("restart_id"):
            restarted = await self.io(poll_restart, self.workspace, ident, cursor)
            if restarted is not None:
                return {**restarted, "generation": self.generation}
        if cold:
            path = self.root / "runs" / f"{ident}.json"
            try:
                rec = await self.io(_load_execution, path)
            except FileNotFoundError as exc:
                raise ValueError("Unknown execution") from exc

        async def page():
            error = rec.get("error")
            error_limit = min(1024, self.response_limit // 4)
            if error is not None:
                error = error.encode(errors="replace")[:error_limit].decode(errors="ignore")
            size = len(json.dumps(error, ensure_ascii=False).encode())
            if cold:
                output, total = await self.io(
                    read_page,
                    self.root / "runs" / f"{ident}.jsonl",
                    cursor,
                    self.response_limit,
                    size,
                )
            else:
                total = len(rec["events"])
                if cursor > total:
                    raise ValueError("Invalid output cursor")
                output = []
                for event in rec["events"][cursor:]:
                    n = len(json.dumps(event, ensure_ascii=False).encode())
                    if output and size + n > self.response_limit:
                        break
                    output.append(event)
                    size += n
            warnings = list(rec.get("warnings", []))
            warnings.extend(
                {"code": event["code"], "text": event["text"]}
                for event in output
                if event.get("type") == "warning"
                and event.get("code") in {"journal_truncated", "journal_corrupt"}
            )
            result = {
                "exec_id": ident,
                "client_id": rec.get("client_id", rec.get("client")),
                "connection_id": rec.get("connection_id"),
                "generation": self.generation,
                "execution_generation": rec["generation"],
                "state": rec["state"],
                "output": output,
                "cursor": cursor + len(output),
                "has_more": cursor + len(output) < total,
                "truncated": rec.get("truncated", False),
                "error": error,
                **(
                    {"error_truncated": True}
                    if rec.get("error_truncated") or error != rec.get("error")
                    else {}
                ),
                **({"warnings": warnings[:4]} if warnings else {}),
                **(
                    {"warnings_truncated": True}
                    if rec.get("warnings_truncated") or len(warnings) > 4
                    else {}
                ),
            }
            return result, total

        result, total = await page()
        wait_seconds = min(30000, max(0, wait_ms)) / 1000
        if (
            not cold
            and wait_seconds
            and (not wake_on_output or cursor >= total)
            and result["state"] not in TERMINAL
        ):
            if wake_on_output:
                revision = rec.get("_revision", 0)
                await self.wait_poll_activity(rec, revision, inbox_client, wait_seconds)
            else:
                await self.wait_activity(inbox_client, wait_seconds, rec["done"])
            result, _ = await page()
        return result

    async def admit_execution(self, client, connection_id, req):
        admission = asyncio.create_task(self._admit_execution(client, connection_id, req))
        return await await_completion(admission)

    async def _admit_execution(self, client, connection_id, req):
        async with self._admission_lock:
            if self.stopping.is_set() or self.resetting or self.restarting or not self.healthy:
                raise RuntimeError("Kernel unavailable; use CLI reset")
            code = req["code"]
            request_id = req.get("request_id")
            old = (
                await self.io(self.history.find_request, client, request_id)
                if request_id is not None
                else None
            )
            if self.stopping.is_set() or self.resetting or self.restarting or not self.healthy:
                raise RuntimeError("Kernel unavailable; use CLI reset")
            if old is not None:
                if old["code"] != code:
                    raise ValueError("request_id already used for different code")
                return {"duplicate": True, "id": old["id"]}
            ident = uuid.uuid4().hex
            rec = dict(
                id=ident,
                generation=self.generation,
                code=code,
                client=client,
                client_id=client,
                connection_id=connection_id,
                request_id=request_id,
                state="queued",
                events=[],
                bytes=0,
                truncated=False,
                done=asyncio.Event(),
                idle=asyncio.Event(),
                created=time.time(),
                _changed=asyncio.Condition(),
                _persist_lock=asyncio.Lock(),
                _revision=0,
            )
            await self.persist_execution(rec, event="queued")
            self.execs[ident] = rec
            if not self.healthy:
                await self.finish(rec, "lost", self.health_error or "Kernel unavailable")
            else:
                self.queue.put_nowait(rec)
            return rec

    async def dispatch(self, req):
        op = req.get("op")
        connection_id = req.get("connection_id")
        result = await self._dispatch(req)
        connection = self.clients.get(connection_id)
        if (
            op in {"init", "execute", "poll"}
            and connection
            and connection["client_id"] is not None
            and self.messages is not None
        ):
            result = {
                **result,
                "inbox": await self.io(self.messages.inbox, connection["client_id"]),
            }
        return result

    async def _dispatch(self, req):
        op = req.pop("op")
        requested_client = req.pop("client_id", None)
        connection_id = req.pop("connection_id", None)
        connection = self.clients.get(connection_id)
        if self.restarting and op in {
            "execute",
            "reset",
            "shell_start",
            "packages_add",
            "scan_start",
            "browser_server",
        }:
            raise RuntimeError(f"Workspace is restarting: {self.restarting}")
        if self.stopping.is_set() and op in {
            "init",
            "execute",
            "shell_start",
            "packages_add",
            "reset",
            "mcp",
        }:
            raise RuntimeError("Workspace manager is stopping")
        if op == "init":
            return await self.initialize_client(connection_id, requested_client)
        if connection:
            if requested_client is not None and connection["client_id"] != requested_client:
                raise ValueError("Connection belongs to another client")
            connection["last_activity"] = time.time()
        client = (connection["client_id"] if connection else requested_client) or "anonymous"
        if op == "status":
            connections = []
            for info in self.clients.values():
                owned = [
                    r["id"]
                    for r in self.task_records.values()
                    if info["client_id"] is not None
                    and r.get("client_id") == info["client_id"]
                    and r["state"] not in TERMINAL
                ]
                active = [
                    rec["id"]
                    for rec in self.active.values()
                    if rec.get("connection_id") == info["connection_id"]
                ]
                connections.append(dict(info, active=active, task_ids=owned))
            return {
                **descriptor(),
                **(
                    runtime_info(descriptor(), connection["target"]["version"])
                    if connection and connection.get("target")
                    else {}
                ),
                "restarting": self.restarting,
                "pid": os.getpid(),
                "workspace_id": self.workspace_id,
                "workspace": str(self.workspace),
                "workspace_available": self.workspace_available(),
                "generation": self.generation,
                "healthy": self.healthy,
                "health_error": self.health_error,
                "resetting": self.resetting,
                "connections": connections,
                "connection_count": len(connections),
                "client_count": len(
                    {c["client_id"] for c in connections if c["client_id"] is not None}
                ),
                "active": list(self.active),
                "queued": [r["id"] for r in self.execs.values() if r["state"] == "queued"],
            }
        if op in {"history_list", "logs"}:
            method = self.history.list if op == "history_list" else self.history.logs
            return await self.io(
                method,
                client_id=req.get("filter_client_id"),
                limit=req.get("limit", 20),
                cursor=req.get("cursor"),
            )
        if op == "history_get":
            record = await self.io(self.history.get, req.get("id", req.get("exec_id")))
            if record is None:
                raise ValueError("Unknown history ID")
            if (
                record["kind"] in {"shell", "package", "scan"}
                and record["id"] in self.task_records
                and record.get("generation") == self.generation
            ):
                record = await self.refresh_shell(self.shells, self.task_records[record["id"]])
            if record["kind"] == "execution":
                page = await self.poll(record["id"], wait_ms=0)
                record.update(
                    {
                        key: page[key]
                        for key in (
                            "output",
                            "cursor",
                            "has_more",
                            "truncated",
                            "warnings",
                            "warnings_truncated",
                        )
                        if key in page
                    }
                )
            elif record["kind"] == "python":
                journal = self.task_journal_path(record)
                if journal is not None and await self.io(journal.is_file):
                    try:
                        output, total = await self.io(read_page, journal, 0, self.response_limit)
                    except (OSError, RuntimeError, ValueError) as exc:
                        record["warnings"] = [
                            {"code": "journal_unavailable", "text": safe_error(exc)}
                        ]
                        record["output_truncated"] = True
                    else:
                        record.update(
                            output=output,
                            cursor=len(output),
                            has_more=len(output) < total,
                            output_truncated=bool(record.get("output_truncated")),
                        )
            return record
        if op == "history_task_read":
            history_id = req.get("id")
            record = await self.io(self.history.get, history_id)
            if record is None or record.get("kind") not in {"python", "execution"}:
                raise ValueError("Unknown Python task history ID")
            expected_history_id = (
                self.task_history_id(record) if record.get("kind") == "python" else record.get("id")
            )
            if expected_history_id != history_id:
                raise ValueError("History ID does not identify this task generation")
            cursor = req.get("cursor", 0)
            if type(cursor) is not int or cursor < 0:
                raise ValueError("Invalid task output cursor")
            budget = req.get("max_bytes", self.response_limit)
            if type(budget) is not int or budget < 0 or budget > self.output_limit:
                raise ValueError("Invalid task output budget")
            journal = (
                self.task_journal_path(record)
                if record.get("kind") == "python"
                else self.root / "runs" / f"{record['id']}.jsonl"
            )
            if journal is None:
                return {
                    "id": record.get("id"),
                    "history_id": expected_history_id,
                    "kind": record.get("kind"),
                    "generation": record.get("generation"),
                    "client_id": record.get("client_id", record.get("client")),
                    "connection_id": record.get("connection_id"),
                    "exec_id": record.get("exec_id"),
                    "output": [],
                    "cursor": cursor,
                    "has_more": False,
                    "truncated": True,
                }
            output, total = await self.io(read_page, journal, cursor, budget)
            return {
                "id": record.get("id"),
                "history_id": expected_history_id,
                "kind": record.get("kind"),
                "generation": record.get("generation"),
                "client_id": record.get("client_id", record.get("client")),
                "connection_id": record.get("connection_id"),
                "exec_id": record.get("exec_id"),
                "output": output,
                "cursor": cursor + len(output),
                "has_more": cursor + len(output) < total,
                "truncated": bool(record.get("output_truncated")),
            }
        generation = req.pop("generation", None)
        if generation and generation != self.generation:
            raise RuntimeError("Expired kernel generation")
        if op == "cell_terminal":
            rec = self.execs.get(req.get("exec_id"))
            if rec is None or rec["generation"] != self.generation:
                raise ValueError("Unknown current cell")
            if req.get("state") == "reset":
                rec["idle"].set()
                return None
            if req.get("state") not in TERMINAL:
                raise ValueError("Expected a terminal cell state")
            rec["error_truncated"] = req.get("error_truncated", False)
            await self.finish(rec, req["state"], req.get("error"))
            return None
        if op in {"message_send", "message_reply", "message_read", "message_ack"}:
            if not (connection and connection["client_id"]) and not requested_client:
                raise RuntimeError("Messages require a client identity")
            if self.stopping.is_set():
                raise RuntimeError("Workspace manager is stopping")
            if op == "message_send":
                task = asyncio.create_task(
                    self.send_message(
                        client,
                        req["to"],
                        req["text"],
                        data=req.get("data"),
                        reply_to=req.get("reply_to"),
                    )
                )
                return await await_completion(task)
            if op == "message_reply":
                task = asyncio.create_task(
                    self.reply_message(
                        client,
                        req["message_id"],
                        req["text"],
                        data=req.get("data"),
                    )
                )
                return await await_completion(task)
            if op == "message_ack":
                return await self.io(self.messages.ack, client, req["ids"])
            wait_ms = req.get("wait_ms", 0)
            if type(wait_ms) is not int or not 0 <= wait_ms <= 30000:
                raise ValueError("wait_ms must be an integer between 0 and 30000")
            deadline = asyncio.get_running_loop().time() + wait_ms / 1000
            while True:
                page = await self.io(
                    self.messages.read,
                    client,
                    limit=req.get("limit", 20),
                    after=req.get("after"),
                    sender=req.get("sender"),
                    reply_to=req.get("reply_to"),
                )
                remaining = deadline - asyncio.get_running_loop().time()
                if page["messages"] or remaining <= 0:
                    return page
                await self.wait_activity(
                    client,
                    remaining,
                    after=req.get("after"),
                    sender=req.get("sender"),
                    reply_to=req.get("reply_to"),
                )

        if op == "execute":
            if not self.workspace_available():
                raise RuntimeError("The workspace moved; stop its manager and reconnect")
            if not self.healthy or self.resetting:
                raise RuntimeError("Kernel unavailable; use CLI reset")
            if connection is None or connection["client_id"] is None:
                raise RuntimeError("Call init on an active connection before execute")
            rec = await self.admit_execution(client, connection_id, req)
            return await self.poll(
                rec["id"],
                wait_ms=req.get("wait_ms", 1000),
                inbox_client=client,
                wake_on_output=False,
            )
        if op == "poll":
            return await self.poll(
                req["exec_id"],
                req.get("cursor") or 0,
                req.get("wait_ms", 1000),
                inbox_client=connection["client_id"] if connection else None,
            )
        if op == "scan_start":
            if self.stopping.is_set() or self.resetting or not self.healthy:
                raise RuntimeError("Workspace is not accepting scans")
            return await self.scans.start(
                req["mode"],
                targets=req["targets"],
                ports=req.get("ports"),
                concurrency=req.get("concurrency", 64),
                rate=req.get("rate", 200),
                timeout=req.get("timeout", 1.0),
                args=req.get("args"),
                client_id=client,
                connection_id=connection_id,
                exec_id=req.get("exec_id"),
            )
        if op == "scan_results":
            return await self.scans.results(
                req["id"],
                cursor=req.get("cursor"),
                max_entries=req.get("max_entries", 100),
                max_bytes=req.get("max_bytes", 32768),
            )
        if op == "scan_summary":
            return await self.scans.summary(req["id"], wait_ms=req.get("wait_ms", 0))
        if op == "scan_cancel":
            return await self.scans.cancel(req["id"])
        if op == "browser_server":
            if self.stopping.is_set() or self.resetting or not self.healthy:
                raise RuntimeError("Workspace is not accepting browser requests")
            async with self._resource_lock:
                if self.browser is None:
                    self.browser = BrowserService(
                        self.workspace,
                        self.py,
                        kernel_pid=self.km.provisioner.pid,
                        generation=self.generation,
                        shells=self.shells,
                    )
                browser_service = self.browser

            def track_install(ident, **fields):
                self.track_shell(ident, client, connection_id, req.get("exec_id"), **fields)

            return await browser_service.ensure(
                req.get("browser", "chromium"),
                launch_options=req.get("launch_options"),
                track=track_install,
            )
        if op in {"search", "git"}:
            if self.stopping.is_set():
                raise RuntimeError("Workspace manager is stopping")
            runner = ManagedCommands(self, client, connection_id, req.get("exec_id"))
            args = req.get("args", {})
            if not isinstance(args, dict):
                raise TypeError("args must be an object")
            if op == "search":
                return await Search(self.workspace, runner).search(**args)
            method = req.get("method")
            if method not in {"status", "diff", "show"}:
                raise ValueError("Unknown Git method")
            return await getattr(Git(self.workspace, runner), method)(**args)
        if op == "shell_start":
            job = await self.shells.start(
                req["command"],
                req.get("cwd", str(self.workspace)),
                req.get("env", dict(os.environ)),
                input=req.get("input"),
                stdin=req.get("stdin", False),
                pty=req.get("pty", False),
                rows=req.get("rows", 24),
                cols=req.get("cols", 80),
            )
            self.track_shell(
                job["id"],
                client,
                connection_id,
                req.get("exec_id"),
                command=req["command"],
                pty=req.get("pty", False),
                **(
                    {"rows": req.get("rows", 24), "cols": req.get("cols", 80)}
                    if req.get("pty")
                    else {}
                ),
            )
            return job
        if op == "shell_poll":
            return await self.shells.poll(req["id"], req.get("cursor", 0))
        if op == "shell_read":
            return await self.shells.read(
                req["id"],
                req.get("cursor", 0),
                stream=req.get("stream"),
                max_bytes=req.get("max_bytes", 32768),
                wait_ms=req.get("wait_ms", 0),
            )
        if op == "shell_wait":
            return await self.shells.wait(req["id"])
        if op == "shell_write":
            return await self.shells.write(
                req["id"], req.get("text", ""), eof=req.get("eof", False)
            )
        if op == "shell_resize":
            return await self.shells.resize(req["id"], req["rows"], req["cols"])
        if op == "shell_cancel":
            return await self.shells.cancel(req["id"])
        if op == "mcp":
            method = req["method"]
            args = req.get("args", {})
            if method not in MCP_MUTATIONS:
                return await self.mcp.dispatch(method, args)
            event = {
                "client_id": client,
                "connection_id": connection_id,
                "exec_id": req.get("exec_id"),
                "server": args.get("server"),
            }
            await self.io(
                self.history.append, "mcp", method, dict(event, state="running"), critical=True
            )
            try:
                result = await self.mcp.dispatch(method, args)
            except Exception as exc:
                await self.io(
                    self.history.append,
                    "mcp",
                    method,
                    dict(event, state="failed", error=str(exc)),
                    critical=True,
                )
                raise
            await self.io(
                self.history.append,
                "mcp",
                method,
                dict(event, state="succeeded"),
                critical=True,
            )
            return result
        if op == "packages_add":
            import shlex

            specs = req["specs"]
            if not specs or any(not s or s.startswith("-") for s in specs):
                raise ValueError("Expected package requirements, not command options")
            command = shlex.join(["uv", "pip", "install", "--python", str(self.py), *specs])
            command += " && " + shlex.join(
                ["uv", "--color", "never", "pip", "freeze", "--python", str(self.py)]
            )
            command += " > " + shlex.quote(str(self.root / "requirements.txt"))
            job = await self.shells.start(command, str(self.workspace), dict(os.environ))
            self.track_shell(
                job["id"], client, connection_id, req.get("exec_id"), kind="package", specs=specs
            )
            return job
        if op in {"task_event", "task_terminal"}:
            event = dict(req["event"])
            if op == "task_terminal" and event.get("state") not in TERMINAL:
                raise ValueError("Expected a terminal task state")
            event.update(
                client_id=client,
                connection_id=connection_id,
                exec_id=req.get("exec_id"),
                generation=self.generation,
            )
            task = asyncio.create_task(self.record_task_event(event, client, connection_id, op))
            return await await_completion(task)
        if op == "restart":
            from .restart import request_restart

            current = self.execs.get(req.get("exec_id"))
            if (
                current is None
                or current["state"] in TERMINAL
                or current["generation"] != self.generation
                or current.get("connection_id") != connection_id
                or generation != self.generation
            ):
                raise RuntimeError("Restart must originate from a running foreground cell")
            if self.restarting or self.resetting:
                raise RuntimeError(
                    f"Workspace restart/reset already in progress: {self.restarting}"
                )
            force = req.get("force", False)
            if type(force) is not bool:
                raise TypeError("force must be a boolean")
            self._check_restart_busy(current, force)
            target = self.clients.get(connection_id, {}).get("target")
            if target is None:
                raise RuntimeError(
                    "This MCP connection cannot restart; reconnect using the new client"
                )
            origin = {
                "exec_id": current["id"],
                "client_id": client,
                "connection_id": connection_id,
                "generation": self.generation,
                "request_id": current.get("request_id"),
            }
            ticket = await request_restart(self.workspace, target, force=force, origin=origin)
            await self._reserve_restart(ticket["id"], current, force)
            return {"accepted": True, "restart_id": ticket["id"]}
        if op == "restart_prepare":
            from .restart import read_ticket

            ident = req.get("restart_id")
            ticket = read_ticket(self.workspace, ident)
            if (
                not ticket
                or ticket.get("old_pid") != os.getpid()
                or ticket.get("old_generation") != self.generation
            ):
                raise RuntimeError("Expired restart request")
            if self.resetting or (self.restarting and self.restarting != ident):
                raise RuntimeError("Workspace restart/reset already in progress")
            origin = ticket.get("origin") or {}
            current = self.execs.get(origin.get("exec_id"))
            self._check_restart_busy(current, ticket.get("force", False))
            await self._reserve_restart(ident, current, ticket.get("force", False))
            return {"prepared": True, "restart_id": ident}
        if op == "reset":
            from_kernel = req.get("from_kernel", False)
            async with self._admission_lock:
                current = self.execs.get(req.get("exec_id")) if from_kernel else None
                if from_kernel and (
                    current is None
                    or current["state"] in TERMINAL
                    or current["generation"] != self.generation
                ):
                    raise RuntimeError("Reset must originate from a running cell")
                busy = any(
                    rec is not current and rec["state"] not in TERMINAL
                    for rec in self.execs.values()
                )
                python_busy = not from_kernel and any(
                    rec["kind"] == "python" and rec["state"] not in TERMINAL
                    for rec in self.task_records.values()
                )
                if self.resetting:
                    raise RuntimeError("Reset already in progress")
                if self.restarting:
                    raise RuntimeError("Workspace restart/reset already in progress")
                if not req.get("force", False) and (busy or python_busy or self.shells.active):
                    raise RuntimeError("Workspace has active work; pass force=True to reset")
                self.resetting = True
            if from_kernel:
                self.spawn(self.reset(current))
                return {"accepted": True}
            await self.reset(None)
            return {"generation": self.generation, "reset": True}
        if op == "stop":
            restart_id = req.get("restart_id")
            planned = restart_id is not None and restart_id == self.restarting
            if planned:
                origin = next(
                    (rec for rec in self.execs.values() if rec.get("restart_id") == restart_id),
                    None,
                )
                if origin is not None:
                    with contextlib.suppress(TimeoutError):
                        await asyncio.wait_for(origin["idle"].wait(), 3)
            async with self._admission_lock:
                planned = restart_id is not None and restart_id == self.restarting
                if restart_id is not None and not planned:
                    raise RuntimeError("Restart reservation does not match")
                if req.get("manager_pid", os.getpid()) != os.getpid():
                    raise RuntimeError("Workspace manager changed before stop")
                if (
                    not planned
                    and not req.get("force")
                    and (
                        any(rec["state"] not in TERMINAL for rec in self.execs.values())
                        or any(
                            rec["kind"] == "python" and rec["state"] not in TERMINAL
                            for rec in self.task_records.values()
                        )
                        or self.shells.active
                    )
                ):
                    raise RuntimeError("Workspace has active work; pass --force")
                self.stopping.set()
            return {"stopping": True, "pid": os.getpid()}
        raise ValueError(f"Unknown operation: {op}")

    async def record_task_event(self, event, client, connection_id, op):
        lock = self._task_locks.setdefault(event["id"], asyncio.Lock())
        async with lock:
            old = self.task_records.get(event["id"])
            if old is None:
                old = await self.io(self.history.get, event["id"]) or {}
            old = copy.deepcopy(old)
            if old.get("generation") != self.generation:
                old = {}
            if old.get("state") in TERMINAL:
                return None
            record = {**old, **event, "kind": "python"}
            delta = event.pop("output_delta", None)
            output_stream = event.pop("output_stream", "stdout")
            record.pop("output_delta", None)
            record.pop("output_stream", None)
            if delta is None and event.get("output") != old.get("output"):
                delta = event.get("output")
            output = None
            journal = self.task_journal_path(record)
            if delta:
                output = {
                    "journal": {
                        "type": "stream",
                        "stream": output_stream,
                        "text": str(delta),
                        "generation": self.generation,
                    },
                    "history": {
                        "id": event["id"],
                        "history_id": self.task_history_id(record),
                        "generation": self.generation,
                        "exec_id": event.get("exec_id"),
                        "client_id": client,
                        "connection_id": connection_id,
                        "stream": output_stream,
                        "text": delta,
                        "truncated": event.get("output_truncated", False),
                    },
                }
            await self.io(
                _persist_task_update,
                self.history,
                journal,
                "python",
                record,
                event["state"] if old.get("state") != event["state"] else None,
                output,
                self.task_history_id(record),
                critical=True,
            )
            self.task_records[event["id"]] = record
            if event["state"] in TERMINAL:
                self.retain_completed("python", record)
            return None

    async def send_message(self, client, to, text, *, data=None, reply_to=None):
        message = await self.io(
            self.messages.send,
            client,
            to,
            text,
            data=data,
            reply_to=reply_to,
        )
        self.notify_message(to)
        return message

    async def reply_message(self, client, message_id, text, *, data=None):
        message = await self.io(
            self.messages.reply,
            client,
            message_id,
            text,
            data=data,
        )
        self.notify_message(message["to"])
        return message

    def notify_message(self, client):
        for waiter in self.message_waiters.get(client, ()):
            if not waiter.done():
                waiter.set_result(None)

    def _check_restart_busy(self, current, force):
        if not force and (
            any(rec is not current and rec["state"] not in TERMINAL for rec in self.execs.values())
            or (
                current is None
                and any(
                    rec["kind"] == "python" and rec["state"] not in TERMINAL
                    for rec in self.task_records.values()
                )
            )
            or self.shells.active
        ):
            raise RuntimeError("Workspace has active work; pass force=True to restart")

    async def _reserve_restart(self, ident, current, force):
        task = asyncio.create_task(self._reserve_restart_locked(ident, current, force))
        return await await_completion(task)

    async def _reserve_restart_locked(self, ident, current, force):
        async with self._admission_lock:
            if self.resetting or self.stopping.is_set():
                raise RuntimeError("Workspace restart/reset already in progress")
            if self.restarting and self.restarting != ident:
                raise RuntimeError("Workspace restart/reset already in progress")
            self._check_restart_busy(current, force)
            was_reserved = self.restarting == ident
            self.restarting = ident
            if current is not None:
                await self.update_execution(
                    current,
                    {"restart_id": ident, "state": "restarting"},
                    event="restarting",
                )
            if not was_reserved:
                self.spawn(self._watch_restart(ident, current))

    async def _watch_restart(self, ident, current):
        from .restart import active_ticket, recover_ticket, wait_ticket

        try:
            while True:
                try:
                    ticket = await wait_ticket(self.workspace, ident)
                    break
                except TimeoutError:
                    if active_ticket(self.workspace) is not None:
                        continue
                    ticket = await recover_ticket(self.workspace)
                    if ticket is None or ticket.get("state") != "failed":
                        raise
                except Exception:
                    if active_ticket(self.workspace) is not None:
                        await asyncio.sleep(0.1)
                        continue
                    ticket = await recover_ticket(self.workspace)
                    if ticket is None or ticket.get("state") != "failed":
                        raise
            if ticket["state"] == "failed" and not self.stopping.is_set():
                if current is not None:
                    saved = await self.io(
                        _load_execution, self.root / "runs" / f"{current['id']}.json"
                    )
                    current.update(
                        {
                            key: saved[key]
                            for key in ("restart_finalized", "restart_result")
                            if key in saved
                        }
                    )
                    await self.finish(current, "failed", ticket.get("error"))
                if self.restarting == ident:
                    self.restarting = None
        except Exception as exc:
            self.health_error = f"Restart monitoring failed: {safe_error(exc)}"

    async def reset(self, current):
        async with self._lifecycle_lock:
            try:
                if self.stopping.is_set():
                    if current is None:
                        raise RuntimeError("Workspace manager is stopping")
                    return
                if current:
                    with contextlib.suppress(TimeoutError):
                        await asyncio.wait_for(current["idle"].wait(), 3)
                for rec in list(self.execs.values()):
                    if rec is not current and rec["state"] not in TERMINAL:
                        await self.finish(rec, "cancelled", "Workspace reset")
                await self.close_kernel()
                await self.lose_python_tasks("Workspace reset", state="cancelled")
                await self.close_shells()
                await self.mcp.close()
                self.mcp = MCPBridge(self.workspace)
                self.shells = self.new_shells()
                self.scans = ScanService(self.workspace, self.shells, self.track_shell)
                self.queue = asyncio.Queue()
                self.generation = uuid.uuid4().hex
                await self.start_kernel()
                if current:
                    await self.append(
                        current, {"type": "result", "text": "Workspace reset completed"}
                    )
                    await self.finish(current, "succeeded")
            except Exception as exc:
                self.healthy = False
                if current:
                    await self.finish(current, "lost", f"Reset failed: {exc}")
                else:
                    raise
            finally:
                self.resetting = False

    async def shutdown_resources(self):
        async with self._lifecycle_lock:
            for rec in list(self.execs.values()):
                if rec["state"] not in TERMINAL and not rec.get("restart_id"):
                    with contextlib.suppress(Exception):
                        await self.finish(
                            rec,
                            "cancelled" if self.restarting else "lost",
                            "Workspace restarted" if self.restarting else "Manager stopped",
                        )
            await self.close_kernel()
            with contextlib.suppress(Exception):
                await self.lose_python_tasks("Manager stopped")
            await self.close_shells()
            await self.mcp.close()

    async def cleanup_kernel_resources(self):
        if not self.kc or not self.km or not self.replies or self.replies.done():
            return
        msg = self.kc.session.msg(
            "execute_request",
            {
                "code": "",
                "silent": True,
                "store_history": False,
                "user_expressions": {},
                "allow_stdin": False,
                "stop_on_error": False,
            },
            metadata={"mypr_control": "cleanup", "generation": self.generation},
        )
        ident = msg["header"]["msg_id"]
        waiter = asyncio.get_running_loop().create_future()
        self.control_waiters[ident] = waiter
        try:
            self.kc.shell_channel.send(msg)
            async with asyncio.timeout(10):
                result = await waiter
            if result.get("status") != "ok":
                raise RuntimeError(result.get("evalue", "Kernel resource cleanup failed"))
        except Exception as exc:
            if self.history:
                with contextlib.suppress(Exception):
                    await self.io(
                        self.history.append,
                        "runtime",
                        "cleanup_warning",
                        {"error": safe_error(exc)},
                        critical=True,
                    )
        finally:
            self.control_waiters.pop(ident, None)

    async def close_kernel(self):
        self.healthy = False
        await self.cleanup_kernel_resources()
        browser, self.browser = self.browser, None
        if browser is not None:
            try:
                async with asyncio.timeout(8):
                    await browser.close()
            except Exception as exc:
                if self.history:
                    with contextlib.suppress(Exception):
                        await self.io(
                            self.history.append,
                            "runtime",
                            "cleanup_warning",
                            {"error": safe_error(exc)},
                            critical=True,
                        )
        for task in [self.worker, self.iopub, self.replies, self.monitor]:
            if task:
                task.cancel()
        await asyncio.gather(
            *(t for t in [self.worker, self.iopub, self.replies, self.monitor] if t),
            return_exceptions=True,
        )
        if self.kc:
            self.kc.stop_channels()
        if self.km:
            with contextlib.suppress(Exception):
                await self.km.shutdown_kernel(now=True)

    @staticmethod
    def public_record(rec):
        return {
            k: v
            for k, v in rec.items()
            if k not in {"done", "idle", "events"} and not k.startswith("_")
        }

    @staticmethod
    def task_history_id(record):
        if record.get("kind") == "python" and record.get("generation"):
            return f"python:{record['generation']}:{record['id']}"
        return None

    def task_journal_path(self, record):
        history_id = self.task_history_id(record)
        if history_id is None:
            return None
        digest = hashlib.sha256(history_id.encode()).hexdigest()
        return self.root / "runs" / f"task-{digest}.jsonl"

    async def initialize_client(self, connection_id, requested_id):
        task = asyncio.create_task(self._initialize_client_locked(connection_id, requested_id))
        return await await_completion(task)

    async def _initialize_client_locked(self, connection_id, requested_id):
        async with self._initialize_lock:
            return await self._initialize_client(connection_id, requested_id)

    async def _initialize_client(self, connection_id, requested_id):
        connection = self.clients.get(connection_id)
        if connection is None:
            raise RuntimeError("Connection is no longer attached")
        if not self.workspace_available():
            raise RuntimeError("The workspace moved; stop its manager and reconnect")
        if requested_id is not None and (
            not isinstance(requested_id, str)
            or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", requested_id)
        ):
            raise ValueError("Client ID must be 1..128 identifier characters")
        current = connection["client_id"]
        if current is not None:
            if requested_id is not None and requested_id != current:
                raise RuntimeError(
                    "This connection is already initialized with a different client ID"
                )
            connection["last_activity"] = time.time()
            return {"client_id": current}
        if requested_id is not None and any(
            info["client_id"] == requested_id for info in self.clients.values()
        ):
            raise RuntimeError("Client ID is already bound to another connection")
        client_id = (
            requested_id
            if requested_id is not None
            else await self.io(self.history.allocate_client_id, critical=True)
        )
        if requested_id is not None:
            await self.io(self.history.reserve_client_id, client_id, critical=True)
        initialized = dict(connection, client_id=client_id, last_activity=time.time())
        await self.io(self.history.append, "connection", "initialized", initialized, critical=True)
        connection.update(initialized)
        return {"client_id": client_id}

    async def attach(self, reader, writer, req):
        if self.stopping.is_set():
            raise RuntimeError("Workspace manager is stopping")
        if not self.workspace_available():
            raise RuntimeError("The workspace moved; stop its manager and reconnect")
        if "client_id" in req:
            raise ValueError("Client IDs are assigned by the workspace manager")
        connection_id = req.get("connection_id")
        if not isinstance(connection_id, str) or not re.fullmatch(
            r"[A-Za-z0-9_.:-]{1,128}", connection_id
        ):
            raise ValueError("Connection ID must be 1..128 identifier characters")
        if connection_id in self.clients:
            raise ValueError("Connection ID already attached")
        info = dict(
            client_id=None,
            connection_id=connection_id,
            connected_at=time.time(),
            last_activity=time.time(),
        )
        target = req.get("target")
        if target is not None:
            if not isinstance(target, dict) or any(
                not isinstance(target.get(key), str)
                for key in ("python", "package_root", "version")
            ):
                raise ValueError("Invalid MCP installation descriptor")
            info["target"] = target
        self.clients[connection_id] = info
        self.attachments[connection_id] = writer
        try:
            await self.io(self.history.append, "connection", "connected", info, critical=True)
            status = await self.dispatch({"op": "status"})
            writer.write(json.dumps({"ok": True, "result": status}).encode() + b"\n")
            await writer.drain()
            await reader.read(1)
        finally:
            self.clients.pop(connection_id, None)
            self.attachments.pop(connection_id, None)
            if self.history:
                with contextlib.suppress(Exception):
                    await self.io(
                        self.history.append,
                        "connection",
                        "disconnected",
                        info,
                        critical=True,
                    )
            writer.close()
            with contextlib.suppress(ConnectionError):
                await writer.wait_closed()

    def track_shell(self, ident, client_id, connection_id, exec_id, kind="shell", **fields):
        record = dict(
            id=ident,
            kind=kind,
            client_id=client_id,
            connection_id=connection_id,
            exec_id=exec_id,
            generation=self.generation,
            created=time.time(),
            state="running",
            **fields,
        )
        self.task_records[ident] = record
        task = self.spawn(self.register_shell(self.shells, record))
        self.shell_watchers.add(task)
        task.add_done_callback(self.shell_watchers.discard)
        task.add_done_callback(self.critical_done)

    async def register_shell(self, shells, record):
        await self.io(self.history.record, record["kind"], record, event="running", critical=True)
        await self.watch_shell(shells, record)

    async def close_shells(self):
        if self.scans is not None:
            await self.scans.close()
        await self.shells.close()
        await asyncio.gather(*list(self.shell_watchers), return_exceptions=True)

    async def watch_shell(self, shells, record):
        cursor = 0
        while True:
            result = await shells.poll(record["id"], cursor)
            for output in result["output"]:
                await self.io(
                    self.history.append,
                    record["kind"],
                    "output",
                    {
                        "id": record["id"],
                        "client_id": record["client_id"],
                        "connection_id": record["connection_id"],
                        "exec_id": record["exec_id"],
                        **output,
                    },
                    critical=True,
                )
            cursor = result["cursor"]
            if result["state"] in TERMINAL:
                await self.refresh_shell(shells, record)
                return
            await asyncio.sleep(0.25)

    async def refresh_shell(self, shells, record):
        full = await shells.poll(record["id"])
        if full["state"] not in TERMINAL:
            return record
        previous = record["state"]
        text = "".join(item["text"] for item in full["output"])
        fields = dict(
            state=full["state"],
            finished=record.get("finished", time.time()),
            error=full.get("error"),
            result=full.get("result"),
            output=text.encode()[:65536].decode(errors="ignore"),
            output_truncated=full.get("truncated") or len(text.encode()) > 65536,
            **({key: full[key] for key in ("pty", "rows", "cols") if key in full}),
            **({"warnings": full["warnings"]} if full.get("warnings") else {}),
            **({"warnings_truncated": True} if full.get("warnings_truncated") else {}),
        )
        candidate = {**record, **fields}
        await self.io(
            self.history.record,
            record["kind"],
            candidate,
            event=full["state"] if previous != full["state"] else None,
            critical=True,
        )
        record.update(fields)
        self.retain_completed(record["kind"], record)
        return record

    async def lose_python_tasks(self, error, state="lost"):
        task = asyncio.create_task(self._lose_python_tasks(error, state))
        return await await_completion(task)

    async def _lose_python_tasks(self, error, state):
        for ident in list(self.task_records):
            lock = self._task_locks.setdefault(ident, asyncio.Lock())
            async with lock:
                record = self.task_records.get(ident)
                if (
                    record is None
                    or record.get("kind") != "python"
                    or record.get("state") in TERMINAL
                ):
                    continue
                candidate = {**record, "state": state, "error": error, "finished": time.time()}
                await self.io(
                    self.history.record,
                    "python",
                    candidate,
                    event=state,
                    entity_id=self.task_history_id(record),
                    critical=True,
                )
                record.update(candidate)
                self.retain_completed("python", record)

    async def drain_background(self):
        current = asyncio.current_task()
        failure = self._persistence_failure_task
        while True:
            pending = [
                task
                for task in self.background
                if task is not current and task is not failure and not task.done()
            ]
            if not pending:
                break
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
        if failure is not None:
            await asyncio.gather(failure, return_exceptions=True)

    async def connection(self, reader, writer):
        try:
            req = json.loads(await reader.readline())
            if req.get("op") == "attach":
                await self.attach(reader, writer, req)
                return
            if req.get("op") in {
                "message_read",
                "execute",
                "poll",
                "search",
                "git",
                "shell_read",
                "shell_wait",
                "history_task_read",
                "browser_server",
                "scan_start",
                "scan_results",
                "scan_summary",
            } or (req.get("op") == "mcp" and req.get("method") not in MCP_MUTATIONS):
                operation = asyncio.create_task(self.dispatch(req))
                disconnected = asyncio.create_task(reader.read(1))
                try:
                    done, _ = await asyncio.wait(
                        [operation, disconnected], return_when=asyncio.FIRST_COMPLETED
                    )
                    if operation not in done:
                        operation.cancel()
                        await asyncio.gather(operation, return_exceptions=True)
                        writer.close()
                        await writer.wait_closed()
                        return
                    result = await operation
                finally:
                    disconnected.cancel()
                    operation.cancel()
                    await asyncio.gather(operation, disconnected, return_exceptions=True)
            else:
                result = await self.dispatch(req)
            response = {"ok": True, "result": result}
        except Exception as exc:
            response = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
        try:
            writer.write(json.dumps(response, ensure_ascii=False).encode() + b"\n")
            await writer.drain()
        except ConnectionError, BrokenPipeError:
            pass
        finally:
            writer.close()
            with contextlib.suppress(ConnectionError):
                await writer.wait_closed()

    async def run(self):
        lock = (self.root / "manager.lock").open("a")
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        os.umask(0o077)
        try:
            await self.prepare()
            self.socket.unlink(missing_ok=True)
            server = await asyncio.start_unix_server(
                self.connection, path=str(self.socket), limit=MAX_MESSAGE
            )
            try:
                await self.start_kernel()
                for sig in (signal.SIGTERM, signal.SIGINT):
                    asyncio.get_running_loop().add_signal_handler(sig, self.stopping.set)
                await self.stopping.wait()
            finally:
                self.stopping.set()
                server.close()
                for writer in list(self.attachments.values()):
                    writer.close()
                await self.shutdown_resources()
                server.close_clients()
                await server.wait_closed()
                await self.drain_background()
        finally:
            for writer in list(self.attachments.values()):
                writer.close()
            self.socket.unlink(missing_ok=True)
            if self.persistence is not None:
                if self.messages is not None and self.history is not None:
                    with contextlib.suppress(Exception):
                        if self.persistence.available:
                            await self.io(
                                _close_stores,
                                self.history,
                                self.messages,
                                critical=True,
                            )
                        else:
                            await self.persistence.close()
                            await asyncio.to_thread(_close_stores, self.history, self.messages)
                    self.messages = None
                    self.history = None
                with contextlib.suppress(Exception):
                    await self.persistence.close()
                self.persistence = None
            lock.close()
