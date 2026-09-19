import asyncio
import base64
import contextlib
import fcntl
import hashlib
import json
import os
import re
import signal
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
from .protocol import descriptor, runtime_info
from .restart_records import poll_restart
from .scan_service import ScanService
from .search import Search
from .services import MCPBridge, Shells
from .transport import MAX_MESSAGE, socket_path, workspace_id

TERMINAL = {"succeeded", "failed", "cancelled", "lost"}
MCP_MUTATIONS = {"configure", "remove", "restart", "reload"}


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
        size = len(json.dumps(self.public_record(rec), ensure_ascii=True).encode())
        size += len(json.dumps(rec.get("events", []), ensure_ascii=True).encode())
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
        if self.stopping.is_set() or self.resetting or not self.healthy:
            return
        error = (
            "cancelled"
            if task.cancelled()
            else safe_error(task.exception() or RuntimeError("exited"))
        )
        self.healthy = False
        self.health_error = f"Runtime worker {task.get_name()} failed: {error}"
        print(self.health_error, file=sys.stderr)
        for rec in list(self.execs.values()):
            if rec["state"] not in TERMINAL:
                try:
                    self.finish(rec, "lost", self.health_error)
                except Exception as exc:
                    print(safe_error(exc), file=sys.stderr)
        try:
            self.lose_python_tasks(self.health_error)
        except Exception as exc:
            print(safe_error(exc), file=sys.stderr)

    def spawn(self, coro):
        task = asyncio.create_task(coro)
        self.background.add(task)
        task.add_done_callback(self.background.discard)
        return task

    async def prepare(self):
        self.history = History(self.workspace)
        self.history.recover()
        self.messages = MessageStore(self.workspace)
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
        for path in (self.root / "runs").glob("*.json"):
            try:
                old = json.loads(path.read_text())
                if old["state"] not in TERMINAL and old["state"] != "restarting":
                    old.update(state="lost", error="Manager stopped before completion")
                    path.write_text(json.dumps(old))
                old.setdefault("client_id", old.get("client") or "legacy")
                self.history.record("execution", old)
            except ValueError, KeyError:
                continue

    async def command(self, *args):
        proc = await asyncio.create_subprocess_exec(*args, stdout=sys.stderr, stderr=sys.stderr)
        if await proc.wait():
            raise RuntimeError(f"Command failed: {args[0]}")

    async def start_kernel(self):
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

    def save(self, rec):
        data = {k: v for k, v in rec.items() if k not in {"done", "idle", "events"}}
        path = self.root / "runs" / f"{rec['id']}.json"
        temp = path.with_suffix(".tmp")
        temp.write_text(json.dumps(data))
        temp.replace(path)
        if self.history is not None:
            self.history.record("execution", data)

    def finish(self, rec, state, error=None):
        if rec["state"] in TERMINAL:
            return
        self.active.pop(rec["id"], None)
        self.by_msg.pop(rec.get("msg_id"), None)
        if error is not None:
            full = str(error)
            error = full.encode(errors="replace")[:1024].decode(errors="ignore")
            rec["error_truncated"] = rec.get("error_truncated", False) or error != full
        rec.update(state=state, error=error, finished=time.time())
        rec["done"].set()
        self.save(rec)
        self.history.record("execution", self.public_record(rec), event=state)
        self.retain_completed("execution", rec)

    async def watch_kernel(self):
        while True:
            await asyncio.sleep(1)
            if not await self.km.is_alive():
                self.healthy = False
                self.lose_python_tasks("Kernel exited")
                self.health_error = "Python kernel exited; use CLI reset"
                for rec in list(self.execs.values()):
                    if rec["state"] not in TERMINAL:
                        self.finish(rec, "lost", "Python kernel exited; use CLI reset")
                return

    async def run_queue(self):
        while True:
            rec = await self.queue.get()
            if rec["state"] in TERMINAL or self.resetting:
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
                rec.update(state="running", started=time.time())
                self.active[rec["id"]] = rec
                self.save(rec)
                self.history.record("execution", self.public_record(rec), event="running")
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
                self.finish(rec, "lost", str(exc))

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
                self.finish(rec, "failed", content.get("evalue", "Cell submission failed"))

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
                    rec.update(state="running", started=time.time())
                    self.active[rec["id"]] = rec
                    self.save(rec)
                    self.history.record("execution", self.public_record(rec), event="running")
                elif state == "reset":
                    rec["idle"].set()
                elif state in {"succeeded", "failed", "cancelled"}:
                    rec["error_truncated"] = content.get("error_truncated", False)
                    self.finish(rec, state, content.get("error"))
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
                        path.write_bytes(raw)
                    except (ValueError, TypeError, OSError) as exc:
                        self.warn(rec, "artifact_error", safe_error(exc, limit=256))
                        continue
                    rec["bytes"] += len(raw)
                    artifacts.append({"mime": mime, "path": str(path)})
                event["artifacts"] = artifacts
            if event:
                if not isinstance(event.get("text"), str):
                    self.warn(rec, "invalid_output", "text/plain output must be a string")
                    event["text"] = ""
                text = event["text"]
                for start in range(0, max(1, len(text)), 16384):
                    piece = {**event, "text": text[start : start + 16384]}
                    if start:
                        piece.pop("artifacts", None)
                    self.append(rec, piece)
                    await asyncio.sleep(0)

    def warn(self, rec, code, text):
        warnings = rec.setdefault("warnings", [])
        if len(warnings) < 4:
            warnings.append(
                {"code": code, "text": text.encode(errors="replace")[:256].decode(errors="ignore")}
            )
        else:
            rec["warnings_truncated"] = True
        self.save(rec)

    def append(self, rec, event):
        raw = event.get("text", "").encode(errors="replace")
        room = max(0, self.output_limit - rec["bytes"])
        was_truncated = rec["truncated"]
        if len(raw) > room:
            rec["truncated"] = True
        text = raw[:room].decode(errors="ignore")
        if not text and not event.get("artifacts"):
            if rec["truncated"] != was_truncated:
                self.save(rec)
            return
        step = min(1024, max(1, (self.response_limit - 256) // 12))
        pieces, batch = [], []
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
        append_events(self.root / "runs" / f"{rec['id']}.jsonl", pieces)
        rec["events"].extend(pieces)
        self.history.append_many("execution", "output", batch)
        rec["bytes"] += min(len(raw), room)
        self.save(rec)

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
                and self.messages.read(
                    client,
                    limit=1,
                    after=after,
                    sender=sender,
                    reply_to=reply_to,
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

    async def poll(self, ident, cursor=0, wait_ms=1000, *, inbox_client=None):
        restarted = await asyncio.to_thread(poll_restart, self.workspace, ident, cursor)
        if restarted is not None:
            return {**restarted, "generation": self.generation}
        cold = ident not in self.execs
        if cold:
            if len(ident) != 32 or any(c not in "0123456789abcdef" for c in ident):
                raise ValueError("Invalid execution ID")
            path = self.root / "runs" / f"{ident}.json"
            if not path.exists():
                raise ValueError("Unknown execution")
            rec = json.loads(path.read_text())
        else:
            rec = self.execs[ident]
            if rec["state"] not in TERMINAL and wait_ms:
                await self.wait_activity(
                    inbox_client, min(30000, max(0, wait_ms)) / 1000, rec["done"]
                )
        if type(cursor) is not int or cursor < 0:
            raise ValueError("Invalid output cursor")
        error = rec.get("error")
        error_limit = min(1024, self.response_limit // 4)
        if error is not None:
            error = error.encode(errors="replace")[:error_limit].decode(errors="ignore")
        size = len(json.dumps(error).encode())
        if cold:
            output, total = await asyncio.to_thread(
                read_page, self.root / "runs" / f"{ident}.jsonl", cursor, self.response_limit, size
            )
        else:
            total = len(rec["events"])
            if cursor > total:
                raise ValueError("Invalid output cursor")
            output = []
            for event in rec["events"][cursor:]:
                n = len(json.dumps(event).encode())
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
        return {
            "exec_id": ident,
            "client_id": rec.get("client_id", rec.get("client")),
            "connection_id": rec.get("connection_id"),
            "generation": self.generation,
            "execution_generation": rec["generation"],
            "state": rec["state"],
            "output": output,
            "cursor": cursor + len(output),
            "has_more": cursor + len(output) < total,
            "truncated": rec["truncated"],
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
            result = {**result, "inbox": self.messages.inbox(connection["client_id"])}
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
            return self.initialize_client(connection_id, requested_client)
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
            return method(
                client_id=req.get("filter_client_id"),
                limit=req.get("limit", 20),
                cursor=req.get("cursor"),
            )
        if op == "history_get":
            record = self.history.get(req.get("id", req.get("exec_id")))
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
                if journal is not None and journal.is_file():
                    try:
                        output, total = await asyncio.to_thread(
                            read_page, journal, 0, self.response_limit
                        )
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
            record = self.history.get(history_id)
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
            output, total = await asyncio.to_thread(read_page, journal, cursor, budget)
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
            self.finish(rec, req["state"], req.get("error"))
            return None
        if op in {"message_send", "message_reply", "message_read", "message_ack"}:
            if not (connection and connection["client_id"]) and not requested_client:
                raise RuntimeError("Messages require a client identity")
            if self.stopping.is_set():
                raise RuntimeError("Workspace manager is stopping")
            if op == "message_send":
                message = self.messages.send(
                    client,
                    req["to"],
                    req["text"],
                    data=req.get("data"),
                    reply_to=req.get("reply_to"),
                )
                for waiter in self.message_waiters.get(req["to"], ()):
                    if not waiter.done():
                        waiter.set_result(None)
                return message
            if op == "message_reply":
                message = self.messages.reply(
                    client,
                    req["message_id"],
                    req["text"],
                    data=req.get("data"),
                )
                for waiter in self.message_waiters.get(message["to"], ()):
                    if not waiter.done():
                        waiter.set_result(None)
                return message
            if op == "message_ack":
                return self.messages.ack(client, req["ids"])
            wait_ms = req.get("wait_ms", 0)
            if type(wait_ms) is not int or not 0 <= wait_ms <= 30000:
                raise ValueError("wait_ms must be an integer between 0 and 30000")
            deadline = asyncio.get_running_loop().time() + wait_ms / 1000
            while True:
                page = self.messages.read(
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
            code = req["code"]
            request_id = req.get("request_id")
            old = self.history.find_request(client, request_id) if request_id is not None else None
            if old is not None:
                if old["code"] != code:
                    raise ValueError("request_id already used for different code")
                return await self.poll(
                    old["id"], wait_ms=req.get("wait_ms", 1000), inbox_client=client
                )
            else:
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
                )
                self.execs[ident] = rec
                self.save(rec)
                self.history.record("execution", self.public_record(rec), event="queued")
                self.queue.put_nowait(rec)
            return await self.poll(rec["id"], wait_ms=req.get("wait_ms", 1000), inbox_client=client)
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
            self.history.append("mcp", method, dict(event, state="running"))
            try:
                result = await self.mcp.dispatch(method, args)
            except Exception as exc:
                self.history.append("mcp", method, dict(event, state="failed", error=str(exc)))
                raise
            self.history.append("mcp", method, dict(event, state="succeeded"))
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
            old = self.task_records.get(event["id"])
            if old is None:
                old = self.history.get(event["id"]) or {}
            if old.get("generation") != self.generation:
                old = {}
            if old.get("state") in TERMINAL:
                return None
            record = {**old, **event, "kind": "python"}
            self.task_records[event["id"]] = record
            delta = event.pop("output_delta", None)
            output_stream = event.pop("output_stream", "stdout")
            record.pop("output_delta", None)
            record.pop("output_stream", None)
            if delta is None and event.get("output") != old.get("output"):
                delta = event.get("output")
            if delta:
                journal = self.task_journal_path(record)
                if journal is not None:
                    try:
                        append_events(
                            journal,
                            [
                                {
                                    "type": "stream",
                                    "stream": output_stream,
                                    "text": str(delta),
                                    "generation": self.generation,
                                }
                            ],
                        )
                    except (OSError, RuntimeError) as exc:
                        self.warn(record, "task_output_persist_failed", safe_error(exc))
                self.history.append(
                    "python",
                    "output",
                    {
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
                )
            self.history.record(
                "python",
                record,
                event=event["state"] if old.get("state") != event["state"] else None,
                entity_id=self.task_history_id(record),
            )
            if event["state"] in TERMINAL:
                self.retain_completed("python", record)
            return None
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
            self._reserve_restart(ticket["id"], current)
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
            self._reserve_restart(ident, current)
            return {"prepared": True, "restart_id": ident}
        if op == "reset":
            from_kernel = req.get("from_kernel", False)
            current = self.execs.get(req.get("exec_id")) if from_kernel else None
            if from_kernel and (
                current is None
                or current["state"] in TERMINAL
                or current["generation"] != self.generation
            ):
                raise RuntimeError("Reset must originate from a running cell")
            busy = any(
                rec is not current and rec["state"] not in TERMINAL for rec in self.execs.values()
            )
            # Kernel callers check live handles; their history reports may lag.
            python_busy = not from_kernel and any(
                rec["kind"] == "python" and rec["state"] not in TERMINAL
                for rec in self.task_records.values()
            )
            if self.resetting:
                raise RuntimeError("Reset already in progress")
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
            if restart_id is not None and not planned:
                raise RuntimeError("Restart reservation does not match")
            if planned:
                origin = next(
                    (rec for rec in self.execs.values() if rec.get("restart_id") == restart_id),
                    None,
                )
                if origin is not None:
                    with contextlib.suppress(TimeoutError):
                        await asyncio.wait_for(origin["idle"].wait(), 3)
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

    def _reserve_restart(self, ident, current):
        if current is not None:
            current.update(restart_id=ident, state="restarting")
            self.save(current)
        if self.restarting != ident:
            self.restarting = ident
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
                    saved = json.loads((self.root / "runs" / f"{current['id']}.json").read_text())
                    current.update(
                        {
                            key: saved[key]
                            for key in ("restart_finalized", "restart_result")
                            if key in saved
                        }
                    )
                    self.finish(current, "failed", ticket.get("error"))
                if self.restarting == ident:
                    self.restarting = None
        except Exception as exc:
            self.health_error = f"Restart monitoring failed: {safe_error(exc)}"

    async def reset(self, current):
        try:
            if current:
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(current["idle"].wait(), 3)
            for rec in list(self.execs.values()):
                if rec is not current and rec["state"] not in TERMINAL:
                    self.finish(rec, "cancelled", "Workspace reset")
            await self.close_kernel()
            self.lose_python_tasks("Workspace reset", state="cancelled")
            await self.close_shells()
            await self.mcp.close()
            self.mcp = MCPBridge(self.workspace)
            self.shells = self.new_shells()
            self.scans = ScanService(self.workspace, self.shells, self.track_shell)
            self.queue = asyncio.Queue()
            self.generation = uuid.uuid4().hex
            await self.start_kernel()
            if current:
                self.append(current, {"type": "result", "text": "Workspace reset completed"})
                self.finish(current, "succeeded")
        except Exception as exc:
            self.healthy = False
            if current:
                self.finish(current, "lost", f"Reset failed: {exc}")
            else:
                raise
        finally:
            self.resetting = False

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
                self.history.append("runtime", "cleanup_warning", {"error": safe_error(exc)})
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
                    self.history.append("runtime", "cleanup_warning", {"error": safe_error(exc)})
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
        return {k: v for k, v in rec.items() if k not in {"done", "idle", "events"}}

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

    def initialize_client(self, connection_id, requested_id):
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
        client_id = requested_id if requested_id is not None else self.history.allocate_client_id()
        if requested_id is not None:
            self.history.reserve_client_id(client_id)
        initialized = dict(connection, client_id=client_id, last_activity=time.time())
        self.history.append("connection", "initialized", initialized)
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
            self.history.append("connection", "connected", info)
            status = await self.dispatch({"op": "status"})
            writer.write(json.dumps({"ok": True, "result": status}).encode() + b"\n")
            await writer.drain()
            await reader.read(1)
        finally:
            self.clients.pop(connection_id, None)
            self.attachments.pop(connection_id, None)
            if self.history:
                self.history.append("connection", "disconnected", info)
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
        self.history.record(kind, record, event="running")
        task = self.spawn(self.watch_shell(self.shells, record))
        self.shell_watchers.add(task)
        task.add_done_callback(self.shell_watchers.discard)

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
                self.history.append(
                    record["kind"],
                    "output",
                    {
                        "id": record["id"],
                        "client_id": record["client_id"],
                        "connection_id": record["connection_id"],
                        "exec_id": record["exec_id"],
                        **output,
                    },
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
        record.update(
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
        self.history.record(
            record["kind"], record, event=full["state"] if previous != full["state"] else None
        )
        self.retain_completed(record["kind"], record)
        return record

    def lose_python_tasks(self, error, state="lost"):
        for record in list(self.task_records.values()):
            if record["kind"] == "python" and record["state"] not in TERMINAL:
                record.update(state=state, error=error, finished=time.time())
                self.history.record(
                    "python",
                    record,
                    event=state,
                    entity_id=self.task_history_id(record),
                )
                self.retain_completed("python", record)

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
                for rec in list(self.execs.values()):
                    if rec["state"] not in TERMINAL and not rec.get("restart_id"):
                        self.finish(
                            rec,
                            "cancelled" if self.restarting else "lost",
                            "Workspace restarted" if self.restarting else "Manager stopped",
                        )
                await self.close_kernel()
                self.lose_python_tasks("Manager stopped")
                await self.close_shells()
                await self.mcp.close()
                server.close_clients()
                await server.wait_closed()
        finally:
            for writer in list(self.attachments.values()):
                writer.close()
            self.socket.unlink(missing_ok=True)
            if self.messages:
                self.messages.close()
                self.messages = None
            if self.history:
                self.history.close()
                self.history = None
            lock.close()
