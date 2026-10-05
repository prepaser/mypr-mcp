import asyncio
import base64
import contextlib
import copy
import fcntl
import hashlib
import json
import math
import os
import re
import signal
import sqlite3
import sys
import time
import uuid
from collections import OrderedDict
from pathlib import Path
from weakref import WeakValueDictionary

from jupyter_client import AsyncKernelManager
from jupyter_client.kernelspec import KernelSpec

from . import __version__
from .async_utils import wait_owned
from .bootstrap import install_core, prepare_core, run_command
from .browser_service import BrowserService, validate_launch_options
from .config import MAX_WAIT_MS, ConfigStore
from .config_runtime import RuntimeConfig
from .dependency_service import DependencyService
from .diagnostics import RPCError, error_info, error_response, safe_error
from .file_io import open_regular, read_bytes
from .git_api import Git
from .history import History
from .journal import append_events, read_page
from .mail_service import MailService
from .managed_commands import ManagedCommands
from .messages import MessageStore
from .persistence import PersistenceUnavailable, PersistenceWorker, await_completion
from .protocol import descriptor, runtime_info
from .python_dependencies import package_environment
from .restart_records import poll_restart
from .scan_service import ScanService
from .search import Search
from .services import MCPBridge, Shells
from .storage import Storage
from .task_results import load_result, store_result
from .timers import TimerStore
from .timings import Timings
from .transport import MAX_MESSAGE, ensure_workspace_identity, socket_path, workspace_id
from .web_service import WebService

TERMINAL = {"succeeded", "failed", "cancelled", "lost"}
MCP_MUTATIONS = {"configure", "remove", "restart", "reload"}
LIFECYCLE_START_OPS = {"shell_start", "packages_add", "scan_start", "browser_server"}
TIMING_OPS = {
    "init",
    "status",
    "performance",
    "history_list",
    "logs",
    "history_get",
    "history_task_read",
    "cell_terminal",
    "message_send",
    "message_reply",
    "message_read",
    "message_ack",
    "timer_start",
    "timer_check",
    "timer_list",
    "timer_cancel",
    "timer_ack",
    "execute",
    "poll",
    "scan_start",
    "scan_results",
    "scan_summary",
    "scan_cancel",
    "browser_server",
    "search",
    "git",
    "shell_start",
    "shell_poll",
    "shell_read",
    "shell_wait",
    "shell_write",
    "shell_resize",
    "shell_cancel",
    "mcp",
    "mail",
    "web",
    "packages_add",
    "dependencies",
    "task_event",
    "task_terminal",
    "restart",
    "restart_prepare",
    "reset",
    "stop",
}
BRIDGE_TIMINGS = {"bridge_ready", "manager_rpc", "bridge_total"}
COMMAND_TIMEOUT = 180
_UNHANDLED = object()


def _write_json(path, data):
    temp = path.with_suffix(".tmp")
    temp.write_text(json.dumps(data))
    temp.replace(path)


def _persist_execution(
    root,
    history,
    record,
    event=None,
    journal_events=None,
    history_events=None,
    *,
    workspace=None,
    workspace_identity=None,
):
    ensure_workspace_identity(workspace or Path(root).parent, workspace_identity)
    if journal_events:
        append_events(root / "runs" / f"{record['id']}.jsonl", journal_events)
    if history_events:
        history.append_many("execution", "output", history_events)
    _write_json(root / "runs" / f"{record['id']}.json", record)
    return history.record("execution", record, event=event)


def _load_execution(path):
    with open_regular(path) as stream:
        raw = stream.read(MAX_MESSAGE + 1)
    if len(raw) > MAX_MESSAGE:
        raise ValueError("execution record exceeds its size limit")
    record = json.loads(raw)
    if not isinstance(record, dict) or not isinstance(record.get("state"), str):
        raise ValueError("execution record must be an object with a string state")
    return record


def _persist_task_update(
    history,
    journal,
    kind,
    record,
    event=None,
    output=None,
    entity_id=None,
    *,
    workspace=None,
    workspace_identity=None,
):
    if workspace is None and journal is not None:
        workspace = Path(journal).parents[2]
    ensure_workspace_identity(workspace, workspace_identity)
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


def _close_stores(history, messages, timers=None):
    try:
        if timers is not None:
            timers.close()
    finally:
        try:
            messages.close()
        finally:
            history.close()


def _store_task_result(workspace, history, record, encoded, *, workspace_identity=None):
    ensure_workspace_identity(workspace, workspace_identity)

    def commit(reference):
        ensure_workspace_identity(workspace, workspace_identity)
        history.record(
            "python", dict(record, result_ref=reference, result_persisted=True),
            entity_id=f"python:{record['generation']}:{record['id']}",
        )
    return store_result(
        workspace, record["id"], record["generation"], encoded,
        commit=commit, workspace_identity=workspace_identity,
    )


def _recover_runs(root, history):
    for path in (root / "runs").glob("*.json"):
        try:
            old = _load_execution(path)
            if old["state"] not in TERMINAL and old["state"] != "restarting":
                old.update(state="lost", error="Manager stopped before completion")
                _write_json(path, old)
            old.setdefault("client_id", old.get("client") or "legacy")
            existing = None
            get_history = getattr(history, "get", None)
            if callable(get_history):
                existing = get_history(old["id"])
            if existing is not None and existing.get("body_evicted") is True:
                code = old.get("code")
                compact = {
                    key: value
                    for key, value in old.items()
                    if key not in {"code", "output", "events"}
                }
                if isinstance(code, str):
                    compact["code_sha256"] = hashlib.sha256(code.encode()).hexdigest()
                compact["body_evicted"] = True
                if "body_evicted_at" in existing:
                    compact["body_evicted_at"] = existing["body_evicted_at"]
                if compact != old:
                    _write_json(path, compact)
                old = compact
                if any(field in existing for field in {"code", "output", "events"}):
                    history.record("execution", old, preserve_updated=True)
                    existing = history.get(old["id"])
            finished = old.get("finished")
            existing_finished = existing.get("finished") if existing is not None else None
            newer = (
                isinstance(finished, (int, float))
                and not isinstance(finished, bool)
                and math.isfinite(finished)
                and (
                    not isinstance(existing_finished, (int, float))
                    or isinstance(existing_finished, bool)
                    or not math.isfinite(existing_finished)
                    or finished > existing_finished
                )
            )
            if (
                existing is not None
                and existing.get("state") == old.get("state")
                and not newer
            ):
                continue
            kwargs = {}
            if (
                isinstance(finished, (int, float))
                and not isinstance(finished, bool)
                and math.isfinite(finished)
            ):
                kwargs["updated_at"] = finished
            history.record("execution", old, **kwargs)
        except (OSError, ValueError, KeyError, TypeError, OverflowError) as exc:
            print(f"Skipping execution record {path.name}: {safe_error(exc)}", file=sys.stderr)
            continue


class Runtime:
    def __init__(self, workspace):
        self.workspace = Path(workspace).resolve()
        self.root = self.workspace / ".mypr"
        self.root.mkdir(exist_ok=True)
        self.workspace_id = workspace_id(self.workspace)
        self.socket = socket_path(self.workspace)
        self.generation = uuid.uuid4().hex
        self.timings = Timings(
            labels={
                *(f"dispatch.{op}" for op in TIMING_OPS),
                *(f"bridge.{name}" for name in BRIDGE_TIMINGS),
                "kernel.queue",
                "kernel.roundtrip",
            }
        )
        self.execs = {}
        self.completed = OrderedDict()
        self.completed_bytes = 0
        self.queue = asyncio.Queue()
        self.active = {}
        self.clients = {}
        self.attachments = {}
        self.history = None
        self.messages = None
        self.timers = None
        self.mail = None
        self.web = None
        self.dependencies = None
        self.persistence = None
        self._admission_lock = asyncio.Lock()
        self._initialize_lock = asyncio.Lock()
        self._lifecycle_lock = asyncio.Lock()
        self._task_locks = WeakValueDictionary()
        self._persistence_failure_task = None
        self.message_waiters = {}
        self.timer_waiters = {}
        self.task_records = {}
        self.shell_watchers = set()
        self.stopping = asyncio.Event()
        self.resetting = False
        self.restarting = None
        self.healthy = False
        self.health_error = None
        self.registry_error = None
        self._registry_generation = None
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
        self.config_store = ConfigStore(
            self.workspace, workspace_guard=self._ensure_workspace_identity
        )
        snapshot = self.config_store.load()
        self._kernel_snapshot = snapshot
        self.config = snapshot.values
        self.settings = RuntimeConfig(self, self.config_store, snapshot)
        self.storage_policy = self.settings.applied["storage"].copy()
        self.storage = None
        self.storage_maintenance = {"running": False, "last_run": None, "last_error": None}
        self._storage_wake = asyncio.Event()
        limits = self.config.get("limits", {})
        self.output_limit = int(limits.get("output_bytes", 16 * 1024 * 1024))
        self.response_limit = int(limits.get("response_bytes", 32768))
        self.execute_wait_ms = limits["execute_wait_ms"]
        self.poll_wait_ms = limits["poll_wait_ms"]
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

    def connected_client_ids(self):
        return {info["client_id"] for info in self.clients.values() if info["client_id"]}

    def storage_active_ids(self):
        records = [*self.execs.values(), *self.task_records.values()]
        active = {record["id"] for record in records if record.get("state") not in TERMINAL}
        if self.restarting:
            active.add(self.restarting)
        return active

    async def storage_call(self, method, **fields):
        if self.storage is None:
            raise RuntimeError("Workspace storage is not ready")
        task = asyncio.create_task(getattr(self.storage, method)(**fields))
        return await await_completion(task)

    async def maintain_storage(self):
        first = True
        while not self.stopping.is_set():
            if not first:
                changed = await self._wait_storage_interval()
                if self.stopping.is_set():
                    return
                if changed:
                    continue
            first = False
            if (
                self.storage_policy["enabled"] and self.healthy
                and not self.resetting and not self.restarting
            ):
                self.storage_maintenance["running"] = True
                try:
                    result = await self.storage_call(
                        "gc", dry_run=False,
                        older_than_days=self.storage_policy["retention_days"],
                        max_bytes=self.storage_policy["max_bytes"],
                        revision_keep=self.storage_policy["revision_keep"],
                    )
                    self.storage_maintenance.update(
                        last_run=time.time(), last_error=None,
                        last_deleted_bytes=result.get("deleted_bytes", 0),
                    )
                except Exception as exc:
                    self.storage_maintenance.update(
                        last_run=time.time(), last_error=safe_error(exc)
                    )
                finally:
                    self.storage_maintenance["running"] = False

    async def _wait_storage_interval(self):
        stop = asyncio.create_task(self.stopping.wait())
        change = asyncio.create_task(self._storage_wake.wait())
        try:
            done, _ = await asyncio.wait(
                [stop, change], timeout=self.storage_policy["gc_interval_seconds"],
                return_when=asyncio.FIRST_COMPLETED,
            )
            changed = change in done
            if changed:
                self._storage_wake.clear()
            return changed
        finally:
            stop.cancel()
            change.cancel()
            await wait_owned(asyncio.gather(stop, change, return_exceptions=True), propagate=False)

    async def code_config(self, req, *, generation=None):
        if self.mcp is None:
            raise RuntimeError("Workspace configuration is not ready")
        method = req.get("method")
        if method == "get_lsp":
            return await self.mcp.get_lsp()
        if method == "set_lsp":
            async with self._workspace_config_admission(generation):
                if self.settings.applying:
                    raise RuntimeError(
                        "Configuration reload is in progress; retry after it completes"
                    )
                named = (
                    {"name": req["name"], "definition": req.get("definition")}
                    if "name" in req else {}
                )
                return await self.mcp.save_lsp(
                    req.get("definitions"),
                    req.get("expected_servers"),
                    **named,
                )
        if method == "applied_lsp":
            from .lsp_config import validate_servers

            generation = req.get("generation", generation)
            sequence = req.get("sequence")
            current_sequence = getattr(self.settings, "lsp_sequence", 0)
            current_generation = getattr(self, "generation", None)
            if generation != current_generation or type(sequence) is not int or sequence < 0:
                return {
                    "recorded": False,
                    "generation": current_generation,
                    "sequence": current_sequence,
                }
            recorded = self.settings.record_lsp(
                validate_servers(req.get("definitions")),
                sequence=sequence,
                generation=generation,
            )
            return {
                "recorded": recorded,
                "generation": current_generation,
                "sequence": getattr(self.settings, "lsp_sequence", sequence),
            }
        raise ValueError("Unknown workspace configuration operation")

    @contextlib.asynccontextmanager
    async def _workspace_config_admission(self, generation):
        async with self._admission_lock:
            self._check_workspace_config_mutation(generation)
            yield

    def _check_workspace_config_mutation(self, generation):
        if generation and generation != self.generation:
            raise RuntimeError("Expired kernel generation")
        if self.stopping.is_set() or self.resetting or self.restarting:
            raise RuntimeError("Workspace is not accepting configuration changes")
        if not self.workspace_available():
            raise RuntimeError("The workspace moved; stop its manager and reconnect")
        self.mcp._ensure_workspace_identity()

    def new_shells(self):
        return Shells(
            self.workspace,
            output_limit=self.output_limit,
            completed_records=self.completed_records,
            cache_bytes=self.cache_bytes // 2,
        )

    def new_dependencies(self, config=None):
        return DependencyService(
            self.workspace, self.py,
            self.config.get("dependencies", {}) if config is None else config,
            self._install_dependency_packages, self._install_dependency_browser,
            self._record_dependency,
        )

    def retain_completed(self, kind, rec):
        key = (kind, rec.get("generation"), rec["id"])
        size = len(json.dumps(self.public_record(rec), ensure_ascii=False).encode())
        size += len(json.dumps(rec.get("events", []), ensure_ascii=False).encode())
        self.completed_bytes -= self.completed.pop(key, 0)
        self.completed[key] = size
        self.completed_bytes += size
        self._trim_completed()

    def _trim_completed(self):
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
        self.timers = await self.persistence.call(TimerStore, self.workspace)
        self.mail = MailService(
            self.workspace, self.config.get("mail", {}), self.io,
            self.notify_message, self.connected_client_ids,
        )
        await self.mail.start()
        self.web = WebService(self.config.get("web", {}), self._record_web)
        self.storage = Storage(
            self.workspace, history=self.history, mail=self.mail,
            active_ids=self.storage_active_ids,
        )
        for name in ["lib/ws_lib", "skills", "runs", "artifacts", "ipython", "jupyter"]:
            (self.root / name).mkdir(parents=True, exist_ok=True)
        (self.root / "lib/ws_lib/__init__.py").touch(exist_ok=True)
        ignore = self.root / ".gitignore"
        try:
            entries = read_bytes(ignore, max_bytes=16 * 1024 * 1024).decode().splitlines()
        except FileNotFoundError:
            entries = []
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
            "revisions/",
            "document-results/",
            "git-history/",
            "rewrites/",
            "html-results/",
            "task-results/",
            "change-plans/",
            "mail/",
        ]
        missing = [entry for entry in required if entry not in entries]
        if missing:
            with open_regular(ignore, "ab") as file:
                file.write((("\n" if entries else "") + "\n".join(missing) + "\n").encode())
        self.mcp = MCPBridge(
            self.workspace, global_path=self.config_store.global_path,
            snapshot=self._kernel_snapshot,
        )
        py = self.root / "venv/bin/python"
        if not py.exists():
            await self.command("uv", "venv", str(self.root / "venv"), "--python", sys.executable)
        self.py = py
        self.dependencies = self.new_dependencies()
        bin_root = str(self.dependencies.store.bin_root)
        search_path = os.environ.get("PATH", os.defpath).split(os.pathsep)
        if bin_root not in search_path:
            os.environ["PATH"] = os.pathsep.join([*search_path, bin_root])
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
                self.observe_kernel_roundtrip(rec)
                self.active.pop(rec["id"], None)
                self.by_msg.pop(rec.get("msg_id"), None)
                rec["done"].set()
                await self.notify_execution_change(rec)
        for ident in list(self.task_records):
            lock = self.task_lock(ident)
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
            workspace=self.workspace,
            workspace_identity=self.workspace_id,
            critical=True,
        )

    async def notify_execution_change(self, rec):
        condition = rec.setdefault("_changed", asyncio.Condition())
        async with condition:
            rec["_revision"] = rec.get("_revision", 0) + 1
            condition.notify_all()

    def execution_lock(self, rec):
        return rec.setdefault("_persist_lock", asyncio.Lock())

    def task_lock(self, ident):
        return self._task_locks.setdefault(ident, asyncio.Lock())

    async def command(self, *args, env=None):
        await run_command(*args, env=env, timeout_seconds=COMMAND_TIMEOUT)

    def check_persistence(self):
        if self.persistence is not None and not self.persistence.available:
            error = PersistenceUnavailable("Persistence worker is unavailable; restart the manager")
            self.persistence_failed(error)
            raise error

    async def start_kernel(self):
        self.check_persistence()
        if self.dependencies is not None:
            await prepare_core(self.dependencies)
        self.settings.reset_lsp_generation(self.generation)
        env = dict(
            os.environ,
            MYPR_SOCKET=str(self.socket),
            MYPR_WORKSPACE=str(self.workspace),
            MYPR_GENERATION=self.generation,
            MYPR_PARENT_PID=str(os.getpid()),
            MYPR_OUTPUT_LIMIT=str(self.output_limit),
            MYPR_COMPLETED_TASKS=str(self.completed_tasks),
            MYPR_GLOBAL_CONFIG=str(self.config_store.global_path),
            PYTHONDONTWRITEBYTECODE="1",
            IPYTHONDIR=str(self.root / "ipython"),
            JUPYTER_RUNTIME_DIR=str(self.root / "jupyter"),
        )
        boot = Path(__file__).with_name("kernel_boot.py")
        km = AsyncKernelManager(
            autorestart=False,
            transport="ipc",
            ip=str(self.socket.with_suffix(".kernel")),
            connection_file=str(self.root / "kernel.json"),
        )
        km._kernel_spec = KernelSpec(
            argv=[str(self.py), str(boot), "-f", "{connection_file}"],
            display_name="mypr",
            language="python",
        )
        kc = None
        try:
            self.km = km
            await km.start_kernel(
                cwd=str(self.workspace), env=env, stdout=sys.stderr, stderr=sys.stderr
            )
            kc = km.client()
            self.kc = kc
            kc.start_channels()
            await kc.wait_for_ready(timeout=60)
            self.check_persistence()
            self.by_msg = {}
            self.active = {}
            self.iopub = asyncio.create_task(self.read_output())
            self.replies = asyncio.create_task(self.read_replies())
            applied = await self.settings._apply_lsp(
                self._kernel_snapshot, self.generation, False, starting=True,
            )
            if applied["applied"] is not True:
                raise RuntimeError("Kernel startup configuration was deferred")
            recorded = self.settings.record_lsp(
                self._kernel_snapshot.values["lsp"]["servers"],
                sequence=applied.get("sequence"),
                generation=applied.get("generation", self.generation),
            )
            if not recorded:
                raise RuntimeError("Kernel startup returned an outdated LSP configuration")
            self.healthy = True
            self.health_error = None
            self.worker = asyncio.create_task(self.run_queue())
            self.monitor = asyncio.create_task(self.watch_kernel())
            for name in ("iopub", "replies", "worker", "monitor"):
                task = getattr(self, name)
                task.set_name(f"mypr:{name}")
                self.core_workers.add(task)
                task.add_done_callback(self.critical_done)
            self.write_info()
        except BaseException as exc:
            self.healthy = False
            self.health_error = f"Python kernel startup failed: {safe_error(exc)}"
            await wait_owned(self._cleanup_failed_kernel(km, kc), propagate=False)
            raise

    async def _cleanup_failed_kernel(self, km, kc):
        tasks = [self.worker, self.iopub, self.replies, self.monitor]
        for task in tasks:
            if task is not None and not task.done():
                task.cancel()
        await asyncio.gather(*(task for task in tasks if task is not None), return_exceptions=True)
        for task in tasks:
            if task is not None:
                self.core_workers.discard(task)
        if kc is not None:
            with contextlib.suppress(Exception):
                kc.stop_channels()
        with contextlib.suppress(Exception):
            await km.shutdown_kernel(now=True)
        if self.km is km:
            self.km = None
        if self.kc is kc:
            self.kc = None
        self.worker = None
        self.iopub = None
        self.replies = None
        self.monitor = None
        self.by_msg.clear()
        self.control_waiters.clear()
        self.submit_waiters.clear()

    def workspace_available(self):
        try:
            return workspace_id(self.workspace) == self.workspace_id
        except OSError:
            return False

    def _ensure_workspace_identity(self):
        if not self.workspace_available():
            raise RuntimeError("The workspace moved; stop its manager and reconnect")

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
            waiter = self.submit_waiters.get(rec.get("msg_id"))
            if waiter is not None and not waiter.done():
                waiter.set_result(None)
            if rec["state"] in TERMINAL:
                return
            structured_error = rec.get("error_info")
            if error is not None:
                full = str(error)
                error = full.encode(errors="replace")[:1024].decode(errors="ignore")
                error_truncated = rec.get("error_truncated", False) or error != full
                if not isinstance(structured_error, dict):
                    structured_error = error_info(error, operation="execute")
            else:
                error_truncated = rec.get("error_truncated", False)
            fields = dict(
                state=state,
                error=error,
                error_truncated=error_truncated,
                finished=time.time(),
                **({"error_info": structured_error} if structured_error is not None else {}),
            )
            candidate = {**self.public_record(rec), **fields}
            try:
                await self.persist_execution(candidate, event=state)
            finally:
                if state in TERMINAL:
                    self.observe_kernel_roundtrip(rec)
            rec.update(fields)
            self.active.pop(rec["id"], None)
            self.by_msg.pop(rec.get("msg_id"), None)
            rec["done"].set()
            await self.notify_execution_change(rec)
            self.retain_completed("execution", rec)

    def observe_kernel_roundtrip(self, rec):
        sent_at = rec.get("_kernel_sent_at")
        if sent_at is None or rec.get("_kernel_roundtrip_recorded"):
            return
        rec["_kernel_roundtrip_recorded"] = True
        self.timings.observe("kernel.roundtrip", time.perf_counter() - sent_at)

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
                    sent_at = time.perf_counter()
                    self.kc.shell_channel.send(msg)
                    rec["_kernel_sent_at"] = sent_at
                    self.timings.observe(
                        "kernel.queue", sent_at - rec.get("_admitted_at", sent_at)
                    )
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
            content = reply.get("content", {})
            snapshot = (
                content.get("_mypr_applied_lsp")
                if isinstance(content, dict)
                else None
            )
            if snapshot is not None:
                with contextlib.suppress(Exception):
                    self.settings.reconcile_late_lsp(snapshot)
            accepted = self.submit_waiters.get(ident)
            if accepted is not None and not accepted.done():
                accepted.set_result(None)
            waiter = self.control_waiters.get(ident)
            if waiter is not None:
                if not waiter.done():
                    waiter.set_result(content)
                continue
            content = reply.get("content", {})
            snapshot = content.get("_mypr_applied_lsp")
            if snapshot is not None:
                with contextlib.suppress(Exception):
                    self.settings.reconcile_late_lsp(snapshot)
                continue
            rec = self.by_msg.get(ident)
            if rec is None or rec["generation"] != self.generation:
                continue
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
                    if isinstance(content.get("error_info"), dict):
                        rec["error_info"] = content["error_info"]
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
                        path = self.root / "artifacts" / rec["id"] / uuid.uuid4().hex
                        await self.io(path.parent.mkdir, parents=True, exist_ok=True, critical=True)
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

    async def wait_notifications(self, tasks, client, wait_seconds, *, include_timers=False):
        if include_timers and client and self.mail is not None:
            if await self.io(self.mail.snapshot, client):
                return
        if not include_timers or not client or self.timers is None:
            await asyncio.wait(tasks, timeout=wait_seconds, return_when=asyncio.FIRST_COMPLETED)
            return
        loop = asyncio.get_running_loop()
        deadline = loop.time() + wait_seconds
        while True:
            changed = loop.create_future()
            self.timer_waiters.setdefault(client, set()).add(changed)
            try:
                due_at = await self.io(self.timers.next_deadline, client)
                remaining = max(0.0, deadline - loop.time())
                timeout = remaining
                if due_at is not None:
                    timeout = min(timeout, max(0.0, due_at - time.time()))
                if timeout <= 0:
                    return
                ready, _ = await asyncio.wait(
                    [*tasks, changed], timeout=timeout, return_when=asyncio.FIRST_COMPLETED
                )
                if any(task in ready for task in tasks):
                    return
            finally:
                waiters = self.timer_waiters.get(client)
                if waiters is not None:
                    waiters.discard(changed)
                    if not waiters:
                        self.timer_waiters.pop(client, None)
                changed.cancel()

    async def wait_activity(
        self,
        client,
        wait_seconds,
        done=None,
        *,
        after=None,
        sender=None,
        reply_to=None,
        include_timers=False,
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
            await self.wait_notifications(
                tasks, client, wait_seconds, include_timers=include_timers
            )
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
            await self.wait_notifications(tasks, client, wait_seconds, include_timers=True)
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

    def response_budget(self, max_bytes):
        if max_bytes is None:
            return self.response_limit
        if type(max_bytes) is not int or not 1024 <= max_bytes <= 1024 * 1024:
            raise ValueError("max_bytes must be between 1024 and 1048576 bytes")
        return max_bytes

    @staticmethod
    def wait_budget(wait_ms, default):
        if wait_ms is None:
            wait_ms = default
        if type(wait_ms) is not int or not 0 <= wait_ms <= MAX_WAIT_MS:
            raise ValueError(f"wait_ms must be an integer between 0 and {MAX_WAIT_MS}")
        return wait_ms

    async def poll(
        self,
        ident,
        cursor=0,
        wait_ms=None,
        *,
        max_bytes=None,
        inbox_client=None,
        wake_on_output=True,
    ):
        self._ensure_workspace_identity()
        wait_ms = self.wait_budget(wait_ms, self.poll_wait_ms)
        explicit_budget = max_bytes is not None
        response_budget = self.response_budget(max_bytes)
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
        if cold:
            path = self.root / "runs" / f"{ident}.json"
            try:
                rec = await self.io(_load_execution, path)
            except FileNotFoundError as exc:
                raise ValueError("Unknown execution") from exc
            except (OSError, ValueError, TypeError) as exc:
                raise RPCError(
                    "Saved execution metadata is corrupt or unreadable",
                    code="execution_metadata_corrupt",
                    details={"exec_id": ident, "outcome_unknown": True},
                ) from exc
            if (
                rec.get("id", ident) != ident
                or not isinstance(rec.get("generation"), str)
                or not rec["generation"]
                or rec["state"] not in TERMINAL | {"queued", "running", "cancelling", "restarting"}
            ):
                raise RPCError(
                    "Saved execution metadata is invalid",
                    code="execution_metadata_corrupt",
                    details={"exec_id": ident, "outcome_unknown": True},
                )
            get_history = getattr(self.history, "get", None)
            if callable(get_history):
                durable = await self.io(get_history, ident)
                if isinstance(durable, dict) and (
                    durable.get("output_evicted") or durable.get("scan_output_evicted")
                ):
                    rec = {
                        **rec,
                        "output_evicted": True,
                        "scan_output_evicted": bool(durable.get("scan_output_evicted")),
                    }
        if rec.get("restart_id"):
            from .restart import recover_ticket

            await recover_ticket(self.workspace, rec["restart_id"])
        if cold or rec.get("restart_id"):
            restarted = await self.io(
                poll_restart, self.workspace, ident, cursor, max_bytes=max_bytes
            )
            if restarted is not None:
                if explicit_budget:
                    restart_error = restarted.get("error")
                    if restart_error is not None:
                        error_limit = min(1024, response_budget // 4)
                        restart_error = restart_error.encode(errors="replace")[
                            :error_limit
                        ].decode(errors="ignore")
                    error_size = len(
                        json.dumps(restart_error, ensure_ascii=False).encode()
                    )
                    bounded = []
                    size = error_size
                    for event in restarted.get("output", []):
                        event_size = len(json.dumps(event, ensure_ascii=False).encode())
                        if not bounded and size + event_size > response_budget:
                            raise ValueError(
                                f"Output event at cursor {cursor} requires "
                                f"{size + event_size} bytes; increase max_bytes to "
                                "continue (cursor unchanged)"
                            )
                        if bounded and size + event_size > response_budget:
                            break
                        bounded.append(event)
                        size += event_size
                    restarted = {
                        **restarted,
                        "error": restart_error,
                        "output": bounded,
                        "cursor": cursor + len(bounded),
                        "has_more": bool(restarted.get("has_more"))
                        or len(bounded) < len(restarted.get("output", [])),
                    }
                return {**restarted, "generation": self.generation}

        async def page():
            error = rec.get("error")
            error_limit = min(1024, response_budget // 4)
            if error is not None:
                error = error.encode(errors="replace")[:error_limit].decode(errors="ignore")
            size = len(json.dumps(error, ensure_ascii=False).encode())
            output_evicted = bool(
                rec.get("output_evicted") or rec.get("scan_output_evicted")
            )
            if output_evicted:
                output, total = [], cursor
            elif cold:
                output, total = await self.io(
                    read_page,
                    self.root / "runs" / f"{ident}.jsonl",
                    cursor,
                    response_budget,
                    size,
                )
                if explicit_budget and output:
                    first_size = len(json.dumps(output[0], ensure_ascii=False).encode())
                    if size + first_size > response_budget:
                        raise ValueError(
                            f"Output event at cursor {cursor} requires {size + first_size} "
                            f"bytes; increase max_bytes to continue (cursor unchanged)"
                        )
            else:
                total = len(rec["events"])
                if cursor > total:
                    raise ValueError("Invalid output cursor")
                output = []
                for event in rec["events"][cursor:]:
                    n = len(json.dumps(event, ensure_ascii=False).encode())
                    if not output and explicit_budget and size + n > response_budget:
                        raise ValueError(
                            f"Output event at cursor {cursor} requires {size + n} bytes; "
                            f"increase max_bytes to continue (cursor unchanged)"
                        )
                    if output and size + n > response_budget:
                        break
                    output.append(event)
                    size += n
            warnings = list(rec.get("warnings", []))
            if output_evicted:
                warning = {
                    "code": "output_expired",
                    "text": "Retained task output has expired and is no longer available.",
                }
                if warning not in warnings:
                    warnings.append(warning)
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
                **(
                    {"error_info": rec["error_info"]}
                    if isinstance(rec.get("error_info"), dict)
                    else {}
                ),
                **({"warnings": warnings[:4]} if warnings else {}),
                **(
                    {"warnings_truncated": True}
                    if rec.get("warnings_truncated") or len(warnings) > 4
                    else {}
                ),
                **({"output_evicted": True} if output_evicted else {}),
            }
            if output_evicted:
                result["truncated"] = True
            return result, total

        result, total = await page()
        wait_seconds = wait_ms / 1000
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
                await self.wait_activity(
                    inbox_client, wait_seconds, rec["done"], include_timers=True
                )
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
                original = old.get("code")
                stored_digest = old.get("code_sha256")
                if not isinstance(original, str) and (
                    not isinstance(stored_digest, str)
                    or re.fullmatch(r"[0-9a-f]{64}", stored_digest) is None
                ):
                    raise RPCError(
                        "Cannot verify request deduplication: execution source is unavailable",
                        code="history_corrupt", details={"exec_id": old["id"]},
                    )
                matches = (
                    original == code if isinstance(original, str)
                    else stored_digest == hashlib.sha256(code.encode()).hexdigest()
                )
                if not matches:
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
            rec["_admitted_at"] = time.perf_counter()
            self.execs[ident] = rec
            if not self.healthy:
                await self.finish(rec, "lost", self.health_error or "Kernel unavailable")
            else:
                self.queue.put_nowait(rec)
            return rec

    async def dispatch(self, req):
        op = req.get("op")
        bridge_sample = req.pop("_bridge_sample", None)
        if isinstance(bridge_sample, dict):
            for name, milliseconds in bridge_sample.items():
                if name not in BRIDGE_TIMINGS or isinstance(milliseconds, bool):
                    continue
                try:
                    milliseconds = float(milliseconds)
                except (OverflowError, TypeError, ValueError):
                    continue
                if math.isfinite(milliseconds) and milliseconds >= 0:
                    self.timings.observe(f"bridge.{name}", milliseconds / 1000)
        connection_id = req.get("connection_id")
        started = time.perf_counter()
        try:
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
                if self.timers is not None:
                    timers = await self.io(self.timers.notifications, connection["client_id"])
                    if timers is not None:
                        result["timers"] = timers
                if self.mail is not None:
                    mail = await self.io(self.mail.snapshot, connection["client_id"])
                    if mail is not None:
                        result["mail"] = mail
        finally:
            elapsed = time.perf_counter() - started
            label = op if op in TIMING_OPS else "other"
            self.timings.observe(f"dispatch.{label}", elapsed)
        if op in {"init", "execute", "poll"} and isinstance(result, dict):
            result = {
                **result,
                "_timing_ms": {"manager_dispatch": round(elapsed * 1000, 3)},
            }
        return result

    def performance_snapshot(self):
        samples = self.timings.snapshot()
        manager = {
            name.removeprefix("dispatch."): value
            for name, value in samples.items()
            if name.startswith("dispatch.")
        }
        bridge = {
            name.removeprefix("bridge."): value
            for name, value in samples.items()
            if name.startswith("bridge.")
        }
        storage = (
            self.persistence.performance_snapshot()
            if self.persistence is not None
            else {"queue_depth": 0, "queue_capacity": 128, "queue_wait": None, "work": None}
        )
        return {
            "manager_dispatch": manager,
            "bridge": bridge,
            "storage": storage,
            "kernel": {
                "queue_ms": samples.get("kernel.queue"),
                "roundtrip_ms": samples.get("kernel.roundtrip"),
                "roundtrip_semantics": (
                    "manager-observed send-to-finish, including event and persistence delay"
                ),
            },
        }

    async def _dispatch(self, req):
        op = req.pop("op")
        requested_client = req.pop("client_id", None)
        connection_id = req.pop("connection_id", None)
        connection = self.clients.get(connection_id)
        self._check_dispatch_admission(op)
        if op == "init":
            return await self.initialize_client(connection_id, requested_client)
        if connection:
            await self._update_connection(connection, requested_client, op)
        client = (connection["client_id"] if connection else requested_client) or "anonymous"
        if op == "performance":
            return self.performance_snapshot()
        if op == "status":
            return self._dispatch_status(req, connection, op)
        result = await self._dispatch_history(op, req)
        if result is not _UNHANDLED:
            return result
        generation = req.pop("generation", None)
        if generation and generation != self.generation:
            raise RuntimeError("Expired kernel generation")
        if op == "config":
            return await self.settings.dispatch(req)
        context = dict(client=client, connection_id=connection_id, connection=connection,
                       requested_client=requested_client, generation=generation)
        if op == "browser_server":
            browser_name = req.get("browser", "chromium")
            launch_options, executable_path, channel = validate_launch_options(
                browser_name, req.get("launch_options")
            )
            async with self._admission_lock:
                self._check_dispatch_admission(op)
                if generation and generation != self.generation:
                    raise RuntimeError("Expired kernel generation")
                admission_generation = self.generation
                if (
                    self.stopping.is_set()
                    or self.resetting
                    or not self.healthy
                    or not self.workspace_available()
                ):
                    raise RuntimeError("Workspace is not accepting browser requests")
                async with self._resource_lock:
                    if (
                        admission_generation != self.generation
                        or self.stopping.is_set()
                        or self.resetting
                        or not self.healthy
                        or not self.workspace_available()
                    ):
                        raise RuntimeError("Workspace is not accepting browser requests")
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

            dependency_service = getattr(self, "dependencies", None)
            if dependency_service is not None:
                if executable_path is None and channel is None:
                    await dependency_service.ensure(
                        [f"browser:{browser_name}"], automatic=True,
                        context={"client_id": client, "connection_id": connection_id,
                                 "exec_id": req.get("exec_id"),
                                 "generation": admission_generation},
                    )
            options = {"launch_options": launch_options, "track": track_install}
            if dependency_service is not None:
                options["install"] = False
            result = await browser_service.ensure(browser_name, **options)
            async with self._admission_lock:
                if (
                    admission_generation != self.generation
                    or self.stopping.is_set()
                    or self.resetting
                    or not self.healthy
                    or not self.workspace_available()
                    or self.browser is not browser_service
                ):
                    raise RuntimeError("Workspace is not accepting browser requests")
            return result
        if op in LIFECYCLE_START_OPS:
            async with self._admission_lock:
                self._check_dispatch_admission(op)
                if generation and generation != self.generation:
                    raise RuntimeError("Expired kernel generation")
                if op in {"shell_start", "scan_start"} and not self.workspace_available():
                    raise RuntimeError("The workspace moved; stop its manager and reconnect")
                return await self._dispatch_handlers(op, req, context)
        return await self._dispatch_handlers(op, req, context)

    async def _dispatch_handlers(self, op, req, context):
        for handler in (
            self._dispatch_execution, self._dispatch_messages, self._dispatch_timers,
            self._dispatch_mail, self._dispatch_web, self._dispatch_storage,
            self._dispatch_dependencies,
            self._dispatch_tasks, self._dispatch_scan, self._dispatch_tools,
            self._dispatch_shell, self._dispatch_mcp, self._dispatch_lifecycle,
        ):
            result = await handler(op, req, **context)
            if result is not _UNHANDLED:
                return result
        raise ValueError(f"Unknown operation: {op}")

    def _check_dispatch_admission(self, op):
        settings = getattr(self, "settings", None)
        if (
            settings is not None and settings.applying
            and op in {"reset", "stop", "restart", "restart_prepare"}
        ):
            raise RuntimeError("Configuration reload is in progress; retry after it completes")
        if getattr(self, "resetting", False) and op in LIFECYCLE_START_OPS:
            raise RuntimeError("Workspace restart/reset already in progress")
        if self.restarting and op in {
            "execute", "reset", "shell_start", "packages_add",
            "scan_start", "browser_server",
        }:
            raise RuntimeError(f"Workspace is restarting: {self.restarting}")
        if self.stopping.is_set() and (
            op in {"init", "execute", "reset", "mcp"} or op in LIFECYCLE_START_OPS
        ):
            raise RuntimeError("Workspace manager is stopping")

    async def _update_connection(self, connection, requested_client, op):
        if requested_client is not None and connection["client_id"] != requested_client:
            raise ValueError("Connection belongs to another client")
        connection["last_activity"] = time.time()
        if connection["client_id"] is not None and op in {"execute", "poll"}:
            await self.io(self.history.touch_client, connection["client_id"])

    def _dispatch_status(self, req, connection, op):
        if op == "status":
            detail = req.get("detail", True)
            if type(detail) is not bool:
                raise ValueError("detail must be a boolean")
            if not detail:
                target = connection.get("target") if connection else None
                target_version = target.get("version") if target else None
                info = (
                    runtime_info(descriptor(), target_version)
                    if target_version
                    else runtime_info(descriptor())
                )
                result = {
                    "version": info["manager_version"],
                    "manager_version": info["manager_version"],
                    "bridge_version": info["bridge_version"],
                    "protocol_version": info["protocol_version"],
                    "update_pending": info["update_pending"],
                    "workspace_id": self.workspace_id,
                    "generation": self.generation,
                    "healthy": self.healthy,
                    "restarting": self.restarting,
                    "resetting": self.resetting,
                    "workspace_available": self.workspace_available(),
                    "connection_count": len(self.clients),
                    "client_count": len(
                        {
                            item["client_id"]
                            for item in self.clients.values()
                            if item["client_id"]
                        }
                    ),
                    "storage_maintenance": dict(self.storage_maintenance),
                    "mail": self.mail.status() if self.mail is not None else None,
                    "web": self.web.status() if self.web is not None else None,
                    "dependencies": (
                        self.dependencies.status() if getattr(self, "dependencies", None) else None
                    ),
                    "active_count": len(self.active),
                    "queued_count": sum(
                        record["state"] == "queued" for record in self.execs.values()
                    ),
                }
                if self.health_error:
                    result["health_error"] = self.health_error
                if self.registry_error:
                    result["registry_error"] = self.registry_error
                return result
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
                "global_path": str(self.config_store.global_path),
                "workspace_available": self.workspace_available(),
                "generation": self.generation,
                "healthy": self.healthy,
                "health_error": self.health_error,
                "registry_error": self.registry_error,
                "resetting": self.resetting,
                "connections": connections,
                "connection_count": len(connections),
                "client_count": len(
                    {c["client_id"] for c in connections if c["client_id"] is not None}
                ),
                "storage_maintenance": dict(self.storage_maintenance),
                "mail": self.mail.status() if self.mail is not None else None,
                "web": self.web.status() if self.web is not None else None,
                "dependencies": (
                    self.dependencies.status() if getattr(self, "dependencies", None) else None
                ),
                "active": list(self.active),
                "queued": [r["id"] for r in self.execs.values() if r["state"] == "queued"],
            }
        return _UNHANDLED

    async def _dispatch_history(self, op, req):
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
            if record.get("corrupt"):
                return record
            output_evicted = bool(
                record.get("output_evicted") or record.get("scan_output_evicted")
            )
            if output_evicted:
                record.update(
                    self.expired_output(
                        record,
                        history_id=record.get("history_id") or record.get("id"),
                    )
                )
                return record
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
                elif journal is not None and not isinstance(record.get("output"), str):
                    record.update(self.expired_output(record, history_id=record.get("history_id")))
            return record
        if op == "history_task_read":
            history_id = req.get("id")
            record = await self.io(self.history.get, history_id)
            if record is None or record.get("kind") not in {"python", "execution"}:
                raise ValueError("Unknown Python task history ID")
            if record.get("corrupt"):
                raise RPCError(
                    "Saved task metadata is corrupt", code="history_corrupt",
                    details={"history_id": history_id, "outcome_unknown": True},
                )
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
            if record.get("output_evicted") or record.get("scan_output_evicted"):
                return self.expired_output(record, cursor=cursor, history_id=expected_history_id)
            journal = (
                self.task_journal_path(record)
                if record.get("kind") == "python"
                else self.root / "runs" / f"{record['id']}.jsonl"
            )
            if journal is None:
                return self.expired_output(record, cursor=cursor, history_id=expected_history_id)
            if not await self.io(journal.is_file):
                return self.expired_output(record, cursor=cursor, history_id=expected_history_id)
            try:
                output, total = await self.io(read_page, journal, cursor, budget)
            except (FileNotFoundError, ValueError):
                if not await self.io(journal.is_file):
                    return self.expired_output(
                        record, cursor=cursor, history_id=expected_history_id
                    )
                raise
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
        return _UNHANDLED

    async def _dispatch_execution(
        self, op, req, *, client, connection_id, connection,
        requested_client, generation,
    ):
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
            if isinstance(req.get("error_info"), dict):
                rec["error_info"] = req["error_info"]
            await self.finish(rec, req["state"], req.get("error"))
            return None
        if op == "execute":
            if not self.workspace_available():
                raise RuntimeError("The workspace moved; stop its manager and reconnect")
            if not self.healthy or self.resetting:
                raise RuntimeError("Kernel unavailable; use CLI reset")
            if connection is None or connection["client_id"] is None:
                raise RuntimeError("Call init on an active connection before execute")
            wait_ms = self.wait_budget(req.get("wait_ms"), self.execute_wait_ms)
            self.response_budget(req.get("max_bytes"))
            rec = await self.admit_execution(client, connection_id, req)
            return await self.poll(
                rec["id"],
                wait_ms=wait_ms,
                max_bytes=req.get("max_bytes"),
                inbox_client=client,
                wake_on_output=False,
            )
        if op == "poll":
            return await self.poll(
                req["exec_id"],
                req.get("cursor") or 0,
                self.wait_budget(req.get("wait_ms"), self.poll_wait_ms),
                max_bytes=req.get("max_bytes"),
                inbox_client=connection["client_id"] if connection else None,
            )
        return _UNHANDLED

    async def _dispatch_messages(
        self, op, req, *, client, connection_id, connection,
        requested_client, generation,
    ):
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
        return _UNHANDLED

    async def _dispatch_timers(
        self, op, req, *, client, connection_id, connection,
        requested_client, generation,
    ):
        if op not in {"timer_start", "timer_check", "timer_list", "timer_cancel", "timer_ack"}:
            return _UNHANDLED
        if not (connection and connection["client_id"]) and not requested_client:
            raise RuntimeError("Timers require a client identity")
        if self.stopping.is_set():
            raise RuntimeError("Workspace manager is stopping")
        if self.timers is None:
            raise RuntimeError("Workspace timers are unavailable")
        if op == "timer_check":
            return await self.io(self.timers.check, client, req["timer_id"])
        if op == "timer_list":
            return await self.io(
                self.timers.list, client, state=req.get("state"),
                limit=req.get("limit", 50), cursor=req.get("cursor"),
            )

        async def change():
            if op == "timer_start":
                result = await self.io(
                    self.timers.start, client, req.get("seconds"),
                    at=req.get("at"), label=req.get("label", ""),
                )
            elif op == "timer_cancel":
                result = await self.io(self.timers.cancel, client, req["timer_id"])
            else:
                result = await self.io(self.timers.ack, client, req["ids"])
            for waiter in self.timer_waiters.get(client, ()):
                if not waiter.done():
                    waiter.set_result(None)
            return result

        return await await_completion(asyncio.create_task(change()))

    async def _dispatch_mail(
        self, op, req, *, client, connection_id, connection,
        requested_client, generation,
    ):
        if op != "mail":
            return _UNHANDLED
        if not (connection and connection["client_id"]) and not requested_client:
            raise RuntimeError("Mail requires a client identity")
        if self.mail is None:
            raise RuntimeError("Workspace mail is unavailable")
        method = req.get("method")
        params = req.get("params", {})
        if not isinstance(method, str) or not isinstance(params, dict):
            raise ValueError("Mail requires a method and parameter object")
        async def run():
            async with self._admission_lock:
                if self.stopping.is_set() or self.restarting:
                    raise RuntimeError("Workspace manager is stopping or restarting")
                if self.settings.applying:
                    raise RuntimeError(
                        "Configuration reload is in progress; retry after it completes"
                    )
                operation = asyncio.create_task(self.mail.dispatch(method, client, params))
                await asyncio.sleep(0)
            return await await_completion(operation)

        return await await_completion(asyncio.create_task(run()))

    async def _record_web(self, method, fields):
        await self.io(self.history.append, "web", method, fields)

    async def _dispatch_web(
        self, op, req, *, client, connection_id, connection,
        requested_client, generation,
    ):
        if op != "web":
            return _UNHANDLED
        if not connection or not connection["client_id"]:
            raise RuntimeError("Web requires a connected, initialized client")
        method, params = req.get("method"), req.get("params", {})
        if not isinstance(method, str) or not isinstance(params, dict):
            raise ValueError("Web requires a method and parameter object")

        async def run():
            async with self._admission_lock:
                if (
                    self.stopping.is_set() or self.resetting or self.restarting
                    or (generation and generation != self.generation)
                ):
                    raise RuntimeError("Workspace manager is stopping or restarting")
                if self.clients.get(connection_id) is not connection:
                    raise RuntimeError("Web client disconnected")
                if self.settings.applying:
                    raise RuntimeError(
                        "Configuration reload is in progress; retry after it completes"
                    )
                if self.web is None:
                    raise RPCError(
                        "Workspace web service is unavailable", code="service_unavailable"
                    )
                operation = asyncio.create_task(self.web.dispatch(method, client, params))
                await asyncio.sleep(0)
            return await await_completion(operation)

        return await await_completion(asyncio.create_task(run()))

    async def _dispatch_storage(
        self, op, req, *, client, connection_id, connection,
        requested_client, generation,
    ):
        if op == "code_config":
            return await self.code_config(req, generation=generation)
        if op == "storage_usage":
            return await self.storage_call("usage")
        if op == "storage_gc":
            return await self.storage_call(
                "gc", dry_run=req.get("dry_run", True),
                older_than_days=req.get("older_than_days", self.storage_policy["retention_days"]),
                max_bytes=req.get("max_bytes", self.storage_policy["max_bytes"]),
                revision_keep=req.get("revision_keep", self.storage_policy["revision_keep"]),
            )
        if op == "storage_gc_apply":
            return await self.storage_call("gc_apply", plan_id=req["plan_id"])
        if op == "message_clients":
            active_ids = {info["client_id"] for info in self.clients.values() if info["client_id"]}
            page = await self.io(
                self.history.clients, prefix=req.get("prefix"), cursor=req.get("cursor"),
                limit=req.get("limit", 50), connected=req.get("connected"), active_ids=active_ids,
            )
            for item in page["clients"]:
                item["connected"] = item["id"] in active_ids
            return page
        return _UNHANDLED

    async def _dispatch_dependencies(
        self, op, req, *, client, connection_id, connection, requested_client, generation,
    ):
        if op != "dependencies":
            return _UNHANDLED
        service = self.dependencies
        if service is None:
            raise RuntimeError("Workspace dependencies are unavailable")
        method = req.get("method")
        params = req.get("params", {})
        if not isinstance(params, dict):
            raise ValueError("dependency parameters must be an object")
        if method == "list":
            return await service.list(**params)
        if method != "ensure":
            raise ValueError("Unknown dependency operation")
        if not (connection and connection.get("client_id")) and not requested_client:
            raise RuntimeError("Dependency installation requires an initialized client")
        async with self._admission_lock:
            self._check_dispatch_admission("shell_start")
            if self.resetting or not self.healthy or not self.workspace_available():
                raise RuntimeError("Workspace is not accepting dependency installation")
            task = asyncio.create_task(service.ensure(
                params.get("names"), automatic=params.get("automatic", False),
                context={"client_id": client, "connection_id": connection_id,
                         "exec_id": req.get("exec_id"), "generation": self.generation},
            ))
            await asyncio.sleep(0)
        return await task

    async def _record_dependency(self, state, fields):
        if self.history is not None:
            await self.io(self.history.append, "dependency", state, fields, critical=True)

    async def _install_dependency_packages(self, names, context):
        if context.get("bootstrap"):
            return await install_core(
                self.py, self.root, names, self.dependencies.config, self.command,
            )
        async with self._admission_lock:
            self._check_dispatch_admission("packages_add")
            if context.get("generation") and context["generation"] != self.generation:
                raise RuntimeError("Expired kernel generation")
            job = await self._start_package_job(names, context, automatic=True)
        try:
            async with asyncio.timeout(300):
                result = await self.shells.wait(job["id"])
        except BaseException:
            with contextlib.suppress(Exception):
                await self.shells.cancel(job["id"])
            raise
        if result.get("state") != "succeeded":
            raise RPCError(
                "Package installation failed; inspect the package job output for the conflict.",
                code="dependency_install_failed", details={"job_id": job["id"], "names": names},
            )
        return result

    async def _start_package_job(self, specs, context, *, automatic=False):
        from .package_worker import _validate_specs

        specs = _validate_specs(specs)
        command = [
            sys.executable, "-I", str(Path(__file__).with_name("package_worker.py")),
            "--python", str(self.py), "--root", str(self.root),
            "--spec-json", json.dumps(specs),
        ]
        if automatic:
            command.append("--automatic")
        launch = asyncio.create_task(self.shells.start(
            command, str(self.workspace),
            package_environment(self.dependencies.config if self.dependencies else {}),
            kind="package",
        ))
        cancelled = False
        try:
            job = await asyncio.shield(launch)
        except asyncio.CancelledError:
            job = await await_completion(launch)
            cancelled = True
        self.track_shell(
            job["id"], context.get("client_id"), context.get("connection_id"),
            context.get("exec_id"), kind="package", specs=specs, automatic=automatic,
        )
        if cancelled:
            await self.shells.cancel(job["id"])
            raise asyncio.CancelledError
        return job

    async def _install_dependency_browser(self, name, context):
        async with self._admission_lock:
            self._check_dispatch_admission("browser_server")
            if self.resetting or not self.healthy:
                raise RuntimeError("Workspace is not accepting browser installation")
            if context.get("generation") and context["generation"] != self.generation:
                raise RuntimeError("Expired kernel generation")
            async with self._resource_lock:
                if self.browser is None:
                    self.browser = BrowserService(
                        self.workspace, self.py, kernel_pid=self.km.provisioner.pid,
                        generation=self.generation, shells=self.shells,
                    )
                browser = self.browser

        def track(ident, **fields):
            self.track_shell(ident, context.get("client_id"), context.get("connection_id"),
                             context.get("exec_id"), **fields)

        return await browser.prepare(name, track=track)

    async def _dispatch_tasks(
        self, op, req, *, client, connection_id, connection,
        requested_client, generation,
    ):
        if op == "task_result_store":
            ident = req["id"]
            async with self.task_lock(ident):
                record = self.task_records.get(ident)
                if record is None:
                    record = {
                        "id": ident, "kind": "python", "generation": self.generation,
                        "client_id": client, "connection_id": connection_id,
                        "exec_id": req.get("exec_id"), "state": "running", "created": time.time(),
                    }
                if record.get("generation") != self.generation or record.get("state") in TERMINAL:
                    raise ValueError("Task result no longer belongs to an active generation")
                if record.get("client_id") != client:
                    raise ValueError("Task belongs to another client")
                reference = await self.io(
                    _store_task_result, self.workspace, self.history, record, req["encoded"],
                    workspace_identity=self.workspace_id,
                    critical=True,
                )
                record.update(result_ref=reference, result_persisted=True)
                self.task_records[ident] = record
                return reference
        if op == "task_result_get":
            record = await self.io(self.history.get, req["id"])
            if record and record.get("corrupt"):
                raise RPCError(
                    "Saved task metadata is corrupt", code="history_corrupt",
                    details={"history_id": req["id"], "outcome_unknown": True},
                )
            if not record or not record.get("result_ref") or record.get("result_evicted"):
                raise RuntimeError("Persisted task result is unavailable or expired")
            self._ensure_workspace_identity()
            return await self.io(
                load_result,
                self.workspace,
                record["result_ref"],
                workspace_identity=self.workspace_id,
            )
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
            task = asyncio.create_task(self.admit_task_event(event, client, connection_id, op))
            return await await_completion(task)
        return _UNHANDLED

    async def _dispatch_scan(
        self, op, req, *, client, connection_id, connection,
        requested_client, generation,
    ):
        if op == "scan_start":
            if self.stopping.is_set() or self.resetting or not self.healthy:
                raise RuntimeError("Workspace is not accepting scans")
            return await self.scans.start(
                req["mode"],
                targets=req["targets"],
                ports=req.get("ports"),
                concurrency=req.get("concurrency"),
                rate=req.get("rate"),
                timeout=req.get("timeout"),
                args=req.get("args"),
                client_id=client,
                connection_id=connection_id,
                exec_id=req.get("exec_id"),
                max_probes=req.get("max_probes", 1_000_000),
                max_duration=req.get("max_duration", 3600),
                continue_after_output_limit=req.get("continue_after_output_limit", False),
                family=req.get("family", "any"),
                retries=req.get("retries"),
                banner=req.get("banner", False),
                banner_timeout=req.get("banner_timeout", 0.5),
                banner_bytes=req.get("banner_bytes", 1024),
                open_only=req.get("open_only", False),
                per_host_rate=req.get("per_host_rate"),
                probe=req.get("probe"),
                payload_b64=req.get("payload_b64"),
                capture_response=req.get("capture_response", False),
                response_bytes=req.get("response_bytes", 1024),
            )
        if op == "scan_results":
            history_record = await self.io(self.history.get, req["id"])
            if history_record and (
                history_record.get("output_evicted")
                or history_record.get("scan_output_evicted")
            ):
                expired = self.expired_output(history_record, cursor=req.get("cursor"))
                return {
                    "id": req["id"],
                    "results": [],
                    "cursor": None,
                    "next_cursor": None,
                    "has_more": False,
                    "state": history_record.get("state", "unknown"),
                    "truncated": True,
                    "expired": True,
                    "warnings": expired["warnings"],
                }
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
        return _UNHANDLED

    async def _dispatch_tools(
        self, op, req, *, client, connection_id, connection,
        requested_client, generation,
    ):
        if op in {"search", "git"}:
            async with self._admission_lock:
                self._check_dispatch_admission(op)
                if generation and generation != self.generation:
                    raise RuntimeError("Expired kernel generation")
                admission_generation = self.generation
                self._check_managed_command_admission(
                    admission_generation, self.shells
                )
                shells = self.shells
                dependency_service = getattr(self, "dependencies", None)
            runner = ManagedCommands(
                self,
                client,
                connection_id,
                req.get("exec_id"),
                shells=shells,
                generation=admission_generation,
            )
            args = req.get("args", {})
            if not isinstance(args, dict):
                raise TypeError("args must be an object")
            if op == "search":
                callback = None
                if dependency_service is not None:
                    async def callback(*names, automatic=True):
                        async with self._admission_lock:
                            self._check_managed_command_admission(
                                admission_generation, shells
                            )
                        return await dependency_service.ensure(
                            names, automatic=automatic,
                            context={"client_id": client, "connection_id": connection_id,
                                     "exec_id": req.get("exec_id"),
                                     "generation": admission_generation},
                        )
                return await Search(
                    self.workspace, runner, ensure_dependencies=callback,
                ).search(**args)
            method = req.get("method")
            if method not in {"status", "diff", "show", "log", "blame", "commit_info"}:
                raise ValueError("Unknown Git method")
            return await getattr(Git(self.workspace, runner), method)(**args)
        return _UNHANDLED

    def _check_managed_command_admission(self, generation, shells):
        if generation != self.generation or shells is not self.shells:
            raise RuntimeError("Expired kernel generation")
        if self.stopping.is_set() or self.resetting or self.restarting:
            raise RuntimeError("Workspace is not accepting managed commands")
        if not self.workspace_available():
            raise RuntimeError("The workspace moved; stop its manager and reconnect")

    async def start_managed_command(
        self, generation, shells, command, cwd, env, *, input=None
    ):
        async with self._admission_lock:
            self._check_managed_command_admission(generation, shells)
            return await shells.start(command, cwd, env, input=input)

    async def _dispatch_shell(
        self, op, req, *, client, connection_id, connection,
        requested_client, generation,
    ):
        if op == "shell_start":
            self._ensure_workspace_identity()
            job = await self.shells.start(
                req["command"],
                req.get("cwd", str(self.workspace)),
                req.get("env"),
                inherit_env=req.get("inherit_env", req.get("env") is None),
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
            self._ensure_workspace_identity()
            history_record = await self.io(self.history.get, req["id"])
            if history_record and history_record.get("output_evicted"):
                return self.expired_output(history_record, cursor=req.get("cursor", 0))
            return await self.shells.poll(req["id"], req.get("cursor", 0))
        if op == "shell_read":
            self._ensure_workspace_identity()
            history_record = await self.io(self.history.get, req["id"])
            if history_record and history_record.get("output_evicted"):
                return self.expired_output(history_record, cursor=req.get("cursor", 0))
            return await self.shells.read(
                req["id"],
                req.get("cursor", 0),
                stream=req.get("stream"),
                max_bytes=req.get("max_bytes", 32768),
                wait_ms=req.get("wait_ms", 0),
            )
        if op == "shell_wait":
            self._ensure_workspace_identity()
            return await self.shells.wait(req["id"])
        if op == "shell_write":
            self._ensure_workspace_identity()
            return await self.shells.write(
                req["id"], req.get("text", ""), eof=req.get("eof", False)
            )
        if op == "shell_resize":
            self._ensure_workspace_identity()
            return await self.shells.resize(req["id"], req["rows"], req["cols"])
        if op == "shell_cancel":
            self._ensure_workspace_identity()
            return await self.shells.cancel(req["id"])
        if op == "packages_add":
            self._ensure_workspace_identity()
            return await self._start_package_job(req["specs"], {
                "client_id": client, "connection_id": connection_id,
                "exec_id": req.get("exec_id"),
            })
        return _UNHANDLED

    async def _dispatch_mcp(
        self, op, req, *, client, connection_id, connection,
        requested_client, generation,
    ):
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
            async with self._workspace_config_admission(generation):
                await self.io(
                    self.history.append,
                    "mcp", method, dict(event, state="running"), critical=True
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
        return _UNHANDLED

    async def _dispatch_lifecycle(
        self, op, req, *, client, connection_id, connection,
        requested_client, generation,
    ):
        if op == "restart":
            from .restart import request_restart

            self._ensure_workspace_identity()
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

            self._ensure_workspace_identity()
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
            await self._reserve_restart(ident, current, ticket.get("force", False))
            return {"prepared": True, "restart_id": ident}
        if op == "reset":
            from_kernel = req.get("from_kernel", False)
            async with self._admission_lock:
                self._ensure_workspace_identity()
                self._check_dispatch_admission(op)
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
                if not req.get("force", False) and (
                    busy or python_busy or self.shells.active
                    or (self.web is not None and self.web.active_count)
                    or (getattr(self, "dependencies", None) is not None
                        and self.dependencies.active_count)
                ):
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
                self._check_dispatch_admission(op)
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
                        or (self.mail is not None and self.mail.active_count)
                        or (self.web is not None and self.web.active_count)
                        or (getattr(self, "dependencies", None) is not None
                            and self.dependencies.active_count)
                    )
                ):
                    raise RuntimeError("Workspace has active work; pass --force")
                self.stopping.set()
            return {"stopping": True, "pid": os.getpid()}
        return _UNHANDLED

    async def record_task_event(self, event, client, connection_id, op):
        lock = self.task_lock(event["id"])
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
                workspace=self.workspace,
                workspace_identity=self.workspace_id,
                critical=True,
            )
            self.task_records[event["id"]] = record
            if event["state"] in TERMINAL:
                self.retain_completed("python", record)
            return None

    async def admit_task_event(self, event, client, connection_id, op):
        async with self._admission_lock:
            if (
                event.get("state") not in TERMINAL
                and (self.stopping.is_set() or self.resetting or self.restarting)
            ):
                raise RuntimeError("Workspace is not accepting new Python tasks")
            await self.record_task_event(event, client, connection_id, op)

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
            or any(
                rec["kind"] == "python" and rec["state"] not in TERMINAL
                for rec in self.task_records.values()
            )
            or self.shells.active
            or (getattr(self, "mail", None) is not None and self.mail.active_count)
            or (getattr(self, "web", None) is not None and self.web.active_count)
            or (getattr(self, "dependencies", None) is not None and self.dependencies.active_count)
        ):
            raise RuntimeError("Workspace has active work; pass force=True to restart")

    async def _reserve_restart(self, ident, current, force):
        task = asyncio.create_task(self._reserve_restart_locked(ident, current, force))
        return await await_completion(task)

    async def _reserve_restart_locked(self, ident, current, force):
        async with self._admission_lock:
            self._ensure_workspace_identity()
            if self.settings.applying:
                raise RuntimeError("Configuration reload is in progress; retry after it completes")
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
                    ticket = await recover_ticket(self.workspace, ident)
                    if ticket is None or ticket.get("state") != "failed":
                        raise
                except Exception:
                    if active_ticket(self.workspace) is not None:
                        await asyncio.sleep(0.1)
                        continue
                    ticket = await recover_ticket(self.workspace, ident)
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

    async def _register_manager(self):
        from .runtime_registry import register

        try:
            await wait_owned(asyncio.to_thread(
                register, self.workspace, self.socket, self.generation,
                self.config_store.global_path,
            ))
        except Exception as exc:
            self.registry_error = f"manager registry unavailable: {safe_error(exc)}"
            print(self.registry_error, file=sys.stderr)
            await self._unregister_manager()
        else:
            self._registry_generation = self.generation
            self.registry_error = None

    async def _unregister_manager(self):
        from .runtime_registry import unregister

        previous = getattr(self, "_registry_generation", None)
        generations = dict.fromkeys(value for value in (self.generation, previous) if value)
        for generation in generations:
            try:
                removed = await wait_owned(asyncio.to_thread(
                    unregister, self.workspace, generation, identity=self.workspace_id,
                ), propagate=False)
            except Exception as exc:
                print(f"manager registry cleanup failed: {safe_error(exc)}", file=sys.stderr)
            else:
                if removed:
                    self._registry_generation = None

    async def reset(self, current):
        async with self._lifecycle_lock:
            try:
                self._ensure_workspace_identity()
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
                if self.web is not None:
                    await self.web.reset()
                dependencies = getattr(self, "dependencies", None)
                if dependencies is not None:
                    await dependencies.close()
                await self.close_kernel()
                await self.lose_python_tasks("Workspace reset", state="cancelled")
                await self.close_shells()
                await self.mcp.close()
                self._kernel_snapshot = await wait_owned(asyncio.to_thread(self.config_store.load))
                self.mcp = MCPBridge(
                    self.workspace, global_path=self.config_store.global_path,
                    snapshot=self._kernel_snapshot,
                )
                self.shells = self.new_shells()
                self.scans = ScanService(self.workspace, self.shells, self.track_shell)
                if dependencies is not None:
                    self.dependencies = self.new_dependencies(dependencies.config)
                self.queue = asyncio.Queue()
                self._registry_generation = (
                    getattr(self, "_registry_generation", None) or self.generation
                )
                self.generation = uuid.uuid4().hex
                await self.start_kernel()
                await self._register_manager()
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
            if self.web is not None:
                await self.web.close()
            await self.close_kernel()
            with contextlib.suppress(Exception):
                await self.lose_python_tasks("Manager stopped")
            if self.dependencies is not None:
                await self.dependencies.close()
            await self.close_shells()
            await self.mcp.close()
            if self.mail is not None:
                await self.mail.close()

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
        async with self._resource_lock:
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
        self.core_workers.difference_update(
            task for task in [self.worker, self.iopub, self.replies, self.monitor] if task
        )
        self.by_msg.clear()
        self.control_waiters.clear()
        self.submit_waiters.clear()
        if self.kc:
            self.kc.stop_channels()
        if self.km:
            with contextlib.suppress(Exception):
                await self.km.shutdown_kernel(now=True)
        self.worker = None
        self.iopub = None
        self.replies = None
        self.monitor = None
        self.kc = None
        self.km = None

    @staticmethod
    def public_record(rec):
        return {
            k: v
            for k, v in rec.items()
            if k not in {"done", "idle", "events"} and not k.startswith("_")
        }

    @staticmethod
    def expired_output(record, *, cursor=0, history_id=None):
        warnings = list(record.get("warnings", []))
        warning = {
            "code": "output_expired",
            "text": "Retained task output has expired and is no longer available.",
        }
        if warning not in warnings:
            warnings.append(warning)
        return {
            "id": record.get("id"),
            "history_id": history_id or record.get("history_id"),
            "kind": record.get("kind"),
            "generation": record.get("generation"),
            "client_id": record.get("client_id", record.get("client")),
            "connection_id": record.get("connection_id"),
            "exec_id": record.get("exec_id"),
            "state": record.get("state", "unknown"),
            "result": record.get("result"),
            "error": record.get("error"),
            "output": [],
            "cursor": cursor,
            "has_more": False,
            "truncated": True,
            "output_evicted": True,
            "warnings": warnings,
            "warnings_truncated": bool(record.get("warnings_truncated", False)),
            "pty": bool(record.get("pty", False)),
            "rows": record.get("rows", 24),
            "cols": record.get("cols", 80),
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
        await self.io(self.history.touch_client, client_id, critical=True)
        connection.update(initialized)
        if self.mail is not None:
            await self.mail.client_changed()
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
            async with self._initialize_lock:
                async with self._admission_lock:
                    self.clients.pop(connection_id, None)
                    self.attachments.pop(connection_id, None)
                if self.web is not None and info.get("client_id") is not None:
                    with contextlib.suppress(Exception):
                        await self.web.drop_client(info["client_id"])
            if self.mail is not None:
                with contextlib.suppress(Exception):
                    await self.mail.client_changed()
            if self.history:
                if info.get("client_id") is not None:
                    with contextlib.suppress(Exception):
                        await self.io(self.history.touch_client, info["client_id"], critical=True)
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
            lock = self.task_lock(ident)
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
        operation_name = None
        try:
            req = json.loads(await reader.readline())
            if not isinstance(req, dict):
                raise TypeError("request must be an object")
            operation_name = req.get("op")
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
            response = error_response(exc, operation_name)
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
        lock = open_regular(self.root / "manager.lock", "ab")
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
                await self._register_manager()
                self.spawn(self.maintain_storage())
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
            await self._unregister_manager()
            for writer in list(self.attachments.values()):
                writer.close()
            self.socket.unlink(missing_ok=True)
            if self.web is not None:
                with contextlib.suppress(Exception):
                    await self.web.close()
                self.web = None
            if self.mail is not None:
                with contextlib.suppress(Exception):
                    await self.mail.close()
                self.mail = None
            if self.dependencies is not None:
                with contextlib.suppress(Exception):
                    await self.dependencies.close()
                self.dependencies = None
            if self.persistence is not None:
                if self.messages is not None and self.history is not None:
                    with contextlib.suppress(Exception):
                        if self.persistence.available:
                            await self.io(
                                _close_stores,
                                self.history,
                                self.messages,
                                self.timers,
                                critical=True,
                            )
                        else:
                            await self.persistence.close()
                            await asyncio.to_thread(
                                _close_stores, self.history, self.messages, self.timers
                            )
                    self.messages = None
                    self.timers = None
                    self.history = None
                with contextlib.suppress(Exception):
                    await self.persistence.close()
                self.persistence = None
            lock.close()
