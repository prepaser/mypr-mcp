"""Keep an MCP session attached across explicitly coordinated manager restarts."""

from __future__ import annotations

import asyncio
import contextlib
import time
import uuid
from pathlib import Path
from typing import Any

from .async_utils import wait_owned
from .config import DEFAULT_LIMITS, ConfigError, ConfigStore
from .diagnostics import RPCError
from .mail_store import MailStore
from .protocol import check_compatibility, runtime_info, target_installation
from .restart import active_ticket, read_ticket, recover_ticket, wait_ticket
from .restart_records import poll_restart, restart_id_for_execution
from .timers import TimerStore
from .transport import HANDSHAKE_TIMEOUT, attachment, find_runtime, rpc


class ConnectionBridge:
    def __init__(self, workspace: Path):
        self.workspace = workspace
        self.connection_id = uuid.uuid4().hex
        self.client_id = None
        self.path = None
        self.attachment = None
        self._context = None
        self._task = None
        self._ready = asyncio.Event()
        self._error = None
        self._stopped = False
        self._state = None
        self._reported_generation = None
        self._last_timing = None
        self._close_task = None

    @property
    def generation(self):
        return (self._state or {}).get("generation")

    async def start(self):
        if (
            self._task is not None
            and self._task.done()
            and not self._stopped
            and self.client_id is None
            and isinstance(self._error, RPCError)
            and self._error.code == "dependency_missing"
            and self._error.details.get("prepare_command")
        ):
            self._task = None
            self._error = None
            self._ready.clear()
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="mypr-manager-bridge")

    async def wait_ready(self):
        await self.start()
        if self.attachment is not None and self.attachment.closed.is_set() and not self._error:
            self._ready.clear()
        await self._ready.wait()
        if self._error is not None:
            if isinstance(self._error, RPCError):
                raise self._error
            raise RuntimeError(str(self._error)) from self._error
        if self._stopped or self.path is None:
            raise RuntimeError("Workspace manager connection is closed")

    def _ticket(self):
        ticket = read_ticket(self.workspace)
        if ticket and ticket.get("old_generation") == self.generation:
            return ticket
        return None

    def _decorate(self, result, *, init=False):
        if self._state is None:
            return result
        full = init or self._reported_generation != self.generation
        info = runtime_info(self._state, include_instructions=full)
        self._reported_generation = self.generation
        return {**result, "runtime": info}

    def _restart_result(self, exec_id, cursor, max_bytes):
        result = poll_restart(self.workspace, exec_id, cursor, max_bytes=max_bytes)
        if result is None or self.client_id is None:
            return result, None
        preview, due_at = TimerStore.snapshot_existing(
            self.workspace, self.client_id, include_deadline=result["state"] == "running"
        )
        if preview is not None:
            result["timers"] = preview
        mail = MailStore.snapshot_existing(self.workspace, self.client_id, offline=True)
        if mail is not None:
            result["mail"] = mail
        return result, due_at

    async def _poll_restart(self, exec_id, cursor, wait_ms, max_bytes=None):
        await self._recover_restart(exec_id)
        if wait_ms is None:
            try:
                snapshot = await asyncio.to_thread(ConfigStore(self.workspace).load)
                wait_ms = snapshot.values["limits"]["poll_wait_ms"]
            except (ConfigError, OSError, RuntimeError):
                wait_ms = DEFAULT_LIMITS["poll_wait_ms"]
        deadline = time.monotonic() + min(30000, max(0, wait_ms)) / 1000
        while True:
            result, due_at = await asyncio.to_thread(
                self._restart_result, exec_id, cursor, max_bytes
            )
            if (
                result is None
                or result["state"] != "running"
                or result["output"]
                or result.get("timers")
                or result.get("mail")
                or time.monotonic() >= deadline
            ):
                return result
            pause = min(0.05, max(0, deadline - time.monotonic()))
            if due_at is not None:
                pause = min(pause, max(0, due_at - time.time()))
            await asyncio.sleep(pause)

    async def _recover_restart(self, exec_id):
        ident = await asyncio.to_thread(restart_id_for_execution, self.workspace, exec_id)
        if ident is not None:
            await recover_ticket(self.workspace, ident)

    async def request(self, op: str, **fields: Any):
        started = time.perf_counter()
        timing = {"bridge_ready": 0.0, "manager_rpc": 0.0}
        if self._last_timing is not None:
            fields["_bridge_sample"], self._last_timing = self._last_timing, None
        try:
            result = await self._request(op, fields, timing)
        finally:
            timing["bridge_total"] = (time.perf_counter() - started) * 1000
            timing = {key: round(value, 3) for key, value in timing.items()}
            self._last_timing = dict(timing)
        return {**result, "_timing_ms": {**result.get("_timing_ms", {}), **timing}}

    async def _request(self, op, fields, timing):
        async def ready():
            started = time.perf_counter()
            try:
                await self.wait_ready()
            finally:
                timing["bridge_ready"] += (time.perf_counter() - started) * 1000

        async def call_manager(connection_id):
            started = time.perf_counter()
            try:
                return await rpc(self.path, op=op, connection_id=connection_id, **fields)
            finally:
                timing["manager_rpc"] += (time.perf_counter() - started) * 1000

        if op == "poll" and (
            not self._ready.is_set()
            or self._error is not None
            or (self.attachment is not None and self.attachment.closed.is_set())
        ):
            recorded = await self._poll_restart(
                fields["exec_id"],
                fields.get("cursor") or 0,
                fields.get("wait_ms"),
                fields.get("max_bytes"),
            )
            if recorded is not None:
                return self._decorate(recorded)
        if op == "execute":
            ticket = active_ticket(self.workspace)
            if ticket is not None:
                raise RuntimeError(f"Workspace is restarting: {ticket['id']}")
        await ready()
        connection_id = self.connection_id
        try:
            result = await call_manager(connection_id)
        except ConnectionError, OSError, RuntimeError:
            ticket = self._ticket()
            origin = (ticket or {}).get("origin") or {}
            if (
                op == "execute"
                and origin.get("connection_id") == connection_id
                and origin.get("request_id") == fields.get("request_id")
            ):
                result = await self._poll_restart(
                    origin["exec_id"],
                    0,
                    0,
                    fields.get("max_bytes"),
                )
                if result is not None:
                    return self._decorate(result)
            if op == "poll" and ticket:
                result = await self._poll_restart(
                    fields["exec_id"],
                    fields.get("cursor") or 0,
                    0,
                    fields.get("max_bytes"),
                )
                if result is not None:
                    return self._decorate(result)
                if ticket["state"] == "succeeded":
                    await ready()
                    result = await call_manager(self.connection_id)
                    return self._decorate(result)
            raise
        if op == "init":
            self.client_id = result["client_id"]
        return self._decorate(result, init=op == "init")

    async def bind_client(self, client_id):
        self.client_id = client_id

    async def _run(self):
        try:
            from .cli import ensure

            self.path = await ensure(self.workspace)
            await self._attach(self.path)
            while not self._stopped:
                await self.attachment.wait_closed()
                if self._stopped:
                    return
                self._ready.clear()
                ticket = self._ticket()
                if ticket is None:
                    raise RuntimeError("Workspace manager disconnected; reconnect explicitly")
                result = await wait_ticket(self.workspace, ticket["id"])
                if result["state"] != "succeeded":
                    raise RuntimeError(f"Workspace restart failed: {result.get('error')}")
                found = await find_runtime(self.workspace)
                if found is None or found[1].get("generation") != result["new_generation"]:
                    raise RuntimeError("Restarted workspace manager is unavailable")
                await self._attach(found[0])
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._error = exc
            self._ready.set()

    async def _attach(self, path):
        if self._context is not None:
            context, self._context = self._context, None
            self.attachment = None
            self.path = None
            self._state = None
            with contextlib.suppress(ConnectionError, OSError):
                await context.__aexit__(None, None, None)
        self.connection_id = uuid.uuid4().hex
        self.attachment = None
        self.path = None
        self._state = None
        self._ready.clear()
        context = attachment(path, self.connection_id, target=target_installation())
        try:
            attached = await context.__aenter__()
            self._context = context
            self.attachment = attached
            self.path = path
            self._state = check_compatibility(attached.status)
            if self.client_id is not None:
                async with asyncio.timeout(HANDSHAKE_TIMEOUT):
                    await rpc(
                        path,
                        op="init",
                        connection_id=self.connection_id,
                        client_id=self.client_id,
                    )
        except BaseException as exc:
            if self._context is context:
                self._context = None
            self.attachment = None
            self.path = None
            self._state = None
            with contextlib.suppress(BaseException):
                await context.__aexit__(type(exc), exc, exc.__traceback__)
            raise
        self._error = None
        self._ready.set()

    async def close(self):
        cleanup = self._close_task
        if cleanup is None:
            if self._stopped and self._context is None:
                self._ready.set()
                return
            origin = asyncio.current_task()
            cleanup = asyncio.create_task(self._close_owned(origin), name="mypr:bridge-close")
            self._close_task = cleanup

            def clear_cleanup(task: asyncio.Task[None]) -> None:
                if self._close_task is task:
                    self._close_task = None

            cleanup.add_done_callback(clear_cleanup)
        await wait_owned(cleanup)

    async def _close_owned(self, origin: asyncio.Task[Any] | None) -> None:
        self._stopped = True
        try:
            task = self._task
            if task is not None and task is not origin:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            context = self._context
            if context is not None:
                try:
                    await context.__aexit__(None, None, None)
                except ConnectionError, OSError:
                    pass
                else:
                    if self._context is context:
                        self._context = None
                    self.attachment = None
                    self.path = None
                    self._state = None
        finally:
            self._ready.set()
