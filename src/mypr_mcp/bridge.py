"""Keep an MCP session attached across explicitly coordinated manager restarts."""

from __future__ import annotations

import asyncio
import contextlib
import time
import uuid
from pathlib import Path
from typing import Any

from .protocol import check_compatibility, runtime_info, target_installation
from .restart import active_ticket, read_ticket, wait_ticket
from .restart_records import poll_restart
from .transport import attachment, find_runtime, rpc


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

    @property
    def generation(self):
        return (self._state or {}).get("generation")

    async def start(self):
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="mypr-manager-bridge")

    async def wait_ready(self):
        await self.start()
        if self.attachment is not None and self.attachment.closed.is_set() and not self._error:
            self._ready.clear()
        await self._ready.wait()
        if self._error is not None:
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

    async def _poll_restart(self, exec_id, cursor, wait_ms):
        deadline = time.monotonic() + min(30000, max(0, wait_ms)) / 1000
        while True:
            result = await asyncio.to_thread(poll_restart, self.workspace, exec_id, cursor)
            if (
                result is None
                or result["state"] != "running"
                or result["output"]
                or time.monotonic() >= deadline
            ):
                return result
            await asyncio.sleep(min(0.05, max(0, deadline - time.monotonic())))

    async def request(self, op: str, **fields: Any):
        if op == "poll" and (
            not self._ready.is_set()
            or self._error is not None
            or (self.attachment is not None and self.attachment.closed.is_set())
        ):
            recorded = await self._poll_restart(
                fields["exec_id"], fields.get("cursor") or 0, fields.get("wait_ms", 0)
            )
            if recorded is not None:
                return self._decorate(recorded)
        if op == "execute":
            ticket = active_ticket(self.workspace)
            if ticket is not None:
                raise RuntimeError(f"Workspace is restarting: {ticket['id']}")
        await self.wait_ready()
        connection_id = self.connection_id
        try:
            result = await rpc(self.path, op=op, connection_id=connection_id, **fields)
        except ConnectionError, OSError, RuntimeError:
            ticket = self._ticket()
            origin = (ticket or {}).get("origin") or {}
            if (
                op == "execute"
                and origin.get("connection_id") == connection_id
                and origin.get("request_id") == fields.get("request_id")
            ):
                result = await asyncio.to_thread(poll_restart, self.workspace, origin["exec_id"])
                if result is not None:
                    return self._decorate(result)
            if op == "poll" and ticket:
                result = await asyncio.to_thread(
                    poll_restart,
                    self.workspace,
                    fields["exec_id"],
                    fields.get("cursor") or 0,
                )
                if result is not None:
                    return self._decorate(result)
                if ticket["state"] == "succeeded":
                    await self.wait_ready()
                    result = await rpc(self.path, op=op, connection_id=self.connection_id, **fields)
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
            await context.__aexit__(None, None, None)
        self.connection_id = uuid.uuid4().hex
        context = attachment(path, self.connection_id, target=target_installation())
        attached = await context.__aenter__()
        self._context = context
        self.attachment = attached
        self.path = path
        self._state = check_compatibility(attached.status)
        if self.client_id is not None:
            await rpc(path, op="init", connection_id=self.connection_id, client_id=self.client_id)
        self._error = None
        self._ready.set()

    async def close(self):
        self._stopped = True
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
        if self._context is not None:
            context, self._context = self._context, None
            with contextlib.suppress(ConnectionError, OSError):
                await context.__aexit__(None, None, None)
        self._ready.set()
