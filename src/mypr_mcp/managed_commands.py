"""Managed command adapter for manager-owned workspace tools."""

from __future__ import annotations

import asyncio
import math
from contextlib import suppress
from typing import Any

_ACTIVE = {"queued", "running", "cancelling"}
_PAGE_BYTES = 32 * 1024
_MAX_WARNINGS = 4


class ManagedCommands:
    def __init__(self, runtime, client_id, connection_id, exec_id):
        self.runtime = runtime
        self.client_id = client_id
        self.connection_id = connection_id
        self.exec_id = exec_id

    async def run(
        self,
        command,
        *,
        cwd=None,
        env=None,
        input=None,
        timeout=None,  # noqa: ASYNC109
        check=False,
        max_bytes=32768,
    ) -> dict[str, Any]:
        if type(max_bytes) is not int or max_bytes < 0:
            raise ValueError("max_bytes must be a non-negative integer")
        if timeout is not None and (
            type(timeout) not in (int, float)
            or isinstance(timeout, bool)
            or not math.isfinite(timeout)
            or timeout < 0
        ):
            raise ValueError("timeout must be a finite non-negative number or None")
        shells = self.runtime.shells
        launch = asyncio.create_task(
            shells.start(command, cwd or str(self.runtime.workspace), env, input=input)
        )
        try:
            job = await asyncio.shield(launch)
        except asyncio.CancelledError:
            # A cancellation can arrive after create_subprocess_exec but before
            # start() returns its ID. Finish start, register the job, then kill it.
            job = await _uncancelled(launch, propagate=False)
            ident = await self._track_or_cancel(shells, job, command)
            with suppress(Exception):
                await self._cancel(shells, ident)
            raise

        ident = await self._track_or_cancel(shells, job, command)
        streams = {"stdout": bytearray(), "stderr": bytearray()}
        remaining = max_bytes
        cursor = None
        truncated = False
        warnings: list[Any] = []
        warnings_truncated = False
        timed_out = False
        page = {}

        def consume(value: Any) -> None:
            nonlocal cursor, remaining, truncated, warnings_truncated
            if not isinstance(value, dict):
                raise RuntimeError("shell read returned an invalid result")
            events = value.get("events", value.get("output", []))
            if isinstance(events, str):
                events = [{"stream": "stdout", "text": events}]
            elif isinstance(events, dict):
                events = [events]
            if not isinstance(events, list):
                raise RuntimeError("shell read returned invalid output events")
            for event in events:
                if not isinstance(event, dict):
                    continue
                raw = str(event.get("text", "")).encode("utf-8", "replace")
                if remaining == 0:
                    truncated |= bool(raw)
                    break
                if raw:
                    kept = raw[:remaining]
                    if len(kept) < len(raw):
                        truncated = True
                    remaining -= len(kept)
                    stream = "stderr" if event.get("stream") == "stderr" else "stdout"
                    streams[stream].extend(kept)
            truncated |= bool(value.get("truncated"))
            page_warnings = value.get("warnings", [])
            if not isinstance(page_warnings, list):
                page_warnings = [
                    {"code": "invalid_warnings", "text": "Shell returned invalid warnings"}
                ]
            for warning in page_warnings:
                if warning in warnings:
                    continue
                if len(warnings) < _MAX_WARNINGS:
                    warnings.append(warning)
                else:
                    warnings_truncated = True
            warnings_truncated |= bool(value.get("warnings_truncated"))
            if "cursor" in value:
                cursor = value["cursor"]

        async def drain() -> None:
            nonlocal page
            for _ in range(1024):
                old_cursor = cursor
                page = await shells.read(ident, cursor=cursor, max_bytes=_PAGE_BYTES, wait_ms=0)
                consume(page)
                if not page.get("has_more") or page.get("cursor") == old_cursor:
                    return
            add_warning({"code": "output_drain_limit", "text": "Output drain limit reached"})

        def add_warning(warning: Any) -> None:
            nonlocal warnings_truncated
            if warning in warnings:
                return
            if len(warnings) < _MAX_WARNINGS:
                warnings.append(warning)
            else:
                warnings_truncated = True

        try:
            async with asyncio.timeout(timeout):
                while True:
                    page = await shells.read(
                        ident, cursor=cursor, max_bytes=_PAGE_BYTES, wait_ms=1000
                    )
                    consume(page)
                    if page.get("state") not in _ACTIVE and not page.get("has_more"):
                        break
        except TimeoutError:
            timed_out = True
            await self._cancel(shells, ident)
            with suppress(Exception):
                await drain()
        except BaseException:
            with suppress(Exception):
                await self._cancel(shells, ident)
            raise
        result = {
            "id": ident,
            "state": page.get("state", "unknown"),
            "returncode": (page.get("result") or {}).get("returncode"),
            "stdout": bytes(streams["stdout"]).decode("utf-8", "ignore"),
            "stderr": bytes(streams["stderr"]).decode("utf-8", "ignore"),
            "error": page.get("error"),
            "truncated": truncated,
            "warnings": warnings,
            "timed_out": timed_out,
        }
        if warnings_truncated:
            result["warnings_truncated"] = True
        if check and (timed_out or result["state"] != "succeeded"):
            raise RuntimeError(f"Command failed: {result['error'] or result['returncode']}")
        return result

    async def _track_or_cancel(self, shells, job: Any, command: Any) -> str:
        if not isinstance(job, dict) or not job.get("id"):
            raise RuntimeError("shell start returned no job ID")
        ident = str(job["id"])
        try:
            self.runtime.track_shell(
                ident,
                self.client_id,
                self.connection_id,
                self.exec_id,
                command=command,
            )
        except BaseException:
            with suppress(Exception):
                await self._cancel(shells, ident)
            raise
        return ident

    @staticmethod
    async def _cancel(shells, ident):
        task = asyncio.create_task(shells.cancel(ident))
        return await _uncancelled(task)


async def _uncancelled(task: asyncio.Task[Any], *, propagate: bool = True) -> Any:
    cancelled = False
    while True:
        try:
            value = await asyncio.shield(task)
            break
        except asyncio.CancelledError:
            if task.cancelled():
                raise
            cancelled = True
    if cancelled and propagate:
        raise asyncio.CancelledError
    return value
