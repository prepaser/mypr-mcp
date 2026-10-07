"""Immutable, bounded per-client accessibility snapshots."""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import os
import secrets
import signal
import sys
import time
from collections import OrderedDict
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .async_utils import finish_owned, wait_owned
from .browser_observation import _safe_url

_MAX_SNAPSHOTS = 32
_MAX_STORE_BYTES = 16 * 1024 * 1024
_MAX_CAPTURE_BYTES = _MAX_STORE_BYTES
_MAX_OUTPUT_BYTES = 32 * 1024
_MAX_LIMIT = _MAX_OUTPUT_BYTES
_WORKER_TIMEOUT = 2.0
_CLEANUP_TIMEOUT = 4.0


@dataclass(slots=True)
class _Snapshot:
    ident: str
    text: str
    size: int
    url: str
    title: str
    captured_at: float


def _cut(text: str, offset: int, byte_limit: int) -> tuple[str, int]:
    end = offset
    used = 0
    while end < len(text):
        char_size = len(text[end].encode("utf-8"))
        if used + char_size > byte_limit:
            break
        used += char_size
        end += 1
    return text[offset:end], end


class BrowserSnapshots:
    def __init__(self) -> None:
        self._clients: dict[str, OrderedDict[str, _Snapshot]] = {}
        self._sizes: dict[str, int] = {}
        self._generation = 0
        self._owner_generations: dict[str, int] = {}

    async def snapshot(
        self,
        owner: str,
        page: Any,
        *,
        selector: str | None = None,
        cursor: str | None = None,
        depth: int | None = None,
        mode: str | None = None,
        boxes: bool | None = None,
        limit: int = _MAX_OUTPUT_BYTES,
    ) -> dict[str, Any]:
        if type(limit) is not int or not 2048 <= limit <= _MAX_LIMIT:
            raise ValueError(f"limit must be an integer from 2048 to {_MAX_LIMIT}")
        if cursor is not None:
            if any(value is not None for value in (depth, mode, boxes, selector)):
                raise ValueError("snapshot options cannot be changed while paging")
            ident, offset = self._parse_cursor(cursor)
            item = self._get(owner, ident)
            return self._page(item, offset, limit)
        if selector is not None and (not isinstance(selector, str) or not selector):
            raise ValueError("selector must be a non-empty string or None")
        if depth is not None and (type(depth) is not int or depth < 0):
            raise ValueError("depth must be a non-negative integer or None")
        if mode is not None and mode not in {"default", "ai"}:
            raise ValueError("mode must be 'default', 'ai', or None")
        if boxes is not None and type(boxes) is not bool:
            raise TypeError("boxes must be a boolean or None")
        if getattr(page, "is_closed", lambda: False)():
            raise RuntimeError("cannot snapshot a closed page")
        generation = self._generation
        owner_generation = self._owner_generations.get(owner, 0)
        locator = page.locator(selector) if selector is not None else page.locator("body")
        capture = locator.aria_snapshot
        requested = {"depth": depth, "mode": mode, "boxes": boxes}
        try:
            parameters = inspect.signature(capture).parameters
        except TypeError, ValueError:
            parameters = {}
        kwargs = {}
        for name, value in requested.items():
            if value is None:
                continue
            if name not in parameters:
                raise ValueError(f"installed Playwright does not support aria_snapshot({name}=...)")
            kwargs[name] = value
        result = await capture(**kwargs)
        if not isinstance(result, str):
            raise RuntimeError("Playwright returned a non-text accessibility snapshot")
        text = result
        size = sys.getsizeof(text) + 512
        if size > _MAX_CAPTURE_BYTES:
            raise ValueError("accessibility snapshot exceeds the 16 MiB per-client memory limit")
        ident = secrets.token_hex(8)
        item = _Snapshot(
            ident=ident,
            text=text,
            size=size,
            url=_safe_url(getattr(page, "url", ""))[:1024],
            title=str(await page.title())[:512],
            captured_at=time.time(),
        )
        if (
            generation != self._generation
            or owner_generation != self._owner_generations.get(owner, 0)
        ):
            raise RuntimeError("browser snapshots were cleared during capture")
        snapshots = self._clients.setdefault(owner, OrderedDict())
        snapshots[ident] = item
        total = self._sizes.get(owner, 0) + size
        while len(snapshots) > _MAX_SNAPSHOTS or total > _MAX_STORE_BYTES:
            _, removed = snapshots.popitem(last=False)
            total -= removed.size
        self._sizes[owner] = total
        return self._page(item, 0, limit)

    async def find(
        self,
        owner: str,
        snapshot_id: str,
        text: str,
        *,
        regex: bool = False,
        cursor: str | None = None,
        limit: int = 100,
    ) -> dict[str, Any]:
        if not isinstance(text, str) or not text:
            raise ValueError("text must be a non-empty string")
        if len(text) > 1024:
            raise ValueError("text must be at most 1024 characters")
        if type(regex) is not bool:
            raise TypeError("regex must be a boolean")
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ValueError("limit must be an integer from 1 to 1000")
        item = self._get(owner, snapshot_id)
        query = hashlib.sha256(("1" if regex else "0").encode() + text.encode()).hexdigest()[:12]
        offset = 0
        if cursor is not None:
            parts = cursor.split(".")
            if len(parts) != 4 or parts[0] != "f" or parts[1] != item.ident or parts[2] != query:
                raise ValueError("cursor does not match this snapshot search")
            try:
                offset = int(parts[3])
            except ValueError as exc:
                raise ValueError("invalid search cursor") from exc
            if offset < 0:
                raise ValueError("invalid search cursor")
        result = await self._run_worker(
            {
                "operation": "find",
                "snapshot": item.text,
                "query": text,
                "regex": regex,
                "offset": offset,
                "limit": limit,
            }
        )
        next_line = int(result["next_line"])
        has_more = bool(result["has_more"])
        matches = list(result["matches"])

        def page() -> dict[str, Any]:
            return {
                "snapshot_id": item.ident,
                "matches": matches,
                "next_cursor": f"f.{item.ident}.{query}.{next_line}" if has_more else None,
                "has_more": has_more,
            }

        while matches and self._encoded_size(page()) > _MAX_OUTPUT_BYTES:
            removed = matches.pop()
            next_line = removed["line"] - 1
            has_more = True
        return page()

    async def diff(
        self,
        owner: str,
        before_id: str,
        after_id: str,
        *,
        cursor: str | None = None,
        limit: int = _MAX_OUTPUT_BYTES,
    ) -> dict[str, Any]:
        if type(limit) is not int or not 2048 <= limit <= _MAX_LIMIT:
            raise ValueError(f"limit must be an integer from 2048 to {_MAX_LIMIT}")
        before = self._get(owner, before_id)
        after = self._get(owner, after_id)
        offset = 0
        if cursor is not None:
            parts = cursor.split(".")
            if len(parts) != 4 or parts[:3] != ["d", before.ident, after.ident]:
                raise ValueError("cursor does not match these snapshots")
            try:
                offset = int(parts[3])
            except ValueError as exc:
                raise ValueError("invalid diff cursor") from exc
            if offset < 0:
                raise ValueError("invalid diff cursor")
        result = await self._run_worker(
            {
                "operation": "diff",
                "before": before.text,
                "after": after.text,
                "before_id": before.ident,
                "after_id": after.ident,
                "offset": offset,
                "limit": limit,
            }
        )
        end = int(result["next_offset"])
        more = bool(result["has_more"])
        diff = result["diff"]

        def page() -> dict[str, Any]:
            return {
                "before_id": before.ident,
                "after_id": after.ident,
                "diff": diff,
                "next_cursor": f"d.{before.ident}.{after.ident}.{end}" if more else None,
                "has_more": more,
            }

        while diff and self._encoded_size(page()) > limit:
            diff = diff[: max(0, len(diff) // 2)]
            end = offset + len(diff)
            more = True
        return page()

    async def _run_worker(self, value: dict[str, Any]) -> dict[str, Any]:
        payload = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        guard = os.fspath(Path(__file__).with_name("process_guard.py"))
        worker = os.fspath(Path(__file__).with_name("browser_snapshot_worker.py"))
        launch = asyncio.create_task(
            asyncio.create_subprocess_exec(
                sys.executable,
                guard,
                "--parent-pid",
                str(os.getpid()),
                "--tree",
                "--",
                sys.executable,
                worker,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                start_new_session=True,
                limit=64 * 1024,
            )
        )
        try:
            process, cancelled = await self._finish_launch(launch)
        except OSError as exc:
            raise RuntimeError(f"unable to start snapshot worker: {exc}") from exc
        if cancelled:
            await self._finish_cleanup(process)
            raise asyncio.CancelledError
        communication = asyncio.create_task(process.communicate(payload))
        try:
            async with asyncio.timeout(_WORKER_TIMEOUT):
                output, errors = await asyncio.shield(communication)
        except TimeoutError as exc:
            await self._finish_cleanup(process, communication)
            raise TimeoutError(
                "snapshot search or diff exceeded its 2-second worker limit"
            ) from exc
        except BaseException:
            await self._finish_cleanup(process, communication)
            raise
        if process.returncode != 0:
            detail = errors.decode("utf-8", errors="replace")[:2048].strip()
            if detail.startswith("ValueError: "):
                raise ValueError(detail.removeprefix("ValueError: "))
            if detail.startswith("error: "):
                raise ValueError(detail.removeprefix("error: "))
            raise RuntimeError(f"snapshot worker failed: {detail or process.returncode}")
        if len(output) > 64 * 1024:
            raise RuntimeError("snapshot worker returned too much output")
        try:
            result = json.loads(output)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RuntimeError("snapshot worker returned invalid JSON") from exc
        if not isinstance(result, dict):
            raise RuntimeError("snapshot worker returned an invalid result")
        return result

    async def _finish_launch(
        self, launch: asyncio.Task[asyncio.subprocess.Process]
    ) -> tuple[asyncio.subprocess.Process, bool]:
        return await finish_owned(launch)

    async def _finish_cleanup(
        self,
        process: asyncio.subprocess.Process,
        communication: asyncio.Task | None = None,
    ) -> None:
        await wait_owned(self._stop_worker(process, communication), propagate=False)

    @staticmethod
    async def _stop_worker(
        process: asyncio.subprocess.Process,
        communication: asyncio.Task | None = None,
    ) -> None:
        if process.returncode is None:
            with suppress(ProcessLookupError):
                process.send_signal(signal.SIGTERM)

        async def finish() -> None:
            await process.wait()
            if communication is not None:
                await asyncio.gather(communication, return_exceptions=True)

        wait_task = asyncio.create_task(finish())
        try:
            async with asyncio.timeout(_CLEANUP_TIMEOUT):
                await asyncio.shield(wait_task)
        except TimeoutError:
            if process.returncode is None:
                with suppress(ProcessLookupError):
                    os.killpg(process.pid, signal.SIGKILL)
            with suppress(Exception, asyncio.CancelledError):
                await asyncio.shield(wait_task)

    def clear(self, owner: str | None = None) -> None:
        if owner is None:
            self._generation += 1
            self._owner_generations.clear()
            self._clients.clear()
            self._sizes.clear()
        else:
            self._owner_generations[owner] = self._owner_generations.get(owner, 0) + 1
            self._clients.pop(owner, None)
            self._sizes.pop(owner, None)

    def _get(self, owner: str, ident: str) -> _Snapshot:
        snapshots = self._clients.get(owner)
        if snapshots is None or ident not in snapshots:
            raise KeyError(f"snapshot {ident!r} is no longer retained for this client")
        return snapshots[ident]

    @staticmethod
    def _encoded_size(value: dict[str, Any]) -> int:
        return len(json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))

    def _parse_cursor(self, cursor: str) -> tuple[str, int]:
        parts = cursor.split(".")
        if len(parts) != 3 or parts[0] != "s":
            raise ValueError("invalid snapshot cursor")
        try:
            offset = int(parts[2])
        except ValueError as exc:
            raise ValueError("invalid snapshot cursor") from exc
        if offset < 0:
            raise ValueError("invalid snapshot cursor")
        return parts[1], offset

    @staticmethod
    def _page(item: _Snapshot, offset: int, limit: int) -> dict[str, Any]:
        if offset > len(item.text):
            raise ValueError("snapshot cursor is out of range")
        metadata = {
            "snapshot_id": item.ident,
            "url": item.url,
            "title": item.title,
            "captured_at": item.captured_at,
        }
        text, end = "", offset
        more = end < len(item.text)

        def page() -> dict[str, Any]:
            return {
                **metadata,
                "text": text,
                "next_cursor": f"s.{item.ident}.{end}" if more else None,
                "has_more": more,
            }

        encoded_size = BrowserSnapshots._encoded_size(page())
        if encoded_size > limit:
            raise ValueError("snapshot metadata exceeds the requested output limit")
        text_limit = limit - encoded_size
        text, end = _cut(item.text, offset, text_limit)
        more = end < len(item.text)
        while BrowserSnapshots._encoded_size(page()) > limit and text:
            text = text[: len(text) // 2]
            end = offset + len(text)
            more = end < len(item.text)
        if BrowserSnapshots._encoded_size(page()) > limit:
            raise ValueError("snapshot result exceeds the requested output limit")
        if more and end == offset:
            raise ValueError("snapshot metadata leaves no room for the next text character")
        return page()


__all__ = ["BrowserSnapshots"]
