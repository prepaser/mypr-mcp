"""Bounded HTML extraction isolated from the workspace Python kernel."""

from __future__ import annotations

import asyncio
import base64
import json
import os
import secrets
import signal
import sys
import time
from collections import OrderedDict
from pathlib import Path
from typing import Any

from .async_utils import wait_owned
from .http_tools import _client_id

_WORKER = Path(__file__).with_name("html_worker.py")
_WORKER_PYTHON = sys.executable
_WORKERS = asyncio.Semaphore(2)
MAX_INPUT_BYTES = 16 * 1024 * 1024
_MAX_WORKER_OUTPUT = 24 * 1024 * 1024
_MAX_SNAPSHOT_BYTES = 16 * 1024 * 1024
_MAX_SNAPSHOTS = 32
_SNAPSHOT_TTL = 5 * 60
_TIMEOUT = 20
_CLEANUP_TIMEOUT = 6
_DEFAULT_OUTPUT_BYTES = 32 * 1024
_MIN_OUTPUT_BYTES = 4 * 1024
_MAX_OUTPUT_BYTES = 1024 * 1024


class HTMLToolError(RuntimeError):
    """A bounded HTML extraction operation failed in its worker."""


def _validate_output_limit(value: int) -> None:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not _MIN_OUTPUT_BYTES <= value <= _MAX_OUTPUT_BYTES
    ):
        raise ValueError(
            f"max_bytes must be between {_MIN_OUTPUT_BYTES} and {_MAX_OUTPUT_BYTES}"
        )


def _encode_html(html: str) -> bytes:
    try:
        encoded = html.encode("utf-8")
    except UnicodeEncodeError:
        raise ValueError("html contains invalid Unicode") from None
    if len(encoded) > MAX_INPUT_BYTES:
        raise ValueError(f"html exceeds the {MAX_INPUT_BYTES} byte input limit")
    return encoded


def _kill_worker(process: asyncio.subprocess.Process) -> None:
    if process.returncode is None:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass


def _signal_worker(process: asyncio.subprocess.Process, signum: signal.Signals) -> None:
    if process.returncode is None:
        try:
            os.killpg(process.pid, signum)
        except ProcessLookupError:
            pass


async def _cleanup_worker(
    process: asyncio.subprocess.Process, communication: asyncio.Task[Any]
) -> None:
    if process.stdin is not None and not process.stdin.is_closing():
        process.stdin.close()
    _signal_worker(process, signal.SIGTERM)
    try:
        await asyncio.wait_for(asyncio.shield(process.wait()), _CLEANUP_TIMEOUT)
    except TimeoutError:
        _kill_worker(process)
        try:
            await asyncio.wait_for(asyncio.shield(process.wait()), _CLEANUP_TIMEOUT)
        except TimeoutError:
            pass
    if communication.done():
        await asyncio.gather(communication, return_exceptions=True)
    else:
        transport = getattr(process, "_transport", None)
        stdout_transport = transport.get_pipe_transport(1) if transport is not None else None
        if stdout_transport is not None:
            stdout_transport.close()
        communication.cancel()
        try:
            await asyncio.wait_for(
                asyncio.gather(communication, return_exceptions=True), _CLEANUP_TIMEOUT
            )
        except TimeoutError:
            pass
    if process.returncode is None:
        try:
            await asyncio.wait_for(asyncio.shield(process.wait()), _CLEANUP_TIMEOUT)
        except TimeoutError:
            pass


async def _cleanup_worker_uncancellable(
    process: asyncio.subprocess.Process, communication: asyncio.Task[Any]
) -> None:
    await wait_owned(_cleanup_worker(process, communication), propagate=False)


async def _finish_launch(task: asyncio.Task[asyncio.subprocess.Process]):
    return await wait_owned(task, propagate=False)


async def _run_worker(
    html: str,
    *,
    url: str | None,
    selector: str | None,
    include_structure: bool = False,
) -> dict[str, Any]:
    encoded = _encode_html(html)
    header = json.dumps(
        {
            "url": url,
            "selector": selector,
            "include_structure": include_structure,
        },
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    request = header + b"\n" + encoded

    env = {
        key: value
        for key, value in os.environ.items()
        if key
        in {
            "PATH",
            "LANG",
            "LC_ALL",
            "LC_CTYPE",
            "LD_LIBRARY_PATH",
            "DYLD_LIBRARY_PATH",
            "SYSTEMROOT",
            "WINDIR",
            "TEMP",
            "TMP",
            "TMPDIR",
        }
    }
    async with _WORKERS:
        guard = Path(__file__).with_name("process_guard.py")
        launch = asyncio.create_task(
            asyncio.create_subprocess_exec(
                _WORKER_PYTHON,
                "-I",
                str(guard),
                "--parent-pid",
                str(os.getpid()),
                "--tree",
                "--",
                _WORKER_PYTHON,
                "-I",
                str(_WORKER),
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
                env=env,
                start_new_session=True,
            )
        )
        process = None

        async def communicate_bounded() -> tuple[bytes, bool]:
            async def read_stdout() -> tuple[bytes, bool]:
                chunks = []
                size = 0
                exceeded = False
                while chunk := await process.stdout.read(64 * 1024):
                    if exceeded:
                        continue
                    remaining = _MAX_WORKER_OUTPUT - size
                    if len(chunk) > remaining:
                        if remaining > 0:
                            chunks.append(chunk[:remaining])
                        exceeded = True
                        _signal_worker(process, signal.SIGTERM)
                        continue
                    chunks.append(chunk)
                    size += len(chunk)
                return b"".join(chunks), exceeded

            reader = asyncio.create_task(read_stdout())
            try:
                process.stdin.write(request)
                await process.stdin.drain()
                process.stdin.close()
                stdout, exceeded = await reader
                await process.wait()
                return stdout, exceeded
            except BaseException:
                if not reader.done():
                    reader.cancel()
                await asyncio.gather(reader, return_exceptions=True)
                raise

        try:
            process = await asyncio.shield(launch)
            communication = asyncio.create_task(communicate_bounded())
            try:
                stdout, exceeded = await asyncio.wait_for(
                    asyncio.shield(communication), _TIMEOUT
                )
            except TimeoutError:
                await _cleanup_worker_uncancellable(process, communication)
                raise HTMLToolError("HTML extraction exceeded its 20-second time limit") from None
            except BaseException:
                await _cleanup_worker_uncancellable(process, communication)
                raise
        except BaseException:
            if process is None:
                process = await _finish_launch(launch)
            if process.returncode is None:
                communication = locals().get("communication")
                if communication is None:
                    communication = asyncio.create_task(asyncio.sleep(0))
                await _cleanup_worker_uncancellable(process, communication)
            raise

    if exceeded:
        raise HTMLToolError("HTML worker response exceeded its size limit")
    if process.returncode != 0:
        raise HTMLToolError(
            f"HTML worker exited with status {process.returncode}; the input may be malformed"
        )
    try:
        response = json.loads(stdout)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise HTMLToolError("HTML worker returned an invalid response") from exc
    if not isinstance(response, dict):
        raise HTMLToolError("HTML worker returned an invalid response")
    if response.get("ok") is not True:
        kind = response.get("kind")
        message = response.get("error", "HTML extraction failed")
        if kind == "MissingDependency":
            raise ImportError(message)
        if kind == "ValueError":
            raise ValueError(message)
        raise HTMLToolError(message)
    result = response.get("result")
    if not isinstance(result, dict):
        raise HTMLToolError("HTML worker returned an invalid result")
    return result


def _text_items(text: str) -> list[dict[str, str]]:
    raw = text.encode("utf-8")
    items = []
    offset = 0
    while offset < len(raw):
        end = min(offset + 512, len(raw))
        while True:
            try:
                value = raw[offset:end].decode("utf-8")
                break
            except UnicodeDecodeError:
                end -= 1
        if end == offset:
            raise ValueError("could not split extracted text at a Unicode boundary")
        items.append({"kind": "text", "text": value})
        offset = end
    return items


class HTMLExtractor:
    """Own bounded per-client snapshots for HTML extraction results."""

    def __init__(self, http_tools: Any) -> None:
        self._http_tools = http_tools
        self._snapshots: OrderedDict[tuple[str, str], dict[str, Any]] = OrderedDict()
        self._snapshot_bytes = 0

    async def extract_html(
        self,
        html: str | None = None,
        *,
        url: str | None = None,
        selector: str | None = None,
        include_structure: bool = False,
        max_bytes: int = _DEFAULT_OUTPUT_BYTES,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        _validate_output_limit(max_bytes)
        if not isinstance(include_structure, bool):
            raise ValueError("include_structure must be a boolean")
        owner = _client_id(self._http_tools._identity)
        if cursor is not None:
            if html is not None:
                raise ValueError("html must be omitted when cursor is provided")
            if url is not None or selector is not None:
                raise ValueError("url and selector cannot be combined with cursor")
            ident, offset, structure_offset = self._decode_cursor(cursor)
            snapshot = self._load(owner, ident)
            return self._page(snapshot, offset, max_bytes, structure_offset)
        if not isinstance(html, str):
            raise TypeError("html must be a string")
        if url is not None and (not isinstance(url, str) or len(url) > 8192):
            raise ValueError("url must be a string no longer than 8192 characters")
        if selector is not None and (not isinstance(selector, str) or len(selector) > 4096):
            raise ValueError("selector must be a string no longer than 4096 characters")
        _encode_html(html)
        ensure = getattr(self._http_tools, "_ensure", None)
        if ensure is not None:
            await ensure("trafilatura", *("cssselect",) if selector else ())
        result = await _run_worker(
            html, url=url, selector=selector, include_structure=include_structure
        )
        items = await asyncio.to_thread(_text_items, result.get("text", ""))
        for link in result.get("links", []):
            items.append({"kind": "link", "url": link["url"], "text": link["text"]})
        ident = secrets.token_hex(16)
        source_url = result.get("url")
        safe_url = _truncate_text(source_url, 1024)
        url_truncated = isinstance(source_url, str) and safe_url != source_url
        warnings = list(result.get("warnings", []))
        if url_truncated:
            warnings.append("source URL was shortened for the bounded result")
        snapshot = {
            "id": ident,
            "title": _truncate_text(result.get("title", ""), 1024),
            "url": safe_url if source_url is not None else None,
            "url_truncated": url_truncated,
            "source_hash": result["source_hash"],
            "items": items,
            "complete": bool(result.get("complete", True)),
            "stop_reason": result.get("stop_reason"),
            "warnings": warnings,
            "created": time.monotonic(),
        }
        if include_structure:
            snapshot["structure"] = result.get(
                "structure",
                {"headings": [], "metadata": {}},
            )
            snapshot["structure_truncated"] = bool(result.get("structure_truncated", False))
        snapshot["size"] = len(
            json.dumps(
                {key: value for key, value in snapshot.items() if key not in {"created", "size"}},
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode()
        )
        if snapshot["size"] > _MAX_SNAPSHOT_BYTES:
            raise HTMLToolError("HTML result exceeds the in-memory snapshot limit")
        key = (owner, ident)
        self._snapshots[key] = snapshot
        self._snapshot_bytes += snapshot["size"]
        self._prune()
        return self._page(snapshot, 0, max_bytes)

    def clear(self) -> None:
        self._snapshots.clear()
        self._snapshot_bytes = 0

    def _prune(self) -> None:
        now = time.monotonic()
        for key, snapshot in tuple(self._snapshots.items()):
            if now - snapshot["created"] > _SNAPSHOT_TTL:
                self._snapshot_bytes -= snapshot["size"]
                self._snapshots.pop(key, None)
        while (
            len(self._snapshots) > _MAX_SNAPSHOTS
            or self._snapshot_bytes > _MAX_SNAPSHOT_BYTES
        ):
            _, snapshot = self._snapshots.popitem(last=False)
            self._snapshot_bytes -= snapshot["size"]

    def _load(self, owner: str, ident: str) -> dict[str, Any]:
        self._prune()
        key = (owner, ident)
        try:
            snapshot = self._snapshots.pop(key)
        except KeyError:
            raise ValueError(
                "HTML result snapshot has expired or belongs to another client"
            ) from None
        self._snapshots[key] = snapshot
        return snapshot

    @staticmethod
    def _decode_cursor(cursor: str) -> tuple[str, int, int]:
        if not isinstance(cursor, str) or len(cursor) > 512:
            raise ValueError("invalid HTML result cursor")
        try:
            payload = json.loads(base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4)))
            ident, offset = payload["id"], payload["offset"]
            structure_offset = payload.get("structure_offset", 0)
        except (ValueError, TypeError, KeyError, json.JSONDecodeError) as exc:
            raise ValueError("invalid HTML result cursor") from exc
        if (
            not isinstance(ident, str)
            or len(ident) != 32
            or any(char not in "0123456789abcdef" for char in ident)
            or type(offset) is not int
            or offset < 0
            or type(structure_offset) is not int
            or structure_offset < 0
        ):
            raise ValueError("invalid HTML result cursor")
        return ident, offset, structure_offset

    @staticmethod
    def _cursor(ident: str, offset: int, structure_offset: int = 0) -> str:
        payload = json.dumps(
            {"id": ident, "offset": offset, "structure_offset": structure_offset},
            separators=(",", ":"),
        ).encode()
        return base64.urlsafe_b64encode(payload).decode().rstrip("=")

    def _page(
        self,
        snapshot: dict[str, Any],
        offset: int,
        max_bytes: int,
        structure_offset: int = 0,
    ) -> dict[str, Any]:
        items = snapshot["items"]
        structure = snapshot.get("structure")
        structure_entries = _structure_entries(structure) if structure else []
        if offset > len(items) or structure_offset > len(structure_entries):
            raise ValueError("invalid HTML result cursor")
        ident = snapshot["id"]
        page: dict[str, Any] = {
            "title": snapshot["title"],
            "url": snapshot["url"],
            "url_truncated": snapshot["url_truncated"],
            "source_hash": snapshot["source_hash"],
            "text": "",
            "links": [],
            "snapshot_id": ident,
            "page_cursor": self._cursor(ident, offset, structure_offset),
            "next_cursor": None,
            "has_more": False,
            "truncated": False,
            "complete": snapshot["complete"],
            "stop_reason": snapshot["stop_reason"],
            "warnings": list(snapshot["warnings"]),
        }
        if structure is not None:
            page["structure"] = {"headings": [], "metadata": {}}
            page["structure_truncated"] = bool(snapshot.get("structure_truncated", False))
            page["structure_has_more"] = structure_offset < len(structure_entries)

        def set_page_state(
            candidate: dict[str, Any], item_offset: int, entry_offset: int
        ) -> None:
            more = item_offset < len(items) or entry_offset < len(structure_entries)
            candidate["has_more"] = more
            candidate["truncated"] = more
            candidate["next_cursor"] = (
                self._cursor(ident, item_offset, entry_offset) if more else None
            )
            if "structure" in candidate:
                candidate["structure_has_more"] = entry_offset < len(structure_entries)

        def size(candidate: dict[str, Any]) -> int:
            return len(json.dumps(candidate, ensure_ascii=False, separators=(",", ":")).encode())

        # Include the final cursor when testing whether an entry fits.
        set_page_state(page, offset, structure_offset)
        page_size = size(page)
        structure_index = structure_offset
        while structure_index < len(structure_entries):
            entry = structure_entries[structure_index]
            candidate = json.loads(
                json.dumps(page, ensure_ascii=False, separators=(",", ":"))
            )
            if entry[0] == "metadata":
                candidate["structure"]["metadata"][entry[1]] = entry[2]
            else:
                candidate["structure"]["headings"].append(entry[1])
            set_page_state(candidate, offset, structure_index + 1)
            candidate_size = size(candidate)
            if candidate_size > max_bytes:
                if structure_index == structure_offset:
                    raise ValueError(
                        "max_bytes is too small for the next HTML structure item; increase it"
                    )
                break
            page = candidate
            page_size = candidate_size
            structure_index += 1
        set_page_state(page, offset, structure_index)
        index = offset
        while index < len(items):
            item = items[index]
            old_cursor = page["next_cursor"]
            old_has_more = page["has_more"]
            old_truncated = page["truncated"]
            if item["kind"] == "text":
                page["text"] += item["text"]
            else:
                page["links"].append({"url": item["url"], "text": item["text"]})
            set_page_state(page, index + 1, structure_index)
            # The item representation contains at least the same escaped text
            # as the page representation, plus its kind and object delimiters.
            # Use that conservative delta so paging remains linear for large
            # extracted documents; the final serialization below is exact.
            item_size = (
                len(json.dumps(item, ensure_ascii=False, separators=(",", ":")).encode()) + 2
            )
            old_values = (old_cursor, old_has_more, old_truncated)
            new_values = (page["next_cursor"], page["has_more"], page["truncated"])
            metadata_delta = sum(
                len(json.dumps(new, ensure_ascii=False).encode())
                - len(json.dumps(old, ensure_ascii=False).encode())
                for old, new in zip(old_values, new_values, strict=True)
            )
            estimated_size = page_size + item_size + metadata_delta
            if estimated_size > max_bytes:
                if item["kind"] == "text":
                    if item["text"]:
                        page["text"] = page["text"][: -len(item["text"])]
                else:
                    page["links"].pop()
                set_page_state(page, index, structure_index)
                if index == offset:
                    if structure_index == structure_offset:
                        raise ValueError(
                            "max_bytes is too small for the next result item; "
                            "increase it to continue"
                        )
                    break
                break
            page_size = estimated_size
            index += 1
        set_page_state(page, index, structure_index)
        if size(page) > max_bytes:
            raise HTMLToolError("HTML result page exceeded its byte limit")
        return page


def _truncate_text(value: Any, max_bytes: int) -> str:
    if not isinstance(value, str):
        return ""
    value = "".join(char for char in value if ord(char) >= 32)
    encoded = value.encode("utf-8")
    return encoded[:max_bytes].decode("utf-8", errors="ignore")


def _structure_entries(structure: dict[str, Any] | None) -> list[tuple[str, Any, Any]]:
    if not isinstance(structure, dict):
        return []
    entries: list[tuple[str, Any, Any]] = []
    metadata = structure.get("metadata", {})
    if isinstance(metadata, dict):
        entries.extend(("metadata", key, value) for key, value in metadata.items())
    headings = structure.get("headings", [])
    if isinstance(headings, list):
        entries.extend(("heading", heading, None) for heading in headings)
    return entries


__all__ = ["HTMLExtractor", "HTMLToolError", "MAX_INPUT_BYTES"]
