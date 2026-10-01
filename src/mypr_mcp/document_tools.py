"""Workspace OCR and Office extraction APIs backed by bounded workers."""

from __future__ import annotations

import asyncio
import base64
import contextlib
import hashlib
import json
import os
import secrets
import shutil
import signal
import stat
import sys
import tempfile
import time
import zlib
from pathlib import Path
from threading import RLock
from typing import Any

from .async_utils import wait_owned
from .storage_lock import StorageLock

_WORKER = Path(__file__).with_name("document_worker.py")
_GUARD = Path(__file__).with_name("process_guard.py")
_WORKER_PYTHON = sys.executable
_WORKERS = asyncio.Semaphore(2)
_MAX_INPUT_BYTES = 64 * 1024 * 1024
_MAX_WORKER_OUTPUT = 8 * 1024 * 1024
_MAX_RESULT_BYTES = 7 * 1024 * 1024
_MAX_RESULTS = 16
_MAX_STORED_BYTES = 32 * 1024 * 1024
_MAX_PAGE_BYTES = 256 * 1024
_TIMEOUT = 60
_CLEANUP_TIMEOUT = 6


class DocumentToolError(RuntimeError):
    """A bounded document operation failed in its worker process."""


class _ResultStore:
    def __init__(self, workspace: Path) -> None:
        self.root = workspace / ".mypr" / "document-results"
        self.root.mkdir(parents=True, exist_ok=True)
        self._storage_lock_path = self.root.parent / "storage.lock"
        self._lock = RLock()

    @contextlib.contextmanager
    def _store_locked(self):
        with StorageLock(self._storage_lock_path):
            with self._locked():
                yield

    @contextlib.contextmanager
    def _locked(self):
        with self._lock:
            lock_fd = None
            try:
                import fcntl
            except ImportError:
                fcntl = None
            try:
                if fcntl is not None:
                    lock_fd = os.open(
                        self.root / ".lock",
                        os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0),
                        0o600,
                    )
                    fcntl.flock(lock_fd, fcntl.LOCK_EX)
                yield
            finally:
                if lock_fd is not None and fcntl is not None:
                    with contextlib.suppress(OSError):
                        fcntl.flock(lock_fd, fcntl.LOCK_UN)
                    os.close(lock_fd)

    def create(self, payload: dict[str, Any]) -> str:
        ident = secrets.token_hex(16)
        payload = {"id": ident, "created": time.time(), **payload}
        resume = payload.get("resume")
        resume_cache: bytes | None = None
        if isinstance(resume, dict) and isinstance(resume.get("tsv"), str):
            resume_cache = _decode_resume_tsv(resume["tsv"], compressed=True)
            resume = {key: value for key, value in resume.items() if key != "tsv"}
            resume.update(
                {
                    "cache": f"{ident}.resume",
                    "cache_bytes": len(resume_cache),
                    "cache_sha256": hashlib.sha256(resume_cache).hexdigest(),
                }
            )
            payload["resume"] = resume
        encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()
        if len(encoded) > _MAX_RESULT_BYTES:
            raise DocumentToolError("Document result exceeds its 7 MiB snapshot limit")
        with self._store_locked():
            cache_temporary = None
            fd, temporary = tempfile.mkstemp(prefix=f".{ident}.", suffix=".tmp", dir=self.root)
            try:
                with os.fdopen(fd, "wb") as stream:
                    stream.write(encoded)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(temporary, self.root / f"{ident}.json")
                if resume_cache is not None:
                    cache_path = self.root / f"{ident}.resume"
                    cache_fd, cache_temporary = tempfile.mkstemp(
                        prefix=f".{ident}.", suffix=".resume.tmp", dir=self.root
                    )
                    try:
                        with os.fdopen(cache_fd, "wb") as stream:
                            stream.write(resume_cache)
                            stream.flush()
                            os.fsync(stream.fileno())
                        os.replace(cache_temporary, cache_path)
                        cache_temporary = None
                    except BaseException:
                        with contextlib.suppress(FileNotFoundError):
                            (self.root / f"{ident}.json").unlink()
                        raise
                    finally:
                        if cache_temporary is not None:
                            with contextlib.suppress(FileNotFoundError):
                                os.unlink(cache_temporary)
                directory_fd = os.open(self.root, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
            finally:
                with contextlib.suppress(FileNotFoundError):
                    os.unlink(temporary)
            files = []
            for path in self.root.glob("[0-9a-f]" * 32 + ".json"):
                with contextlib.suppress(FileNotFoundError):
                    info = path.lstat()
                    if stat.S_ISREG(info.st_mode):
                        cache_path = path.with_suffix(".resume")
                        cache_info = cache_path.stat() if cache_path.is_file() else None
                        total = info.st_size + (cache_info.st_size if cache_info else 0)
                        mtime = max(info.st_mtime_ns, cache_info.st_mtime_ns if cache_info else 0)
                        files.append((path, total, mtime))
            files.sort(key=lambda item: item[2])
            sizes = {path: size for path, size, _ in files}
            while files and (len(files) > _MAX_RESULTS or sum(sizes.values()) > _MAX_STORED_BYTES):
                oldest, _, _ = files.pop(0)
                sizes.pop(oldest, None)
                with contextlib.suppress(FileNotFoundError):
                    oldest.unlink()
                with contextlib.suppress(FileNotFoundError):
                    oldest.with_suffix(".resume").unlink()
        return ident

    def load(self, ident: str) -> dict[str, Any]:
        with self._store_locked():
            return self._load_unlocked(ident)

    def _load_unlocked(self, ident: str) -> dict[str, Any]:
        if (
            not isinstance(ident, str)
            or len(ident) != 32
            or any(c not in "0123456789abcdef" for c in ident)
        ):
            raise ValueError("invalid document result cursor")
        path = self.root / f"{ident}.json"
        flags = os.O_RDONLY | getattr(os, "O_NONBLOCK", 0) | getattr(os, "O_NOFOLLOW", 0)
        try:
            fd = os.open(path, flags)
        except FileNotFoundError as exc:
            raise ValueError("document result has expired") from exc
        try:
            if not stat.S_ISREG(os.fstat(fd).st_mode):
                raise RuntimeError("invalid stored document result")
            with os.fdopen(fd, "rb") as stream:
                fd = -1
                encoded = stream.read(_MAX_RESULT_BYTES + 1)
        finally:
            if fd >= 0:
                os.close(fd)
        if len(encoded) > _MAX_RESULT_BYTES:
            raise RuntimeError("invalid stored document result")
        try:
            payload = json.loads(encoded)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RuntimeError("invalid stored document result") from exc
        if (
            not isinstance(payload, dict)
            or payload.get("id") != ident
            or not isinstance(payload.get("items"), list)
        ):
            raise RuntimeError("invalid stored document result")
        return payload

    def decode(self, cursor: str, *, kind: str, path: str) -> tuple[dict[str, Any], int]:
        if not isinstance(cursor, str) or not cursor or len(cursor) > 512:
            raise ValueError("invalid document result cursor")
        try:
            raw = base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4))
            payload = json.loads(raw)
        except (ValueError, TypeError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("invalid document result cursor") from exc
        if (
            not isinstance(payload, dict)
            or type(payload.get("offset")) is not int
            or payload["offset"] < 0
        ):
            raise ValueError("invalid document result cursor")
        snapshot = self.load(payload.get("id"))
        if snapshot.get("kind") != kind or snapshot.get("source", {}).get("path") != path:
            raise ValueError("cursor belongs to a different document query")
        if payload["offset"] > len(snapshot["items"]):
            raise ValueError("invalid document result cursor")
        return snapshot, payload["offset"]

    @staticmethod
    def cursor(ident: str, offset: int) -> str:
        raw = json.dumps({"id": ident, "offset": offset}, separators=(",", ":")).encode()
        return base64.urlsafe_b64encode(raw).decode().rstrip("=")

    @staticmethod
    def resume_cursor(ident: str, offset: int, tsv_offset: int = 0) -> str:
        raw = json.dumps(
            {
                "id": ident,
                "offset": offset,
                "tsv_offset": tsv_offset,
                "kind": "ocr-resume",
            },
            separators=(",", ":"),
        ).encode()
        return base64.urlsafe_b64encode(raw).decode().rstrip("=")

    def decode_resume(
        self, cursor: str, *, path: str
    ) -> tuple[dict[str, Any], int, int, bytes]:
        if not isinstance(cursor, str) or not cursor or len(cursor) > 512:
            raise ValueError("invalid OCR resume cursor")
        try:
            raw = base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4))
            payload = json.loads(raw)
        except (ValueError, TypeError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("invalid OCR resume cursor") from exc
        if (
            not isinstance(payload, dict)
            or payload.get("kind") != "ocr-resume"
            or type(payload.get("offset")) is not int
            or payload["offset"] < 0
            or type(payload.get("tsv_offset", 0)) is not int
            or payload.get("tsv_offset", 0) < 0
        ):
            raise ValueError("invalid OCR resume cursor")
        with self._store_locked():
            snapshot = self._load_unlocked(payload.get("id"))
            if snapshot.get("kind") != "ocr" or snapshot.get("source", {}).get("path") != path:
                raise ValueError("cursor belongs to a different OCR query")
            resume = snapshot.get("resume")
            if not isinstance(resume, dict) or not (
                isinstance(resume.get("tsv"), str) or isinstance(resume.get("cache"), str)
            ):
                raise ValueError("OCR result has no resumable page")
            if payload["offset"] < resume.get("word_offset", 0):
                raise ValueError("invalid OCR resume cursor")
            data = self._load_resume_unlocked(snapshot)
            if not _valid_resume_position(
                data, payload["offset"], payload.get("tsv_offset", 0)
            ):
                raise ValueError("invalid OCR resume cursor")
        return snapshot, payload["offset"], payload.get("tsv_offset", 0), data

    def load_resume(self, snapshot: dict[str, Any]) -> bytes:
        with self._store_locked():
            return self._load_resume_unlocked(snapshot)

    def _load_resume_unlocked(self, snapshot: dict[str, Any]) -> bytes:
        resume = snapshot.get("resume")
        if not isinstance(resume, dict):
            raise DocumentToolError("OCR result has no resumable page")
        if isinstance(resume.get("tsv"), str):
            return _decode_resume_tsv(resume["tsv"])
        cache = resume.get("cache")
        ident = snapshot.get("id")
        if cache != f"{ident}.resume":
            raise DocumentToolError("Stored OCR resume cache is invalid")
        path = self.root / cache
        flags = os.O_RDONLY | getattr(os, "O_NONBLOCK", 0) | getattr(os, "O_NOFOLLOW", 0)
        try:
            fd = os.open(path, flags)
        except FileNotFoundError as exc:
            raise ValueError("OCR resume cache has expired") from exc
        try:
            if not stat.S_ISREG(os.fstat(fd).st_mode):
                raise DocumentToolError("Stored OCR resume cache is invalid")
            with os.fdopen(fd, "rb") as stream:
                fd = -1
                encoded = stream.read(12 * 1024 * 1024 + 1)
        finally:
            if fd >= 0:
                os.close(fd)
        if len(encoded) > 12 * 1024 * 1024:
            raise DocumentToolError("Stored OCR resume data exceeds its encoded size limit")
        if hashlib.sha256(encoded).hexdigest() != resume.get("cache_sha256"):
            raise DocumentToolError("Stored OCR resume cache is corrupt")
        return _decode_resume_bytes(encoded)


class DocumentExtractor:
    """Run OCR and Office parsing outside the persistent Python kernel."""

    def __init__(self, filesystem: Any) -> None:
        self._filesystem = filesystem
        self._store = _ResultStore(filesystem.workspace)

    async def backends(self) -> dict[str, Any]:
        """Report optional Python packages, Tesseract, and installed language data."""
        return await _run_worker("backends", None, None, {}, limit_seconds=8)

    async def ocr(
        self,
        path: str | os.PathLike[str],
        *,
        language: str = "eng",
        start_page: int = 1,
        max_pages: int = 5,
        dpi: int = 200,
        cursor: str | None = None,
        resume_cursor: str | None = None,
        max_bytes: int = 32_768,
        max_input_bytes: int = _MAX_INPUT_BYTES,
    ) -> dict[str, Any]:
        """OCR PDF, PNG, or JPEG and page immutable text/word-coordinate results."""
        _validate_page("start_page", start_page)
        _validate_range("max_pages", max_pages, 5)
        _validate_range("dpi", dpi, 300)
        if dpi < 36:
            raise ValueError("dpi must be between 36 and 300")
        _validate_page_bytes(max_bytes)
        _validate_range("max_input_bytes", max_input_bytes, _MAX_INPUT_BYTES)
        if cursor is not None and resume_cursor is not None:
            raise ValueError("cursor and resume_cursor cannot be combined")
        if (
            not isinstance(language, str)
            or not language
            or len(language) > 64
            or not all(char.isalnum() or char in "_+-" for char in language)
        ):
            raise ValueError("language must be a short Tesseract language code")
        resolved, display = self._filesystem._path(path)
        if Path(display).suffix.lower() in {".png", ".jpg", ".jpeg"} and start_page != 1:
            raise ValueError("start_page must be 1 for image files")
        if resume_cursor is not None:
            snapshot, offset, tsv_offset, raw = await _thread_settle(
                self._store.decode_resume, resume_cursor, path=display
            )
            return await self._resume_ocr(
                snapshot, offset, tsv_offset, raw, resolved, display, max_bytes
            )
        if cursor is not None:
            snapshot, offset = await _thread_settle(
                self._store.decode, cursor, kind="ocr", path=display
            )
            return await asyncio.to_thread(_page, self._store, snapshot, offset, max_bytes)
        result = await _run_worker(
            "ocr",
            resolved,
            display,
            {
                "language": language,
                "start_page": start_page,
                "max_pages": max_pages,
                "dpi": dpi,
                "max_input_bytes": max_input_bytes,
            },
        )
        snapshot = {
            "kind": "ocr",
            "options": {
                "language": language,
                "start_page": start_page,
                "max_pages": max_pages,
                "dpi": dpi,
                "max_input_bytes": max_input_bytes,
            },
            **result,
        }
        try:
            ident = await _thread_settle(self._store.create, snapshot)
        except DocumentToolError:
            if "resume" not in snapshot:
                raise
            snapshot.pop("resume", None)
            snapshot.setdefault("warnings", []).append(
                "The truncated OCR page could not be cached for resumption"
            )
            ident = await _thread_settle(self._store.create, snapshot)
        snapshot["id"] = ident
        return await asyncio.to_thread(_page, self._store, snapshot, 0, max_bytes)

    async def _resume_ocr(
        self,
        snapshot: dict[str, Any],
        offset: int,
        tsv_offset: int,
        raw: bytes,
        resolved: Path,
        display: str,
        max_bytes: int,
    ) -> dict[str, Any]:
        resume = snapshot["resume"]
        items, item_offsets = await asyncio.to_thread(
            _ocr_tsv_items_with_offsets,
            raw,
            int(resume["page"]),
            offset,
            tsv_offset,
        )
        if items:
            page_snapshot = {
                **snapshot,
                "items": items,
                "complete": False,
                "truncated": True,
                "truncation_reason": "result_size_limit",
                "resume": resume,
            }
            page = await asyncio.to_thread(_page, self._store, page_snapshot, 0, max_bytes)
            consumed = _cursor_offset(page.get("next_cursor"))
            if consumed is None:
                consumed = len(items)
            absolute = offset + consumed
            if absolute < len(items) + offset:
                page["next_cursor"] = None
                page["has_more"] = False
                page["resume_cursor"] = self._store.resume_cursor(
                    snapshot["id"], absolute, item_offsets[consumed]
                )
                page["complete"] = False
                page["truncated"] = True
                return page
            if resume.get("next_page") is None:
                page.pop("resume_cursor", None)
                page["complete"] = True
                page["truncated"] = False
                page["truncation_reason"] = None
                return page
        next_page = resume.get("next_page")
        if next_page is None:
            terminal = {
                **snapshot,
                "items": [],
                "complete": True,
                "truncated": False,
                "truncation_reason": None,
            }
            terminal.pop("resume", None)
            return await asyncio.to_thread(_page, self._store, terminal, 0, max_bytes)
        options = snapshot.get("options", {})
        remaining_pages = max(1, int(resume.get("remaining_pages", 1)))
        current_revision = await asyncio.to_thread(
            _file_revision, resolved, int(options.get("max_input_bytes", _MAX_INPUT_BYTES))
        )
        if current_revision != snapshot.get("source", {}).get("revision"):
            raise ValueError("source changed before the next OCR page was processed")
        result = await _run_worker(
            "ocr",
            resolved,
            display,
            {
                "language": options.get("language", "eng"),
                "start_page": int(next_page),
                "max_pages": remaining_pages,
                "dpi": int(options.get("dpi", 200)),
                "max_input_bytes": int(options.get("max_input_bytes", _MAX_INPUT_BYTES)),
            },
        )
        next_snapshot = {"kind": "ocr", "options": options, **result}
        try:
            ident = await _thread_settle(self._store.create, next_snapshot)
        except DocumentToolError:
            if "resume" not in next_snapshot:
                raise
            next_snapshot.pop("resume", None)
            next_snapshot.setdefault("warnings", []).append(
                "The truncated OCR page could not be cached for resumption"
            )
            ident = await _thread_settle(self._store.create, next_snapshot)
        next_snapshot["id"] = ident
        return await asyncio.to_thread(_page, self._store, next_snapshot, 0, max_bytes)

    async def extract(
        self,
        path: str | os.PathLike[str],
        *,
        cursor: str | None = None,
        max_bytes: int = 32_768,
        max_input_bytes: int = _MAX_INPUT_BYTES,
        cached_values: bool = False,
    ) -> dict[str, Any]:
        """Extract bounded structured blocks from DOCX, PPTX, or XLSX files."""
        _validate_page_bytes(max_bytes)
        _validate_range("max_input_bytes", max_input_bytes, _MAX_INPUT_BYTES)
        if not isinstance(cached_values, bool):
            raise ValueError("cached_values must be a boolean")
        resolved, display = self._filesystem._path(path)
        if cursor is not None:
            snapshot, offset = await _thread_settle(
                self._store.decode, cursor, kind="extract", path=display
            )
            if snapshot.get("options", {}).get("cached_values") != cached_values:
                raise ValueError("cursor belongs to a different document query")
            return await asyncio.to_thread(_page, self._store, snapshot, offset, max_bytes)
        result = await _run_worker(
            "extract",
            resolved,
            display,
            {"max_input_bytes": max_input_bytes, "cached_values": cached_values},
        )
        snapshot = {"kind": "extract", "options": {"cached_values": cached_values}, **result}
        ident = await _thread_settle(self._store.create, snapshot)
        snapshot["id"] = ident
        return await asyncio.to_thread(_page, self._store, snapshot, 0, max_bytes)


async def _thread_settle(function, *args, **kwargs):
    task = asyncio.create_task(asyncio.to_thread(function, *args, **kwargs))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                continue
        with contextlib.suppress(Exception):
            task.result()
        raise


def _page(
    store: _ResultStore, snapshot: dict[str, Any], offset: int, max_bytes: int
) -> dict[str, Any]:
    items = snapshot["items"]
    start = offset
    selected: list[Any] = []

    def response(page_items: list[Any], end: int) -> dict[str, Any]:
        more = end < len(items)
        result = {
            "source": snapshot["source"],
            "items": page_items,
            "offset": start,
            "total_items": len(items),
            "complete": snapshot["complete"],
            "truncated": snapshot["truncated"],
            "truncation_reason": snapshot.get("truncation_reason"),
            "has_more": more,
            "next_cursor": store.cursor(snapshot["id"], end) if more else None,
            "snapshot_id": snapshot["id"],
            "warnings": snapshot.get("warnings", []),
        }
        if snapshot.get("resume"):
            result["resume_cursor"] = store.resume_cursor(
                snapshot["id"],
                int(snapshot["resume"].get("word_offset", 0)),
                int(snapshot["resume"].get("tsv_offset", 0)),
            )
        if "coordinate_space" in snapshot:
            result["coordinate_space"] = snapshot["coordinate_space"]
            result["pages"] = snapshot.get("pages", [])
        if "next_page" in snapshot:
            result["next_page"] = snapshot["next_page"]
        return result

    while offset < len(items):
        candidate = [*selected, items[offset]]
        if (
            len(json.dumps(response(candidate, offset + 1), separators=(",", ":")).encode())
            > max_bytes
        ):
            if not selected:
                raise DocumentToolError("A stored document item exceeds the page output limit")
            break
        selected.append(items[offset])
        offset += 1
    return response(selected, offset)


def _file_revision(path: Path, limit: int) -> str:
    with path.open("rb") as stream:
        digest = hashlib.sha256()
        size = 0
        while chunk := stream.read(1024 * 1024):
            size += len(chunk)
            if size > limit:
                raise ValueError("File exceeds max_input_bytes")
            digest.update(chunk)
    return digest.hexdigest()


def _decode_resume_tsv(encoded: str, *, compressed: bool = False) -> bytes:
    try:
        if len(encoded) > 12 * 1024 * 1024:
            raise DocumentToolError("Stored OCR resume data exceeds its encoded size limit")
        data = base64.b64decode(encoded, validate=True)
    except (ValueError, zlib.error) as exc:
        raise DocumentToolError("Stored OCR resume data is invalid") from exc
    if compressed:
        _decode_resume_bytes(data)
        return data
    return _decode_resume_bytes(data)


def _decode_resume_bytes(data: bytes) -> bytes:
    try:
        decoder = zlib.decompressobj()
        tsv = decoder.decompress(data, 8 * 1024 * 1024 + 1)
        if len(tsv) <= 8 * 1024 * 1024:
            tsv += decoder.flush(8 * 1024 * 1024 + 1 - len(tsv))
        if len(tsv) > 8 * 1024 * 1024 or decoder.unconsumed_tail:
            raise DocumentToolError("Stored OCR resume data exceeds its 8 MiB limit")
    except zlib.error as exc:
        raise DocumentToolError("Stored OCR resume data is invalid") from exc
    return tsv


def _ocr_tsv_items(tsv: bytes, page: int, offset: int) -> list[dict[str, Any]]:
    items, _ = _ocr_tsv_items_with_offsets(tsv, page, offset, 0)
    return items


def _ocr_tsv_items_with_offsets(
    tsv: bytes, page: int, offset: int, tsv_offset: int = 0
) -> tuple[list[dict[str, Any]], list[int]]:
    items: list[dict[str, Any]] = []
    item_offsets: list[int] = []
    word_index = offset if tsv_offset else 0
    rows = tsv.splitlines(keepends=True)
    byte_offset = len(rows[0]) if rows else 0
    for row_bytes in rows[1:]:
        row_offset = byte_offset
        byte_offset = row_offset + len(row_bytes)
        if row_offset < tsv_offset:
            continue
        row = row_bytes.decode("utf-8", "replace").rstrip("\r\n")
        fields = row.split("\t", 11)
        if len(fields) != 12 or fields[0] != "5":
            continue
        word = _clean_ocr_text(fields[11])
        if not word:
            continue
        try:
            confidence = float(fields[10])
            left, top, box_width, box_height = map(int, fields[6:10])
            block, paragraph, line = map(int, fields[2:5])
        except ValueError:
            continue
        if word_index >= offset:
            item_offsets.append(row_offset)
            items.append(
                {
                    "type": "word",
                    "page": page,
                    "text": word,
                    "confidence": confidence,
                    "bbox": [left, top, box_width, box_height],
                    "block": block,
                    "paragraph": paragraph,
                    "line": line,
                }
            )
        word_index += 1
    item_offsets.append(byte_offset)
    return items, item_offsets


def _valid_resume_position(tsv: bytes, offset: int, tsv_offset: int) -> bool:
    if tsv_offset == 0:
        return offset == 0
    items, positions = _ocr_tsv_items_with_offsets(tsv, 1, 0, 0)
    del items
    if tsv_offset not in positions:
        return False
    return positions.index(tsv_offset) == offset


def _clean_ocr_text(text: str) -> str:
    return "".join(char if char in "\t\n\r" or char.isprintable() else " " for char in text)


def _cursor_offset(cursor: str | None) -> int | None:
    if not cursor:
        return None
    try:
        raw = base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4))
        payload = json.loads(raw)
        return payload.get("offset") if type(payload.get("offset")) is int else None
    except (ValueError, TypeError, UnicodeDecodeError, json.JSONDecodeError):
        return None


def _validate_page(name: str, value: int) -> None:
    if type(value) is not int or value < 1:
        raise ValueError(f"{name} must be a positive integer")


def _validate_range(name: str, value: int, maximum: int) -> None:
    if type(value) is not int or not 1 <= value <= maximum:
        raise ValueError(f"{name} must be between 1 and {maximum}")


def _validate_page_bytes(value: int) -> None:
    _validate_range("max_bytes", value, _MAX_PAGE_BYTES)
    if value < 16_384:
        raise ValueError("max_bytes must be at least 16384")


async def _run_worker(
    operation: str,
    path: Path | None,
    display: str | None,
    options: dict[str, Any],
    *,
    limit_seconds: int = _TIMEOUT,
) -> dict[str, Any]:
    request = json.dumps(
        {
            "operation": operation,
            "path": str(path) if path is not None else None,
            "display": display,
            **options,
        },
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode()
    if len(request) > 64 * 1024:
        raise ValueError("Document request exceeds its size limit")
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
            "TESSDATA_PREFIX",
            "OMP_THREAD_LIMIT",
            "SYSTEMROOT",
            "WINDIR",
        }
    }
    env.setdefault("OMP_THREAD_LIMIT", "1")
    command = [_WORKER_PYTHON, "-I", str(_WORKER)]
    if sys.platform == "linux":
        command = [
            _WORKER_PYTHON,
            str(_GUARD),
            "--parent-pid",
            str(os.getpid()),
            "--tree",
            "--",
            *command,
        ]
    async with _WORKERS:
        temporary = tempfile.mkdtemp(prefix="mypr-document-")
        env.update(TMPDIR=temporary, TEMP=temporary, TMP=temporary)
        try:
            return await _run_worker_process(command, request, env, limit_seconds)
        finally:
            await _thread_settle(shutil.rmtree, temporary, True)


async def _run_worker_process(
    command: list[str], request: bytes, env: dict[str, str], limit_seconds: int
) -> dict[str, Any]:
    launch = asyncio.create_task(
        asyncio.create_subprocess_exec(
            *command,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
            env=env,
            start_new_session=True,
        )
    )
    try:
        process = await asyncio.shield(launch)
    except BaseException:
        try:
            process = await _await_uncancelled(launch)
        except BaseException:
            process = None
        if process is not None:
            await _cleanup_uncancelled(process, None)
        raise

    communication = asyncio.create_task(_communicate_bounded(process, request))
    try:
        async with asyncio.timeout(limit_seconds):
            stdout, exceeded = await asyncio.shield(communication)
    except TimeoutError:
        await _cleanup_uncancelled(process, communication)
        raise DocumentToolError(
            f"Document operation exceeded its {limit_seconds}-second time limit"
        ) from None
    except BaseException:
        await _cleanup_uncancelled(process, communication)
        raise
    if exceeded:
        raise DocumentToolError("Document worker response exceeded its 8 MiB size limit")
    if process.returncode != 0:
        raise DocumentToolError(
            f"Document worker exited with status {process.returncode}; "
            "the file may be malformed or exceed resource limits"
        )
    try:
        result = json.loads(stdout)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DocumentToolError("Document worker returned an invalid response") from exc
    if not isinstance(result, dict) or result.get("ok") is not True:
        if not isinstance(result, dict):
            raise DocumentToolError("Document worker returned an invalid response")
        kind = result.get("kind")
        message = result.get("error", "Document operation failed")
        if kind == "MissingDependency":
            raise ImportError(message)
        if kind == "FileNotFoundError":
            raise FileNotFoundError(message)
        if kind == "PermissionError":
            raise PermissionError(message)
        if kind in {"ValueError", "TimeoutError"}:
            if kind == "TimeoutError":
                raise DocumentToolError(message)
            raise ValueError(message)
        raise DocumentToolError(message)
    return result["result"]


async def _communicate_bounded(
    process: asyncio.subprocess.Process, request: bytes
) -> tuple[bytes, bool]:
    async def read_stdout() -> tuple[bytes, bool]:
        chunks: list[bytes] = []
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
                _signal_group(process, signal.SIGTERM)
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


def _signal_group(process: asyncio.subprocess.Process, signum: signal.Signals) -> None:
    try:
        os.killpg(process.pid, signum)
    except ProcessLookupError, PermissionError:
        with contextlib.suppress(ProcessLookupError):
            process.send_signal(signum)


async def _cleanup_uncancelled(
    process: asyncio.subprocess.Process, communication: asyncio.Task | None
) -> None:
    async def cleanup() -> None:
        if process.returncode is None:
            _signal_group(process, signal.SIGTERM)
            try:
                await asyncio.wait_for(asyncio.shield(process.wait()), _CLEANUP_TIMEOUT - 1)
            except TimeoutError:
                _signal_group(process, signal.SIGKILL)
        _signal_group(process, signal.SIGKILL)
        if communication is not None:
            if not communication.done():
                transport = getattr(process, "_transport", None)
                stdout_transport = transport.get_pipe_transport(1) if transport else None
                if stdout_transport:
                    stdout_transport.close()
                communication.cancel()
            await asyncio.gather(communication, return_exceptions=True)
        if process.returncode is None:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(asyncio.shield(process.wait()), 1)

    await wait_owned(cleanup(), propagate=False)


async def _await_uncancelled(task: asyncio.Task):
    return await wait_owned(task, propagate=False)
