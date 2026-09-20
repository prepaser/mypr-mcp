"""Async, revision-aware filesystem helpers for the workspace API."""

from __future__ import annotations

import asyncio
import codecs
import difflib
import hashlib
import heapq
import os
import stat
import tempfile
from collections.abc import Mapping
from pathlib import Path
from threading import Lock
from typing import Any
from weakref import WeakValueDictionary

_PATH_LOCKS: WeakValueDictionary[Path, asyncio.Lock] = WeakValueDictionary()
_PATH_LOCKS_GUARD = Lock()


class Filesystem:
    """Filesystem operations rooted at a workspace.

    Relative paths are resolved below ``workspace``. Absolute paths are
    intentionally accepted because this API runs inside the trusted
    workstation process. Returned revisions are SHA-256 hashes of the exact
    file bytes. Reads never decode a partial UTF-8 code point; when a long
    line is cut, ``next_cursor`` points at its byte offset so the caller can
    continue from the same line.
    """

    def __init__(
        self, workspace: str | os.PathLike[str], shell: Any = None, searcher: Any = None
    ) -> None:
        self.workspace = Path(workspace).expanduser().resolve()
        self._shell = shell
        self._searcher = searcher

    def _path(self, path: str | os.PathLike[str]) -> tuple[Path, str]:
        supplied = Path(path).expanduser()
        candidate = supplied if supplied.is_absolute() else self.workspace / supplied
        # Resolving follows a symlink for writes, keeping the link itself in
        # place while making relative symlink paths behave like normal files.
        resolved = candidate.resolve(strict=False)
        display = (
            str(resolved.relative_to(self.workspace))
            if _below(resolved, self.workspace)
            else str(resolved)
        )
        return resolved, display or "."

    @staticmethod
    def _lock(path: Path) -> asyncio.Lock:
        with _PATH_LOCKS_GUARD:
            return _PATH_LOCKS.setdefault(path, asyncio.Lock())

    async def read(
        self,
        path: str | os.PathLike[str],
        *,
        start_line: int = 1,
        end_line: int | None = None,
        start_byte: int | None = None,
        max_bytes: int = 32_768,
        encoding: str = "utf-8",
    ) -> dict[str, Any]:
        """Read a bounded, one-based line range with a content revision."""
        _validate_line_range(start_line, end_line, start_byte, max_bytes)
        resolved, display = self._path(path)
        return await _to_thread_uncancelled(
            _read_file,
            resolved,
            display,
            start_line,
            end_line,
            start_byte,
            max_bytes,
            encoding,
        )

    async def write(
        self,
        path: str | os.PathLike[str],
        text: str,
        *,
        expected_hash: str | None = None,
        overwrite: bool = False,
        encoding: str = "utf-8",
        create_parents: bool = False,
    ) -> dict[str, Any]:
        """Atomically create or replace a text file using an optional CAS."""
        if not isinstance(text, str):
            raise TypeError("text must be a string")
        if type(overwrite) is not bool or type(create_parents) is not bool:
            raise TypeError("overwrite and create_parents must be booleans")
        resolved, display = self._path(path)
        encoded = text.encode(encoding)
        lock = self._lock(resolved)
        await lock.acquire()
        try:
            return await _to_thread_uncancelled(
                _write_file,
                resolved,
                display,
                encoded,
                expected_hash,
                overwrite,
                create_parents,
            )
        finally:
            lock.release()

    async def apply_patch(
        self,
        patch: str,
        *,
        expected_hashes: Mapping[str, str | None] | None = None,
        dry_run: bool = False,
        max_diff_bytes: int = 32768,
    ) -> dict[str, Any]:
        from .patching import apply_patch

        return await apply_patch(
            self,
            patch,
            expected_hashes=expected_hashes,
            dry_run=dry_run,
            max_diff_bytes=max_diff_bytes,
        )

    async def image(self, path: str | os.PathLike[str], *, max_bytes: int = 2 * 1024 * 1024):
        """Load a PNG or JPEG for inline display in an execute result."""
        if type(max_bytes) is not int or max_bytes < 1:
            raise ValueError("max_bytes must be a positive integer")
        resolved, display = self._path(path)
        data, image_format = await _to_thread_uncancelled(_read_image, resolved, display, max_bytes)
        from IPython.display import Image

        return Image(data=data, format=image_format, embed=True)

    async def patch(
        self,
        path: str | os.PathLike[str],
        edits: list[dict[str, Any]],
        *,
        expected_hash: str | None = None,
        dry_run: bool = False,
        encoding: str = "utf-8",
        max_diff_bytes: int = 32_768,
    ) -> dict[str, Any]:
        """Apply exact text replacements atomically and return a bounded diff."""
        if not isinstance(edits, list):
            raise TypeError("edits must be a list")
        if type(dry_run) is not bool:
            raise TypeError("dry_run must be a boolean")
        if (
            not isinstance(max_diff_bytes, int)
            or isinstance(max_diff_bytes, bool)
            or max_diff_bytes < 1
        ):
            raise ValueError("max_diff_bytes must be a positive integer")
        resolved, display = self._path(path)
        lock = self._lock(resolved)
        await lock.acquire()
        try:
            return await _to_thread_uncancelled(
                _patch_file,
                resolved,
                display,
                edits,
                expected_hash,
                dry_run,
                encoding,
                max_diff_bytes,
            )
        finally:
            lock.release()

    async def search(self, pattern=None, **options):
        return await self._search_query("rg", pattern, options)

    async def search_docs(self, pattern=None, **options):
        return await self._search_query("rga", pattern, options)

    async def search_ast(self, pattern=None, **options):
        return await self._search_query("ast", pattern, options)

    async def search_backends(self):
        return await self._search_query("info", None, {})

    async def _search_query(self, backend, pattern, options):
        if "backend" in options:
            raise ValueError("select a search method instead of overriding backend")
        args = dict(pattern=pattern, backend=backend, **options)
        if self._searcher is not None:
            return await self._searcher(**args)
        if self._shell is None:
            raise RuntimeError("workspace search requires the workspace shell")
        from .search import Search

        return await Search(self.workspace, self._shell).search(**args)

    async def tree(
        self,
        path: str | os.PathLike[str] = ".",
        *,
        depth: int = 3,
        max_entries: int = 200,
        hidden: bool = False,
    ) -> dict[str, Any]:
        """Return a deterministic, bounded directory tree without following links."""
        if not isinstance(depth, int) or isinstance(depth, bool) or depth < 0:
            raise ValueError("depth must be a non-negative integer")
        if not isinstance(max_entries, int) or isinstance(max_entries, bool) or max_entries < 1:
            raise ValueError("max_entries must be a positive integer")
        if type(hidden) is not bool:
            raise TypeError("hidden must be a boolean")
        candidate = _lexical_path(self.workspace, path)
        return await _to_thread_uncancelled(
            _tree,
            candidate,
            _display_path(candidate, self.workspace),
            self.workspace,
            depth,
            max_entries,
            hidden,
        )

    async def stat(
        self,
        path: str | os.PathLike[str],
        *,
        follow_symlinks: bool = False,
    ) -> dict[str, Any]:
        """Return bounded metadata for a path, preserving terminal symlinks by default."""
        if type(follow_symlinks) is not bool:
            raise TypeError("follow_symlinks must be a boolean")
        candidate = _lexical_path(self.workspace, path)
        return await _to_thread_uncancelled(
            _stat_path,
            candidate,
            _display_path(candidate, self.workspace),
            follow_symlinks,
        )


def _below(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def _lexical_path(root: Path, path: str | os.PathLike[str]) -> Path:
    supplied = Path(path).expanduser()
    candidate = supplied if supplied.is_absolute() else root / supplied
    return Path(os.path.normpath(candidate))


def _display_path(path: Path, root: Path) -> str:
    try:
        relative = path.relative_to(root)
    except ValueError:
        return str(path)
    return str(relative) or "."


def _metadata(path: Path, display: str, *, follow_symlinks: bool = False) -> dict[str, Any]:
    info = path.stat() if follow_symlinks else path.lstat()
    mode = stat.S_IMODE(info.st_mode)
    if stat.S_ISLNK(info.st_mode):
        kind = "symlink"
    elif stat.S_ISDIR(info.st_mode):
        kind = "directory"
    elif stat.S_ISREG(info.st_mode):
        kind = "file"
    else:
        kind = "other"
    result: dict[str, Any] = {
        "path": display,
        "kind": kind,
        "size": info.st_size,
        "mtime_ns": info.st_mtime_ns,
        "mode": mode,
    }
    if kind == "symlink":
        result["target"] = os.readlink(path)
    return result


class _ReverseName:
    __slots__ = ("value",)

    def __init__(self, value: str) -> None:
        self.value = value

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, _ReverseName):
            return NotImplemented
        return self.value > other.value


def _children(path: Path, limit: int, hidden: bool) -> tuple[list[os.DirEntry[str]], bool]:
    heap: list[tuple[_ReverseName, str, os.DirEntry[str]]] = []
    try:
        entries = os.scandir(path)
        with entries:
            for entry in entries:
                if not hidden and entry.name.startswith("."):
                    continue
                item = (_ReverseName(entry.name), entry.name, entry)
                if len(heap) < limit:
                    heapq.heappush(heap, item)
                elif entry.name < heap[0][1]:
                    heapq.heapreplace(heap, item)
    except OSError:
        raise
    return [item[2] for item in sorted(heap, key=lambda item: item[1])], len(heap) == limit


def _tree(
    path: Path,
    display: str,
    root: Path,
    depth: int,
    max_entries: int,
    hidden: bool,
) -> dict[str, Any]:
    count = 0
    truncated = False

    def visit(current: Path, current_display: str, remaining_depth: int) -> dict[str, Any]:
        nonlocal count, truncated
        node = _metadata(current, current_display)
        if node["kind"] != "directory" or remaining_depth <= 0:
            return node
        if count >= max_entries:
            truncated = True
            return node
        children, overflow = _children(current, max_entries - count + 1, hidden)
        truncated |= overflow
        result_children: list[dict[str, Any]] = []
        for entry in children:
            if count >= max_entries:
                truncated = True
                break
            child = Path(entry.path)
            child_display = _display_path(child, root)
            count += 1
            result_children.append(visit(child, child_display, remaining_depth - 1))
        if result_children:
            node["entries"] = result_children
        return node

    result = visit(path, display, depth)
    result["truncated"] = truncated
    return result


def _stat_path(path: Path, display: str, follow_symlinks: bool) -> dict[str, Any]:
    return _metadata(path, display, follow_symlinks=follow_symlinks)


def _validate_line_range(
    start_line: int, end_line: int | None, start_byte: int | None, max_bytes: int
) -> None:
    if not isinstance(start_line, int) or isinstance(start_line, bool) or start_line < 1:
        raise ValueError("start_line must be a positive integer")
    if end_line is not None and (
        not isinstance(end_line, int) or isinstance(end_line, bool) or end_line < start_line
    ):
        raise ValueError("end_line must be at least start_line")
    if start_byte is not None and (
        not isinstance(start_byte, int) or isinstance(start_byte, bool) or start_byte < 0
    ):
        raise ValueError("start_byte must be a non-negative integer")
    if not isinstance(max_bytes, int) or isinstance(max_bytes, bool) or max_bytes < 1:
        raise ValueError("max_bytes must be a positive integer")


def _read_image(path: Path, display: str, limit: int) -> tuple[bytes, str]:
    fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK)
    try:
        before = os.fstat(fd)
        _require_regular(before, display)
        if before.st_size > limit:
            raise ValueError(f"Image exceeds max_bytes: {display}")
        with os.fdopen(fd, "rb") as stream:
            fd = -1
            data = stream.read(limit + 1)
            after = os.fstat(stream.fileno())
        if len(data) > limit:
            raise ValueError(f"Image exceeds max_bytes: {display}")
        if _signature(before) != _signature(after):
            raise RuntimeError(f"File changed while reading: {display}")
        if data.startswith(b"\x89PNG\r\n\x1a\n"):
            return data, "png"
        if data.startswith(b"\xff\xd8\xff"):
            return data, "jpeg"
        raise ValueError("image expects a PNG or JPEG file")
    finally:
        if fd >= 0:
            os.close(fd)


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _signature(info: os.stat_result) -> tuple[int, int, int, int, int]:
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def _require_regular(info: os.stat_result, display: str) -> None:
    if stat.S_ISDIR(info.st_mode):
        raise IsADirectoryError(display)
    if not stat.S_ISREG(info.st_mode):
        raise ValueError(f"Path must be a regular file: {display}")


def _read_regular(path: Path, display: str) -> tuple[bytes, os.stat_result]:
    fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK)
    try:
        info = os.fstat(fd)
        _require_regular(info, display)
        with os.fdopen(fd, "rb") as stream:
            fd = -1
            return stream.read(), info
    finally:
        if fd >= 0:
            os.close(fd)


def _check_expected(old: bytes | None, expected_hash: str | None) -> None:
    if expected_hash is not None and (old is None or _sha256(old) != expected_hash):
        actual = None if old is None else _sha256(old)
        raise ValueError(f"Revision mismatch: expected {expected_hash}, got {actual}")


def _read_file(
    path: Path,
    display: str,
    start_line: int,
    end_line: int | None,
    start_byte: int | None,
    max_bytes: int,
    encoding: str,
) -> dict[str, Any]:
    if encoding.lower().replace("-", "") not in {"utf8"}:
        raise ValueError("read currently supports UTF-8 only")
    before = path.stat()
    _require_regular(before, display)
    digest = hashlib.sha256()
    decoder = codecs.getincrementaldecoder("utf-8")()
    output = bytearray()
    capture_limit = max_bytes + 3
    selected_seen = 0
    line_no = 1
    total = 0
    has_data = False
    start_line_at_byte: int | None = 1 if start_byte == 0 else None
    capture_start: int | None = start_byte
    ends_newline = False
    trunc_line: int | None = None

    def validate(chunk: bytes) -> None:
        nonlocal start_line_at_byte
        if start_byte is not None and start_byte == total:
            if decoder.getstate()[0]:
                raise ValueError("start_byte must be at an encoding boundary")
            start_line_at_byte = line_no
        if start_byte is None or not total < start_byte < total + len(chunk):
            decoder.decode(chunk, final=False)
            return
        split = start_byte - total
        decoder.decode(chunk[:split], final=False)
        if decoder.getstate()[0]:
            raise ValueError("start_byte must be at an encoding boundary")
        start_line_at_byte = start_line_at_byte or line_no
        decoder.decode(chunk[split:], final=False)

    def capture(segment: bytes, segment_start: int, current_line: int) -> None:
        nonlocal capture_start, selected_seen, trunc_line
        if current_line < start_line or (end_line is not None and current_line > end_line):
            return
        if start_byte is not None:
            segment = segment[max(0, start_byte - segment_start) :]
        if not segment:
            return
        if capture_start is None:
            capture_start = segment_start
        selected_seen += len(segment)
        if len(output) < capture_limit:
            output.extend(segment[: capture_limit - len(output)])
        if selected_seen > max_bytes and trunc_line is None:
            trunc_line = current_line

    fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK)
    opened = os.fstat(fd)
    try:
        _require_regular(opened, display)
        stream = os.fdopen(fd, "rb")
        fd = -1
    finally:
        if fd >= 0:
            os.close(fd)
    with stream:
        while chunk := stream.read(64 * 1024):
            has_data = True
            digest.update(chunk)
            validate(chunk)
            position = 0
            while position < len(chunk):
                newline = chunk.find(b"\n", position)
                if newline < 0:
                    if start_byte is not None and total + position <= start_byte < total + len(
                        chunk
                    ):
                        start_line_at_byte = line_no
                    capture(chunk[position:], total + position, line_no)
                    break
                segment_end = newline + 1
                if start_byte is not None and total + position <= start_byte < total + segment_end:
                    start_line_at_byte = line_no
                capture(chunk[position:segment_end], total + position, line_no)
                line_no += 1
                if start_byte == total + segment_end:
                    start_line_at_byte = line_no
                ends_newline = True
                position = segment_end
            total += len(chunk)
            ends_newline = chunk.endswith(b"\n")
    decoder.decode(b"", final=True)
    after = path.stat()
    if _signature(before) != _signature(after):
        raise RuntimeError(f"File changed while reading: {display}")
    if start_byte is not None and start_byte > total:
        raise ValueError("start_byte is beyond the file")
    if start_byte is not None and start_byte == total:
        start_line_at_byte = line_no
    if (
        start_byte is not None
        and start_line_at_byte is not None
        and start_line_at_byte != start_line
    ):
        raise ValueError("start_byte does not belong to start_line")
    if not has_data:
        line_no = 0
    if trunc_line is not None:
        valid = _utf8_prefix(bytes(output), max_bytes)
        if not valid:
            raise ValueError("max_bytes is too small for the first UTF-8 code point")
        text = valid.decode("utf-8")
        next_byte = (capture_start if capture_start is not None else 0) + len(valid)
        next_line = trunc_line
        actual_end = start_line + valid.count(b"\n") - (1 if valid.endswith(b"\n") else 0)
    else:
        valid = bytes(output)
        text = valid.decode("utf-8")
        next_byte = None
        next_line = None
        last_line = line_no - 1 if ends_newline else line_no
        actual_end = (
            start_line - 1
            if not valid
            else last_line
            if end_line is None
            else min(last_line, end_line)
        )
    revision = digest.hexdigest()
    return {
        "path": display,
        "text": text,
        "start_line": start_line,
        "start_byte": start_byte,
        "end_line": actual_end,
        "next_line": next_line,
        "next_byte": next_byte,
        "next_cursor": None if next_line is None else {"line": next_line, "byte": next_byte},
        "truncated": trunc_line is not None,
        "revision": revision,
        "sha256": revision,
        "size": total,
    }


def _utf8_prefix(data: bytes, limit: int) -> bytes:
    candidate = data[:limit]
    try:
        candidate.decode("utf-8")
    except UnicodeDecodeError as exc:
        if exc.reason != "unexpected end of data":
            raise
        candidate = candidate[: exc.start]
    return candidate


def _write_file(
    path: Path,
    display: str,
    data: bytes,
    expected_hash: str | None,
    overwrite: bool,
    create_parents: bool,
) -> dict[str, Any]:
    try:
        old_stat = path.stat()
    except FileNotFoundError:
        old_stat = None
    if old_stat is not None:
        _require_regular(old_stat, display)
        old, old_stat = _read_regular(path, display)
    else:
        old = None
    _check_expected(old, expected_hash)
    if old is not None and not (overwrite or expected_hash is not None):
        raise FileExistsError(f"File already exists: {display}")
    if create_parents:
        path.parent.mkdir(parents=True, exist_ok=True)
    elif not path.parent.exists():
        raise FileNotFoundError(str(path.parent))
    info = _atomic_write(path, data, old, old_stat)
    return _write_result(display, data, info, old is not None)


def _patch_file(
    path: Path,
    display: str,
    edits: list[dict[str, Any]],
    expected_hash: str | None,
    dry_run: bool,
    encoding: str,
    max_diff_bytes: int,
) -> dict[str, Any]:
    old_stat = path.stat()
    _require_regular(old_stat, display)
    old, old_stat = _read_regular(path, display)
    _check_expected(old, expected_hash)
    original = old.decode(encoding)
    updated = _apply_edits(original, edits)
    old_hash = _sha256(old)
    new_bytes = updated.encode(encoding)
    new_hash = _sha256(new_bytes)
    diff, diff_truncated = _bounded_diff(display, original, updated, max_diff_bytes)
    if not dry_run and new_bytes != old:
        _atomic_write(path, new_bytes, old, old_stat)
    return {
        "path": display,
        "changed": new_bytes != old,
        "dry_run": dry_run,
        "old_revision": old_hash,
        "revision": new_hash,
        "sha256": new_hash,
        "size": len(new_bytes),
        "diff": diff,
        "diff_truncated": diff_truncated,
    }


def _apply_edits(original: str, edits: list[dict[str, Any]]) -> str:
    result = original
    for number, edit in enumerate(edits, start=1):
        if (
            not isinstance(edit, dict)
            or not isinstance(edit.get("old"), str)
            or not isinstance(edit.get("new"), str)
        ):
            raise TypeError(f"edit {number} requires string old and new values")
        old, new = edit["old"], edit["new"]
        if not old:
            raise ValueError(f"edit {number} has an empty old value")
        count = edit.get("count")
        if count is None:
            matches = result.count(old)
            if matches != 1:
                raise ValueError(f"edit {number} expected one match, found {matches}")
            result = result.replace(old, new, 1)
            continue
        if count == "all" or (type(count) is int and count == -1):
            if result.count(old) == 0:
                raise ValueError(f"edit {number} expected at least one match, found 0")
            result = result.replace(old, new)
            continue
        if not isinstance(count, int) or isinstance(count, bool) or count < 1:
            raise ValueError(f"edit {number} count must be a positive integer or 'all'")
        matches = result.count(old)
        if matches < count:
            raise ValueError(f"edit {number} requested {count} matches, found {matches}")
        result = result.replace(old, new, count)
    return result


def _bounded_diff(path: str, old: str, new: str, limit: int) -> tuple[str, bool]:
    lines = difflib.unified_diff(
        _diff_lines(old),
        _diff_lines(new),
        fromfile=path,
        tofile=path,
    )
    output = bytearray()
    truncated = False
    for line in lines:
        if not line.endswith("\n"):
            line += "\n\\ No newline at end of file\n"
        encoded = line.encode("utf-8")
        if len(output) + len(encoded) > limit:
            truncated = True
            break
        output.extend(encoded)
    return output.decode("utf-8"), truncated


def _diff_lines(value: str) -> list[str]:
    if not value:
        return []
    parts = value.split("\n")
    lines = [f"{part}\n" for part in parts[:-1]]
    if parts[-1]:
        lines.append(parts[-1])
    return lines


def _atomic_write(
    path: Path,
    data: bytes,
    old: bytes | None,
    old_stat: os.stat_result | None,
) -> os.stat_result:
    mode = stat.S_IMODE(old_stat.st_mode) if old_stat is not None else 0o600
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary_path = Path(temporary)
    try:
        with os.fdopen(fd, "wb") as stream:
            os.fchmod(stream.fileno(), mode)
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        if old is None:
            os.link(temporary_path, path)
            temporary_path.unlink(missing_ok=True)
        else:
            try:
                current = path.stat()
                current_data, current = _read_regular(path, str(path))
                unchanged = _signature(current) == _signature(old_stat) and current_data == old
            except FileNotFoundError:
                unchanged = False
            if not unchanged:
                raise RuntimeError(f"File changed while updating: {path}")
            os.replace(temporary_path, path)
        return path.stat()
    finally:
        temporary_path.unlink(missing_ok=True)


def _write_result(
    display: str, data: bytes, info: os.stat_result, overwritten: bool
) -> dict[str, Any]:
    digest = _sha256(data)
    return {
        "path": display,
        "created": not overwritten,
        "overwritten": overwritten,
        "revision": digest,
        "sha256": digest,
        "size": len(data),
        "mode": stat.S_IMODE(info.st_mode),
    }


async def _to_thread_uncancelled(function, *args, **kwargs):
    task = asyncio.create_task(asyncio.to_thread(function, *args, **kwargs))
    cancelled = False
    while True:
        try:
            result = await asyncio.shield(task)
        except asyncio.CancelledError:
            cancelled = True
            continue
        break
    if cancelled:
        raise asyncio.CancelledError
    return result
