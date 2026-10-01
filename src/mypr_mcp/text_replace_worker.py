"""Worker process for bounded, dialect-consistent text replacement."""

from __future__ import annotations

import base64
import json
import os
import re
import stat
import sys
from pathlib import Path

MAX_FILES = 100
MAX_BYTES = 16 * 1024 * 1024


class _ByteLimit(Exception):
    pass


class _FileChanged(Exception):
    pass


def main() -> int:
    try:
        request = json.loads(base64.b64decode(sys.stdin.buffer.read(), validate=True))
        result = replace(request)
        encoded = json.dumps(result, ensure_ascii=True, separators=(",", ":")).encode()
        sys.stdout.buffer.write(encoded)
        return 0
    except Exception as exc:
        result = {"complete": False, "reason": f"worker_error:{type(exc).__name__}"}
        sys.stdout.buffer.write(json.dumps(result, separators=(",", ":")).encode())
        return 0


def replace(request: dict) -> dict:
    if not isinstance(request, dict):
        return {"complete": False, "reason": "invalid_request"}
    root = Path(request.get("root", "")).resolve()
    values = request.get("paths")
    pattern = request.get("pattern")
    replacement = request.get("replacement")
    fixed = request.get("fixed", True)
    ignore_case = request.get("ignore_case", False)
    max_files = request.get("max_files", MAX_FILES)
    max_bytes = request.get("max_bytes", MAX_BYTES)
    if (
        not root.is_dir()
        or not isinstance(values, list)
        or not isinstance(pattern, str)
        or not isinstance(replacement, str)
        or type(fixed) is not bool
        or type(ignore_case) is not bool
        or type(max_files) is not int
        or not 1 <= max_files <= MAX_FILES
        or type(max_bytes) is not int
        or not 1 <= max_bytes <= MAX_BYTES
    ):
        return {"complete": False, "reason": "invalid_request"}
    try:
        expression = re.compile(
            re.escape(pattern) if fixed else pattern,
            re.IGNORECASE if ignore_case else 0,
        )
    except re.error:
        return {"complete": False, "reason": "invalid_pattern"}
    operations = []
    scanned_bytes = 0
    output_bytes = 0
    for raw in values:
        if not isinstance(raw, str):
            return {"complete": False, "reason": "invalid_path"}
        path = _lexical_path(root, raw)
        if not _inside(root, path) or _has_symlink_component(root, path):
            return {"complete": False, "reason": "path_outside_workspace"}
        if path == root or ".mypr" in path.relative_to(root).parts:
            continue
        try:
            data = _read_bounded(path, max_bytes - scanned_bytes - output_bytes)
        except FileNotFoundError:
            return {"complete": False, "reason": "file_changed_during_scan"}
        except _ByteLimit:
            return {"complete": False, "reason": "byte_limit"}
        except _FileChanged:
            return {"complete": False, "reason": "file_changed_during_scan"}
        if data is None:
            continue
        scanned_bytes += len(data)
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError:
            continue
        try:
            new, count, changed = _substitute_bounded(
                text,
                expression,
                replacement,
                fixed,
                max_bytes - scanned_bytes - output_bytes,
            )
        except (IndexError, re.error, ValueError):
            return {"complete": False, "reason": "invalid_replacement"}
        if count == 0 or not changed:
            continue
        if len(operations) >= max_files:
            return {"complete": False, "reason": "file_limit"}
        if new is None:
            return {"complete": False, "reason": "byte_limit"}
        output_bytes += len(new)
        operations.append(
            {
                "path": raw,
                "old": base64.b64encode(data).decode("ascii"),
                "new": base64.b64encode(new).decode("ascii"),
                "matches": count,
            }
        )
    return {"complete": True, "operations": operations}


def _read_bounded(path: Path, limit: int) -> bytes | None:
    """Read one regular file without exceeding the remaining input budget."""
    info = path.lstat()
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
        return None
    if info.st_size > limit:
        raise _ByteLimit
    flags = os.O_RDONLY | os.O_NONBLOCK
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    fd = os.open(path, flags)
    try:
        opened = os.fstat(fd)
        if not stat.S_ISREG(opened.st_mode):
            return None
        if opened.st_size > limit:
            raise _ByteLimit
        with os.fdopen(fd, "rb") as stream:
            fd = -1
            data = stream.read(limit + 1)
            after = os.fstat(stream.fileno())
        if len(data) > limit:
            raise _ByteLimit
        if _signature(info) != _signature(opened) or _signature(info) != _signature(after):
            raise _FileChanged
        return data
    finally:
        if fd >= 0:
            os.close(fd)


def _signature(info: os.stat_result) -> tuple[int, int, int, int, int]:
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def _substitute_bounded(
    text: str,
    expression: re.Pattern[str],
    replacement: str,
    fixed: bool,
    output_limit: int,
) -> tuple[bytes | None, int, bool]:
    """Apply substitutions in two passes, bounding the materialized result."""
    template = None if fixed else _parse_template(replacement, expression)
    cursor = 0
    output_size = 0
    count = 0
    changed = False
    for match in expression.finditer(text):
        source = text[cursor : match.start()]
        replacement_size, replacement_equal = _replacement_info(
            match,
            template,
            replacement,
            text[match.start() : match.end()],
        )
        output_size += len(source.encode("utf-8")) + replacement_size
        changed |= not replacement_equal
        count += 1
        cursor = match.end()
        if changed and output_size > output_limit:
            return None, count, True
    tail = text[cursor:]
    output_size += len(tail.encode("utf-8"))
    if count == 0 or not changed:
        return None, count, False
    if output_size > output_limit:
        return None, count, True

    result = bytearray(output_size)
    position = 0
    cursor = 0
    for match in expression.finditer(text):
        source = text[cursor : match.start()].encode("utf-8")
        result[position : position + len(source)] = source
        position += len(source)
        position = _write_replacement(result, position, match, template, replacement)
        cursor = match.end()
    tail = text[cursor:].encode("utf-8")
    result[position:] = tail
    return bytes(result), count, True


def _replacement_parts(
    match: re.Match[str], template: list[str | int] | None, replacement: str
):
    if template is None:
        yield replacement
        return
    for part in template:
        yield (match.group(part) or "") if isinstance(part, int) else part


def _replacement_info(
    match: re.Match[str],
    template: list[str | int] | None,
    replacement: str,
    matched: str,
) -> tuple[int, bool]:
    size = 0
    offset = 0
    equal = True
    for part in _replacement_parts(match, template, replacement):
        size += len(part.encode("utf-8"))
        if equal and not matched.startswith(part, offset):
            equal = False
        offset += len(part)
    return size, equal and offset == len(matched)


def _write_replacement(
    output: bytearray,
    position: int,
    match: re.Match[str],
    template: list[str | int] | None,
    replacement: str,
) -> int:
    for part in _replacement_parts(match, template, replacement):
        encoded = part.encode("utf-8")
        output[position : position + len(encoded)] = encoded
        position += len(encoded)
    return position


def _parse_template(replacement: str, expression: re.Pattern[str]) -> list[str | int]:
    parser = getattr(re, "_parser", None)
    parse_template = getattr(parser, "parse_template", None)
    if parse_template is None:
        raise re.error("replacement templates are unavailable")
    return parse_template(replacement, expression)


def _lexical_path(root: Path, value: str) -> Path:
    supplied = Path(value).expanduser()
    candidate = supplied if supplied.is_absolute() else root / supplied
    return candidate


def _inside(root: Path, path: Path) -> bool:
    normalized = Path(os.path.normpath(str(path)))
    try:
        normalized.relative_to(root)
    except ValueError:
        return False
    return True


def _has_symlink_component(root: Path, path: Path) -> bool:
    current = Path(path.anchor) if path.is_absolute() else root
    parts = path.parts[1:] if path.is_absolute() else path.parts
    for part in parts:
        if part in ("", "."):
            continue
        if part == "..":
            current = current.parent
            continue
        current /= part
        if current.is_symlink():
            return True
    return False


if __name__ == "__main__":
    raise SystemExit(main())
