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
            info = path.lstat()
        except FileNotFoundError:
            return {"complete": False, "reason": "file_changed_during_scan"}
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
            continue
        data = path.read_bytes()
        scanned_bytes += len(data)
        if scanned_bytes + output_bytes > max_bytes:
            return {"complete": False, "reason": "byte_limit"}
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError:
            continue
        updated, count = expression.subn(
            (lambda _match: replacement) if fixed else replacement,
            text,
        )
        if count == 0 or updated == text:
            continue
        new = updated.encode("utf-8")
        if len(operations) >= max_files:
            return {"complete": False, "reason": "file_limit"}
        output_bytes += len(new)
        if scanned_bytes + output_bytes > max_bytes:
            return {"complete": False, "reason": "byte_limit"}
        operations.append(
            {
                "path": raw,
                "old": base64.b64encode(data).decode("ascii"),
                "new": base64.b64encode(new).decode("ascii"),
                "matches": count,
            }
        )
    return {"complete": True, "operations": operations}


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
