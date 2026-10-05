"""Bounded JSON return values for completed Python tasks."""

from __future__ import annotations

import hashlib
import json
import math
import os
import stat
import tempfile
from pathlib import Path
from typing import Any

from .storage_lock import StorageLock
from .transport import ensure_workspace_identity

MAX_RESULT_BYTES = 256 * 1024


def encode_result(value: Any) -> str:
    pending = [(value, 0)]
    nodes = 0
    while pending:
        item, depth = pending.pop()
        nodes += 1
        if depth > 64 or nodes > 100_000:
            raise ValueError("task result exceeds the JSON structure limit")
        kind = type(item)
        if kind is str:
            if len(item) > MAX_RESULT_BYTES:
                raise ValueError("task result exceeds the 256 KiB limit")
            try:
                item.encode("utf-8", "surrogateescape")
            except UnicodeEncodeError:
                raise ValueError("task result contains invalid Unicode") from None
        elif kind in (list, tuple):
            if len(item) > 100_000:
                raise ValueError("task result exceeds the JSON structure limit")
            pending.extend((child, depth + 1) for child in item)
        elif kind is dict:
            if len(item) > 100_000 or any(type(key) is not str for key in item):
                raise ValueError("task result requires string JSON keys")
            pending.extend((child, depth + 1) for pair in item.items() for child in pair)
        elif kind is float:
            if not math.isfinite(item):
                raise ValueError("task result requires finite JSON numbers")
        elif kind not in (type(None), bool, int):
            raise TypeError("task result must contain only JSON values")
    chunks = []
    size = 0
    encoder = json.JSONEncoder(ensure_ascii=False, allow_nan=False, separators=(",", ":"))
    for chunk in encoder.iterencode(value):
        chunk = chunk.encode("utf-8", "backslashreplace").decode("utf-8")
        size += len(chunk.encode("utf-8"))
        if size > MAX_RESULT_BYTES:
            raise ValueError("task result exceeds the 256 KiB limit")
        chunks.append(chunk)
    return "".join(chunks)


def store_result(
    workspace: Path,
    task_id: str,
    generation: str,
    encoded: str,
    *,
    commit=None,
    workspace_identity: str | None = None,
) -> dict[str, Any]:
    ensure_workspace_identity(workspace, workspace_identity)
    with StorageLock(Path(workspace) / ".mypr" / "storage.lock"):
        ensure_workspace_identity(workspace, workspace_identity)
        reference = _store_result(workspace, task_id, generation, encoded)
        if commit is not None:
            commit(reference)
        return reference


def _store_result(workspace: Path, task_id: str, generation: str, encoded: str) -> dict[str, Any]:
    if (
        not isinstance(encoded, str)
        or len(encoded.encode("utf-8", "backslashreplace")) > MAX_RESULT_BYTES
    ):
        raise ValueError("task result exceeds the 256 KiB limit")
    value = json.loads(encoded, parse_constant=lambda _: _invalid_number())
    encoded = encode_result(value)
    data = encoded.encode("utf-8")
    root = Path(workspace).resolve()
    folder = root / ".mypr" / "task-results"
    folder.mkdir(parents=True, exist_ok=True)
    if folder.resolve() != folder:
        raise ValueError("task result directory must not be a symlink")
    key = hashlib.sha256(json.dumps([generation, task_id]).encode()).hexdigest()
    path = folder / f"{key}.json"
    fd, temporary = tempfile.mkstemp(prefix=".result-", suffix=".tmp", dir=folder)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory_fd = os.open(folder, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        Path(temporary).unlink(missing_ok=True)
    return {
        "path": str(path.relative_to(root)),
        "sha256": hashlib.sha256(data).hexdigest(),
        "size": len(data),
    }


def load_result(
    workspace: Path, reference: dict[str, Any], *, workspace_identity: str | None = None
) -> Any:
    ensure_workspace_identity(workspace, workspace_identity)
    with StorageLock(Path(workspace) / ".mypr" / "storage.lock"):
        ensure_workspace_identity(workspace, workspace_identity)
        return _load_result(workspace, reference)


def _load_result(workspace: Path, reference: dict[str, Any]) -> Any:
    root = Path(workspace).resolve()
    relative = reference.get("path")
    if not isinstance(relative, str):
        raise ValueError("invalid task result reference")
    path = root / relative
    if path.parent != root / ".mypr" / "task-results" or path.resolve() != path:
        raise ValueError("invalid task result path")
    if len(path.stem) != 64 or any(char not in "0123456789abcdef" for char in path.stem):
        raise ValueError("invalid task result name")
    flags = os.O_RDONLY | getattr(os, "O_NONBLOCK", 0) | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags)
    with os.fdopen(fd, "rb") as stream:
        if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
            raise ValueError("task result is not a regular file")
        data = stream.read(MAX_RESULT_BYTES + 1)
    if len(data) > MAX_RESULT_BYTES or len(data) != reference.get("size"):
        raise ValueError("task result size does not match its record")
    if hashlib.sha256(data).hexdigest() != reference.get("sha256"):
        raise ValueError("task result hash does not match its record")
    return json.loads(data, parse_constant=lambda _: _invalid_number())


def _invalid_number():
    raise ValueError("task result requires finite JSON numbers")
