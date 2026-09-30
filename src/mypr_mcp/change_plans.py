"""Durable bounded plans for multi-file workspace changes."""

from __future__ import annotations

import base64
import json
import os
import secrets
import stat
import tempfile
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from .storage_lock import StorageLock

MAX_FILES = 100
MAX_INPUT_OUTPUT_BYTES = 16 * 1024 * 1024
MAX_STORE_BYTES = 64 * 1024 * 1024
MAX_PLANS = 16
PLAN_TTL = 60 * 60


class ChangePlanError(ValueError):
    """A change plan is invalid, stale, or no longer available."""


class ChangePlanStore:
    """Persist bounded JSON plans below ``.mypr/change-plans/<kind>``.

    Payloads may contain ``Path`` and ``bytes`` values. Bytes are encoded as
    base64 and decoded on load, so callers can keep exact source buffers in a
    plan without relying on the current process memory.
    """

    def __init__(
        self,
        workspace: str | os.PathLike[str],
        kind: str,
        *,
        max_files: int = MAX_FILES,
        max_input_output_bytes: int = MAX_INPUT_OUTPUT_BYTES,
    ) -> None:
        if not isinstance(kind, str) or not kind or "/" in kind or "\\" in kind:
            raise ValueError("plan kind must be a non-empty name")
        if type(max_files) is not int or not 1 <= max_files <= MAX_FILES:
            raise ValueError(f"max_files must be between 1 and {MAX_FILES}")
        if (
            type(max_input_output_bytes) is not int
            or not 1 <= max_input_output_bytes <= MAX_STORE_BYTES
        ):
            raise ValueError(
                f"max_input_output_bytes must be between 1 and {MAX_STORE_BYTES}"
            )
        self.workspace = Path(workspace).expanduser().resolve()
        self.root = self.workspace / ".mypr" / "change-plans" / kind
        self.kind = kind
        self.max_files = max_files
        self.max_input_output_bytes = max_input_output_bytes
        self.lock_path = self.workspace / ".mypr" / "storage.lock"

    def create(self, payload: Mapping[str, Any]) -> str:
        if not isinstance(payload, Mapping):
            raise TypeError("plan payload must be a mapping")
        value = _encode(dict(payload))
        if not isinstance(value, dict):
            raise TypeError("plan payload must be an object")
        value.setdefault("created_at", time.time())
        value["workspace_id"] = _workspace_identity(self.workspace)
        operations = value.get("operations", value.get("files"))
        if operations is not None:
            if not isinstance(operations, list) or len(operations) > self.max_files:
                raise ChangePlanError(f"plan contains more than {self.max_files} files")
        encoded = json.dumps(value, ensure_ascii=True, separators=(",", ":")).encode("ascii")
        if len(encoded) > MAX_STORE_BYTES:
            raise ChangePlanError("change plan exceeds the bounded plan store")
        if _payload_bytes(value) > self.max_input_output_bytes:
            raise ChangePlanError("change plan source buffers exceed the bounded plan size")
        ident = secrets.token_hex(16)
        self.root.mkdir(parents=True, exist_ok=True)
        with self._lock():
            files = self._files()
            now = time.time()
            valid = [path for path in files if now - path.stat().st_mtime <= PLAN_TTL]
            for path in files:
                if path not in valid:
                    path.unlink(missing_ok=True)
            sizes = {path: path.stat().st_size for path in valid}
            total = sum(sizes.values())
            while valid and (len(valid) >= MAX_PLANS or total + len(encoded) > MAX_STORE_BYTES):
                oldest = min(valid, key=lambda path: path.stat().st_mtime_ns)
                valid.remove(oldest)
                total -= sizes.pop(oldest)
                oldest.unlink(missing_ok=True)
            if total + len(encoded) > MAX_STORE_BYTES:
                raise ChangePlanError("change plan exceeds the bounded plan store")
            target = self.root / f"{ident}.json"
            fd, temporary = tempfile.mkstemp(prefix=f".{ident}.", suffix=".tmp", dir=self.root)
            temporary_path = Path(temporary)
            try:
                with os.fdopen(fd, "wb") as stream:
                    stream.write(encoded)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(temporary_path, target)
            finally:
                temporary_path.unlink(missing_ok=True)
        return ident

    def load(self, ident: str) -> dict[str, Any]:
        target = self._target(ident)
        with self._lock():
            try:
                stat_result = target.lstat()
                if not stat.S_ISREG(stat_result.st_mode) or target.is_symlink():
                    raise ChangePlanError("invalid change plan file")
                if time.time() - stat_result.st_mtime > PLAN_TTL:
                    target.unlink(missing_ok=True)
                    raise ChangePlanError("change plan has expired")
                if stat_result.st_size > MAX_STORE_BYTES:
                    raise ChangePlanError("invalid change plan size")
                payload = _decode(json.loads(_read_plan(target)))
            except FileNotFoundError as exc:
                raise ChangePlanError("change plan has expired or does not exist") from exc
            if not isinstance(payload, dict):
                raise ChangePlanError("invalid change plan")
            if payload.get("workspace_id") != _workspace_identity(self.workspace):
                raise ChangePlanError("change plan belongs to another workspace")
            operations = payload.get("operations", payload.get("files"))
            if operations is not None and (
                not isinstance(operations, list)
                or len(operations) > self.max_files
                or _payload_bytes(payload) > self.max_input_output_bytes
            ):
                raise ChangePlanError("change plan exceeds its bounded payload")
            return payload

    def remove(self, ident: str) -> None:
        with self._lock():
            self._target(ident).unlink(missing_ok=True)

    def _target(self, ident: str) -> Path:
        if not isinstance(ident, str) or len(ident) != 32 or any(
            char not in "0123456789abcdef" for char in ident
        ):
            raise ChangePlanError("invalid change plan ID")
        return self.root / f"{ident}.json"

    def _files(self) -> list[Path]:
        return sorted(
            (
                path
                for path in self.root.glob("[0-9a-f]" * 32 + ".json")
                if path.is_file() and not path.is_symlink()
            ),
            key=lambda path: path.stat().st_mtime_ns,
        )

    def _lock(self):
        self.root.mkdir(parents=True, exist_ok=True)
        return StorageLock(self.lock_path)


def _read_plan(path: Path) -> str:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_STORE_BYTES:
            raise ChangePlanError("invalid change plan file")
        chunks = []
        remaining = MAX_STORE_BYTES + 1
        while remaining:
            chunk = os.read(fd, remaining)
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        data = b"".join(chunks)
    finally:
        os.close(fd)
    if len(data) > MAX_STORE_BYTES:
        raise ChangePlanError("invalid change plan size")
    try:
        return data.decode("ascii")
    except UnicodeDecodeError as exc:
        raise ChangePlanError("change plan is not valid ASCII JSON") from exc


def _workspace_identity(workspace: Path) -> str:
    info = workspace.stat()
    if not stat.S_ISDIR(info.st_mode):
        raise NotADirectoryError(workspace)
    return f"{info.st_dev:x}:{info.st_ino:x}"


def _encode(value: Any) -> Any:
    if isinstance(value, Path):
        return {"__mypr_type__": "path", "value": str(value)}
    if isinstance(value, bytes):
        return {"__mypr_type__": "bytes", "value": base64.b64encode(value).decode("ascii")}
    if isinstance(value, Mapping):
        return {str(key): _encode(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_encode(item) for item in value]
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    raise TypeError(f"plan value is not serializable: {type(value).__name__}")


def _decode(value: Any) -> Any:
    if isinstance(value, list):
        return [_decode(item) for item in value]
    if isinstance(value, dict):
        marker = value.get("__mypr_type__")
        if marker == "path" and isinstance(value.get("value"), str):
            return Path(value["value"])
        if marker == "bytes" and isinstance(value.get("value"), str):
            try:
                return base64.b64decode(value["value"], validate=True)
            except ValueError as exc:
                raise ChangePlanError("invalid binary data in change plan") from exc
        return {key: _decode(item) for key, item in value.items()}
    return value


def _payload_bytes(value: Any) -> int:
    if isinstance(value, bytes):
        return len(value)
    if isinstance(value, Mapping):
        if value.get("__mypr_type__") == "bytes" and isinstance(value.get("value"), str):
            return len(base64.b64decode(value["value"]))
        return sum(_payload_bytes(item) for item in value.values())
    if isinstance(value, (list, tuple)):
        return sum(_payload_bytes(item) for item in value)
    return 0


__all__ = ["ChangePlanError", "ChangePlanStore"]
