"""Small, durable stores for bounded query results."""

from __future__ import annotations

import base64
import contextlib
import json
import os
import secrets
import stat
import tempfile
import time
from pathlib import Path
from typing import Any

_MAX_SNAPSHOT_BYTES = 64 * 1024 * 1024
_MAX_CURSOR_BYTES = 1024


class SnapshotStore:
    """Persist JSON query results and issue query-bound opaque cursors."""

    def __init__(self, root: Path, *, name: str) -> None:
        self.root = Path(root).resolve() / name
        self.root.mkdir(parents=True, exist_ok=True)

    def create(self, query: dict[str, Any], items: list[Any], **extra: Any) -> str:
        ident = secrets.token_hex(16)
        payload = {
            "id": ident,
            "query": query,
            "items": items,
            "created": time.time(),
            **extra,
        }
        self._write(ident, payload)
        return ident

    def load(self, ident: str) -> dict[str, Any]:
        if (
            not isinstance(ident, str)
            or len(ident) != 32
            or not all(char in "0123456789abcdef" for char in ident)
        ):
            raise ValueError("invalid snapshot cursor")
        try:
            raw = _read_snapshot(self.root / f"{ident}.json")
        except FileNotFoundError as exc:
            raise ValueError("snapshot has expired") from exc
        try:
            payload = json.loads(raw)
        except (UnicodeError, ValueError) as exc:
            raise RuntimeError("invalid persisted snapshot JSON") from exc
        if not isinstance(payload, dict) or payload.get("id") != ident:
            raise RuntimeError("invalid persisted snapshot")
        if not isinstance(payload.get("items"), list):
            raise RuntimeError("invalid persisted snapshot items")
        return payload

    def cursor(self, ident: str, offset: int, kind: str | None = None) -> str:
        payload = {"id": ident, "offset": offset}
        if kind is not None:
            payload["kind"] = kind
        raw = json.dumps(payload, separators=(",", ":")).encode()
        return base64.urlsafe_b64encode(raw).decode().rstrip("=")

    def decode(
        self, cursor: str, *, expected_kind: str | None = None
    ) -> tuple[dict[str, Any], int]:
        if not isinstance(cursor, str) or not cursor or len(cursor) > _MAX_CURSOR_BYTES:
            raise ValueError("invalid snapshot cursor")
        try:
            padding = "=" * (-len(cursor) % 4)
            payload = json.loads(base64.urlsafe_b64decode(cursor + padding))
        except (ValueError, TypeError, json.JSONDecodeError) as exc:
            raise ValueError("invalid snapshot cursor") from exc
        if not isinstance(payload, dict):
            raise ValueError("invalid snapshot cursor")
        ident, offset = payload.get("id"), payload.get("offset")
        if not isinstance(ident, str) or type(offset) is not int or offset < 0:
            raise ValueError("invalid snapshot cursor")
        snapshot = self.load(ident)
        if expected_kind is not None and snapshot.get("kind") != expected_kind:
            raise ValueError("cursor belongs to a different query")
        if payload.get("kind") is not None and payload.get("kind") != snapshot.get("kind"):
            raise ValueError("invalid snapshot cursor")
        if offset > len(snapshot["items"]):
            raise ValueError("invalid snapshot cursor")
        return snapshot, offset

    def _write(self, ident: str, payload: dict[str, Any]) -> None:
        target = self.root / f"{ident}.json"
        fd, temporary = tempfile.mkstemp(prefix=f".{ident}.", suffix=".tmp", dir=self.root)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as file:
                encoder = json.JSONEncoder(ensure_ascii=True, separators=(",", ":"))
                size = 0
                for chunk in encoder.iterencode(payload):
                    size += len(chunk)
                    if size > _MAX_SNAPSHOT_BYTES:
                        raise ValueError("snapshot exceeds its size limit")
                    file.write(chunk)
                file.flush()
                os.fsync(file.fileno())
            os.replace(temporary, target)
        finally:
            with contextlib.suppress(FileNotFoundError):
                os.unlink(temporary)


def _read_snapshot(path: Path) -> bytes:
    flags = os.O_RDONLY | getattr(os, "O_NONBLOCK", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or before.st_size > _MAX_SNAPSHOT_BYTES:
            raise RuntimeError("invalid persisted snapshot file or size")
        with os.fdopen(descriptor, "rb") as file:
            descriptor = -1
            data = file.read(_MAX_SNAPSHOT_BYTES + 1)
            after = os.fstat(file.fileno())
        if len(data) > _MAX_SNAPSHOT_BYTES or (
            before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns
        ) != (
            after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns
        ):
            raise RuntimeError("persisted snapshot changed while reading")
        return data
    finally:
        if descriptor >= 0:
            os.close(descriptor)
