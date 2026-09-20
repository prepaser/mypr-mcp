"""Small, durable stores for bounded query results."""

from __future__ import annotations

import base64
import contextlib
import json
import os
import secrets
import tempfile
import time
from pathlib import Path
from typing import Any


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
            payload = json.loads((self.root / f"{ident}.json").read_text(encoding="utf-8"))
        except FileNotFoundError as exc:
            raise ValueError("snapshot has expired") from exc
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
        if not isinstance(cursor, str) or not cursor:
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
                json.dump(payload, file, ensure_ascii=True, separators=(",", ":"))
                file.flush()
                os.fsync(file.fileno())
            os.replace(temporary, target)
        finally:
            with contextlib.suppress(FileNotFoundError):
                os.unlink(temporary)
