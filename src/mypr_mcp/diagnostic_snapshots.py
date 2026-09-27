"""Short-lived snapshots for paged local diagnostics."""

from __future__ import annotations

import json
import secrets
import time
from collections import OrderedDict
from copy import deepcopy
from typing import Any

_MAX_PAGE_BYTES = 32768


class DiagnosticSnapshots:
    def __init__(self, *, max_snapshots: int = 8, ttl: float = 60.0) -> None:
        self.max_snapshots = max_snapshots
        self.ttl = ttl
        self._snapshots: OrderedDict[str, dict[str, Any]] = OrderedDict()
        self._cursors: dict[str, tuple[str, int]] = {}
        self._cursor_by_offset: dict[tuple[str, int], str] = {}

    def page(
        self,
        rows: list[dict[str, Any]] | None = None,
        *,
        cursor: str | None = None,
        limit: int = 20,
        total: int | None = None,
        truncated: bool = False,
        warnings: list[dict[str, str]] | None = None,
    ) -> dict[str, Any]:
        self._expire()
        if cursor is None:
            if rows is None:
                raise ValueError("rows are required to create a diagnostic snapshot")
            snapshot_id = secrets.token_urlsafe(18)
            self._snapshots[snapshot_id] = {
                "rows": rows,
                "total": len(rows) if total is None else total,
                "truncated": truncated,
                "warnings": warnings or [],
                "expires": time.monotonic() + self.ttl,
            }
            while len(self._snapshots) > self.max_snapshots:
                expired_id, _ = self._snapshots.popitem(last=False)
                self._drop_cursors(expired_id)
            offset = 0
        else:
            entry = self._cursors.get(cursor)
            if entry is None:
                raise ValueError("diagnostic cursor is invalid or expired")
            snapshot_id, offset = entry
            snapshot = self._snapshots.get(snapshot_id)
            if snapshot is None:
                self._cursors.pop(cursor, None)
                raise ValueError("diagnostic cursor is invalid or expired")
            self._snapshots.move_to_end(snapshot_id)
        snapshot = self._snapshots[snapshot_id]
        result = self._render(snapshot_id, snapshot, offset, limit)
        if cursor is not None:
            # Keep a cursor reusable until its snapshot expires; retries remain stable.
            self._cursors[cursor] = (snapshot_id, offset)
        return deepcopy(result)

    def _render(
        self, snapshot_id: str, snapshot: dict[str, Any], offset: int, limit: int
    ) -> dict[str, Any]:
        rows = snapshot["rows"]
        end = min(offset + limit, len(rows))
        page_rows = rows[offset:end]
        result: dict[str, Any] = {
            "sockets": page_rows,
            "total": snapshot["total"],
            "returned": len(page_rows),
            "has_more": end < len(rows),
            "truncated": snapshot["truncated"],
            "warnings": snapshot["warnings"][:8],
            "snapshot_id": snapshot_id,
            "omitted_by_source": max(0, snapshot["total"] - len(rows)),
        }
        self._set_cursor(result, snapshot_id, end, len(rows))
        while len(json.dumps(result, ensure_ascii=False).encode()) > _MAX_PAGE_BYTES:
            if len(page_rows) > 1:
                page_rows.pop()
                end -= 1
            elif page_rows:
                page_rows.clear()
                end = min(offset + 1, len(rows))
                result["warnings"] = result["warnings"][:7] + [
                    {
                        "code": "record_output_limit",
                        "message": "One socket record was omitted because it exceeds 32 KiB",
                    }
                ]
                result["truncated"] = True
                result["omitted_by_output_limit"] = 1
            else:
                raise RuntimeError("diagnostic page metadata exceeds the 32 KiB output limit")
            result["returned"] = len(page_rows)
            result["has_more"] = end < len(rows)
            result["truncated"] = True
            self._set_cursor(result, snapshot_id, end, len(rows))
            if not page_rows:
                break
        if len(json.dumps(result, ensure_ascii=False).encode()) > _MAX_PAGE_BYTES:
            raise RuntimeError("diagnostic page metadata exceeds the 32 KiB output limit")
        return result

    def _set_cursor(self, result: dict[str, Any], snapshot_id: str, offset: int, count: int):
        next_cursor = None
        if offset < count:
            offset_key = (snapshot_id, offset)
            next_cursor = self._cursor_by_offset.get(offset_key)
            if next_cursor is None:
                next_cursor = secrets.token_urlsafe(24)
                self._cursor_by_offset[offset_key] = next_cursor
                self._cursors[next_cursor] = offset_key
        result["cursor"] = next_cursor
        result["next_cursor"] = next_cursor

    def _expire(self) -> None:
        now = time.monotonic()
        for snapshot_id, snapshot in list(self._snapshots.items()):
            if snapshot["expires"] <= now:
                self._snapshots.pop(snapshot_id, None)
                self._drop_cursors(snapshot_id)

    def _drop_cursors(self, snapshot_id: str) -> None:
        for cursor, (owner, _offset) in list(self._cursors.items()):
            if owner == snapshot_id:
                self._cursors.pop(cursor, None)
                self._cursor_by_offset.pop((owner, _offset), None)


__all__ = ["DiagnosticSnapshots"]
