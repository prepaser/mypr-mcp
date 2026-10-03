"""Bounded, client-owned pages of fetched web results."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import secrets
import time
from collections import OrderedDict
from copy import deepcopy
from typing import Any

from .diagnostics import RPCError


def page_limit(value: Any) -> int:
    if type(value) is not int or not 4096 <= value <= 1024 * 1024:
        raise ValueError("max_bytes must be an integer between 4096 and 1048576")
    return value


def _size(value: Any) -> int:
    return len(json.dumps(value, ensure_ascii=True, separators=(",", ":")).encode())


class WebSnapshots:
    def __init__(self, *, ttl: float = 300, max_count: int = 32, max_bytes: int = 16 * 1024**2):
        self.ttl = ttl
        self.max_count = max_count
        self.max_bytes = max_bytes
        self._key = secrets.token_bytes(32)
        self._snapshots: OrderedDict[str, dict[str, Any]] = OrderedDict()
        self._bytes = 0

    def create(self, owner: str, result: dict[str, Any], max_bytes: int) -> dict[str, Any]:
        page_limit(max_bytes)
        size = _size(result)
        if size > self.max_bytes:
            raise RPCError("Web result exceeds the snapshot limit", code="result_too_large")
        self._expire()
        ident = secrets.token_urlsafe(18)
        snapshot = {
            "owner": owner,
            "result": deepcopy(result),
            "expires": time.monotonic() + self.ttl,
            "size": size,
        }
        self._snapshots[ident] = snapshot
        self._bytes += size
        while len(self._snapshots) > self.max_count or self._bytes > self.max_bytes:
            self._drop(next(iter(self._snapshots)))
        try:
            return self._render(ident, snapshot, 0, 0, max_bytes)
        except ValueError as exc:
            raise RPCError(
                str(exc),
                code="output_limit",
                operation="web",
                details={"page_cursor": self._cursor(ident, 0, 0)},
            ) from None
        except BaseException:
            self._drop(ident)
            raise

    def page(self, owner: str, cursor: str, max_bytes: int) -> dict[str, Any]:
        page_limit(max_bytes)
        self._expire()
        try:
            if not isinstance(cursor, str) or len(cursor) > 512:
                raise ValueError
            encoded, signature = cursor.split(".")
            expected = self._sign(encoded)
            if not hmac.compare_digest(signature, expected):
                raise ValueError
            payload = json.loads(base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4)))
            ident, index, offset, field_index = payload
            if any(type(value) is not int or value < 0 for value in (index, offset, field_index)):
                raise ValueError
            snapshot = self._snapshots[ident]
            if snapshot["owner"] != owner:
                raise ValueError
        except (ValueError, TypeError, KeyError, UnicodeError) as exc:
            raise RPCError("Web cursor is invalid or expired", code="invalid_cursor") from exc
        self._snapshots.move_to_end(ident)
        return self._render(ident, snapshot, index, offset, max_bytes, field_index)

    def clear(self, owner: str | None = None) -> None:
        for ident, snapshot in tuple(self._snapshots.items()):
            if owner is None or snapshot["owner"] == owner:
                self._drop(ident)

    def _drop(self, ident: str) -> None:
        snapshot = self._snapshots.pop(ident, None)
        if snapshot is not None:
            self._bytes -= snapshot["size"]

    def _expire(self) -> None:
        now = time.monotonic()
        for ident, snapshot in tuple(self._snapshots.items()):
            if snapshot["expires"] <= now:
                self._drop(ident)

    def _sign(self, encoded: str) -> str:
        digest = hmac.digest(self._key, encoded.encode(), hashlib.sha256)
        return base64.urlsafe_b64encode(digest).decode().rstrip("=")

    def _cursor(self, ident: str, index: int, offset: int, field_index: int = 0) -> str:
        raw = json.dumps([ident, index, offset, field_index], separators=(",", ":")).encode()
        encoded = base64.urlsafe_b64encode(raw).decode().rstrip("=")
        return f"{encoded}.{self._sign(encoded)}"

    def _render(self, ident, snapshot, index, offset, max_bytes, field_index=0):
        result = snapshot["result"]
        rows = [("results", i, row) for i, row in enumerate(result["results"])]
        rows += [
            ("failed_results", i, row) for i, row in enumerate(result.get("failed_results", []))
        ]
        if index > len(rows) or (index == len(rows) and (offset or field_index)):
            raise RPCError("Web cursor is invalid or expired", code="invalid_cursor")
        page = {
            key: deepcopy(value)
            for key, value in result.items()
            if key not in {"results", "failed_results"}
        }
        page.update(
            results=[],
            snapshot_id=ident,
            result_count=len(result["results"]),
            failed_count=len(result.get("failed_results", [])),
            page_cursor=self._cursor(ident, index, offset, field_index),
            next_cursor=None,
            has_more=False,
            truncated=False,
        )
        if "failed_results" in result:
            page["failed_results"] = []

        def advance(next_index, next_offset=0, next_field=0):
            page["has_more"] = next_index < len(rows)
            page["next_cursor"] = (
                self._cursor(ident, next_index, next_offset, next_field)
                if page["has_more"]
                else None
            )

        def part(count, row, field, text, index, field_count):
            row[field] = text[offset : offset + count]
            row["text_has_more"] = offset + count < len(text)
            if row["text_has_more"]:
                advance(index, offset + count, field_index)
            elif field_index + 1 < field_count:
                advance(index, 0, field_index + 1)
            else:
                advance(index + 1)

        while index < len(rows):
            section, row_index, original = rows[index]
            fields = [
                key for key in ("snippet", "content", "error") if isinstance(original.get(key), str)
            ]
            if (fields and field_index >= len(fields)) or (not fields and (offset or field_index)):
                raise RPCError("Web cursor is invalid or expired", code="invalid_cursor")
            if (
                not offset
                and not field_index
                and sum(len(original[key]) for key in fields) <= max_bytes
            ):
                page[section].append({**original, "result_index": row_index})
                advance(index + 1)
                if _size(page) <= max_bytes:
                    index += 1
                    continue
                page[section].pop()
            if any(page[key] for key in ("results", "failed_results") if key in page):
                break
            if not fields:
                raise ValueError("max_bytes is too small for web result metadata; increase it")
            field = fields[field_index]
            text = original[field]
            if offset > len(text):
                raise RPCError("Web cursor is invalid or expired", code="invalid_cursor")
            row = {key: value for key, value in original.items() if key not in fields}
            row.update(
                result_index=row_index, text_field=field, text_offset=offset, text_has_more=True
            )
            page[section].append(row)

            low, high = 0, min(len(text) - offset, max_bytes)
            part(0, row, field, text, index, len(fields))
            if _size(page) > max_bytes:
                raise ValueError("max_bytes is too small for web result metadata; increase it")
            while low < high:
                count = (low + high + 1) // 2
                part(count, row, field, text, index, len(fields))
                if _size(page) <= max_bytes:
                    low = count
                else:
                    high = count - 1
            if low == 0 and offset < len(text):
                raise ValueError("max_bytes is too small for web result metadata; increase it")
            part(low, row, field, text, index, len(fields))
            return deepcopy(page)
        advance(index, offset, field_index)
        if _size(page) > max_bytes:
            raise ValueError("max_bytes is too small for web result metadata; increase it")
        return deepcopy(page)
