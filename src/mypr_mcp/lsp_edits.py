"""Validation and durable bounded plans for language-server workspace edits."""

from __future__ import annotations

import difflib
import hashlib
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .change_plans import ChangePlanError, ChangePlanStore


class EditError(ValueError):
    """A language-server edit cannot be safely represented or applied."""


def sha256(data: bytes | None) -> str | None:
    return hashlib.sha256(data).hexdigest() if data is not None else None


def _line_starts(text: str) -> tuple[list[int], list[str]]:
    starts = [0]
    lines: list[str] = []
    index = 0
    while index < len(text):
        begin = index
        while index < len(text) and text[index] not in "\r\n":
            index += 1
        lines.append(text[begin:index])
        if index >= len(text):
            break
        if text.startswith("\r\n", index):
            index += 2
        else:
            index += 1
        starts.append(index)
    if not lines or starts[-1] == len(text):
        lines.append("")
    return starts, lines


def position_offset(text: str, position: Any, encoding: str) -> int:
    if not isinstance(position, dict) or type(position.get("line")) is not int:
        raise EditError("LSP edit has an invalid position")
    line = position["line"]
    character = position.get("character")
    if line < 0 or type(character) is not int or character < 0:
        raise EditError("LSP edit has an invalid position")
    starts, lines = _line_starts(text)
    if line >= len(lines):
        raise EditError("LSP edit position is outside the document")
    value = lines[line]
    if encoding == "utf-32":
        if character > len(value):
            raise EditError("LSP edit position is outside the line")
        return starts[line] + character
    if encoding == "utf-8":
        units = 0
        offset = 0
        for char in value:
            if units == character:
                return starts[line] + offset
            width = len(char.encode("utf-8"))
            if units + width > character:
                raise EditError("LSP edit splits a UTF-8 code point")
            units += width
            offset += 1
        if units != character:
            raise EditError("LSP edit position is outside the line")
        return starts[line] + offset
    units = 0
    offset = 0
    for char in value:
        if units == character:
            return starts[line] + offset
        width = len(char.encode("utf-16-le")) // 2
        if units + width > character:
            raise EditError("LSP edit splits a UTF-16 code point")
        units += width
        offset += 1
    if units != character:
        raise EditError("LSP edit position is outside the line")
    return starts[line] + offset


def apply_text_edits(text: str, edits: Any, encoding: str) -> str:
    if not isinstance(edits, list):
        raise EditError("LSP text edits must be a list")
    converted: list[tuple[int, int, str]] = []
    for item in edits:
        if not isinstance(item, dict) or not isinstance(item.get("range"), dict):
            raise EditError("LSP text edit is malformed")
        raw_range = item["range"]
        start = position_offset(text, raw_range.get("start"), encoding)
        end = position_offset(text, raw_range.get("end"), encoding)
        if end < start:
            raise EditError("LSP text edit range is reversed")
        replacement = item.get("newText")
        if not isinstance(replacement, str) or "\x00" in replacement:
            raise EditError("LSP text edit replacement is invalid")
        converted.append((start, end, replacement))
    converted.sort(key=lambda value: (value[0], value[1]), reverse=True)
    previous_start = len(text) + 1
    for start, end, _ in converted:
        if end > previous_start:
            raise EditError("overlapping LSP text edits are unsupported")
        previous_start = start
    for start, end, replacement in converted:
        text = text[:start] + replacement + text[end:]
    return text


def _safe_path(root: Path, value: Any) -> Path:
    if not isinstance(value, str) or not value:
        raise EditError("LSP workspace edit URI is invalid")
    parsed = value
    if not parsed.startswith("file://"):
        raise EditError("LSP workspace edits must use file URIs")
    from urllib.parse import unquote, urlparse

    uri = urlparse(parsed)
    if uri.netloc not in ("", "localhost"):
        raise EditError("LSP workspace edit URI is outside the workspace")
    path = Path(unquote(uri.path))
    resolved = path.resolve(strict=False)
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise EditError("LSP workspace edit path is outside the workspace") from exc
    current = path
    while current != root:
        if current.is_symlink():
            raise EditError("LSP workspace edits cannot target symlink paths")
        current = current.parent
    return resolved


@dataclass(slots=True)
class PlannedOperation:
    operation: str
    path: Path
    old: bytes | None
    new: bytes | None
    expected: str | None
    source: Path | None = None
    source_old: bytes | None = None
    destination_expected: str | None = None
    version: int | None = None

    def public(self, root: Path) -> dict[str, Any]:
        def display(path: Path) -> str:
            try:
                return str(path.relative_to(root))
            except ValueError:
                return str(path)

        old = self.source_old if self.operation == "rename" else self.old
        return {
            "operation": self.operation,
            "path": display(self.path),
            "source": display(self.source) if self.source is not None else None,
            "old_revision": sha256(old),
            "revision": sha256(self.new),
            "size": len(self.new or b""),
        }


@dataclass(slots=True)
class EditPlan:
    ident: str
    root: Path
    operations: list[PlannedOperation]
    generation: str
    title: str
    unsupported_reason: str | None = None
    created: float = 0.0
    server: str | None = None

    def payload(self) -> dict[str, Any]:
        return {
            "generation": self.generation,
            "title": self.title,
            "unsupported_reason": self.unsupported_reason,
            "server": self.server,
            "operations": [
                {
                    "operation": operation.operation,
                    "path": operation.path,
                    "old": operation.old,
                    "new": operation.new,
                    "expected": operation.expected,
                    "source": operation.source,
                    "source_old": operation.source_old,
                    "destination_expected": operation.destination_expected,
                    "version": operation.version,
                }
                for operation in self.operations
            ],
        }

    @classmethod
    def from_payload(cls, root: Path, ident: str, payload: dict[str, Any]) -> EditPlan:
        raw_operations = payload.get("operations")
        if not isinstance(raw_operations, list):
            raise EditError("invalid persisted LSP edit plan")
        operations = []
        for raw in raw_operations:
            if not isinstance(raw, dict):
                raise EditError("invalid persisted LSP edit operation")
            path = raw.get("path")
            source = raw.get("source")
            if not isinstance(path, Path) or (source is not None and not isinstance(source, Path)):
                raise EditError("invalid persisted LSP edit path")
            for candidate in (path, source):
                if candidate is None:
                    continue
                try:
                    candidate.resolve(strict=False).relative_to(root)
                except ValueError as exc:
                    raise EditError("persisted LSP edit path is outside the workspace") from exc
                current = candidate
                while current != root:
                    if current.is_symlink():
                        raise EditError("persisted LSP edit path uses a symlink")
                    current = current.parent
            operation = raw.get("operation")
            if operation not in {"create", "update", "delete", "rename"}:
                raise EditError("invalid persisted LSP edit operation")
            operations.append(
                PlannedOperation(
                    operation,
                    path,
                    raw.get("old"),
                    raw.get("new"),
                    raw.get("expected"),
                    source=source,
                    source_old=raw.get("source_old"),
                    destination_expected=raw.get("destination_expected"),
                    version=raw.get("version"),
                )
            )
        return cls(
            ident,
            root,
            operations,
            str(payload.get("generation", "")),
            str(payload.get("title", "LSP edit")),
            payload.get("unsupported_reason"),
            time.monotonic(),
            payload.get("server") if isinstance(payload.get("server"), str) else None,
        )

    def result(self, *, diff_limit: int = 32_768) -> dict[str, Any]:
        changes = [operation.public(self.root) for operation in self.operations]
        chunks: list[str] = []
        total = 0
        truncated = False
        for operation in self.operations:
            if operation.operation not in {"create", "update", "delete"}:
                continue
            old = (operation.old or b"").decode("utf-8", "replace").splitlines(keepends=True)
            new = (operation.new or b"").decode("utf-8", "replace").splitlines(keepends=True)
            name = str(operation.path.relative_to(self.root))
            for line in difflib.unified_diff(old, new, fromfile=name, tofile=name):
                raw = line if line.endswith("\n") else line + "\n"
                size = len(raw.encode("utf-8"))
                if total + size > diff_limit:
                    truncated = True
                    break
                chunks.append(raw)
                total += size
            if truncated:
                break
        result = {
            "plan_id": self.ident,
            "title": self.title,
            "changes": changes,
            "diff": "".join(chunks),
            "diff_truncated": truncated,
            "applicable": self.unsupported_reason is None,
            "unsupported_reason": self.unsupported_reason,
            "generation": self.generation,
        }
        return result


class EditPlanStore:
    def __init__(self, root: Path, *, max_plans: int = 16, ttl: float = 3600.0) -> None:
        self.max_plans = max_plans
        self.ttl = ttl
        self._plans: dict[str, EditPlan] = {}
        self._durable = ChangePlanStore(root, "lsp", max_files=100)

    def create(
        self,
        root: Path,
        operations: list[PlannedOperation],
        generation: str,
        title: str,
        unsupported_reason: str | None = None,
        server: str | None = None,
    ) -> EditPlan:
        plan = EditPlan(
            "",
            root,
            operations,
            generation,
            title,
            unsupported_reason,
            time.monotonic(),
            server,
        )
        try:
            durable_id = self._durable.create(plan.payload())
        except ChangePlanError as exc:
            raise EditError(str(exc)) from exc
        plan.ident = durable_id
        self._plans[durable_id] = plan
        while len(self._plans) > self.max_plans:
            self._plans.pop(next(iter(self._plans)))
        return plan

    def get(self, ident: str) -> EditPlan:
        if not isinstance(ident, str) or not ident or len(ident) > 128:
            raise EditError("invalid LSP edit plan id")
        try:
            plan = self._plans[ident]
        except KeyError:
            try:
                payload = self._durable.load(ident)
                plan = EditPlan.from_payload(self._durable.workspace, ident, payload)
            except (ChangePlanError, EditError) as exc:
                raise EditError(str(exc)) from exc
            self._plans[ident] = plan
        if time.monotonic() - plan.created > self.ttl:
            self._plans.pop(ident, None)
            raise EditError("LSP edit plan has expired")
        return plan

    def remove(self, ident: str) -> EditPlan:
        plan = self.get(ident)
        self._plans.pop(plan.ident, None)
        try:
            self._durable.remove(plan.ident)
        except ChangePlanError as exc:
            raise EditError(str(exc)) from exc
        return plan

    def clear(self) -> None:
        self._plans.clear()


__all__ = [
    "EditError",
    "EditPlan",
    "EditPlanStore",
    "PlannedOperation",
    "apply_text_edits",
    "position_offset",
    "sha256",
]
