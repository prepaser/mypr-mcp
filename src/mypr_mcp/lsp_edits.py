"""Validation and durable bounded plans for language-server workspace edits."""

from __future__ import annotations

import asyncio
import difflib
import hashlib
import math
import re
import stat
import threading
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .async_utils import wait_owned
from .change_plans import PLAN_TTL, ChangePlanError, ChangePlanStore


class EditError(ValueError):
    """A language-server edit cannot be safely represented or applied."""


_HASH = re.compile(r"[0-9a-f]{64}\Z")
_MAX_PRECONDITION_TARGETS = 101
_MISSING = object()


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


def _plan_path(root: Path, value: Any) -> Path:
    if not isinstance(value, Path) or not value.is_absolute():
        raise EditError("LSP plan metadata path must be absolute")
    workspace = root.resolve()
    try:
        resolved = value.resolve(strict=False)
    except (OSError, RuntimeError) as exc:
        raise EditError("LSP plan metadata path cannot be resolved") from exc
    if value != resolved:
        raise EditError("LSP plan metadata path must be canonical")
    try:
        resolved.relative_to(workspace)
    except ValueError as exc:
        raise EditError("LSP plan metadata path is outside the workspace") from exc
    current = value
    while current != workspace:
        if current.is_symlink():
            raise EditError("LSP plan metadata path cannot use symlinks")
        current = current.parent
    if value.exists() and (value.is_symlink() or not value.is_file()):
        raise EditError("LSP plan metadata path must be a regular file")
    return resolved


def _revision(value: Any, *, nullable: bool) -> str | None:
    if nullable and value is None:
        return None
    if not isinstance(value, str) or _HASH.fullmatch(value) is None:
        raise EditError("LSP plan metadata has an invalid SHA-256 revision")
    return value


def _normalize_preconditions(
    root: Path, values: Mapping[Path, str | None] | None,
) -> dict[Path, str | None]:
    if values is None:
        return {}
    if not isinstance(values, Mapping):
        raise EditError("LSP plan preconditions must be a mapping")
    normalized: dict[Path, str | None] = {}
    for raw_path, expected in values.items():
        path = _plan_path(root, raw_path)
        if path in normalized:
            raise EditError("duplicate LSP plan precondition")
        normalized[path] = _revision(expected, nullable=True)
    return normalized


def _normalize_documents(
    root: Path, values: Mapping[Path, tuple[int, str]] | None,
) -> dict[Path, tuple[int, str]]:
    if values is None:
        return {}
    if not isinstance(values, Mapping):
        raise EditError("LSP plan documents must be a mapping")
    normalized: dict[Path, tuple[int, str]] = {}
    for raw_path, raw_document in values.items():
        path = _plan_path(root, raw_path)
        if path in normalized:
            raise EditError("duplicate LSP plan document")
        if not isinstance(raw_document, tuple) or len(raw_document) != 2:
            raise EditError("LSP plan document metadata is invalid")
        version, digest = raw_document
        if type(version) is not int or version < 0:
            raise EditError("LSP plan document version is invalid")
        normalized[path] = (version, _revision(digest, nullable=False))
    return normalized


def _parse_preconditions(root: Path, raw: Any) -> dict[Path, str | None]:
    if raw is _MISSING:
        return {}
    if not isinstance(raw, list):
        raise EditError("LSP plan preconditions must be a list")
    if len(raw) > _MAX_PRECONDITION_TARGETS:
        raise EditError("LSP edit plan has too many precondition targets")
    values: dict[Path, str | None] = {}
    for item in raw:
        if not isinstance(item, dict) or "path" not in item or "revision" not in item:
            raise EditError("invalid persisted LSP plan precondition")
        path = _plan_path(root, item["path"])
        if path in values:
            raise EditError("duplicate LSP plan precondition")
        values[path] = _revision(item["revision"], nullable=True)
    return values


def _parse_documents(root: Path, raw: Any) -> dict[Path, tuple[int, str]]:
    if raw is _MISSING:
        return {}
    if not isinstance(raw, list):
        raise EditError("LSP plan documents must be a list")
    if len(raw) > _MAX_PRECONDITION_TARGETS:
        raise EditError("LSP edit plan has too many precondition targets")
    values: dict[Path, tuple[int, str]] = {}
    for item in raw:
        if (
            not isinstance(item, dict)
            or "path" not in item
            or "version" not in item
            or "digest" not in item
        ):
            raise EditError("invalid persisted LSP plan document")
        path = _plan_path(root, item["path"])
        if path in values:
            raise EditError("duplicate LSP plan document")
        version = item["version"]
        if type(version) is not int or version < 0:
            raise EditError("LSP plan document version is invalid")
        values[path] = (version, _revision(item["digest"], nullable=False))
    return values


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
    preconditions: dict[Path, str | None] = field(default_factory=dict)
    documents: dict[Path, tuple[int, str]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.preconditions = _normalize_preconditions(self.root, self.preconditions)
        self.documents = _normalize_documents(self.root, self.documents)
        if len(set(self.preconditions) | set(self.documents)) > _MAX_PRECONDITION_TARGETS:
            raise EditError("LSP edit plan has too many precondition targets")

    def payload(self) -> dict[str, Any]:
        return {
            "generation": self.generation,
            "title": self.title,
            "unsupported_reason": self.unsupported_reason,
            "server": self.server,
            "preconditions": [
                {"path": path, "revision": revision}
                for path, revision in self.preconditions.items()
            ],
            "documents": [
                {"path": path, "version": version, "digest": digest}
                for path, (version, digest) in self.documents.items()
            ],
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
        preconditions = _parse_preconditions(root, payload.get("preconditions", _MISSING))
        documents = _parse_documents(root, payload.get("documents", _MISSING))
        operations = []
        for raw in raw_operations:
            if not isinstance(raw, dict):
                raise EditError("invalid persisted LSP edit operation")
            path = _plan_path(root, raw.get("path"))
            raw_source = raw.get("source")
            source = _plan_path(root, raw_source) if raw_source is not None else None
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
            ident=ident,
            root=root,
            operations=operations,
            generation=str(payload.get("generation", "")),
            title=str(payload.get("title", "LSP edit")),
            unsupported_reason=payload.get("unsupported_reason"),
            created=time.monotonic(),
            server=payload.get("server") if isinstance(payload.get("server"), str) else None,
            preconditions=preconditions,
            documents=documents,
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
        if type(max_plans) is not int or max_plans < 1:
            raise ValueError("max_plans must be a positive integer")
        if (
            isinstance(ttl, bool) or not isinstance(ttl, (int, float))
            or not math.isfinite(ttl) or ttl <= 0
        ):
            raise ValueError("ttl must be positive and finite")
        self.max_plans = max_plans
        self.ttl = float(ttl)
        self._plans: dict[str, EditPlan] = {}
        self._durable = ChangePlanStore(root, "lsp", max_files=100)
        self._cache_lock = threading.RLock()
        self._workspace_key = _workspace_key(self._durable.workspace)

    def create(
        self,
        root: Path,
        operations: list[PlannedOperation],
        generation: str,
        title: str,
        unsupported_reason: str | None = None,
        server: str | None = None,
        *,
        preconditions: dict[Path, str | None] | None = None,
        documents: dict[Path, tuple[int, str]] | None = None,
    ) -> EditPlan:
        with self._cache_lock:
            plan = EditPlan(
                "",
                root,
                operations,
                generation,
                title,
                unsupported_reason,
                time.monotonic(),
                server,
                preconditions=preconditions,
                documents=documents,
            )
            try:
                durable_id = self._durable.create(plan.payload())
            except ChangePlanError as exc:
                raise EditError(str(exc)) from exc
            plan.ident = durable_id
            self._cache(plan)
            self._sync_cache()
            return plan

    async def acreate(
        self,
        root: Path,
        operations: list[PlannedOperation],
        generation: str,
        title: str,
        unsupported_reason: str | None = None,
        server: str | None = None,
        *,
        preconditions: dict[Path, str | None] | None = None,
        documents: dict[Path, tuple[int, str]] | None = None,
    ) -> EditPlan:
        operation = asyncio.ensure_future(
            asyncio.to_thread(
                self.create,
                root,
                operations,
                generation,
                title,
                unsupported_reason,
                server,
                preconditions=preconditions,
                documents=documents,
            )
        )
        cancelled = False
        while True:
            try:
                plan = await asyncio.shield(operation)
                break
            except asyncio.CancelledError:
                if operation.cancelled():
                    raise
                cancelled = True
        if cancelled:
            await wait_owned(asyncio.to_thread(self.remove, plan.ident), propagate=False)
            raise asyncio.CancelledError
        return plan

    def get(self, ident: str) -> EditPlan:
        with self._cache_lock:
            if not isinstance(ident, str) or not ident or len(ident) > 128:
                raise EditError("invalid LSP edit plan id")
            self._sync_cache()
            try:
                payload = self._load_payload(ident)
                plan = EditPlan.from_payload(self._durable.workspace, ident, payload)
            except (ChangePlanError, EditError) as exc:
                self._plans.pop(ident, None)
                raise EditError(str(exc)) from exc
            age = _payload_age(payload)
            if age is not None:
                plan.created = time.monotonic() - age
            self._cache(plan)
            return plan

    async def aget(self, ident: str) -> EditPlan:
        return await wait_owned(asyncio.to_thread(self.get, ident))

    def remove(self, ident: str) -> EditPlan:
        with self._cache_lock:
            plan = self.get(ident)
            try:
                self._durable.remove(plan.ident)
            except ChangePlanError as exc:
                raise EditError(str(exc)) from exc
            self._plans.pop(plan.ident, None)
            return plan

    async def aremove(self, ident: str) -> EditPlan:
        return await wait_owned(asyncio.to_thread(self.remove, ident))

    def consume(self, plan: EditPlan) -> EditPlan:
        with self._cache_lock:
            if not isinstance(plan, EditPlan):
                raise TypeError("plan must be an EditPlan")
            try:
                self._durable.remove(plan.ident)
            except ChangePlanError as exc:
                raise EditError(str(exc)) from exc
            self._plans.pop(plan.ident, None)
            return plan

    async def aconsume(self, plan: EditPlan) -> EditPlan:
        return await wait_owned(asyncio.to_thread(self.consume, plan))

    def clear(self) -> None:
        with self._cache_lock:
            self._plans.clear()

    def _cache(self, plan: EditPlan) -> None:
        self._plans.pop(plan.ident, None)
        self._plans[plan.ident] = plan
        while len(self._plans) > self.max_plans:
            self._plans.pop(next(iter(self._plans)))

    def _load_payload(self, ident: str) -> dict[str, Any]:
        payload = self._durable.load(ident)
        age = _payload_age(payload)
        if age is not None and age > self.ttl:
            self._durable.remove(ident)
            raise ChangePlanError("LSP edit plan has expired")
        return payload

    def _sync_cache(self) -> None:
        workspace_key = _workspace_key(self._durable.workspace)
        if workspace_key != self._workspace_key:
            self._plans.clear()
            self._workspace_key = workspace_key
            return
        cutoff = min(self.ttl, PLAN_TTL)
        for ident in tuple(self._plans):
            try:
                target = self._durable._target(ident)
                info = target.lstat()
                valid = stat.S_ISREG(info.st_mode) and not target.is_symlink()
                valid = valid and time.time() - info.st_mtime <= cutoff
            except (ChangePlanError, OSError):
                valid = False
            if not valid:
                self._plans.pop(ident, None)


def _payload_age(payload: dict[str, Any]) -> float | None:
    created = payload.get("created_at")
    if isinstance(created, bool) or not isinstance(created, (int, float)):
        return None
    return max(0.0, time.time() - float(created))


def _workspace_key(path: Path) -> tuple[int, int] | None:
    try:
        info = path.stat()
    except OSError:
        return None
    return info.st_dev, info.st_ino


__all__ = [
    "EditError",
    "EditPlan",
    "EditPlanStore",
    "PlannedOperation",
    "apply_text_edits",
    "position_offset",
    "sha256",
]
