"""Content-addressed history for workspace modules and skills."""

from __future__ import annotations

import asyncio
import base64
import contextvars
import hashlib
import json
import os
import re
import stat
import tempfile
from collections.abc import Iterable
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from threading import Lock
from typing import Any
from weakref import WeakValueDictionary

from .async_utils import wait_owned
from .file_io import open_regular, read_bytes
from .json_utils import json_bytes, json_text
from .storage_lock import StorageLock

_HASH = re.compile(r"[0-9a-f]{64}\Z")
_TRANSACTION_LOCKS: WeakValueDictionary[Path, asyncio.Lock] = WeakValueDictionary()
_TRANSACTION_LOCKS_GUARD = Lock()
_MAX_INDEX_BYTES = 16 * 1024 * 1024
_MAX_BLOB_BYTES = 64 * 1024 * 1024
_MAX_HISTORY_RECORDS = 10_000
_V2_KINDS = {"modules", "skills", "files"}
_TEMP_PREFIX_NAME_LIMIT = 32
_ABSENT_REVISION = "absent"
_ACTIVE_STORAGE_LOCKS: contextvars.ContextVar[frozenset[tuple[str, int]]] = contextvars.ContextVar(
    "mypr_revision_storage_locks", default=frozenset()
)


def _temporary_prefix(name: str, suffix: str = ".") -> str:
    return f".{name[:_TEMP_PREFIX_NAME_LIMIT]}{suffix}"


class RevisionIndexOutcomeUnknown(RuntimeError):
    """Raised when an index write cannot be distinguished from an external update."""

    def __init__(self, message: str, *, resource: str | None = None) -> None:
        self.code = "outcome_unknown"
        self.details = {"outcome_unknown": True}
        if resource is not None:
            self.details["resource"] = resource
        super().__init__(message)


def _empty_index(kind: str, resource: str, version: int) -> dict[str, Any]:
    index: dict[str, Any] = {
        "version": version,
        "kind": kind,
        "resource": resource,
        "count": 0,
        "revisions": [],
    }
    if version == 2:
        index["next_sequence"] = 1
    return index


def _validate_index(
    index: Any, kind: str, resource: str, encoded_size: int
) -> dict[str, Any]:
    if encoded_size > _MAX_INDEX_BYTES:
        raise ValueError("revision index exceeds its size limit")
    if (
        not isinstance(index, dict)
        or type(index.get("version")) is not int
        or index.get("version") not in {1, 2}
        or index.get("kind") != kind
        or index.get("resource") != resource
        or not isinstance(index.get("revisions"), list)
        or type(index.get("count")) is not int
        or index.get("count") != len(index["revisions"])
    ):
        raise ValueError("revision index metadata is invalid")
    version = index["version"]
    next_sequence = index.get("next_sequence", 1)
    if version == 2 and (type(next_sequence) is not int or next_sequence < 1):
        raise ValueError("revision index next sequence is invalid")
    pruned_before = index.get("pruned_before", 0)
    if version == 2 and (type(pruned_before) is not int or pruned_before < 0):
        raise ValueError("revision index prune marker is invalid")
    previous = 0
    for item in index["revisions"]:
        is_absent = (
            isinstance(item, dict)
            and version == 2
            and kind == "files"
            and item.get("revision") == _ABSENT_REVISION
            and item.get("absent") is True
        )
        if (
            not isinstance(item, dict)
            or type(item.get("sequence")) is not int
            or item["sequence"] <= previous
            or version == 1
            and item["sequence"] != previous + 1
            or not isinstance(item.get("revision"), str)
            or not is_absent
            and not _HASH.fullmatch(item["revision"])
            or type(item.get("size")) is not int
            or not 0 <= item["size"] <= _MAX_BLOB_BYTES
            or is_absent
            and item["size"] != 0
            or not isinstance(item.get("created_at"), str)
            or len(item["created_at"]) > 64
        ):
            raise ValueError("revision index record is invalid")
        previous = item["sequence"]
    if version == 2 and next_sequence <= previous:
        raise ValueError("revision index next sequence is behind its records")
    records = index["revisions"]
    if version == 2 and pruned_before and records and pruned_before >= records[0]["sequence"]:
        raise ValueError("revision index prune marker is invalid")
    return index


async def _uncancelled(function, *args):
    return await wait_owned(asyncio.to_thread(function, *args))


@asynccontextmanager
async def _storage_transaction(workspace: Path):
    key = str(workspace)
    active = _ACTIVE_STORAGE_LOCKS.get()
    owner = id(asyncio.current_task())
    marker = (key, owner)
    if marker in active:
        yield
        return
    lock = StorageLock(workspace / ".mypr" / "storage.lock")
    try:
        await _uncancelled(lock.__enter__)
    except asyncio.CancelledError:
        await _uncancelled(lock.__exit__, None, None, None)
        raise
    token = _ACTIVE_STORAGE_LOCKS.set(active | {marker})
    try:
        yield
    finally:
        _ACTIVE_STORAGE_LOCKS.reset(token)
        await _uncancelled(lock.__exit__, None, None, None)


class RevisionStore:
    def __init__(self, workspace: str | os.PathLike[str], fs: Any, kind: str) -> None:
        if kind not in {"modules", "skills", "files"}:
            raise ValueError("revision kind must be modules, skills, or files")
        self.workspace = Path(workspace).expanduser().resolve()
        self.fs = fs
        self.kind = kind
        self._version = 2 if kind in _V2_KINDS else 1
        self._prepared_index_snapshot: dict[str, bytes | None] | None = None

    @asynccontextmanager
    async def transaction(self, resource_path: str):
        target = self._safe_path(resource_path)
        with _TRANSACTION_LOCKS_GUARD:
            lock = _TRANSACTION_LOCKS.setdefault(target, asyncio.Lock())
        async with _storage_transaction(self.workspace):
            async with lock:
                yield

    async def record(self, resource_path: str, contents: Iterable[str | bytes | None]) -> None:
        resource = _validate_resource(resource_path)
        unique = _content_map(contents, include_absent=self.kind == "files")
        if not unique:
            return
        await self.prepare_bytes(resource, unique.values())
        index_path = self._index_path(resource)
        index, index_hash = await self._load_index(resource, index_path)
        _upgrade_index(index)
        previous_count = index["count"]
        previous_next = index.get("next_sequence")
        _append_records(index, unique, version=self._version)
        _prune_index(index)
        if index["count"] == previous_count and index.get("next_sequence") == previous_next:
            return
        _check_index_size(index)
        encoded = json_text(index, separators=(",", ":"))
        planned_hash = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
        try:
            await self.fs.write(
                index_path,
                encoded,
                expected_hash=index_hash,
                create_parents=True,
                history=False,
            )
        except BaseException as write_error:
            try:
                current = await self.fs.read(index_path, max_bytes=_MAX_INDEX_BYTES)
            except FileNotFoundError:
                if index_hash is None:
                    raise
                raise RevisionIndexOutcomeUnknown(
                    f"revision index disappeared after a failed write: {index_path}"
                ) from write_error
            except BaseException as check_error:
                raise RevisionIndexOutcomeUnknown(
                    f"unable to verify revision index after a failed write: {check_error}"
                ) from write_error
            current_hash = current.get("revision")
            if not isinstance(current_hash, str):
                raise RevisionIndexOutcomeUnknown(
                    f"revision index read returned no content hash: {index_path}"
                ) from write_error
            if current_hash == planned_hash:
                return
            if current_hash != index_hash:
                raise RevisionIndexOutcomeUnknown(
                    f"revision index changed during a failed write: {index_path}"
                ) from write_error
            raise

    async def record_bytes(
        self, resource_path: str, contents: Iterable[str | bytes | None]
    ) -> None:
        """Record UTF-8 or binary content revisions for a resource."""
        await self.record(resource_path, contents)

    async def commit(
        self,
        resource_path: str,
        old: str | None,
        new: str,
        *,
        expected_hash: str | None,
    ) -> dict[str, Any]:
        """Write a resource and its history, compensating if history is rejected."""
        resource = _validate_resource(resource_path)
        old_revision = _hash_text(old) if old is not None else None
        new_revision = _hash_text(new)
        await self.ensure_capacity(resource, (old, new))
        await self.prepare(resource, (old, new))
        try:
            result = await self.fs.write(
                resource,
                new,
                expected_hash=expected_hash,
                create_parents=True,
                history=False,
            )
        except BaseException as exc:
            try:
                current = await self._target_revision(resource)
            except BaseException as check_error:
                raise self._write_failure(
                    resource, old_revision, new_revision, exc, check_error
                ) from exc
            if current == new_revision and current != old_revision:
                try:
                    await self._compensate(resource, old, new_revision)
                except BaseException as rollback_error:
                    raise self._write_failure(
                        resource, old_revision, new_revision, exc, rollback_error
                    ) from exc
            elif current not in {old_revision, None}:
                raise self._write_failure(
                    resource,
                    old_revision,
                    new_revision,
                    exc,
                    RuntimeError(f"target now has revision {current}"),
                ) from exc
            raise
        try:
            await self.record(resource, (old, new))
        except RevisionIndexOutcomeUnknown as exc:
            raise RevisionIndexOutcomeUnknown(
                f"Revision index outcome is unknown for {resource}; the target remains at "
                f"revision {new_revision}. Prior content is recoverable with revision "
                f"{old_revision or 'absent'}; inspect read_revision() and history() before "
                "retrying.",
                resource=resource,
            ) from exc
        except BaseException as exc:
            try:
                await self._compensate(resource, old, new_revision)
            except BaseException as rollback_error:
                raise self._write_failure(
                    resource, old_revision, new_revision, exc, rollback_error
                ) from exc
            raise RuntimeError(
                f"Revision index write failed for {resource}; the file change was rolled back. "
                f"The prior content remains available as revision {old_revision or 'absent'}."
            ) from exc
        return result

    async def ensure_capacity(
        self, resource_path: str, contents: Iterable[str | bytes | None]
    ) -> None:
        resource = _validate_resource(resource_path)
        index, _ = await self._load_index(resource, self._index_path(resource))
        _upgrade_index(index)
        unique = _content_map(contents, include_absent=self.kind == "files")
        _append_records(index, unique, version=self._version)
        _prune_index(index)
        _check_index_size(index)

    async def prepare(self, resource_path: str, contents: Iterable[str | bytes | None]) -> None:
        await self.prepare_bytes(resource_path, contents)

    async def prepare_bytes(
        self, resource_path: str, contents: Iterable[str | bytes | None]
    ) -> None:
        _validate_resource(resource_path)
        unique = _content_map(contents)
        for revision, data in unique.items():
            if data is not None:
                await self._store_blob(revision, data)

    def prepare_changes_sync(
        self, plans: Iterable[dict[str, Any]]
    ) -> dict[str, bytes | None]:
        """Preflight and store blobs for a multi-file filesystem transaction."""
        changes = list(self._plan_changes(plans))
        resources = {resource for resource, _, _ in changes}
        snapshots = {resource: self._read_index_bytes_sync(resource) for resource in resources}
        for resource, old, new in changes:
            for data in (old, new):
                if data is not None and len(data) > _MAX_BLOB_BYTES:
                    raise ValueError(
                        f"history for {resource} exceeds {_MAX_BLOB_BYTES} bytes; "
                        "retry with history=False"
                    )
                self._store_blob_sync(data)
            index = self._load_index_sync(resource)
            _upgrade_index(index)
            _append_records(
                index,
                _content_map((old, new), include_absent=self.kind == "files"),
                version=self._version,
            )
            _prune_index(index)
            _check_index_size(index)
        self._prepared_index_snapshot = snapshots
        return snapshots

    def record_changes_sync(self, plans: Iterable[dict[str, Any]]) -> None:
        changes = list(self._plan_changes(plans))
        snapshots = self._prepared_index_snapshot
        if snapshots is None:
            resources = {resource for resource, _, _ in changes}
            snapshots = {
                resource: self._read_index_bytes_sync(resource) for resource in resources
            }
        expected: dict[str, bytes] = {}
        try:
            for resource, old, new in changes:
                current = self._read_index_bytes_sync(resource)
                if current != snapshots.get(resource):
                    raise RevisionIndexOutcomeUnknown(
                        f"revision index changed before recording {resource}"
                    )
                self._record_one_sync(resource, old, new, expected=expected)
        except BaseException:
            if expected:
                try:
                    self._restore_index_changes_sync(snapshots, expected)
                except BaseException as rollback_error:
                    raise RevisionIndexOutcomeUnknown(
                        "revision index rollback is uncertain; inspect history before retrying"
                    ) from rollback_error
            raise
        finally:
            self._prepared_index_snapshot = None

    def _plan_changes(self, plans: Iterable[dict[str, Any]]):
        for plan in plans:
            operation = plan.get("operation")
            if operation == "move":
                source = plan.get("source")
                source_old = plan.get("source_old")
                if source is not None and source_old is not None:
                    resource = self._resource_for_path(source)
                    if resource is not None:
                        yield resource, source_old, None
                target = self._resource_for_path(plan["path"])
                if target is not None:
                    yield target, None, plan.get("new")
                continue
            resource = self._resource_for_path(plan["path"])
            if resource is not None:
                yield resource, plan.get("old"), plan.get("new")

    def _resource_for_path(self, path: Path) -> str | None:
        try:
            relative = path.resolve(strict=False).relative_to(self.workspace)
        except ValueError:
            return None
        if not relative.parts:
            return None
        if relative.parts[0] == ".mypr":
            editable = relative.parts[1:2] == ("skills",) or relative.parts[:3] == (
                ".mypr",
                "lib",
                "ws_lib",
            )
            if self.kind != "files" or not editable:
                return None
        return relative.as_posix()

    def _record_one_sync(
        self,
        resource: str,
        old: bytes | None,
        new: bytes | None,
        *,
        expected: dict[str, bytes] | None = None,
    ) -> None:
        self._store_blob_sync(old)
        self._store_blob_sync(new)
        index = self._load_index_sync(resource)
        _upgrade_index(index)
        _append_records(
            index,
            _content_map((old, new), include_absent=self.kind == "files"),
            version=self._version,
        )
        _prune_index(index)
        _check_index_size(index)
        encoded = _encode_index(index)
        if expected is not None:
            expected[resource] = encoded
        self._write_index_sync(resource, index)

    def _read_index_bytes_sync(self, resource: str) -> bytes | None:
        path = self._metadata_path(self._index_path(resource))
        try:
            return read_bytes(path, max_bytes=_MAX_INDEX_BYTES)
        except FileNotFoundError:
            return None

    def _restore_index_changes_sync(
        self, snapshots: dict[str, bytes | None], expected: dict[str, bytes]
    ) -> None:
        for resource in reversed(tuple(expected)):
            path = self._metadata_path(self._index_path(resource))
            current = self._read_index_bytes_sync(resource)
            original = snapshots.get(resource)
            if current == original:
                continue
            if current != expected[resource]:
                raise RevisionIndexOutcomeUnknown(
                    f"revision index changed during rollback for {resource}"
                )
            if original is None:
                path.unlink(missing_ok=True)
            else:
                _write_index_bytes_sync(path, original)

    def _load_index_sync(self, resource: str) -> dict[str, Any]:
        path = self._index_path(resource)
        metadata_path = self._metadata_path(path)
        try:
            raw = read_bytes(metadata_path, max_bytes=_MAX_INDEX_BYTES)
        except FileNotFoundError:
            return _empty_index(self.kind, resource, self._version)
        except ValueError as exc:
            raise ValueError("revision index exceeds its size limit") from exc
        try:
            index = json.loads(raw.decode("utf-8"))
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise ValueError("revision index is invalid JSON") from exc
        return _validate_index(index, self.kind, resource, len(raw))

    def _write_index_sync(self, resource: str, index: dict[str, Any]) -> None:
        path = self._metadata_path(self._index_path(resource))
        _write_index_bytes_sync(path, _encode_index(index))

    def _store_blob_sync(self, data: bytes | None) -> None:
        if data is None:
            return
        revision = hashlib.sha256(data).hexdigest()
        if len(data) > _MAX_BLOB_BYTES:
            raise ValueError(
                f"history revision exceeds {_MAX_BLOB_BYTES} bytes; retry with history=False"
            )
        _write_blob(self._blob_path(revision), data)

    async def history(
        self, resource_path: str, *, limit: int = 20, cursor: int | None = None
    ) -> dict[str, Any]:
        _validate_page(limit, cursor)
        resource = _validate_resource(resource_path)
        index, _ = await self._load_index(resource, self._index_path(resource))
        eligible = index["revisions"]
        if cursor is not None:
            eligible = [item for item in eligible if item["sequence"] < cursor]
        page = list(reversed(eligible[-limit:]))
        has_more = len(eligible) > len(page)
        result = {
            "resource": resource,
            "items": page,
            "next_cursor": page[-1]["sequence"] if page and has_more else None,
            "has_more": has_more,
        }
        if index.get("version") == 2:
            result["pruned_before"] = index.get("pruned_before", 0)
        return result

    async def read_revision(
        self,
        resource_path: str,
        revision: str,
        *,
        start_byte: int = 0,
        max_bytes: int = 32_768,
    ) -> dict[str, Any]:
        _validate_revision(self.kind, revision)
        _validate_page_size(start_byte, max_bytes)
        resource = _validate_resource(resource_path)
        index_error = None
        try:
            index, _ = await self._load_index(resource, self._index_path(resource))
            record = next(
                (item for item in index["revisions"] if item["revision"] == revision), None
            )
        except Exception as exc:
            record = None
            index_error = str(exc)
        if revision == _ABSENT_REVISION:
            if record is None:
                raise FileNotFoundError(f"Revision {revision} is not recorded for {resource}")
            if start_byte:
                raise ValueError("start_byte is past the end of the absent revision")
            return {
                "resource": resource,
                "revision": revision,
                "recorded": True,
                "absent": True,
                "text": "",
                "binary": False,
                "start_byte": 0,
                "size": 0,
                "next_cursor": None,
                "truncated": False,
            }
        data = await asyncio.to_thread(self._read_blob, revision)
        if record is not None and len(data) != record["size"]:
            raise RuntimeError(f"Revision {revision} has an inconsistent recorded size")
        if start_byte > len(data):
            raise ValueError("start_byte is past the end of the revision")
        chunk = data[start_byte : start_byte + max_bytes]
        try:
            text = chunk.decode("utf-8")
            consumed = len(chunk)
            binary = False
        except UnicodeDecodeError as exc:
            if exc.reason == "unexpected end of data" and exc.end == len(chunk):
                chunk = chunk[: exc.start]
                if not chunk and start_byte < len(data):
                    lead = data[start_byte]
                    width = 2 if lead & 0xE0 == 0xC0 else 3 if lead & 0xF0 == 0xE0 else 4
                    chunk = data[start_byte : start_byte + width]
                text = chunk.decode("utf-8")
                consumed = len(chunk)
                binary = False
            else:
                if self.kind != "files":
                    raise ValueError("start_byte must point to a UTF-8 boundary") from exc
                text = None
                consumed = len(chunk)
                binary = True
        next_byte = start_byte + consumed
        result = {
            "resource": resource,
            "revision": revision,
            "recorded": record is not None,
            "text": text,
            "start_byte": start_byte,
            "size": len(data),
            "next_cursor": next_byte if next_byte < len(data) else None,
            "truncated": next_byte < len(data),
        }
        if self.kind == "files":
            result["binary"] = binary
        if binary:
            result["data_base64"] = base64.b64encode(chunk).decode("ascii")
            result.pop("text", None)
        if index_error is not None:
            result["index_error"] = index_error
        return result

    async def _target_revision(self, resource: str) -> str | None:
        try:
            page = await self.fs.read(resource, max_bytes=_MAX_BLOB_BYTES)
        except FileNotFoundError:
            return None
        if page.get("truncated"):
            raise ValueError("resource exceeds revision rollback size limit")
        return page["revision"]

    async def _compensate(self, resource: str, old: str | None, new_revision: str) -> None:
        if old is not None:
            try:
                await self.fs.write(
                    resource,
                    old,
                    expected_hash=new_revision,
                    create_parents=True,
                    history=False,
                )
            except BaseException:
                if await self._target_revision(resource) == _hash_text(old):
                    return
                raise
            return
        path = self._safe_path(resource)
        lock_factory = getattr(self.fs, "_lock", None)
        lock = lock_factory(path) if lock_factory is not None else None
        if lock is not None:
            await lock.acquire()
        try:
            status, detail = await asyncio.to_thread(_unlink_if_revision, path, new_revision)
        finally:
            if lock is not None:
                lock.release()
        if status not in {"deleted", "already_absent"}:
            raise RuntimeError(detail)

    def _write_failure(
        self,
        resource: str,
        old_revision: str | None,
        new_revision: str,
        index_error: BaseException,
        rollback_error: BaseException,
    ) -> RuntimeError:
        return RuntimeError(
            f"Revision index write failed for {resource} ({index_error}); rollback could not "
            f"safely restore the target ({rollback_error}). Recovery did not overwrite the target. "
            f"Prior content is recoverable with revision {old_revision or 'absent'}; "
            f"the attempted content is revision {new_revision}. Unindexed revisions can be "
            "read with read_revision()."
        )

    async def restore_content(self, resource_path: str, revision: str) -> str:
        data = await self.restore_bytes(resource_path, revision)
        if data is None:
            raise ValueError(f"Revision {revision} represents an absent file")
        try:
            return data.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ValueError(f"Revision {revision} is binary") from exc

    async def restore_bytes(self, resource_path: str, revision: str) -> bytes | None:
        _validate_revision(self.kind, revision)
        resource = _validate_resource(resource_path)
        index, _ = await self._load_index(resource, self._index_path(resource))
        record = next((item for item in index["revisions"] if item["revision"] == revision), None)
        if record is None:
            raise FileNotFoundError(f"Revision {revision} is not recorded for {resource}")
        if revision == _ABSENT_REVISION:
            return None
        data = await asyncio.to_thread(self._read_blob, revision)
        if len(data) != record["size"]:
            raise RuntimeError(f"Revision {revision} has an inconsistent recorded size")
        return data

    async def _store_blob(self, revision: str, data: bytes) -> None:
        if len(data) > _MAX_BLOB_BYTES:
            raise ValueError(f"revision exceeds {_MAX_BLOB_BYTES} byte storage limit")
        path = self._blob_path(revision)
        await asyncio.to_thread(_write_blob, path, data)

    def _read_blob(self, revision: str) -> bytes:
        path = self._blob_path(revision)
        try:
            path.relative_to(self.workspace)
        except ValueError as exc:
            raise ValueError("revision object path escapes workspace") from exc
        return _read_blob_path(path, revision)

    async def _load_index(
        self, resource: str, path: str
    ) -> tuple[dict[str, Any], str | None]:
        metadata_path = self._metadata_path(path)
        try:
            page = await self.fs.read(metadata_path, max_bytes=_MAX_INDEX_BYTES)
        except FileNotFoundError:
            return _empty_index(self.kind, resource, self._version), None
        if page.get("truncated"):
            raise ValueError("revision index exceeds its size limit")
        try:
            text = page["text"]
            index = json.loads(text)
            encoded_size = len(text.encode("utf-8"))
        except (AttributeError, KeyError, TypeError, UnicodeError, json.JSONDecodeError) as exc:
            raise ValueError("revision index is invalid JSON") from exc
        return _validate_index(index, self.kind, resource, encoded_size), page["revision"]

    def _index_path(self, resource: str) -> str:
        key = hashlib.sha256(os.fsencode(f"{self.kind}\0{resource}")).hexdigest()
        return f".mypr/revisions/index/{self.kind}/{key}.json"

    def _blob_path(self, revision: str) -> Path:
        _validate_hash(revision)
        return self._metadata_path(f".mypr/revisions/objects/{revision}")

    def _metadata_path(self, relative: str) -> Path:
        path = Path(relative)
        if path.is_absolute() or ".." in path.parts:
            raise ValueError("revision metadata path escapes workspace")
        candidate = self.workspace.joinpath(path)
        current = self.workspace
        for part in path.parts:
            current /= part
            try:
                info = current.lstat()
            except FileNotFoundError:
                continue
            if stat.S_ISLNK(info.st_mode):
                raise ValueError("revision metadata path must not contain symlinks")
        return candidate

    def _safe_path(self, relative: str) -> Path:
        candidate = (self.workspace / relative).resolve(strict=False)
        try:
            candidate.relative_to(self.workspace)
        except ValueError as exc:
            raise ValueError("revision store path escapes workspace") from exc
        return candidate


def _validate_resource(resource: str) -> str:
    if not isinstance(resource, str) or "\\" in resource or "\x00" in resource:
        raise ValueError("revision resource must be a workspace-relative path")
    path = PurePosixPath(resource)
    if path.is_absolute() or not path.parts or any(part in {"", ".", ".."} for part in path.parts):
        raise ValueError("revision resource must be a workspace-relative path")
    return path.as_posix()


def _validate_hash(revision: str) -> None:
    if not isinstance(revision, str) or not _HASH.fullmatch(revision):
        raise ValueError("revision must be a lowercase SHA-256 hash")


def _validate_revision(kind: str, revision: str) -> None:
    if kind == "files" and revision == _ABSENT_REVISION:
        return
    _validate_hash(revision)


def _validate_page(limit: int, cursor: int | None) -> None:
    if type(limit) is not int or not 1 <= limit <= 100:
        raise ValueError("limit must be an integer between 1 and 100")
    if cursor is not None and (type(cursor) is not int or cursor < 1):
        raise ValueError("cursor must be a positive integer or None")


def _validate_page_size(start_byte: int, max_bytes: int) -> None:
    if type(start_byte) is not int or start_byte < 0:
        raise ValueError("start_byte must be a non-negative integer")
    if type(max_bytes) is not int or not 1 <= max_bytes <= 1024 * 1024:
        raise ValueError("max_bytes must be an integer between 1 and 1048576")


def _content_map(
    contents: Iterable[str | bytes | None], *, include_absent: bool = False
) -> dict[str, bytes | None]:
    unique: dict[str, bytes | None] = {}
    for content in contents:
        if content is None:
            if include_absent:
                unique.setdefault(_ABSENT_REVISION, None)
            continue
        if isinstance(content, str):
            data = content.encode("utf-8")
        elif isinstance(content, bytes):
            data = content
        else:
            raise TypeError("revision contents must be text, bytes, or None")
        unique.setdefault(hashlib.sha256(data).hexdigest(), data)
    return unique


def _hash_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _unlink_if_revision(path: Path, expected_revision: str) -> tuple[str, str]:
    try:
        current = path.lstat()
    except FileNotFoundError:
        return "already_absent", "target is already absent"
    if not stat.S_ISREG(current.st_mode):
        raise RuntimeError("target is no longer a regular file")
    with open_regular(path) as stream:
        current_revision = hashlib.sha256(stream.read(_MAX_BLOB_BYTES + 1)).hexdigest()
    if current_revision != expected_revision:
        return "changed", f"target changed concurrently to revision {current_revision}"
    fd, backup_name = tempfile.mkstemp(
        prefix=_temporary_prefix(path.name, ".rollback-"), dir=path.parent
    )
    os.close(fd)
    backup = Path(backup_name)
    preserve_backup = False
    try:
        try:
            os.replace(path, backup)
        except FileNotFoundError:
            backup.unlink(missing_ok=True)
            return "already_absent", "target is already absent"
        try:
            displaced = backup.lstat()
            if not stat.S_ISREG(displaced.st_mode):
                raise RuntimeError("displaced target is no longer a regular file")
            with open_regular(backup) as stream:
                revision = hashlib.sha256(stream.read(_MAX_BLOB_BYTES + 1)).hexdigest()
            if revision == expected_revision:
                backup.unlink()
                return "deleted", "target removed"
            try:
                os.link(backup, path, follow_symlinks=False)
            except FileExistsError:
                preserve_backup = True
                return "changed", f"concurrent target preserved; displaced file remains at {backup}"
            backup.unlink()
            return "changed", "target changed concurrently and was restored"
        except BaseException as exc:
            if backup.exists() or backup.is_symlink():
                try:
                    os.link(backup, path, follow_symlinks=False)
                except FileExistsError:
                    preserve_backup = True
                    raise RuntimeError(
                        f"displaced file preserved at {backup}; a concurrent target now exists"
                    ) from exc
                except OSError as restore_error:
                    preserve_backup = True
                    raise RuntimeError(
                        f"displaced file preserved at {backup}; target restoration failed: "
                        f"{restore_error}"
                    ) from exc
                else:
                    backup.unlink()
            raise
    finally:
        if not preserve_backup:
            backup.unlink(missing_ok=True)


def _upgrade_index(index: dict[str, Any]) -> None:
    if index.get("version") != 1:
        return
    records = index.get("revisions", [])
    last = records[-1]["sequence"] if records else 0
    index["version"] = 2
    index["next_sequence"] = last + 1
    index.setdefault("pruned_before", 0)


def _append_records(
    index: dict[str, Any], contents: dict[str, bytes | None], *, version: int
) -> None:
    records = index["revisions"]
    next_sequence = index.get("next_sequence", records[-1]["sequence"] + 1 if records else 1)
    for revision, data in contents.items():
        if records and records[-1]["revision"] == revision:
            continue
        item = {
            "sequence": next_sequence,
            "revision": revision,
            "size": 0 if data is None else len(data),
            "created_at": datetime.now(UTC).isoformat(),
        }
        if revision == _ABSENT_REVISION:
            item["absent"] = True
        records.append(item)
        next_sequence += 1
    index["count"] = len(records)
    if version == 2:
        index["next_sequence"] = next_sequence


def _prune_index(index: dict[str, Any]) -> None:
    if index.get("version") != 2:
        return
    records = index["revisions"]
    removed: list[dict[str, Any]] = []
    if len(records) > _MAX_HISTORY_RECORDS:
        count = len(records) - _MAX_HISTORY_RECORDS
        removed.extend(records[:count])
        del records[:count]
    while len(records) > 1 and len(
        json_bytes(index, separators=(",", ":"))
    ) > _MAX_INDEX_BYTES:
        removed.append(records.pop(0))
    if removed:
        index["count"] = len(records)
        index["pruned_before"] = removed[-1]["sequence"]


def _check_index_size(index: dict[str, Any]) -> None:
    encoded = json_bytes(index, separators=(",", ":"))
    if len(encoded) > _MAX_INDEX_BYTES:
        raise ValueError("revision history has reached its metadata size limit")


def _encode_index(index: dict[str, Any]) -> bytes:
    return json_bytes(index, separators=(",", ":"))


def _write_index_bytes_sync(path: Path, encoded: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(
        prefix=_temporary_prefix(path.name), dir=path.parent
    )
    temporary_path = Path(temporary)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_path, path)
    finally:
        temporary_path.unlink(missing_ok=True)


def _write_blob(path: Path, data: bytes) -> None:
    if len(data) > _MAX_BLOB_BYTES:
        raise ValueError(f"revision exceeds {_MAX_BLOB_BYTES} byte storage limit")
    revision = hashlib.sha256(data).hexdigest()
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        existing = _read_blob_path(path, revision)
    except FileNotFoundError:
        pass
    else:
        if existing != data:
            raise RuntimeError(f"Revision object {revision} does not match its hash")
        return
    fd, temporary = tempfile.mkstemp(
        prefix=_temporary_prefix(path.name), dir=path.parent
    )
    temporary_path = Path(temporary)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temporary_path, path)
        except FileExistsError as exc:
            try:
                existing = _read_blob_path(path, revision)
            except FileNotFoundError:
                raise RuntimeError(
                    f"Revision object {revision} disappeared during install"
                ) from exc
            if existing != data:
                raise RuntimeError(f"Revision object {revision} does not match its hash") from exc
    finally:
        temporary_path.unlink(missing_ok=True)


def _read_blob_path(path: Path, revision: str) -> bytes:
    before = path.lstat()
    if stat.S_ISLNK(before.st_mode):
        raise RuntimeError(f"Revision object {revision} is a symlink")
    if not stat.S_ISREG(before.st_mode):
        raise RuntimeError(f"Revision object {revision} is not a regular file")
    flags = os.O_RDONLY | os.O_NONBLOCK | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags)
    try:
        opened = os.fstat(fd)
        if not stat.S_ISREG(opened.st_mode):
            raise RuntimeError(f"Revision object {revision} is not a regular file")
        if opened.st_size > _MAX_BLOB_BYTES:
            raise ValueError(f"revision exceeds {_MAX_BLOB_BYTES} byte read limit")
        with os.fdopen(fd, "rb") as stream:
            fd = -1
            data = stream.read(_MAX_BLOB_BYTES + 1)
            after = os.fstat(stream.fileno())
    finally:
        if fd >= 0:
            os.close(fd)
    try:
        current = path.lstat()
    except FileNotFoundError as exc:
        raise RuntimeError(f"Revision object changed while being validated: {path}") from exc
    if (
        not stat.S_ISREG(current.st_mode)
        or _blob_signature(before) != _blob_signature(opened)
        or _blob_signature(opened) != _blob_signature(after)
        or _blob_signature(after) != _blob_signature(current)
    ):
        raise RuntimeError(f"Revision object {revision} changed while being validated")
    if len(data) > _MAX_BLOB_BYTES:
        raise ValueError(f"revision exceeds {_MAX_BLOB_BYTES} byte read limit")
    if hashlib.sha256(data).hexdigest() != revision:
        raise RuntimeError(f"Revision object {revision} failed its SHA-256 check")
    return data


def _blob_signature(info: os.stat_result) -> tuple[int, int, int, int, int]:
    return (
        info.st_dev,
        info.st_ino,
        info.st_size,
        info.st_mtime_ns,
        info.st_ctime_ns,
    )
