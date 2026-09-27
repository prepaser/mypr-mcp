"""Content-addressed history for workspace modules and skills."""

from __future__ import annotations

import asyncio
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

_HASH = re.compile(r"[0-9a-f]{64}\Z")
_TRANSACTION_LOCKS: WeakValueDictionary[Path, asyncio.Lock] = WeakValueDictionary()
_TRANSACTION_LOCKS_GUARD = Lock()
_MAX_INDEX_BYTES = 16 * 1024 * 1024
_MAX_BLOB_BYTES = 64 * 1024 * 1024


class RevisionIndexOutcomeUnknown(RuntimeError):
    """Raised when an index write cannot be distinguished from an external update."""


class RevisionStore:
    def __init__(self, workspace: str | os.PathLike[str], fs: Any, kind: str) -> None:
        if kind not in {"modules", "skills"}:
            raise ValueError("revision kind must be modules or skills")
        self.workspace = Path(workspace).expanduser().resolve()
        self.fs = fs
        self.kind = kind

    @asynccontextmanager
    async def transaction(self, resource_path: str):
        target = self._safe_path(resource_path)
        with _TRANSACTION_LOCKS_GUARD:
            lock = _TRANSACTION_LOCKS.setdefault(target, asyncio.Lock())
        async with lock:
            yield

    async def record(self, resource_path: str, contents: Iterable[str | None]) -> None:
        resource = _validate_resource(resource_path)
        unique = _content_map(contents)
        if not unique:
            return
        await self.prepare(resource, (data.decode("utf-8") for data in unique.values()))
        index_path = self._index_path(resource)
        index, index_hash = await self._load_index(resource, index_path)
        previous_count = index["count"]
        _append_records(index, unique)
        if index["count"] == previous_count:
            return
        _check_index_size(index)
        encoded = json.dumps(index, ensure_ascii=False, separators=(",", ":"))
        planned_hash = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
        try:
            await self.fs.write(
                index_path,
                encoded,
                expected_hash=index_hash,
                create_parents=True,
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
            raise RuntimeError(
                f"Revision index outcome is unknown for {resource}; the target remains at "
                f"revision {new_revision}. Prior content is recoverable with revision "
                f"{old_revision or 'absent'}; inspect read_revision() and history() before "
                "retrying."
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

    async def ensure_capacity(self, resource_path: str, contents: Iterable[str | None]) -> None:
        resource = _validate_resource(resource_path)
        index, _ = await self._load_index(resource, self._index_path(resource))
        unique = _content_map(contents)
        _append_records(index, unique)
        _check_index_size(index)

    async def prepare(self, resource_path: str, contents: Iterable[str | None]) -> None:
        _validate_resource(resource_path)
        unique: dict[str, bytes] = {}
        for content in contents:
            if content is None:
                continue
            data = content.encode("utf-8")
            unique.setdefault(hashlib.sha256(data).hexdigest(), data)
        for revision, data in unique.items():
            await self._store_blob(revision, data)

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
        return {
            "resource": resource,
            "items": page,
            "next_cursor": page[-1]["sequence"] if page and has_more else None,
            "has_more": has_more,
        }

    async def read_revision(
        self,
        resource_path: str,
        revision: str,
        *,
        start_byte: int = 0,
        max_bytes: int = 32_768,
    ) -> dict[str, Any]:
        _validate_hash(revision)
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
        data = await asyncio.to_thread(self._read_blob, revision)
        if record is not None and len(data) != record["size"]:
            raise RuntimeError(f"Revision {revision} has an inconsistent recorded size")
        if start_byte > len(data):
            raise ValueError("start_byte is past the end of the revision")
        chunk = data[start_byte : start_byte + max_bytes]
        try:
            text = chunk.decode("utf-8")
            consumed = len(chunk)
        except UnicodeDecodeError as exc:
            if exc.reason != "unexpected end of data" or exc.end != len(chunk):
                raise ValueError("start_byte must point to a UTF-8 boundary") from exc
            chunk = chunk[: exc.start]
            if not chunk and start_byte < len(data):
                lead = data[start_byte]
                width = 2 if lead & 0xE0 == 0xC0 else 3 if lead & 0xF0 == 0xE0 else 4
                chunk = data[start_byte : start_byte + width]
            text = chunk.decode("utf-8")
            consumed = len(chunk)
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
        _validate_hash(revision)
        resource = _validate_resource(resource_path)
        index, _ = await self._load_index(resource, self._index_path(resource))
        if not any(item["revision"] == revision for item in index["revisions"]):
            raise FileNotFoundError(f"Revision {revision} is not recorded for {resource}")
        data = await asyncio.to_thread(self._read_blob, revision)
        return data.decode("utf-8")

    async def _store_blob(self, revision: str, data: bytes) -> None:
        if len(data) > _MAX_BLOB_BYTES:
            raise ValueError(f"revision exceeds {_MAX_BLOB_BYTES} byte storage limit")
        path = self._blob_path(revision)
        relative = str(path.relative_to(self.workspace))
        try:
            await self.fs.write(relative, data.decode("utf-8"), create_parents=True)
        except FileExistsError as exc:
            existing = await asyncio.to_thread(self._read_blob, revision)
            if existing != data:
                raise RuntimeError(f"Revision object {revision} does not match its hash") from exc

    def _read_blob(self, revision: str) -> bytes:
        path = self._blob_path(revision)
        if path.is_symlink():
            raise RuntimeError(f"Revision object {revision} is a symlink")
        try:
            path.relative_to(self.workspace)
        except ValueError as exc:
            raise ValueError("revision object path escapes workspace") from exc
        with path.open("rb") as stream:
            data = stream.read(_MAX_BLOB_BYTES + 1)
        if len(data) > _MAX_BLOB_BYTES:
            raise ValueError(f"revision exceeds {_MAX_BLOB_BYTES} byte read limit")
        if hashlib.sha256(data).hexdigest() != revision:
            raise RuntimeError(f"Revision object {revision} failed its SHA-256 check")
        return data

    async def _load_index(
        self, resource: str, path: str
    ) -> tuple[dict[str, Any], str | None]:
        self._safe_path(path)
        try:
            page = await self.fs.read(path, max_bytes=_MAX_INDEX_BYTES)
        except FileNotFoundError:
            return {
                "version": 1,
                "kind": self.kind,
                "resource": resource,
                "count": 0,
                "revisions": [],
            }, None
        if page.get("truncated"):
            raise ValueError("revision index exceeds its size limit")
        try:
            index = json.loads(page["text"])
        except (KeyError, TypeError, json.JSONDecodeError) as exc:
            raise ValueError("revision index is invalid JSON") from exc
        if (
            not isinstance(index, dict)
            or type(index.get("version")) is not int
            or index.get("version") != 1
            or index.get("kind") != self.kind
            or index.get("resource") != resource
            or not isinstance(index.get("revisions"), list)
            or type(index.get("count")) is not int
            or index.get("count") != len(index["revisions"])
        ):
            raise ValueError("revision index metadata is invalid")
        if len(page["text"].encode("utf-8")) > _MAX_INDEX_BYTES:
            raise ValueError("revision index exceeds its size limit")
        previous = 0
        for item in index["revisions"]:
            if (
                not isinstance(item, dict)
                or type(item.get("sequence")) is not int
                or item["sequence"] != previous + 1
                or not isinstance(item.get("revision"), str)
                or not _HASH.fullmatch(item["revision"])
                or type(item.get("size")) is not int
                or not 0 <= item["size"] <= _MAX_BLOB_BYTES
                or not isinstance(item.get("created_at"), str)
                or len(item["created_at"]) > 64
            ):
                raise ValueError("revision index record is invalid")
            previous = item["sequence"]
        return index, page["revision"]

    def _index_path(self, resource: str) -> str:
        key = hashlib.sha256(f"{self.kind}\0{resource}".encode()).hexdigest()
        return f".mypr/revisions/index/{self.kind}/{key}.json"

    def _blob_path(self, revision: str) -> Path:
        _validate_hash(revision)
        return self._safe_path(f".mypr/revisions/objects/{revision}")

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


def _content_map(contents: Iterable[str | None]) -> dict[str, bytes]:
    unique: dict[str, bytes] = {}
    for content in contents:
        if content is None:
            continue
        data = content.encode("utf-8")
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
    with path.open("rb") as stream:
        current_revision = hashlib.sha256(stream.read(_MAX_BLOB_BYTES + 1)).hexdigest()
    if current_revision != expected_revision:
        return "changed", f"target changed concurrently to revision {current_revision}"
    fd, backup_name = tempfile.mkstemp(prefix=f".{path.name}.rollback-", dir=path.parent)
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
            with backup.open("rb") as stream:
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


def _append_records(index: dict[str, Any], contents: dict[str, bytes]) -> None:
    records = index["revisions"]
    for revision, data in contents.items():
        if records and records[-1]["revision"] == revision:
            continue
        records.append(
            {
                "sequence": (records[-1]["sequence"] if records else 0) + 1,
                "revision": revision,
                "size": len(data),
                "created_at": datetime.now(UTC).isoformat(),
            }
        )
    index["count"] = len(records)


def _check_index_size(index: dict[str, Any]) -> None:
    encoded = json.dumps(index, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    if len(encoded) > _MAX_INDEX_BYTES:
        raise ValueError("revision history has reached its metadata size limit")
