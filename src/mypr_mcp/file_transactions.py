from __future__ import annotations

import asyncio
import difflib
import hashlib
import os
import stat
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .async_utils import wait_owned
from .revisions import RevisionIndexOutcomeUnknown

_TEMP_PREFIX_NAME_LIMIT = 32


class _ReadLimitExceeded(ValueError):
    def __init__(self, path: Path, limit: int) -> None:
        super().__init__(f"file exceeds {limit} bytes: {path}")
        self.path = path
        self.limit = limit


def _signature(info: os.stat_result) -> tuple[int, int, int, int, int]:
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def _require_regular(info: os.stat_result, display: str) -> None:
    if stat.S_ISDIR(info.st_mode):
        raise IsADirectoryError(display)
    if not stat.S_ISREG(info.st_mode):
        raise ValueError(f"Path must be a regular file: {display}")


def _read_regular(
    path: Path,
    display: str,
    *,
    max_bytes: int | None = None,
    nofollow: bool = False,
) -> tuple[bytes, os.stat_result]:
    flags = os.O_RDONLY | os.O_NONBLOCK
    if nofollow:
        flags |= getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags)
    try:
        info = os.fstat(fd)
        _require_regular(info, display)
        if max_bytes is not None and info.st_size > max_bytes:
            raise _ReadLimitExceeded(path, max_bytes)
        with os.fdopen(fd, "rb") as stream:
            fd = -1
            data = stream.read(-1 if max_bytes is None else max_bytes + 1)
        if max_bytes is not None and len(data) > max_bytes:
            raise _ReadLimitExceeded(path, max_bytes)
        return data, info
    finally:
        if fd >= 0:
            os.close(fd)


def _check_expected(old: bytes | None, expected_hash: str | None) -> None:
    if expected_hash is not None and (old is None or _sha256(old) != expected_hash):
        actual = None if old is None else _sha256(old)
        raise ValueError(f"Revision mismatch: expected {expected_hash}, got {actual}")


def _diff_lines(value: str) -> list[str]:
    if not value:
        return []
    parts = value.split("\n")
    lines = [f"{part}\n" for part in parts[:-1]]
    if parts[-1]:
        lines.append(parts[-1])
    return lines


def _history_blob_limit() -> int:
    from .revisions import _MAX_BLOB_BYTES

    return _MAX_BLOB_BYTES


def _history_size_error(display: str, limit: int | None = None) -> ValueError:
    limit = _history_blob_limit() if limit is None else limit
    return ValueError(
        f"history for {display} exceeds {limit} bytes; retry with history=False"
    )


async def _to_thread_uncancelled(function, *args, **kwargs):
    return await wait_owned(asyncio.to_thread(function, *args, **kwargs))


def _temporary_prefix(name: str, suffix: str = "") -> str:
    return f".{name[:_TEMP_PREFIX_NAME_LIMIT]}{suffix}"


def _open_artifact_parent(path: Path) -> int:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    proc_fd = Path("/proc/self/fd")
    try:
        relative = path.relative_to(proc_fd)
    except ValueError:
        relative = None
    if relative and relative.parts and relative.parts[0].isdigit():
        descriptor = os.dup(int(relative.parts[0]))
        try:
            for part in relative.parts[1:]:
                next_descriptor = os.open(part, flags, dir_fd=descriptor)
                os.close(descriptor)
                descriptor = next_descriptor
            info = os.fstat(descriptor)
            if not stat.S_ISDIR(info.st_mode):
                raise NotADirectoryError(str(path))
            return descriptor
        except BaseException:
            os.close(descriptor)
            raise
    return os.open(path, flags)


def _artifact_identity(fd: int) -> tuple[int, int]:
    info = os.fstat(fd)
    return info.st_dev, info.st_ino


def _bind_artifact_parent(fd: int, fallback: int) -> int:
    try:
        target = Path(os.readlink(f"/proc/self/fd/{fd}")).parent
        bound = _open_artifact_parent(target)
    except (OSError, ValueError):
        return fallback
    os.close(fallback)
    return bound


def _remove_artifact(
    path: Path,
    parent_fd: int,
    identity: tuple[int, int],
) -> None:
    try:
        info = path.lstat()
    except FileNotFoundError:
        info = None
    if info is not None and (info.st_dev, info.st_ino) == identity:
        parent_info = path.parent.lstat()
        owner_info = os.fstat(parent_fd)
        if (parent_info.st_dev, parent_info.st_ino) == (
            owner_info.st_dev,
            owner_info.st_ino,
        ):
            path.unlink(missing_ok=True)
            return
    try:
        info = os.stat(path.name, dir_fd=parent_fd, follow_symlinks=False)
    except FileNotFoundError:
        return
    if (info.st_dev, info.st_ino) != identity:
        return
    os.unlink(path.name, dir_fd=parent_fd)


def _remove_created_dirs(
    created: list[tuple[Path, tuple[int, int]]],
) -> list[tuple[Path, OSError]]:
    errors: list[tuple[Path, OSError]] = []
    for directory, identity in reversed(created):
        try:
            info = directory.lstat()
            if (
                not stat.S_ISDIR(info.st_mode)
                or (info.st_dev, info.st_ino) != identity
            ):
                continue
            directory.rmdir()
        except FileNotFoundError:
            continue
        except OSError as exc:
            errors.append((directory, exc))
    return errors


@dataclass
class _State:
    path: Path
    display: str
    exists: bool
    data: bytes | None
    info: os.stat_result | None

    @property
    def revision(self) -> str | None:
        return None if self.data is None else _sha256(self.data)


def _history_limit(path: Path, history_store: Any) -> int | None:
    if history_store is None or history_store._resource_for_path(path) is None:
        return None
    return _history_blob_limit()


def _read_state(
    path: Path,
    display: str,
    max_bytes: int | None = None,
    *,
    no_symlink: bool = False,
) -> _State:
    try:
        info = path.lstat() if no_symlink else path.stat()
    except FileNotFoundError:
        return _State(path, display, False, None, None)
    if no_symlink and stat.S_ISLNK(info.st_mode):
        raise ValueError(f"Path must not be a symlink: {display}")
    _require_regular(info, display)
    if max_bytes is not None and info.st_size > max_bytes:
        raise _history_size_error(display, max_bytes)
    try:
        data, opened = _read_regular(
            path, display, max_bytes=max_bytes, nofollow=no_symlink
        )
    except _ReadLimitExceeded as exc:
        if max_bytes is not None:
            raise _history_size_error(display, exc.limit) from exc
        raise
    after = path.lstat() if no_symlink else path.stat()
    if no_symlink and stat.S_ISLNK(after.st_mode):
        raise ValueError(f"Path must not be a symlink: {display}")
    if _signature(info) != _signature(opened) or _signature(info) != _signature(after):
        raise RuntimeError(f"File changed while reading: {display}")
    return _State(path, display, True, data, after)


def _plan(
    operation: str,
    path: Path,
    display: str,
    old: bytes | None,
    new: bytes | None,
    old_info: os.stat_result | None,
    mode: int | None,
    *,
    source: Path | None = None,
    source_display: str | None = None,
    source_old: bytes | None = None,
    source_old_info: os.stat_result | None = None,
) -> dict[str, Any]:
    return {
        "operation": operation,
        "path": path,
        "display": display,
        "source": source,
        "source_display": source_display,
        "source_old": source_old,
        "source_old_info": source_old_info,
        "old": old,
        "new": new,
        "old_info": old_info,
        "mode": mode,
    }


def _result(plans: list[dict[str, Any]], dry_run: bool, limit: int) -> dict[str, Any]:
    output: list[dict[str, Any]] = []
    diff_parts: list[str] = []
    total = 0
    truncated = False
    for plan in plans:
        old_value = plan["source_old"] if plan["operation"] == "move" else plan["old"]
        old = old_value or b""
        new = plan["new"] or b""
        changed = (
            old != new or plan["operation"] in {"add", "delete", "move"} or _mode_changed(plan)
        )
        entry = {
            "operation": plan["operation"],
            "path": plan["display"],
            "changed": changed,
            "old_revision": _sha256(old) if old_value is not None else None,
            "revision": _sha256(new) if plan["new"] is not None else None,
            "size": len(new) if plan["new"] is not None else 0,
        }
        if plan["source_display"] is not None:
            entry["source"] = plan["source_display"]
        output.append(entry)
        if not truncated:
            for line in difflib.unified_diff(
                _diff_lines(old.decode("utf-8", "replace")),
                _diff_lines(new.decode("utf-8", "replace")),
                fromfile=(plan["source_display"] or plan["display"])
                if old_value is not None
                else "/dev/null",
                tofile=plan["display"] if plan["new"] is not None else "/dev/null",
            ):
                if not line.endswith("\n"):
                    line += "\n\\ No newline at end of file\n"
                encoded = line.encode("utf-8")
                if total + len(encoded) > limit:
                    truncated = True
                    break
                diff_parts.append(line)
                total += len(encoded)
    return {
        "changes": output,
        "dry_run": dry_run,
        "diff": "".join(diff_parts),
        "diff_truncated": truncated,
    }


def _commit(
    plans: list[dict[str, Any]],
    states: dict[Path, _State],
    limit: int,
    *,
    history_store: Any = None,
    no_symlink: bool = False,
) -> dict[str, Any]:
    temporaries: dict[Path, Path] = {}
    backups: dict[Path, Path] = {}
    created_dirs: list[tuple[Path, tuple[int, int]]] = []
    artifact_parents: dict[Path, tuple[int, tuple[int, int]]] = {}
    committed: list[
        tuple[Path, bytes | None, os.stat_result | None, bytes | None, os.stat_result | None]
    ] = []
    preserve_backups = False
    primary: BaseException | None = None
    wrapped: BaseException | None = None
    completed = False
    active_plans = [plan for plan in plans if not _is_noop_update(plan)]
    try:
        for plan in active_plans:
            if plan["new"] is None:
                continue
            parent = plan["path"].parent
            if not parent.exists():
                missing: list[Path] = []
                current = parent
                while not current.exists():
                    missing.append(current)
                    current = current.parent
                for directory in reversed(missing):
                    directory.mkdir()
                    info = directory.lstat()
                    created_dirs.append((directory, (info.st_dev, info.st_ino)))
            parent_fd = _open_artifact_parent(parent)
            fd: int | None = None
            try:
                fd, name = tempfile.mkstemp(
                    prefix=_temporary_prefix(plan["path"].name, "."), dir=parent
                )
                parent_fd = _bind_artifact_parent(fd, parent_fd)
                artifact_parents[Path(name)] = (parent_fd, _artifact_identity(fd))
            except BaseException:
                if fd is not None:
                    os.close(fd)
                os.close(parent_fd)
                raise
            temp = Path(name)
            temporaries[plan["path"]] = temp
            with os.fdopen(fd, "wb") as stream:
                if plan["mode"] is not None:
                    os.fchmod(stream.fileno(), plan["mode"])
                stream.write(plan["new"])
                stream.flush()
                os.fsync(stream.fileno())
        for plan in active_plans:
            backup_path = plan["source"] or plan["path"]
            state = states[backup_path]
            if not state.exists or backup_path in backups:
                continue
            parent_fd = _open_artifact_parent(backup_path.parent)
            fd = None
            try:
                fd, name = tempfile.mkstemp(
                    prefix=_temporary_prefix(backup_path.name, ".mypr-backup."),
                    dir=backup_path.parent,
                )
                parent_fd = _bind_artifact_parent(fd, parent_fd)
                artifact_parents[Path(name)] = (parent_fd, _artifact_identity(fd))
            except BaseException:
                if fd is not None:
                    os.close(fd)
                os.close(parent_fd)
                raise
            backup = Path(name)
            backups[backup_path] = backup
            with os.fdopen(fd, "wb") as stream:
                os.fchmod(stream.fileno(), stat.S_IMODE(state.info.st_mode))
                stream.write(state.data)
                stream.flush()
                os.fsync(stream.fileno())
        for state in states.values():
            _assert_unchanged(
                state,
                _read_state(
                    state.path,
                    state.display,
                    _history_limit(state.path, history_store),
                ),
            )
        for plan in active_plans:
            path = plan["path"]
            state = states.get(path)
            current = _read_state(
                path,
                plan["display"],
                _history_limit(path, history_store),
                no_symlink=no_symlink,
            )
            if state is None:
                state = _State(path, plan["display"], False, None, None)
            _assert_unchanged(state, current)
            if plan["operation"] == "delete":
                path.unlink()
                committed.append((path, plan["old"], plan["old_info"], None, None))
            elif state.exists:
                os.replace(temporaries[path], path)
                committed.append((path, plan["old"], plan["old_info"], plan["new"], None))
            else:
                os.link(temporaries[path], path)
                committed.append((path, plan["old"], plan["old_info"], plan["new"], None))
                temporaries[path].unlink(missing_ok=True)
            if plan["operation"] != "delete":
                post_info = path.stat()
                committed[-1] = (
                    path,
                    plan["old"],
                    plan["old_info"],
                    plan["new"],
                    post_info,
                )
            if plan["source"] is not None:
                source = plan["source"]
                source_state = states[source]
                current_source = _read_state(
                    source,
                    plan["source_display"] or str(source),
                    _history_limit(source, history_store),
                    no_symlink=no_symlink,
                )
                _assert_unchanged(source_state, current_source)
                source.unlink()
                committed.append((source, source_state.data, source_state.info, None, None))
        if history_store is not None:
            try:
                history_store.record_changes_sync(plans)
            except RevisionIndexOutcomeUnknown as exc:
                preserve_backups = True
                if backups:
                    exc.add_note(
                        "patch changes remain in place; recovery backups preserved at "
                        + ", ".join(str(path) for path in backups.values())
                    )
                raise
            except BaseException as exc:
                raise RuntimeError("history record failed; patch will be rolled back") from exc
        result = _result(plans, False, limit)
        completed = True
        return result
    except BaseException as exc:
        primary = exc
        if isinstance(exc, RevisionIndexOutcomeUnknown):
            preserve_backups = True
            raise
        recovery: list[str] = []
        for path, old, old_info, expected_new, expected_info in reversed(committed):
            try:
                current = _read_state(
                    path,
                    str(path),
                    _history_limit(path, history_store),
                    no_symlink=no_symlink,
                )
                if expected_new is None and current.exists:
                    raise RuntimeError("path changed outside this patch")
                if expected_new is not None and (
                    not current.exists or current.data != expected_new
                ):
                    raise RuntimeError("path changed outside this patch")
                if expected_info is not None and (
                    current.info is None or _signature(current.info) != _signature(expected_info)
                ):
                    raise RuntimeError("path metadata changed outside this patch")
                if old is None:
                    if current.exists:
                        path.unlink()
                else:
                    _restore(path, old, old_info)
            except BaseException as restore_exc:
                recovery.append(f"{path}: {restore_exc}")
        if recovery:
            preserve_backups = True
            wrapped = RuntimeError(
                "patch failed and rollback was incomplete: "
                f"{'; '.join(recovery)}; backups preserved at "
                f"{', '.join(str(path) for path in backups.values())}"
            )
            raise wrapped from exc
        raise
    finally:
        cleanup_errors: list[str] = []
        for temporary in temporaries.values():
            try:
                parent = artifact_parents.get(temporary)
                if parent is None:
                    temporary.unlink(missing_ok=True)
                else:
                    _remove_artifact(temporary, parent[0], parent[1])
            except BaseException as cleanup_exc:
                cleanup_errors.append(f"{temporary}: {cleanup_exc}")
        if not preserve_backups:
            for backup in backups.values():
                try:
                    parent = artifact_parents.get(backup)
                    if parent is None:
                        backup.unlink(missing_ok=True)
                    else:
                        _remove_artifact(backup, parent[0], parent[1])
                except BaseException as cleanup_exc:
                    cleanup_errors.append(f"{backup}: {cleanup_exc}")
        if not completed and not isinstance(primary, RevisionIndexOutcomeUnknown):
            for directory, cleanup_error in _remove_created_dirs(created_dirs):
                cleanup_errors.append(f"{directory}: {cleanup_error}")
        for parent_fd, _ in artifact_parents.values():
            try:
                os.close(parent_fd)
            except OSError as cleanup_exc:
                cleanup_errors.append(f"artifact parent fd: {cleanup_exc}")
        if cleanup_errors:
            preserve_backups = True
            note = "patch cleanup failed; artifacts may remain at: " + "; ".join(cleanup_errors)
            target = wrapped or primary
            if target is not None:
                target.add_note(note)
            elif completed:
                result["warnings"] = [note]
            else:
                raise RuntimeError(note)


def _assert_unchanged(expected: _State, current: _State) -> None:
    if (
        expected.exists != current.exists
        or expected.data != current.data
        or expected.info is not None
        and current.info is not None
        and _signature(expected.info) != _signature(current.info)
    ):
        raise RuntimeError(f"File changed while applying patch: {expected.display}")


def _mode_changed(plan: dict[str, Any]) -> bool:
    old_info = plan["old_info"]
    return (
        plan["operation"] == "update"
        and plan["old"] == plan["new"]
        and old_info is not None
        and plan["mode"] is not None
        and stat.S_IMODE(old_info.st_mode) != plan["mode"]
    )


def _is_noop_update(plan: dict[str, Any]) -> bool:
    return plan["operation"] == "update" and plan["old"] == plan["new"] and not _mode_changed(plan)


def _set_history_status(
    result: dict[str, Any], history_store: Any, plans: list[dict[str, Any]]
) -> None:
    resources = {
        history_store._resource_for_path(path)
        for plan in plans
        for path in (plan["path"], plan["source"])
        if path is not None
    }
    recorded = bool(resources - {None})
    result["history_recorded"] = recorded
    if None in resources:
        if recorded:
            result["history_partial"] = True
        result.setdefault("warnings", []).append(
            "History was not recorded for paths outside the workspace history scope."
        )


def _restore(path: Path, data: bytes, info: os.stat_result | None) -> None:
    mode = stat.S_IMODE(info.st_mode) if info is not None else 0o600
    parent = path.parent
    fd, name = tempfile.mkstemp(
        prefix=_temporary_prefix(path.name, ".rollback."), dir=parent
    )
    temporary = Path(name)
    try:
        with os.fdopen(fd, "wb") as stream:
            os.fchmod(stream.fileno(), mode)
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()
