"""Small, durable registry for managers that can be reloaded without starting them."""

from __future__ import annotations

import fcntl
import hashlib
import heapq
import json
import os
import stat
import tempfile
from pathlib import Path
from typing import Any

from .protocol import CAPABILITIES
from .transport import workspace_id

_MAX_RECORD_BYTES = 64 * 1024
_MAX_RECORDS = 1024
_GENERATION_LIMIT = 256


class RegistryError(RuntimeError):
    """The manager registry could not be read or updated."""


def registry_dir() -> Path:
    state_home = os.environ.get("XDG_STATE_HOME")
    base = Path(state_home).expanduser() if state_home else Path.home() / ".local" / "state"
    return base / "mypr" / "managers"


def _record_name(identity: str) -> str:
    return hashlib.sha256(f"workspace:{identity}".encode()).hexdigest()[:32] + ".json"


def _canonical_path(value: str | os.PathLike[str] | None) -> str | None:
    if value is None:
        return None
    return str(Path(value).expanduser().resolve(strict=False))


def _read_record(path: Path) -> dict[str, Any] | None:
    try:
        info = path.lstat()
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise RegistryError(f"unable to inspect manager record {path}: {exc}") from exc
    if not stat.S_ISREG(info.st_mode) or path.is_symlink() or info.st_size > _MAX_RECORD_BYTES:
        return None
    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )
    fd = -1
    try:
        fd = os.open(path, flags)
        opened = os.fstat(fd)
        if not stat.S_ISREG(opened.st_mode) or opened.st_size > _MAX_RECORD_BYTES:
            return None
        raw = os.read(fd, _MAX_RECORD_BYTES + 1)
        if len(raw) > _MAX_RECORD_BYTES:
            return None
        value = json.loads(raw.decode("utf-8"))
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise RegistryError(f"unable to read manager record {path}: {exc}") from exc
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError):
        return None
    finally:
        if fd >= 0:
            os.close(fd)
    if not isinstance(value, dict):
        return None
    if (
        not isinstance(value.get("workspace_id"), str)
        or not isinstance(value.get("workspace"), str)
        or type(value.get("pid")) is not int
        or value["pid"] <= 1
        or not isinstance(value.get("generation"), str)
        or not 1 <= len(value["generation"]) <= _GENERATION_LIMIT
        or not isinstance(value.get("socket"), str)
        or not isinstance(value.get("capabilities"), list)
        or not all(isinstance(item, str) and item for item in value["capabilities"])
        or not isinstance(value.get("global_path"), (str, type(None)))
    ):
        return None
    return value


def _lock(directory: Path, *, exclusive: bool):
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    directory.chmod(0o700)
    stream = (directory / ".lock").open("a")
    fcntl.flock(stream, fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
    return stream


def register(
    workspace: str | os.PathLike[str],
    socket: str | os.PathLike[str],
    generation: str,
    global_path: str | os.PathLike[str] | None = None,
) -> dict[str, Any]:
    root = Path(workspace).expanduser().resolve()
    identity = workspace_id(root)
    if not isinstance(generation, str) or not 1 <= len(generation) <= _GENERATION_LIMIT:
        raise ValueError("generation must be a bounded non-empty string")
    record = {
        "workspace": str(root),
        "workspace_id": identity,
        "pid": os.getpid(),
        "generation": generation,
        "socket": str(Path(socket).expanduser().resolve(strict=False)),
        "global_path": _canonical_path(global_path),
        "capabilities": list(CAPABILITIES),
    }
    directory = registry_dir()
    lock = _lock(directory, exclusive=True)
    try:
        target = directory / _record_name(identity)
        fd, temporary = tempfile.mkstemp(prefix=f".{target.name}.", dir=directory)
        temporary_path = Path(temporary)
        try:
            os.fchmod(fd, 0o600)
            with os.fdopen(fd, "w", encoding="utf-8") as stream:
                fd = -1
                json.dump(record, stream, ensure_ascii=True, separators=(",", ":"))
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary_path, target)
        finally:
            if fd >= 0:
                os.close(fd)
            temporary_path.unlink(missing_ok=True)
    finally:
        fcntl.flock(lock, fcntl.LOCK_UN)
        lock.close()
    return record


def unregister(
    workspace: str | os.PathLike[str], generation: str, identity: str | None = None
) -> bool:
    root = Path(workspace).expanduser().resolve()
    workspace_identity = identity if identity is not None else workspace_id(root)
    target = registry_dir() / _record_name(workspace_identity)
    lock = _lock(registry_dir(), exclusive=True)
    try:
        record = _read_record(target)
        if (
            record is None
            or record.get("workspace_id") != workspace_identity
            or record.get("pid") != os.getpid()
            or record.get("generation") != generation
        ):
            return False
        try:
            target.unlink()
        except FileNotFoundError:
            return False
        return True
    finally:
        fcntl.flock(lock, fcntl.LOCK_UN)
        lock.close()


def list_managers(
    global_path: str | os.PathLike[str] | None,
) -> list[dict[str, Any]]:
    selected = _canonical_path(global_path)
    directory = registry_dir()
    try:
        lock = _lock(directory, exclusive=False)
    except OSError as exc:
        raise RegistryError(f"unable to access manager registry {directory}: {exc}") from exc
    try:
        def candidates():
            for path in directory.iterdir():
                if path.suffix != ".json":
                    continue
                record = _read_record(path)
                if record is not None and record.get("global_path") == selected:
                    yield path.name, record

        return [
            record
            for _, record in heapq.nsmallest(
                _MAX_RECORDS, candidates(), key=lambda item: item[0]
            )
        ]
    except RegistryError:
        raise
    except OSError as exc:
        raise RegistryError(f"unable to read manager registry {directory}: {exc}") from exc
    finally:
        fcntl.flock(lock, fcntl.LOCK_UN)
        lock.close()


__all__ = ["RegistryError", "list_managers", "register", "registry_dir", "unregister"]
