"""Small, bounded records for failures before the workspace manager is reachable."""

from __future__ import annotations

import json
import os
import tempfile
import time
from pathlib import Path
from typing import Any

from .diagnostics import RPCError, error_info, safe_error, safe_text
from .file_io import read_bytes

_NAME = "startup-error.json"
_MAX_BYTES = 16 * 1024


def _path(root: str | os.PathLike[str]) -> Path:
    return Path(root).resolve() / _NAME


def write_startup_failure(
    root: str | os.PathLike[str],
    exc: BaseException,
    *,
    operation: str = "manager_start",
    details: dict[str, Any] | None = None,
) -> Path | None:
    """Persist one bounded startup failure without replacing an existing file midway."""

    target = _path(root)
    bounded_details = dict(details or {})
    for key in ("path", "line", "column"):
        value = getattr(exc, key, None)
        if value is not None:
            bounded_details.setdefault(key, value)
    payload = {
        "schema": 1,
        "time": time.time(),
        "operation": safe_text(operation, 128),
        "error": safe_error(exc, 4096),
        "error_info": error_info(exc, operation, details=bounded_details, limit=4096),
    }
    encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    if len(encoded) > _MAX_BYTES:
        payload["error_info"].pop("details", None)
        payload["error_info"]["truncated"] = True
        encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    while len(encoded) > _MAX_BYTES:
        payload["error"] = payload["error"][:len(payload["error"]) // 2]
        encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="wb", dir=target.parent, prefix=f".{target.name}.", delete=False
            ) as stream:
                temporary = Path(stream.name)
                stream.write(encoded)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, target)
            return target
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
    except OSError:
        return None


def read_startup_failure(root: str | os.PathLike[str]) -> dict[str, Any] | None:
    target = _path(root)
    try:
        data = read_bytes(target, max_bytes=_MAX_BYTES, follow_symlinks=False)
    except (OSError, ValueError):
        return None
    try:
        value = json.loads(data)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(value, dict) or value.get("schema") != 1:
        return None
    return value


def clear_startup_failure(root: str | os.PathLike[str]) -> None:
    try:
        _path(root).unlink()
    except FileNotFoundError:
        pass
    except OSError:
        pass


def startup_error(root: str | os.PathLike[str]) -> RPCError | None:
    record = read_startup_failure(root)
    if record is None:
        return None
    info = record.get("error_info")
    if not isinstance(info, dict):
        info = {}
    details = info.get("details")
    return RPCError(
        str(record.get("error") or info.get("message") or "Workspace manager startup failed"),
        code=str(info.get("code") or "manager_start_failed"),
        operation=str(record.get("operation") or "manager_start"),
        details=details if isinstance(details, dict) else None,
        error_type=str(info.get("type")) if info.get("type") else None,
    )


__all__ = [
    "clear_startup_failure",
    "read_startup_failure",
    "startup_error",
    "write_startup_failure",
]
